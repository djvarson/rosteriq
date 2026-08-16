"""
The event-log spine (services/events.py + db.list_events + the request
middleware hooks) tested against MemoryStore.

Covers: record_event meta keys and category fallback; _scrub redaction and
size/depth caps; oversize-payload truncation; never-raise guarantees;
list_events filters; the 401/403 security rows written by the request
middleware (and the /api/auth/* exemption); the global exception handler's
http.unhandled error row; and PostgresStore._event_row's details parsing.
"""

import json
import uuid
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, PostgresStore, get_db
from rosteriq.services import events as ev
from rosteriq.services.events import (
    _scrub, audit, error, record_event, security, _MAX_DETAIL_CHARS,
)

PASSWORD = "Passw0rd!234"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PASSWORD, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, owner_h, vid):
    r = c.post("/venues", json={
        "id": vid, "name": "Events Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code in (200, 201), r.text


def _manager_at(c, email, vid):
    """Register a user and make them a manager of ``vid`` only (store-level)."""
    h = _register_login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    return h


class _Boom(Exception):
    pass


class _ExplodingStore:
    """A store whose save_audit_log always raises — the write must be swallowed."""

    def __init__(self):
        self.calls = 0

    def save_audit_log(self, entry):
        self.calls += 1
        raise _Boom("db down")


# ---------------------------------------------------------------------------
# 1) record_event: meta keys, created_at, category fallback
# ---------------------------------------------------------------------------

def test_record_event_writes_row_with_meta_keys_and_created_at():
    store = MemoryStore()
    before = datetime.utcnow()
    record_event("audit", "timesheet.approve", venue_id="v1", resource_type="timesheet",
                 resource_id="ts-9", details={"employee": "Ava", "hours": 7.5}, db=store)
    rows = store.list_events(venue_id="v1")
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "timesheet.approve"
    assert row["venue_id"] == "v1"
    assert row["resource_type"] == "timesheet"
    assert row["resource_id"] == "ts-9"
    d = row["details"]
    # meta keys always present
    for k in ("category", "outcome", "correlation_id", "ip", "role"):
        assert k in d, k
    assert d["category"] == "audit"
    assert d["outcome"] == "ok"
    assert isinstance(d["correlation_id"], str) and d["correlation_id"]
    # caller kwargs land beside the meta keys
    assert d["employee"] == "Ava" and d["hours"] == 7.5
    assert isinstance(row["created_at"], datetime)
    assert before - timedelta(seconds=1) <= row["created_at"] <= datetime.utcnow() + timedelta(seconds=1)


def test_record_event_unknown_category_falls_back_to_audit():
    store = MemoryStore()
    record_event("bogus", "thing.do", venue_id="v1", db=store)
    rows = store.list_events(venue_id="v1")
    assert len(rows) == 1
    assert rows[0]["details"]["category"] == "audit"
    # and it is findable via the category filter
    assert store.list_events(category="audit")[0]["action"] == "thing.do"
    assert store.list_events(category="bogus") == []


def test_record_event_outcome_and_explicit_user_id_are_stored():
    store = MemoryStore()
    record_event("security", "auth.login_failed", outcome="denied",
                 user_id="u-42", details={"email": "x@y.com"}, db=store)
    row = store.list_events(category="security")[0]
    assert row["action"] == "auth.login_failed"
    assert row["user_id"] == "u-42"
    assert row["venue_id"] is None
    assert row["details"]["outcome"] == "denied"
    assert row["details"]["email"] == "x@y.com"


def test_wrappers_audit_security_error_shape():
    # The wrappers take no db= argument; they write to the global store, which
    # conftest resets per test.
    audit("roster.publish", "v1", "roster", "r-1", week="2026-08-17", shifts=42)
    security("auth.locked", user_id="u-1", email="a@b.com")
    security("rate.limited", outcome="throttled", venue_id="v1", path="/x")
    error("job.failed", ValueError("bad thing"), venue_id="v1", job="nightly")

    db = get_db()
    a = db.list_events(action_prefix="roster.")[0]
    assert a["action"] == "roster.publish"
    assert a["venue_id"] == "v1" and a["resource_type"] == "roster" and a["resource_id"] == "r-1"
    assert a["details"]["category"] == "audit" and a["details"]["outcome"] == "ok"
    assert a["details"]["week"] == "2026-08-17" and a["details"]["shifts"] == 42

    s = db.list_events(action_prefix="auth.locked")[0]
    assert s["details"]["category"] == "security"
    assert s["details"]["outcome"] == "denied"  # security() default
    assert s["user_id"] == "u-1" and s["details"]["email"] == "a@b.com"

    t = db.list_events(action_prefix="rate.")[0]
    assert t["details"]["outcome"] == "throttled" and t["venue_id"] == "v1"

    e = db.list_events(category="error")[0]
    assert e["action"] == "job.failed"
    assert e["details"]["outcome"] == "error"
    assert e["details"]["exception"] == "ValueError"
    assert e["details"]["message"] == "bad thing"
    assert e["details"]["job"] == "nightly"


def test_caller_context_is_captured_from_contextvars():
    """user_id/role/correlation_id/ip come from the tenant + logging contextvars."""
    from rosteriq.middleware.tenant import TenantContext, _tenant_context_var
    from rosteriq.middleware.logging import (
        _correlation_id_var, _request_ip_var, set_correlation_id, set_request_ip,
    )
    store = MemoryStore()
    t_tok = _tenant_context_var.set(TenantContext(user_id="owner-1", venue_ids=[], is_owner=True))
    c_tok = _correlation_id_var.set(None)
    i_tok = _request_ip_var.set(None)
    try:
        set_correlation_id("cid-ctx-1")
        set_request_ip("203.0.113.9")
        record_event("audit", "employee.update", venue_id="v1", db=store)
    finally:
        _tenant_context_var.reset(t_tok)
        _correlation_id_var.reset(c_tok)
        _request_ip_var.reset(i_tok)
    row = store.list_events(venue_id="v1")[0]
    assert row["user_id"] == "owner-1"
    assert row["details"]["role"] == "owner"
    assert row["details"]["correlation_id"] == "cid-ctx-1"
    assert row["details"]["ip"] == "203.0.113.9"

    # explicit user_id wins over the context
    t_tok = _tenant_context_var.set(TenantContext(user_id="owner-1", venue_ids=[], is_owner=True))
    try:
        record_event("audit", "employee.update", venue_id="v2", user_id="explicit-u", db=store)
    finally:
        _tenant_context_var.reset(t_tok)
    assert store.list_events(venue_id="v2")[0]["user_id"] == "explicit-u"


# ---------------------------------------------------------------------------
# 2) _scrub
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("key", [
    "password", "Password", "new_password", "secret", "client_secret", "token",
    "api_key", "API_KEY", "authorization", "Authorization", "refresh_token",
    "access_token", "cf_password", "card", "card_number", "cvv", "tfn",
    "tax_file_number", "apikey", "passwd",
])
def test_scrub_redacts_secret_shaped_keys_top_level(key):
    out = _scrub({key: "s3cr3t", "name": "ok"})
    assert out[key] == "[redacted]"
    assert out["name"] == "ok"


def test_scrub_redacts_at_any_depth_including_inside_lists():
    payload = {
        "user": {"email": "a@b.com", "password": "pw", "profile": {"api_key": "k", "bio": "hi"}},
        "connections": [
            {"provider": "xero", "access_token": "tok", "refresh_token": "r"},
            {"provider": "myob", "nested": {"cf_password": "x", "keep": 1}},
        ],
        # NB: key is "payment_methods" not "cards" — a key containing "card"
        # would (correctly) redact the whole list; here we want to reach inside.
        "payment_methods": [{"card": "4111", "cvv": "123", "last4": "1111"}],
        "tfn": "123 456 789",
    }
    out = _scrub(payload)
    assert out["user"]["password"] == "[redacted]"
    assert out["user"]["email"] == "a@b.com"
    assert out["user"]["profile"]["api_key"] == "[redacted]"
    assert out["user"]["profile"]["bio"] == "hi"
    assert out["connections"][0]["access_token"] == "[redacted]"
    assert out["connections"][0]["refresh_token"] == "[redacted]"
    assert out["connections"][0]["provider"] == "xero"
    assert out["connections"][1]["nested"]["cf_password"] == "[redacted]"
    assert out["connections"][1]["nested"]["keep"] == 1
    assert out["payment_methods"][0]["card"] == "[redacted]"
    assert out["payment_methods"][0]["cvv"] == "[redacted]"
    assert out["payment_methods"][0]["last4"] == "1111"
    assert out["tfn"] == "[redacted]"
    # a key that merely CONTAINS a hint ("cards") redacts the whole value
    assert _scrub({"cards": [{"last4": "1111"}]}) == {"cards": "[redacted]"}
    # the secret VALUES never appear anywhere in the scrubbed output
    encoded = json.dumps(out)
    for secret in ("pw", '"k"', '"tok"', '"r"', '"x"', "4111", '"123"', "123 456 789"):
        assert secret not in encoded, secret


def test_scrub_caps_long_strings_at_500():
    long = "a" * 1200
    out = _scrub({"note": long, "short": "b" * 500})
    assert len(out["note"]) == 500
    assert out["note"].endswith("...")
    assert out["note"].startswith("a" * 497)
    assert out["short"] == "b" * 500  # exactly 500 is left alone
    # bare string too
    assert len(_scrub("z" * 10_000)) == 500


def test_scrub_caps_lists_at_50():
    out = _scrub({"items": list(range(200))})
    assert out["items"] == list(range(50))
    assert _scrub(tuple(range(80))) == list(range(50))


def test_scrub_caps_depth():
    deep = {"a": {"b": {"c": {"d": {"e": {"f": {"g": 1}}}}}}}
    out = _scrub(deep)
    # depth 0..4 survive; anything below is collapsed to the ellipsis marker
    assert out["a"]["b"]["c"]["d"]["e"] == "…"
    # lists count towards depth too
    nested_list = [[[[[["too deep"]]]]]]
    assert _scrub(nested_list) == [[[[["…"]]]]]


def test_scrub_passes_scalars_and_stringifies_unknown_types():
    class Thing:
        def __repr__(self):
            return "<Thing " + "x" * 400 + ">"
    out = _scrub({"n": 1, "f": 1.5, "b": True, "none": None, "obj": Thing()})
    assert out["n"] == 1 and out["f"] == 1.5 and out["b"] is True and out["none"] is None
    assert isinstance(out["obj"], str) and len(out["obj"]) == 200
    # non-string keys are stringified
    assert _scrub({1: "one"}) == {"1": "one"}


def test_record_event_scrubs_details_before_storing():
    store = MemoryStore()
    record_event("audit", "xero.connect", venue_id="v1", db=store,
                 details={"tenant": "Acme", "access_token": "abc", "refresh_token": "def",
                          "nested": {"client_secret": "zzz"}})
    d = store.list_events(venue_id="v1")[0]["details"]
    assert d["tenant"] == "Acme"
    assert d["access_token"] == "[redacted]"
    assert d["refresh_token"] == "[redacted]"
    assert d["nested"]["client_secret"] == "[redacted]"
    assert "abc" not in json.dumps(d) and "zzz" not in json.dumps(d)


# ---------------------------------------------------------------------------
# 3) oversize payloads collapse but still land
# ---------------------------------------------------------------------------

def test_oversize_details_truncate_to_preview_but_row_still_lands():
    store = MemoryStore()
    # 50 strings of 490 chars each (each under the per-string cap) -> ~24 KB
    big = {"lines": ["x" * 490 for _ in range(50)], "who": "Ava"}
    record_event("audit", "stocktake.complete", venue_id="v1", resource_id="st-1",
                 details=big, db=store)
    rows = store.list_events(venue_id="v1")
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "stocktake.complete" and row["resource_id"] == "st-1"
    d = row["details"]
    assert d["truncated"] is True
    assert d["category"] == "audit" and d["outcome"] == "ok"
    assert "correlation_id" in d
    assert "lines" not in d  # the original payload was dropped
    assert isinstance(d["preview"], str)
    assert len(d["preview"]) <= _MAX_DETAIL_CHARS - 200
    assert d["preview"].startswith('{"category": "audit"')
    assert len(json.dumps(d, default=str)) <= _MAX_DETAIL_CHARS


def test_details_just_under_cap_are_stored_intact():
    store = MemoryStore()
    payload = {"blob": "y" * 400, "more": ["z" * 100 for _ in range(10)]}
    record_event("audit", "waste.log", venue_id="v1", details=payload, db=store)
    d = store.list_events(venue_id="v1")[0]["details"]
    assert "truncated" not in d
    assert d["blob"] == "y" * 400
    assert len(d["more"]) == 10


# ---------------------------------------------------------------------------
# 4) writes never raise
# ---------------------------------------------------------------------------

def test_record_event_swallows_store_write_errors():
    store = _ExplodingStore()
    assert record_event("audit", "employee.create", venue_id="v1", db=store) is None
    assert store.calls == 1


def test_record_event_swallows_errors_from_global_store(monkeypatch):
    db = get_db()

    def boom(entry):
        raise _Boom("disk full")

    monkeypatch.setattr(db, "save_audit_log", boom)
    assert audit("employee.delete", "v1", "employee", "e-1", name="Ben") is None
    assert security("auth.login_failed", email="a@b.com") is None
    assert error("job.failed", RuntimeError("x")) is None


def test_record_event_swallows_get_db_failure(monkeypatch):
    import rosteriq.database as dbmod

    def boom():
        raise _Boom("no database")

    monkeypatch.setattr(dbmod, "get_db", boom)
    assert record_event("audit", "employee.create", venue_id="v1") is None
    assert audit("timesheet.approve", "v1") is None


def test_record_event_swallows_unserialisable_details():
    """Weird detail values (bytes, sets, objects) must not blow up the write."""
    store = MemoryStore()
    record_event("audit", "order.create", venue_id="v1", db=store,
                 details={"raw": b"\x00\x01", "tags": {"a", "b"}, "obj": object()})
    row = store.list_events(venue_id="v1")[0]
    assert row["action"] == "order.create"
    assert isinstance(row["details"]["raw"], str)


# ---------------------------------------------------------------------------
# 5) list_events filters
# ---------------------------------------------------------------------------

def _seed(store):
    base = datetime(2026, 8, 1, 12, 0, 0)
    spec = [
        # (venue, category, action, minutes-offset)
        ("v1", "audit", "timesheet.approve", 0),
        ("v1", "audit", "timesheet.reject", 1),
        ("v1", "security", "access.denied", 2),
        ("v2", "audit", "roster.publish", 3),
        ("v2", "error", "http.unhandled", 4),
        (None, "security", "auth.login_failed", 5),
        ("v1", "audit", "roster.publish", 6),
    ]
    for venue, cat, action, mins in spec:
        store.save_audit_log({
            "venue_id": venue, "user_id": "u", "action": action,
            "resource_type": None, "resource_id": None,
            "details": {"category": cat, "outcome": "ok", "n": mins},
            "created_at": base + timedelta(minutes=mins),
        })
    return base


def test_list_events_newest_first_and_platform_wide_by_default():
    store = MemoryStore()
    _seed(store)
    rows = store.list_events()
    assert len(rows) == 7
    ns = [r["details"]["n"] for r in rows]
    assert ns == sorted(ns, reverse=True)
    assert rows[0]["action"] == "roster.publish" and rows[0]["venue_id"] == "v1"
    assert rows[-1]["action"] == "timesheet.approve"


def test_list_events_filter_venue():
    store = MemoryStore()
    _seed(store)
    v1 = store.list_events(venue_id="v1")
    assert [r["details"]["n"] for r in v1] == [6, 2, 1, 0]
    assert all(r["venue_id"] == "v1" for r in v1)
    v2 = store.list_events(venue_id="v2")
    assert [r["action"] for r in v2] == ["http.unhandled", "roster.publish"]
    assert store.list_events(venue_id="nope") == []


def test_list_events_filter_category():
    store = MemoryStore()
    _seed(store)
    sec = store.list_events(category="security")
    assert [r["action"] for r in sec] == ["auth.login_failed", "access.denied"]
    assert store.list_events(category="error")[0]["action"] == "http.unhandled"
    assert len(store.list_events(category="audit")) == 4
    # combined with venue
    assert [r["action"] for r in store.list_events(venue_id="v1", category="security")] == ["access.denied"]


def test_list_events_filter_action_prefix():
    store = MemoryStore()
    _seed(store)
    ts = store.list_events(action_prefix="timesheet.")
    assert [r["action"] for r in ts] == ["timesheet.reject", "timesheet.approve"]
    rp = store.list_events(action_prefix="roster.publish")
    assert [r["venue_id"] for r in rp] == ["v1", "v2"]
    assert store.list_events(action_prefix="zzz") == []
    # prefix is a plain startswith, so "roster" also matches
    assert len(store.list_events(action_prefix="roster")) == 2


def test_list_events_filter_since():
    store = MemoryStore()
    base = _seed(store)
    rows = store.list_events(since=base + timedelta(minutes=4))
    assert [r["details"]["n"] for r in rows] == [6, 5, 4]  # since is inclusive
    assert store.list_events(since=base + timedelta(hours=1)) == []
    assert len(store.list_events(since=base - timedelta(days=1))) == 7


def test_list_events_limit_offset_paginate():
    store = MemoryStore()
    _seed(store)
    p1 = store.list_events(limit=3, offset=0)
    p2 = store.list_events(limit=3, offset=3)
    p3 = store.list_events(limit=3, offset=6)
    p4 = store.list_events(limit=3, offset=9)
    assert [r["details"]["n"] for r in p1] == [6, 5, 4]
    assert [r["details"]["n"] for r in p2] == [3, 2, 1]
    assert [r["details"]["n"] for r in p3] == [0]
    assert p4 == []
    # limit/offset compose with filters
    assert [r["details"]["n"] for r in store.list_events(venue_id="v1", limit=2, offset=1)] == [2, 1]


def test_list_events_returns_copies_not_live_rows():
    store = MemoryStore()
    _seed(store)
    row = store.list_events(limit=1)[0]
    row["action"] = "tampered"
    assert store.list_events(limit=1)[0]["action"] != "tampered"


# ---------------------------------------------------------------------------
# 6) request middleware: 401/403 security rows
# ---------------------------------------------------------------------------

def test_403_on_venue_scoped_route_writes_access_denied_row():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = f"ev-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    mgr_h = _manager_at(c, f"m{uuid.uuid4().hex[:8]}@x.com", "some-other-venue")

    cid = f"cid-403-{uuid.uuid4().hex[:6]}"
    r = c.get(f"/dashboard/{vid}/overview", headers={**mgr_h, "X-Request-ID": cid})
    assert r.status_code == 403, r.text
    assert r.headers.get("x-correlation-id") == cid

    db = get_db()
    rows = db.list_events(category="security", action_prefix="access.denied")
    assert rows, "expected an access.denied security row"
    row = rows[0]
    assert row["action"] == "access.denied"
    d = row["details"]
    assert d["category"] == "security"
    assert d["outcome"] == "denied"
    assert d["path"] == f"/dashboard/{vid}/overview"
    assert d["method"] == "GET"
    assert d["correlation_id"] == cid
    assert d["ip"]  # client IP captured (testclient)
    assert "user_agent" in d


def test_owner_success_writes_no_security_row():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = f"ev-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    db = get_db()
    before = len(db.list_events(category="security"))
    r = c.get(f"/dashboard/{vid}/overview", headers=owner_h)
    assert r.status_code == 200, r.text
    assert len(db.list_events(category="security")) == before


def test_401_on_protected_route_writes_access_unauthenticated_row():
    c = TestClient(app)
    cid = f"cid-401-{uuid.uuid4().hex[:6]}"
    r = c.get("/venues", headers={"X-Request-ID": cid})
    assert r.status_code == 401
    assert r.headers.get("x-correlation-id") == cid

    db = get_db()
    rows = db.list_events(category="security", action_prefix="access.unauthenticated")
    assert rows
    row = rows[0]
    assert row["action"] == "access.unauthenticated"
    assert row["user_id"] is None
    d = row["details"]
    assert d["category"] == "security" and d["outcome"] == "denied"
    assert d["path"] == "/venues" and d["method"] == "GET"
    assert d["correlation_id"] == cid


def test_401_query_string_is_recorded_but_capped():
    c = TestClient(app)
    r = c.get("/venues", params={"venue_id": "abc", "q": "x" * 500})
    assert r.status_code == 401
    d = get_db().list_events(action_prefix="access.unauthenticated")[0]["details"]
    assert d["query"].startswith("venue_id=abc")
    assert len(d["query"]) <= 200


def test_auth_endpoint_401s_do_not_create_access_rows():
    c = TestClient(app)
    email = f"o{uuid.uuid4().hex[:8]}@x.com"
    _register_login(c, email)
    db = get_db()
    before = len(db.list_events(action_prefix="access."))

    r = c.post("/api/auth/login", json={"email": email, "password": "wrong-password"})
    assert r.status_code == 401
    r = c.post("/api/auth/login", json={"email": "nobody@nowhere.com", "password": "whatever1!"})
    assert r.status_code == 401
    r = c.get("/api/auth/me", headers={"Authorization": "Bearer not-a-token"})
    assert r.status_code == 401

    assert len(db.list_events(action_prefix="access.")) == before


# ---------------------------------------------------------------------------
# 7) global exception handler -> http.unhandled error row
# ---------------------------------------------------------------------------

_BOOM_PATH = "/__events_spine_boom"


def _ensure_boom_route():
    """Throwaway route (test-only) that raises RuntimeError. Not in api.py."""
    if any(getattr(r, "path", None) == _BOOM_PATH for r in app.routes):
        return

    async def _boom():
        raise RuntimeError("kaboom for events spine test")

    app.add_api_route(_BOOM_PATH, _boom, methods=["GET"])


def test_unhandled_exception_writes_http_unhandled_error_row():
    _ensure_boom_route()
    c = TestClient(app, raise_server_exceptions=False)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")

    r = c.get(_BOOM_PATH, headers=owner_h)
    assert r.status_code == 500
    body = r.json()
    assert body["error"]["code"] == "INTERNAL_SERVER_ERROR"
    assert "kaboom" not in r.text  # internals are not leaked

    db = get_db()
    rows = db.list_events(category="error", action_prefix="http.unhandled")
    assert rows, "expected an http.unhandled error row"
    row = rows[0]
    assert row["action"] == "http.unhandled"
    d = row["details"]
    assert d["category"] == "error"
    assert d["outcome"] == "error"
    assert d["exception"] == "RuntimeError"
    assert "kaboom" in d["message"]
    assert d["method"] == "GET"
    assert d["path"] == _BOOM_PATH
    assert isinstance(d["correlation_id"], str) and d["correlation_id"]


def test_unhandled_exception_response_carries_correlation_id_header():
    _ensure_boom_route()
    c = TestClient(app, raise_server_exceptions=False)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    cid = f"cid-500-{uuid.uuid4().hex[:6]}"
    r = c.get(_BOOM_PATH, headers={**owner_h, "X-Request-ID": cid})
    assert r.status_code == 500
    assert r.headers.get("x-correlation-id"), "500 response should carry X-Correlation-ID"


# ---------------------------------------------------------------------------
# 8) PostgresStore._event_row details parsing
# ---------------------------------------------------------------------------

def test_event_row_dict_details_pass_through():
    row = {"id": 1, "action": "a.b", "details": {"category": "audit", "x": 1}}
    out = PostgresStore._event_row(row)
    assert out["details"] == {"category": "audit", "x": 1}
    assert out["id"] == 1 and out["action"] == "a.b"
    assert out is not row  # a copy


def test_event_row_json_string_details_are_parsed():
    row = {"id": 2, "details": json.dumps({"category": "security", "path": "/venues"})}
    out = PostgresStore._event_row(row)
    assert out["details"] == {"category": "security", "path": "/venues"}


def test_event_row_garbage_string_details_become_raw():
    row = {"id": 3, "details": "not {json at all"}
    out = PostgresStore._event_row(row)
    assert out["details"] == {"raw": "not {json at all"}


def test_event_row_none_details_left_alone():
    out = PostgresStore._event_row({"id": 4, "details": None})
    assert out["details"] is None


def test_denied_access_event_carries_the_targeted_venue():
    """A venue manager must be able to see 'someone tried to get into MY
    venue' in their own Activity tab, not just the platform owner."""
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    for v in ("ev-target", "ev-other"):
        c.post("/venues", json={"id": v, "name": v, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=owner_h)
    other_email = f"m{uuid.uuid4().hex[:8]}@x.com"
    other_h = _register_login(c, other_email)
    db = get_db(); rec = db.get_user_by_email(other_email); rec["role"] = "manager"; rec["venue_ids"] = ["ev-other"]; db.save_user(rec)
    other_h = _login_headers(c, other_email) if "_login_headers" in globals() else _register_login(c, other_email)
    r = c.get("/api/snapshot?venue_id=ev-target", headers=other_h)
    assert r.status_code == 403
    rows = db.list_events(venue_id="ev-target", category="security", action_prefix="access.denied")
    assert rows, "the denied attempt must be filed under the venue that was targeted"
    assert rows[0]["details"]["path"] == "/api/snapshot"
    # And the target venue's OWN listing surfaces it via the API
    ev = c.get("/api/events?venue_id=ev-target&category=security", headers=owner_h).json()
    assert any(e["action"] == "access.denied" for e in ev["events"])
