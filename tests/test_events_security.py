"""
Security + identity events on the event-log spine (services/events.py).

Covers the auth surface (routes/auth.py): auth.login_failed / auth.locked /
auth.register / auth.login / demo.session; the api.py request middleware's
access.denied (403, with the actor's user_id via request.state) and
access.unauthenticated (401) rows; that a failing audit-log write can never
turn a 403/401 into a 500; and the audit rows for privacy.export /
privacy.anonymise (routes/privacy.py) and user.venue_grant /
user.role_change (first-venue bootstrap in api.py).

All against MemoryStore (conftest resets it per test), so the first registered
user in each test bootstraps as owner.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.demo import DEMO_USER_ID, DEMO_STAFF_USER_ID

PASSWORD = "Passw0rd!234"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _email(prefix="u"):
    return f"{prefix}{uuid.uuid4().hex[:8]}@x.com"


def _register(c, email, name="U"):
    r = c.post("/api/auth/register", json={"email": email, "password": PASSWORD, "name": name})
    assert r.status_code == 201, r.text
    return r.json()


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _register_login(c, email):
    _register(c, email)
    return _login(c, email)


def _venue(c, owner_h, vid):
    r = c.post("/venues", json={
        "id": vid, "name": "Events Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code in (200, 201), r.text


def _employee(c, headers, eid, venue_id, name="Emp Test"):
    r = c.post("/employees", json={
        "id": eid, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": venue_id,
        "hourly_base_rate": "30.00", "email": f"{eid}@x.com", "skills": ["bar"],
        "created_at": "2026-06-20T00:00:00", "updated_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code in (200, 201), r.text


def _manager_at(c, email, vid):
    """Register a user and make them a manager of ``vid`` only (store-level)."""
    h = _register_login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    return h, rec["id"]


def _no_secrets(details: dict):
    """No password / token shaped values may ever land in a row."""
    flat = str(details).lower()
    assert PASSWORD.lower() not in flat
    assert "wrong-password" not in flat
    for k in details:
        if any(h in k.lower() for h in ("password", "token", "secret")):
            assert details[k] == "[redacted]"


# ---------------------------------------------------------------------------
# 1) auth.login_failed
# ---------------------------------------------------------------------------

def test_login_failed_bad_password_writes_security_row_with_email_and_user():
    c = TestClient(app)
    email = _email("o")
    user = _register(c, email)["user"]
    db = get_db()

    cid = f"cid-lf-{uuid.uuid4().hex[:6]}"
    r = c.post("/api/auth/login", json={"email": email, "password": "wrong-password"},
               headers={"X-Request-ID": cid})
    assert r.status_code == 401

    rows = db.list_events(category="security", action_prefix="auth.login_failed")
    assert rows, "expected an auth.login_failed security row"
    row = rows[0]
    assert row["action"] == "auth.login_failed"
    assert row["user_id"] == user["id"]          # the identity under attack
    d = row["details"]
    assert d["category"] == "security"
    assert d["outcome"] == "failed"
    assert d["email"] == email
    assert d["reason"] == "bad_password"
    assert d["correlation_id"] == cid
    assert d["ip"]                                # client ip captured
    _no_secrets(d)
    # A failed login must NOT double up as an access.* row (auth path exempt).
    assert db.list_events(action_prefix="access.") == []


def test_login_failed_unknown_user_has_no_user_id_but_keeps_email():
    c = TestClient(app)
    ghost = _email("ghost")
    r = c.post("/api/auth/login", json={"email": ghost, "password": "whatever1!"})
    assert r.status_code == 401
    row = get_db().list_events(action_prefix="auth.login_failed")[0]
    assert row["user_id"] is None
    assert row["details"]["email"] == ghost
    assert row["details"]["reason"] == "unknown_user"
    _no_secrets(row["details"])


# ---------------------------------------------------------------------------
# 2) auth.locked — the 5th failure in a minute from one IP trips the lockout
# ---------------------------------------------------------------------------

def test_lockout_writes_auth_locked_once_then_denied_attempts():
    # A dedicated client IP so the per-IP counter can't leak into other tests.
    c = TestClient(app, client=(f"10.99.{uuid.uuid4().int % 250}.7", 4321))
    email = _email("lock")
    _register(c, email)
    db = get_db()

    for i in range(5):
        r = c.post("/api/auth/login", json={"email": email, "password": "wrong-password"})
        assert r.status_code == 401, (i, r.text)

    locked = db.list_events(category="security", action_prefix="auth.locked")
    assert len(locked) == 1, "lockout trigger must be recorded exactly once"
    d = locked[0]["details"]
    assert d["outcome"] == "denied"
    assert d["email"] == email
    assert d["max_attempts"] == 5 and d["window_minutes"] == 1
    _no_secrets(d)

    # 6th attempt (even with the RIGHT password) is refused while locked out and
    # is itself recorded as a denied login attempt — but no second auth.locked.
    r = c.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 429
    failed = db.list_events(category="security", action_prefix="auth.login_failed")
    assert failed[0]["details"]["reason"] == "locked"
    assert failed[0]["details"]["outcome"] == "denied"
    assert len(db.list_events(action_prefix="auth.locked")) == 1
    # 5 real failures + 1 while-locked attempt
    assert len(failed) == 6
    # /api/auth/* is exempt from the middleware's rate.limited row (no double count)
    assert db.list_events(action_prefix="rate.limited") == []


# ---------------------------------------------------------------------------
# 3) auth.register / auth.login (audit, outcome ok)
# ---------------------------------------------------------------------------

def test_register_and_login_write_audit_rows_with_identity():
    c = TestClient(app)
    email = _email("reg")
    user = _register(c, email, name="Reg Person")["user"]
    db = get_db()

    reg = db.list_events(category="audit", action_prefix="auth.register")
    assert reg and reg[0]["action"] == "auth.register"
    assert reg[0]["user_id"] == user["id"]
    assert reg[0]["resource_type"] == "user" and reg[0]["resource_id"] == user["id"]
    d = reg[0]["details"]
    assert d["category"] == "audit" and d["outcome"] == "ok"
    assert d["email"] == email and d["name"] == "Reg Person"
    assert d["role"] == "owner" and d["bootstrap_owner"] is True   # first user in a fresh store
    _no_secrets(d)

    _login(c, email)
    login = db.list_events(category="audit", action_prefix="auth.login")
    # newest first; auth.login_failed does not exist here so prefix is safe
    assert login[0]["action"] == "auth.login"
    assert login[0]["user_id"] == user["id"]
    assert login[0]["details"]["outcome"] == "ok"
    assert login[0]["details"]["email"] == email
    assert login[0]["details"]["role"] == "owner"
    _no_secrets(login[0]["details"])
    # low noise: exactly one row per successful login
    assert len([r for r in login if r["action"] == "auth.login"]) == 1


# ---------------------------------------------------------------------------
# 4) demo.session — which identity was minted
# ---------------------------------------------------------------------------

def test_demo_session_records_which_identity_was_minted():
    c = TestClient(app)
    db = get_db()

    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    rows = db.list_events(category="audit", action_prefix="demo.session")
    assert rows and rows[0]["user_id"] == DEMO_USER_ID
    assert rows[0]["details"]["identity"] == "manager"
    assert rows[0]["details"]["outcome"] == "ok"

    r = c.post("/api/auth/demo?as=staff")
    assert r.status_code == 200, r.text
    rows = db.list_events(category="audit", action_prefix="demo.session")
    assert rows[0]["user_id"] == DEMO_STAFF_USER_ID
    assert rows[0]["details"]["identity"] == "staff"
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# 5) request middleware: 403 (cross-venue) carries path + user_id; 401 too
# ---------------------------------------------------------------------------

def test_403_cross_venue_writes_access_denied_with_path_and_user_id():
    c = TestClient(app)
    owner_h = _register_login(c, _email("o"))
    vid = f"ev-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    mgr_h, mgr_id = _manager_at(c, _email("m"), "some-other-venue")

    cid = f"cid-403-{uuid.uuid4().hex[:6]}"
    r = c.get(f"/dashboard/{vid}/overview", headers={**mgr_h, "X-Request-ID": cid})
    assert r.status_code == 403, r.text

    rows = get_db().list_events(category="security", action_prefix="access.denied")
    assert rows, "expected an access.denied security row"
    row = rows[0]
    assert row["action"] == "access.denied"
    assert row["user_id"] == mgr_id, "the denied actor must be identifiable"
    d = row["details"]
    assert d["category"] == "security" and d["outcome"] == "denied"
    assert d["path"] == f"/dashboard/{vid}/overview"
    assert d["method"] == "GET"
    assert d["role"] == "manager"
    assert d["correlation_id"] == cid
    assert d["ip"]


def test_401_unauthenticated_writes_access_unauthenticated_with_path():
    c = TestClient(app)
    r = c.get("/venues")
    assert r.status_code == 401
    rows = get_db().list_events(category="security", action_prefix="access.unauthenticated")
    assert rows
    row = rows[0]
    assert row["user_id"] is None
    assert row["details"]["path"] == "/venues"
    assert row["details"]["method"] == "GET"
    assert row["details"]["outcome"] == "denied"

    # A garbage bearer token is still a 401 -> also recorded, still no actor
    r = c.get("/venues", headers={"Authorization": "Bearer not-a-token"})
    assert r.status_code == 401
    rows = get_db().list_events(action_prefix="access.unauthenticated")
    assert len(rows) == 2 and rows[0]["user_id"] is None


# ---------------------------------------------------------------------------
# 6) recording can never break the request
# ---------------------------------------------------------------------------

def test_middleware_security_write_failure_still_returns_403_and_401(monkeypatch):
    c = TestClient(app)
    owner_h = _register_login(c, _email("o"))
    vid = f"ev-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    mgr_h, _ = _manager_at(c, _email("m"), "some-other-venue")

    db = get_db()
    calls = {"n": 0}

    def _boom(entry):
        calls["n"] += 1
        raise RuntimeError("audit_logs table is on fire")

    monkeypatch.setattr(db, "save_audit_log", _boom)

    r = c.get(f"/dashboard/{vid}/overview", headers=mgr_h)
    assert r.status_code == 403, r.text          # not 500
    r = c.get("/venues")
    assert r.status_code == 401                  # not 500
    assert calls["n"] >= 2, "the middleware did try to record both events"


def test_login_failure_still_401_when_event_store_raises(monkeypatch):
    c = TestClient(app)
    email = _email("o")
    _register(c, email)
    db = get_db()
    monkeypatch.setattr(db, "save_audit_log",
                        lambda entry: (_ for _ in ()).throw(RuntimeError("down")))
    r = c.post("/api/auth/login", json={"email": email, "password": "wrong-password"})
    assert r.status_code == 401                  # not 500
    # and a good login still works while the event store is down
    r = c.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# 7) privacy.export / privacy.anonymise (audit)
# ---------------------------------------------------------------------------

def test_privacy_export_and_anonymise_write_audit_rows():
    c = TestClient(app)
    owner_email = _email("o")
    owner_h = _register_login(c, owner_email)
    owner_id = get_db().get_user_by_email(owner_email)["id"]
    vid = f"pv-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    eid = f"emp-{uuid.uuid4().hex[:6]}"
    _employee(c, owner_h, eid, vid, name="Priya Vale")
    db = get_db()

    # employee export
    r = c.post(f"/api/privacy/export/employee/{eid}", headers=owner_h)
    assert r.status_code == 200, r.text
    rows = db.list_events(venue_id=vid, action_prefix="privacy.export")
    assert rows and rows[0]["action"] == "privacy.export"
    assert rows[0]["user_id"] == owner_id
    assert rows[0]["resource_type"] == "employee" and rows[0]["resource_id"] == eid
    d = rows[0]["details"]
    assert d["category"] == "audit" and d["outcome"] == "ok"
    assert d["subject"] == "employee" and d["employee_name"] == "Priya Vale"
    assert d["role"] == "owner"

    # self export (no venue)
    r = c.post("/api/privacy/export/me", headers=owner_h)
    assert r.status_code == 200, r.text
    me_rows = [x for x in db.list_events(action_prefix="privacy.export")
               if x["details"].get("subject") == "user"]
    assert me_rows and me_rows[0]["resource_id"] == owner_id
    assert me_rows[0]["details"]["self_service"] is True

    # anonymise (owner only) — irreversible, must be on the record
    r = c.post(f"/api/privacy/anonymise/employee/{eid}", headers=owner_h)
    assert r.status_code == 200, r.text
    rows = db.list_events(venue_id=vid, action_prefix="privacy.anonymise")
    assert rows and rows[0]["action"] == "privacy.anonymise"
    assert rows[0]["user_id"] == owner_id
    assert rows[0]["resource_id"] == eid
    d = rows[0]["details"]
    assert d["employee_name_before"] == "Priya Vale"
    assert "name" in d["anonymised_fields"] and "email" in d["anonymised_fields"]
    assert d["outcome"] == "ok"


def test_privacy_denied_paths_write_no_privacy_audit_rows():
    """A refused export/anonymise (staff role, cross-venue 404) must not
    pretend the data left: no privacy.* audit row, only the access row."""
    c = TestClient(app)
    owner_h = _register_login(c, _email("o"))
    vid = f"pv-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    eid = f"emp-{uuid.uuid4().hex[:6]}"
    _employee(c, owner_h, eid, vid)
    mgr_h, _ = _manager_at(c, _email("m"), "some-other-venue")
    db = get_db()

    r = c.post(f"/api/privacy/export/employee/{eid}", headers=mgr_h)
    assert r.status_code == 404, r.text          # cross-venue is not an oracle
    r = c.post(f"/api/privacy/anonymise/employee/{eid}", headers=mgr_h)
    assert r.status_code == 404, r.text
    assert db.list_events(action_prefix="privacy.") == []


# ---------------------------------------------------------------------------
# 8) user.venue_grant / user.role_change — first-venue self-serve bootstrap
# ---------------------------------------------------------------------------

def test_first_venue_bootstrap_writes_venue_grant_and_role_change():
    c = TestClient(app)
    _register_login(c, _email("o"))                       # owner (bootstrap)
    staff_email = _email("s")
    staff_h = _register_login(c, staff_email)             # plain staff, no venues
    db = get_db()
    staff_id = db.get_user_by_email(staff_email)["id"]
    assert db.get_user_by_email(staff_email)["role"] == "staff"

    vid = f"boot-{uuid.uuid4().hex[:6]}"
    _venue(c, staff_h, vid)                               # brand-new id -> bootstrap

    grants = db.list_events(venue_id=vid, action_prefix="user.venue_grant")
    assert grants and grants[0]["user_id"] == staff_id
    assert grants[0]["resource_type"] == "user" and grants[0]["resource_id"] == staff_id
    assert vid in grants[0]["details"]["venue_ids"]
    assert grants[0]["details"]["email"] == staff_email
    assert grants[0]["details"]["reason"] == "first_venue_bootstrap"

    roles = db.list_events(venue_id=vid, action_prefix="user.role_change")
    assert roles and roles[0]["details"]["old_role"] == "staff"
    assert roles[0]["details"]["new_role"] == "manager"
    assert roles[0]["user_id"] == staff_id
    assert roles[0]["details"]["category"] == "audit"

    # A second venue created by the (now) manager: grant again, but no role change.
    vid2 = f"boot-{uuid.uuid4().hex[:6]}"
    _venue(c, staff_h, vid2)
    assert db.list_events(venue_id=vid2, action_prefix="user.venue_grant")
    assert db.list_events(venue_id=vid2, action_prefix="user.role_change") == []


def test_staff_email_auto_link_writes_venue_grant():
    """A staff user whose email matches an employee gets the venue granted on
    first portal use — that implicit grant is on the record."""
    c = TestClient(app)
    owner_h = _register_login(c, _email("o"))
    vid = f"al-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    eid = f"emp-{uuid.uuid4().hex[:6]}"
    staff_email = f"{eid}@x.com"                       # _employee uses "<eid>@x.com"
    _employee(c, owner_h, eid, vid, name="Auto Link")
    staff_h = _register_login(c, staff_email)          # role staff, no venues yet
    db = get_db()
    staff_id = db.get_user_by_email(staff_email)["id"]

    r = c.get("/api/me/profile", headers=staff_h)
    assert r.status_code == 200, r.text
    assert vid in db.get_user_by_id(staff_id)["venue_ids"]

    rows = db.list_events(venue_id=vid, action_prefix="user.venue_grant")
    assert rows and rows[0]["resource_id"] == staff_id
    d = rows[0]["details"]
    assert d["reason"] == "staff_email_auto_link"
    assert d["employee_id"] == eid and d["email"] == staff_email
    assert d["category"] == "audit"


# ---------------------------------------------------------------------------
# 9) the vocabulary is stable and lands in the events API for an owner
# ---------------------------------------------------------------------------

def test_security_rows_visible_platform_wide_to_owner_via_events_api():
    c = TestClient(app)
    owner_h = _register_login(c, _email("o"))
    c.post("/api/auth/login", json={"email": _email("nobody"), "password": "nope-nope1!"})
    c.get("/venues")                                       # 401
    r = c.get("/api/events?category=security", headers=owner_h)
    assert r.status_code == 200, r.text
    body = r.json()
    items = body if isinstance(body, list) else (body.get("items") or body.get("events") or [])
    actions = {i["action"] for i in items}
    assert {"auth.login_failed", "access.unauthenticated"} <= actions
