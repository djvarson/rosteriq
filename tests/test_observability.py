"""
The observability spine: every category of log, the aggregation behind the
health view, and the human feedback loop.

What these pin down:
* the four new categories (perf / integration / ai / job) are recordable and
  queryable alongside audit / security / error
* errors carry a FINGERPRINT that groups repeats — the same bug with different
  ids is one row in the digest, not four hundred
* traffic-driven categories are throttled per action per minute, so a storm
  costs a bounded number of rows; audit and security are NEVER dropped
* ``track()`` records success AND failure (a failure rate needs a denominator)
  and never swallows the exception
* ``event_rollup`` groups by action or by a details key, with failure counts
  and p95 — and ``prune_events`` trims by age
* /api/events/insights ranks what to fix and is venue-scoped
* /api/feedback and /api/ai/feedback capture the human signal
"""

import uuid
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services import events


PW = "Passw0rd!234"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _register(c, email):
    r = c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    assert r.status_code in (200, 201), r.text


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    b = r.json()
    tok = b.get("access_token") or b.get("tokens", {}).get("access_token")
    assert tok, r.text
    return {"Authorization": f"Bearer {tok}"}


def _user(c, role, venue_ids, email=None):
    email = email or f"{role}_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)
    return _login(c, email)


def _venue(c, headers, vid):
    r = c.post("/venues", json={
        "id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code in (200, 201), r.text


@pytest.fixture(autouse=True)
def _clear_throttle():
    """The per-minute throttle is module state; a test that fills it must not
    starve the next one."""
    events._throttle_window.clear()
    yield
    events._throttle_window.clear()


@pytest.fixture
def world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    vid = f"obs_{tag}"
    owner = _user(c, "owner", [])
    _venue(c, owner, vid)
    mgr = _user(c, "manager", [vid])
    return dict(c=c, vid=vid, owner=owner, mgr=mgr, tag=tag)


# ---------------------------------------------------------------------------
# categories
# ---------------------------------------------------------------------------

def test_every_category_is_recordable_and_queryable():
    db = get_db()
    events.audit("roster.publish", "v1", "roster", "r1")
    events.security("access.denied", venue_id="v1")
    events.error("http.unhandled", ValueError("boom"), venue_id="v1")
    events.perf("http.slow", 4200, venue_id="v1", route="/x")
    events.integration("xero", "POST Invoices", outcome="ok", duration_ms=120, venue_id="v1")
    events.ai("ai.chat", outcome="ok", venue_id="v1", model="MiniMax-M3")
    events.job("event_retention", outcome="ok", duration_ms=12)

    for category in ("audit", "security", "error", "perf", "integration", "ai"):
        rows = db.list_events(venue_id="v1", category=category, limit=10)
        assert rows, f"no rows recorded for category {category}"
    assert db.list_events(category="job", limit=10)


def test_errors_are_fingerprinted_so_repeats_group():
    db = get_db()
    vid = f"fp_{uuid.uuid4().hex[:6]}"
    for i in range(4):
        events.error("http.unhandled", KeyError(f"Employee emp{i}0092 not found"), venue_id=vid)
    events.error("http.unhandled", ZeroDivisionError("division by zero"), venue_id=vid)

    rows = db.list_events(venue_id=vid, category="error", limit=20)
    prints = {(r["details"] or {}).get("fingerprint") for r in rows}
    # four id-varying KeyErrors collapse to one print; the other bug is its own
    assert len(prints) == 2, prints


def test_fingerprint_normalises_ids_numbers_paths_and_quotes():
    fp = events.fingerprint
    assert fp("KeyError", "Employee abc123 missing") == fp("KeyError", "Employee zz9987 missing")
    assert fp("500", "GET /api/venues/ven-abc/roster") == fp("500", "GET /api/venues/ven-xyz/roster")
    assert fp("ValueError", "bad date 2026-08-17") == fp("ValueError", "bad date 2026-01-02")
    assert fp("KeyError", "missing") != fp("ZeroDivisionError", "missing")
    assert len(fp("x", "y")) == 10


def test_high_volume_categories_are_throttled_but_audit_is_never_dropped():
    db = get_db()
    vid = f"thr_{uuid.uuid4().hex[:6]}"
    for _ in range(60):
        events.perf("http.slow", 3000, venue_id=vid, route="/same")
    recorded = len(db.list_events(venue_id=vid, category="perf", limit=500))
    assert recorded == events.MAX_PER_ACTION_PER_MINUTE, recorded

    # Audit/security are human-scale and must survive a storm untouched.
    for _ in range(40):
        events.audit("roster.publish", vid, "roster", "r1")
    assert len(db.list_events(venue_id=vid, category="audit", limit=500)) == 40


def test_throttle_is_per_action_not_global():
    db = get_db()
    vid = f"thr2_{uuid.uuid4().hex[:6]}"
    for _ in range(30):
        events.perf("http.slow", 3000, venue_id=vid, route="/a")
    for _ in range(5):
        events.perf("db.slow", 3000, venue_id=vid)
    rows = db.list_events(venue_id=vid, category="perf", limit=500)
    actions = [r["action"] for r in rows]
    assert actions.count("db.slow") == 5          # a different action is unaffected


def test_perf_below_threshold_is_not_recorded():
    db = get_db()
    vid = f"pf_{uuid.uuid4().hex[:6]}"
    events.perf("http.slow", 120, venue_id=vid, threshold_ms=2000, route="/fast")
    events.perf("http.slow", 9000, venue_id=vid, threshold_ms=2000, route="/slow")
    rows = db.list_events(venue_id=vid, category="perf", limit=10)
    assert len(rows) == 1
    assert rows[0]["details"]["route"] == "/slow"


# ---------------------------------------------------------------------------
# track()
# ---------------------------------------------------------------------------

def test_track_records_success_and_failure_and_reraises():
    db = get_db()
    vid = f"trk_{uuid.uuid4().hex[:6]}"
    with events.track("xero", "POST Invoices", venue_id=vid) as t:
        t.detail(invoice_id="inv-1")

    with pytest.raises(RuntimeError):
        with events.track("xero", "POST Invoices", venue_id=vid):
            raise RuntimeError("connection reset")

    rows = db.list_events(venue_id=vid, category="integration", limit=10)
    outcomes = sorted((r["details"] or {}).get("outcome") for r in rows)
    assert outcomes == ["failed", "ok"], outcomes
    ok_row = [r for r in rows if r["details"]["outcome"] == "ok"][0]
    assert ok_row["details"]["invoice_id"] == "inv-1"
    fail_row = [r for r in rows if r["details"]["outcome"] == "failed"][0]
    assert fail_row["details"]["exception"] == "RuntimeError"
    assert fail_row["details"]["fingerprint"]


def test_track_marks_a_slow_success_as_slow(monkeypatch):
    db = get_db()
    vid = f"trks_{uuid.uuid4().hex[:6]}"
    ticks = iter([0.0, 5.0])          # 5 seconds elapsed
    monkeypatch.setattr("time.perf_counter", lambda: next(ticks))
    with events.track("myob", "GET Supplier", venue_id=vid, slow_ms=3000):
        pass
    row = db.list_events(venue_id=vid, category="integration", limit=5)[0]
    assert row["details"]["outcome"] == "slow"
    assert row["details"]["duration_ms"] >= 4000


def test_recording_never_breaks_the_caller():
    """A broken store must not take the action down with it."""
    class Exploding:
        def save_audit_log(self, entry):
            raise RuntimeError("db is on fire")

    events.record_event("audit", "roster.publish", venue_id="v", db=Exploding())  # no raise


# ---------------------------------------------------------------------------
# rollup + prune
# ---------------------------------------------------------------------------

def test_event_rollup_groups_counts_failures_and_p95():
    db = get_db()
    vid = f"rl_{uuid.uuid4().hex[:6]}"
    for ms, outcome in ((100, "ok"), (200, "ok"), (900, "failed")):
        events.integration("xero", "POST Invoices", outcome=outcome, duration_ms=ms, venue_id=vid)
    since = datetime.utcnow() - timedelta(hours=1)

    by_provider = db.event_rollup(since, venue_id=vid, category="integration", group_by="provider")
    assert by_provider[0]["key"] == "xero"
    assert by_provider[0]["count"] == 3
    assert by_provider[0]["failures"] == 1
    assert by_provider[0]["p95_ms"] == 900

    by_action = db.event_rollup(since, venue_id=vid, group_by="action")
    assert by_action[0]["key"] == "xero.POST Invoices"


def test_event_rollup_is_venue_scoped():
    db = get_db()
    a, b = f"rlA_{uuid.uuid4().hex[:6]}", f"rlB_{uuid.uuid4().hex[:6]}"
    events.audit("roster.publish", a, "roster", "r1")
    events.audit("roster.publish", b, "roster", "r2")
    events.audit("roster.publish", b, "roster", "r3")
    since = datetime.utcnow() - timedelta(hours=1)
    assert db.event_rollup(since, venue_id=a, group_by="action")[0]["count"] == 1
    assert db.event_rollup(since, venue_id=b, group_by="action")[0]["count"] == 2


def test_prune_events_removes_only_the_old():
    db = get_db()
    vid = f"pr_{uuid.uuid4().hex[:6]}"
    events.audit("roster.publish", vid, "roster", "new")
    # age one row by hand
    rows = db.list_events(venue_id=vid, limit=5)
    assert rows
    old = dict(rows[0])
    old["created_at"] = datetime.utcnow() - timedelta(days=200)
    old["action"] = "roster.publish.old"
    db.save_audit_log(old)

    removed = db.prune_events(datetime.utcnow() - timedelta(days=90))
    assert removed >= 1
    left = {r["action"] for r in db.list_events(venue_id=vid, limit=10)}
    assert "roster.publish" in left and "roster.publish.old" not in left


# ---------------------------------------------------------------------------
# insights API
# ---------------------------------------------------------------------------

def test_insights_ranks_what_to_fix(world):
    c, vid = world["c"], world["vid"]
    for i in range(4):
        events.error("http.unhandled", KeyError(f"Employee e{i}00777 not found"), venue_id=vid)
    for i in range(5):
        events.integration("xero", "POST Invoices", outcome="failed" if i < 4 else "ok",
                           duration_ms=100, venue_id=vid)
    events.perf("http.slow", 8000, venue_id=vid, route="/api/rosters/generate")
    events.job("weekly_digest", outcome="failed", duration_ms=10, venue_id=vid)

    r = c.get(f"/api/events/insights?venue_id={vid}&days=7", headers=world["mgr"])
    assert r.status_code == 200, r.text
    d = r.json()

    kinds = {a["kind"] for a in d["attention"]}
    assert {"error", "integration", "perf"} <= kinds, kinds
    assert d["errors"][0]["count"] == 4
    assert d["errors"][0]["exception"] == "KeyError"
    xero = [i for i in d["integrations"] if i["provider"] == "xero"][0]
    assert xero["failures"] == 4 and xero["failure_pct"] == 80.0
    assert d["slow_routes"][0]["route"] == "/api/rosters/generate"


def test_insights_is_venue_scoped(world):
    c = world["c"]
    other = f"obsB_{world['tag']}"
    _venue(c, world["owner"], other)
    events.error("http.unhandled", ValueError("secret to venue B"), venue_id=other)

    r = c.get(f"/api/events/insights?venue_id={other}", headers=world["mgr"])
    assert r.status_code == 403, r.text
    # the manager's own venue shows nothing of B's
    mine = c.get(f"/api/events/insights?venue_id={world['vid']}", headers=world["mgr"]).json()
    assert all("secret to venue B" != (e.get("message") or "") for e in mine["errors"])
    # owner sees platform-wide
    assert c.get("/api/events/insights", headers=world["owner"]).status_code == 200
    assert c.get("/api/events/insights", headers=world["mgr"]).status_code == 403


def test_insights_survives_a_broken_rollup(world, monkeypatch):
    """A health view that 500s when one query fails is worse than useless."""
    c, vid = world["c"], world["vid"]
    db = get_db()
    monkeypatch.setattr(type(db), "event_rollup",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bad sql")))
    r = c.get(f"/api/events/insights?venue_id={vid}", headers=world["mgr"])
    assert r.status_code == 200, r.text
    assert r.json()["errors"] == []


# ---------------------------------------------------------------------------
# feedback: the human signal
# ---------------------------------------------------------------------------

def test_feedback_is_recorded_with_page_and_reference(world):
    c, vid = world["c"], world["vid"]
    r = c.post("/api/feedback", json={
        "kind": "bug", "message": "Publish button spins forever",
        "venue_id": vid, "page": "roster", "reference": "abc123",
    }, headers=world["mgr"])
    assert r.status_code == 200, r.text

    rows = get_db().list_events(venue_id=vid, action_prefix="feedback.", limit=5)
    assert rows and rows[0]["action"] == "feedback.bug"
    d = rows[0]["details"]
    assert d["page"] == "roster" and d["reference"] == "abc123"
    assert d["fingerprint"]

    listed = c.get(f"/api/feedback?venue_id={vid}", headers=world["mgr"]).json()
    assert listed["count"] == 1
    assert listed["items"][0]["message"].startswith("Publish button")


def test_feedback_for_a_foreign_venue_is_kept_but_unattributed(world):
    """Never lose a report over a metadata detail — but never let it label
    another tenant's venue either."""
    c = world["c"]
    other = f"obsC_{world['tag']}"
    _venue(c, world["owner"], other)
    r = c.post("/api/feedback", json={"kind": "bug", "message": "x marks the spot",
                                      "venue_id": other}, headers=world["mgr"])
    assert r.status_code == 200
    assert not [x for x in get_db().list_events(venue_id=other, action_prefix="feedback.", limit=5)]


def test_ai_thumbs_feed_the_satisfaction_rate(world):
    c, vid = world["c"], world["vid"]
    for helpful in (True, True, False):
        r = c.post("/api/ai/feedback", json={
            "helpful": helpful, "venue_id": vid,
            "question": "how many staff tonight?", "answer_preview": "six",
        }, headers=world["mgr"])
        assert r.status_code == 200, r.text

    d = c.get(f"/api/events/insights?venue_id={vid}", headers=world["mgr"]).json()
    assert d["ai"]["thumbs_up"] == 2
    assert d["ai"]["thumbs_down"] == 1
    assert d["ai"]["satisfaction_pct"] == 66.7


def test_staff_cannot_read_the_feedback_inbox(world):
    c, vid = world["c"], world["vid"]
    staff = _user(c, "staff", [vid])
    # they may SEND
    assert c.post("/api/feedback", json={"kind": "confusing", "message": "where is my roster",
                                         "venue_id": vid}, headers=staff).status_code == 200
    # but not READ everyone else's
    assert c.get(f"/api/feedback?venue_id={vid}", headers=staff).status_code == 403
    assert c.get("/api/feedback", headers=staff).status_code == 403


# ---------------------------------------------------------------------------
# WebSocket live feed: mounted, and venue-scoped on the upgrade
#
# setup_websocket(app) was never called, so every live-update feature was dead
# and the dashboard reconnect-looped forever. Mounting it needed a tenancy
# check first: a socket upgrade does NOT pass through TenantMiddleware, so
# without one, any valid token could stream any venue's operations.
# ---------------------------------------------------------------------------

def test_websocket_routes_are_actually_mounted(world):
    """Proven by behaviour, not by introspection: this app wraps routers in an
    _IncludedRouter, so a mounted websocket route does not appear in
    app.routes. A connection that reaches the handler is the real proof."""
    c, vid = world["c"], world["vid"]
    tok = world["mgr"]["Authorization"].split()[1]
    with c.websocket_connect(f"/ws/ws/{vid}?token={tok}") as ws:
        assert ws.receive_json()["type"] == "initial_state"


def test_websocket_accepts_own_venue_and_refuses_everything_else(world):
    c, vid = world["c"], world["vid"]
    other = f"wsB_{world['tag']}"
    _venue(c, world["owner"], other)
    tok = world["mgr"]["Authorization"].split()[1]

    with c.websocket_connect(f"/ws/ws/{vid}?token={tok}") as ws:
        first = ws.receive_json()
        assert first["type"] == "initial_state"
        assert first["venue_id"] == vid

    from starlette.websockets import WebSocketDisconnect
    for url, why in (
        (f"/ws/ws/{other}?token={tok}", "another tenant's venue"),
        (f"/ws/ws/{vid}", "no token at all"),
        (f"/ws/ws/{vid}?token=not-a-jwt", "an undecodable token"),
    ):
        with pytest.raises((WebSocketDisconnect, Exception)) as ei:
            with c.websocket_connect(url) as ws:
                ws.receive_json()
        assert ei.value is not None, why


def test_websocket_refusal_is_recorded_as_a_security_event(world):
    c, vid = world["c"], world["vid"]
    other = f"wsC_{world['tag']}"
    _venue(c, world["owner"], other)
    tok = world["mgr"]["Authorization"].split()[1]
    try:
        with c.websocket_connect(f"/ws/ws/{other}?token={tok}") as ws:
            ws.receive_json()
    except Exception:
        pass
    rows = get_db().list_events(venue_id=other, category="security", limit=10)
    assert any((r["details"] or {}).get("transport") == "websocket" for r in rows), rows
