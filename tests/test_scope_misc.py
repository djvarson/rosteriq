"""
Cross-venue pins for the "misc" leaks:

* AutoScheduler.generate_week / preview_week drew on EVERY tenant's staff, so
  venue A's auto-schedule rostered (and saved) venue B's employees and A's
  schedule preview counted B's head-count.
* GET /api/ai/insights had no venue gate: any login read any venue's labour
  cost and staffing cards.
* GET /api/me/profile told an unlinked login the NAME of whichever venue had
  its (unverified) email on a staff record.
* GET /api/status and GET /metrics published platform-wide venue / staff /
  roster counts to anonymous callers.
* The daily-digest job gathered every roster's shifts into each venue's email.

Two venues, two non-owner managers; the first registered user is the owner.
"""

import asyncio
import uuid
from datetime import date, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

PW = "Passw0rd!234"


def _register(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _employee(c, h, vid, eid, name, etype="part_time", email=None):
    body = {
        "id": eid, "venue_id": vid, "name": name, "employment_type": etype,
        "award_level": "level_2", "state": "vic", "hourly_base_rate": "40.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }
    if email:
        body["email"] = email
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text


def _world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:8]
    owner_email = f"sm_boot_{tag}@x.com"
    _register(c, owner_email)                       # first user -> platform owner
    a_email, b_email = f"sm_a_{tag}@x.com", f"sm_b_{tag}@x.com"
    va, vb = f"sm-a-{tag}", f"sm-b-{tag}"
    for email, vid, name in ((a_email, va, f"VenueA-{tag}"), (b_email, vb, f"SecretVenueB-{tag}")):
        _register(c, email)
        r = c.post("/venues", json={"id": vid, "name": name, "state": "vic", "max_labour_pct": 30,
                                    "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                   headers=_login(c, email))
        assert r.status_code == 200, r.text
    ha, hb = _login(c, a_email), _login(c, b_email)  # fresh tokens carry the venue
    db = get_db()
    for email, vid in ((a_email, va), (b_email, vb)):
        u = db.get_user_by_email(email)
        assert u["role"] == "manager" and not u.get("is_owner") and u["venue_ids"] == [vid], u
    assert db.get_user_by_email(owner_email)["role"] == "owner"
    _employee(c, ha, va, f"a1-{tag}", f"Alice-{tag}")
    _employee(c, ha, va, f"a2-{tag}", f"Adam-{tag}", etype="full_time")
    _employee(c, hb, vb, f"b1-{tag}", f"Bob-{tag}", etype="full_time")
    return SimpleNamespace(c=c, tag=tag, ha=ha, hb=hb, va=va, vb=vb,
                           owner=_login(c, owner_email), a_email=a_email,
                           vb_name=f"SecretVenueB-{tag}")


def _demo(c):
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _staff_of(c, vid):
    """A staff-role login that holds ``vid``."""
    email = f"sm_staff_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"], rec["venue_ids"] = "staff", [vid]
    db.save_user(rec)
    return _login(c, email)


def _next_monday():
    d = date.today() + timedelta(days=7)
    return (d - timedelta(days=d.weekday())).isoformat()


# ---------------------------------------------------------------------------
# AutoScheduler
# ---------------------------------------------------------------------------

def test_auto_schedule_only_rosters_own_venue_staff():
    w = _world()
    c = w.c
    r = c.post(f"/api/v1/venues/{w.va}/auto-schedule", json={"week_start": _next_monday()},
               headers=w.ha)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_shifts"] >= 1 and 1 <= body["employees_used"] <= 2
    saved = c.get(f"/rosters/{body['roster_id']}", headers=w.ha)
    assert saved.status_code == 200, saved.text
    used = {s["employee_id"] for s in saved.json()["shifts"]}
    assert used <= {f"a1-{w.tag}", f"a2-{w.tag}"}, used
    # A cannot drive B's scheduler; neither can the public demo
    assert c.post(f"/api/v1/venues/{w.vb}/auto-schedule", json={"week_start": _next_monday()},
                  headers=w.ha).status_code == 403
    assert c.post(f"/api/v1/venues/{w.va}/auto-schedule", json={"week_start": _next_monday()},
                  headers=_demo(c)).status_code == 403


def test_auto_schedule_does_not_borrow_another_venues_staff():
    """A venue with no staff of its own gets no roster built from B's staff."""
    w = _world()
    c = w.c
    empty_email = f"sm_e_{w.tag}@x.com"
    ve = f"sm-e-{w.tag}"
    _register(c, empty_email)
    assert c.post("/venues", json={"id": ve, "name": ve, "state": "vic", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=_login(c, empty_email)).status_code == 200
    he = _login(c, empty_email)
    r = c.post(f"/api/v1/venues/{ve}/auto-schedule", json={"week_start": _next_monday()},
               headers=he)
    assert r.status_code == 400, r.text           # "No employees found"
    assert not [x for x in get_db().list_rosters() if x.venue_id == ve]


def test_schedule_preview_counts_only_own_staff():
    w = _world()
    c = w.c
    ws = _next_monday()
    s1 = c.get(f"/api/v1/venues/{w.va}/schedule-preview", params={"week_start": ws},
               headers=w.ha)
    assert s1.status_code == 200, s1.text
    assert s1.json()["available_staff"] == {"full_time": 1, "part_time": 1, "casual": 0, "total": 2}
    for i in range(3):
        _employee(c, w.hb, w.vb, f"bc{i}-{w.tag}", f"Bc{i}", etype="casual")
    s2 = c.get(f"/api/v1/venues/{w.va}/schedule-preview", params={"week_start": ws},
               headers=w.ha).json()["available_staff"]
    assert s2 == s1.json()["available_staff"]     # B's hiring is invisible to A
    assert c.get(f"/api/v1/venues/{w.vb}/schedule-preview", params={"week_start": ws},
                 headers=w.ha).status_code == 403
    assert c.get(f"/api/v1/venues/{w.vb}/schedule-preview", params={"week_start": ws},
                 headers=_demo(c)).status_code == 403


def test_hiring_suggestions_scoped():
    w = _world()
    c = w.c
    ws = _next_monday()
    r = c.get(f"/api/v1/venues/{w.va}/hiring-suggestions", params={"week_start": ws},
              headers=w.ha)
    assert r.status_code == 200, r.text
    assert r.json()["venue_id"] == w.va
    assert c.get(f"/api/v1/venues/{w.vb}/hiring-suggestions", params={"week_start": ws},
                 headers=w.ha).status_code == 403


# ---------------------------------------------------------------------------
# AI insights
# ---------------------------------------------------------------------------

def test_ai_insights_refuse_other_venues_and_non_managers():
    w = _world()
    c = w.c
    today = date.today()
    ws = (today - timedelta(days=today.weekday())).isoformat()
    assert c.post("/rosters/generate", json={"venue_id": w.vb, "week_start": ws},
                  headers=w.hb).status_code == 200
    q = {"venue_id": w.vb, "max_count": 10}
    assert c.get("/api/ai/insights", params=q, headers=w.ha).status_code == 403
    assert c.get("/api/ai/insights", params=q, headers=_demo(c)).status_code == 403
    # cards carry labour cost -> a staff member of B is refused too
    assert c.get("/api/ai/insights", params=q, headers=_staff_of(c, w.vb)).status_code == 403
    # B's own manager and A on A still work
    own_b = c.get("/api/ai/insights", params=q, headers=w.hb)
    assert own_b.status_code == 200 and own_b.json()["venue_id"] == w.vb
    own_a = c.get("/api/ai/insights", params={"venue_id": w.va}, headers=w.ha)
    assert own_a.status_code == 200 and own_a.json()["venue_id"] == w.va


# ---------------------------------------------------------------------------
# /api/me/* for an unlinked login
# ---------------------------------------------------------------------------

def test_unlinked_profile_is_constant_whether_or_not_email_is_on_a_record():
    w = _world()
    c = w.c
    victim = f"carol_{w.tag}@venueb.example"
    _employee(c, w.hb, w.vb, f"bv-{w.tag}", f"Carol-{w.tag}", etype="casual", email=victim)
    control = f"nobody_{w.tag}@x.com"
    _register(c, victim)
    _register(c, control)
    hv, hc = _login(c, victim), _login(c, control)

    def norm(resp, email):
        assert resp.status_code == 200, resp.text
        body = resp.json()
        body["message"] = body["message"].replace(email, "<email>")
        return body

    for path in ("/api/me/profile", "/api/me/shifts", "/api/me/timesheets", "/api/me/leave"):
        pv = norm(c.get(path, headers=hv), victim)
        pc = norm(c.get(path, headers=hc), control)
        assert pv == pc, (path, pv, pc)
        assert pv["linked"] is False
        assert w.vb_name not in pv["message"] and w.vb not in pv["message"]
    assert get_db().get_user_by_email(victim).get("venue_ids") in (None, [])


def test_linked_profile_still_resolves_own_venue():
    w = _world()
    c = w.c
    email = f"sm_linked_{w.tag}@x.com"
    _employee(c, w.ha, w.va, f"al-{w.tag}", f"Linda-{w.tag}", email=email)
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"], rec["venue_ids"] = "staff", [w.va]
    db.save_user(rec)
    prof = c.get("/api/me/profile", headers=_login(c, email)).json()
    assert prof["linked"] is True and prof["venue_id"] == w.va
    assert prof["employee_id"] == f"al-{w.tag}"


# ---------------------------------------------------------------------------
# /api/status and /metrics
# ---------------------------------------------------------------------------

def test_status_publishes_no_tenant_counts():
    w = _world()
    c = w.c
    before = c.get("/api/status")
    assert before.status_code == 200, before.text
    body = before.json()
    assert body["status"] == "ok" and "roster_optimiser" in body["modules"]
    assert "venues_loaded" not in body and "employees_loaded" not in body
    for i in range(3):
        _employee(c, w.hb, w.vb, f"bs{i}-{w.tag}", f"Bs{i}")
    assert c.get("/api/status", headers=w.ha).json() == body


def test_metrics_is_owner_only():
    w = _world()
    c = w.c
    assert c.get("/metrics").status_code == 401
    assert c.get("/metrics", headers=w.ha).status_code == 403
    assert c.get("/metrics", headers=_demo(c)).status_code == 403
    r = c.get("/metrics", headers=w.owner)
    assert r.status_code == 200, r.text
    assert r.json()["data"]["venues"] >= 2
    # health probes stay public for the platform healthcheck
    assert c.get("/health").status_code == 200
    assert c.get("/ready").status_code in (200, 503)
    assert c.get("/ready").status_code != 401


# ---------------------------------------------------------------------------
# Daily digest job
# ---------------------------------------------------------------------------

def test_daily_digest_only_carries_the_venues_own_shifts():
    from rosteriq.services.task_scheduler import AsyncTaskScheduler, JobConfig, JobType

    today = date.today()
    shift = lambda sid: SimpleNamespace(id=sid, date=today)
    venues = [SimpleNamespace(id="dg-a", name="A", manager_email="a@x.com"),
              SimpleNamespace(id="dg-b", name="B", manager_email="b@x.com")]
    rosters = [SimpleNamespace(venue_id="dg-a", shifts=[shift("sa")]),
               SimpleNamespace(venue_id="dg-b", shifts=[shift("sb1"), shift("sb2")])]
    sent = {}

    class _Notify:
        async def send_daily_digest(self, venue_id, roster_shifts, **_):
            sent[venue_id] = sorted(s.id for s in roster_shifts)

    sched = AsyncTaskScheduler()
    sched._db = SimpleNamespace(list_venues=lambda: venues, list_rosters=lambda: rosters)
    sched._notification_service = _Notify()
    asyncio.run(sched._handle_daily_digest(JobConfig(job_type=JobType.DAILY_DIGEST)))
    assert sent == {"dg-a": ["sa"], "dg-b": ["sb1", "sb2"]}
