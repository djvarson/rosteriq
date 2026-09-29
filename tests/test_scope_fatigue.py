"""
Fatigue / burnout routes are venue-private, manager-level analytics.

routes/fatigue.py had no venue gate: any signed-in user (including the public
demo) could read another venue's employee name via /clopenings, probe ids via
the 404-vs-500 split on the other per-employee routes, and request any venue's
/team-fatigue report — which was built from the platform-wide staff list.
"""

import uuid
from datetime import date, time, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Shift, ShiftStatus
from rosteriq.services.fatigue_predictor import FatiguePredictor

PW = "Passw0rd!234"
EMPLOYEE_ROUTES = ("fatigue-risk", "clopenings", "burnout-prediction", "recovery-plan")


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, staff_name):
    """A fresh non-owner manager with one venue and one employee."""
    tag = uuid.uuid4().hex[:8]
    email = f"sf_{tag}@x.com"
    h = _login(c, email)
    vid = f"sf-{tag}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "vic", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    h = _login(c, email)
    user = get_db().get_user_by_email(email)
    assert user["role"] == "manager" and not user.get("is_owner") and user["venue_ids"] == [vid]
    eid = f"{vid}-e0"
    assert c.post("/employees", json={
        "id": eid, "venue_id": vid, "name": staff_name, "employment_type": "full_time",
        "award_level": "level_2", "state": "vic", "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"},
        headers=h).status_code == 200
    return h, vid, eid


def _recent_shifts(eid):
    db = get_db()
    for back in (1, 2, 3):
        db.save_shift(Shift(
            id=f"{eid}-s{back}", employee_id=eid, date=date.today() - timedelta(days=back),
            start_time=time(9, 0), end_time=time(17, 0), status=ShiftStatus.scheduled,
            role="bar",
        ))


def _world():
    c = TestClient(app, raise_server_exceptions=False)
    _login(c, f"sf_boot_{uuid.uuid4().hex[:8]}@x.com")
    tag = uuid.uuid4().hex[:6]
    a_h, a_vid, a_eid = _venue(c, f"Alice Own {tag}")
    b_h, b_vid, b_eid = _venue(c, f"Bob Neighbour {tag}")
    return c, tag, (a_h, a_vid, a_eid), (b_h, b_vid, b_eid)


def test_other_venues_employee_is_indistinguishable_from_missing():
    c, tag, (a_h, _, _), (_, _, b_eid) = _world()
    _recent_shifts(b_eid)
    for route in EMPLOYEE_ROUTES:
        r = c.get(f"/api/v1/employees/{b_eid}/{route}", headers=a_h)
        missing = c.get(f"/api/v1/employees/nope-{tag}/{route}", headers=a_h)
        assert r.status_code == 404, (route, r.status_code, r.text[:200])
        assert r.text == missing.text, route
        assert "Bob Neighbour" not in r.text


def test_other_venues_team_fatigue_is_refused():
    c, _, (a_h, _, _), (_, b_vid, b_eid) = _world()
    _recent_shifts(b_eid)
    r = c.get(f"/api/v1/venues/{b_vid}/team-fatigue", headers=a_h)
    assert r.status_code == 403, r.text
    assert "Bob Neighbour" not in r.text


def test_demo_session_cannot_reach_a_real_venue():
    c, _, _, (_, b_vid, b_eid) = _world()
    _recent_shifts(b_eid)
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    for route in EMPLOYEE_ROUTES:
        r = c.get(f"/api/v1/employees/{b_eid}/{route}", headers=demo_h)
        assert r.status_code == 404, (route, r.status_code, r.text[:200])
        assert "Bob Neighbour" not in r.text
    assert c.get(f"/api/v1/venues/{b_vid}/team-fatigue", headers=demo_h).status_code == 403


def test_staff_role_member_cannot_read_fatigue_analytics():
    c, _, (_, a_vid, a_eid), _ = _world()
    email = f"sf_staff_{uuid.uuid4().hex[:8]}@x.com"
    staff_h = _login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"], rec["venue_ids"] = "staff", [a_vid]
    db.save_user(rec)
    assert c.get(f"/api/v1/venues/{a_vid}/team-fatigue", headers=staff_h).status_code == 403
    for route in EMPLOYEE_ROUTES:
        r = c.get(f"/api/v1/employees/{a_eid}/{route}", headers=staff_h)
        assert r.status_code == 403, (route, r.status_code, r.text[:200])


def test_own_venue_still_works_and_lists_only_own_staff(monkeypatch):
    # assess_fatigue <-> _calculate_trend recurse without end for anyone with
    # recent shifts (pre-existing, tracked separately); stub the trend so the
    # scoping of the data path can be pinned.
    monkeypatch.setattr(FatiguePredictor, "_calculate_trend", lambda self, eid, start: "stable")
    c, tag, (a_h, a_vid, a_eid), (_, _, b_eid) = _world()

    r = c.get(f"/api/v1/employees/{a_eid}/clopenings", headers=a_h)
    assert r.status_code == 200, r.text
    assert r.json()["employee_name"] == f"Alice Own {tag}"

    _recent_shifts(a_eid)
    _recent_shifts(b_eid)

    r = c.get(f"/api/v1/venues/{a_vid}/team-fatigue", headers=a_h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["venue_id"] == a_vid
    assert [e["employee_id"] for e in body["employees"]] == [a_eid]
    assert "Bob Neighbour" not in r.text

    for route in EMPLOYEE_ROUTES:
        r = c.get(f"/api/v1/employees/{a_eid}/{route}", headers=a_h)
        assert r.status_code == 200, (route, r.status_code, r.text[:200])
        assert r.json()["employee_id"] == a_eid
    r = c.get(f"/api/v1/employees/{a_eid}/fatigue-risk", headers=a_h)
    assert r.json()["employee_name"] == f"Alice Own {tag}"
