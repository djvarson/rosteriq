"""
Native time clock — the first pillar of replacing external attendance software.

Covers the kiosk lifecycle: board -> clock in -> clock out (worked minutes +
variance vs the rostered shift) -> timesheets feed, plus PIN protection,
double-punch rejection, and tenant scoping.
"""

import uuid
from datetime import date, datetime, time as dtime

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus


def _owner(c):
    email = f"tc{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _setup_venue(c, h, venue_id="clock-venue"):
    c.post("/venues", json={
        "id": venue_id, "name": "Clock Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": "clk-emp-1", "name": "Clock Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": venue_id,
        "hourly_base_rate": "31.50", "skills": ["bar"],
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    return venue_id


def test_full_clock_lifecycle_with_variance():
    c = TestClient(app)
    h = _owner(c)
    vid = _setup_venue(c, h)

    # Roster today's 8h shift (17:00-01:00 would complicate; use 09:00-17:00, 30m break)
    today = date.today()
    from datetime import timedelta
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="clk-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="clk-shift-1", employee_id="clk-emp-1", date=today,
                      start_time=dtime(9, 0), end_time=dtime(17, 0),
                      break_minutes=30, status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))

    # Board shows the employee, rostered, state off
    board = c.get(f"/api/clock/board?venue_id={vid}", headers=h).json()
    me = [s for s in board["staff"] if s["employee_id"] == "clk-emp-1"][0]
    assert me["state"] == "off" and me["rostered"]["start"] == "09:00"

    # Clock in
    r = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "clk-emp-1"}, headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "clocked_in" and r.json()["rostered"] is True

    # Double clock-in rejected
    r2 = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "clk-emp-1"}, headers=h)
    assert r2.status_code == 409

    # Board now shows on
    board = c.get(f"/api/clock/board?venue_id={vid}", headers=h).json()
    me = [s for s in board["staff"] if s["employee_id"] == "clk-emp-1"][0]
    assert me["state"] == "on"

    # Clock out with a 30m break
    r3 = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": "clk-emp-1",
                                        "break_minutes": 30}, headers=h)
    assert r3.status_code == 200, r3.text
    out = r3.json()
    assert out["status"] == "clocked_out"
    # Worked ~0 minutes minus break floor at 0; variance = worked - rostered(450m)
    assert out["variance_minutes"] is not None
    assert out["variance_minutes"] <= -400  # clocked out immediately vs 7.5h rostered

    # Clock out again rejected
    r4 = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": "clk-emp-1"}, headers=h)
    assert r4.status_code == 409

    # Timesheet feed contains the closed sheet
    feed = c.get(f"/api/clock/timesheets?venue_id={vid}&start_date={today}&end_date={today}",
                 headers=h).json()
    assert feed["count"] == 1
    row = feed["timesheets"][0]
    assert row["employee_name"] == "Clock Tester" and row["status"] == "closed"


def test_pin_protection():
    c = TestClient(app)
    h = _owner(c)
    vid = _setup_venue(c, h, "clock-venue-pin")

    # Set a PIN
    r = c.post("/api/clock/pin", json={"venue_id": vid, "employee_id": "clk-emp-1",
                                       "pin": "4321"}, headers=h)
    assert r.status_code == 200

    # Wrong PIN rejected
    bad = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "clk-emp-1",
                                        "pin": "9999"}, headers=h)
    assert bad.status_code == 403

    # Correct PIN accepted and marked verified
    ok = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "clk-emp-1",
                                       "pin": "4321"}, headers=h)
    assert ok.status_code == 200 and ok.json()["pin_verified"] is True


def test_clock_requires_venue_access():
    """A staff user scoped to another venue can't punch this venue's clock."""
    c = TestClient(app)
    h = _owner(c)
    vid = _setup_venue(c, h, "clock-venue-scope")

    other_email = f"tc{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other_email, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other_email)
    rec["venue_ids"] = ["some-other-venue"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other_email, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    r = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "clk-emp-1"}, headers=sh)
    assert r.status_code == 403


def test_unknown_employee_404():
    c = TestClient(app)
    h = _owner(c)
    vid = _setup_venue(c, h, "clock-venue-404")
    r = c.post("/api/clock/in", json={"venue_id": vid, "employee_id": "ghost"}, headers=h)
    assert r.status_code == 404
