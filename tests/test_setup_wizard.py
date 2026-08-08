"""
First-run setup wizard: step status computed from real venue state, progress
advances as steps are completed, optional steps don't block completion, and
venue scoping.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus


def _owner(c):
    email = f"sw{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "SW Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _step(status, key):
    return [s for s in status["steps"] if s["key"] == key][0]


def test_wizard_advances_as_steps_complete():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sw-venue-1")

    # Fresh venue: nothing done, next step is staff, 0%
    s0 = c.get(f"/api/setup?venue_id={vid}", headers=h).json()
    assert s0["complete"] is False and s0["percent"] == 0
    assert s0["steps_total"] == 4  # staff, menu, stock, roster (connect optional)
    assert s0["next_step"]["key"] == "staff"
    assert _step(s0, "staff")["done"] is False

    # Add staff -> staff done, next is menu
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "First Hire", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    s1 = c.get(f"/api/setup?venue_id={vid}", headers=h).json()
    assert _step(s1, "staff")["done"] is True
    assert s1["next_step"]["key"] == "menu" and s1["percent"] == 25

    # Seed menu -> menu done
    c.post("/api/menu/seed", json={"venue_id": vid}, headers=h)
    s2 = c.get(f"/api/setup?venue_id={vid}", headers=h).json()
    assert _step(s2, "menu")["done"] is True and s2["next_step"]["key"] == "stock"

    # Set a par level -> stock done
    ing = c.get(f"/api/menu/ingredients?venue_id={vid}", headers=h).json()["ingredients"][0]["id"]
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 5, "par_level": 3}, headers=h)
    s3 = c.get(f"/api/setup?venue_id={vid}", headers=h).json()
    assert _step(s3, "stock")["done"] is True and s3["next_step"]["key"] == "roster"

    # Save a roster -> roster done -> COMPLETE (connect is optional)
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="sw-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="sw-s1", employee_id=f"{vid}-emp", date=today,
                      start_time=dtime(9, 0), end_time=dtime(17, 0), break_minutes=30,
                      status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))
    s4 = c.get(f"/api/setup?venue_id={vid}", headers=h).json()
    assert _step(s4, "roster")["done"] is True
    assert s4["complete"] is True and s4["percent"] == 100 and s4["next_step"] is None
    # The optional Deputy step is still not done, but doesn't block completion
    assert _step(s4, "connect")["optional"] is True and _step(s4, "connect")["done"] is False


def test_wizard_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sw-venue-scope")
    other = f"sw{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.get(f"/api/setup?venue_id={vid}", headers=sh).status_code == 403
