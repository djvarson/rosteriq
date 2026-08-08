"""
Daily briefing: synthesises prime cost, today's roster+coverage, pending
approvals, and kitchen flags into one payload, with a deterministic
'attention' list and all_clear flag. Venue scoped.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus


def _owner(c):
    email = f"br{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "BR Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Brief Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00", "email": f"{vid}staff@x.com",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def test_all_clear_when_nothing_needs_attention():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "br-venue-clear")
    b = c.get(f"/api/briefing?venue_id={vid}", headers=h).json()
    assert b["all_clear"] is True and b["attention"] == []
    assert b["approvals"]["pending_leave"] == 0
    assert b["kitchen"]["below_par_count"] == 0


def test_briefing_surfaces_everything_that_needs_attention():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "br-venue-full")

    # Below-par ingredient + a flagged (expensive) dish
    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Wagyu", "unit": "kg",
        "purchase_size": 1, "purchase_cost": 100,
    }, headers=h).json()["ingredient_id"]
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 1, "par_level": 5,
    }, headers=h)
    c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Wagyu Burger", "sell_price_inc_gst": 22.0,
        "items": [{"ingredient_id": ing, "qty": 150, "unit": "g"}],
    }, headers=h)  # 150g*$100 = $15 on $20 ex -> 75% food cost, flagged

    # A pending leave request
    db = get_db()
    db.save_leave_request({
        "id": "br-lv", "venue_id": vid, "employee_id": f"{vid}-emp",
        "start_date": date.today() + timedelta(days=3),
        "end_date": date.today() + timedelta(days=4),
        "reason": "Trip", "status": "pending", "created_at": datetime(2026, 7, 1),
    })

    b = c.get(f"/api/briefing?venue_id={vid}", headers=h).json()
    assert b["all_clear"] is False
    joined = " ".join(b["attention"]).lower()
    assert "below par" in joined
    assert "food-cost target" in joined
    assert "leave request" in joined
    assert b["approvals"]["pending_leave"] == 1
    assert b["kitchen"]["below_par_count"] == 1
    assert b["kitchen"]["flagged_dishes"] == 1


def test_briefing_counts_uncosted_labour_and_survives_bad_recipe():
    """Two regressions: (1) a shift with no cached cost still counts toward
    labour/prime (was silently dropped -> fail-silent low prime cost); (2) a
    recipe with a bad unit conversion doesn't crash the whole briefing."""
    from datetime import time as dtime, timedelta
    from rosteriq.models import Roster, Shift, ShiftStatus

    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "br-venue-labour")

    # Sales so net > 0
    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Beans", "unit": "kg",
        "purchase_size": 1, "purchase_cost": 10,
    }, headers=h).json()["ingredient_id"]
    recipe = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing, "qty": 100, "unit": "g"}],
    }, headers=h).json()["recipe_id"]
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 100}],
    }, headers=h)

    # A rostered shift with cost=None (uncosted) — must still count as labour
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="br-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="br-s1", employee_id=f"{vid}-emp", date=today,
                      start_time=dtime(9, 0), end_time=dtime(17, 0), break_minutes=0,
                      status=ShiftStatus.scheduled, role="floor", cost=None)],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))
    # A recipe whose unit can't convert (g into an 'each' ingredient)
    bad_ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Eggs", "unit": "each",
        "purchase_size": 12, "purchase_cost": 6,
    }, headers=h).json()["ingredient_id"]
    db.save_recipe({
        "id": "br-badrcp", "venue_id": vid, "name": "Bad Dish",
        "sell_price_inc_gst": 10.0, "yield_portions": 1, "active": True,
        "items": [{"ingredient_id": bad_ing, "qty": 100, "unit": "g"}],  # g -> each
    })

    r = c.get(f"/api/briefing?venue_id={vid}", headers=h)
    assert r.status_code == 200, r.text          # did NOT crash on the bad recipe
    b = r.json()
    # 8h * $30 = $240 labour counted despite cost=None
    assert b["prime_cost_pct_7d"] is not None and b["prime_cost_pct_7d"] > 20


def test_briefing_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "br-venue-scope")
    other = f"br{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.get(f"/api/briefing?venue_id={vid}", headers=sh).status_code == 403
