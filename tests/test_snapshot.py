"""
Business snapshot: prime cost math from rostered labour + sales, ex-GST net
sales basis, honest nulls when there are no sales, and venue scoping.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus


def _owner(c):
    email = f"sn{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "SN Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Snap Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _menu(c, h, vid):
    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Chicken", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 50,
    }, headers=h).json()["ingredient_id"]
    recipe = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing, "qty": 250, "unit": "g"}],
    }, headers=h).json()["recipe_id"]
    return recipe


def test_prime_cost_math_and_bands():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sn-venue-1")
    recipe = _menu(c, h, vid)
    today = date.today()

    # 100 bowls today: revenue 1100 inc GST -> net 1000; COGS 250 (100 * $2.50)
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 100}],
    }, headers=h)

    # A rostered shift today WITHOUT a cached cost -> falls back to rate*hours:
    # 09:00-17:30, 30m break = 8h * $30 = $240 labour
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="sn-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="sn-s1", employee_id=f"{vid}-emp", date=today,
                      start_time=dtime(9, 0), end_time=dtime(17, 30),
                      break_minutes=30, status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))

    s = c.get(f"/api/snapshot?venue_id={vid}&start_date={today}&end_date={today}",
              headers=h).json()
    assert s["revenue_inc_gst"] == 1100.0
    assert s["net_sales"] == 1000.0
    assert s["cogs"] == 250.0
    assert s["labour"] == 240.0
    assert s["food_cost_pct"] == 25.0          # 250 / 1000
    assert s["labour_pct"] == 24.0             # 240 / 1000
    assert s["prime_cost_pct"] == 49.0         # (240+250) / 1000
    assert s["prime_cost_band"] == "healthy"   # <= 60
    assert s["gross_profit"] == 510.0          # 1000 - 250 - 240
    assert s["labour_basis"] == "rostered"
    # per-day series covers exactly the one requested day
    assert len(s["days"]) == 1 and s["days"][0]["prime_pct"] == 49.0


def test_no_sales_yields_null_ratios_not_fake_zero():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sn-venue-empty")
    today = date.today()
    s = c.get(f"/api/snapshot?venue_id={vid}&start_date={today}&end_date={today}",
              headers=h).json()
    assert s["net_sales"] == 0.0
    assert s["food_cost_pct"] is None
    assert s["labour_pct"] is None
    assert s["prime_cost_pct"] is None
    assert s["prime_cost_band"] == "no_sales"


def test_high_prime_cost_is_flagged():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sn-venue-high")
    recipe = _menu(c, h, vid)
    today = date.today()
    # Only 10 bowls: net sales 100, COGS 25; add heavy labour to push prime high
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 10}],
    }, headers=h)
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="sn-roster-h", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="sn-h1", employee_id=f"{vid}-emp", date=today,
                      start_time=dtime(9, 0), end_time=dtime(15, 0),
                      break_minutes=0, status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))  # 6h * $30 = $180 labour on $100 net sales
    s = c.get(f"/api/snapshot?venue_id={vid}&start_date={today}&end_date={today}",
              headers=h).json()
    assert s["prime_cost_pct"] > 65
    assert s["prime_cost_band"] == "high"


def test_snapshot_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "sn-venue-scope")
    other = f"sn{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.get(f"/api/snapshot?venue_id={vid}", headers=sh).status_code == 403
