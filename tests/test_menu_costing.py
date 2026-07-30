"""
Food & recipe costing: purchase-pack -> cost/unit, unit conversion, GST-aware
margins, food-cost flags, price-change ripple, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app


def _owner(c):
    email = f"mc{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "MC Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def test_costing_math_gst_and_conversion():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mc-venue-math")

    # 5 kg of chicken for $50 -> $10/kg
    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Chicken", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 50,
    }, headers=h).json()
    assert ing["cost_per_unit"] == 10.0

    # Recipe: 250 g chicken (unit conversion), sells $11 inc GST
    r = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Chicken Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing["ingredient_id"], "qty": 250, "unit": "g"}],
    }, headers=h)
    assert r.status_code == 200, r.text
    costing = r.json()["costing"]
    assert costing["cost_per_portion"] == 2.50            # 0.25 kg * $10
    assert costing["sell_price_ex_gst"] == 10.0           # 11 / 1.1
    assert costing["margin_per_portion"] == 7.50
    assert costing["food_cost_pct"] == 25.0
    assert costing["flagged"] is False


def test_flag_above_target_and_price_ripple():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mc-venue-flag")

    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Wagyu", "unit": "kg",
        "purchase_size": 1, "purchase_cost": 100,
    }, headers=h).json()
    c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Wagyu Burger", "sell_price_inc_gst": 22.0,
        "items": [{"ingredient_id": ing["ingredient_id"], "qty": 150, "unit": "g"}],
    }, headers=h)

    # 150g * $100/kg = $15 cost on $20 ex-GST -> 75% food cost -> flagged
    menu = c.get(f"/api/menu/costing?venue_id={vid}", headers=h).json()
    assert menu["flagged_count"] == 1
    assert menu["recipes"][0]["food_cost_pct"] == 75.0

    # Supplier price falls to $40/kg -> ripples through with no recipe edit
    c.post("/api/menu/ingredients", json={
        "venue_id": vid, "id": ing["ingredient_id"], "name": "Wagyu", "unit": "kg",
        "purchase_size": 1, "purchase_cost": 40,
    }, headers=h)
    menu2 = c.get(f"/api/menu/costing?venue_id={vid}", headers=h).json()
    assert menu2["recipes"][0]["cost_per_portion"] == 6.0
    assert menu2["recipes"][0]["food_cost_pct"] == 30.0
    assert menu2["flagged_count"] == 0


def test_recipe_rejects_unknown_ingredient_and_bad_units():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mc-venue-guard")
    r = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Ghost Dish", "sell_price_inc_gst": 10,
        "items": [{"ingredient_id": "nope", "qty": 1}],
    }, headers=h)
    assert r.status_code == 422

    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Beans", "unit": "kg",
        "purchase_size": 1, "purchase_cost": 30,
    }, headers=h).json()
    bad_unit = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Beany", "sell_price_inc_gst": 5,
        "items": [{"ingredient_id": ing["ingredient_id"], "qty": 100, "unit": "ml"}],
    }, headers=h)
    assert bad_unit.status_code == 422  # ml into kg is not a thing


def test_seed_idempotent_and_costed():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mc-venue-seed")
    r = c.post("/api/menu/seed", json={"venue_id": vid}, headers=h).json()
    assert "Chicken Parmigiana" in r["recipes_created"]
    r2 = c.post("/api/menu/seed", json={"venue_id": vid}, headers=h).json()
    assert r2["recipes_created"] == [] and r2["ingredients_created"] == []

    menu = c.get(f"/api/menu/costing?venue_id={vid}", headers=h).json()
    assert menu["recipe_count"] == 3
    parmy = [x for x in menu["recipes"] if x["name"] == "Chicken Parmigiana"][0]
    assert 0 < parmy["cost_per_portion"] < parmy["sell_price_ex_gst"]
    assert parmy["lines"]  # itemised breakdown present


def test_menu_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mc-venue-scope")
    from rosteriq.database import get_db
    other = f"mc{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.get(f"/api/menu/costing?venue_id={vid}", headers=sh).status_code == 403
