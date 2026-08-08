"""
Wastage logging: recording spoiled/dropped stock depletes the ingredient and
books the dollar loss (with unit conversion), the report totals by reason,
reason validation, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"ws{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "WS Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _ingredient(c, h, vid, name="Chicken", unit="kg", size=5, cost=50):
    return c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": name, "unit": unit,
        "purchase_size": size, "purchase_cost": cost,
    }, headers=h).json()["ingredient_id"]


def test_waste_depletes_stock_and_values_with_unit_conversion():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ws-venue-1")
    ing = _ingredient(c, h, vid)  # $10/kg
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 5, "par_level": 2}, headers=h)

    # Waste 500g of spoiled chicken -> 0.5kg, $5.00, stock 5 -> 4.5
    r = c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 500, "unit": "g",
        "reason": "spoiled", "note": "left out overnight",
    }, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["value"] == 5.0 and out["stock_after"] == 4.5
    assert "negative_stock_warning" not in out

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["items"][0]["stock_qty"] == 4.5


def test_waste_report_totals_by_reason():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ws-venue-rep")
    ing = _ingredient(c, h, vid)  # $10/kg
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 20, "par_level": 2}, headers=h)

    c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 1, "reason": "spoiled"}, headers=h)   # $10
    c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 2, "reason": "spoiled"}, headers=h)   # $20
    c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 0.5, "reason": "dropped"}, headers=h) # $5

    rep = c.get(f"/api/inventory/waste?venue_id={vid}", headers=h).json()
    assert rep["total_waste_value"] == 35.0
    by = {b["reason"]: b["value"] for b in rep["by_reason"]}
    assert by["spoiled"] == 30.0 and by["dropped"] == 5.0
    assert rep["by_reason"][0]["reason"] == "spoiled"  # worst first
    assert len(rep["entries"]) == 3
    # Stock depleted by all three: 20 - 3.5 = 16.5
    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["items"][0]["stock_qty"] == 16.5


def test_waste_guards():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ws-venue-guard")
    ing = _ingredient(c, h, vid)

    # Bad reason -> 422
    assert c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 1, "reason": "meteor"},
        headers=h).status_code == 422
    # Unknown ingredient -> 404
    assert c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": "ghost", "qty": 1, "reason": "spoiled"},
        headers=h).status_code == 404
    # Incompatible unit (ml into kg) -> 422
    assert c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 100, "unit": "ml", "reason": "spoiled"},
        headers=h).status_code == 422
    # qty <= 0 rejected by pydantic
    assert c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 0, "reason": "spoiled"},
        headers=h).status_code == 422


def test_waste_negative_stock_is_flagged():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ws-venue-neg")
    ing = _ingredient(c, h, vid)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 1, "par_level": 2}, headers=h)
    out = c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 3, "reason": "expired"}, headers=h).json()
    assert out["negative_stock_warning"] == "Chicken" and out["stock_after"] == -2.0


def test_waste_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ws-venue-scope")
    ing = _ingredient(c, h, vid)
    other = f"ws{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.post("/api/inventory/waste", json={
        "venue_id": vid, "ingredient_id": ing, "qty": 1, "reason": "spoiled"},
        headers=sh).status_code == 403
    assert c.get(f"/api/inventory/waste?venue_id={vid}", headers=sh).status_code == 403
