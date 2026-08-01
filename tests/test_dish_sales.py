"""
Dish sales: recording depletes stock through recipes (with unit conversion),
revenue/COGS/food-cost math, negative-stock is loud not clamped, summary
aggregation, and venue scoping.
"""

import uuid
from datetime import date, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"ds{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "DS Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _setup_menu(c, h, vid):
    """Chicken $10/kg; Chicken Bowl = 250g chicken, sells $11 inc GST
    (cost $2.50/portion, ex-GST $10, FC 25%)."""
    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Chicken", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 50,
    }, headers=h).json()["ingredient_id"]
    recipe = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Chicken Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing, "qty": 250, "unit": "g"}],
    }, headers=h).json()["recipe_id"]
    return ing, recipe


def test_record_depletes_stock_and_computes_money():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-1")
    ing, recipe = _setup_menu(c, h, vid)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 5, "par_level": 2,
    }, headers=h)

    r = c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 12}],
    }, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["total_revenue_inc_gst"] == 132.0     # 12 * $11
    assert out["total_cogs"] == 30.0                 # 12 * $2.50
    assert out["stock_depleted"][ing] == 3.0         # 12 * 250g = 3kg
    assert out["negative_stock_warning"] == []

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["items"][0]["stock_qty"] == 2.0       # 5kg - 3kg


def test_negative_stock_is_loud_not_clamped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-neg")
    ing, recipe = _setup_menu(c, h, vid)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 0.5, "par_level": 2,
    }, headers=h)

    out = c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 4}],  # needs 1kg
    }, headers=h).json()
    assert out["negative_stock_warning"] == ["Chicken"]
    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["items"][0]["stock_qty"] == -0.5      # visible, not hidden


def test_summary_aggregates_and_food_cost_pct():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-sum")
    ing, recipe = _setup_menu(c, h, vid)

    yesterday = date.today() - timedelta(days=1)
    c.post("/api/sales/record", json={
        "venue_id": vid, "sale_date": yesterday.isoformat(),
        "items": [{"recipe_id": recipe, "qty": 10}],
    }, headers=h)
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 5}],
    }, headers=h)

    s = c.get(f"/api/sales/summary?venue_id={vid}", headers=h).json()
    assert s["total_revenue_inc_gst"] == 165.0       # 15 * $11
    assert s["total_revenue_ex_gst"] == 150.0
    assert s["total_cogs"] == 37.5                   # 15 * $2.50
    assert s["gross_profit"] == 112.5
    assert s["food_cost_pct"] == 25.0
    assert s["by_recipe"][0]["name"] == "Chicken Bowl" and s["by_recipe"][0]["qty"] == 15.0
    assert len(s["by_day"]) == 2

    # Future sale dates refused; unknown recipe refused with its id named
    assert c.post("/api/sales/record", json={
        "venue_id": vid, "sale_date": (date.today() + timedelta(days=1)).isoformat(),
        "items": [{"recipe_id": recipe, "qty": 1}],
    }, headers=h).status_code == 422
    bad = c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": "ghost-dish", "qty": 1}],
    }, headers=h)
    assert bad.status_code == 422


def test_import_auto_map_explicit_map_and_unmapped_honesty():
    """POS names matching recipes auto-map; strangers come back unmapped;
    an explicit mapping fixes them; nothing is silently dropped."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-imp")
    ing, recipe = _setup_menu(c, h, vid)

    r = c.post("/api/sales/import", json={
        "venue_id": vid, "rows": [
            {"item_name": "  CHICKEN  bowl ", "qty": 3},   # auto-maps (case/space)
            {"item_name": "Mystery Special", "qty": 2},     # unknown
        ],
    }, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "imported" and out["recorded_lines"] == 1
    assert out["auto_mapped"] == ["CHICKEN  bowl"]
    assert out["unmapped"] == [{"item_name": "Mystery Special", "qty": 2.0}]
    assert out["total_revenue_inc_gst"] == 33.0             # 3 * $11

    # Map the stranger to the recipe, re-import just the remainder
    m = c.post("/api/sales/mapping", json={
        "venue_id": vid, "item_name": "Mystery Special", "recipe_id": recipe,
    }, headers=h)
    assert m.status_code == 200 and m.json()["recipe_name"] == "Chicken Bowl"
    r2 = c.post("/api/sales/import", json={
        "venue_id": vid, "rows": [{"item_name": "Mystery Special", "qty": 2}],
    }, headers=h).json()
    assert r2["recorded_lines"] == 1 and r2["unmapped"] == []

    # Mapping list shows both; removing one works
    maps = c.get(f"/api/sales/mapping?venue_id={vid}", headers=h).json()
    assert maps["count"] == 2
    c.post("/api/sales/mapping", json={
        "venue_id": vid, "item_name": "Mystery Special", "recipe_id": "",
    }, headers=h)
    assert c.get(f"/api/sales/mapping?venue_id={vid}", headers=h).json()["count"] == 1

    # Summary sees all 5 sold
    s = c.get(f"/api/sales/summary?venue_id={vid}", headers=h).json()
    assert s["by_recipe"][0]["qty"] == 5.0


def test_import_duplicate_batch_refused():
    """Uploading the same product-mix twice must not double-deplete stock."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-dup")
    ing, _recipe = _setup_menu(c, h, vid)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 5, "par_level": 1}, headers=h)

    rows = {"venue_id": vid, "rows": [{"item_name": "Chicken Bowl", "qty": 4}]}
    assert c.post("/api/sales/import", json=rows, headers=h).status_code == 200
    dup = c.post("/api/sales/import", json=rows, headers=h)
    assert dup.status_code == 409

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["items"][0]["stock_qty"] == 4.0   # 5 - 1kg, depleted ONCE

    # Nothing-matched imports don't burn the batch: unknown-only upload, then
    # map it, then the same upload again succeeds
    unk = {"venue_id": vid, "rows": [{"item_name": "Loaded Fries", "qty": 2}]}
    first = c.post("/api/sales/import", json=unk, headers=h).json()
    assert first["status"] == "nothing_imported"
    c.post("/api/sales/mapping", json={
        "venue_id": vid, "item_name": "Loaded Fries",
        "recipe_id": c.get(f"/api/menu/costing?venue_id={vid}", headers=h).json()["recipes"][0]["id"],
    }, headers=h)
    assert c.post("/api/sales/import", json=unk, headers=h).json()["status"] == "imported"


def test_sales_are_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ds-venue-scope")
    _ing, recipe = _setup_menu(c, h, vid)

    other = f"ds{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 1}],
    }, headers=sh).status_code == 403
    assert c.get(f"/api/sales/summary?venue_id={vid}", headers=sh).status_code == 403
