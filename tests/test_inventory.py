"""
Inventory: stock levels + low flags, the stocktake lifecycle (variance valued
in dollars, stock corrected, uncounted items never invented), supplier order
drafts with pack rounding, receiving books stock in, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"inv{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "Inv Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _ingredient(c, h, vid, name, unit="kg", size=5, cost=50, supplier="Acme Foods"):
    return c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": name, "unit": unit,
        "purchase_size": size, "purchase_cost": cost, "supplier": supplier,
    }, headers=h).json()["ingredient_id"]


def test_levels_low_flags_and_value():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-lvl")
    chicken = _ingredient(c, h, vid, "Chicken")  # $10/kg

    r = c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": chicken, "stock_qty": 3, "par_level": 10,
    }, headers=h)
    assert r.status_code == 200, r.text

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["low_count"] == 1
    item = inv["items"][0]
    assert item["low"] is True and item["stock_value"] == 30.0  # 3kg * $10
    assert inv["total_stock_value"] == 30.0

    # Unknown ingredient 404; nothing-to-set 422
    assert c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": "ghost", "stock_qty": 1}, headers=h).status_code == 404
    assert c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": chicken}, headers=h).status_code == 422


def test_stocktake_lifecycle_variance_and_stock_correction():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-st")
    chicken = _ingredient(c, h, vid, "Chicken")            # $10/kg
    cheese = _ingredient(c, h, vid, "Cheese", cost=100)    # $20/kg
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": chicken, "stock_qty": 10, "par_level": 10}, headers=h)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": cheese, "stock_qty": 5, "par_level": 5}, headers=h)

    st = c.post("/api/inventory/stocktake/start", json={"venue_id": vid}, headers=h)
    assert st.status_code == 200, st.text
    st_id = st.json()["stocktake_id"]
    assert st.json()["item_count"] == 2

    # Second open stocktake refused
    assert c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                  headers=h).status_code == 409
    # Completing with zero counts refused
    assert c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st_id}, headers=h).status_code == 422

    # Count chicken at 8 (2kg short = -$20); leave cheese uncounted
    r = c.post("/api/inventory/stocktake/count", json={
        "venue_id": vid, "stocktake_id": st_id, "ingredient_id": chicken, "counted": 8,
    }, headers=h)
    assert r.status_code == 200

    done = c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st_id}, headers=h).json()
    assert done["total_variance_value"] == -20.0
    assert done["uncounted"] == ["Cheese"]

    # Chicken stock corrected to the count; cheese untouched
    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    by_name = {i["name"]: i for i in inv["items"]}
    assert by_name["Chicken"]["stock_qty"] == 8.0
    assert by_name["Cheese"]["stock_qty"] == 5.0

    # Double-complete refused; history shows it
    assert c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st_id}, headers=h).status_code == 409
    hist = c.get(f"/api/inventory/stocktakes?venue_id={vid}", headers=h).json()
    assert hist["count"] == 1 and hist["stocktakes"][0]["status"] == "completed"


def test_order_draft_pack_rounding_and_receive():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-ord")
    # Chicken: 5kg packs @ $50, par 10, stock 2 -> need 8 -> 2 packs = $100
    chicken = _ingredient(c, h, vid, "Chicken")
    # Beans: different supplier, 1kg packs @ $30, par 3, stock 1 -> 2 packs
    beans = _ingredient(c, h, vid, "Beans", size=1, cost=30, supplier="Bean Bros")
    # Milk: at par -> not ordered
    milk = _ingredient(c, h, vid, "Milk", unit="l", size=10, cost=15)
    for ing, stock, par in ((chicken, 2, 10), (beans, 1, 3), (milk, 10, 10)):
        c.post("/api/inventory/levels", json={
            "venue_id": vid, "ingredient_id": ing, "stock_qty": stock, "par_level": par,
        }, headers=h)

    draft = c.post("/api/inventory/order/draft", json={"venue_id": vid}, headers=h).json()
    assert len(draft["orders"]) == 2  # one per supplier
    by_supplier = {o["supplier"]: o for o in draft["orders"]}
    assert by_supplier["Acme Foods"]["total_cost"] == 100.0  # 2 packs * $50
    assert by_supplier["Bean Bros"]["total_cost"] == 60.0    # 2 packs * $30

    acme_id = by_supplier["Acme Foods"]["order_id"]
    # Can't receive a draft (must be ordered first)
    assert c.post(f"/api/inventory/order/{acme_id}/status", json={
        "venue_id": vid, "status": "received"}, headers=h).status_code == 409

    assert c.post(f"/api/inventory/order/{acme_id}/status", json={
        "venue_id": vid, "status": "ordered"}, headers=h).status_code == 200
    assert c.post(f"/api/inventory/order/{acme_id}/status", json={
        "venue_id": vid, "status": "received"}, headers=h).status_code == 200

    # Receiving booked 2 packs * 5kg = 10kg on top of the 2 in stock
    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    chicken_row = [i for i in inv["items"] if i["name"] == "Chicken"][0]
    assert chicken_row["stock_qty"] == 12.0 and chicken_row["low"] is False

    # Beans are still below par BUT already covered by their open draft —
    # drafting again must NOT duplicate the order (double-ordering money guard)
    assert c.post("/api/inventory/order/draft", json={"venue_id": vid},
                  headers=h).status_code == 409

    # Cancel the Bean Bros draft -> the shortfall is uncovered again -> draftable
    beans_id = by_supplier["Bean Bros"]["order_id"]
    assert c.post(f"/api/inventory/order/{beans_id}/status", json={
        "venue_id": vid, "status": "cancelled"}, headers=h).status_code == 200
    draft2 = c.post("/api/inventory/order/draft", json={"venue_id": vid}, headers=h).json()
    assert [o["supplier"] for o in draft2["orders"]] == ["Bean Bros"]


def test_pack_rounding_is_float_safe():
    """par 0.8, stock 0.2, pack 0.2 -> exactly 3 packs, never 4 (0.6000...01
    binary-float artifact must not buy an extra pack of real money)."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-float")
    saffron = _ingredient(c, h, vid, "Saffron", unit="kg", size=0.2, cost=200)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": saffron, "stock_qty": 0.2, "par_level": 0.8,
    }, headers=h)
    draft = c.post("/api/inventory/order/draft", json={"venue_id": vid}, headers=h).json()
    line = draft["orders"][0]
    assert line["total_cost"] == 600.0  # 3 packs * $200, NOT 4


def test_receive_blocked_during_open_stocktake():
    """A delivery booked in mid-count would be overwritten when the stocktake
    completes — receiving must refuse until the count is done."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-block")
    chicken = _ingredient(c, h, vid, "Chicken")
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": chicken, "stock_qty": 2, "par_level": 10}, headers=h)
    order_id = c.post("/api/inventory/order/draft", json={"venue_id": vid},
                      headers=h).json()["orders"][0]["order_id"]
    c.post(f"/api/inventory/order/{order_id}/status", json={
        "venue_id": vid, "status": "ordered"}, headers=h)

    st_id = c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                   headers=h).json()["stocktake_id"]
    blocked = c.post(f"/api/inventory/order/{order_id}/status", json={
        "venue_id": vid, "status": "received"}, headers=h)
    assert blocked.status_code == 409
    body = blocked.json()
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "stocktake" in message.lower()

    c.post("/api/inventory/stocktake/count", json={
        "venue_id": vid, "stocktake_id": st_id, "ingredient_id": chicken, "counted": 2,
    }, headers=h)
    c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st_id}, headers=h)
    assert c.post(f"/api/inventory/order/{order_id}/status", json={
        "venue_id": vid, "status": "received"}, headers=h).status_code == 200


def test_inactive_ingredients_excluded():
    """Deactivated ingredients must not be valued, counted, or ordered."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-inactive")
    old = _ingredient(c, h, vid, "Discontinued Sauce")
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": old, "stock_qty": 0, "par_level": 5}, headers=h)
    # Deactivate it
    c.post("/api/menu/ingredients", json={
        "venue_id": vid, "id": old, "name": "Discontinued Sauce", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 50, "active": False,
    }, headers=h)

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    assert inv["count"] == 0
    assert c.post("/api/inventory/order/draft", json={"venue_id": vid},
                  headers=h).status_code == 409
    assert c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                  headers=h).status_code == 422  # nothing active to count


def test_price_edit_preserves_stock_levels():
    """Editing an ingredient's price in Menu & Costing must not wipe the
    stock/par set in Inventory."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-keep")
    chicken = _ingredient(c, h, vid, "Chicken")
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": chicken, "stock_qty": 7, "par_level": 12,
    }, headers=h)

    # Supplier price rises $50 -> $60 for the same 5kg pack
    c.post("/api/menu/ingredients", json={
        "venue_id": vid, "id": chicken, "name": "Chicken", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 60, "supplier": "Acme Foods",
    }, headers=h)

    inv = c.get(f"/api/inventory?venue_id={vid}", headers=h).json()
    row = inv["items"][0]
    assert row["stock_qty"] == 7.0 and row["par_level"] == 12.0
    assert row["cost_per_unit"] == 12.0  # new price flowed through


def test_usage_report_theoretical_vs_actual():
    """Opening 10kg + received 5kg - closing 10kg = 5kg actual; sales say
    3kg theoretical -> 2kg unexplained = $20 waste at $10/kg."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-usage")
    ing = _ingredient(c, h, vid, "Chicken")   # $10/kg, 5kg packs
    recipe = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Chicken Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing, "qty": 250, "unit": "g"}],
    }, headers=h).json()["recipe_id"]

    # Not enough stocktakes yet -> honest 422
    assert c.get(f"/api/inventory/usage?venue_id={vid}", headers=h).status_code == 422

    # Opening stocktake: 10kg counted (par 12: after the 3kg sale below,
    # stock 7 -> needed 5 -> exactly one 5kg pack gets ordered)
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 10, "par_level": 12}, headers=h)
    st1 = c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                 headers=h).json()["stocktake_id"]
    c.post("/api/inventory/stocktake/count", json={
        "venue_id": vid, "stocktake_id": st1, "ingredient_id": ing, "counted": 10}, headers=h)
    c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st1}, headers=h)

    # In the window: sell 12 bowls (3kg theoretical), receive 1 pack (5kg)
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 12}]}, headers=h)
    order_id = c.post("/api/inventory/order/draft", json={"venue_id": vid},
                      headers=h).json()["orders"][0]["order_id"]
    c.post(f"/api/inventory/order/{order_id}/status", json={
        "venue_id": vid, "status": "ordered"}, headers=h)
    c.post(f"/api/inventory/order/{order_id}/status", json={
        "venue_id": vid, "status": "received"}, headers=h)

    # Closing stocktake: 10kg counted (12 in system: 10-3+5; 2kg walked out)
    st2 = c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                 headers=h).json()["stocktake_id"]
    c.post("/api/inventory/stocktake/count", json={
        "venue_id": vid, "stocktake_id": st2, "ingredient_id": ing, "counted": 10}, headers=h)
    c.post("/api/inventory/stocktake/complete", json={
        "venue_id": vid, "stocktake_id": st2}, headers=h)

    r = c.get(f"/api/inventory/usage?venue_id={vid}", headers=h)
    assert r.status_code == 200, r.text
    report = r.json()
    row = report["items"][0]
    assert row["opening"] == 10.0 and row["received"] == 5.0 and row["closing"] == 10.0
    assert row["actual_usage"] == 5.0
    assert row["theoretical_usage"] == 3.0
    assert row["variance"] == 2.0
    assert row["variance_value"] == 20.0      # 2kg * $10
    assert report["total_variance_value"] == 20.0


def test_inventory_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "inv-venue-scope")
    _ingredient(c, h, vid, "Chicken")

    other = f"inv{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.get(f"/api/inventory?venue_id={vid}", headers=sh).status_code == 403
    assert c.post("/api/inventory/stocktake/start", json={"venue_id": vid},
                  headers=sh).status_code == 403
    assert c.post("/api/inventory/order/draft", json={"venue_id": vid},
                  headers=sh).status_code == 403
