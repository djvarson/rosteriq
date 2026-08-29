"""
Sections on inventory: kitchen stock, bar stock and cellar stock are
different worlds in a real venue — a chef doing a stocktake should never
scroll through the beer, and vice versa.

Pins:
* an ingredient carries a section; created without one it lands in "kitchen"
* the section survives a price edit (like stock/par — an edit that doesn't
  mention it must not reset it)
* the inventory list returns it, so the UI can build section pills
* the starter seed demonstrates it (coffee stock is "bar", food "kitchen")
* section strings are normalised (trim/lowercase)
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


PW = "Passw0rd!234"


def _err(r):
    body = r.json()
    return body.get("detail") or body.get("error", {}).get("message", "")


def _world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    email = f"sec_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    vid = f"sec-{tag}"
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text
    return c, h, vid


def _ing(c, h, vid, name, **kw):
    body = {"venue_id": vid, "name": name, "unit": "kg",
            "purchase_size": 5, "purchase_cost": 50, **kw}
    r = c.post("/api/menu/ingredients", json=body, headers=h)
    assert r.status_code == 200, r.text
    return r.json()["ingredient_id"]


def test_section_defaults_to_kitchen_and_round_trips():
    c, h, vid = _world()
    _ing(c, h, vid, "Chicken")                        # no section given
    _ing(c, h, vid, "Lager kegs", section="cellar")
    _ing(c, h, vid, "Coffee beans", section="  BAR ") # normalised

    items = {i["name"]: i for i in
             c.get(f"/api/inventory?venue_id={vid}", headers=h).json()["items"]}
    assert items["Chicken"]["section"] == "kitchen"
    assert items["Lager kegs"]["section"] == "cellar"
    assert items["Coffee beans"]["section"] == "bar"


def test_price_edit_preserves_the_section():
    """The upsert carries stock/par across edits; section must ride along —
    a supplier price change silently re-homing bar stock into the kitchen
    would wreck every sectioned stocktake after it."""
    c, h, vid = _world()
    ing_id = _ing(c, h, vid, "House red", section="cellar")

    r = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "id": ing_id, "name": "House red", "unit": "l",
        "purchase_size": 6, "purchase_cost": 72,          # price edit, no section
    }, headers=h)
    assert r.status_code == 200, r.text

    items = {i["name"]: i for i in
             c.get(f"/api/inventory?venue_id={vid}", headers=h).json()["items"]}
    assert items["House red"]["section"] == "cellar", "edit reset the section"


def test_starter_seed_sections_bar_stock():
    c, h, vid = _world()
    r = c.post("/api/menu/seed", json={"venue_id": vid}, headers=h)
    assert r.status_code == 200, r.text
    items = {i["name"]: i for i in
             c.get(f"/api/inventory?venue_id={vid}", headers=h).json()["items"]}
    assert items["Coffee beans"]["section"] == "bar"
    assert items["Milk"]["section"] == "bar"
    assert items["Chicken breast"]["section"] == "kitchen"


# ---------------------------------------------------------------------------
# Sections reach the WORKFLOWS, not just the list view
# ---------------------------------------------------------------------------

def test_sectioned_stocktake_snapshots_only_that_section():
    c, h, vid = _world()
    _ing(c, h, vid, "Chicken")                       # kitchen
    _ing(c, h, vid, "Lager kegs", section="cellar")

    r = c.post("/api/inventory/stocktake/start",
               json={"venue_id": vid, "section": " Cellar "}, headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["section"] == "cellar"
    assert r.json()["item_count"] == 1

    hist = c.get(f"/api/inventory/stocktakes?venue_id={vid}", headers=h).json()
    st = hist["stocktakes"][0]
    assert st["section"] == "cellar"
    assert [i["name"] for i in st["items"]] == ["Lager kegs"]


def test_different_sections_count_concurrently_same_section_blocks():
    """The chef counts the kitchen WHILE the bar manager counts the cellar."""
    c, h, vid = _world()
    _ing(c, h, vid, "Chicken")
    _ing(c, h, vid, "Lager kegs", section="cellar")

    assert c.post("/api/inventory/stocktake/start",
                  json={"venue_id": vid, "section": "kitchen"}, headers=h).status_code == 200
    # a different section may start alongside it
    assert c.post("/api/inventory/stocktake/start",
                  json={"venue_id": vid, "section": "cellar"}, headers=h).status_code == 200
    # ...but the same section can't be counted twice at once
    r = c.post("/api/inventory/stocktake/start",
               json={"venue_id": vid, "section": "cellar"}, headers=h)
    assert r.status_code == 409 and "cellar" in _err(r)
    # ...and a whole-venue count can't start over open section counts
    r = c.post("/api/inventory/stocktake/start", json={"venue_id": vid}, headers=h)
    assert r.status_code == 409


def test_whole_venue_stocktake_blocks_every_section():
    c, h, vid = _world()
    _ing(c, h, vid, "Chicken")
    assert c.post("/api/inventory/stocktake/start",
                  json={"venue_id": vid}, headers=h).status_code == 200
    r = c.post("/api/inventory/stocktake/start",
               json={"venue_id": vid, "section": "kitchen"}, headers=h)
    assert r.status_code == 409 and "whole-venue" in _err(r)


def test_empty_section_stocktake_is_a_clear_422():
    c, h, vid = _world()
    _ing(c, h, vid, "Chicken")   # kitchen only — bar is empty
    r = c.post("/api/inventory/stocktake/start",
               json={"venue_id": vid, "section": "bar"}, headers=h)
    assert r.status_code == 422 and "bar" in _err(r)


def test_sectioned_order_draft_ignores_other_sections_shortfalls():
    c, h, vid = _world()
    kid = _ing(c, h, vid, "Chicken")                        # kitchen, below par
    cid = _ing(c, h, vid, "Lager kegs", section="cellar")   # cellar, below par
    for iid in (kid, cid):
        assert c.post("/api/inventory/levels",
                      json={"venue_id": vid, "ingredient_id": iid,
                            "stock_qty": 1, "par_level": 10}, headers=h).status_code == 200

    r = c.post("/api/inventory/order/draft",
               json={"venue_id": vid, "section": "cellar"}, headers=h)
    assert r.status_code == 200, r.text

    def _drafted_names():
        rows = c.get(f"/api/inventory/orders?venue_id={vid}",
                     headers=h).json()["orders"]
        return [i["name"] for o in rows if o["status"] == "draft"
                for i in o["items"]]

    assert _drafted_names() == ["Lager kegs"]

    # the kitchen shortfall is still there for an unsectioned draft,
    # while the already-drafted cellar line isn't duplicated
    r2 = c.post("/api/inventory/order/draft", json={"venue_id": vid}, headers=h)
    assert r2.status_code == 200, r2.text
    assert sorted(_drafted_names()) == ["Chicken", "Lager kegs"]


def test_importer_maps_a_section_column():
    c, h, vid = _world()
    content = ("Name\tUnit\tPack Size\tCost\tSupplier\tDepartment\n"
               "Espresso beans\tkg\t5\t120\tBeanCo\t BAR \n"
               "Flour\tkg\t12.5\t18\tMillers\t\n")
    r = c.post("/api/setup/import-ingredients",
               json={"venue_id": vid, "content": content}, headers=h)
    assert r.status_code == 200, r.text
    created = {row["name"]: row for row in r.json()["created"]}
    assert created["Espresso beans"]["section"] == "bar"
    assert created["Flour"]["section"] == "kitchen"   # blank cell -> default

    items = {i["name"]: i["section"] for i in
             c.get(f"/api/inventory?venue_id={vid}", headers=h).json()["items"]}
    assert items["Espresso beans"] == "bar" and items["Flour"] == "kitchen"
