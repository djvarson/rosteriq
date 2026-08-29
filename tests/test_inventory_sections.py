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
