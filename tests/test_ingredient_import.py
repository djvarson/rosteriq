"""
Ingredient import from an export: header mapping, unit normalisation, $ cost
stripping, cost-per-unit derivation, no-header inference, dedup, honest skips,
and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"ii{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "II Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def test_import_ingredients_maps_and_normalises():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ii-venue-1")
    content = (
        "Product,UOM,Pack Size,Pack Cost,Supplier\n"
        "Chicken Breast,Kilogram,5,\"$50.00\",Acme Foods\n"
        "Olive Oil,Litre,4,32,Acme Foods\n"
        "Napkins,each,1000,25,Paper Co\n"
    )
    r = c.post("/api/setup/import-ingredients", json={"venue_id": vid, "content": content}, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["created_count"] == 3 and out["skipped_count"] == 0

    ings = {i["name"]: i for i in get_db().list_ingredients(vid)}
    assert ings["Chicken Breast"]["unit"] == "kg"          # Kilogram -> kg
    assert ings["Chicken Breast"]["cost_per_unit"] == 10.0  # $50 / 5kg
    assert ings["Chicken Breast"]["supplier"] == "Acme Foods"
    assert ings["Olive Oil"]["unit"] == "l"                # Litre -> l
    assert ings["Olive Oil"]["cost_per_unit"] == 8.0       # $32 / 4l


def test_import_ingredients_defaults_missing_cost_and_size():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ii-venue-def")
    content = "Name,Unit\nSalt,kg\nPepper,g\n"  # no size/cost
    out = c.post("/api/setup/import-ingredients", json={"venue_id": vid, "content": content}, headers=h).json()
    assert out["created_count"] == 2
    ings = {i["name"]: i for i in get_db().list_ingredients(vid)}
    assert ings["Salt"]["purchase_size"] == 1 and ings["Salt"]["purchase_cost"] == 0
    assert ings["Salt"]["cost_per_unit"] == 0


def test_import_ingredients_dedupes_and_skips():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ii-venue-dup")
    c.post("/api/setup/import-ingredients", json={
        "venue_id": vid, "content": "Name,Unit,Pack Size,Cost\nBeans,kg,1,10\n"}, headers=h)
    out = c.post("/api/setup/import-ingredients", json={
        "venue_id": vid,
        "content": "Name,Unit,Pack Size,Cost\nBeans,kg,1,10\n,kg,1,5\nRice,kg,10,20\n",
    }, headers=h).json()
    assert out["created_count"] == 1  # only Rice
    reasons = {s.get("reason") for s in out["skipped"]}
    assert "already exists" in reasons and "no name" in reasons
    assert len(get_db().list_ingredients(vid)) == 2


def test_import_ingredients_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ii-venue-scope")
    other = f"ii{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.post("/api/setup/import-ingredients", json={
        "venue_id": vid, "content": "Name\nX\n"}, headers=sh).status_code == 403
