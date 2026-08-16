"""
Kitchen, comms and integration actions leave an audit trail — including the
ones that FAIL (a refused Xero push is recorded with outcome=failed so a
manager can see it happened, and nothing else is written).
"""

import asyncio
import uuid
from datetime import datetime, timedelta

import httpx
import respx
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.xero_integration import XeroCredentials, save_xero_credentials
from rosteriq.tests.test_xero_bills import _bind_xero_routes_to_fresh_store  # noqa: F401 — autouse fixture: xero routes close over the store

XERO_URL = "https://api.xero.com/api.xro/2.0/Invoices"


def _owner(c):
    email = f"ek{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={"id": vid, "name": "EK", "state": "wa", "max_labour_pct": 30,
                            "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)


def _rows(vid, prefix):
    return get_db().list_events(venue_id=vid, action_prefix=prefix)


def _connect_xero(vid):
    now = datetime.utcnow()
    asyncio.run(save_xero_credentials(get_db(), XeroCredentials(
        venue_id=vid, client_id="c", client_secret="s", tenant_id=str(uuid.uuid4()),
        access_token="t", refresh_token="r", token_expires=now + timedelta(hours=8),
        created_at=now, updated_at=now)))


def test_kitchen_actions_are_audited():
    c = TestClient(app); h = _owner(c); vid = "ek-kitchen"; _venue(c, h, vid)
    ing = c.post("/api/menu/ingredients", json={"venue_id": vid, "name": "Chicken", "unit": "kg",
                                                "purchase_size": 5, "purchase_cost": 50, "supplier": "Acme"},
                 headers=h).json()["ingredient_id"]
    r = c.post("/api/inventory/invoice", json={"venue_id": vid, "supplier": "Acme", "invoice_number": "INV-9",
                                              "lines": [{"ingredient_id": ing, "packs": 2, "pack_cost": 55}]}, headers=h)
    assert r.status_code == 200, r.text
    inv = _rows(vid, "invoice.enter"); assert inv and inv[0]["details"]["invoice_number"] == "INV-9"
    assert inv[0]["details"]["category"] == "audit" and inv[0]["details"]["outcome"] == "ok"
    pc = _rows(vid, "ingredient.price_change"); assert pc
    d = pc[0]["details"]; assert d["old_purchase_cost"] == 50 and d["new_purchase_cost"] == 55 and d["name"] == "Chicken"
    r = c.post("/api/inventory/waste", json={"venue_id": vid, "ingredient_id": ing, "qty": 1, "unit": "kg",
                                            "reason": "spoiled"}, headers=h)
    assert r.status_code == 200, r.text
    assert _rows(vid, "waste.log")[0]["details"]["reason"] == "spoiled"
    st = c.post("/api/inventory/stocktake/start", json={"venue_id": vid}, headers=h)
    assert st.status_code == 200, st.text
    assert _rows(vid, "stocktake.start")


def test_comms_and_sops_are_audited():
    c = TestClient(app); h = _owner(c); vid = "ek-comms"; _venue(c, h, vid)
    r = c.post("/api/announcements", json={"venue_id": vid, "title": "Hello", "body": "team", "pinned": False,
                                           "send_sms": False}, headers=h)
    assert r.status_code == 200, r.text
    assert _rows(vid, "announcement.publish")[0]["details"]["title"] == "Hello"
    r = c.post("/api/sops/documents", json={"venue_id": vid, "title": "Glass", "category": "sop", "body": "sweep"}, headers=h)
    doc = r.json()["document_id"]
    assert _rows(vid, "sop.publish")[0]["details"]["title"] == "Glass"
    c.put(f"/api/sops/documents/{doc}", json={"body": "sweep, never hand-pick"}, headers=h)
    up = _rows(vid, "sop.update"); assert up and up[0]["details"].get("version_bumped") is True
    c.put(f"/api/sops/documents/{doc}", json={"active": False}, headers=h)
    assert _rows(vid, "sop.retire")
    p = c.post("/api/feed/posts", json={"venue_id": vid, "body": "hi"}, headers=h).json()["id"]
    c.post(f"/api/feed/posts/{p}/pin", json={"pinned": True}, headers=h)
    assert _rows(vid, "feed.pin")
    c.delete(f"/api/feed/posts/{p}", headers=h)
    assert _rows(vid, "feed.remove")


@respx.mock
def test_xero_push_records_ok_and_failed_outcomes():
    c = TestClient(app); h = _owner(c); vid = "ek-xero"; _venue(c, h, vid)
    ing = c.post("/api/menu/ingredients", json={"venue_id": vid, "name": "Beans", "unit": "kg",
                                                "purchase_size": 1, "purchase_cost": 30, "supplier": "Acme"},
                 headers=h).json()["ingredient_id"]
    inv_id = c.post("/api/inventory/invoice", json={"venue_id": vid, "supplier": "Acme", "invoice_number": "X-1",
                                                   "lines": [{"ingredient_id": ing, "packs": 1}]}, headers=h).json()["invoice_id"]
    _connect_xero(vid)
    route = respx.post(XERO_URL).mock(return_value=httpx.Response(400, json={"Message": "Account archived"}))
    r = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id}, headers=h)
    assert r.status_code == 502
    failed = [e for e in _rows(vid, "xero.bill_push") if e["details"]["outcome"] == "failed"]
    assert failed, "a refused push must be recorded"
    assert "archived" in str(failed[0]["details"]).lower()
    route.mock(return_value=httpx.Response(200, json={"Invoices": [{"InvoiceID": "x1", "InvoiceNumber": "X-1", "Status": "DRAFT"}]}))
    r = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id}, headers=h)
    assert r.status_code == 200, r.text
    ok = [e for e in _rows(vid, "xero.bill_push") if e["details"]["outcome"] == "ok"]
    assert ok and ok[0]["resource_id"] == inv_id
    # secrets never in details
    for e in _rows(vid, "xero."):
        assert "access_token" not in str(e["details"]) or "[redacted]" in str(e["details"])
