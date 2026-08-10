"""
Xero bill push — a supplier invoice becomes an ACCPAY draft bill, exactly once.

Supplier invoices are immutable once entered (re-saving one is a silent
no-op), so the push ledger is the only record of what reached Xero. These
tests pin the one-push-per-invoice guarantee end to end: the exact ACCPAY
payload Xero receives, the "already_pushed" replay answer with NO second API
call, the honest 400 when Xero isn't connected, the 502 envelope when Xero
rejects the bill (which must NOT burn the invoice's single push), and venue
scoping on both the push and the ledger listing.
"""

import asyncio
import json
import uuid
from datetime import datetime, timedelta

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.xero_integration import XeroCredentials, save_xero_credentials

XERO_INVOICES_URL = "https://api.xero.com/api.xro/2.0/Invoices"


def _walk_routes(routes, depth=0):
    """Yield every route, recursing into routers (newer FastAPI keeps included
    routers nested as _IncludedRouter instead of flattening them)."""
    if depth > 4:
        return
    for r in routes:
        yield r
        for sub in (getattr(r, "routes", None),
                    getattr(getattr(r, "original_router", None), "routes", None)):
            if sub:
                yield from _walk_routes(sub, depth + 1)


@pytest.fixture(autouse=True)
def _bind_xero_routes_to_fresh_store(reset_global_db):
    """
    setup_xero_routes(app, get_db()) closed over the store that existed at
    import time; conftest's reset_db() builds a fresh store per test, so the
    /api/xero handlers' captured ``db`` goes stale (same class of trap as the
    auth-service re-bind in conftest). Re-point every /api/xero closure cell
    named ``db`` at the current store so the routes, the seeding helpers and
    the assertions all share one store. Harmless if a handler calls get_db()
    itself (no matching free variable -> nothing to re-point).
    """
    fresh = get_db()
    for route in _walk_routes(app.routes):
        if not str(getattr(route, "path", "")).startswith("/api/xero"):
            continue
        fn = getattr(route, "endpoint", None)
        code = getattr(fn, "__code__", None)
        cells = getattr(fn, "__closure__", None)
        if not code or not cells:
            continue
        for name, cell in zip(code.co_freevars, cells):
            if name == "db":
                cell.cell_contents = fresh
    yield


# ----------------------------------------------------------------------------
# Helpers (house pattern: owner via real register/login, venue via /venues,
# invoice seeded through the real inventory API so the record shape is real)
# ----------------------------------------------------------------------------

def _owner(c):
    email = f"xb{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "Xero Bills Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _ingredient(c, h, vid, name, unit="kg", size=5, cost=50, supplier="Acme Foods"):
    return c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": name, "unit": unit,
        "purchase_size": size, "purchase_cost": cost, "supplier": supplier,
    }, headers=h).json()["ingredient_id"]


def _seed_invoice(c, h, vid, invoice_number="INV-7001", supplier="Acme Foods"):
    """Chicken 3 packs @ $60 + Beans 2 packs @ stored $30 = $240 total."""
    chicken = _ingredient(c, h, vid, "Chicken")               # 5kg packs @ $50
    beans = _ingredient(c, h, vid, "Beans", size=1, cost=30)  # 1kg packs @ $30
    r = c.post("/api/inventory/invoice", json={
        "venue_id": vid, "supplier": supplier, "invoice_number": invoice_number,
        "invoice_date": "2026-08-01",
        "lines": [
            {"ingredient_id": chicken, "packs": 3, "pack_cost": 60},
            {"ingredient_id": beans, "packs": 2},
        ],
    }, headers=h)
    assert r.status_code == 200, r.text
    return r.json()["invoice_id"]


def _connect_xero(vid):
    """Seed credentials directly; token_expires far out so no refresh fires."""
    now = datetime.utcnow()
    creds = XeroCredentials(
        venue_id=vid,
        client_id="cid",
        client_secret="sec",
        tenant_id=str(uuid.uuid4()),
        access_token="tok",
        refresh_token="ref",
        token_expires=now + timedelta(hours=8),
        created_at=now,
        updated_at=now,
    )
    asyncio.run(save_xero_credentials(get_db(), creds))


def _mock_xero_ok():
    return respx.post(XERO_INVOICES_URL).mock(
        return_value=httpx.Response(200, json={"Invoices": [{
            "InvoiceID": "xero-uuid-1", "InvoiceNumber": "INV-001", "Status": "DRAFT",
        }]})
    )


def _msg(resp):
    body = resp.json()
    return str(body.get("detail") or body.get("error", {}).get("message", "") or body)


# ----------------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------------

def test_push_without_credentials_is_400_not_connected():
    """No Xero connection -> honest 400 pointing at the Connections page,
    before any invoice lookup or network traffic."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-nocred")
    inv_id = _seed_invoice(c, h, vid)

    r = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 400, r.text
    assert "not connected" in _msg(r).lower()


@respx.mock
def test_push_bill_happy_path_payload_and_response():
    """The bill Xero receives is the invoice, line for line: ACCPAY draft,
    GST-inclusive, supplier as contact, packs/pack-cost as quantity/unit
    amount on the caller's account code, dated off the invoice with 14-day
    terms, and a reference that traces back to the RosterIQ invoice."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-push")
    inv_id = _seed_invoice(c, h, vid)
    _connect_xero(vid)
    route = _mock_xero_ok()

    r = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "pushed"
    assert out["invoice_id"] == inv_id
    assert out["xero_invoice_id"] == "xero-uuid-1"
    assert out["xero_invoice_number"] == "INV-001"
    assert out["xero_status"] == "DRAFT"
    assert out["total"] == 240.0

    assert route.called
    sent = json.loads(route.calls.last.request.content)
    bill = sent["Invoices"][0]
    assert bill["Type"] == "ACCPAY"
    assert bill["Status"] == "DRAFT"
    assert bill["LineAmountTypes"] == "Inclusive"
    assert bill["Contact"]["Name"] == "Acme Foods"
    assert bill["InvoiceNumber"] == "INV-7001"
    assert inv_id in bill["Reference"]
    assert bill["Date"] == "2026-08-01"
    assert bill["DueDate"] == "2026-08-15"          # invoice date + 14 days

    lines = bill["LineItems"]
    assert len(lines) == 2
    chicken_line, beans_line = lines                # invoice line order preserved
    assert "Chicken" in chicken_line["Description"]
    assert chicken_line["Quantity"] == 3.0
    assert chicken_line["UnitAmount"] == 60.0
    assert "Beans" in beans_line["Description"]
    assert beans_line["Quantity"] == 2.0
    assert beans_line["UnitAmount"] == 30.0         # stored price, no override
    for line in lines:
        assert line["AccountCode"] == "300"
        assert line["TaxType"] == "INPUT"


@respx.mock
def test_second_push_replays_ledger_with_no_second_api_call():
    """The ledger's whole reason to exist: pushing the same invoice twice
    answers 'already_pushed' from the ledger and never re-calls Xero."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-twice")
    inv_id = _seed_invoice(c, h, vid)
    _connect_xero(vid)
    route = _mock_xero_ok()

    first = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                   headers=h)
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "pushed"

    second = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                    headers=h)
    assert second.status_code == 200, second.text
    out = second.json()
    assert out["status"] == "already_pushed"
    assert out["invoice_id"] == inv_id
    assert out["xero_invoice_id"] == "xero-uuid-1"
    assert out["xero_invoice_number"] == "INV-001"
    assert route.call_count == 1                    # ONE bill in Xero, ever


def test_push_unknown_invoice_is_404():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-ghost")
    _connect_xero(vid)                              # creds present -> past the 400

    r = c.post("/api/xero/push-bill",
               json={"venue_id": vid, "invoice_id": "si-doesnotexist"}, headers=h)
    assert r.status_code == 404, r.text


@respx.mock
def test_xero_rejection_maps_to_502_and_does_not_burn_the_push():
    """Xero refusing the bill surfaces as a 502 with Xero's reason — and the
    failed attempt must NOT be recorded, so a retry after fixing the problem
    still gets its one real push."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-reject")
    inv_id = _seed_invoice(c, h, vid)
    _connect_xero(vid)
    route = respx.post(XERO_INVOICES_URL).mock(
        return_value=httpx.Response(400, json={
            "Message": "Account code '300' has been archived"})
    )

    r = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 502, r.text
    assert "xero rejected the bill" in _msg(r).lower()

    # Nothing landed in the ledger…
    listing = c.get(f"/api/xero/bill-pushes?venue_id={vid}", headers=h)
    assert listing.status_code == 200, listing.text
    assert listing.json()["count"] == 0

    # …so once Xero accepts, the same invoice still gets its real push.
    route.mock(return_value=httpx.Response(200, json={"Invoices": [{
        "InvoiceID": "xero-uuid-1", "InvoiceNumber": "INV-001", "Status": "DRAFT"}]}))
    retry = c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                   headers=h)
    assert retry.status_code == 200, retry.text
    assert retry.json()["status"] == "pushed"


@respx.mock
def test_bill_pushes_listing_shows_the_ledger():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-list")
    inv_id = _seed_invoice(c, h, vid)
    _connect_xero(vid)
    _mock_xero_ok()

    # Empty before any push
    before = c.get(f"/api/xero/bill-pushes?venue_id={vid}", headers=h)
    assert before.status_code == 200, before.text
    assert before.json() == {"venue_id": vid, "count": 0, "pushes": []}

    c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
           headers=h)

    after = c.get(f"/api/xero/bill-pushes?venue_id={vid}", headers=h).json()
    assert after["venue_id"] == vid and after["count"] == 1
    row = after["pushes"][0]
    assert row["invoice_id"] == inv_id
    assert row["venue_id"] == vid
    assert row["xero_invoice_id"] == "xero-uuid-1"
    assert row["xero_invoice_number"] == "INV-001"
    assert row["status"] == "pushed"
    assert isinstance(row["pushed_at"], str) and row["pushed_at"]


@respx.mock
def test_push_and_ledger_are_venue_scoped():
    """A user scoped to another venue can neither push the invoice nor read
    the ledger — and the denial happens before any Xero traffic (no respx
    route is mounted, so a leaked outbound call would blow up loudly)."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "xb-venue-scope")
    inv_id = _seed_invoice(c, h, vid)
    _connect_xero(vid)

    other = f"xb{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.post("/api/xero/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                  headers=sh).status_code == 403
    assert c.get(f"/api/xero/bill-pushes?venue_id={vid}", headers=sh).status_code == 403
