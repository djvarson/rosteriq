"""
MYOB bill push — a supplier invoice becomes a Service purchase bill, exactly once.

Mirror of test_xero_bills.py for the MYOB leg, with MYOB's own personality
pinned: everything is referenced by UID (supplier, account, tax code all get
resolved first, and an unknown supplier is created on the fly), amounts are
tax-inclusive line totals rather than qty x unit, and there is NO draft state
— the bill lands open, which is correct because a RosterIQ supplier invoice
is already the verified actual delivery. The one-push-per-invoice ledger
semantics are identical to Xero's: "already_pushed" replays with no second
API call, a MYOB rejection maps to 502 and burns nothing, and both routes
are venue-scoped.

routes/myob.py calls get_db() inside each handler, so the closure-rebinding
fixture test_xero_bills.py needs is NOT required here.
"""

import json
import uuid
from datetime import datetime, timedelta

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

CF_BASE = "https://api.myob.com/accountright/cf-test-0001"
SUPPLIER_URL = f"{CF_BASE}/Contact/Supplier"
ACCOUNT_URL = f"{CF_BASE}/GeneralLedger/Account"
TAXCODE_URL = f"{CF_BASE}/GeneralLedger/TaxCode"
BILL_URL = f"{CF_BASE}/Purchase/Bill/Service"

SUPPLIER_UID = "sup-uid-0001"
ACCOUNT_UID = "acct-uid-5-1000"
TAXCODE_UID = "tax-uid-gst"
BILL_UID = "bill-uid-0001"


# ----------------------------------------------------------------------------
# Helpers (house pattern, mirrored from test_xero_bills.py)
# ----------------------------------------------------------------------------

def _owner(c):
    email = f"mb{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "MYOB Bills Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def _ingredient(c, h, vid, name, unit="kg", size=5, cost=50, supplier="Acme Foods"):
    return c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": name, "unit": unit,
        "purchase_size": size, "purchase_cost": cost, "supplier": supplier,
    }, headers=h).json()["ingredient_id"]


def _seed_invoice(c, h, vid, invoice_number="INV-8001", supplier="Acme Foods"):
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


def _connect_myob(vid):
    """Seed the plugin_install record the way the install flow writes it;
    token far from expiry so no refresh traffic fires."""
    get_db().save_plugin_install({
        "organisation_id": f"myob_{vid}",
        "provider": "myob",
        "venue_id": vid,
        "status": "active",
        "installed_at": datetime.utcnow().isoformat(),
        "tokens": {
            "api_key": "test-api-key",
            "api_secret": "test-api-secret",
            "access_token": "test-access-token",
            "refresh_token": "test-refresh-token",
            "token_expires_at": (datetime.utcnow() + timedelta(hours=8)).isoformat(),
            "company_file_id": "cf-test-0001",
            "company_file_uri": CF_BASE,
            "product": "AccountRight",
        },
    })


def _mock_lookups(supplier_found=True):
    """Mount the three UID-resolution GETs. Returns the supplier GET route."""
    supplier_route = respx.get(SUPPLIER_URL).mock(
        return_value=httpx.Response(200, json={
            "Items": [{"UID": SUPPLIER_UID, "CompanyName": "Acme Foods"}] if supplier_found else [],
        })
    )
    respx.get(ACCOUNT_URL).mock(
        return_value=httpx.Response(200, json={
            "Items": [{"UID": ACCOUNT_UID, "DisplayID": "5-1000"}],
        })
    )
    respx.get(TAXCODE_URL).mock(
        return_value=httpx.Response(200, json={
            "Items": [{"UID": TAXCODE_UID, "Code": "GST"}],
        })
    )
    return supplier_route


def _mock_bill_ok():
    return respx.post(BILL_URL).mock(
        return_value=httpx.Response(200, json={
            "UID": BILL_UID, "Number": "00000042", "Status": "Open",
        })
    )


def _msg(resp):
    body = resp.json()
    return str(body.get("detail") or body.get("error", {}).get("message", "") or body)


# ----------------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------------

def test_push_without_connection_is_400_not_connected():
    """No MYOB install -> honest 400 pointing at the Connections page (parity
    with the Xero route's wording, NOT the older MYOB routes' 404)."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-nocred")
    inv_id = _seed_invoice(c, h, vid)

    r = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 400, r.text
    assert "not connected" in _msg(r).lower()


@respx.mock
def test_push_bill_happy_path_payload_and_response():
    """The bill MYOB receives is the invoice, line for line: tax-inclusive
    Service bill against the resolved supplier/account/taxcode UIDs, line
    totals as MYOB wants them, dated off the invoice, with a JournalMemo
    tracing back to the RosterIQ invoice."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-push")
    inv_id = _seed_invoice(c, h, vid)
    _connect_myob(vid)
    supplier_route = _mock_lookups()
    bill_route = _mock_bill_ok()

    r = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "pushed"
    assert out["invoice_id"] == inv_id
    assert out["myob_bill_uid"] == BILL_UID
    assert out["myob_bill_number"] == "00000042"
    assert out["myob_status"] == "Open"
    assert out["total"] == 240.0

    # Supplier resolved by name filter (with OData quoting), not created
    assert supplier_route.call_count == 1
    sfilter = supplier_route.calls.last.request.url.params.get("$filter")
    assert sfilter == "CompanyName eq 'Acme Foods'"

    assert bill_route.called
    assert "returnBody=true" in str(bill_route.calls.last.request.url)
    sent = json.loads(bill_route.calls.last.request.content)
    assert sent["Supplier"] == {"UID": SUPPLIER_UID}
    assert sent["Date"] == "2026-08-01"
    assert sent["SupplierInvoiceNumber"] == "INV-8001"
    assert sent["IsTaxInclusive"] is True
    assert inv_id in sent["JournalMemo"]

    lines = sent["Lines"]
    assert len(lines) == 2
    chicken_line, beans_line = lines                # invoice line order preserved
    assert "Chicken" in chicken_line["Description"]
    assert chicken_line["Total"] == 180.0           # 3 packs x $60
    assert "Beans" in beans_line["Description"]
    assert beans_line["Total"] == 60.0              # 2 packs x stored $30
    for line in lines:
        assert line["Type"] == "Transaction"
        assert line["Account"] == {"UID": ACCOUNT_UID}
        assert line["TaxCode"] == {"UID": TAXCODE_UID}


@respx.mock
def test_unknown_supplier_is_created_on_the_fly():
    """MYOB needs a supplier UID; when the name has no match the adapter
    creates the contact (returnBody=true) and bills against the new UID."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-newsup")
    inv_id = _seed_invoice(c, h, vid, invoice_number="INV-8002",
                           supplier="Brand New Farm Co")
    _connect_myob(vid)
    _mock_lookups(supplier_found=False)
    create_route = respx.post(SUPPLIER_URL).mock(
        return_value=httpx.Response(200, json={
            "UID": "sup-uid-created", "CompanyName": "Brand New Farm Co",
        })
    )
    bill_route = _mock_bill_ok()

    r = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 200, r.text

    assert create_route.called
    created = json.loads(create_route.calls.last.request.content)
    assert created == {"CompanyName": "Brand New Farm Co", "IsIndividual": False}
    assert "returnBody=true" in str(create_route.calls.last.request.url)

    sent = json.loads(bill_route.calls.last.request.content)
    assert sent["Supplier"] == {"UID": "sup-uid-created"}


@respx.mock
def test_second_push_replays_ledger_with_no_second_api_call():
    """One bill in MYOB, ever: the second push answers from the ledger."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-twice")
    inv_id = _seed_invoice(c, h, vid)
    _connect_myob(vid)
    _mock_lookups()
    bill_route = _mock_bill_ok()

    first = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                   headers=h)
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "pushed"

    second = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                    headers=h)
    assert second.status_code == 200, second.text
    out = second.json()
    assert out["status"] == "already_pushed"
    assert out["invoice_id"] == inv_id
    assert out["myob_bill_uid"] == BILL_UID
    assert out["myob_bill_number"] == "00000042"
    assert bill_route.call_count == 1


def test_push_unknown_invoice_is_404():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-ghost")
    _connect_myob(vid)                              # install present -> past the 400

    r = c.post("/api/myob/push-bill",
               json={"venue_id": vid, "invoice_id": "si-doesnotexist"}, headers=h)
    assert r.status_code == 404, r.text


@respx.mock
def test_myob_rejection_maps_to_502_and_does_not_burn_the_push():
    """A MYOB rejection surfaces as 502 with the reason — and the failed
    attempt is NOT recorded, so a retry still gets its one real push."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-reject")
    inv_id = _seed_invoice(c, h, vid)
    _connect_myob(vid)
    _mock_lookups()
    bill_route = respx.post(BILL_URL).mock(
        return_value=httpx.Response(400, json={
            "Errors": [{"Message": "Account 5-1000 is inactive"}]})
    )

    r = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
               headers=h)
    assert r.status_code == 502, r.text
    assert "myob rejected the bill" in _msg(r).lower()

    # Nothing landed in the ledger…
    listing = c.get(f"/api/myob/bill-pushes?venue_id={vid}", headers=h)
    assert listing.status_code == 200, listing.text
    assert listing.json()["count"] == 0

    # …so once MYOB accepts, the same invoice still gets its real push.
    bill_route.mock(return_value=httpx.Response(200, json={
        "UID": BILL_UID, "Number": "00000042", "Status": "Open"}))
    retry = c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                   headers=h)
    assert retry.status_code == 200, retry.text
    assert retry.json()["status"] == "pushed"


@respx.mock
def test_bill_pushes_listing_shows_the_ledger():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-list")
    inv_id = _seed_invoice(c, h, vid)
    _connect_myob(vid)
    _mock_lookups()
    _mock_bill_ok()

    before = c.get(f"/api/myob/bill-pushes?venue_id={vid}", headers=h)
    assert before.status_code == 200, before.text
    assert before.json() == {"venue_id": vid, "count": 0, "pushes": []}

    c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
           headers=h)

    after = c.get(f"/api/myob/bill-pushes?venue_id={vid}", headers=h).json()
    assert after["venue_id"] == vid and after["count"] == 1
    row = after["pushes"][0]
    assert row["invoice_id"] == inv_id
    assert row["venue_id"] == vid
    assert row["myob_bill_uid"] == BILL_UID
    assert row["myob_bill_number"] == "00000042"
    assert row["status"] == "pushed"
    assert isinstance(row["pushed_at"], str) and row["pushed_at"]


@respx.mock
def test_push_and_ledger_are_venue_scoped():
    """A user scoped to another venue can neither push nor read the ledger —
    and the denial happens before any MYOB traffic (no respx routes mounted,
    so a leaked outbound call would blow up loudly)."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "mb-venue-scope")
    inv_id = _seed_invoice(c, h, vid)
    _connect_myob(vid)

    other = f"mb{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.post("/api/myob/push-bill", json={"venue_id": vid, "invoice_id": inv_id},
                  headers=sh).status_code == 403
    assert c.get(f"/api/myob/bill-pushes?venue_id={vid}", headers=sh).status_code == 403
