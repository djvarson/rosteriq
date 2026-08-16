"""
Tenant scoping for the privacy (PII export / anonymise) and shift-bidding
routes.

Before this, both route families gated on ROLE only:

* /api/privacy/export/employee/{id} and /anonymise/employee/{id} loaded the
  employee by id and never checked that the caller could access the
  employee's venue — a venue-A manager could export / anonymise venue-B PII.
  The public "Try Demo" identity was likewise not refused.
* /api/bidding/shifts/{id} (bids), /assign, /eligible-employees etc. checked
  "is manager" only — a venue-A manager could read venue-B bids, auto-assign
  venue-B shifts, or list venue-B staff. list/post used any venue_id passed.

Now: owner passes; a venue-scoped manager/staff must have the resource's venue
in their venue_ids. By-id routes return 404 (not 403) on cross-venue so ids are
not an oracle; venue-query routes return 403.
"""

import uuid
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.demo import DEMO_USER_ID


PW = "Passw0rd!234"


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def _register(c, email):
    r = c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    assert r.status_code in (200, 201), r.text


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    body = r.json()
    tok = body.get("access_token") or body.get("tokens", {}).get("access_token")
    assert tok, r.text
    return {"Authorization": f"Bearer {tok}"}


def _user(c, role, venue_ids):
    """Register a user, force role + venue scope directly in the store, log in."""
    email = f"{role}_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)
    return _login(c, email)


def _venue(c, headers, vid):
    r = c.post("/venues", json={
        "id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code in (200, 201), r.text


def _employee(c, headers, eid, venue_id):
    r = c.post("/employees", json={
        "id": eid, "name": f"Emp {eid}", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": venue_id,
        "hourly_base_rate": "30.00", "email": f"{eid}@x.com", "skills": ["bar"],
        "created_at": "2026-06-20T00:00:00", "updated_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code in (200, 201), r.text


def _err(r):
    body = r.json()
    return body.get("detail") or body.get("error", {}).get("message")


def _shift_payload():
    d = date.today() + timedelta(days=14)
    return {
        "date": d.isoformat(),
        "start_time": "10:00:00",
        "end_time": "16:00:00",
        "role_required": "bar",
        "skills_required": ["bar"],
        "min_rate": "30.00",
    }


# ----------------------------------------------------------------------------
# fixture: two venues, one owner, one manager per venue, one employee per venue,
# one open shift per venue (posted by that venue's manager via the API)
# ----------------------------------------------------------------------------

@pytest.fixture
def world():
    # Function-scoped: conftest's autouse reset_global_db wipes the in-memory
    # store between tests, so the world is rebuilt per test.
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    ven_a, ven_b = f"tsA_{tag}", f"tsB_{tag}"

    owner = _user(c, "owner", [])
    _venue(c, owner, ven_a)
    _venue(c, owner, ven_b)

    emp_a, emp_b = f"empA_{tag}", f"empB_{tag}"
    emp_a2 = f"empA2_{tag}"  # dedicated to the anonymise test (it mutates)
    _employee(c, owner, emp_a, ven_a)
    _employee(c, owner, emp_a2, ven_a)
    _employee(c, owner, emp_b, ven_b)

    mgr_a = _user(c, "manager", [ven_a])
    mgr_b = _user(c, "manager", [ven_b])

    # Same-venue manager can post (proves post_open_shift still works in-scope).
    ra = c.post("/api/bidding/shifts", params={"venue_id": ven_a},
                json=_shift_payload(), headers=mgr_a)
    assert ra.status_code == 201, ra.text
    rb = c.post("/api/bidding/shifts", params={"venue_id": ven_b},
                json=_shift_payload(), headers=mgr_b)
    assert rb.status_code == 201, rb.text

    return {
        "c": c, "ven_a": ven_a, "ven_b": ven_b,
        "owner": owner, "mgr_a": mgr_a, "mgr_b": mgr_b,
        "emp_a": emp_a, "emp_a2": emp_a2, "emp_b": emp_b,
        "shift_a": ra.json()["id"], "shift_b": rb.json()["id"],
    }


# ============================================================================
# PRIVACY: export
# ============================================================================

def test_export_cross_venue_manager_gets_404(world):
    c = world["c"]
    r = c.post(f"/api/privacy/export/employee/{world['emp_b']}", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    # Same body as a truly-missing employee: id is not an oracle.
    r2 = c.post("/api/privacy/export/employee/does-not-exist", headers=world["mgr_a"])
    assert r2.status_code == 404
    assert _err(r) == _err(r2)


def test_export_same_venue_manager_ok(world):
    c = world["c"]
    r = c.post(f"/api/privacy/export/employee/{world['emp_a']}", headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    assert r.json()["employee"]["id"] == world["emp_a"]


def test_export_owner_passes_any_venue(world):
    c = world["c"]
    for eid in (world["emp_a"], world["emp_b"]):
        r = c.post(f"/api/privacy/export/employee/{eid}", headers=world["owner"])
        assert r.status_code == 200, r.text
        assert r.json()["employee"]["id"] == eid


def test_export_demo_identity_403(world):
    c = world["c"]
    tok = c.post("/api/auth/demo").json()["access_token"]
    demo = {"Authorization": f"Bearer {tok}"}
    # Sanity: this really is the demo identity.
    assert get_db().get_user_by_id(DEMO_USER_ID) is not None
    r = c.post(f"/api/privacy/export/employee/{world['emp_a']}", headers=demo)
    assert r.status_code == 403, r.text
    assert "demo" in (_err(r) or "").lower()
    # Anonymise is refused for the demo identity too.
    r = c.post(f"/api/privacy/anonymise/employee/{world['emp_a']}", headers=demo)
    assert r.status_code == 403, r.text
    assert "demo" in (_err(r) or "").lower()


# ============================================================================
# PRIVACY: anonymise
# ============================================================================

def test_anonymise_cross_venue_manager_gets_404(world):
    c = world["c"]
    r = c.post(f"/api/privacy/anonymise/employee/{world['emp_b']}", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    # Untouched: venue-B PII still intact.
    emp = get_db().get_employee(world["emp_b"])
    assert emp.name == f"Emp {world['emp_b']}"


def test_anonymise_same_venue_manager_still_needs_owner(world):
    # Scope passes (not 404); the operation itself stays owner-only (403).
    c = world["c"]
    r = c.post(f"/api/privacy/anonymise/employee/{world['emp_a2']}", headers=world["mgr_a"])
    assert r.status_code == 403, r.text
    assert "owner" in (_err(r) or "").lower()


def test_anonymise_owner_ok(world):
    c = world["c"]
    r = c.post(f"/api/privacy/anonymise/employee/{world['emp_a2']}", headers=world["owner"])
    assert r.status_code == 200, r.text
    assert r.json()["success"] is True
    assert r.json()["employee_id"] == world["emp_a2"]


# ============================================================================
# BIDDING: venue-query routes (list / post) -> 403 cross-venue
# ============================================================================

def test_bidding_list_cross_venue_403_same_venue_ok_owner_ok(world):
    c = world["c"]
    assert c.get("/api/bidding/shifts", params={"venue_id": world["ven_b"]},
                 headers=world["mgr_a"]).status_code == 403
    r = c.get("/api/bidding/shifts", params={"venue_id": world["ven_a"]}, headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    assert {s["id"] for s in r.json()} == {world["shift_a"]}
    r = c.get("/api/bidding/shifts", params={"venue_id": world["ven_b"]}, headers=world["owner"])
    assert r.status_code == 200, r.text
    assert {s["id"] for s in r.json()} == {world["shift_b"]}


def test_bidding_post_cross_venue_403(world):
    c = world["c"]
    r = c.post("/api/bidding/shifts", params={"venue_id": world["ven_b"]},
               json=_shift_payload(), headers=world["mgr_a"])
    assert r.status_code == 403, r.text
    # Nothing was created in venue B.
    r = c.get("/api/bidding/shifts", params={"venue_id": world["ven_b"]}, headers=world["owner"])
    assert {s["id"] for s in r.json()} == {world["shift_b"]}


# ============================================================================
# BIDDING: by-id manager routes -> 404 cross-venue (no id oracle)
# ============================================================================

def test_bids_view_cross_venue_404_same_venue_ok(world):
    c = world["c"]
    r = c.get(f"/api/bidding/shifts/{world['shift_b']}", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    r_missing = c.get("/api/bidding/shifts/nope", headers=world["mgr_a"])
    assert r_missing.status_code == 404
    assert _err(r) == _err(r_missing)

    r = c.get(f"/api/bidding/shifts/{world['shift_a']}", headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    assert r.json()["shift"]["id"] == world["shift_a"]
    assert r.json()["bid_count"] == 0


def test_auto_assign_cross_venue_404_same_venue_reaches_service(world):
    c = world["c"]
    r = c.post(f"/api/bidding/shifts/{world['shift_b']}/assign", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    # Same venue passes scope; no bids yet -> the service's 400, not a 404/403.
    r = c.post(f"/api/bidding/shifts/{world['shift_a']}/assign", headers=world["mgr_a"])
    assert r.status_code == 400, r.text


def test_award_and_cancel_cross_venue_404(world):
    c = world["c"]
    r = c.post(f"/api/bidding/shifts/{world['shift_b']}/award/some-bid", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    r = c.delete(f"/api/bidding/shifts/{world['shift_b']}", headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    # Venue-B shift is still open (cancel did not happen).
    r = c.get("/api/bidding/shifts", params={"venue_id": world["ven_b"]}, headers=world["owner"])
    assert {s["id"] for s in r.json()} == {world["shift_b"]}


def test_eligible_cross_venue_404_same_venue_only_own_staff(world):
    c = world["c"]
    r = c.get(f"/api/bidding/shifts/{world['shift_b']}/eligible-employees", headers=world["mgr_a"])
    assert r.status_code == 404, r.text

    r = c.get(f"/api/bidding/shifts/{world['shift_a']}/eligible-employees", headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    ids = {e["id"] for e in r.json()}
    # Service-level venue filter: venue-A staff only, never venue-B staff.
    assert world["emp_a"] in ids
    assert world["emp_b"] not in ids
    assert all(get_db().get_employee(i).venue_id == world["ven_a"] for i in ids)


def test_owner_passes_tenant_scope_on_by_id_routes(world):
    # Owner is not blocked by scope: it reaches the manager-role gate (403 with
    # the role message), never the 404 a cross-venue caller gets.
    c = world["c"]
    r = c.get(f"/api/bidding/shifts/{world['shift_b']}", headers=world["owner"])
    assert r.status_code == 403, r.text
    assert "manager" in (_err(r) or "").lower()


def test_staff_cannot_bid_on_other_venue_shift(world):
    c = world["c"]
    staff_a = _user(c, "staff", [world["ven_a"]])
    r = c.post("/api/bidding/bids", params={"open_shift_id": world["shift_b"]},
               json={"offered_rate": "35.00"}, headers=staff_a)
    assert r.status_code == 404, r.text
