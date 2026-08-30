"""
Waves 2-3 of the 2026-08-30 authz remediation: role gating across the app.

Proves that a linked role-`staff` user (a member of the venue) is now 403'd on
manager-level mutations, that a manager/owner of the venue is NOT blocked by the
gate, that manager-of-A cannot act on venue B, and that owner-only global admin
endpoints refuse a mere manager. Representative sample across the gate patterns
(body.venue_id, path {venue_id}, resource-id resolution, owner-only).
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

# Non-raising client: for "manager is allowed" checks the downstream service may
# error in the test env (no real integration), but the AUTH gate must not be
# what stops the request — so we assert the status is not 401/403.
nc = TestClient(app, raise_server_exceptions=False)


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _set_role(email, role, venue_ids):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)


def _manager_with_venue():
    """A fresh user who bootstraps a brand-new venue and thereby becomes its
    manager. Returns (headers, venue_id, email)."""
    c = TestClient(app)
    email = f"az{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "staff", [])
    vid = f"az-venue-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={
        "id": vid, "name": "Gate Test", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-08-30T00:00:00"}, headers=h)
    assert r.status_code == 200, r.text
    return h, vid, email


def _staff_at(vid):
    """A linked role-staff user who is a MEMBER of vid."""
    c = TestClient(app)
    email = f"az{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "staff", [vid])
    return h


def _owner():
    c = TestClient(app)
    email = f"az{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "owner", [])
    return h


# ---- representative manager endpoints: (method, path builder, json builder) ----

def _emp_body(vid):
    return {"id": f"{vid}-e1", "name": "X", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "venue_id": vid,
            "hourly_base_rate": "31.50", "created_at": "2026-08-30T00:00:00",
            "updated_at": "2026-08-30T00:00:00"}


MANAGER_CASES = [
    ("POST", "/employees", lambda vid: _emp_body(vid)),
    ("POST", "/rosters/generate", lambda vid: {"venue_id": vid, "week_start": "2026-09-01", "covers_per_staff": 8}),
    ("POST", "/forecasts", lambda vid: [{"id": f"{vid}-f1", "venue_id": vid, "date": "2026-09-01", "hour": 12, "predicted_covers": 20.0, "confidence": 0.8, "model_version": "test"}]),
    ("POST", "/api/keypay/install", lambda vid: {"venue_id": vid, "api_key": "keytoken12345", "business_id": "biz1"}),
    ("POST", "/api/inventory/levels", lambda vid: {"venue_id": vid, "ingredient_id": f"{vid}-i1", "stock_qty": 5.0}),
    ("POST", "/api/menu/ingredients", lambda vid: {"venue_id": vid, "name": "Flour", "purchase_size": 1000.0, "purchase_cost": 2.0}),
]


def test_staff_403_on_manager_endpoints():
    """A linked staff member of the venue is refused every manager mutation."""
    mgr_h, vid, _ = _manager_with_venue()
    staff_h = _staff_at(vid)
    c = TestClient(app)
    for method, path, body in MANAGER_CASES:
        r = c.request(method, path, json=body(vid), headers=staff_h)
        assert r.status_code == 403, f"{method} {path}: expected 403, got {r.status_code}: {r.text[:160]}"


def test_manager_not_blocked_on_manager_endpoints():
    """The venue's manager passes the gate (may hit downstream 400/404/200, but
    never 401/403)."""
    mgr_h, vid, _ = _manager_with_venue()
    for method, path, body in MANAGER_CASES:
        r = nc.request(method, path, json=body(vid), headers=mgr_h)
        assert r.status_code not in (401, 403), f"{method} {path}: manager blocked ({r.status_code}): {r.text[:160]}"


def test_theme_put_path_venue_gate():
    """Path-{venue_id} manager gate (theming)."""
    mgr_h, vid, _ = _manager_with_venue()
    staff_h = _staff_at(vid)
    c = TestClient(app)
    body = {"company_name": "Pwned"}
    assert c.put(f"/api/theme/{vid}", json=body, headers=staff_h).status_code == 403
    assert nc.put(f"/api/theme/{vid}", json=body, headers=mgr_h).status_code not in (401, 403)


def test_clock_pin_gate():
    """body.venue_id manager gate (timeclock kiosk PIN)."""
    mgr_h, vid, _ = _manager_with_venue()
    staff_h = _staff_at(vid)
    c = TestClient(app)
    body = {"venue_id": vid, "employee_id": f"{vid}-e1", "pin": "1234"}
    assert c.post("/api/clock/pin", json=body, headers=staff_h).status_code == 403


def test_manager_of_A_cannot_act_on_B():
    """Cross-venue: a manager of venue A is refused on venue B (membership half
    of the gate still holds)."""
    mgr_a, vid_a, _ = _manager_with_venue()
    _mgr_b, vid_b, _ = _manager_with_venue()
    c = TestClient(app)
    r = c.post("/rosters/generate", json={
        "venue_id": vid_b, "week_start": "2026-09-01", "covers_per_staff": 8}, headers=mgr_a)
    assert r.status_code == 403, f"manager of A reached venue B: {r.status_code}"


def test_owner_only_endpoints_refuse_manager():
    """Global admin endpoints require owner; a venue manager is refused, an
    owner is not blocked by the gate."""
    mgr_h, _vid, _ = _manager_with_venue()
    owner_h = _owner()
    c = TestClient(app)

    # (The DB pool-resize router only mounts when a real pool is configured, so
    # it isn't reachable in the test app; the enforce_owner() gate on it is
    # covered by inspection. A/B experiments exercise the same enforce_owner.)

    # A/B experiment creation (spans venues)
    exp = {"name": "e", "description": "d", "control_strategy": "cost_optimised",
           "variant_strategy": "balanced", "start_date": "2026-09-01", "end_date": "2026-09-30"}
    assert c.post("/api/experiments", json=exp, headers=mgr_h).status_code == 403
    assert nc.post("/api/experiments", json=exp, headers=owner_h).status_code not in (401, 403)


def test_demo_dashboard_user_is_manager_and_can_generate():
    """The public demo dashboard identity must be able to run its manager
    features (regression guard for the demo-role promotion)."""
    from rosteriq.services.demo import seed_demo_environment, DEMO_USER_ID, DEMO_VENUE_ID
    db = get_db()
    seed_demo_environment(db)
    assert db.get_user_by_id(DEMO_USER_ID)["role"] == "manager"
    # Mint the demo session and hit a manager endpoint.
    c = TestClient(app)
    tok = c.post("/api/auth/demo").json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    r = nc.post("/rosters/generate", json={
        "venue_id": DEMO_VENUE_ID, "week_start": "2026-09-01", "covers_per_staff": 8}, headers=h)
    assert r.status_code not in (401, 403), f"demo can't generate roster: {r.status_code}"
