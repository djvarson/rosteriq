"""
Preflight regressions: self-serve first-venue bootstrap, IsolationViolation
mapped to a clean 403 (never 500), and Deputy route venue scoping + the
unconfigured-OAuth 503.

These encode the 2026-07-31 production preflight findings so they can never
silently regress.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _make_staff(email, venue_ids=None):
    """Force a user record to non-owner (staff, no venues) — production's
    register default — regardless of the test-env bootstrap role."""
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = list(venue_ids or [])
    db.save_user(rec)


def test_first_venue_bootstrap():
    """A fresh staff user with NO venues can create their first venue and
    immediately operate it (the production 500 found in preflight)."""
    c = TestClient(app)
    email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _make_staff(email)  # production reality: new signups are staff with []

    vid = f"ob-venue-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={
        "id": vid, "name": "Bootstrap Cafe", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r.status_code == 200, r.text

    # The grant is durable: the next request (fresh context) can add staff
    r2 = c.post("/employees", json={
        "id": f"{vid}-emp", "name": "First Hire", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r2.status_code == 200, r2.text
    board = c.get(f"/api/clock/board?venue_id={vid}", headers=h)
    assert board.status_code == 200 and len(board.json()["staff"]) == 1

    # And the user record was promoted to manager of exactly this venue
    rec = get_db().get_user_by_email(email)
    assert rec["venue_ids"] == [vid] and rec["role"] == "manager"


def test_bootstrap_cannot_claim_existing_venue():
    """The bootstrap path must not allow hijacking an existing venue id."""
    c = TestClient(app)
    owner_email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    owner_h = _register_login(c, owner_email)
    vid = f"ob-owned-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={
        "id": vid, "name": "Someone's Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h).status_code == 200

    attacker_email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    a_h = _register_login(c, attacker_email)
    _make_staff(attacker_email)
    r = c.post("/venues", json={
        "id": vid, "name": "Hijack Attempt", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=a_h)
    assert r.status_code == 403
    # No access was granted
    rec = get_db().get_user_by_email(attacker_email)
    assert vid not in (rec.get("venue_ids") or [])


def test_isolation_violation_is_403_not_500():
    """Any path that raises IsolationViolation must surface as a clean 403."""
    c = TestClient(app)
    email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _make_staff(email, venue_ids=["some-other-venue"])

    # GET /venues/{id} goes through the tenant-scoped store's access check
    r = c.get("/venues/demo-venue-001", headers=h)
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text[:200]}"


def test_deputy_routes_are_venue_scoped():
    """Preflight W2: Deputy status/sync must refuse other venues' data."""
    c = TestClient(app)
    email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _make_staff(email, venue_ids=["mine-only"])

    assert c.get("/api/deputy/status?venue_id=demo-venue-001", headers=h).status_code == 403
    assert c.post("/api/deputy/sync/employees", json={
        "venue_id": "demo-venue-001"}, headers=h).status_code == 403
    assert c.post("/api/deputy/sync/shifts", json={
        "venue_id": "demo-venue-001", "start_date": "2026-08-01",
        "end_date": "2026-08-07"}, headers=h).status_code == 403


def test_deputy_oauth_unconfigured_is_503_with_guidance(monkeypatch):
    """Preflight W1: the OAuth connect button must fail with instructions for
    the token method, not a bare 500."""
    from rosteriq.routes import deputy as deputy_mod
    monkeypatch.setattr(deputy_mod, "DEPUTY_CLIENT_ID", "")
    monkeypatch.setattr(deputy_mod, "DEPUTY_CLIENT_SECRET", "")

    c = TestClient(app)
    email = f"ob{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _make_staff(email)
    vid = f"ob-dep-{uuid.uuid4().hex[:6]}"
    c.post("/venues", json={
        "id": vid, "name": "Deputy Cafe", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)

    r = c.post("/api/deputy/install", json={"venue_id": vid}, headers=h)
    assert r.status_code == 503
    body = r.json()
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "Access Token method" in message
