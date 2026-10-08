"""
Cross-tenant READ holes found 2026-10-06 by the residual-authz audit (same
class as the venue-benchmarks cluster, missed by the earlier sweeps): venue-id
reads that returned another tenant's data with no membership check.

- POS live revenue/hourly/variance (routes/pos_realtime.py) — financial
- Onboarding status/summary (routes/onboarding.py)
- Revenue budget-check (routes/revenue.py)
- Direct-bookings signals (routes/direct_bookings.py) — also ingests

Each now calls enforce_venue_access(venue_id) first, so a member of one venue
is 403'd on another tenant's data (owner still passes).
"""

import uuid
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


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
    c = TestClient(app)
    email = f"ct{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "staff", [])
    vid = f"ct-venue-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={
        "id": vid, "name": "CT", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-10-06T00:00:00"}, headers=h).status_code == 200
    return h, vid


def test_other_tenant_reads_are_403():
    """Manager of venue A is 403'd reading venue B across the found endpoints."""
    mgr_a, _vid_a = _manager_with_venue()
    _mgr_b, vid_b = _manager_with_venue()
    c = TestClient(app)

    # POS live revenue (financial) — and its hourly/variance siblings
    assert c.get(f"/api/pos/live/revenue/{vid_b}", headers=mgr_a).status_code == 403
    assert c.get(f"/api/pos/live/hourly/{vid_b}", headers=mgr_a).status_code == 403
    assert c.get(f"/api/pos/live/variance/{vid_b}", headers=mgr_a).status_code == 403

    # Onboarding status/summary
    assert c.get(f"/api/onboarding/status/{vid_b}", headers=mgr_a).status_code == 403
    assert c.get(f"/api/onboarding/summary/{vid_b}", headers=mgr_a).status_code == 403

    # Revenue budget-check (gate fires before the roster lookup)
    assert c.post("/api/revenue/budget-check", json={
        "venue_id": vid_b, "roster_id": "x", "target_labour_pct": 30}, headers=mgr_a).status_code == 403

    # Direct-bookings signals (also ingests)
    assert c.get("/api/reservations/direct/signals", params={
        "venue_id": vid_b, "start": "2026-10-01", "end": "2026-10-07"}, headers=mgr_a).status_code == 403


def test_unauthenticated_rejected():
    c = TestClient(app)
    assert c.get("/api/pos/live/revenue/some-venue").status_code == 401
    assert c.get("/api/onboarding/status/some-venue").status_code == 401
