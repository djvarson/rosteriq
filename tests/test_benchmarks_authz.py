"""
Cross-tenant read holes in the venue-benchmarks endpoints (found 2026-10-06 by
the residual-authz audit, missed by the earlier tenant-scope sweeps).

routes/benchmarks.py had NO authorization on any handler: any authenticated
single-venue user could read every other tenant's confidential financials
(labour % of revenue, cost per cover, compliance score) via compare / rankings
/ insights / {venue_id}/industry / {venue_id}/efficiency. Now each handler
scopes to the caller's venues (owner = all).
"""

import uuid
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

BASE = "/api/v1/analytics/venue-benchmarks"
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
    c = TestClient(app)
    email = f"bm{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "staff", [])
    vid = f"bm-venue-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={
        "id": vid, "name": "BM", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-10-06T00:00:00"}, headers=h).status_code == 200
    return h, vid


def _owner():
    c = TestClient(app)
    email = f"bm{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "owner", [])
    return h


def test_cannot_read_other_tenants_benchmarks():
    """Manager of venue A is 403'd on venue B's benchmark reads."""
    mgr_a, _vid_a = _manager_with_venue()
    _mgr_b, vid_b = _manager_with_venue()
    c = TestClient(app)

    assert c.get(f"{BASE}/{vid_b}/industry", headers=mgr_a).status_code == 403
    assert c.get(f"{BASE}/{vid_b}/efficiency", headers=mgr_a).status_code == 403
    assert c.post(f"{BASE}/compare", params={"venue_ids": [vid_b]}, headers=mgr_a).status_code == 403
    assert c.post(f"{BASE}/insights", params={"venue_ids": [vid_b]}, headers=mgr_a).status_code == 403


def test_rankings_scoped_not_platform_wide():
    """A non-owner's rankings never include another tenant's venue, even with no
    venue_ids filter (previously returned every venue platform-wide)."""
    mgr_a, vid_a = _manager_with_venue()
    _mgr_b, vid_b = _manager_with_venue()
    c = TestClient(app)
    r = c.get(f"{BASE}/rankings", params={"metric": "labour_pct_of_revenue"}, headers=mgr_a)
    assert r.status_code == 200, r.text
    assert vid_b not in r.text, "ranking leaked another tenant's venue"


def test_own_venue_reads_not_blocked():
    """The caller's own venue is reachable (gate allows membership)."""
    mgr_a, vid_a = _manager_with_venue()
    assert nc.get(f"{BASE}/{vid_a}/industry", headers=mgr_a).status_code not in (401, 403)
    assert nc.get(f"{BASE}/{vid_a}/efficiency", headers=mgr_a).status_code not in (401, 403)
    assert nc.post(f"{BASE}/compare", params={"venue_ids": [vid_a]}, headers=mgr_a).status_code not in (401, 403)


def test_owner_sees_any_venue():
    """Platform owner is not blocked (owner bypasses venue scope)."""
    _mgr, vid = _manager_with_venue()
    owner_h = _owner()
    assert nc.get(f"{BASE}/{vid}/industry", headers=owner_h).status_code not in (401, 403)


def test_unauthenticated_rejected():
    c = TestClient(app)
    assert c.get(f"{BASE}/some-venue/industry").status_code == 401
