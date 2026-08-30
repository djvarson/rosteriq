"""
Wave 1 of the 2026-08-30 authz remediation: the webhook surface.

Root cause: WEBHOOK_EXEMPT / WEBHOOK_PATHS matched the "/api/webhooks" prefix,
so every admin route mounted under it (queue management, register/deregister,
the outbound-webhook manager) was reachable with NO JWT at all. The exemption
is now an exact-path allowlist of the one true public receiver
(/api/webhooks/tanda); admin routes require auth + the right role.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _set_role(email, role, venue_ids=None):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    if venue_ids is not None:
        rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)


# ---------------------------------------------------------------- Tier 0: unauth

def test_webhook_queue_purge_requires_auth():
    """The dead-letter purge was reachable unauthenticated via the prefix hole."""
    c = TestClient(app)
    assert c.post("/api/webhooks/queue/purge").status_code == 401


def test_webhook_register_requires_auth():
    c = TestClient(app)
    r = c.post("/api/webhooks/register",
               params={"venue_id": "v1", "event_type": "roster.published",
                       "endpoint_url": "https://x.example/hook"})
    assert r.status_code == 401


def test_outbound_subscribe_requires_auth():
    c = TestClient(app)
    r = c.post("/api/webhooks/subscribe", json={
        "venue_id": "v1", "callback_url": "https://x.example/hook",
        "events": ["roster.published"], "secret": "s"})
    assert r.status_code == 401


# ------------------------------------------------------ owner-only admin surface

def test_queue_purge_is_owner_only():
    c = TestClient(app)
    staff_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    staff_h = _register_login(c, staff_email)
    _set_role(staff_email, "staff", venue_ids=[])
    assert c.post("/api/webhooks/queue/purge", headers=staff_h).status_code == 403

    owner_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    owner_h = _register_login(c, owner_email)
    _set_role(owner_email, "owner", venue_ids=[])
    # Owner passes the gate; the handler itself may 200 (or a backend error),
    # but must not be blocked by auth/role.
    assert c.post("/api/webhooks/queue/purge", headers=owner_h).status_code not in (401, 403)


# ------------------------------------------------ venue-manager admin surface

def test_register_requires_venue_manager():
    c = TestClient(app)
    vid = f"wh-venue-{uuid.uuid4().hex[:6]}"

    # A linked staff member of the venue is NOT enough.
    staff_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    staff_h = _register_login(c, staff_email)
    _set_role(staff_email, "staff", venue_ids=[vid])
    r = c.post("/api/webhooks/register", headers=staff_h,
               params={"venue_id": vid, "event_type": "roster.published",
                       "endpoint_url": "https://x.example/hook"})
    assert r.status_code == 403

    # A manager of the venue passes the gate. The downstream Tanda service is
    # unavailable in the test env and may error; a non-raising client lets us
    # assert only that the auth/role gate was NOT what stopped the request.
    mgr_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    mgr_h = _register_login(c, mgr_email)
    _set_role(mgr_email, "manager", venue_ids=[vid])
    nc = TestClient(app, raise_server_exceptions=False)
    r2 = nc.post("/api/webhooks/register", headers=mgr_h,
                 params={"venue_id": vid, "event_type": "roster.published",
                         "endpoint_url": "https://x.example/hook"})
    assert r2.status_code not in (401, 403)


def test_register_rejects_other_venue():
    """Manager of venue A cannot register a webhook for venue B."""
    c = TestClient(app)
    mgr_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    mgr_h = _register_login(c, mgr_email)
    _set_role(mgr_email, "manager", venue_ids=["venue-A"])
    r = c.post("/api/webhooks/register", headers=mgr_h,
               params={"venue_id": "venue-B", "event_type": "roster.published",
                       "endpoint_url": "https://x.example/hook"})
    assert r.status_code == 403


def test_outbound_subscribe_requires_venue_manager():
    c = TestClient(app)
    vid = f"wh-venue-{uuid.uuid4().hex[:6]}"
    staff_email = f"wh{uuid.uuid4().hex[:8]}@x.com"
    staff_h = _register_login(c, staff_email)
    _set_role(staff_email, "staff", venue_ids=[vid])
    r = c.post("/api/webhooks/subscribe", headers=staff_h, json={
        "venue_id": vid, "callback_url": "https://x.example/hook",
        "events": ["roster.published"], "secret": "s"})
    assert r.status_code == 403


# ------------------------------------------------ public receiver still open

def test_tanda_receiver_still_public(monkeypatch):
    """The one true public receiver must remain reachable with NO JWT."""
    monkeypatch.setenv("ENVIRONMENT", "test")  # skip the prod fail-closed secret check
    # Non-raising: the async background processing may error in the test env, but
    # the receiver returns 200 on the request path — proving it was NOT blocked
    # by the middleware as an unauthenticated request.
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/webhooks/tanda", json={
        "id": f"wh-{uuid.uuid4().hex[:8]}", "event_type": "roster.published",
        "timestamp": "2026-08-30T00:00:00Z", "venue_id": "v1",
        "user_id": "u1", "data": {}})
    assert r.status_code == 200
