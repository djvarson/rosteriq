"""
Follow-up to the 2026-08-30 authz remediation: the Tanda Marketplace plugin
install/uninstall routes.

Those routes were hardened on 2026-08-30 with an interim ``enforce_owner()``
gate — but their real caller is the Tanda Marketplace itself, which holds no
RosterIQ JWT (at install time no venue/user exists yet), so the JWT-based gate
locked out the genuine caller entirely. The proper fix moves the routes onto the
signed-webhook auth path (WEBHOOK_EXEMPT / WEBHOOK_PATHS) and authenticates them
with the X-Tanda-Signature HMAC check (verify_tanda_signature), which fails
CLOSED exactly like the /api/webhooks/tanda receiver.

These tests pin that contract:
  * no JWT is required (the middleware no longer 401s the route);
  * an unconfigured secret rejects in production (fail closed);
  * a wrong/missing signature is rejected when the secret is set;
  * a valid signature reaches the handler.
"""

import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
import rosteriq.routes.tanda_plugin as tanda_plugin


SECRET = "tanda-marketplace-secret-0123456789"

INSTALL_PAYLOAD = {
    "organisation_id": "org-123",
    "auth_code": "auth-abc",
    "redirect_uri": "https://api.rosteriq.com.au/callback",
    "state": "vic",
}
UNINSTALL_PAYLOAD = {"organisation_id": "org-123"}


def _sign(body: bytes, secret: str = SECRET) -> str:
    """HMAC-SHA256 hex digest over the exact request body bytes, as Tanda signs."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _body(payload: dict) -> bytes:
    return json.dumps(payload).encode()


def _detail(resp) -> str:
    """The error message, tolerant of the app's error envelope: FastAPI's raw
    ``{"detail": ...}`` or the security middleware's ``{"error": {"message": ...}}``."""
    body = resp.json()
    if "detail" in body:
        return body["detail"]
    return body.get("error", {}).get("message", "")


class _FakeService:
    """Stand-in so a signature-authenticated request never makes a real OAuth
    call to Tanda — lets us assert the handler ran once the gate passed."""

    async def handle_install(self, organisation_id, auth_code, redirect_uri, state_code="vic"):
        return {"status": "installed", "organisation_id": organisation_id, "fake": True}

    async def handle_uninstall(self, organisation_id):
        return {"status": "uninstalled", "organisation_id": organisation_id, "fake": True}


@pytest.fixture
def fake_service(monkeypatch):
    monkeypatch.setattr(tanda_plugin, "get_plugin_service", lambda: _FakeService())


# --------------------------------------------------------- no JWT is required

def test_install_does_not_require_jwt(monkeypatch, fake_service):
    """A genuine marketplace call carries no Authorization header. The route
    must reach the signature gate, not be 401'd by TenantMiddleware as
    unauthenticated. In dev/test with no secret configured, the gate lets it
    through and the handler runs."""
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.delenv("TANDA_WEBHOOK_SECRET", raising=False)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=_body(INSTALL_PAYLOAD),
               headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    assert r.json().get("fake") is True


# ------------------------------------------------ fail closed without a secret

def test_install_rejected_in_production_without_secret(monkeypatch):
    """No secret + production (unset ENVIRONMENT is treated as production) must
    reject with 503 — never silently accept a forged, unsigned install."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("TANDA_WEBHOOK_SECRET", raising=False)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=_body(INSTALL_PAYLOAD),
               headers={"Content-Type": "application/json"})
    assert r.status_code == 503
    # Proves the request reached the handler's signature dependency, not the
    # middleware's "Missing Authorization header" 401.
    assert _detail(r) == "Marketplace signature verification not configured"


# --------------------------------------------- signature required & verified

def test_install_rejected_without_signature_when_secret_set(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=_body(INSTALL_PAYLOAD),
               headers={"Content-Type": "application/json"})
    assert r.status_code == 401
    assert _detail(r) == "Missing X-Tanda-Signature header"


def test_install_rejected_with_bad_signature(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=_body(INSTALL_PAYLOAD),
               headers={"Content-Type": "application/json",
                        "X-Tanda-Signature": "deadbeef" * 8})
    assert r.status_code == 401
    assert _detail(r) == "Invalid signature"


def test_install_rejected_when_signature_signed_with_wrong_secret(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    body = _body(INSTALL_PAYLOAD)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=body,
               headers={"Content-Type": "application/json",
                        "X-Tanda-Signature": _sign(body, "the-wrong-secret")})
    assert r.status_code == 401
    assert _detail(r) == "Invalid signature"


def test_install_accepted_with_valid_signature(monkeypatch, fake_service):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    body = _body(INSTALL_PAYLOAD)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/install", content=body,
               headers={"Content-Type": "application/json",
                        "X-Tanda-Signature": _sign(body)})
    assert r.status_code == 200
    assert r.json().get("fake") is True
    assert r.json().get("organisation_id") == "org-123"


# ------------------------------------------------------------------ uninstall

def test_uninstall_rejected_with_bad_signature(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/uninstall", content=_body(UNINSTALL_PAYLOAD),
               headers={"Content-Type": "application/json",
                        "X-Tanda-Signature": "deadbeef" * 8})
    assert r.status_code == 401
    assert _detail(r) == "Invalid signature"


def test_uninstall_accepted_with_valid_signature(monkeypatch, fake_service):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    body = _body(UNINSTALL_PAYLOAD)
    c = TestClient(app, raise_server_exceptions=False)
    r = c.post("/api/tanda/plugin/uninstall", content=body,
               headers={"Content-Type": "application/json",
                        "X-Tanda-Signature": _sign(body)})
    assert r.status_code == 200
    assert r.json().get("fake") is True


# --------------------------------------------- the exemption lists stay in sync

def test_exemption_lists_kept_in_sync():
    """WEBHOOK_EXEMPT (tenant) and WEBHOOK_PATHS (auth) must list the same public
    receivers, including the two plugin routes — a drift here is how a route ends
    up JWT-blocked on one path and waved through on the other."""
    from rosteriq.middleware.tenant import WEBHOOK_EXEMPT
    from rosteriq.middleware.auth import WEBHOOK_PATHS

    assert "/api/tanda/plugin/install" in WEBHOOK_EXEMPT
    assert "/api/tanda/plugin/uninstall" in WEBHOOK_EXEMPT
    assert WEBHOOK_EXEMPT == WEBHOOK_PATHS


# ---------------------------------------------------------------------------
# Review-hardening pins (landed with the HMAC gate)
# ---------------------------------------------------------------------------

def test_non_ascii_signature_is_401_not_500(monkeypatch):
    """A hostile header byte must read as 'invalid signature', never crash."""
    monkeypatch.setenv("TANDA_WEBHOOK_SECRET", SECRET)
    c = TestClient(app)
    body = _body(INSTALL_PAYLOAD)
    # httpx refuses non-ASCII str headers; raw bytes reach the server as the
    # latin-1-decoded str the middleware would see from a hostile client.
    r = c.post("/api/tanda/plugin/install", content=body,
               headers={b"X-Tanda-Signature": b"\xff\xfebad",
                        b"Content-Type": b"application/json"})
    assert r.status_code == 401, r.text


def test_replayed_uninstall_is_a_noop_once_uninstalled(monkeypatch):
    """The signature carries no timestamp, so a captured uninstall verifies
    forever — the service must refuse to re-kill an already-uninstalled org
    (and so never revoke a reinstalled org's fresh tokens via replay)."""
    import asyncio
    from rosteriq.database import get_db
    from rosteriq.services.tanda_plugin import TandaPluginService

    db = get_db()
    org = "org-replay-1"
    db.save_plugin_install({
        "organisation_id": org, "venue_id": "v-replay", "status": "uninstalled",
        "access_token": "t", "refresh_token": "r",
        "installed_at": "2026-09-01T00:00:00", "uninstalled_at": "2026-09-02T00:00:00",
    })
    svc = TandaPluginService(db=db)
    # asyncio.run: a fresh loop every time — get_event_loop() inherits a
    # closed loop when async tests ran earlier in the session.
    out = asyncio.run(svc.handle_uninstall(org))
    assert out["status"] == "already_uninstalled"


def test_status_route_is_venue_gated(monkeypatch):
    """An install record is tenant data — only the mapped venue's members
    (or the platform owner) may read it."""
    import uuid as _uuid
    from rosteriq.database import get_db

    c = TestClient(app)
    tag = _uuid.uuid4().hex[:6]

    def login(email):
        c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
        tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
        return {"Authorization": f"Bearer {tok}"}

    h_a = login(f"tp_a_{tag}@x.com")   # first user bootstraps as global owner
    c.post("/venues", json={"id": f"tp-a-{tag}", "name": "A", "state": "wa",
                            "max_labour_pct": 30, "tanda_org_id": "",
                            "created_at": "2026-07-01T00:00:00"}, headers=h_a)
    h_b = login(f"tp_b_{tag}@x.com")   # second user: plain tenant of venue B
    c.post("/venues", json={"id": f"tp-b-{tag}", "name": "B", "state": "wa",
                            "max_labour_pct": 30, "tanda_org_id": "",
                            "created_at": "2026-07-01T00:00:00"}, headers=h_b)

    get_db().save_plugin_install({
        "organisation_id": f"org-{tag}", "venue_id": f"tp-a-{tag}",
        "status": "installed", "access_token": "t", "refresh_token": "r",
        "installed_at": "2026-09-01T00:00:00",
    })
    # a member of a different venue may not read venue A's install record
    r = c.get(f"/api/tanda/plugin/status/org-{tag}", headers=h_b)
    assert r.status_code == 403, r.text
