"""
OAuth callbacks must be public; OAuth *initiation* must not.

A provider redirects the user's browser to /<provider>/callback?code=...&state=...
with no Authorization header — the venue is identified from the signed `state`.
If the callback isn't auth-exempt it 401s and the whole OAuth connect flow breaks.
Conversely the connect-initiation endpoints (which a logged-in venue calls) must
stay protected, so we don't over-exempt.
"""

from fastapi.testclient import TestClient

from rosteriq.api import app

CALLBACKS = [
    "/deputy/callback",
    "/api/deputy/callback",
    "/api/xero/callback",
    "/api/myob/callback",
    "/api/humanforce/callback",
]


def test_oauth_callbacks_are_not_auth_blocked():
    c = TestClient(app)
    for path in CALLBACKS:
        r = c.get(path + "?code=x&state=y")
        # Reaches the handler (which rejects the bogus code) — must NOT be the
        # tenant-middleware 'Missing Authorization header' 401.
        assert not (r.status_code == 401 and "Authorization" in r.text), (
            f"{path} is auth-blocked ({r.status_code}): OAuth callback would fail"
        )


def test_oauth_initiation_still_requires_auth():
    """The connect-initiation endpoints (called by a logged-in venue) stay protected."""
    c = TestClient(app)
    for path in ["/api/deputy/install", "/api/myob/install", "/api/humanforce/install"]:
        r = c.post(path, json={"venue_id": "v1"})
        assert r.status_code == 401, f"{path} should require auth, got {r.status_code}"


def test_arbitrary_protected_route_still_requires_auth():
    """Sanity: the /callback exemption didn't open up unrelated routes."""
    c = TestClient(app)
    r = c.get("/api/connections/catalog")
    assert r.status_code == 401
