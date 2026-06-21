"""
OAuth token-refresh tests for the MYOB and Xero Payroll integrations.

Verifies that an expired access token is automatically refreshed before/around
API calls, that the refreshed tokens are persisted via the on_token_refresh
callback, and (for MYOB) that a server-side 401 triggers a single refresh+retry.
"""

from datetime import datetime, timedelta

import httpx
import pytest
import respx

from rosteriq.myob_adapter import MYOBAdapter, MYOBCredentials, MYOBOAuth
from rosteriq.services.xero_payroll import XeroPayrollClient
from rosteriq.xero_integration import XeroCredentials, XeroOAuth
from rosteriq.tanda_adapter import TandaAdapter
from rosteriq.humanforce_adapter import HumanForceAdapter, HumanForceCredentials
from rosteriq.models import TandaCredentials


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _myob_creds(expired: bool, refresh_token: str = "myob-refresh-1") -> MYOBCredentials:
    return MYOBCredentials(
        api_key="myob-key",
        api_secret="myob-secret",
        access_token="old-access",
        refresh_token=refresh_token,
        token_expires_at=datetime.now() + (timedelta(hours=-1) if expired else timedelta(hours=1)),
        company_file_uri="https://api.myob.com/accountright",
        product="AccountRight",
    )


def _xero_creds(expired: bool) -> XeroCredentials:
    now = datetime.utcnow()
    return XeroCredentials(
        venue_id="venue-1",
        client_id="xero-client",
        client_secret="xero-secret",
        tenant_id="tenant-uuid",
        access_token="old-access",
        refresh_token="xero-refresh-1",
        token_expires=now + (timedelta(hours=-1) if expired else timedelta(hours=1)),
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------------------
# MYOB
# ---------------------------------------------------------------------------

@respx.mock
async def test_myob_proactive_refresh_on_expired_token():
    """An expired token is refreshed before the API call, and persisted."""
    token_route = respx.post(MYOBOAuth.TOKEN_URL).mock(
        return_value=httpx.Response(200, json={
            "access_token": "new-access",
            "refresh_token": "myob-refresh-2",
            "expires_in": 1200,
        })
    )
    api_url = "https://api.myob.com/accountright/Contact/Employee"
    api_route = respx.get(api_url).mock(return_value=httpx.Response(200, json={"Items": []}))

    persisted = []
    creds = _myob_creds(expired=True)
    async with MYOBAdapter(creds, on_token_refresh=persisted.append) as adapter:
        result = await adapter._get(api_url)

    assert result == {"Items": []}
    assert token_route.called, "expired token should have triggered a refresh"
    assert creds.access_token == "new-access"
    assert creds.refresh_token == "myob-refresh-2"  # rotated refresh token kept
    assert not creds.is_expired
    assert persisted and persisted[0] is creds  # callback fired with updated creds
    # The outgoing request used the NEW bearer token.
    assert api_route.calls.last.request.headers["Authorization"] == "Bearer new-access"


@respx.mock
async def test_myob_reactive_refresh_on_401():
    """A valid-looking token that the server rejects (401) refreshes once and retries."""
    respx.post(MYOBOAuth.TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "new-access", "expires_in": 1200})
    )
    api_url = "https://api.myob.com/accountright/Contact/Employee"
    api_route = respx.get(api_url).mock(side_effect=[
        httpx.Response(401, json={"Errors": ["expired"]}),
        httpx.Response(200, json={"Items": [{"UID": "e1"}]}),
    ])

    creds = _myob_creds(expired=False)  # not locally expired -> only the 401 path triggers refresh
    async with MYOBAdapter(creds) as adapter:
        result = await adapter._get(api_url)

    assert result == {"Items": [{"UID": "e1"}]}
    assert api_route.call_count == 2, "should retry once after refreshing"
    assert creds.access_token == "new-access"


@respx.mock
async def test_myob_401_without_refresh_token_raises():
    """A 401 with no refresh token surfaces an auth error (no infinite retry)."""
    api_url = "https://api.myob.com/accountright/Contact/Employee"
    respx.get(api_url).mock(return_value=httpx.Response(401, json={"Errors": ["bad"]}))

    creds = _myob_creds(expired=False, refresh_token="")
    async with MYOBAdapter(creds) as adapter:
        with pytest.raises(Exception) as exc:
            await adapter._get(api_url)
    assert "authentication failed" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Xero Payroll
# ---------------------------------------------------------------------------

@respx.mock
async def test_xero_payroll_refresh_on_expired_token():
    """Xero payroll _ensure_valid_token refreshes an expired token and persists it."""
    token_route = respx.post(XeroOAuth.TOKEN_URL).mock(
        return_value=httpx.Response(200, json={
            "access_token": "xero-new-access",
            "refresh_token": "xero-refresh-2",
            "expires_in": 1800,
        })
    )

    persisted = []
    creds = _xero_creds(expired=True)
    client = XeroPayrollClient(creds, on_token_refresh=persisted.append)

    await client._ensure_valid_token()

    assert token_route.called
    assert client.credentials.access_token == "xero-new-access"
    assert client.credentials.refresh_token == "xero-refresh-2"
    assert client._token_expires > datetime.utcnow()
    assert persisted and persisted[0].access_token == "xero-new-access"


@respx.mock
async def test_xero_payroll_no_refresh_when_token_valid():
    """A still-valid token is not refreshed."""
    token_route = respx.post(XeroOAuth.TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "should-not-be-used", "expires_in": 1800})
    )
    creds = _xero_creds(expired=False)
    client = XeroPayrollClient(creds)

    await client._ensure_valid_token()

    assert not token_route.called
    assert client.credentials.access_token == "old-access"


# ---------------------------------------------------------------------------
# Tanda — verifies the refresh hits the CORRECT endpoint with form encoding
# (the bug was: wrong path /oauth/token + JSON body -> 404/415 in production).
# ---------------------------------------------------------------------------

@respx.mock
async def test_tanda_refresh_uses_correct_endpoint_and_form_encoding():
    token_route = respx.post("https://my.tanda.co/api/oauth/token").mock(
        return_value=httpx.Response(200, json={
            "access_token": "tanda-new", "refresh_token": "tanda-refresh-2", "expires_in": 7200,
        })
    )
    creds = TandaCredentials(
        client_id="t-id", client_secret="t-secret",
        access_token="tanda-old", refresh_token="tanda-refresh-1", org_id="org-1",
    )
    adapter = TandaAdapter(creds)
    await adapter.refresh_token()

    assert token_route.called, "refresh must hit https://my.tanda.co/api/oauth/token"
    # Body must be form-encoded (not JSON) — the wrong content type 415'd before.
    sent = token_route.calls.last.request
    assert sent.headers["content-type"].startswith("application/x-www-form-urlencoded")
    assert b"grant_type=refresh_token" in sent.content
    assert creds.access_token == "tanda-new"


# ---------------------------------------------------------------------------
# HumanForce — verifies refresh persists via the on_token_refresh callback.
# ---------------------------------------------------------------------------

@respx.mock
async def test_humanforce_refresh_persists_via_callback():
    from rosteriq.humanforce_adapter import HumanForceOAuth
    respx.post(HumanForceOAuth.TOKEN_URL).mock(
        return_value=httpx.Response(200, json={
            "access_token": "hf-new", "refresh_token": "hf-refresh-2", "expires_in": 3600,
        })
    )
    persisted = []
    creds = HumanForceCredentials(
        client_id="hf-id", client_secret="hf-secret",
        access_token="hf-old", refresh_token="hf-refresh-1",
        token_expiry=datetime.now() - timedelta(hours=1),  # expired
    )
    adapter = HumanForceAdapter(creds, on_token_refresh=persisted.append)
    await adapter._refresh_token()

    assert creds.access_token == "hf-new"
    assert creds.refresh_token == "hf-refresh-2"
    assert persisted and persisted[0] is creds  # refreshed tokens handed to the persistence callback
