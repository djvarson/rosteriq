"""Tests for Twilio SMS integration.

- verify_twilio_signature: HMAC-SHA1 verification with hand-computed signatures
- TwilioSMSProvider: constructor validation, httpx mocking, error handling
- Twilio webhook: form-encoded parsing, signature verification, inbound handling
"""

import asyncio
import base64
import hashlib
import hmac
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.call_in import (
    CallInService,
    CallInStore,
    SMSProviderError,
    TwilioSMSProvider,
    verify_twilio_signature,
)


def _run(coro):
    """Helper to run async tests without pytest-asyncio."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# verify_twilio_signature tests
# ---------------------------------------------------------------------------


def test_verify_twilio_signature_valid():
    """Valid Twilio signature should return True."""
    # Hand-computed known values
    auth_token = "my_auth_token_123"
    url = "https://myserver.example.com/api/v1/call-in/webhook/twilio"
    params = {"From": "+61412345678", "Body": "yes", "MessageSid": "SM123"}

    # Build canonical string: url + sorted params
    canonical = url + "".join(k + v for k, v in sorted(params.items()))
    # url + "BodyyesfromP61412345678MessageSidSM123"

    # Compute expected signature
    expected_sig = hmac.new(
        auth_token.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    expected_b64 = base64.b64encode(expected_sig).decode("utf-8")

    # Verify returns True
    assert verify_twilio_signature(expected_b64, url, params, auth_token) is True


def test_verify_twilio_signature_invalid():
    """Invalid signature should return False."""
    auth_token = "my_auth_token_123"
    url = "https://myserver.example.com/api/v1/call-in/webhook/twilio"
    params = {"From": "+61412345678", "Body": "yes"}

    # Use a wrong signature
    wrong_sig = "invalid_signature_base64"

    assert verify_twilio_signature(wrong_sig, url, params, auth_token) is False


def test_verify_twilio_signature_empty_params():
    """Empty params should still work (just url + empty string)."""
    auth_token = "token"
    url = "https://example.com/webhook"
    params = {}

    # Canonical is just the url
    canonical = url
    expected_sig = hmac.new(
        auth_token.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    expected_b64 = base64.b64encode(expected_sig).decode("utf-8")

    assert verify_twilio_signature(expected_b64, url, params, auth_token) is True


def test_verify_twilio_signature_uses_compare_digest():
    """Verify uses hmac.compare_digest to prevent timing attacks.

    We can't directly test compare_digest behavior, but we document that
    it's used for safety. This test just confirms the function works with
    valid/invalid sigs (which it must use compare_digest internally).
    """
    auth_token = "token"
    url = "https://example.com/webhook"
    params = {"Key": "Value"}

    canonical = url + "".join(k + v for k, v in sorted(params.items()))
    expected_sig = hmac.new(
        auth_token.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    expected_b64 = base64.b64encode(expected_sig).decode("utf-8")

    # Same signature should pass
    assert verify_twilio_signature(expected_b64, url, params, auth_token) is True

    # Different signature should fail (if compare_digest working, won't leak timing)
    assert verify_twilio_signature("wrong", url, params, auth_token) is False


# ---------------------------------------------------------------------------
# TwilioSMSProvider tests
# ---------------------------------------------------------------------------


def test_twilio_sms_provider_raises_without_credentials():
    """TwilioSMSProvider raises ValueError when no credentials and require_credentials=True."""
    import os

    # Save existing env vars
    old_sid = os.environ.pop("TWILIO_ACCOUNT_SID", None)
    old_token = os.environ.pop("TWILIO_AUTH_TOKEN", None)
    old_from = os.environ.pop("TWILIO_FROM", None)

    try:
        with pytest.raises(ValueError, match="requires TWILIO"):
            TwilioSMSProvider(require_credentials=True)
    finally:
        # Restore
        if old_sid:
            os.environ["TWILIO_ACCOUNT_SID"] = old_sid
        if old_token:
            os.environ["TWILIO_AUTH_TOKEN"] = old_token
        if old_from:
            os.environ["TWILIO_FROM"] = old_from


def test_twilio_sms_provider_accepts_explicit_kwargs():
    """TwilioSMSProvider accepts explicit kwargs (win over env)."""
    provider = TwilioSMSProvider(
        account_sid="ACxyz",
        auth_token="token123",
        twilio_from="+61212345678",
        require_credentials=True,
    )
    assert provider.account_sid == "ACxyz"
    assert provider.auth_token == "token123"
    assert provider.twilio_from == "+61212345678"


def test_twilio_sms_provider_require_credentials_false():
    """TwilioSMSProvider with require_credentials=False allows partial init."""
    provider = TwilioSMSProvider(require_credentials=False)
    # Should not raise, even though credentials are missing
    assert provider.account_sid is None


@pytest.mark.asyncio
async def test_twilio_sms_provider_send_success():
    """TwilioSMSProvider.send makes httpx POST and returns receipt dict."""
    provider = TwilioSMSProvider(
        account_sid="ACxyz",
        auth_token="token123",
        twilio_from="+61212345678",
    )

    # Mock httpx.AsyncClient
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json.return_value = {
        "sid": "SM1234567890abcdef",
        "status": "queued",
        "to": "+61412345678",
        "body": "Hi Alex — can you come in tonight?",
    }

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    with patch("rosteriq.call_in.httpx.AsyncClient") as MockAsyncClient:
        MockAsyncClient.return_value.__aenter__.return_value = mock_client

        receipt = await provider.send("+61412345678", "Hi Alex — can you come in tonight?")

        # Verify httpx call
        assert mock_client.post.called
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "https://api.twilio.com/2010-04-01/Accounts/ACxyz/Messages.json"
        assert call_args[1]["data"]["To"] == "+61412345678"
        assert call_args[1]["data"]["From"] == "+61212345678"
        assert "Hi Alex" in call_args[1]["data"]["Body"]

        # Verify receipt shape
        assert receipt["id"] == "SM1234567890abcdef"
        assert receipt["status"] == "queued"
        assert receipt["to"] == "+61412345678"
        assert "raw" in receipt


@pytest.mark.asyncio
async def test_twilio_sms_provider_send_error_400():
    """TwilioSMSProvider.send raises SMSProviderError on 400."""
    provider = TwilioSMSProvider(
        account_sid="ACxyz",
        auth_token="token123",
        twilio_from="+61212345678",
    )

    # Mock error response
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {"message": "Invalid To parameter"}

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    with patch("rosteriq.call_in.httpx.AsyncClient") as MockAsyncClient:
        MockAsyncClient.return_value.__aenter__.return_value = mock_client

        with pytest.raises(SMSProviderError, match="400.*Invalid To"):
            await provider.send("+61412345678", "Hi")


@pytest.mark.asyncio
async def test_twilio_sms_provider_send_error_500():
    """TwilioSMSProvider.send raises SMSProviderError on 500."""
    provider = TwilioSMSProvider(
        account_sid="ACxyz",
        auth_token="token123",
        twilio_from="+61212345678",
    )

    # Mock error response (no JSON)
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.side_effect = Exception("not json")
    mock_response.text = "Internal Server Error"

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response

    with patch("rosteriq.call_in.httpx.AsyncClient") as MockAsyncClient:
        MockAsyncClient.return_value.__aenter__.return_value = mock_client

        with pytest.raises(SMSProviderError, match="500.*Internal Server Error"):
            await provider.send("+61412345678", "Hi")


# ---------------------------------------------------------------------------
# Webhook endpoint tests
# ---------------------------------------------------------------------------


def test_twilio_webhook_endpoint_valid_signature():
    """POST /api/v1/call-in/webhook/twilio with valid signature returns 200."""
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from rosteriq.call_in_router import router
    from rosteriq.call_in import get_service, reset_singletons

    # Set up auth token in env
    import os
    old_token = os.environ.get("TWILIO_AUTH_TOKEN")
    os.environ["TWILIO_AUTH_TOKEN"] = "my_auth_token"

    try:
        reset_singletons()

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        # Create a pending request first
        service = get_service()
        _run(service.create_and_send(
            venue_id="venue_1",
            employee_id="emp_1",
            employee_name="Alex",
            phone="+61412345678",
            shift_start=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
            shift_end=datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc),
        ))

        # Build webhook request
        url = "http://testserver/api/v1/call-in/webhook/twilio"
        params = {"From": "+61412345678", "Body": "yes", "MessageSid": "SM123", "To": "+61212345678"}

        # Compute valid signature
        canonical = url + "".join(k + v for k, v in sorted(params.items()))
        sig = hmac.new(
            "my_auth_token".encode("utf-8"),
            canonical.encode("utf-8"),
            hashlib.sha1,
        ).digest()
        sig_b64 = base64.b64encode(sig).decode("utf-8")

        # POST with valid signature
        response = client.post(
            "/api/v1/call-in/webhook/twilio",
            data=params,
            headers={"X-Twilio-Signature": sig_b64},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["matched"] is True
        assert data["request"]["status"] == "accepted"

    finally:
        if old_token:
            os.environ["TWILIO_AUTH_TOKEN"] = old_token
        else:
            os.environ.pop("TWILIO_AUTH_TOKEN", None)
        reset_singletons()


def test_twilio_webhook_endpoint_invalid_signature():
    """POST /api/v1/call-in/webhook/twilio with invalid signature returns 403."""
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from rosteriq.call_in_router import router
    from rosteriq.call_in import reset_singletons

    import os
    old_token = os.environ.get("TWILIO_AUTH_TOKEN")
    os.environ["TWILIO_AUTH_TOKEN"] = "my_auth_token"

    try:
        reset_singletons()

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        # POST with invalid signature
        response = client.post(
            "/api/v1/call-in/webhook/twilio",
            data={"From": "+61412345678", "Body": "yes"},
            headers={"X-Twilio-Signature": "wrong_signature"},
        )

        assert response.status_code == 403

    finally:
        if old_token:
            os.environ["TWILIO_AUTH_TOKEN"] = old_token
        else:
            os.environ.pop("TWILIO_AUTH_TOKEN", None)
        reset_singletons()


def test_twilio_webhook_endpoint_demo_mode():
    """POST /api/v1/call-in/webhook/twilio without auth token accepts in demo mode."""
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from rosteriq.call_in_router import router
    from rosteriq.call_in import get_service, reset_singletons

    import os
    old_token = os.environ.pop("TWILIO_AUTH_TOKEN", None)

    try:
        reset_singletons()

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        # Create a pending request
        service = get_service()
        _run(service.create_and_send(
            venue_id="venue_1",
            employee_id="emp_1",
            employee_name="Alex",
            phone="+61412345678",
            shift_start=datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc),
            shift_end=datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc),
        ))

        # POST without valid signature (demo mode)
        response = client.post(
            "/api/v1/call-in/webhook/twilio",
            data={"From": "+61412345678", "Body": "yes"},
            headers={"X-Twilio-Signature": "anything"},
        )

        # Should accept in demo mode
        assert response.status_code == 200
        data = response.json()
        assert data["matched"] is True

    finally:
        if old_token:
            os.environ["TWILIO_AUTH_TOKEN"] = old_token
        reset_singletons()


def test_twilio_webhook_endpoint_body_too_large():
    """POST /api/v1/call-in/webhook/twilio with body > 1KB returns 400."""
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from rosteriq.call_in_router import router

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    # POST with large body (> 1KB)
    large_body = {"From": "+61412345678", "Body": "x" * 1024}
    response = client.post(
        "/api/v1/call-in/webhook/twilio",
        data=large_body,
    )

    assert response.status_code == 400
