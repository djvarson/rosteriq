"""
Tests for credential encryption at rest (secret_box) and the KeyPay connect routes.
"""

import asyncio

import httpx
import pytest
import respx
from cryptography.fernet import Fernet

from rosteriq.services import secret_box
from rosteriq.database import MemoryStore
from rosteriq.routes import keypay as kp
from rosteriq.services.keypay_export import KeyPayClient


# --- secret_box ------------------------------------------------------------

def test_encrypt_decrypt_roundtrip(monkeypatch):
    monkeypatch.setattr(secret_box, "_cipher", Fernet(Fernet.generate_key()))
    tokens = {"access_token": "secret-abc", "refresh_token": "r-123",
              "business_id": "42", "token_expires_at": "2026-07-01"}
    enc = secret_box.encrypt_tokens(tokens)
    # Sensitive values are encrypted (prefixed, changed); non-secrets untouched.
    assert enc["access_token"].startswith("enc::") and enc["access_token"] != "secret-abc"
    assert enc["refresh_token"].startswith("enc::")
    assert enc["business_id"] == "42"
    assert enc["token_expires_at"] == "2026-07-01"
    # Round-trips back to the originals.
    dec = secret_box.decrypt_tokens(enc)
    assert dec["access_token"] == "secret-abc"
    assert dec["refresh_token"] == "r-123"


def test_encrypt_is_idempotent(monkeypatch):
    monkeypatch.setattr(secret_box, "_cipher", Fernet(Fernet.generate_key()))
    once = secret_box.encrypt_tokens({"api_key": "k"})
    twice = secret_box.encrypt_tokens(once)
    assert once["api_key"] == twice["api_key"]  # not double-encrypted
    assert secret_box.decrypt_tokens(twice)["api_key"] == "k"


def test_noop_without_cipher(monkeypatch):
    monkeypatch.setattr(secret_box, "_cipher", None)
    tokens = {"access_token": "plain"}
    assert secret_box.encrypt_tokens(tokens) == tokens
    assert secret_box.decrypt_tokens(tokens) == tokens


def test_decrypt_plaintext_passthrough(monkeypatch):
    """Legacy plaintext rows (no prefix) decrypt to themselves."""
    monkeypatch.setattr(secret_box, "_cipher", Fernet(Fernet.generate_key()))
    assert secret_box.decrypt_tokens({"access_token": "legacy"}) == {"access_token": "legacy"}


# --- KeyPay routes ---------------------------------------------------------

_EMP_URL = f"{KeyPayClient.API_BASE}/business/42/employees"


@respx.mock
def test_keypay_install_status_uninstall(monkeypatch):
    respx.get(_EMP_URL).mock(return_value=httpx.Response(200, json={"employees": []}))
    db = MemoryStore()
    monkeypatch.setattr(kp, "get_db", lambda: db)

    r = asyncio.run(kp.install(kp.KeyPayInstallRequest(
        venue_id="v1", api_key="key12345", business_id="42")))
    assert r["status"] == "success"

    st = asyncio.run(kp.status("v1"))
    assert st["status"] == "active" and st["business_id"] == "42"

    un = asyncio.run(kp.uninstall(kp.KeyPayVenueRequest(venue_id="v1")))
    assert un["status"] == "success"
    assert asyncio.run(kp.status("v1"))["status"] == "not_installed"


@respx.mock
def test_keypay_install_rejects_bad_key(monkeypatch):
    respx.get(_EMP_URL).mock(return_value=httpx.Response(401, text="unauthorized"))
    db = MemoryStore()
    monkeypatch.setattr(kp, "get_db", lambda: db)
    try:
        asyncio.run(kp.install(kp.KeyPayInstallRequest(
            venue_id="v1", api_key="badkey12", business_id="42")))
        assert False, "expected rejection"
    except Exception as e:
        assert "rejected" in str(e) or "400" in str(e)
