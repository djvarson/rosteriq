"""
Tests for rosteriq.auth — JWT, password hashing, API keys, user CRUD.

Runs with: python tests/test_auth.py
Pure-stdlib runner — no pytest, no FastAPI required.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import auth  # noqa: E402


def _reset():
    """Clear in-memory stores between tests."""
    auth._users.clear()
    auth._api_keys.clear()
    auth._failed_login_attempts.clear()


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

def test_hash_and_verify_password():
    _reset()
    hashed = auth.hash_password("Secure123!")
    assert hashed != "Secure123!"
    assert auth.verify_password("Secure123!", hashed) is True
    assert auth.verify_password("WrongPass", hashed) is False


def test_hash_password_rejects_short():
    _reset()
    try:
        auth.hash_password("short")
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "at least" in str(e).lower()


# ---------------------------------------------------------------------------
# JWT tokens
# ---------------------------------------------------------------------------

def test_create_and_decode_token():
    _reset()
    token = auth.create_access_token("user_1", "venue_1", "manager")
    payload = auth.decode_token(token)
    assert payload["sub"] == "user_1"
    assert payload["venue_id"] == "venue_1"
    assert payload["role"] == "manager"


def test_expired_token_rejected():
    _reset()
    token = auth.create_access_token("u", "v", "m", expires_delta=timedelta(seconds=-1))
    try:
        auth.decode_token(token)
        raise AssertionError("Expected error for expired token")
    except Exception as e:
        assert "expire" in str(e).lower() or "invalid" in str(e).lower()


def test_tampered_token_rejected():
    _reset()
    token = auth.create_access_token("u", "v", "m")
    # Flip one character
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    try:
        auth.decode_token(tampered)
        raise AssertionError("Expected error for tampered token")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------

def test_create_user():
    _reset()
    user_create = auth.UserCreate(
        email="test@venue.com.au",
        password="TestPass123!",
        name="Test User",
        venue_id="venue_001",
    )
    user = auth.create_user(user_create)
    assert user.email == "test@venue.com.au"
    assert user.name == "Test User"
    assert user.venue_id == "venue_001"
    assert user.id  # should have generated an id


def test_create_duplicate_user_rejected():
    _reset()
    user_create = auth.UserCreate(
        email="dupe@venue.com.au",
        password="TestPass123!",
        name="Test",
        venue_id="v1",
    )
    auth.create_user(user_create)
    try:
        auth.create_user(user_create)
        raise AssertionError("Expected error for duplicate email")
    except Exception as e:
        assert "already" in str(e).lower() or "exists" in str(e).lower() or "registered" in str(e).lower()


def test_get_user_by_email():
    _reset()
    auth.create_user(auth.UserCreate(
        email="lookup@v.com", password="TestPass123!", name="Look", venue_id="v1",
    ))
    user = auth.get_user_by_email("lookup@v.com")
    assert user is not None
    assert user.email == "lookup@v.com"

    assert auth.get_user_by_email("nope@v.com") is None


def test_authenticate_user():
    _reset()
    auth.create_user(auth.UserCreate(
        email="auth@v.com", password="CorrectHorse1!", name="Auth", venue_id="v1",
    ))
    user = auth.authenticate_user("auth@v.com", "CorrectHorse1!")
    assert user is not None
    assert user.email == "auth@v.com"

    assert auth.authenticate_user("auth@v.com", "WrongPassword1!") is None


# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------

def test_generate_and_verify_api_key():
    _reset()
    api_key = auth.generate_api_key("venue_1", "Integration Key")
    assert api_key.key.startswith(auth.API_KEY_PREFIX)
    assert api_key.venue_id == "venue_1"
    assert api_key.active is True

    data = auth.verify_api_key(api_key.key)
    assert data["venue_id"] == "venue_1"


def test_verify_invalid_api_key():
    _reset()
    try:
        auth.verify_api_key("riq_boguskey12345678901234567890")
        raise AssertionError("Expected error for invalid key")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Demo user
# ---------------------------------------------------------------------------

def test_create_demo_user():
    _reset()
    user = auth.create_demo_user()
    assert user.email == "demo@rosteriq.com.au"
    assert user.venue_id == "venue_demo_001"
    # Should be able to authenticate with the demo password
    auth_user = auth.authenticate_user("demo@rosteriq.com.au", "DemoPassword1!")
    assert auth_user is not None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
