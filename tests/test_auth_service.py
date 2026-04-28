"""
Comprehensive tests for AuthService.

Tests password hashing/verification, JWT token generation/verification,
refresh tokens, API key management, user creation, and rate limiting.
"""

import sys
import os
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

# Ensure rosteriq imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.services.auth import (
    AuthService, JWT_SECRET, JWT_ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS,
)
from rosteriq.database import MemoryStore
import jwt


class TestPasswordHashing:
    """Test password hashing and verification."""

    def test_hash_password_creates_bcrypt_hash(self):
        """Password is hashed and result is different from plaintext."""
        password = "my_secure_password"
        hashed = AuthService.hash_password(password)

        # Hash should not equal plaintext
        assert hashed != password
        # Hash should contain bcrypt markers
        assert hashed.startswith("$2b$") or hashed.startswith("$2a$")

    def test_verify_password_correct(self):
        """Verify password returns True for correct password."""
        password = "correct_password"
        hashed = AuthService.hash_password(password)

        assert AuthService.verify_password(password, hashed) is True

    def test_verify_password_incorrect(self):
        """Verify password returns False for incorrect password."""
        password = "correct_password"
        hashed = AuthService.hash_password(password)

        assert AuthService.verify_password("wrong_password", hashed) is False

    def test_verify_password_empty_password(self):
        """Verify password with empty string returns False."""
        hashed = AuthService.hash_password("some_password")
        assert AuthService.verify_password("", hashed) is False


class TestAccessTokens:
    """Test JWT access token creation and verification."""

    def test_create_access_token_valid(self):
        """Created access token can be decoded."""
        user_id = "user-123"
        email = "user@example.com"
        role = "manager"

        token, expires = AuthService.create_access_token(user_id, email, role)

        # Token should be a string
        assert isinstance(token, str)
        # Expires should be a datetime
        assert isinstance(expires, datetime)
        # Expires should be in the future
        assert expires > datetime.utcnow()

    def test_access_token_expiration(self):
        """Access token expires in correct time."""
        user_id = "user-123"
        email = "user@example.com"
        role = "manager"

        token, expires = AuthService.create_access_token(user_id, email, role)

        # Should expire in approximately ACCESS_TOKEN_EXPIRE_MINUTES
        expected_min = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES - 1)
        expected_max = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES + 1)

        assert expected_min < expires < expected_max

    def test_verify_access_token_valid(self):
        """Valid access token is verified and decoded."""
        user_id = "user-123"
        email = "user@example.com"
        role = "manager"

        token, _ = AuthService.create_access_token(user_id, email, role)
        payload = AuthService.verify_access_token(token)

        assert payload is not None
        assert payload["sub"] == user_id
        assert payload["email"] == email
        assert payload["role"] == role
        assert payload["type"] == "access"

    def test_verify_access_token_invalid_signature(self):
        """Invalid token signature returns None."""
        user_id = "user-123"
        email = "user@example.com"
        role = "manager"

        token, _ = AuthService.create_access_token(user_id, email, role)
        # Tamper with token
        tampered = token[:-10] + "0000000000"

        assert AuthService.verify_access_token(tampered) is None

    def test_verify_access_token_wrong_type(self):
        """Token with wrong type is rejected."""
        # Create a token with type="refresh" instead of "access"
        payload = {
            "sub": "user-123",
            "email": "user@example.com",
            "role": "manager",
            "type": "refresh",
            "iat": datetime.utcnow().timestamp(),
            "exp": (datetime.utcnow() + timedelta(hours=1)).timestamp(),
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        assert AuthService.verify_access_token(token) is None

    def test_verify_access_token_expired(self):
        """Expired token returns None."""
        # Create an expired token
        now = datetime.utcnow()
        payload = {
            "sub": "user-123",
            "email": "user@example.com",
            "role": "manager",
            "type": "access",
            "iat": (now - timedelta(hours=2)).timestamp(),
            "exp": (now - timedelta(hours=1)).timestamp(),  # Expired 1 hour ago
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        assert AuthService.verify_access_token(token) is None


class TestRefreshTokens:
    """Test refresh token creation, verification, and revocation."""

    def test_create_refresh_token(self, memory_store):
        """Refresh token is created and stored."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"

        token, token_hash, expires = auth.create_refresh_token(user_id)

        # Token should be a non-empty string
        assert isinstance(token, str)
        assert len(token) > 0
        # Hash should be a hex string
        assert isinstance(token_hash, str)
        assert len(token_hash) == 64  # SHA256 hex is 64 chars
        # Expires should be in the future
        assert expires > datetime.utcnow()

    def test_refresh_token_expiration(self, memory_store):
        """Refresh token expires in correct time."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"

        token, token_hash, expires = auth.create_refresh_token(user_id)

        # Should expire in approximately REFRESH_TOKEN_EXPIRE_DAYS
        expected_min = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS - 1)
        expected_max = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS + 1)

        assert expected_min < expires < expected_max

    def test_verify_refresh_token_valid(self, memory_store):
        """Valid refresh token is verified and returns user_id."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"

        token, token_hash, expires = auth.create_refresh_token(user_id)
        verified_user_id = auth.verify_refresh_token(token)

        assert verified_user_id == user_id

    def test_verify_refresh_token_invalid(self, memory_store):
        """Invalid refresh token returns None."""
        auth = AuthService(db=memory_store)

        verified = auth.verify_refresh_token("invalid_token_xyz")
        assert verified is None

    def test_verify_refresh_token_revoked(self, memory_store):
        """Revoked refresh token returns None."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"

        token, token_hash, expires = auth.create_refresh_token(user_id)
        # Revoke the token
        auth.revoke_refresh_token(token)

        verified = auth.verify_refresh_token(token)
        assert verified is None

    def test_revoke_refresh_token(self, memory_store):
        """Refresh token can be revoked."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"

        token, token_hash, expires = auth.create_refresh_token(user_id)
        # Token should be valid before revocation
        assert auth.verify_refresh_token(token) == user_id

        # Revoke
        result = auth.revoke_refresh_token(token)
        assert result is True

        # Token should be invalid after revocation
        assert auth.verify_refresh_token(token) is None


class TestAPIKeys:
    """Test API key generation and verification."""

    def test_generate_api_key(self, memory_store):
        """API key is generated and stored."""
        auth = AuthService(db=memory_store)
        user_id = "user-123"
        # Create user first
        auth.create_user("user@example.com", "password", "User", "manager")
        user = auth.get_user_by_id(user_id)

        api_key = auth.generate_api_key(user_id)

        # Key should be a non-empty string
        assert isinstance(api_key, str)
        assert len(api_key) > 0
        # Key should have the correct prefix
        assert api_key.startswith("riq_")

    def test_generate_api_key_stores_hash(self, memory_store):
        """Generated API key hash is stored in database."""
        auth = AuthService(db=memory_store)
        # Create a user
        user = auth.create_user("user@example.com", "password", "User", "manager")
        user_id = user["id"]

        api_key = auth.generate_api_key(user_id)

        # Verify hash is stored
        stored_user = auth.get_user_by_id(user_id)
        assert stored_user["api_key_hash"] != ""
        assert stored_user["api_key_hash"] != api_key  # Should be hashed, not plaintext

    def test_verify_api_key_valid(self, memory_store):
        """Valid API key is verified and returns user."""
        auth = AuthService(db=memory_store)
        user = auth.create_user("user@example.com", "password", "User", "manager")
        user_id = user["id"]

        api_key = auth.generate_api_key(user_id)
        verified_user = auth.verify_api_key(api_key)

        assert verified_user is not None
        assert verified_user["id"] == user_id
        assert verified_user["email"] == "user@example.com"

    def test_verify_api_key_invalid(self, memory_store):
        """Invalid API key returns None."""
        auth = AuthService(db=memory_store)
        verified = auth.verify_api_key("riq_invalid_key_xyz")
        assert verified is None

    def test_verify_api_key_inactive_user(self, memory_store):
        """API key of inactive user returns None."""
        auth = AuthService(db=memory_store)
        user = auth.create_user("user@example.com", "password", "User", "manager")
        user_id = user["id"]

        api_key = auth.generate_api_key(user_id)

        # Deactivate user
        user["is_active"] = False
        auth.db.save_user(user)

        verified = auth.verify_api_key(api_key)
        assert verified is None


class TestUserCreation:
    """Test user creation and retrieval."""

    def test_create_user_success(self, memory_store):
        """User is created successfully."""
        auth = AuthService(db=memory_store)
        email = "newuser@example.com"
        password = "secure_password"
        name = "New User"

        user = auth.create_user(email, password, name, role="manager")

        assert user["id"] is not None
        assert user["email"] == email
        assert user["name"] == name
        assert user["role"] == "manager"
        assert user["is_active"] is True
        assert user["created_at"] is not None
        # Password should be hashed
        assert user["password_hash"] != password

    def test_create_user_duplicate_email(self, memory_store):
        """Creating user with duplicate email raises ValueError."""
        auth = AuthService(db=memory_store)
        email = "duplicate@example.com"

        # Create first user
        auth.create_user(email, "password1", "User One", "manager")

        # Try to create second user with same email
        with pytest.raises(ValueError, match="already exists"):
            auth.create_user(email, "password2", "User Two", "staff")

    def test_create_user_default_role(self, memory_store):
        """User is created with default role 'staff' if not specified."""
        auth = AuthService(db=memory_store)

        user = auth.create_user("user@example.com", "password", "User")

        assert user["role"] == "staff"

    def test_get_user_by_email(self, memory_store):
        """User can be retrieved by email."""
        auth = AuthService(db=memory_store)
        email = "user@example.com"
        auth.create_user(email, "password", "User", "manager")

        user = auth.get_user_by_email(email)

        assert user is not None
        assert user["email"] == email

    def test_get_user_by_email_not_found(self, memory_store):
        """Getting non-existent user by email returns None."""
        auth = AuthService(db=memory_store)

        user = auth.get_user_by_email("nonexistent@example.com")

        assert user is None

    def test_get_user_by_id(self, memory_store):
        """User can be retrieved by ID."""
        auth = AuthService(db=memory_store)
        created_user = auth.create_user("user@example.com", "password", "User")

        retrieved_user = auth.get_user_by_id(created_user["id"])

        assert retrieved_user is not None
        assert retrieved_user["id"] == created_user["id"]
        assert retrieved_user["email"] == "user@example.com"

    def test_get_user_by_id_not_found(self, memory_store):
        """Getting non-existent user by ID returns None."""
        auth = AuthService(db=memory_store)

        user = auth.get_user_by_id("nonexistent-id")

        assert user is None


class TestRateLimiting:
    """Test login rate limiting."""

    def test_check_login_rate_limit_under_limit(self, memory_store):
        """Rate limit check returns True when under limit."""
        auth = AuthService(db=memory_store)
        ip = "192.168.1.1"

        # Record 2 failed attempts (limit is 5)
        auth.record_login_attempt("user@example.com", ip, success=False)
        auth.record_login_attempt("user@example.com", ip, success=False)

        allowed = auth.check_login_rate_limit(ip, max_attempts=5)
        assert allowed is True

    def test_check_login_rate_limit_over_limit(self, memory_store):
        """Rate limit check returns False when over limit."""
        auth = AuthService(db=memory_store)
        ip = "192.168.1.2"
        max_attempts = 3

        # Record 4 failed attempts (over limit of 3)
        for i in range(4):
            auth.record_login_attempt("user@example.com", ip, success=False)

        allowed = auth.check_login_rate_limit(ip, max_attempts=max_attempts)
        assert allowed is False

    def test_check_login_rate_limit_successful_attempts_ignored(self, memory_store):
        """Successful login attempts are not counted in rate limit."""
        auth = AuthService(db=memory_store)
        ip = "192.168.1.3"

        # Record mix of successful and failed attempts
        auth.record_login_attempt("user@example.com", ip, success=True)
        auth.record_login_attempt("user@example.com", ip, success=False)
        auth.record_login_attempt("user@example.com", ip, success=True)
        auth.record_login_attempt("user@example.com", ip, success=False)

        # Only 2 failed attempts (limit is 5)
        allowed = auth.check_login_rate_limit(ip, max_attempts=5)
        assert allowed is True

    def test_check_login_rate_limit_per_ip(self, memory_store):
        """Rate limit is tracked per IP address."""
        auth = AuthService(db=memory_store)
        ip1 = "192.168.1.10"
        ip2 = "192.168.1.20"

        # Record 3 failed attempts from IP1
        for i in range(3):
            auth.record_login_attempt("user@example.com", ip1, success=False)

        # IP1 should be rate limited
        assert auth.check_login_rate_limit(ip1, max_attempts=2) is False

        # IP2 should not be rate limited
        assert auth.check_login_rate_limit(ip2, max_attempts=2) is True

    def test_record_login_attempt(self, memory_store):
        """Login attempt is recorded."""
        auth = AuthService(db=memory_store)
        email = "user@example.com"
        ip = "192.168.1.1"

        auth.record_login_attempt(email, ip, success=True)

        # Verify it was recorded by checking rate limit
        attempts = memory_store.check_login_rate_limit(ip)
        # Should be 0 failed attempts
        assert attempts == 0

    def test_update_last_login(self, memory_store):
        """User's last_login timestamp is updated."""
        auth = AuthService(db=memory_store)
        user = auth.create_user("user@example.com", "password", "User")
        user_id = user["id"]

        # Initial last_login should be None
        assert user["last_login"] is None

        # Update last_login
        auth.update_last_login(user_id)

        # Verify it was updated
        updated_user = auth.get_user_by_id(user_id)
        assert updated_user["last_login"] is not None
        assert isinstance(updated_user["last_login"], datetime)


class TestAuthServiceIntegration:
    """Integration tests combining multiple auth features."""

    def test_full_login_flow(self, memory_store):
        """Complete login flow: create user, verify password, generate tokens."""
        auth = AuthService(db=memory_store)

        # Create user
        password = "secure_password"
        user = auth.create_user("user@example.com", password, "User", "manager")

        # Verify password
        stored_user = auth.get_user_by_email("user@example.com")
        assert auth.verify_password(password, stored_user["password_hash"])

        # Generate tokens
        access_token, _ = auth.create_access_token(user["id"], user["email"], user["role"])
        refresh_token, _, _ = auth.create_refresh_token(user["id"])

        # Verify tokens
        access_payload = auth.verify_access_token(access_token)
        assert access_payload is not None
        assert access_payload["sub"] == user["id"]

        assert auth.verify_refresh_token(refresh_token) == user["id"]

    def test_api_key_authentication(self, memory_store):
        """Complete API key flow: create, store, verify."""
        auth = AuthService(db=memory_store)

        # Create user
        user = auth.create_user("api@example.com", "password", "API User", "staff")

        # Generate API key
        api_key = auth.generate_api_key(user["id"])

        # Verify API key authenticates correctly
        authenticated_user = auth.verify_api_key(api_key)
        assert authenticated_user is not None
        assert authenticated_user["email"] == "api@example.com"
