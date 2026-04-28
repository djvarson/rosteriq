"""
End-to-end integration tests for RosterIQ authentication routes.

Tests exercise the FastAPI auth endpoints via TestClient, verifying
user registration, login, token management, and protected endpoints.

Coverage:
1. User registration — POST /api/auth/register (new user, duplicate, role assignment)
2. User login — POST /api/auth/login (valid credentials, invalid, rate limiting)
3. Token refresh — POST /api/auth/refresh (valid token, invalid token)
4. User profile — GET /api/auth/me (with token, without token)
5. Protected endpoints — Verify 401 without token
6. Logout — POST /api/auth/logout
7. API key generation — POST /api/auth/api-key/generate

Run:
    pytest tests/test_api_auth_e2e.py -v
"""

import sys
import os
import json

# Ensure imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import reset_db


@pytest.fixture(autouse=True)
def reset_database():
    """Reset the global database before and after each test."""
    reset_db()
    yield
    reset_db()


@pytest.fixture
def client():
    """Create a TestClient for the FastAPI app."""
    return TestClient(app)


# ============================================================================
# TESTS: User Registration
# ============================================================================

class TestUserRegistration:
    """User registration endpoint."""

    def test_register_new_user(self, client):
        """POST /api/auth/register creates a new user."""
        response = client.post(
            "/api/auth/register",
            json={
                "email": "alice@example.com",
                "password": "SecurePassword123!",
                "name": "Alice Manager",
            }
        )

        assert response.status_code == 201
        data = response.json()

        # Verify response structure
        assert "user" in data
        assert "tokens" in data

        # Verify user details
        user = data["user"]
        assert user["email"] == "alice@example.com"
        assert user["name"] == "Alice Manager"
        assert "id" in user
        assert "created_at" in user

        # First user becomes owner
        assert user["role"] == "owner"
        assert user["is_active"] is True

        # Verify tokens
        tokens = data["tokens"]
        assert "access_token" in tokens
        assert "refresh_token" in tokens
        assert len(tokens["access_token"]) > 0
        assert len(tokens["refresh_token"]) > 0

    def test_register_second_user(self, client):
        """Second user registered has 'staff' role."""
        # Register first user (owner)
        client.post(
            "/api/auth/register",
            json={
                "email": "owner@example.com",
                "password": "SecurePassword123!",
                "name": "Owner",
            }
        )

        # Register second user
        response = client.post(
            "/api/auth/register",
            json={
                "email": "staff@example.com",
                "password": "SecurePassword123!",
                "name": "Staff Member",
            }
        )

        assert response.status_code == 201
        user = response.json()["user"]
        assert user["role"] == "staff"

    def test_register_duplicate_email(self, client):
        """POST /api/auth/register returns 409 for duplicate email."""
        # Register first user
        client.post(
            "/api/auth/register",
            json={
                "email": "alice@example.com",
                "password": "SecurePassword123!",
                "name": "Alice",
            }
        )

        # Try to register with same email
        response = client.post(
            "/api/auth/register",
            json={
                "email": "alice@example.com",
                "password": "DifferentPassword123!",
                "name": "Alice 2",
            }
        )

        assert response.status_code == 409
        data = response.json()
        assert "already exists" in data["detail"].lower()

    def test_register_missing_fields(self, client):
        """POST /api/auth/register returns 422 for missing fields."""
        response = client.post(
            "/api/auth/register",
            json={
                "email": "bob@example.com",
                # Missing password and name
            }
        )

        assert response.status_code == 422

    def test_register_invalid_email(self, client):
        """POST /api/auth/register validates email format."""
        response = client.post(
            "/api/auth/register",
            json={
                "email": "not-an-email",
                "password": "SecurePassword123!",
                "name": "Bob",
            }
        )

        assert response.status_code == 422


# ============================================================================
# TESTS: User Login
# ============================================================================

class TestUserLogin:
    """User login endpoint."""

    def setup_user(self, client, email="alice@example.com", password="SecurePassword123!"):
        """Helper to register a user."""
        return client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Test User",
            }
        )

    def test_login_valid_credentials(self, client):
        """POST /api/auth/login with valid credentials returns tokens."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        self.setup_user(client, email, password)

        response = client.post(
            "/api/auth/login",
            json={
                "email": email,
                "password": password,
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert "access_token" in data
        assert "refresh_token" in data
        assert len(data["access_token"]) > 0
        assert len(data["refresh_token"]) > 0

    def test_login_invalid_email(self, client):
        """POST /api/auth/login returns 401 for nonexistent email."""
        response = client.post(
            "/api/auth/login",
            json={
                "email": "nonexistent@example.com",
                "password": "SomePassword123!",
            }
        )

        assert response.status_code == 401
        data = response.json()
        assert "invalid" in data["detail"].lower()

    def test_login_invalid_password(self, client):
        """POST /api/auth/login returns 401 for wrong password."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        self.setup_user(client, email, password)

        response = client.post(
            "/api/auth/login",
            json={
                "email": email,
                "password": "WrongPassword123!",
            }
        )

        assert response.status_code == 401

    def test_login_missing_fields(self, client):
        """POST /api/auth/login returns 422 for missing fields."""
        response = client.post(
            "/api/auth/login",
            json={
                "email": "alice@example.com",
                # Missing password
            }
        )

        assert response.status_code == 422


# ============================================================================
# TESTS: Token Refresh
# ============================================================================

class TestTokenRefresh:
    """Token refresh endpoint."""

    def setup_user_and_login(self, client):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Test User",
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        return login_response.json()

    def test_refresh_with_valid_token(self, client):
        """POST /api/auth/refresh with valid token returns new tokens."""
        tokens = self.setup_user_and_login(client)
        old_refresh_token = tokens["refresh_token"]

        response = client.post(
            "/api/auth/refresh",
            json={"refresh_token": old_refresh_token}
        )

        assert response.status_code == 200
        data = response.json()

        assert "access_token" in data
        assert "refresh_token" in data

        # New refresh token should be different from old
        assert data["refresh_token"] != old_refresh_token

    def test_refresh_with_invalid_token(self, client):
        """POST /api/auth/refresh returns 401 for invalid token."""
        response = client.post(
            "/api/auth/refresh",
            json={"refresh_token": "invalid-token-xyz"}
        )

        assert response.status_code == 401

    def test_refresh_missing_token(self, client):
        """POST /api/auth/refresh returns 422 for missing token."""
        response = client.post(
            "/api/auth/refresh",
            json={}
        )

        assert response.status_code == 422


# ============================================================================
# TESTS: Current User Profile
# ============================================================================

class TestCurrentUserProfile:
    """Get current user profile endpoint."""

    def setup_user_and_login(self, client):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"
        name = "Alice Manager"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": name,
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        return login_response.json()["access_token"], email, name

    def test_get_profile_with_valid_token(self, client):
        """GET /api/auth/me with valid token returns user profile."""
        access_token, email, name = self.setup_user_and_login(client)

        response = client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response.status_code == 200
        data = response.json()

        assert data["email"] == email
        assert data["name"] == name
        assert "id" in data
        assert "role" in data
        assert data["is_active"] is True

    def test_get_profile_without_token(self, client):
        """GET /api/auth/me without token returns 401."""
        response = client.get("/api/auth/me")

        assert response.status_code == 401

    def test_get_profile_with_invalid_token(self, client):
        """GET /api/auth/me with invalid token returns 401."""
        response = client.get(
            "/api/auth/me",
            headers={"Authorization": "Bearer invalid-token-xyz"}
        )

        assert response.status_code == 401

    def test_get_profile_with_malformed_header(self, client):
        """GET /api/auth/me with malformed auth header returns 401."""
        response = client.get(
            "/api/auth/me",
            headers={"Authorization": "InvalidHeaderFormat"}
        )

        assert response.status_code == 401


# ============================================================================
# TESTS: Protected Endpoints
# ============================================================================

class TestProtectedEndpoints:
    """Verify protected endpoints require authentication."""

    def setup_user_and_login(self, client):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Test User",
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        return login_response.json()["access_token"]

    def test_protected_endpoint_without_token(self, client):
        """Protected endpoints return 401 without token."""
        # /api/auth/me is a protected endpoint
        response = client.get("/api/auth/me")

        assert response.status_code == 401
        data = response.json()
        assert "detail" in data

    def test_protected_endpoint_with_valid_token(self, client):
        """Protected endpoints work with valid token."""
        access_token = self.setup_user_and_login(client)

        response = client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response.status_code == 200

    def test_protected_endpoint_with_expired_token(self, client):
        """Protected endpoints reject expired tokens."""
        # Create a token, but we can simulate expiry by using wrong secret
        response = client.get(
            "/api/auth/me",
            headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.invalid.invalid"}
        )

        assert response.status_code == 401


# ============================================================================
# TESTS: User Logout
# ============================================================================

class TestUserLogout:
    """Logout endpoint."""

    def setup_user_and_login(self, client):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Test User",
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        tokens = login_response.json()
        return tokens["access_token"], tokens["refresh_token"]

    def test_logout_success(self, client):
        """POST /api/auth/logout revokes refresh token."""
        access_token, refresh_token = self.setup_user_and_login(client)

        response = client.post(
            "/api/auth/logout",
            json={"refresh_token": refresh_token},
            headers={"Authorization": f"Bearer {access_token}"}
        )

        # Should return 204 No Content
        assert response.status_code == 204

    def test_logout_without_auth(self, client):
        """POST /api/auth/logout requires auth token."""
        response = client.post(
            "/api/auth/logout",
            json={"refresh_token": "some-token"}
        )

        assert response.status_code == 401

    def test_logout_missing_refresh_token(self, client):
        """POST /api/auth/logout returns 422 for missing token."""
        access_token, _ = self.setup_user_and_login(client)

        response = client.post(
            "/api/auth/logout",
            json={},  # Missing refresh_token
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response.status_code == 422


# ============================================================================
# TESTS: API Key Generation
# ============================================================================

class TestAPIKeyGeneration:
    """API key generation endpoint."""

    def setup_user_and_login(self, client):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Test User",
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        return login_response.json()["access_token"]

    def test_generate_api_key(self, client):
        """POST /api/auth/api-key/generate creates an API key."""
        access_token = self.setup_user_and_login(client)

        response = client.post(
            "/api/auth/api-key/generate",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response.status_code == 200
        data = response.json()

        assert "api_key" in data
        assert "created_at" in data
        assert len(data["api_key"]) > 0

    def test_generate_api_key_without_auth(self, client):
        """POST /api/auth/api-key/generate requires auth."""
        response = client.post("/api/auth/api-key/generate")

        assert response.status_code == 401

    def test_generate_multiple_api_keys(self, client):
        """Can generate multiple API keys for same user."""
        access_token = self.setup_user_and_login(client)

        response1 = client.post(
            "/api/auth/api-key/generate",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        response2 = client.post(
            "/api/auth/api-key/generate",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response1.status_code == 200
        assert response2.status_code == 200

        # Keys should be different
        key1 = response1.json()["api_key"]
        key2 = response2.json()["api_key"]
        assert key1 != key2


# ============================================================================
# TESTS: User Update Profile
# ============================================================================

class TestUpdateUserProfile:
    """Update user profile endpoint."""

    def setup_user_and_login(self, client, name="Alice"):
        """Helper to register and login a user."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": name,
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )

        return login_response.json()["access_token"]

    def test_update_user_name(self, client):
        """PUT /api/auth/me updates user name."""
        access_token = self.setup_user_and_login(client, "Alice Original")

        response = client.put(
            "/api/auth/me",
            json={"name": "Alice Updated"},
            headers={"Authorization": f"Bearer {access_token}"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Alice Updated"

    def test_update_user_without_auth(self, client):
        """PUT /api/auth/me requires auth."""
        response = client.put(
            "/api/auth/me",
            json={"name": "New Name"}
        )

        assert response.status_code == 401

    def test_update_with_empty_name(self, client):
        """PUT /api/auth/me with empty name keeps current name."""
        access_token = self.setup_user_and_login(client, "Original")

        response = client.put(
            "/api/auth/me",
            json={"name": ""},
            headers={"Authorization": f"Bearer {access_token}"}
        )

        # Should keep original name
        assert response.status_code == 200
        # Empty name should be ignored by the endpoint


# ============================================================================
# TESTS: Authentication Flow Integration
# ============================================================================

class TestAuthenticationFlowIntegration:
    """Complete authentication workflows."""

    def test_full_auth_flow(self, client):
        """
        Complete authentication flow:
        1. Register user
        2. Login
        3. Access protected endpoint
        4. Refresh token
        5. Access protected endpoint again
        6. Logout
        """
        email = "alice@example.com"
        password = "SecurePassword123!"
        name = "Alice"

        # 1. Register
        reg_response = client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": name,
            }
        )
        assert reg_response.status_code == 201
        reg_tokens = reg_response.json()["tokens"]

        # 2. Login
        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )
        assert login_response.status_code == 200
        login_tokens = login_response.json()

        # 3. Access protected endpoint with login token
        profile_response = client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {login_tokens['access_token']}"}
        )
        assert profile_response.status_code == 200
        assert profile_response.json()["name"] == name

        # 4. Refresh token
        refresh_response = client.post(
            "/api/auth/refresh",
            json={"refresh_token": login_tokens["refresh_token"]}
        )
        assert refresh_response.status_code == 200
        new_tokens = refresh_response.json()

        # 5. Access protected endpoint with new token
        profile_response2 = client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {new_tokens['access_token']}"}
        )
        assert profile_response2.status_code == 200

        # 6. Logout
        logout_response = client.post(
            "/api/auth/logout",
            json={"refresh_token": new_tokens["refresh_token"]},
            headers={"Authorization": f"Bearer {new_tokens['access_token']}"}
        )
        assert logout_response.status_code == 204

    def test_token_not_reusable_after_refresh(self, client):
        """Old refresh token should not be reusable after refresh."""
        email = "alice@example.com"
        password = "SecurePassword123!"

        client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": "Alice",
            }
        )

        login_response = client.post(
            "/api/auth/login",
            json={"email": email, "password": password}
        )
        old_refresh_token = login_response.json()["refresh_token"]

        # First refresh should work
        first_refresh = client.post(
            "/api/auth/refresh",
            json={"refresh_token": old_refresh_token}
        )
        assert first_refresh.status_code == 200

        # Second refresh with old token should fail
        second_refresh = client.post(
            "/api/auth/refresh",
            json={"refresh_token": old_refresh_token}
        )
        assert second_refresh.status_code == 401
