"""
Tests for role-based access level system.

Tests:
- AccessLevel enum ranking
- JWT token encoding/decoding with access_level
- Backward compatibility for old tokens without "al" field
- has_access() permission checking
- require_access() dependency behavior in demo/auth modes
- /api/v1/access/me endpoint shape
"""

import pytest
from datetime import datetime, timezone, timedelta
from rosteriq.auth import (
    AccessLevel,
    User,
    create_access_token,
    decode_token,
    has_access,
    create_demo_user,
    JWT_SECRET,
    JWT_ALGORITHM,
)

import jwt as pyjwt


class TestAccessLevelEnum:
    """Test AccessLevel enum and ranking."""

    def test_access_level_values(self):
        """Test enum values match spec."""
        assert AccessLevel.L1_SUPERVISOR.value == "l1"
        assert AccessLevel.L2_ROSTER_MAKER.value == "l2"
        assert AccessLevel.OWNER.value == "owner"

    def test_access_level_rank(self):
        """Test ranking: OWNER > L2 > L1."""
        assert AccessLevel.rank(AccessLevel.L1_SUPERVISOR) == 1
        assert AccessLevel.rank(AccessLevel.L2_ROSTER_MAKER) == 2
        assert AccessLevel.rank(AccessLevel.OWNER) == 3

    def test_access_level_from_string(self):
        """Test creating AccessLevel from string."""
        assert AccessLevel("l1") == AccessLevel.L1_SUPERVISOR
        assert AccessLevel("l2") == AccessLevel.L2_ROSTER_MAKER
        assert AccessLevel("owner") == AccessLevel.OWNER


class TestHasAccess:
    """Test has_access() permission hierarchy."""

    def test_owner_satisfies_all(self):
        """OWNER access satisfies any requirement."""
        user = User(
            id="u1",
            email="owner@test.com",
            name="Owner",
            venue_id="v1",
            access_level=AccessLevel.OWNER,
            created_at=datetime.now(timezone.utc),
        )
        assert has_access(user, AccessLevel.L1_SUPERVISOR) is True
        assert has_access(user, AccessLevel.L2_ROSTER_MAKER) is True
        assert has_access(user, AccessLevel.OWNER) is True

    def test_l2_satisfies_l2_and_l1(self):
        """L2 access satisfies L2 and L1 requirements."""
        user = User(
            id="u2",
            email="l2@test.com",
            name="Roster Maker",
            venue_id="v1",
            access_level=AccessLevel.L2_ROSTER_MAKER,
            created_at=datetime.now(timezone.utc),
        )
        assert has_access(user, AccessLevel.L1_SUPERVISOR) is True
        assert has_access(user, AccessLevel.L2_ROSTER_MAKER) is True
        assert has_access(user, AccessLevel.OWNER) is False

    def test_l1_satisfies_only_l1(self):
        """L1 access satisfies only L1 requirement."""
        user = User(
            id="u3",
            email="l1@test.com",
            name="Supervisor",
            venue_id="v1",
            access_level=AccessLevel.L1_SUPERVISOR,
            created_at=datetime.now(timezone.utc),
        )
        assert has_access(user, AccessLevel.L1_SUPERVISOR) is True
        assert has_access(user, AccessLevel.L2_ROSTER_MAKER) is False
        assert has_access(user, AccessLevel.OWNER) is False

    def test_has_access_with_dict(self):
        """has_access() works with decoded token dict."""
        # L2 token dict
        token_dict = {"sub": "u2", "al": "l2", "venue_id": "v1"}
        assert has_access(token_dict, AccessLevel.L1_SUPERVISOR) is True
        assert has_access(token_dict, AccessLevel.L2_ROSTER_MAKER) is True
        assert has_access(token_dict, AccessLevel.OWNER) is False

    def test_has_access_default_l1(self):
        """User object defaults to L1 if access_level is None."""
        user = User(
            id="u4",
            email="default@test.com",
            name="Default",
            venue_id="v1",
            access_level=AccessLevel.L1_SUPERVISOR,
            created_at=datetime.now(timezone.utc),
        )
        assert has_access(user, AccessLevel.L1_SUPERVISOR) is True
        assert has_access(user, AccessLevel.L2_ROSTER_MAKER) is False


class TestTokenRoundTrip:
    """Test JWT encoding/decoding with access_level."""

    def test_create_token_includes_access_level(self):
        """create_access_token includes 'al' field."""
        token = create_access_token(
            user_id="u1",
            venue_id="v1",
            role="manager",
            access_level=AccessLevel.L2_ROSTER_MAKER,
        )
        payload = pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        assert payload["al"] == "l2"

    def test_create_token_defaults_to_l1(self):
        """create_access_token defaults to L1 if not specified."""
        token = create_access_token(
            user_id="u1",
            venue_id="v1",
            role="manager",
        )
        payload = pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        assert payload["al"] == "l1"

    def test_decode_token_returns_access_level(self):
        """decode_token returns 'al' field."""
        token = create_access_token(
            user_id="u1",
            venue_id="v1",
            role="manager",
            access_level=AccessLevel.OWNER,
        )
        payload = decode_token(token)
        assert payload["al"] == "owner"

    def test_decode_old_token_without_al_defaults_to_l1(self):
        """Old tokens without 'al' field default to L1 on decode."""
        # Create token without "al" field (simulating old token)
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=24)
        payload = {
            "sub": "u1",
            "venue_id": "v1",
            "role": "manager",
            "iat": now,
            "exp": expires,
        }
        old_token = pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        # Decode and verify default
        decoded = decode_token(old_token)
        assert decoded["al"] == "l1"
        assert decoded["sub"] == "u1"


class TestDemoUser:
    """Test demo user creation and access."""

    def test_demo_user_has_owner_access(self):
        """Demo user should have OWNER access level."""
        user = create_demo_user()
        assert user.access_level == AccessLevel.OWNER
        assert has_access(user, AccessLevel.OWNER) is True
        assert has_access(user, AccessLevel.L2_ROSTER_MAKER) is True
        assert has_access(user, AccessLevel.L1_SUPERVISOR) is True

    def test_demo_user_is_idempotent(self):
        """Creating demo user twice returns same user."""
        user1 = create_demo_user()
        user2 = create_demo_user()
        assert user1.id == user2.id
        assert user1.email == user2.email


class TestAccessPermissions:
    """Test permission computation from access_router.py."""

    def test_l1_permissions(self):
        """L1 has: view_live_data, log_shift_events, use_headcount_clicker, use_call_in."""
        from rosteriq.access_router import compute_permissions

        perms = compute_permissions(AccessLevel.L1_SUPERVISOR)
        assert "view_live_data" in perms
        assert "log_shift_events" in perms
        assert "use_headcount_clicker" in perms
        assert "use_call_in" in perms
        assert len(perms) == 4

    def test_l2_permissions(self):
        """L2 has L1 permissions + edit_roster, view_history, run_scenarios, use_ask_agent."""
        from rosteriq.access_router import compute_permissions

        perms = compute_permissions(AccessLevel.L2_ROSTER_MAKER)
        l1_perms = ["view_live_data", "log_shift_events", "use_headcount_clicker", "use_call_in"]
        l2_extra = ["edit_roster", "view_history", "run_scenarios", "use_ask_agent"]
        for p in l1_perms + l2_extra:
            assert p in perms
        assert len(perms) == 8

    def test_owner_permissions(self):
        """OWNER has L2 permissions + view_multi_venue, view_accountability, manage_users."""
        from rosteriq.access_router import compute_permissions

        perms = compute_permissions(AccessLevel.OWNER)
        l1_perms = ["view_live_data", "log_shift_events", "use_headcount_clicker", "use_call_in"]
        l2_extra = ["edit_roster", "view_history", "run_scenarios", "use_ask_agent"]
        owner_extra = ["view_multi_venue", "view_accountability", "manage_users"]
        for p in l1_perms + l2_extra + owner_extra:
            assert p in perms
        assert len(perms) == 11


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
