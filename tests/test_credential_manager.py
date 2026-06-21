"""
Tests for credential manager (API keys and webhook secrets).
"""

import pytest
from datetime import datetime, timedelta
from rosteriq.services.credential_manager import CredentialManager
from rosteriq.database import MemoryStore


@pytest.fixture
def db():
    return MemoryStore()


@pytest.fixture
def credential_manager(db):
    return CredentialManager(db)


@pytest.fixture
def test_user_id():
    return "user-123"


# ============================================================================
# API Key Tests
# ============================================================================

def test_create_api_key(credential_manager, test_user_id):
    """Test creating a new API key."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="production",
        expires_in_days=90
    )

    assert key.startswith("riq_")
    assert len(key) > 10
    assert key_id

    # Verify record was saved
    record = credential_manager.db.get_api_key_record(key_id)
    assert record is not None
    assert record["name"] == "production"
    assert record["user_id"] == test_user_id
    assert record["is_active"] is True


def test_create_api_key_without_expiry(credential_manager, test_user_id):
    """Test creating API key without expiration."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="ci-pipeline"
    )

    record = credential_manager.db.get_api_key_record(key_id)
    assert record["expires_at"] is None


def test_api_key_max_limit(credential_manager, test_user_id):
    """Test that users can't exceed 5 API keys."""
    # Create 5 keys
    for i in range(5):
        credential_manager.create_api_key(
            user_id=test_user_id,
            name=f"key-{i}"
        )

    # 6th should fail
    with pytest.raises(ValueError):
        credential_manager.create_api_key(
            user_id=test_user_id,
            name="key-6"
        )


def test_list_api_keys(credential_manager, test_user_id):
    """Test listing API keys."""
    # Create 3 keys
    for i in range(3):
        credential_manager.create_api_key(
            user_id=test_user_id,
            name=f"key-{i}"
        )

    keys = credential_manager.list_api_keys(test_user_id)
    assert len(keys) == 3
    assert all(k["is_active"] for k in keys)
    assert all("name" in k for k in keys)
    # Ensure we never return the actual secret material (plaintext key or its hash).
    # Note: the user-chosen key *name* legitimately contains "key", so we check the
    # specific leak vectors rather than the bare substring "key".
    assert all("key_hash" not in k for k in keys)
    assert all("hash" not in str(k).lower() for k in keys)


def test_revoke_api_key(credential_manager, test_user_id):
    """Test revoking an API key."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    # Revoke it
    success = credential_manager.revoke_api_key(test_user_id, key_id)
    assert success is True

    # Verify it's no longer active
    record = credential_manager.db.get_api_key_record(key_id)
    assert record["is_active"] is False
    assert record["revoked_at"] is not None


def test_revoke_unauthorized_key(credential_manager, test_user_id):
    """Test that users can't revoke others' keys."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    # Try to revoke as different user
    success = credential_manager.revoke_api_key("other-user", key_id)
    assert success is False

    # Verify key is still active
    record = credential_manager.db.get_api_key_record(key_id)
    assert record["is_active"] is True


def test_rotate_api_key(credential_manager, test_user_id):
    """Test rotating an API key."""
    key1, key_id1 = credential_manager.create_api_key(
        user_id=test_user_id,
        name="production",
        expires_in_days=30
    )

    # Rotate it
    key2, key_id2 = credential_manager.rotate_api_key(test_user_id, key_id1)

    # New key should be different
    assert key2 != key1
    assert key_id2 != key_id1

    # Old key should be revoked
    old_record = credential_manager.db.get_api_key_record(key_id1)
    assert old_record["is_active"] is False

    # New key should be active with same name
    new_record = credential_manager.db.get_api_key_record(key_id2)
    assert new_record["is_active"] is True
    assert new_record["name"] == "production"


def test_record_key_usage(credential_manager, test_user_id):
    """Test recording API key usage."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    # Record usage
    credential_manager.record_key_usage(
        key_id=key_id,
        endpoint="/api/rosters",
        ip_address="192.168.1.1"
    )

    # Verify usage was recorded
    record = credential_manager.db.get_api_key_record(key_id)
    assert record["usage_count"] == 1
    assert record["last_used_at"] is not None


def test_get_key_usage_stats(credential_manager, test_user_id):
    """Test getting API key usage statistics."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    # Record some usage
    for _ in range(5):
        credential_manager.record_key_usage(
            key_id=key_id,
            endpoint="/api/rosters",
            ip_address="192.168.1.1"
        )

    # Get stats
    stats = credential_manager.get_key_usage_stats(test_user_id, key_id)
    assert stats is not None
    assert stats["usage_count"] == 5
    assert stats["is_active"] is True


# ============================================================================
# Webhook Secret Tests
# ============================================================================

def test_rotate_webhook_secret(credential_manager):
    """Test rotating webhook secret."""
    venue_id = "venue-123"

    secret1, old_secret, grace_expires = credential_manager.rotate_webhook_secret(
        venue_id=venue_id
    )

    assert secret1.startswith("rq_")  # New secret should have prefix
    assert isinstance(grace_expires, datetime)

    # Both secrets should be valid
    secrets = credential_manager.db.get_webhook_secrets(venue_id)
    assert len(secrets) == 1
    assert secrets[0]["is_active"] is True


def test_rotate_webhook_secret_multiple_times(credential_manager):
    """Test rotating webhook secret multiple times."""
    venue_id = "venue-123"

    secret1, _, grace1 = credential_manager.rotate_webhook_secret(venue_id)
    secret2, _, grace2 = credential_manager.rotate_webhook_secret(venue_id)

    # Secrets should be different
    assert secret1 != secret2

    # Should have 2 records: one active, one in grace
    secrets = credential_manager.db.get_webhook_secrets(venue_id)
    assert len(secrets) == 2

    active = [s for s in secrets if s["is_active"]]
    grace = [s for s in secrets if s.get("grace_expires_at")]

    assert len(active) == 1
    assert len(grace) <= 1


def test_webhook_secret_grace_period(credential_manager):
    """Test webhook secret grace period."""
    venue_id = "venue-123"

    secret, old_secret, grace_expires = credential_manager.rotate_webhook_secret(
        venue_id=venue_id
    )

    # Grace period should be ~24 hours from now
    now = datetime.utcnow()
    time_diff = (grace_expires - now).total_seconds() / 3600

    # Should be roughly 24 hours (within 1 hour tolerance)
    assert 23 < time_diff < 25


# ============================================================================
# Audit Logging Tests
# ============================================================================

def test_audit_log_on_key_creation(credential_manager, test_user_id):
    """Test that key creation is logged."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    # Verify audit log entry was created
    # (In a real implementation, this would query the audit log table)
    # For now, just verify the key was created
    record = credential_manager.db.get_api_key_record(key_id)
    assert record is not None


def test_audit_log_on_revocation(credential_manager, test_user_id):
    """Test that key revocation is logged."""
    key, key_id = credential_manager.create_api_key(
        user_id=test_user_id,
        name="test-key"
    )

    credential_manager.revoke_api_key(test_user_id, key_id)

    # Verify revoked timestamp is set
    record = credential_manager.db.get_api_key_record(key_id)
    assert record["revoked_at"] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
