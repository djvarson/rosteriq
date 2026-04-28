"""
Comprehensive tests for webhook processing.

Tests HMAC signature verification and database-backed idempotency.
"""

import sys
import os
import hmac
import hashlib
import json

import pytest

# Ensure rosteriq imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.routes.webhook_routes import (
    verify_hmac_signature,
    is_webhook_duplicate,
)
from rosteriq.database import MemoryStore


class TestHMACSignatureVerification:
    """Test HMAC-SHA256 signature verification."""

    def test_verify_hmac_signature_valid(self):
        """Valid HMAC signature is verified successfully."""
        payload = b'{"event_type": "roster.published", "venue_id": "venue-123"}'
        secret = "my_webhook_secret"

        # Compute expected signature
        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        # Verify it passes
        assert verify_hmac_signature(payload, expected, secret) is True

    def test_verify_hmac_signature_invalid(self):
        """Invalid HMAC signature is rejected."""
        payload = b'{"event_type": "roster.published"}'
        secret = "my_webhook_secret"

        # Use wrong signature
        wrong_signature = "0000000000000000000000000000000000000000000000000000000000000000"

        assert verify_hmac_signature(payload, wrong_signature, secret) is False

    def test_verify_hmac_signature_empty_secret(self):
        """Empty secret returns False."""
        payload = b'{"event_type": "roster.published"}'
        secret = ""

        signature = "valid_signature_string"

        assert verify_hmac_signature(payload, signature, secret) is False

    def test_verify_hmac_signature_empty_signature(self):
        """Empty signature returns False."""
        payload = b'{"event_type": "roster.published"}'
        secret = "my_secret"

        assert verify_hmac_signature(payload, "", secret) is False

    def test_verify_hmac_signature_both_empty(self):
        """Both empty secret and signature return False."""
        payload = b'{"event_type": "roster.published"}'

        assert verify_hmac_signature(payload, "", "") is False

    def test_verify_hmac_signature_different_payload(self):
        """Different payload produces different signature."""
        secret = "my_secret"
        payload1 = b'{"event": "create"}'
        payload2 = b'{"event": "update"}'

        sig1 = hmac.new(secret.encode(), payload1, hashlib.sha256).hexdigest()
        sig2 = hmac.new(secret.encode(), payload2, hashlib.sha256).hexdigest()

        # Same secret, different payloads should produce different signatures
        assert sig1 != sig2

        # Verify payload1 with sig1 should pass
        assert verify_hmac_signature(payload1, sig1, secret) is True
        # Verify payload1 with sig2 should fail
        assert verify_hmac_signature(payload1, sig2, secret) is False

    def test_verify_hmac_signature_case_sensitive(self):
        """Signature verification is case-sensitive."""
        payload = b'{"event_type": "roster.published"}'
        secret = "my_webhook_secret"

        # Generate correct signature
        correct = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        # Uppercase version should fail (case-sensitive comparison)
        assert verify_hmac_signature(payload, correct.upper(), secret) is False

    def test_verify_hmac_signature_tampered_payload(self):
        """Tampered payload fails verification."""
        payload = b'{"event_type": "roster.published", "venue_id": "venue-123"}'
        secret = "my_webhook_secret"

        # Sign original payload
        signature = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        # Tamper with payload
        tampered_payload = b'{"event_type": "roster.published", "venue_id": "venue-999"}'

        # Should fail with tampered payload
        assert verify_hmac_signature(tampered_payload, signature, secret) is False


class TestWebhookIdempotency:
    """Test database-backed webhook idempotency."""

    def test_is_webhook_duplicate_first_call(self, memory_store):
        """First webhook call is not a duplicate."""
        # Mock the get_db function to return our memory_store
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: memory_store

        try:
            webhook_id = "webhook-123"
            payload = b'{"event_type": "roster.published"}'
            event_type = "roster.published"

            result = is_webhook_duplicate(webhook_id, payload, event_type)

            # First call should not be a duplicate
            assert result is False
        finally:
            webhook_module.get_db = original_get_db

    def test_is_webhook_duplicate_second_call(self, memory_store):
        """Second webhook call with same ID is a duplicate."""
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: memory_store

        try:
            webhook_id = "webhook-456"
            payload = b'{"event_type": "roster.published"}'
            event_type = "roster.published"

            # First call
            first_result = is_webhook_duplicate(webhook_id, payload, event_type)
            assert first_result is False

            # Second call with same ID
            second_result = is_webhook_duplicate(webhook_id, payload, event_type)
            assert second_result is True
        finally:
            webhook_module.get_db = original_get_db

    def test_is_webhook_duplicate_different_ids(self, memory_store):
        """Different webhook IDs are not duplicates of each other."""
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: memory_store

        try:
            payload = b'{"event_type": "roster.published"}'
            event_type = "roster.published"

            # First webhook
            result1 = is_webhook_duplicate("webhook-001", payload, event_type)
            assert result1 is False

            # Second webhook (different ID)
            result2 = is_webhook_duplicate("webhook-002", payload, event_type)
            assert result2 is False
        finally:
            webhook_module.get_db = original_get_db

    def test_is_webhook_duplicate_no_database(self):
        """When database is unavailable, idempotency is disabled (returns False)."""
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: None

        try:
            webhook_id = "webhook-789"
            payload = b'{"event_type": "roster.published"}'
            event_type = "roster.published"

            # Should return False (no duplicate tracking) when DB is unavailable
            result = is_webhook_duplicate(webhook_id, payload, event_type)
            assert result is False
        finally:
            webhook_module.get_db = original_get_db

    def test_webhook_event_saved(self, memory_store):
        """Webhook event is saved to database with correct data."""
        webhook_id = "webhook-saved-001"
        payload = b'{"event_type": "shift.created", "venue_id": "v-123"}'
        event_type = "shift.created"

        # Record the webhook
        payload_hash = hashlib.sha256(payload).hexdigest()
        memory_store.save_webhook_event(webhook_id, event_type, payload_hash)

        # Verify it was saved
        assert memory_store.is_webhook_processed(webhook_id) is True

    def test_webhook_event_payload_hash(self, memory_store):
        """Webhook payload hash is computed correctly."""
        payload1 = b'{"event_type": "roster.published"}'
        payload2 = b'{"event_type": "roster.cancelled"}'

        # Different payloads should have different hashes
        hash1 = hashlib.sha256(payload1).hexdigest()
        hash2 = hashlib.sha256(payload2).hexdigest()

        assert hash1 != hash2
        assert len(hash1) == 64  # SHA256 hex is 64 chars
        assert len(hash2) == 64

    def test_webhook_idempotency_multiple_events(self, memory_store):
        """Multiple different webhook events are tracked separately."""
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: memory_store

        try:
            events = [
                ("webhook-a", b'{"type": "roster.published"}', "roster.published"),
                ("webhook-b", b'{"type": "shift.created"}', "shift.created"),
                ("webhook-c", b'{"type": "employee.updated"}', "employee.updated"),
            ]

            # Record all events
            for webhook_id, payload, event_type in events:
                is_webhook_duplicate(webhook_id, payload, event_type)

            # Verify each is recorded and subsequent calls are duplicates
            for webhook_id, payload, event_type in events:
                result = is_webhook_duplicate(webhook_id, payload, event_type)
                assert result is True  # Should be duplicate on second call
        finally:
            webhook_module.get_db = original_get_db

    def test_webhook_event_auto_cleanup(self, memory_store):
        """Webhook event storage auto-cleans when exceeding 10000 events."""
        # This is an integration test of MemoryStore's cleanup mechanism
        # Record many webhook events
        for i in range(10001):
            webhook_id = f"webhook-{i}"
            memory_store.save_webhook_event(
                webhook_id,
                "test_event",
                hashlib.sha256(str(i).encode()).hexdigest()
            )

        # After cleanup, should have approximately 5000 events (half of 10000)
        # In practice, we keep the last 5000 after cleanup triggers at 10000
        events = memory_store._webhook_events
        assert len(events) <= 5000  # Should have cleaned up

    def test_webhook_signature_and_idempotency_together(self, memory_store):
        """HMAC signature and idempotency work together."""
        import rosteriq.routes.webhook_routes as webhook_module
        original_get_db = webhook_module.get_db
        webhook_module.get_db = lambda: memory_store

        try:
            secret = "webhook_secret"
            payload = b'{"event_type": "roster.published", "id": "webhook-123"}'

            # Compute valid signature
            valid_sig = hmac.new(
                secret.encode(),
                payload,
                hashlib.sha256,
            ).hexdigest()

            # Verify signature
            assert verify_hmac_signature(payload, valid_sig, secret) is True

            # Check idempotency
            result1 = is_webhook_duplicate("webhook-123", payload, "roster.published")
            assert result1 is False  # First time

            result2 = is_webhook_duplicate("webhook-123", payload, "roster.published")
            assert result2 is True  # Duplicate

            # Signature remains valid
            assert verify_hmac_signature(payload, valid_sig, secret) is True
        finally:
            webhook_module.get_db = original_get_db
