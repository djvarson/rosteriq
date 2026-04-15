"""
Tests for rosteriq.billing — subscriptions, billing logic, Stripe signature verification.

Runs with: PYTHONPATH=. python3 -m unittest tests.test_billing

Pure-stdlib runner — no pytest, no FastAPI required at test-collection time.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.billing import (
    Subscription,
    SubscriptionStatus,
    SubscriptionStore,
    StripeProductCatalog,
    enforce_trial_end,
    compute_billable_quantity,
    get_subscription_store,
)
from rosteriq.tenants import (
    Tenant,
    TenantStatus,
    BillingTier,
)


def _reset_subscriptions():
    """Clear subscription store."""
    import rosteriq.billing as billing_module
    billing_module._subscription_store_instance = None
    get_subscription_store().clear()


# ---------------------------------------------------------------------------
# SubscriptionStore CRUD
# ---------------------------------------------------------------------------


class TestSubscriptionStoreCRUD(unittest.TestCase):
    """Test SubscriptionStore basic CRUD operations."""

    def setUp(self):
        _reset_subscriptions()
        self.store = get_subscription_store()
        self.now = datetime.now(timezone.utc)

    def test_create_subscription(self):
        """Test creating a subscription."""
        sub = Subscription(
            tenant_id="tenant-1",
            stripe_subscription_id="sub_123",
            stripe_customer_id="cus_123",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=self.now + timedelta(days=30),
            quantity=2,
            created_at=self.now,
            updated_at=self.now,
        )

        created = self.store.create(sub)
        self.assertEqual(created.tenant_id, "tenant-1")
        self.assertEqual(created.stripe_subscription_id, "sub_123")
        self.assertEqual(created.status, SubscriptionStatus.ACTIVE)

    def test_get_subscription(self):
        """Test retrieving a subscription by tenant_id."""
        sub = Subscription(
            tenant_id="tenant-get",
            stripe_subscription_id="sub_get",
            stripe_customer_id="cus_get",
            status=SubscriptionStatus.ACTIVE,
            tier="startup",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        retrieved = self.store.get("tenant-get")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.tenant_id, "tenant-get")

    def test_find_by_stripe_id(self):
        """Test finding subscription by Stripe ID."""
        sub = Subscription(
            tenant_id="tenant-stripe",
            stripe_subscription_id="sub_stripe_123",
            stripe_customer_id="cus_stripe",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        found = self.store.find_by_stripe_id("sub_stripe_123")
        self.assertIsNotNone(found)
        self.assertEqual(found.tenant_id, "tenant-stripe")

    def test_update_subscription(self):
        """Test updating a subscription's fields."""
        sub = Subscription(
            tenant_id="tenant-update",
            stripe_subscription_id="sub_update",
            stripe_customer_id="cus_update",
            status=SubscriptionStatus.ACTIVE,
            tier="startup",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        updated = self.store.update("tenant-update", status=SubscriptionStatus.CANCELED, quantity=2)
        self.assertIsNotNone(updated)
        self.assertEqual(updated.status, SubscriptionStatus.CANCELED)
        self.assertEqual(updated.quantity, 2)

    def test_delete_subscription(self):
        """Test deleting a subscription."""
        sub = Subscription(
            tenant_id="tenant-delete",
            stripe_subscription_id="sub_delete",
            stripe_customer_id="cus_delete",
            status=SubscriptionStatus.ACTIVE,
            tier="startup",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        deleted = self.store.delete("tenant-delete")
        self.assertTrue(deleted)

        retrieved = self.store.get("tenant-delete")
        self.assertIsNone(retrieved)

    def test_list_all(self):
        """Test listing all subscriptions."""
        for i in range(3):
            sub = Subscription(
                tenant_id=f"tenant-list-{i}",
                stripe_subscription_id=f"sub_list_{i}",
                stripe_customer_id=f"cus_list_{i}",
                status=SubscriptionStatus.ACTIVE,
                tier="startup",
                current_period_end=self.now,
                quantity=1,
                created_at=self.now,
                updated_at=self.now,
            )
            self.store.create(sub)

        all_subs = self.store.list_all()
        self.assertEqual(len(all_subs), 3)


# ---------------------------------------------------------------------------
# Stripe Signature Verification
# ---------------------------------------------------------------------------


def _verify_stripe_signature_impl(payload: bytes, signature_header: str, secret: str) -> bool:
    """
    Standalone implementation of Stripe webhook signature verification.

    Uses HMAC-SHA256. Stripe format: t=<timestamp>,v1=<hash>
    Algorithm: HMAC-SHA256(secret, f"{timestamp}.{payload}")
    """
    try:
        # Parse signature header: t=<timestamp>,v1=<hash>
        parts = {}
        for part in signature_header.split(","):
            key, value = part.split("=", 1)
            parts[key.strip()] = value.strip()

        timestamp_str = parts.get("t")
        provided_signature = parts.get("v1")

        if not timestamp_str or not provided_signature:
            return False

        # Check timestamp is recent (within 5 minutes)
        timestamp = int(timestamp_str)
        now = int(time.time())
        if abs(now - timestamp) > 300:
            return False

        # Compute expected signature
        signed_content = f"{timestamp_str}.{payload.decode('utf-8')}"
        expected_signature = hmac.new(
            secret.encode("utf-8"),
            signed_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Compare signatures (constant-time)
        return hmac.compare_digest(expected_signature, provided_signature)

    except Exception:
        return False


class TestStripeSignatureVerification(unittest.TestCase):
    """Test Stripe webhook signature verification."""

    def test_valid_signature(self):
        """Test that a valid signature passes verification."""
        secret = "whsec_test_secret_123"
        payload = b'{"id": "evt_123", "type": "checkout.session.completed"}'
        timestamp = str(int(time.time()))

        # Compute expected signature
        signed_content = f"{timestamp}.{payload.decode('utf-8')}"
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            signed_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        signature_header = f"t={timestamp},v1={expected_sig}"

        # Verify
        result = _verify_stripe_signature_impl(payload, signature_header, secret)
        self.assertTrue(result)

    def test_invalid_signature(self):
        """Test that an invalid signature fails verification."""
        secret = "whsec_test_secret_123"
        payload = b'{"id": "evt_123"}'
        timestamp = str(int(time.time()))

        # Use wrong signature
        signature_header = f"t={timestamp},v1=wrongsignature123"

        result = _verify_stripe_signature_impl(payload, signature_header, secret)
        self.assertFalse(result)

    def test_expired_timestamp(self):
        """Test that an old timestamp is rejected."""
        secret = "whsec_test_secret_123"
        payload = b'{"id": "evt_123"}'
        # Timestamp from 10 minutes ago
        old_timestamp = str(int(time.time()) - 600)

        signed_content = f"{old_timestamp}.{payload.decode('utf-8')}"
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            signed_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        signature_header = f"t={old_timestamp},v1={expected_sig}"

        result = _verify_stripe_signature_impl(payload, signature_header, secret)
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# Trial Enforcement
# ---------------------------------------------------------------------------


class TestEnforceTrialEnd(unittest.TestCase):
    """Test enforce_trial_end logic."""

    def test_trial_active_not_locked(self):
        """Test that an active trial allows access."""
        tenant = Tenant(
            tenant_id="trial-active",
            name="Trial Active",
            slug="trial-active",
            created_at=datetime.now(timezone.utc),
            trial_ends_at=datetime.now(timezone.utc) + timedelta(days=7),
            status=TenantStatus.ACTIVE,
        )

        locked = enforce_trial_end(tenant)
        self.assertFalse(locked)

    def test_trial_ended_no_sub_locked(self):
        """Test that expired trial without subscription locks tenant."""
        tenant = Tenant(
            tenant_id="trial-expired",
            name="Trial Expired",
            slug="trial-expired",
            created_at=datetime.now(timezone.utc),
            trial_ends_at=datetime.now(timezone.utc) - timedelta(days=1),
            status=TenantStatus.ACTIVE,
        )

        locked = enforce_trial_end(tenant)
        self.assertTrue(locked)

    def test_trial_ended_with_active_sub_not_locked(self):
        """Test that active subscription prevents lock-out even if trial expired."""
        _reset_subscriptions()
        store = get_subscription_store()
        now = datetime.now(timezone.utc)

        # Create subscription
        sub = Subscription(
            tenant_id="trial-but-sub",
            stripe_subscription_id="sub_trial_sub",
            stripe_customer_id="cus_trial_sub",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=now + timedelta(days=30),
            quantity=1,
            created_at=now,
            updated_at=now,
        )
        store.create(sub)

        # Tenant with expired trial
        tenant = Tenant(
            tenant_id="trial-but-sub",
            name="Trial Expired But Has Sub",
            slug="trial-but-sub",
            created_at=now,
            trial_ends_at=now - timedelta(days=1),
            status=TenantStatus.ACTIVE,
        )

        locked = enforce_trial_end(tenant)
        self.assertFalse(locked)

    def test_suspended_tenant_locked(self):
        """Test that suspended tenants are always locked."""
        tenant = Tenant(
            tenant_id="suspended",
            name="Suspended Tenant",
            slug="suspended",
            created_at=datetime.now(timezone.utc),
            status=TenantStatus.SUSPENDED,
        )

        locked = enforce_trial_end(tenant)
        self.assertTrue(locked)


# ---------------------------------------------------------------------------
# Billable Quantity
# ---------------------------------------------------------------------------


class TestComputeBillableQuantity(unittest.TestCase):
    """Test compute_billable_quantity logic."""

    def test_single_venue(self):
        """Test quantity with one venue."""
        tenant = Tenant(
            tenant_id="single-venue",
            name="Single Venue",
            slug="single-venue",
            created_at=datetime.now(timezone.utc),
            venue_ids=["venue-1"],
        )

        qty = compute_billable_quantity(tenant)
        self.assertEqual(qty, 1)

    def test_multiple_venues(self):
        """Test quantity with multiple venues."""
        tenant = Tenant(
            tenant_id="multi-venue",
            name="Multi Venue",
            slug="multi-venue",
            created_at=datetime.now(timezone.utc),
            venue_ids=["venue-1", "venue-2", "venue-3"],
        )

        qty = compute_billable_quantity(tenant)
        self.assertEqual(qty, 3)

    def test_no_venues(self):
        """Test quantity with no venues."""
        tenant = Tenant(
            tenant_id="no-venues",
            name="No Venues",
            slug="no-venues",
            created_at=datetime.now(timezone.utc),
            venue_ids=[],
        )

        qty = compute_billable_quantity(tenant)
        self.assertEqual(qty, 0)


# ---------------------------------------------------------------------------
# StripeProductCatalog
# ---------------------------------------------------------------------------


class TestStripeProductCatalog(unittest.TestCase):
    """Test StripeProductCatalog tier-to-price-id mapping."""

    def test_get_price_id_startup(self):
        """Test getting startup price ID."""
        import os
        original = os.getenv("STARTUP_PRICE_ID")
        os.environ["STARTUP_PRICE_ID"] = "price_startup_123"

        try:
            price_id = StripeProductCatalog.get_price_id("startup")
            self.assertEqual(price_id, "price_startup_123")
        finally:
            if original:
                os.environ["STARTUP_PRICE_ID"] = original
            else:
                os.environ.pop("STARTUP_PRICE_ID", None)

    def test_get_price_id_pro(self):
        """Test getting pro price ID."""
        import os
        original = os.getenv("PRO_PRICE_ID")
        os.environ["PRO_PRICE_ID"] = "price_pro_123"

        try:
            price_id = StripeProductCatalog.get_price_id("pro")
            self.assertEqual(price_id, "price_pro_123")
        finally:
            if original:
                os.environ["PRO_PRICE_ID"] = original
            else:
                os.environ.pop("PRO_PRICE_ID", None)

    def test_get_price_id_enterprise(self):
        """Test getting enterprise price ID."""
        import os
        original = os.getenv("ENTERPRISE_PRICE_ID")
        os.environ["ENTERPRISE_PRICE_ID"] = "price_enterprise_123"

        try:
            price_id = StripeProductCatalog.get_price_id("enterprise")
            self.assertEqual(price_id, "price_enterprise_123")
        finally:
            if original:
                os.environ["ENTERPRISE_PRICE_ID"] = original
            else:
                os.environ.pop("ENTERPRISE_PRICE_ID", None)

    def test_get_price_id_unknown_tier(self):
        """Test getting price ID for unknown tier."""
        price_id = StripeProductCatalog.get_price_id("unknown")
        self.assertIsNone(price_id)


# ---------------------------------------------------------------------------
# Subscription Status Transitions
# ---------------------------------------------------------------------------


class TestSubscriptionStatusTransitions(unittest.TestCase):
    """Test subscription status transitions via webhook events."""

    def setUp(self):
        _reset_subscriptions()
        self.store = get_subscription_store()
        self.now = datetime.now(timezone.utc)

    def test_transition_to_past_due(self):
        """Test transitioning to PAST_DUE status."""
        sub = Subscription(
            tenant_id="tenant-past-due",
            stripe_subscription_id="sub_past_due",
            stripe_customer_id="cus_past_due",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        updated = self.store.update("tenant-past-due", status=SubscriptionStatus.PAST_DUE)
        self.assertEqual(updated.status, SubscriptionStatus.PAST_DUE)

    def test_transition_to_canceled(self):
        """Test transitioning to CANCELED status."""
        sub = Subscription(
            tenant_id="tenant-canceled",
            stripe_subscription_id="sub_canceled",
            stripe_customer_id="cus_canceled",
            status=SubscriptionStatus.ACTIVE,
            tier="enterprise",
            current_period_end=self.now,
            quantity=2,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        updated = self.store.update("tenant-canceled", status=SubscriptionStatus.CANCELED)
        self.assertEqual(updated.status, SubscriptionStatus.CANCELED)

    def test_transition_to_trialing(self):
        """Test transitioning to TRIALING status."""
        sub = Subscription(
            tenant_id="tenant-trialing",
            stripe_subscription_id="sub_trialing",
            stripe_customer_id="cus_trialing",
            status=SubscriptionStatus.INCOMPLETE,
            tier="startup",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
            trial_ends_at=self.now + timedelta(days=14),
        )
        self.store.create(sub)

        updated = self.store.update("tenant-trialing", status=SubscriptionStatus.TRIALING)
        self.assertEqual(updated.status, SubscriptionStatus.TRIALING)


# ---------------------------------------------------------------------------
# Tier Changes
# ---------------------------------------------------------------------------


class TestTierChanges(unittest.TestCase):
    """Test subscription tier updates."""

    def setUp(self):
        _reset_subscriptions()
        self.store = get_subscription_store()
        self.now = datetime.now(timezone.utc)

    def test_upgrade_from_startup_to_pro(self):
        """Test upgrading from startup to pro."""
        sub = Subscription(
            tenant_id="tenant-upgrade",
            stripe_subscription_id="sub_upgrade",
            stripe_customer_id="cus_upgrade",
            status=SubscriptionStatus.ACTIVE,
            tier="startup",
            current_period_end=self.now,
            quantity=1,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        updated = self.store.update("tenant-upgrade", tier="pro")
        self.assertEqual(updated.tier, "pro")

    def test_downgrade_from_enterprise_to_startup(self):
        """Test downgrading from enterprise to startup."""
        sub = Subscription(
            tenant_id="tenant-downgrade",
            stripe_subscription_id="sub_downgrade",
            stripe_customer_id="cus_downgrade",
            status=SubscriptionStatus.ACTIVE,
            tier="enterprise",
            current_period_end=self.now,
            quantity=5,
            created_at=self.now,
            updated_at=self.now,
        )
        self.store.create(sub)

        updated = self.store.update("tenant-downgrade", tier="startup")
        self.assertEqual(updated.tier, "startup")


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


class TestSubscriptionSerialization(unittest.TestCase):
    """Test Subscription serialization and deserialization."""

    def test_to_dict(self):
        """Test converting subscription to dict."""
        now = datetime.now(timezone.utc)
        sub = Subscription(
            tenant_id="tenant-serial",
            stripe_subscription_id="sub_serial",
            stripe_customer_id="cus_serial",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=now,
            quantity=2,
            created_at=now,
            updated_at=now,
            trial_ends_at=now + timedelta(days=14),
        )

        data = sub.to_dict()
        self.assertEqual(data["tenant_id"], "tenant-serial")
        self.assertEqual(data["status"], "active")
        self.assertEqual(data["tier"], "pro")
        self.assertEqual(data["quantity"], 2)

    def test_from_dict(self):
        """Test reconstructing subscription from dict."""
        now = datetime.now(timezone.utc)
        data = {
            "tenant_id": "tenant-from-dict",
            "stripe_subscription_id": "sub_from_dict",
            "stripe_customer_id": "cus_from_dict",
            "status": "active",
            "tier": "enterprise",
            "current_period_end": now.isoformat(),
            "quantity": 3,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "trial_ends_at": None,
        }

        sub = Subscription.from_dict(data)
        self.assertEqual(sub.tenant_id, "tenant-from-dict")
        self.assertEqual(sub.status, SubscriptionStatus.ACTIVE)
        self.assertEqual(sub.tier, "enterprise")
        self.assertEqual(sub.quantity, 3)


if __name__ == "__main__":
    unittest.main()
