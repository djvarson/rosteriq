"""
RosterIQ Billing Module

Provides:
- StripeProductCatalog: mapping BillingTier to Stripe price IDs
- Subscription: dataclass for tenant subscriptions
- SubscriptionStore: in-memory CRUD for subscriptions
- enforce_trial_end: check if trial has ended with no active sub
- compute_billable_quantity: return active venue count for usage-based billing

All pricing is per-venue per-month (quantity = number of venues).
Pricing tiers (from tanda_marketplace.py):
  - Startup: $1.50/venue/month
  - Pro: $3.00/venue/month
  - Enterprise: $5.50/venue/month
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Stripe Configuration
# ============================================================================


class StripeProductCatalog:
    """
    Maps BillingTier to Stripe price IDs.

    Reads from environment:
    - STARTUP_PRICE_ID (e.g., "price_startup_monthly")
    - PRO_PRICE_ID (e.g., "price_pro_monthly")
    - ENTERPRISE_PRICE_ID (e.g., "price_enterprise_monthly")

    All prices are per-venue per-month (usage-based billing).
    """

    @staticmethod
    def get_price_id(tier: str) -> Optional[str]:
        """
        Get Stripe price ID for a given tier.

        Args:
            tier: Billing tier (startup/pro/enterprise)

        Returns:
            Price ID string, or None if not configured
        """
        tier_lower = tier.lower() if isinstance(tier, str) else str(tier)

        if tier_lower == "startup":
            return os.getenv("STARTUP_PRICE_ID")
        elif tier_lower == "pro":
            return os.getenv("PRO_PRICE_ID")
        elif tier_lower == "enterprise":
            return os.getenv("ENTERPRISE_PRICE_ID")
        else:
            logger.warning(f"Unknown billing tier: {tier}")
            return None


# ============================================================================
# Subscription Data Models
# ============================================================================


class SubscriptionStatus(str, Enum):
    """Subscription status."""
    ACTIVE = "active"
    TRIALING = "trialing"
    PAST_DUE = "past_due"
    CANCELED = "canceled"
    INCOMPLETE = "incomplete"


@dataclass
class Subscription:
    """
    Represents a tenant's Stripe subscription.

    Attributes:
        tenant_id: Tenant owning this subscription
        stripe_subscription_id: Stripe subscription ID
        stripe_customer_id: Stripe customer ID for this tenant
        status: Subscription status (active/trialing/past_due/canceled/incomplete)
        tier: Billing tier (startup/pro/enterprise)
        current_period_end: When current billing period ends
        trial_ends_at: When trial period ends (None if not in trial)
        quantity: Number of venues (basis for monthly charge)
        created_at: When subscription was created
        updated_at: Last update timestamp
    """
    tenant_id: str
    stripe_subscription_id: str
    stripe_customer_id: str
    status: SubscriptionStatus
    tier: str  # "startup" | "pro" | "enterprise"
    current_period_end: datetime
    quantity: int
    created_at: datetime
    updated_at: datetime
    trial_ends_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "tenant_id": self.tenant_id,
            "stripe_subscription_id": self.stripe_subscription_id,
            "stripe_customer_id": self.stripe_customer_id,
            "status": self.status.value,
            "tier": self.tier,
            "current_period_end": self.current_period_end.isoformat(),
            "trial_ends_at": self.trial_ends_at.isoformat() if self.trial_ends_at else None,
            "quantity": self.quantity,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Subscription:
        """Reconstruct Subscription from dict."""
        if isinstance(data.get("status"), str):
            data["status"] = SubscriptionStatus(data["status"])

        for field_name in ["current_period_end", "trial_ends_at", "created_at", "updated_at"]:
            if field_name in data and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])

        return cls(**data)


# ============================================================================
# Subscription Store (in-memory, stdlib-only)
# ============================================================================


class SubscriptionStore:
    """
    In-memory store for subscriptions, keyed by tenant_id.

    All operations are in-memory; for production, replace with database.
    """

    def __init__(self):
        """Initialize empty store."""
        self._subscriptions: dict[str, Subscription] = {}  # tenant_id -> Subscription
        self._stripe_id_index: dict[str, str] = {}  # stripe_subscription_id -> tenant_id

    def create(self, subscription: Subscription) -> Subscription:
        """
        Create a new subscription.

        Args:
            subscription: Subscription to create

        Returns:
            Created Subscription

        Raises:
            ValueError: If tenant_id already has a subscription
        """
        if subscription.tenant_id in self._subscriptions:
            raise ValueError(f"Tenant {subscription.tenant_id} already has a subscription")

        self._subscriptions[subscription.tenant_id] = subscription
        self._stripe_id_index[subscription.stripe_subscription_id] = subscription.tenant_id

        logger.info(f"Created subscription {subscription.stripe_subscription_id} for tenant {subscription.tenant_id}")
        return subscription

    def get(self, tenant_id: str) -> Optional[Subscription]:
        """Get subscription by tenant_id."""
        return self._subscriptions.get(tenant_id)

    def find_by_stripe_id(self, stripe_subscription_id: str) -> Optional[Subscription]:
        """Find subscription by Stripe subscription ID."""
        tenant_id = self._stripe_id_index.get(stripe_subscription_id)
        if tenant_id:
            return self._subscriptions.get(tenant_id)
        return None

    def list_all(self) -> list[Subscription]:
        """List all subscriptions."""
        return list(self._subscriptions.values())

    def update(self, tenant_id: str, **fields) -> Optional[Subscription]:
        """
        Update a subscription's fields.

        Args:
            tenant_id: Tenant to update
            **fields: Fields to update (status, tier, current_period_end, quantity, etc.)

        Returns:
            Updated Subscription, or None if not found
        """
        sub = self.get(tenant_id)
        if not sub:
            return None

        # Build updated dict
        data = {
            "tenant_id": sub.tenant_id,
            "stripe_subscription_id": sub.stripe_subscription_id,
            "stripe_customer_id": sub.stripe_customer_id,
            "status": fields.get("status", sub.status),
            "tier": fields.get("tier", sub.tier),
            "current_period_end": fields.get("current_period_end", sub.current_period_end),
            "trial_ends_at": fields.get("trial_ends_at", sub.trial_ends_at),
            "quantity": fields.get("quantity", sub.quantity),
            "created_at": sub.created_at,
            "updated_at": datetime.now(timezone.utc),
        }

        sub = Subscription(**data)
        self._subscriptions[tenant_id] = sub
        logger.info(f"Updated subscription for tenant {tenant_id}: {list(fields.keys())}")
        return sub

    def delete(self, tenant_id: str) -> bool:
        """
        Delete a subscription.

        Args:
            tenant_id: Tenant to delete subscription for

        Returns:
            True if deleted, False if not found
        """
        sub = self._subscriptions.pop(tenant_id, None)
        if sub:
            self._stripe_id_index.pop(sub.stripe_subscription_id, None)
            logger.info(f"Deleted subscription for tenant {tenant_id}")
            return True
        return False

    def clear(self) -> None:
        """Clear all subscriptions (for testing)."""
        self._subscriptions.clear()
        self._stripe_id_index.clear()
        logger.debug("Cleared all subscriptions")


# ============================================================================
# Module Singleton
# ============================================================================

_subscription_store_instance: Optional[SubscriptionStore] = None


def get_subscription_store() -> SubscriptionStore:
    """
    Get or create the module-level SubscriptionStore singleton.

    Returns:
        Global SubscriptionStore instance
    """
    global _subscription_store_instance
    if _subscription_store_instance is None:
        _subscription_store_instance = SubscriptionStore()
    return _subscription_store_instance


# ============================================================================
# Trial Enforcement
# ============================================================================


def enforce_trial_end(tenant: Any) -> bool:
    """
    Check if a tenant's trial has ended and there's no active subscription.

    Call this from route guards to block premium features for locked-out tenants.

    Args:
        tenant: Tenant object (must have trial_ends_at attribute)

    Returns:
        True if trial has ended AND no active subscription (tenant is locked out)
        False if trial is active OR subscription is active
    """
    from rosteriq.tenants import TenantStatus

    # If tenant status is suspended, they're definitely locked out
    if hasattr(tenant, "status") and tenant.status == TenantStatus.SUSPENDED:
        return True

    # Check if trial has ended
    if hasattr(tenant, "trial_ends_at") and tenant.trial_ends_at:
        if datetime.now(timezone.utc) > tenant.trial_ends_at:
            # Trial has ended; check for active subscription
            store = get_subscription_store()
            sub = store.get(tenant.tenant_id)
            if not sub or sub.status != SubscriptionStatus.ACTIVE:
                logger.warning(f"Tenant {tenant.tenant_id} trial ended with no active subscription")
                return True

    return False


# ============================================================================
# Billable Quantity
# ============================================================================


def compute_billable_quantity(tenant: Any) -> int:
    """
    Compute the billable quantity for a tenant (number of active venues).

    This is the basis for usage-based billing: charge = tier_price * quantity.

    Args:
        tenant: Tenant object (must have venue_ids attribute)

    Returns:
        Number of venues (active venue count)
    """
    if hasattr(tenant, "venue_ids") and tenant.venue_ids:
        return len(tenant.venue_ids)
    return 0


if __name__ == "__main__":
    # Quick smoke test
    print("Testing billing module...")

    # Test StripeProductCatalog
    price = StripeProductCatalog.get_price_id("startup")
    print(f"Startup price ID (env var): {price}")

    # Test Subscription creation
    now = datetime.now(timezone.utc)
    sub = Subscription(
        tenant_id="test-tenant-1",
        stripe_subscription_id="sub_123abc",
        stripe_customer_id="cus_123abc",
        status=SubscriptionStatus.ACTIVE,
        tier="pro",
        current_period_end=now,
        quantity=2,
        created_at=now,
        updated_at=now,
    )
    print(f"Created subscription: {sub.stripe_subscription_id}")

    # Test SubscriptionStore
    store = get_subscription_store()
    store.create(sub)
    retrieved = store.get("test-tenant-1")
    print(f"Retrieved subscription: {retrieved.stripe_subscription_id}")

    # Test find_by_stripe_id
    found = store.find_by_stripe_id("sub_123abc")
    print(f"Found by Stripe ID: {found.tenant_id}")

    print("All smoke tests passed!")
