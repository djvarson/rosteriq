"""
Billing Lifecycle Helper

Provides:
- is_tenant_in_good_standing: check subscription status and trial validity

To be used as a dependency in route guards (future rounds).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def is_tenant_in_good_standing(tenant: Any) -> bool:
    """
    Check if a tenant is in good standing (can access premium features).

    A tenant is in good standing if:
    1. Their subscription is ACTIVE (not canceled, past_due, incomplete, etc.)
    2. OR they are still within their trial period

    Args:
        tenant: Tenant object (must have trial_ends_at, status attributes)

    Returns:
        True if tenant can access premium features, False if locked out
    """
    from rosteriq.billing import get_subscription_store, SubscriptionStatus
    from rosteriq.tenants import TenantStatus

    # Check if tenant is suspended
    if hasattr(tenant, "status") and tenant.status == TenantStatus.SUSPENDED:
        logger.debug(f"Tenant {tenant.tenant_id} is suspended")
        return False

    # Check subscription status
    store = get_subscription_store()
    sub = store.get(tenant.tenant_id)
    if sub and sub.status == SubscriptionStatus.ACTIVE:
        logger.debug(f"Tenant {tenant.tenant_id} has active subscription")
        return True

    # Check trial status
    if hasattr(tenant, "trial_ends_at") and tenant.trial_ends_at:
        if datetime.now(timezone.utc) < tenant.trial_ends_at:
            logger.debug(f"Tenant {tenant.tenant_id} is in trial")
            return True

    logger.debug(f"Tenant {tenant.tenant_id} not in good standing (no active sub or trial)")
    return False


if __name__ == "__main__":
    # Quick smoke test
    print("Testing billing_lifecycle...")
    print("billing_lifecycle module initialized successfully")
