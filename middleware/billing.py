"""
Billing tier gating middleware and dependencies.

Enforces feature access based on subscription tier:
- Starter: basic roster, single venue, email notifications
- Pro: AI optimisation, multi-venue, Xero integration, advanced reports
- Enterprise: API access, webhooks, custom integrations, priority support

Usage:
    from rosteriq.middleware.billing import require_tier
    from fastapi import Depends

    @app.get("/api/advanced")
    async def advanced_feature(
        current_user: UserContext = Depends(get_current_user),
        _: None = Depends(require_tier("pro", "enterprise")),
    ):
        # Only pro and enterprise users can access this
        return {"data": "..."}
"""

from typing import Optional
from functools import lru_cache

from fastapi import Depends, HTTPException, status

from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user
from rosteriq.services.billing import SubscriptionTier, SubscriptionStatus


# ============================================================================
# Tier hierarchy
# ============================================================================

TIER_HIERARCHY = {
    SubscriptionTier.starter: 1,
    SubscriptionTier.pro: 2,
    SubscriptionTier.enterprise: 3,
}


# ============================================================================
# Feature matrix by tier
# ============================================================================

TIER_FEATURES = {
    SubscriptionTier.starter: {
        "basic_roster": True,
        "single_venue": True,
        "email_notifications": True,
        "ai_optimisation": False,
        "multi_venue": False,
        "xero_integration": False,
        "advanced_reports": False,
        "api_access": False,
        "custom_integrations": False,
        "priority_support": False,
        "usage_metering": False,
    },
    SubscriptionTier.pro: {
        "basic_roster": True,
        "single_venue": False,
        "email_notifications": True,
        "ai_optimisation": True,
        "multi_venue": True,
        "xero_integration": True,
        "advanced_reports": True,
        "api_access": False,
        "custom_integrations": False,
        "priority_support": False,
        "usage_metering": False,
    },
    SubscriptionTier.enterprise: {
        "basic_roster": True,
        "single_venue": False,
        "email_notifications": True,
        "ai_optimisation": True,
        "multi_venue": True,
        "xero_integration": True,
        "advanced_reports": True,
        "api_access": True,
        "custom_integrations": True,
        "priority_support": True,
        "usage_metering": True,
    },
}


# ============================================================================
# Subscription retrieval (cached)
# ============================================================================

@lru_cache(maxsize=512)
def _get_tier_from_cache(venue_id: str, db_key: str) -> Optional[str]:
    """Cache tier lookups (in production, use Redis)."""
    # This is a simplified in-memory cache
    # In production, use Redis or similar
    return None


async def get_venue_subscription(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
) -> dict:
    """
    Retrieve subscription info for a venue.

    Returns subscription data or raises HTTPException if not found.
    """
    from rosteriq.services.billing import SubscriptionData

    # Verify user has access to this venue
    if venue_id not in current_user.venue_ids and not current_user.is_owner:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    sub_data = db.get_subscription(venue_id)
    if not sub_data:
        # No subscription — default to starter (free tier)
        return {
            "venue_id": venue_id,
            "tier": SubscriptionTier.starter,
            "status": SubscriptionStatus.inactive,
            "features": TIER_FEATURES[SubscriptionTier.starter],
        }

    sub = SubscriptionData.from_dict(sub_data)
    return {
        "venue_id": venue_id,
        "tier": sub.tier,
        "status": sub.status,
        "features": TIER_FEATURES[sub.tier],
        "cancel_at_period_end": sub.cancel_at_period_end,
    }


# ============================================================================
# Tier enforcement dependencies
# ============================================================================

def require_tier(*allowed_tiers: str):
    """
    Dependency to enforce minimum subscription tier.

    Args:
        *allowed_tiers: One or more tier names ('starter', 'pro', 'enterprise')

    Returns None if tier is sufficient, raises 402 Payment Required otherwise.

    Usage:
        @app.get("/api/advanced")
        async def advanced(
            venue_id: str,
            _: None = Depends(require_tier("pro", "enterprise")),
        ):
            return {"data": "..."}
    """

    async def check_tier(
        subscription: dict = Depends(get_venue_subscription),
    ) -> None:
        """Check if subscription tier meets requirement."""
        user_tier = subscription["tier"]
        user_tier_level = TIER_HIERARCHY.get(user_tier, 0)

        # Find minimum required tier level
        min_required = min(
            TIER_HIERARCHY.get(SubscriptionTier(tier), 0)
            for tier in allowed_tiers
        )

        if user_tier_level < min_required:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"This feature requires {' or '.join(allowed_tiers)} subscription",
                headers={"Retry-After": "3600"},
            )

    return check_tier


def require_feature(feature_name: str):
    """
    Dependency to enforce a specific feature requirement.

    Args:
        feature_name: Feature key (e.g., 'ai_optimisation', 'multi_venue')

    Returns None if feature is enabled, raises 402 Payment Required otherwise.

    Usage:
        @app.get("/api/multi-venue")
        async def multi_venue(
            _: None = Depends(require_feature("multi_venue")),
        ):
            return {"venues": [...]}
    """

    async def check_feature(
        subscription: dict = Depends(get_venue_subscription),
    ) -> None:
        """Check if subscription has feature enabled."""
        features = subscription["features"]

        if not features.get(feature_name, False):
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"Feature '{feature_name}' not available in your current plan",
                headers={"Retry-After": "3600"},
            )

    return check_feature


def require_active_subscription(
    subscription: dict = Depends(get_venue_subscription),
) -> None:
    """
    Dependency to enforce active subscription.

    Raises 402 Payment Required if subscription is not active.

    Usage:
        @app.get("/api/protected")
        async def protected(
            _: None = Depends(require_active_subscription),
        ):
            return {"data": "..."}
    """
    from rosteriq.services.billing import SubscriptionStatus

    if subscription["status"] != SubscriptionStatus.active:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Your subscription is not active. Please check your payment method.",
            headers={"Retry-After": "3600"},
        )


def get_available_features(
    subscription: dict = Depends(get_venue_subscription),
) -> dict:
    """
    Get the feature set for the current user's subscription.

    Usage:
        @app.get("/api/features")
        async def list_features(
            features: dict = Depends(get_available_features),
        ):
            return features
    """
    return subscription["features"]


def get_subscription_info(
    subscription: dict = Depends(get_venue_subscription),
) -> dict:
    """
    Get full subscription information for the current user's venue.

    Usage:
        @app.get("/api/subscription-info")
        async def subscription_info(
            info: dict = Depends(get_subscription_info),
        ):
            return info
    """
    return subscription


# ============================================================================
# Decorators for class-based views (not used with FastAPI, but useful for reference)
# ============================================================================

def require_tier_decorator(*allowed_tiers: str):
    """
    Decorator version for manual route handling.
    (FastAPI uses Depends() instead, but this is useful for non-FastAPI code.)
    """
    def decorator(func):
        async def wrapper(
            *args,
            subscription: dict = Depends(get_venue_subscription),
            **kwargs
        ):
            user_tier = subscription["tier"]
            user_tier_level = TIER_HIERARCHY.get(user_tier, 0)
            min_required = min(
                TIER_HIERARCHY.get(SubscriptionTier(tier), 0)
                for tier in allowed_tiers
            )

            if user_tier_level < min_required:
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail=f"This feature requires {' or '.join(allowed_tiers)} subscription",
                )

            return await func(*args, subscription=subscription, **kwargs)

        return wrapper

    return decorator
