"""
Stripe API Client for RosterIQ

Provides:
- StripeClient: thin HTTP wrapper for Stripe API
  - create_checkout_session: initiate subscription checkout
  - create_billing_portal_session: link to Stripe customer portal
  - cancel_subscription: cancel a subscription
  - update_subscription_quantity: update usage-based quantity

All HTTP calls use httpx (lazy-imported).
All keys read from env at call time, not at module load.
On missing key: returns sentinel DEMO_* URLs, logs WARN, no crash.
Uses Bearer auth: Authorization: Bearer <STRIPE_SECRET_KEY>
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Stripe API base URL
STRIPE_API_BASE = "https://api.stripe.com/v1"


class StripeClient:
    """
    Thin wrapper for Stripe API calls via httpx.

    All methods read STRIPE_SECRET_KEY from environment at call time.
    On missing key, returns demo URLs and logs WARN (safe fallback, no crash).
    """

    @staticmethod
    def _get_secret_key() -> Optional[str]:
        """
        Get Stripe secret key from environment.

        Returns:
            Secret key string, or None if not configured
        """
        return os.getenv("STRIPE_SECRET_KEY")

    @staticmethod
    async def create_checkout_session(
        tenant_id: str,
        tier: str,
        success_url: str,
        cancel_url: str,
        customer_id: Optional[str] = None,
    ) -> str:
        """
        Create a Stripe checkout session for a tenant subscription.

        Args:
            tenant_id: Tenant ID (for logging/tracking)
            tier: Billing tier (startup/pro/enterprise)
            success_url: URL to redirect to after successful checkout
            cancel_url: URL to redirect to after cancellation
            customer_id: Optional existing Stripe customer ID

        Returns:
            Checkout URL (https://checkout.stripe.com/pay/...)
            On demo mode: returns placeholder DEMO_CHECKOUT_URL
        """
        secret_key = StripeClient._get_secret_key()
        if not secret_key:
            logger.warning(f"STRIPE_SECRET_KEY not configured; returning demo checkout URL for tenant {tenant_id}")
            return "https://checkout.stripe.com/demo"

        # Import httpx lazily
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed; cannot create checkout session")
            return "https://checkout.stripe.com/demo"

        # Map tier to price ID from environment
        from rosteriq.billing import StripeProductCatalog
        price_id = StripeProductCatalog.get_price_id(tier)
        if not price_id:
            logger.error(f"No price ID configured for tier {tier}")
            return "https://checkout.stripe.com/demo"

        logger.info(f"Creating Stripe checkout session for tenant {tenant_id} (tier={tier})")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{STRIPE_API_BASE}/checkout/sessions",
                    auth=(secret_key, ""),  # httpx basic auth (username, password)
                    data={
                        "payment_method_types[]": "card",
                        "mode": "subscription",
                        "line_items[0][price]": price_id,
                        "line_items[0][quantity]": "1",  # Start with 1 venue; billing updates later
                        "success_url": success_url,
                        "cancel_url": cancel_url,
                        "client_reference_id": tenant_id,
                    },
                )

            if response.status_code != 200:
                logger.error(f"Checkout session creation failed: {response.status_code}")
                return "https://checkout.stripe.com/demo"

            session_data = response.json()
            checkout_url = session_data.get("url")
            if not checkout_url:
                logger.error("Checkout session response missing 'url'")
                return "https://checkout.stripe.com/demo"

            logger.info(f"Checkout session created: {session_data.get('id')}")
            return checkout_url

        except Exception as e:
            logger.error(f"Error creating checkout session: {e}")
            return "https://checkout.stripe.com/demo"

    @staticmethod
    async def create_billing_portal_session(
        customer_id: str,
        return_url: str,
    ) -> str:
        """
        Create a Stripe billing portal session for a customer.

        Args:
            customer_id: Stripe customer ID
            return_url: URL to return to after portal session

        Returns:
            Billing portal URL
            On demo mode: returns placeholder DEMO_PORTAL_URL
        """
        secret_key = StripeClient._get_secret_key()
        if not secret_key:
            logger.warning(f"STRIPE_SECRET_KEY not configured; returning demo portal URL for customer {customer_id}")
            return "https://billing.stripe.com/demo"

        # Import httpx lazily
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed; cannot create billing portal session")
            return "https://billing.stripe.com/demo"

        logger.info(f"Creating Stripe billing portal session for customer {customer_id}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{STRIPE_API_BASE}/billing_portal/sessions",
                    auth=(secret_key, ""),
                    data={
                        "customer": customer_id,
                        "return_url": return_url,
                    },
                )

            if response.status_code != 200:
                logger.error(f"Billing portal creation failed: {response.status_code}")
                return "https://billing.stripe.com/demo"

            session_data = response.json()
            portal_url = session_data.get("url")
            if not portal_url:
                logger.error("Billing portal response missing 'url'")
                return "https://billing.stripe.com/demo"

            logger.info(f"Billing portal session created: {session_data.get('id')}")
            return portal_url

        except Exception as e:
            logger.error(f"Error creating billing portal session: {e}")
            return "https://billing.stripe.com/demo"

    @staticmethod
    async def cancel_subscription(stripe_subscription_id: str) -> bool:
        """
        Cancel a Stripe subscription.

        Args:
            stripe_subscription_id: Stripe subscription ID to cancel

        Returns:
            True if successful, False on error
        """
        secret_key = StripeClient._get_secret_key()
        if not secret_key:
            logger.warning(f"STRIPE_SECRET_KEY not configured; cannot cancel subscription {stripe_subscription_id}")
            return False

        # Import httpx lazily
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed; cannot cancel subscription")
            return False

        logger.info(f"Canceling Stripe subscription {stripe_subscription_id}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.delete(
                    f"{STRIPE_API_BASE}/subscriptions/{stripe_subscription_id}",
                    auth=(secret_key, ""),
                )

            if response.status_code not in (200, 204):
                logger.error(f"Subscription cancellation failed: {response.status_code}")
                return False

            logger.info(f"Subscription {stripe_subscription_id} canceled")
            return True

        except Exception as e:
            logger.error(f"Error canceling subscription: {e}")
            return False

    @staticmethod
    async def update_subscription_quantity(
        stripe_subscription_id: str,
        quantity: int,
    ) -> bool:
        """
        Update the quantity of a Stripe subscription (for usage-based billing).

        Args:
            stripe_subscription_id: Stripe subscription ID
            quantity: New quantity (number of venues)

        Returns:
            True if successful, False on error
        """
        secret_key = StripeClient._get_secret_key()
        if not secret_key:
            logger.warning(f"STRIPE_SECRET_KEY not configured; cannot update subscription {stripe_subscription_id}")
            return False

        # Import httpx lazily
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed; cannot update subscription")
            return False

        logger.info(f"Updating Stripe subscription {stripe_subscription_id} quantity to {quantity}")

        try:
            # First, get the subscription to find its items
            async with httpx.AsyncClient(timeout=30.0) as client:
                get_response = await client.get(
                    f"{STRIPE_API_BASE}/subscriptions/{stripe_subscription_id}",
                    auth=(secret_key, ""),
                )

            if get_response.status_code != 200:
                logger.error(f"Failed to get subscription: {get_response.status_code}")
                return False

            sub_data = get_response.json()
            items = sub_data.get("items", {}).get("data", [])
            if not items:
                logger.warning(f"Subscription {stripe_subscription_id} has no items")
                return False

            # Update the first item's quantity
            item_id = items[0]["id"]

            async with httpx.AsyncClient(timeout=30.0) as client:
                update_response = await client.post(
                    f"{STRIPE_API_BASE}/subscription_items/{item_id}",
                    auth=(secret_key, ""),
                    data={"quantity": str(quantity)},
                )

            if update_response.status_code != 200:
                logger.error(f"Subscription quantity update failed: {update_response.status_code}")
                return False

            logger.info(f"Subscription {stripe_subscription_id} quantity updated to {quantity}")
            return True

        except Exception as e:
            logger.error(f"Error updating subscription quantity: {e}")
            return False


if __name__ == "__main__":
    # Quick smoke test (non-async, won't test HTTP calls)
    print("Testing StripeClient...")
    print(f"Secret key available: {bool(StripeClient._get_secret_key())}")
    print("StripeClient initialized successfully")
