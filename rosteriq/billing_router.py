"""
FastAPI Router for RosterIQ Billing

Endpoints:
- POST /api/v1/billing/checkout — Create Stripe checkout session (OWNER-gated)
- POST /api/v1/billing/portal — Create billing portal session (OWNER-gated)
- POST /api/v1/billing/webhook — Stripe webhook receiver (signature verified)
- GET /api/v1/billing/subscription — Get current subscription (OWNER-gated)

Webhook handlers:
- checkout.session.completed: create Subscription, update Tenant.billing_tier + status
- customer.subscription.updated: update Subscription record
- customer.subscription.deleted: mark Subscription canceled, downgrade Tenant
- invoice.payment_failed: set Subscription status PAST_DUE
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Lazy imports for optional dependencies
try:
    from fastapi import APIRouter, HTTPException, Request, status
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    status = None

try:
    from fastapi.responses import JSONResponse
except ImportError:
    JSONResponse = None

try:
    from rosteriq.auth import require_access, AccessLevel, User
except ImportError:
    require_access = None
    AccessLevel = None
    User = None

from rosteriq.billing import (
    get_subscription_store,
    Subscription,
    SubscriptionStatus,
    StripeProductCatalog,
)
from rosteriq.tenants import get_tenant_store, BillingTier, TenantStatus
from rosteriq.stripe_client import StripeClient

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_SUCCESS_URL = os.getenv("STRIPE_SUCCESS_URL", "http://localhost:8000/billing/success")
STRIPE_CANCEL_URL = os.getenv("STRIPE_CANCEL_URL", "http://localhost:8000/billing/cancel")

# ============================================================================
# Router
# ============================================================================

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])


# ============================================================================
# Helpers
# ============================================================================


def _verify_stripe_signature(payload: bytes, signature_header: str) -> bool:
    """
    Verify Stripe webhook signature using HMAC-SHA256.

    Stripe uses signed_content format: t=<timestamp>,v1=<hash>
    Algorithm: HMAC-SHA256(secret, f"{timestamp}.{payload}")

    Args:
        payload: Raw request body bytes
        signature_header: Value of Stripe-Signature header

    Returns:
        True if signature is valid and timestamp is recent, False otherwise
    """
    if not STRIPE_WEBHOOK_SECRET:
        logger.warning("STRIPE_WEBHOOK_SECRET not configured; skipping signature verification")
        return True  # Demo mode: allow unsigned webhooks

    try:
        # Parse signature header: t=<timestamp>,v1=<hash>
        parts = {}
        for part in signature_header.split(","):
            key, value = part.split("=", 1)
            parts[key.strip()] = value.strip()

        timestamp_str = parts.get("t")
        provided_signature = parts.get("v1")

        if not timestamp_str or not provided_signature:
            logger.error("Malformed Stripe-Signature header")
            return False

        # Check timestamp is recent (within 5 minutes)
        timestamp = int(timestamp_str)
        now = int(time.time())
        if abs(now - timestamp) > 300:  # 5 min tolerance
            logger.error(f"Webhook timestamp too old: {now - timestamp}s ago")
            return False

        # Compute expected signature
        signed_content = f"{timestamp_str}.{payload.decode('utf-8')}"
        expected_signature = hmac.new(
            STRIPE_WEBHOOK_SECRET.encode("utf-8"),
            signed_content.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Compare signatures (constant-time)
        if not hmac.compare_digest(expected_signature, provided_signature):
            logger.error("Stripe signature mismatch")
            return False

        return True

    except Exception as e:
        logger.error(f"Error verifying Stripe signature: {e}")
        return False


async def _require_owner_access(request: Request) -> None:
    """
    Check if request is from OWNER user.

    If auth is not available, allow (demo mode).
    If auth is available, require OWNER access level.

    Raises:
        HTTPException: If not authorized
    """
    if not require_access:
        # Auth not available, demo mode
        return

    try:
        await require_access(AccessLevel.OWNER)(request=request)
    except HTTPException:
        raise HTTPException(status_code=403, detail="Owner access required")


# ============================================================================
# Public Endpoints
# ============================================================================


@router.post("/checkout")
async def create_checkout(
    request: Request,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a Stripe checkout session for a tenant.

    Request body:
    {
      "tier": "startup" | "pro" | "enterprise"
    }

    OWNER-gated: requires require_access(AccessLevel.OWNER).

    Returns:
    {
      "checkout_url": "https://checkout.stripe.com/pay/...",
      "demo_mode": false
    }
    """
    await _require_owner_access(request)

    # In production, extract tenant_id from JWT claims
    # For now, use demo-tenant-001
    tenant_id = "demo-tenant-001"

    tier = body.get("tier", "startup").lower()
    if tier not in ("startup", "pro", "enterprise"):
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")

    tenant_store = get_tenant_store()
    tenant = tenant_store.get(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    logger.info(f"Creating checkout for tenant {tenant_id} (tier={tier})")

    # Call Stripe client
    checkout_url = await StripeClient.create_checkout_session(
        tenant_id=tenant_id,
        tier=tier,
        success_url=STRIPE_SUCCESS_URL,
        cancel_url=STRIPE_CANCEL_URL,
    )

    is_demo = "demo" in checkout_url.lower()

    return {
        "checkout_url": checkout_url,
        "demo_mode": is_demo,
    }


@router.post("/portal")
async def create_portal(request: Request) -> Dict[str, Any]:
    """
    Create a Stripe billing portal session for a tenant.

    OWNER-gated: requires require_access(AccessLevel.OWNER).

    Returns:
    {
      "portal_url": "https://billing.stripe.com/...",
      "demo_mode": false
    }
    """
    await _require_owner_access(request)

    # Extract tenant_id from JWT (demo for now)
    tenant_id = "demo-tenant-001"

    tenant_store = get_tenant_store()
    tenant = tenant_store.get(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    sub_store = get_subscription_store()
    sub = sub_store.get(tenant_id)
    if not sub:
        raise HTTPException(status_code=404, detail="No active subscription")

    logger.info(f"Creating portal for tenant {tenant_id}")

    # Call Stripe client
    portal_url = await StripeClient.create_billing_portal_session(
        customer_id=sub.stripe_customer_id,
        return_url=STRIPE_SUCCESS_URL,
    )

    is_demo = "demo" in portal_url.lower()

    return {
        "portal_url": portal_url,
        "demo_mode": is_demo,
    }


@router.get("/subscription")
async def get_subscription(request: Request) -> Dict[str, Any]:
    """
    Get current subscription for a tenant.

    OWNER-gated: requires require_access(AccessLevel.OWNER).

    Returns:
    {
      "tenant_id": "...",
      "stripe_subscription_id": "...",
      "status": "active" | "trialing" | "past_due" | "canceled" | "incomplete",
      "tier": "startup" | "pro" | "enterprise",
      "current_period_end": "2026-05-15T00:00:00Z",
      "trial_ends_at": "2026-04-30T00:00:00Z" | null,
      "quantity": 3,
      ...
    }
    """
    await _require_owner_access(request)

    # Extract tenant_id from JWT (demo for now)
    tenant_id = "demo-tenant-001"

    sub_store = get_subscription_store()
    sub = sub_store.get(tenant_id)
    if not sub:
        raise HTTPException(status_code=404, detail="No subscription found")

    return sub.to_dict()


# ============================================================================
# Webhook Endpoint (Signature Verified)
# ============================================================================


@router.post("/webhook")
async def stripe_webhook(request: Request) -> Dict[str, Any]:
    """
    Receive and process Stripe webhook events.

    Verifies HMAC-SHA256 signature against STRIPE_WEBHOOK_SECRET.
    Returns 403 if signature is invalid.

    Handled events:
    - checkout.session.completed: Create Subscription, update Tenant
    - customer.subscription.updated: Update Subscription
    - customer.subscription.deleted: Cancel Subscription, downgrade Tenant
    - invoice.payment_failed: Mark Subscription as PAST_DUE

    Returns:
    {
      "received": true,
      "event_id": "evt_...",
      "type": "checkout.session.completed"
    }
    """
    # Get raw body and signature header
    body = await request.body()
    signature_header = request.headers.get("stripe-signature", "")

    if not signature_header:
        logger.error("Missing stripe-signature header")
        raise HTTPException(status_code=403, detail="Missing signature")

    # Verify signature
    if not _verify_stripe_signature(body, signature_header):
        logger.error("Invalid Stripe signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    # Parse event
    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook payload")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_id = event.get("id", "unknown")
    event_type = event.get("type", "unknown")

    logger.info(f"Received Stripe webhook: {event_type} ({event_id})")

    # Handle event types
    if event_type == "checkout.session.completed":
        await _handle_checkout_session_completed(event)
    elif event_type == "customer.subscription.updated":
        await _handle_subscription_updated(event)
    elif event_type == "customer.subscription.deleted":
        await _handle_subscription_deleted(event)
    elif event_type == "invoice.payment_failed":
        await _handle_payment_failed(event)
    else:
        logger.debug(f"Ignoring event type: {event_type}")

    return {
        "received": True,
        "event_id": event_id,
        "type": event_type,
    }


# ============================================================================
# Webhook Event Handlers
# ============================================================================


async def _handle_checkout_session_completed(event: Dict[str, Any]) -> None:
    """
    Handle checkout.session.completed event.

    Creates a Subscription record and updates Tenant.billing_tier + status.
    """
    try:
        session = event.get("data", {}).get("object", {})
        session_id = session.get("id", "unknown")

        client_reference_id = session.get("client_reference_id")  # = tenant_id
        customer_id = session.get("customer")
        subscription_id = session.get("subscription")

        if not client_reference_id or not subscription_id:
            logger.warning(f"checkout.session {session_id} missing client_reference_id or subscription")
            return

        tenant_id = client_reference_id

        logger.info(f"Processing checkout session {session_id} for tenant {tenant_id}")

        # Fetch subscription details from Stripe (in production)
        # For now, assume tier from line items or default to startup
        line_items = session.get("line_items", {}).get("data", [])
        tier = "startup"  # Default
        if line_items:
            # Try to map price ID to tier
            price_id = line_items[0].get("price", {}).get("id")
            if price_id:
                # Check if matches any configured tier
                for tier_name in ("enterprise", "pro", "startup"):
                    if StripeProductCatalog.get_price_id(tier_name) == price_id:
                        tier = tier_name
                        break

        # Create subscription record
        now = datetime.now(timezone.utc)
        sub = Subscription(
            tenant_id=tenant_id,
            stripe_subscription_id=subscription_id,
            stripe_customer_id=customer_id or "unknown",
            status=SubscriptionStatus.ACTIVE,
            tier=tier,
            current_period_end=now,
            quantity=1,  # Start with 1; update_subscription_quantity will adjust
            created_at=now,
            updated_at=now,
        )

        sub_store = get_subscription_store()
        try:
            sub_store.create(sub)
        except ValueError:
            # Subscription already exists, update it
            sub_store.update(
                tenant_id,
                status=SubscriptionStatus.ACTIVE,
                tier=tier,
                stripe_subscription_id=subscription_id,
                stripe_customer_id=customer_id,
            )

        # Update Tenant billing_tier and status
        tenant_store = get_tenant_store()
        tenant = tenant_store.get(tenant_id)
        if tenant:
            tenant_store.update(
                tenant_id,
                billing_tier=BillingTier(tier),
                status=TenantStatus.ACTIVE,
            )
            logger.info(f"Updated tenant {tenant_id}: tier={tier}, status=ACTIVE")

    except Exception as e:
        logger.error(f"Error handling checkout.session.completed: {e}")


async def _handle_subscription_updated(event: Dict[str, Any]) -> None:
    """Handle customer.subscription.updated event."""
    try:
        sub_data = event.get("data", {}).get("object", {})
        subscription_id = sub_data.get("id", "unknown")
        status_str = sub_data.get("status", "unknown")  # active, trialing, past_due, etc.

        # Find subscription by Stripe ID
        sub_store = get_subscription_store()
        sub = sub_store.find_by_stripe_id(subscription_id)
        if not sub:
            logger.warning(f"Subscription {subscription_id} not found in store")
            return

        # Map Stripe status to our SubscriptionStatus
        status_map = {
            "active": SubscriptionStatus.ACTIVE,
            "trialing": SubscriptionStatus.TRIALING,
            "past_due": SubscriptionStatus.PAST_DUE,
            "canceled": SubscriptionStatus.CANCELED,
            "incomplete": SubscriptionStatus.INCOMPLETE,
        }
        new_status = status_map.get(status_str, SubscriptionStatus.INCOMPLETE)

        # Update subscription
        current_period_end = sub_data.get("current_period_end")
        if current_period_end:
            current_period_end = datetime.fromtimestamp(current_period_end, tz=timezone.utc)

        sub_store.update(
            sub.tenant_id,
            status=new_status,
            current_period_end=current_period_end,
        )

        logger.info(f"Updated subscription {subscription_id}: status={new_status.value}")

    except Exception as e:
        logger.error(f"Error handling customer.subscription.updated: {e}")


async def _handle_subscription_deleted(event: Dict[str, Any]) -> None:
    """
    Handle customer.subscription.deleted event.

    Marks Subscription as canceled and downgrades Tenant to STARTUP tier.
    """
    try:
        sub_data = event.get("data", {}).get("object", {})
        subscription_id = sub_data.get("id", "unknown")

        # Find subscription by Stripe ID
        sub_store = get_subscription_store()
        sub = sub_store.find_by_stripe_id(subscription_id)
        if not sub:
            logger.warning(f"Subscription {subscription_id} not found in store")
            return

        # Mark as canceled
        sub_store.update(sub.tenant_id, status=SubscriptionStatus.CANCELED)

        # Downgrade tenant to STARTUP
        tenant_store = get_tenant_store()
        tenant_store.update(sub.tenant_id, billing_tier=BillingTier.STARTUP)

        logger.info(f"Canceled subscription {subscription_id}; downgraded tenant {sub.tenant_id} to STARTUP")

    except Exception as e:
        logger.error(f"Error handling customer.subscription.deleted: {e}")


async def _handle_payment_failed(event: Dict[str, Any]) -> None:
    """Handle invoice.payment_failed event."""
    try:
        invoice_data = event.get("data", {}).get("object", {})
        invoice_id = invoice_data.get("id", "unknown")
        subscription_id = invoice_data.get("subscription")

        if not subscription_id:
            logger.debug(f"invoice.payment_failed {invoice_id} has no subscription")
            return

        # Find subscription by Stripe ID
        sub_store = get_subscription_store()
        sub = sub_store.find_by_stripe_id(subscription_id)
        if not sub:
            logger.warning(f"Subscription {subscription_id} not found for invoice {invoice_id}")
            return

        # Mark as past_due
        sub_store.update(sub.tenant_id, status=SubscriptionStatus.PAST_DUE)

        logger.warning(f"Invoice {invoice_id} payment failed for subscription {subscription_id}; marked as PAST_DUE")

    except Exception as e:
        logger.error(f"Error handling invoice.payment_failed: {e}")


if __name__ == "__main__":
    # Quick smoke test
    print("Testing billing_router...")
    print("billing_router module initialized successfully")
