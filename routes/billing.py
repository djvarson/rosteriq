"""
Stripe billing API routes for RosterIQ.

Provides endpoints for:
- Creating checkout sessions
- Checking subscription status
- Managing tiers and cancellations
- Processing Stripe webhooks
- Creating customer portal sessions

All endpoints require authentication except the webhook handler.

Routes:
    POST   /api/billing/checkout              Create checkout session
    GET    /api/billing/status/{venue_id}    Get subscription status
    POST   /api/billing/cancel/{venue_id}    Cancel subscription
    POST   /api/billing/change-tier          Change subscription tier
    POST   /api/billing/webhook               Stripe webhook handler (no auth)
    GET    /api/billing/portal/{venue_id}    Create Customer Portal session
"""

import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Depends, Query, status
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext, check_venue_access
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.billing import billing_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/billing", tags=["billing"])


# ============================================================================
# Request/Response schemas
# ============================================================================

class CheckoutRequest(BaseModel):
    """Create a checkout session."""
    venue_id: str
    tier: str  # 'starter', 'pro', 'enterprise'
    success_url: str
    cancel_url: str
    email: Optional[str] = None


class CheckoutResponse(BaseModel):
    """Checkout session created."""
    session_id: str
    client_secret: Optional[str] = None
    url: str
    venue_id: str
    tier: str


class SubscriptionStatusResponse(BaseModel):
    """Current subscription status."""
    venue_id: str
    tier: str
    status: str
    features: dict
    current_period_start: Optional[str] = None
    current_period_end: Optional[str] = None
    next_billing_date: Optional[str] = None
    cancel_at_period_end: bool = False
    stripe_subscription_id: Optional[str] = None


class ChangeTierRequest(BaseModel):
    """Change subscription tier."""
    venue_id: str
    new_tier: str


class ChangeTierResponse(BaseModel):
    """Tier change result."""
    venue_id: str
    old_tier: str
    new_tier: str
    effective_date: str


class CancelSubscriptionResponse(BaseModel):
    """Subscription cancellation result."""
    venue_id: str
    status: str
    cancel_at_period_end: bool
    cancelled_at: str


class PortalSessionResponse(BaseModel):
    """Customer portal session."""
    session_url: str
    venue_id: str


# ============================================================================
# Checkout endpoints
# ============================================================================

@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout_session(
    req: CheckoutRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Create a Stripe checkout session for a subscription.

    Requires authentication. User must have access to the venue.

    Query the returned `url` to redirect the user to the Stripe checkout page.
    """
    enforce_venue_manager(req.venue_id)
    # Verify user has access to this venue
    if not check_venue_access(current_user, req.venue_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    if not billing_service.is_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service not configured",
        )

    try:
        result = await billing_service.create_checkout_session(
            venue_id=req.venue_id,
            tier=req.tier,
            success_url=req.success_url,
            cancel_url=req.cancel_url,
            email=req.email or current_user.email,
        )
        return CheckoutResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error creating checkout session: {e}")
        raise HTTPException(status_code=500, detail="Error creating checkout session")


# ============================================================================
# Subscription status endpoints
# ============================================================================

@router.get("/status/{venue_id}", response_model=SubscriptionStatusResponse)
async def get_subscription_status(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get the current subscription status for a venue.

    Requires authentication. User must have access to the venue.

    Returns subscription tier, status, enabled features, billing dates, and cancellation status.
    """
    if not check_venue_access(current_user, venue_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    try:
        status_data = await billing_service.get_subscription_status(venue_id, db)
        if not status_data:
            raise HTTPException(status_code=404, detail="No subscription found for this venue")
        return SubscriptionStatusResponse(**status_data)
    except Exception as e:
        logger.exception(f"Error retrieving subscription status: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving subscription status")


# ============================================================================
# Subscription management endpoints
# ============================================================================

@router.post("/change-tier", response_model=ChangeTierResponse)
async def change_tier(
    req: ChangeTierRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Upgrade or downgrade a subscription tier.

    Requires authentication. User must have access to the venue.

    Changes take effect immediately with proration.
    """
    enforce_venue_manager(req.venue_id)
    if not check_venue_access(current_user, req.venue_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    if not billing_service.is_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service not configured",
        )

    try:
        result = await billing_service.change_tier(
            venue_id=req.venue_id,
            new_tier=req.new_tier,
            db=db,
        )
        return ChangeTierResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error changing tier: {e}")
        raise HTTPException(status_code=500, detail="Error changing tier")


@router.post("/cancel/{venue_id}", response_model=CancelSubscriptionResponse)
async def cancel_subscription(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
    immediate: bool = Query(False, description="If true, cancel immediately; otherwise cancel at period end"),
):
    """
    Cancel a subscription.

    Requires authentication. User must have access to the venue.

    By default, cancels at the end of the current billing period.
    Set `immediate=true` to cancel immediately (not refundable).
    """
    enforce_venue_manager(venue_id)
    if not check_venue_access(current_user, venue_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    if not billing_service.is_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service not configured",
        )

    try:
        result = await billing_service.cancel_subscription(
            venue_id=venue_id,
            db=db,
            immediate=immediate,
        )
        return CancelSubscriptionResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error cancelling subscription: {e}")
        raise HTTPException(status_code=500, detail="Error cancelling subscription")


# ============================================================================
# Webhook endpoint (no authentication required)
# ============================================================================

@router.post("/webhook")
async def stripe_webhook(
    request: Request,
    db = Depends(get_db),
):
    """
    Receive webhook events from Stripe.

    No authentication required. Verifies request using Stripe signature.

    Security:
    - Verifies webhook signature using raw request body (prevents tampering)
    - Validates event timestamp (rejects events older than 5 minutes)
    - Implements idempotency (prevents double-processing of same event)
    - Logs all events for audit trail

    Handles:
    - checkout.session.completed: Activate new subscription
    - invoice.paid: Record payment
    - invoice.payment_failed: Flag account as past_due
    - customer.subscription.deleted: Deactivate subscription
    - customer.subscription.updated: Update billing dates

    Returns 200 OK if webhook signature is valid (processing happens async).
    Returns 403 Forbidden if signature is invalid or replay detected.
    """
    if not billing_service.is_enabled():
        return {"status": "billing_disabled"}

    sig_header = request.headers.get("stripe-signature", "")

    try:
        # CRITICAL: Read raw body for signature verification
        # Do NOT parse JSON before verifying signature
        payload = await request.body()
    except Exception as e:
        logger.error(f"Error reading webhook body: {e}")
        raise HTTPException(status_code=400, detail="Invalid request body")

    try:
        # Pass raw payload to service for proper verification
        result = await billing_service.handle_webhook_event(
            payload=payload,
            sig_header=sig_header,
            db=db,
        )
        return result
    except ValueError as e:
        # Signature verification failed
        logger.warning(f"Webhook verification failed: {e}")
        raise HTTPException(status_code=403, detail="Invalid webhook signature")
    except Exception as e:
        logger.exception(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail="Error processing webhook")


# ============================================================================
# Customer Portal endpoint
# ============================================================================

@router.get("/portal/{venue_id}", response_model=PortalSessionResponse)
async def create_customer_portal_session(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
    return_url: Optional[str] = Query(None, description="URL to return to after portal session"),
):
    """
    Create a Stripe Customer Portal session.

    Requires authentication. User must have access to the venue.

    Returns a URL to redirect the user to the Stripe Customer Portal where they can:
    - Update payment method
    - View invoices
    - Change subscription
    - Update billing information
    """
    if not check_venue_access(current_user, venue_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )

    if not billing_service.is_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service not configured",
        )

    try:
        import stripe
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Stripe not installed",
        )

    sub_data = db.get_subscription(venue_id)
    if not sub_data or not sub_data.get("stripe_customer_id"):
        raise HTTPException(
            status_code=404,
            detail="No Stripe customer found for this venue",
        )

    try:
        session = stripe.billingportal.Session.create(
            customer=sub_data["stripe_customer_id"],
            return_url=return_url or os.environ.get("APP_URL", "https://app.rosteriq.io"),
        )
        return PortalSessionResponse(
            session_url=session.url,
            venue_id=venue_id,
        )
    except Exception as e:
        logger.exception(f"Error creating customer portal session: {e}")
        raise HTTPException(status_code=500, detail="Error creating customer portal session")
