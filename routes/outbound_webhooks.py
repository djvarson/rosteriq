"""
Outbound webhook routes for RosterIQ.

Provides endpoints for managing webhook subscriptions and viewing delivery logs.

Endpoints:
  POST   /api/webhooks/subscribe          — Register a new subscription
  GET    /api/webhooks/subscriptions/{venue_id}  — List subscriptions for venue
  DELETE /api/webhooks/subscriptions/{subscription_id} — Delete subscription
  PUT    /api/webhooks/subscriptions/{subscription_id} — Update subscription
  GET    /api/webhooks/deliveries/{subscription_id}    — Get delivery log
  POST   /api/webhooks/test/{subscription_id}   — Send test event
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from uuid import uuid4

try:
    from fastapi import APIRouter, HTTPException, status, BackgroundTasks
    from pydantic import BaseModel
except ImportError:
    APIRouter = None
    HTTPException = None
    status = None
    BackgroundTasks = None
    BaseModel = object

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["outbound-webhooks"])


# ============================================================================
# Request/Response Models
# ============================================================================


class SubscribeRequest(BaseModel):
    """Request to subscribe to webhooks."""

    venue_id: str
    callback_url: str
    events: list[str]
    secret: str


class UpdateSubscriptionRequest(BaseModel):
    """Request to update a subscription."""

    events: Optional[list[str]] = None
    callback_url: Optional[str] = None
    active: Optional[bool] = None


class SubscriptionResponse(BaseModel):
    """Response for a subscription."""

    id: str
    venue_id: str
    callback_url: str
    events: list[str]
    active: bool
    created_at: str
    updated_at: str


class DeliveryRecord(BaseModel):
    """Delivery log entry."""

    id: str
    event_type: str
    status: str
    response_code: Optional[int]
    attempts: int
    last_attempt_at: str
    next_retry_at: Optional[str]
    created_at: str


class TestEventRequest(BaseModel):
    """Request to send a test event."""

    event_type: Optional[str] = "roster.published"
    data: Optional[Dict[str, Any]] = None


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/subscribe", response_model=Dict[str, Any])
async def subscribe_webhook(request: SubscribeRequest) -> Dict[str, Any]:
    """
    Register a new webhook subscription.

    Args:
        venue_id: Venue to subscribe for
        callback_url: HTTPS URL to receive events
        events: List of event types (e.g. ['roster.published', 'shift.created'])
        secret: Secret for HMAC-SHA256 signing

    Returns:
        Subscription details with ID
    """
    if not (APIRouter and HTTPException):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    # Validate
    if not request.callback_url.startswith("https://"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="callback_url must be HTTPS",
        )

    if not request.events:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one event type required",
        )

    service = get_outbound_webhook_service()

    try:
        subscription_id = service.register_subscription(
            venue_id=request.venue_id,
            callback_url=request.callback_url,
            events=request.events,
            secret=request.secret,
        )

        return {
            "status": "created",
            "subscription_id": subscription_id,
            "venue_id": request.venue_id,
            "callback_url": request.callback_url,
            "events": request.events,
        }
    except Exception as e:
        logger.error(f"Failed to register subscription: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/subscriptions/{venue_id}", response_model=Dict[str, Any])
async def list_subscriptions(venue_id: str) -> Dict[str, Any]:
    """
    List all webhook subscriptions for a venue.

    Args:
        venue_id: Venue ID

    Returns:
        List of subscriptions
    """
    if not (APIRouter and HTTPException):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    service = get_outbound_webhook_service()

    try:
        subscriptions = service.list_subscriptions(venue_id)
        return {
            "status": "ok",
            "venue_id": venue_id,
            "subscriptions": subscriptions,
            "count": len(subscriptions),
        }
    except Exception as e:
        logger.error(f"Failed to list subscriptions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/subscriptions/{subscription_id}", response_model=Dict[str, Any])
async def delete_subscription(subscription_id: str) -> Dict[str, Any]:
    """
    Delete a webhook subscription.

    Args:
        subscription_id: Subscription ID to delete

    Returns:
        Confirmation
    """
    if not (APIRouter and HTTPException):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    service = get_outbound_webhook_service()

    try:
        success = service.delete_subscription(subscription_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscription {subscription_id} not found",
            )

        return {
            "status": "deleted",
            "subscription_id": subscription_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete subscription: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.put("/subscriptions/{subscription_id}", response_model=Dict[str, Any])
async def update_subscription(
    subscription_id: str,
    request: UpdateSubscriptionRequest,
) -> Dict[str, Any]:
    """
    Update a webhook subscription.

    Args:
        subscription_id: Subscription ID
        events: New event list (optional)
        callback_url: New callback URL (optional)
        active: New active state (optional)

    Returns:
        Updated subscription
    """
    if not (APIRouter and HTTPException):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    service = get_outbound_webhook_service()

    try:
        subscription = service.update_subscription(
            subscription_id=subscription_id,
            events=request.events,
            callback_url=request.callback_url,
            active=request.active,
        )

        if not subscription:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscription {subscription_id} not found",
            )

        return {
            "status": "updated",
            "subscription": subscription,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update subscription: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/deliveries/{subscription_id}", response_model=Dict[str, Any])
async def get_deliveries(
    subscription_id: str,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Get delivery log for a subscription.

    Args:
        subscription_id: Subscription ID
        limit: Max records to return (default 50)

    Returns:
        List of delivery records
    """
    if not (APIRouter and HTTPException):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    service = get_outbound_webhook_service()

    try:
        deliveries = service.get_delivery_log(subscription_id, limit=limit)
        return {
            "status": "ok",
            "subscription_id": subscription_id,
            "deliveries": deliveries,
            "count": len(deliveries),
        }
    except Exception as e:
        logger.error(f"Failed to get delivery log: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/test/{subscription_id}", response_model=Dict[str, Any])
async def send_test_event(
    subscription_id: str,
    request: TestEventRequest,
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """
    Send a test webhook event to a subscription.

    Queues delivery in the background and returns immediately.

    Args:
        subscription_id: Subscription ID
        event_type: Event type (default 'roster.published')
        data: Custom event data (optional)

    Returns:
        Queued message with delivery ID
    """
    if not (APIRouter and HTTPException and BackgroundTasks):
        return {"error": "FastAPI not available"}

    try:
        from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook service not available",
        )

    service = get_outbound_webhook_service()

    try:
        # Get subscription
        from rosteriq.database import get_db

        db = get_db()
        subscription = db.get_webhook_subscription(subscription_id)

        if not subscription:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscription {subscription_id} not found",
            )

        # Build test payload
        test_data = request.data or {
            "test": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "venue_id": subscription.get("venue_id"),
        }

        event_type = request.event_type or "roster.published"
        delivery_id = f"test_{uuid4().hex[:12]}"

        # Queue delivery in background
        background_tasks.add_task(
            service.deliver_webhook,
            subscription=subscription,
            event_type=event_type,
            payload=test_data,
        )

        return {
            "status": "queued",
            "subscription_id": subscription_id,
            "event_type": event_type,
            "delivery_id": delivery_id,
            "message": "Test event queued for delivery",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to queue test event: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
