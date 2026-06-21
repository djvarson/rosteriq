"""
Push notification routes for RosterIQ.

Endpoints:
- POST /api/push/subscribe — Register push subscription
- DELETE /api/push/unsubscribe — Remove subscription
- POST /api/push/test — Send test notification
- GET /api/push/vapid-key — Get public VAPID key
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.services.push_notifications import get_push_service
from rosteriq.middleware.tenant import get_tenant_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/push", tags=["push"])


# ============================================================================
# Request Models
# ============================================================================

class PushSubscription(BaseModel):
    """Web Push API subscription object."""
    endpoint: str
    keys: dict


class PushNotificationRequest(BaseModel):
    """Request to send a test notification."""
    title: str = "Test Notification"
    body: str = "This is a test push notification from RosterIQ"


# ============================================================================
# Routes
# ============================================================================

@router.post("/subscribe")
async def subscribe_to_push(request: Request, subscription: PushSubscription):
    """
    Register a push notification subscription.

    The client sends the subscription object from the Push API.
    We store it associated with the current user.
    """
    try:
        # Get current user from request context
        # This assumes auth middleware has set user info in request.state
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            raise HTTPException(status_code=401, detail="Not authenticated")

        db = get_db()
        push_service = get_push_service(db)

        # Attach the user's venue so venue broadcasts (roster published, staffing
        # alerts) can target them — list_push_subscriptions filters by venue_id,
        # which was never being stored.
        sub_dict = subscription.dict()
        user = db.get_user_by_id(user_id)
        if user and user.get("venue_ids"):
            sub_dict["venue_id"] = user["venue_ids"][0]

        # Store subscription
        success = push_service.subscribe(user_id, sub_dict)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store subscription")

        logger.info(f"User {user_id} subscribed to push notifications")

        return {
            "success": True,
            "message": "Subscribed to push notifications",
            "subscription_endpoint": subscription.endpoint,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error subscribing to push: {e}")
        raise HTTPException(status_code=500, detail="Failed to subscribe to push notifications")


@router.delete("/unsubscribe")
async def unsubscribe_from_push(request: Request, endpoint: Optional[str] = Query(None)):
    """
    Unsubscribe from push notifications.

    Can pass endpoint as query param, or expect it in request body.
    """
    try:
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            raise HTTPException(status_code=401, detail="Not authenticated")

        db = get_db()
        push_service = get_push_service(db)

        success = push_service.unsubscribe(user_id)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to unsubscribe")

        logger.info(f"User {user_id} unsubscribed from push notifications")

        return {
            "success": True,
            "message": "Unsubscribed from push notifications",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error unsubscribing from push: {e}")
        raise HTTPException(status_code=500, detail="Failed to unsubscribe from push notifications")


@router.post("/test")
async def send_test_notification(request: Request, payload: PushNotificationRequest):
    """
    Send a test push notification to the current user.

    Useful for verifying that push notifications are set up correctly.
    """
    try:
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            raise HTTPException(status_code=401, detail="Not authenticated")

        db = get_db()
        push_service = get_push_service(db)

        success = push_service.send_push(
            user_id,
            title=payload.title,
            body=payload.body,
            url="/dashboard",
            notification_type="test",
        )

        if not success:
            raise HTTPException(status_code=500, detail="Failed to send test notification")

        logger.info(f"Test notification sent to {user_id}")

        return {
            "success": True,
            "message": "Test notification sent",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending test notification: {e}")
        raise HTTPException(status_code=500, detail="Failed to send test notification")


@router.get("/vapid-key")
async def get_vapid_public_key(request: Request):
    """
    Get the VAPID public key for client subscription.

    This is needed by the client to subscribe to push notifications.
    No authentication required (public key).
    """
    try:
        db = get_db()
        push_service = get_push_service(db)

        public_key = push_service.get_vapid_public_key()

        return {
            "public_key": public_key,
        }
    except Exception as e:
        logger.error(f"Error getting VAPID key: {e}")
        raise HTTPException(status_code=500, detail="Failed to get VAPID key")


@router.post("/broadcast/{venue_id}")
async def broadcast_to_venue(
    request: Request,
    venue_id: str,
    notification: PushNotificationRequest
):
    """
    Broadcast a push notification to all staff at a venue.

    Requires admin/manager permissions for the venue.
    """
    try:
        user_id = getattr(request.state, "user_id", None)
        if not user_id:
            raise HTTPException(status_code=401, detail="Not authenticated")

        # TODO: Check permissions (must be venue manager/admin)

        db = get_db()
        push_service = get_push_service(db)

        count = push_service.broadcast_venue(
            venue_id,
            title=notification.title,
            body=notification.body,
            url=f"/dashboard?venue={venue_id}",
        )

        logger.info(f"Broadcast notification to {count} staff at venue {venue_id}")

        return {
            "success": True,
            "message": f"Notification sent to {count} staff",
            "recipients": count,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error broadcasting to venue: {e}")
        raise HTTPException(status_code=500, detail="Failed to broadcast notification")
