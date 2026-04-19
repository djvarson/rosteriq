"""
WebSocket and notification REST endpoints for RosterIQ On-Shift dashboard.

Provides:
- ws /api/v1/ws/{venue_id} — WebSocket for live notifications
- GET /api/v1/notifications/{venue_id} — List recent notifications (L1+)
- POST /api/v1/notifications/{venue_id}/acknowledge — Mark as acknowledged (L1+)
- POST /api/v1/notifications/test — Publish test notification (L2+, for development)

Uses lazy import for FastAPI; falls back if unavailable (demo/sandbox).
Auth gating via _gate(request, level_name) pattern.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional, Dict, Any

# Lazy import FastAPI and WebSocket dependencies
try:
    from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request, Query, HTTPException, status
    from pydantic import BaseModel, Field
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    APIRouter = None  # type: ignore
    WebSocket = None  # type: ignore
    WebSocketDisconnect = None  # type: ignore
    Request = None  # type: ignore
    Query = None  # type: ignore
    HTTPException = None  # type: ignore
    status = None  # type: ignore
    BaseModel = object  # type: ignore
    Field = None  # type: ignore

from rosteriq.ws_hub import get_hub, get_notification_store

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore

logger = logging.getLogger(__name__)


# ============================================================================
# Auth Gating Helper
# ============================================================================


async def _gate(request: Request, level_name: str) -> None:
    """
    Apply role gating if auth stack is present; no-op in demo.

    Args:
        request: FastAPI Request object
        level_name: AccessLevel attribute name (e.g., 'L1_SUPERVISOR')
    """
    if require_access is None or AccessLevel is None or not FASTAPI_AVAILABLE:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ============================================================================
# Request/Response Models
# ============================================================================

if FASTAPI_AVAILABLE:

    class NotificationResponse(BaseModel):
        """Notification response model."""

        id: str = Field(..., description="Notification ID")
        venue_id: str = Field(..., description="Venue ID")
        kind: str = Field(..., description="Notification kind (cut_recommendation, etc.)")
        title: str = Field(..., description="Short title")
        body: str = Field(..., description="Detailed message")
        data: Dict[str, Any] = Field(default_factory=dict, description="Additional data")
        created_at: str = Field(..., description="ISO timestamp")
        severity: str = Field(..., description="Severity level (info, warning, critical)")
        acknowledged: bool = Field(..., description="Whether acknowledged")

    class ListNotificationsResponse(BaseModel):
        """List notifications response."""

        venue_id: str = Field(..., description="Venue ID")
        notifications: List[NotificationResponse] = Field(..., description="List of notifications")
        total: int = Field(..., description="Total count")

    class AcknowledgeRequest(BaseModel):
        """Request to acknowledge a notification."""

        notification_id: str = Field(..., description="Notification ID to acknowledge")

    class AcknowledgeResponse(BaseModel):
        """Response after acknowledging."""

        success: bool = Field(..., description="Whether acknowledgement succeeded")
        notification_id: str = Field(..., description="Notification ID")

    class TestNotificationRequest(BaseModel):
        """Request to publish a test notification."""

        venue_id: str = Field(..., description="Venue ID")
        kind: str = Field(..., description="Notification kind")
        title: str = Field(..., description="Notification title")
        body: str = Field(..., description="Notification body")

    class TestNotificationResponse(BaseModel):
        """Response from test notification."""

        success: bool = Field(..., description="Whether publish succeeded")
        notification_id: str = Field(..., description="Notification ID")

# ============================================================================
# Router
# ============================================================================

if FASTAPI_AVAILABLE:
    router = APIRouter(prefix="/api/v1", tags=["notifications"])

    # ────────────────────────────────────────────────────────────────────────
    # WebSocket Endpoint
    # ────────────────────────────────────────────────────────────────────────

    @router.websocket("/ws/{venue_id}")
    async def websocket_endpoint(websocket: WebSocket, venue_id: str) -> None:
        """
        WebSocket endpoint for live notifications.

        On connect, subscribes the connection to the hub for the venue.
        On disconnect, unsubscribes.

        Also handles incoming JSON messages:
        - {"type": "acknowledge", "notification_id": "..."}

        Args:
            websocket: WebSocket connection
            venue_id: Venue identifier
        """
        hub = get_hub()
        store = get_notification_store()

        # Create async send function for this connection
        async def send_notification(data: Dict[str, Any]) -> None:
            """Send JSON to WebSocket."""
            try:
                await websocket.send_json(data)
            except Exception as e:
                logger.error(f"Error sending to WebSocket: {e}")
                raise

        # Accept connection and subscribe
        await websocket.accept()
        hub.subscribe(venue_id, send_notification)
        logger.info(f"WebSocket connected for venue {venue_id}")

        try:
            while True:
                # Receive and handle incoming messages
                data = await websocket.receive_json()

                if data.get("type") == "acknowledge":
                    notif_id = data.get("notification_id")
                    if notif_id:
                        store.acknowledge(notif_id)
                        logger.debug(f"Acknowledged notification {notif_id}")

        except WebSocketDisconnect:
            hub.unsubscribe(venue_id, send_notification)
            logger.info(f"WebSocket disconnected for venue {venue_id}")
        except Exception as e:
            logger.error(f"WebSocket error for venue {venue_id}: {e}")
            hub.unsubscribe(venue_id, send_notification)

    # ────────────────────────────────────────────────────────────────────────
    # REST Endpoints
    # ────────────────────────────────────────────────────────────────────────

    @router.get("/notifications/{venue_id}", response_model=ListNotificationsResponse)
    async def list_notifications(
        venue_id: str,
        request: Request,
        limit: int = Query(50, ge=1, le=200, description="Max notifications to return"),
    ) -> ListNotificationsResponse:
        """
        List recent notifications for a venue.

        Returns newest first.

        Requires: L1 Supervisor or higher

        Args:
            venue_id: Venue identifier
            limit: Max notifications to return (default 50)

        Returns:
            List of notifications
        """
        await _gate(request, "L1_SUPERVISOR")

        store = get_notification_store()
        notifications = store.list_for_venue(venue_id, limit=limit)

        return ListNotificationsResponse(
            venue_id=venue_id,
            notifications=[NotificationResponse(**n.to_dict()) for n in notifications],
            total=len(notifications),
        )

    @router.post("/notifications/{venue_id}/acknowledge", response_model=AcknowledgeResponse)
    async def acknowledge_notification(
        venue_id: str,
        request: Request,
        body: AcknowledgeRequest,
    ) -> AcknowledgeResponse:
        """
        Acknowledge a notification.

        Marks the notification as read/acknowledged.

        Requires: L1 Supervisor or higher

        Args:
            venue_id: Venue identifier
            body: Request body with notification_id

        Returns:
            Success status

        Raises:
            HTTPException 404: If notification not found
        """
        await _gate(request, "L1_SUPERVISOR")

        store = get_notification_store()
        success = store.acknowledge(body.notification_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Notification {body.notification_id} not found",
            )

        return AcknowledgeResponse(success=True, notification_id=body.notification_id)

    @router.post("/notifications/test", response_model=TestNotificationResponse)
    async def publish_test_notification(
        request: Request,
        body: TestNotificationRequest,
    ) -> TestNotificationResponse:
        """
        Publish a test notification (for development).

        Creates a notification and broadcasts to all subscribers for the venue.

        Requires: L2 Roster Maker or higher

        Args:
            body: Request body with venue_id, kind, title, body

        Returns:
            Notification ID
        """
        await _gate(request, "L2_ROSTER_MAKER")

        from rosteriq.ws_hub import Notification

        notif = Notification(
            venue_id=body.venue_id,
            kind=body.kind,
            title=body.title,
            body=body.body,
            data={"test": True},
            severity="info",
        )

        hub = get_hub()
        hub.publish(body.venue_id, notif)
        logger.info(f"Published test notification {notif.id} to {body.venue_id}")

        return TestNotificationResponse(success=True, notification_id=notif.id)

else:
    # FastAPI not available; create a no-op router
    router = None  # type: ignore
