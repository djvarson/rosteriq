"""
RosterIQ Notification Hub Routes

REST endpoints for the unified notification dispatch hub:
- POST /api/v1/notifications/dispatch — manual dispatch (admin)
- GET /api/v1/notifications/history — notification audit log
- GET /api/v1/notifications/preferences/{employee_id} — get preferences
- PUT /api/v1/notifications/preferences/{employee_id} — update preferences
- GET /api/v1/notifications/stats — dispatch statistics
"""

import logging
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel

from rosteriq.middleware.auth import get_current_user
from rosteriq.services.notification_hub import (
    get_notification_hub,
    NotificationEventType,
)
from rosteriq.services.notification_preferences import get_preferences_service
from rosteriq.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications_hub"])


# ============================================================================
# Request/Response Models
# ============================================================================


class DispatchRequest(BaseModel):
    """Request to dispatch a notification."""
    event_type: str  # One of NotificationEventType values
    venue_id: str
    payload: Dict[str, Any] = {}
    target_employee_ids: Optional[List[str]] = None


class DispatchResponse(BaseModel):
    """Response from dispatch request."""
    event_type: str
    venue_id: str
    total_targets: int
    sent: Dict[str, int]
    failed: Dict[str, int]
    skipped: Dict[str, int]


class BulkDispatchRequest(BaseModel):
    """Request to dispatch multiple events."""
    events: List[DispatchRequest]


class BulkDispatchResponse(BaseModel):
    """Response from bulk dispatch."""
    events_processed: int
    results: List[Dict[str, Any]]


class AuditLogEntry(BaseModel):
    """Single audit log entry."""
    timestamp: str
    event_type: str
    employee_id: str
    venue_id: str
    channels_sent: Dict[str, bool]
    status: str


class AuditLogResponse(BaseModel):
    """Response containing audit log entries."""
    total: int
    entries: List[AuditLogEntry]


class NotificationStats(BaseModel):
    """Notification dispatch statistics."""
    total_entries: int
    by_channel: Optional[Dict[str, int]] = None
    by_event_type: Optional[Dict[str, int]] = None
    by_status: Optional[Dict[str, int]] = None


class ChannelPreferences(BaseModel):
    """Notification channel preferences."""
    email: bool = True
    sms: bool = False
    push: bool = False


class NotificationTypePreferences(BaseModel):
    """Notification type preferences."""
    shift_reminders: bool = True
    roster_published: bool = True
    swap_updates: bool = True
    compliance_alerts: bool = True


class QuietHoursPreferences(BaseModel):
    """Quiet hours configuration."""
    enabled: bool = True
    start: str = "22:00"  # HH:MM format
    end: str = "07:00"    # HH:MM format


class NotificationPreferencesRequest(BaseModel):
    """Request to update notification preferences."""
    channels: Optional[ChannelPreferences] = None
    notification_types: Optional[NotificationTypePreferences] = None
    quiet_hours: Optional[QuietHoursPreferences] = None


class NotificationPreferencesResponse(BaseModel):
    """Notification preferences response."""
    channels: ChannelPreferences
    notification_types: NotificationTypePreferences
    quiet_hours: QuietHoursPreferences


# ============================================================================
# Routes
# ============================================================================


@router.post(
    "/dispatch",
    response_model=DispatchResponse,
    summary="Dispatch notification to employees",
    status_code=status.HTTP_202_ACCEPTED,
)
async def dispatch_notification(
    request: DispatchRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> DispatchResponse:
    """
    Dispatch a notification event to target employees.

    Routes to email, SMS, push, and websocket based on employee preferences.

    Args:
        request: Dispatch request with event type, venue, and payload
        current_user: Current authenticated user (admin/manager)

    Returns:
        Summary of dispatch (sent counts, failed, skipped)

    Raises:
        HTTPException: If invalid event type or user not authorized
    """
    # Verify user is admin or manager
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    # Validate event type
    try:
        event_type = NotificationEventType(request.event_type)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event type. Must be one of: {', '.join([e.value for e in NotificationEventType])}",
        )

    try:
        hub = get_notification_hub()
        result = await hub.dispatch(
            event_type,
            request.venue_id,
            request.payload,
            request.target_employee_ids,
        )

        if "error" in result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result["error"],
            )

        return DispatchResponse(
            event_type=result["event_type"],
            venue_id=result["venue_id"],
            total_targets=result["total_targets"],
            sent=result["sent"],
            failed=result["failed"],
            skipped=result["skipped"],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dispatch failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to dispatch notification",
        )


@router.post(
    "/dispatch/bulk",
    response_model=BulkDispatchResponse,
    summary="Dispatch multiple notifications",
    status_code=status.HTTP_202_ACCEPTED,
)
async def bulk_dispatch(
    request: BulkDispatchRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> BulkDispatchResponse:
    """
    Dispatch multiple notification events.

    Groups by channel for efficiency.

    Args:
        request: Bulk dispatch request with list of events
        current_user: Current authenticated user (admin/manager)

    Returns:
        Summary of all dispatches

    Raises:
        HTTPException: If invalid request or user not authorized
    """
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    try:
        hub = get_notification_hub()

        # Convert requests to dispatch format
        events = []
        for req in request.events:
            try:
                event_type = NotificationEventType(req.event_type)
                events.append({
                    "event_type": event_type.value,
                    "venue_id": req.venue_id,
                    "payload": req.payload,
                    "target_employee_ids": req.target_employee_ids,
                })
            except ValueError:
                logger.warning(f"Skipping event with invalid type: {req.event_type}")
                continue

        if not events:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid events to dispatch",
            )

        result = await hub.dispatch_bulk(events)

        if "error" in result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result["error"],
            )

        return BulkDispatchResponse(
            events_processed=result["events_processed"],
            results=result["results"],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bulk dispatch failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to dispatch notifications",
        )


@router.get(
    "/history",
    response_model=AuditLogResponse,
    summary="Get notification audit log",
)
async def get_audit_log(
    venue_id: Optional[str] = Query(None),
    employee_id: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> AuditLogResponse:
    """
    Get notification audit log with optional filters.

    Args:
        venue_id: Filter by venue (optional)
        employee_id: Filter by employee (optional)
        event_type: Filter by event type (optional)
        limit: Max results to return (default 100, max 1000)
        current_user: Current authenticated user (admin/manager)

    Returns:
        List of audit log entries
    """
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    try:
        hub = get_notification_hub()
        entries = hub.get_audit_log(
            venue_id=venue_id,
            employee_id=employee_id,
            event_type=event_type,
            limit=limit,
        )

        return AuditLogResponse(
            total=len(entries),
            entries=[AuditLogEntry(**e) for e in entries],
        )
    except Exception as e:
        logger.error(f"Failed to retrieve audit log: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve audit log",
        )


@router.get(
    "/stats",
    response_model=NotificationStats,
    summary="Get notification dispatch statistics",
)
async def get_notification_stats(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> NotificationStats:
    """
    Get notification dispatch statistics.

    Returns stats on sent/failed notifications by channel and event type.

    Args:
        current_user: Current authenticated user (admin/manager)

    Returns:
        Notification dispatch statistics
    """
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    try:
        hub = get_notification_hub()
        stats = hub.get_stats()

        return NotificationStats(**stats)
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get statistics",
        )


@router.get(
    "/preferences/{employee_id}",
    response_model=NotificationPreferencesResponse,
    summary="Get employee notification preferences",
)
async def get_preferences(
    employee_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> NotificationPreferencesResponse:
    """
    Get notification preferences for an employee.

    Returns user's preferences or defaults if not yet customized.

    Args:
        employee_id: Employee ID
        current_user: Current authenticated user (admin/manager or same employee)

    Returns:
        Employee's notification preferences
    """
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    # Allow access if user is the employee or is admin
    if user_id != employee_id:
        # TODO: Check if user is admin/manager of employee's venue
        pass

    try:
        prefs_svc = get_preferences_service()
        prefs = prefs_svc.get_preferences(employee_id)

        return NotificationPreferencesResponse(
            channels=ChannelPreferences(**prefs["channels"]),
            notification_types=NotificationTypePreferences(**prefs["notification_types"]),
            quiet_hours=QuietHoursPreferences(**prefs["quiet_hours"]),
        )
    except Exception as e:
        logger.error(f"Failed to get preferences for {employee_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve preferences",
        )


@router.put(
    "/preferences/{employee_id}",
    response_model=NotificationPreferencesResponse,
    summary="Update employee notification preferences",
)
async def update_preferences(
    employee_id: str,
    request: NotificationPreferencesRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> NotificationPreferencesResponse:
    """
    Update notification preferences for an employee.

    Only provided fields are updated; others retain existing values.

    Args:
        employee_id: Employee ID
        request: Preferences to update
        current_user: Current authenticated user (admin/manager or same employee)

    Returns:
        Updated preferences
    """
    user_id = current_user.get("id")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    # Allow access if user is the employee or is admin
    if user_id != employee_id:
        # TODO: Check if user is admin/manager of employee's venue
        pass

    try:
        # Build update dict from request
        update_dict = {}
        if request.channels:
            update_dict["channels"] = request.channels.model_dump()
        if request.notification_types:
            update_dict["notification_types"] = request.notification_types.model_dump()
        if request.quiet_hours:
            update_dict["quiet_hours"] = request.quiet_hours.model_dump()

        prefs_svc = get_preferences_service()
        updated_prefs = prefs_svc.update_preferences(employee_id, update_dict)

        return NotificationPreferencesResponse(
            channels=ChannelPreferences(**updated_prefs["channels"]),
            notification_types=NotificationTypePreferences(**updated_prefs["notification_types"]),
            quiet_hours=QuietHoursPreferences(**updated_prefs["quiet_hours"]),
        )
    except Exception as e:
        logger.error(f"Failed to update preferences for {employee_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update preferences",
        )
