"""
RosterIQ Notification Preferences Routes

REST endpoints for managing user notification preferences:
- GET /api/notifications/preferences — get current user's prefs
- PUT /api/notifications/preferences — update prefs
- POST /api/notifications/test-sms — send test SMS to verify number
"""

import logging
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from rosteriq.middleware.auth import get_current_user
from rosteriq.services.notification_preferences import get_preferences_service
from rosteriq.services.sms import get_sms_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


# ============================================================================
# Request/Response Models
# ============================================================================


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


class TestSmsRequest(BaseModel):
    """Request to send test SMS."""
    phone: str


class TestSmsResponse(BaseModel):
    """Response from test SMS request."""
    success: bool
    message: str


# ============================================================================
# Routes
# ============================================================================


@router.get(
    "/preferences",
    response_model=NotificationPreferencesResponse,
    summary="Get current user's notification preferences",
)
async def get_notification_preferences(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> NotificationPreferencesResponse:
    """
    Get notification preferences for the current user.

    Returns user's preferences or defaults if not yet customized.

    Args:
        current_user: Current authenticated user (from JWT token)

    Returns:
        User's notification preferences
    """
    user_id = current_user.user_id
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    prefs_svc = get_preferences_service()
    prefs = prefs_svc.get_preferences(user_id)

    return NotificationPreferencesResponse(
        channels=ChannelPreferences(**prefs["channels"]),
        notification_types=NotificationTypePreferences(**prefs["notification_types"]),
        quiet_hours=QuietHoursPreferences(**prefs["quiet_hours"]),
    )


@router.put(
    "/preferences",
    response_model=NotificationPreferencesResponse,
    summary="Update notification preferences",
)
async def update_notification_preferences(
    request: NotificationPreferencesRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> NotificationPreferencesResponse:
    """
    Update notification preferences for the current user.

    Only provided fields are updated; others retain existing values.

    Args:
        request: Preferences to update
        current_user: Current authenticated user (from JWT token)

    Returns:
        Updated preferences
    """
    user_id = current_user.user_id
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not identified",
        )

    # Build update dict from request
    update_dict = {}
    if request.channels:
        update_dict["channels"] = request.channels.model_dump()
    if request.notification_types:
        update_dict["notification_types"] = request.notification_types.model_dump()
    if request.quiet_hours:
        update_dict["quiet_hours"] = request.quiet_hours.model_dump()

    prefs_svc = get_preferences_service()
    updated_prefs = prefs_svc.update_preferences(user_id, update_dict)

    return NotificationPreferencesResponse(
        channels=ChannelPreferences(**updated_prefs["channels"]),
        notification_types=NotificationTypePreferences(**updated_prefs["notification_types"]),
        quiet_hours=QuietHoursPreferences(**updated_prefs["quiet_hours"]),
    )


@router.post(
    "/test-sms",
    response_model=TestSmsResponse,
    summary="Send test SMS to verify phone number",
)
async def test_sms(
    request: TestSmsRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> TestSmsResponse:
    """
    Send test SMS to verify phone number is correctly configured.

    Useful for users to validate their phone number before enabling
    SMS notifications.

    Args:
        request: Phone number to test
        current_user: Current authenticated user (from JWT token)

    Returns:
        Success status and message
    """
    phone = request.phone.strip()
    if not phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phone number is required",
        )

    sms_svc = get_sms_service()

    # Send test SMS
    success = await sms_svc.send_sms(
        phone,
        "RosterIQ test message. If you received this, your phone number is configured correctly.",
    )

    if not success:
        return TestSmsResponse(
            success=False,
            message="Failed to send SMS. Please check phone number and try again. Ensure Twilio is configured.",
        )

    return TestSmsResponse(
        success=True,
        message="Test SMS sent successfully. Check your phone.",
    )
