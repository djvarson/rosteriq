"""FastAPI router for Staff Communication Hub endpoints.

Provides REST API for messaging:
- POST /api/v1/comms/send - Send message to one staff member
- POST /api/v1/comms/bulk - Bulk send to multiple staff
- GET /api/v1/comms/{venue_id}/messages - Message history
- GET /api/v1/comms/{venue_id}/stats - Delivery statistics
- GET /api/v1/comms/preferences/{employee_id} - Get notification preferences
- PUT /api/v1/comms/preferences/{employee_id} - Update notification preferences
- GET /api/v1/comms/templates - List available templates

All endpoints support optional auth gating via AccessLevel (demo mode: no-op).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.comms_hub import (
    Channel,
    MessagePriority,
    MessageStatus,
    StaffMessage,
    NotificationPreference,
    get_comms_store,
    check_quiet_hours,
    send_message,
    send_bulk,
    get_delivery_stats,
)

logger = logging.getLogger("rosteriq.comms_hub_router")

# Auth gating - fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ============================================================================
# Pydantic Models
# ============================================================================

class SendMessageRequest(BaseModel):
    """Request to send a message to one staff member."""
    venue_id: str = Field(..., description="Venue ID")
    recipient_id: str = Field(..., description="Employee ID")
    recipient_name: str = Field(..., description="Employee name")
    recipient_contact: str = Field(..., description="Phone or email")
    channel: Channel = Field(..., description="SMS, EMAIL, PUSH, or IN_APP")
    priority: MessagePriority = Field(default=MessagePriority.NORMAL, description="Message priority")
    subject: str = Field(..., description="Message subject")
    body: str = Field(..., description="Message body")
    template_id: Optional[str] = Field(None, description="Optional template ID for tracking")


class BulkSendRequest(BaseModel):
    """Request to bulk send to multiple staff."""
    venue_id: str = Field(..., description="Venue ID")
    recipient_ids: List[str] = Field(..., description="List of employee IDs")
    template_id: str = Field(..., description="Template ID from built-in templates")
    variables: Dict[str, str] = Field(..., description="Template variables")
    channel: Optional[Channel] = Field(None, description="Override preferred channel")
    priority: MessagePriority = Field(default=MessagePriority.NORMAL, description="Message priority")


class StaffMessageResponse(BaseModel):
    """Response containing a staff message."""
    message_id: str
    venue_id: str
    recipient_id: str
    recipient_name: str
    recipient_contact: str
    channel: str
    priority: str
    subject: str
    body: str
    status: str
    template_id: Optional[str] = None
    sent_at: Optional[str] = None
    delivered_at: Optional[str] = None
    read_at: Optional[str] = None
    error: Optional[str] = None

    @classmethod
    def from_message(cls, msg: StaffMessage) -> StaffMessageResponse:
        """Convert StaffMessage to response."""
        data = msg.to_dict()
        return cls(**data)


class BulkSendResponse(BaseModel):
    """Response from bulk send."""
    total: int
    sent: int
    failed: int
    messages: List[StaffMessageResponse]


class MessageHistoryResponse(BaseModel):
    """Response containing message history."""
    count: int
    messages: List[StaffMessageResponse]


class DeliveryStatsResponse(BaseModel):
    """Response containing delivery statistics."""
    total: int
    sent: int
    delivered: int
    failed: int
    read: int


class NotificationPreferenceRequest(BaseModel):
    """Request to update notification preferences."""
    preferred_channel: Channel = Field(..., description="Preferred channel")
    roster_changes: bool = Field(default=True, description="Receive roster change notifications")
    shift_offers: bool = Field(default=True, description="Receive shift offer notifications")
    announcements: bool = Field(default=True, description="Receive announcements")
    reminders: bool = Field(default=True, description="Receive reminders")
    quiet_hours_start: int = Field(default=22, description="Quiet hours start (0-23)")
    quiet_hours_end: int = Field(default=8, description="Quiet hours end (0-23)")


class NotificationPreferenceResponse(BaseModel):
    """Response containing notification preferences."""
    employee_id: str
    venue_id: str
    preferred_channel: str
    roster_changes: bool
    shift_offers: bool
    announcements: bool
    reminders: bool
    quiet_hours_start: int
    quiet_hours_end: int

    @classmethod
    def from_preference(cls, pref: NotificationPreference) -> NotificationPreferenceResponse:
        """Convert NotificationPreference to response."""
        data = pref.to_dict()
        return cls(**data)


class MessageTemplate(BaseModel):
    """Response containing a message template."""
    template_id: str
    name: str
    channel: str
    subject_template: str
    body_template: str
    variables: List[str]


class TemplateListResponse(BaseModel):
    """Response containing list of templates."""
    count: int
    templates: List[MessageTemplate]


# ============================================================================
# Router
# ============================================================================

router = APIRouter(prefix="/api/v1/comms", tags=["communications"])


# ============================================================================
# Endpoints
# ============================================================================

@router.post(
    "/send",
    response_model=StaffMessageResponse,
    summary="Send message to one staff member",
    status_code=201,
)
async def send_message_endpoint(
    request: Request,
    body: SendMessageRequest,
) -> StaffMessageResponse:
    """Send a message to one staff member."""
    await _gate(request, "L2")

    store = get_comms_store()

    # Create message
    message = StaffMessage(
        message_id=f"msg_{__import__('uuid').uuid4().hex[:12]}",
        venue_id=body.venue_id,
        recipient_id=body.recipient_id,
        recipient_name=body.recipient_name,
        recipient_contact=body.recipient_contact,
        channel=body.channel,
        priority=body.priority,
        subject=body.subject,
        body=body.body,
        status=MessageStatus.QUEUED,
        template_id=body.template_id,
    )

    # Send it
    sent = send_message(message)
    return StaffMessageResponse.from_message(sent)


@router.post(
    "/bulk",
    response_model=BulkSendResponse,
    summary="Bulk send to multiple staff",
    status_code=201,
)
async def bulk_send_endpoint(
    request: Request,
    body: BulkSendRequest,
) -> BulkSendResponse:
    """Bulk send a templated message to multiple staff members."""
    await _gate(request, "L2")

    if not body.recipient_ids:
        raise HTTPException(status_code=400, detail="recipient_ids cannot be empty")

    result = send_bulk(
        venue_id=body.venue_id,
        recipient_ids=body.recipient_ids,
        template_id=body.template_id,
        variables=body.variables,
        channel=body.channel,
        priority=body.priority,
    )

    return BulkSendResponse(
        total=result.total,
        sent=result.sent,
        failed=result.failed,
        messages=[StaffMessageResponse.from_message(m) for m in result.messages],
    )


@router.get(
    "/{venue_id}/messages",
    response_model=MessageHistoryResponse,
    summary="Get message history",
)
async def get_messages_endpoint(
    request: Request,
    venue_id: str,
    employee_id: Optional[str] = None,
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 100,
) -> MessageHistoryResponse:
    """Get message history for a venue with optional filtering."""
    await _gate(request, "L1")

    store = get_comms_store()

    # Parse optional filters
    status_filter = None
    if status:
        try:
            status_filter = MessageStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    channel_filter = None
    if channel:
        try:
            channel_filter = Channel(channel)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid channel: {channel}")

    messages = store.get_messages(
        venue_id=venue_id,
        employee_id=employee_id,
        status=status_filter,
        channel=channel_filter,
        limit=limit,
    )

    return MessageHistoryResponse(
        count=len(messages),
        messages=[StaffMessageResponse.from_message(m) for m in messages],
    )


@router.get(
    "/{venue_id}/stats",
    response_model=DeliveryStatsResponse,
    summary="Get delivery statistics",
)
async def get_stats_endpoint(
    request: Request,
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> DeliveryStatsResponse:
    """Get delivery statistics for a venue."""
    await _gate(request, "L2")

    stats = get_delivery_stats(venue_id, date_from=date_from, date_to=date_to)
    return DeliveryStatsResponse(**stats)


@router.get(
    "/preferences/{employee_id}",
    response_model=NotificationPreferenceResponse,
    summary="Get notification preferences",
)
async def get_preferences_endpoint(
    request: Request,
    employee_id: str,
    venue_id: str,
) -> NotificationPreferenceResponse:
    """Get notification preferences for an employee."""
    await _gate(request, "L1")

    store = get_comms_store()
    pref = store.get_preference(employee_id, venue_id)
    return NotificationPreferenceResponse.from_preference(pref)


@router.put(
    "/preferences/{employee_id}",
    response_model=NotificationPreferenceResponse,
    summary="Update notification preferences",
)
async def update_preferences_endpoint(
    request: Request,
    employee_id: str,
    venue_id: str,
    body: NotificationPreferenceRequest,
) -> NotificationPreferenceResponse:
    """Update notification preferences for an employee."""
    await _gate(request, "L1")

    store = get_comms_store()

    pref = NotificationPreference(
        employee_id=employee_id,
        venue_id=venue_id,
        preferred_channel=body.preferred_channel,
        roster_changes=body.roster_changes,
        shift_offers=body.shift_offers,
        announcements=body.announcements,
        reminders=body.reminders,
        quiet_hours_start=body.quiet_hours_start,
        quiet_hours_end=body.quiet_hours_end,
    )

    saved = store.save_preference(pref)
    return NotificationPreferenceResponse.from_preference(saved)


@router.get(
    "/templates",
    response_model=TemplateListResponse,
    summary="List available templates",
)
async def list_templates_endpoint(
    request: Request,
) -> TemplateListResponse:
    """List all available message templates."""
    await _gate(request, "L1")

    store = get_comms_store()
    templates = store.list_templates()

    return TemplateListResponse(
        count=len(templates),
        templates=[
            MessageTemplate(
                template_id=t.template_id,
                name=t.name,
                channel=t.channel.value,
                subject_template=t.subject_template,
                body_template=t.body_template,
                variables=t.variables,
            )
            for t in templates
        ],
    )
