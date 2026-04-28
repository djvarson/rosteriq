"""
RosterIQ Message Templates Routes

REST endpoints for managing staff communication templates:
- GET /api/v1/venues/{venue_id}/message-templates — list all
- GET /api/v1/venues/{venue_id}/message-templates/{template_id} — get specific
- PUT /api/v1/venues/{venue_id}/message-templates/{template_id} — customize
- DELETE /api/v1/venues/{venue_id}/message-templates/{template_id} — reset
- POST /api/v1/venues/{venue_id}/message-templates/{template_id}/preview — preview
- POST /api/v1/venues/{venue_id}/message-templates/{template_id}/send — send
"""

import logging
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field

from rosteriq.middleware.auth import get_current_user
from rosteriq.services.message_templates import (
    get_message_template_service, MessageTemplate, RenderedMessage
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/venues",
    tags=["message-templates"]
)


# ============================================================================
# Request/Response Models
# ============================================================================

class MessageTemplateFieldsRequest(BaseModel):
    """Update specific template fields."""
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    sms_body: Optional[str] = None
    push_title: Optional[str] = None
    push_body: Optional[str] = None


class MessageTemplateResponse(BaseModel):
    """Message template response."""
    id: str
    name: str
    event_type: str
    email_subject: str
    email_body: str
    sms_body: str
    push_title: str
    push_body: str
    variables_schema: List[str]
    is_customised: bool
    venue_id: Optional[str] = None

    @staticmethod
    def from_template(template: MessageTemplate) -> "MessageTemplateResponse":
        """Convert MessageTemplate to response."""
        return MessageTemplateResponse(
            id=template.id,
            name=template.name,
            event_type=template.event_type,
            email_subject=template.email_subject,
            email_body=template.email_body,
            sms_body=template.sms_body,
            push_title=template.push_title,
            push_body=template.push_body,
            variables_schema=template.variables_schema,
            is_customised=template.is_customised,
            venue_id=template.venue_id,
        )


class MessageTemplateListResponse(BaseModel):
    """List of message templates."""
    templates: List[MessageTemplateResponse]
    count: int


class PreviewRequest(BaseModel):
    """Request to preview a template."""
    variables: Dict[str, Any] = Field(
        description="Template variables (use sample values)"
    )


class PreviewResponse(BaseModel):
    """Preview of rendered message."""
    email: Dict[str, str] = Field(description="Email subject and body")
    sms: Dict[str, str] = Field(description="SMS text")
    push: Dict[str, str] = Field(description="Push title and body")
    rendered_at: str = Field(description="ISO timestamp")


class SendRequest(BaseModel):
    """Request to send message from template."""
    variables: Dict[str, Any] = Field(description="Template variables")
    recipients: List[str] = Field(description="Email addresses or phone numbers")
    channels: List[str] = Field(
        default=["email"],
        description="Channels: email, sms, push, websocket"
    )


class SendResultResponse(BaseModel):
    """Result of send operation."""
    success: bool
    message_id: Optional[str] = None
    sent_count: int = 0
    failed_count: int = 0
    channel_results: Dict[str, bool] = {}
    error: Optional[str] = None


# ============================================================================
# Routes
# ============================================================================

@router.get(
    "/{venue_id}/message-templates",
    response_model=MessageTemplateListResponse,
    summary="List all message templates",
)
async def list_message_templates(
    venue_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> MessageTemplateListResponse:
    """
    Get all available message templates for a venue.

    Includes default templates and any venue-specific customizations.

    Args:
        venue_id: Venue ID
        current_user: Current authenticated user

    Returns:
        List of MessageTemplateResponse
    """
    try:
        svc = get_message_template_service()
        templates = svc.list_templates(venue_id)

        responses = [
            MessageTemplateResponse.from_template(t) for t in templates
        ]

        return MessageTemplateListResponse(
            templates=responses,
            count=len(responses),
        )
    except Exception as e:
        logger.error(f"Failed to list templates for venue {venue_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list templates",
        )


@router.get(
    "/{venue_id}/message-templates/{template_id}",
    response_model=MessageTemplateResponse,
    summary="Get a specific message template",
)
async def get_message_template(
    venue_id: str,
    template_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> MessageTemplateResponse:
    """
    Get a specific message template.

    Returns venue customization if available, otherwise default.

    Args:
        venue_id: Venue ID
        template_id: Template ID (e.g., "ROSTER_PUBLISHED")
        current_user: Current authenticated user

    Returns:
        MessageTemplateResponse
    """
    try:
        svc = get_message_template_service()
        template = svc.get_template(template_id, venue_id)

        if not template:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Template not found: {template_id}",
            )

        return MessageTemplateResponse.from_template(template)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to get template {template_id} for venue {venue_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get template",
        )


@router.put(
    "/{venue_id}/message-templates/{template_id}",
    response_model=MessageTemplateResponse,
    summary="Customize a message template for a venue",
)
async def customize_message_template(
    venue_id: str,
    template_id: str,
    request: MessageTemplateFieldsRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> MessageTemplateResponse:
    """
    Customize a message template for a specific venue.

    Only provided fields are updated; others retain default values.

    Args:
        venue_id: Venue ID
        template_id: Template ID
        request: Fields to customize
        current_user: Current authenticated user

    Returns:
        Updated MessageTemplateResponse
    """
    try:
        svc = get_message_template_service()

        # Build customizations dict with only provided fields
        customisations = {
            k: v for k, v in request.dict(exclude_unset=True).items()
            if v is not None
        }

        if not customisations:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No customizations provided",
            )

        updated = svc.update_template(venue_id, template_id, customisations)

        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Template not found: {template_id}",
            )

        return MessageTemplateResponse.from_template(updated)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to customize template {template_id} for venue {venue_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to customize template",
        )


@router.delete(
    "/{venue_id}/message-templates/{template_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Reset a customized template to default",
)
async def reset_message_template(
    venue_id: str,
    template_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """
    Reset a venue-customized template to its default version.

    Args:
        venue_id: Venue ID
        template_id: Template ID
        current_user: Current authenticated user
    """
    try:
        svc = get_message_template_service()
        success = svc.reset_template(venue_id, template_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Template not found: {template_id}",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to reset template {template_id} for venue {venue_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset template",
        )


@router.post(
    "/{venue_id}/message-templates/{template_id}/preview",
    response_model=PreviewResponse,
    summary="Preview a rendered message template",
)
async def preview_message_template(
    venue_id: str,
    template_id: str,
    request: PreviewRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> PreviewResponse:
    """
    Preview how a template will render with given variables.

    Does not send any messages. Validates variable substitution.

    Args:
        venue_id: Venue ID
        template_id: Template ID
        request: Variables for rendering
        current_user: Current authenticated user

    Returns:
        PreviewResponse with rendered content for all channels
    """
    try:
        svc = get_message_template_service()
        preview = svc.preview(template_id, request.variables, venue_id)

        if not preview:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Template not found: {template_id}",
            )

        return PreviewResponse(**preview)
    except ValueError as e:
        # Variable substitution error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to preview template {template_id} for venue {venue_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to preview template",
        )


@router.post(
    "/{venue_id}/message-templates/{template_id}/send",
    response_model=SendResultResponse,
    summary="Send a message from a template",
)
async def send_message_from_template(
    venue_id: str,
    template_id: str,
    request: SendRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> SendResultResponse:
    """
    Render and send a message from a template to recipients.

    Dispatches via notification hub respecting user preferences.

    Args:
        venue_id: Venue ID
        template_id: Template ID
        request: Recipients, variables, channels
        current_user: Current authenticated user

    Returns:
        SendResultResponse with send status and counts
    """
    try:
        if not request.recipients:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No recipients provided",
            )

        if not request.channels:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No channels specified",
            )

        svc = get_message_template_service()
        result = await svc.send_from_template(
            template_id=template_id,
            variables=request.variables,
            recipients=request.recipients,
            channels=request.channels,
            venue_id=venue_id,
        )

        return SendResultResponse(
            success=result.success,
            message_id=result.message_id,
            sent_count=result.sent_count,
            failed_count=result.failed_count,
            channel_results=result.channel_results,
            error=result.error,
        )
    except ValueError as e:
        # Variable substitution error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to send from template {template_id} for venue {venue_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send message",
        )
