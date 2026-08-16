"""
Roster Publishing Routes for RosterIQ.

REST API endpoints for managing roster publishing lifecycle, from submission through
publication, approval, recall, and archival.

Endpoints:
- POST /api/v1/rosters/{id}/publish — initiate publish workflow
- POST /api/v1/rosters/{id}/recall — recall a published roster
- POST /api/v1/rosters/{id}/archive — archive a roster
- GET /api/v1/rosters/{id}/state — get current state and history
- POST /api/v1/rosters/{id}/transition — manual state transition (admin)
- GET /api/v1/venues/{id}/publication-history — publication history
- POST /api/v1/rosters/{id}/auto-publish — auto-publish if clean
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Path, Query
from pydantic import BaseModel, Field

from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.database import get_db
from rosteriq.services.roster_publisher import (
    publisher,
    RosterPublishState,
    PublishResult,
    RosterState,
    PublicationEvent,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["publishing"])


def _load_roster_scoped(roster_id: str):
    """
    Load a roster by id and enforce that it belongs to one of the caller's
    venues (owner passes everything). Denial is reported as 404 — identical to
    a missing roster — so roster ids are not an oracle for other tenants' data.
    """
    roster = get_db().get_roster(roster_id)
    not_found = f"Roster {roster_id} not found"
    if not roster:
        raise HTTPException(status_code=404, detail=not_found)
    try:
        enforce_venue_access(roster.venue_id)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise HTTPException(status_code=404, detail=not_found)
        raise
    return roster


# ============================================================================
# Request/Response Models
# ============================================================================


class PublishRosterRequest(BaseModel):
    """Request to publish a roster."""
    skip_approval: bool = Field(
        default=False,
        description="Admin override to skip approval workflow",
    )


class PublishRosterResponse(BaseModel):
    """Response after publishing a roster."""
    success: bool
    roster_id: str
    state: str
    conflicts_found: int
    critical_conflicts: int
    tanda_push_result: Optional[dict] = None
    notifications_sent: int
    message: str
    timestamp: str
    approval_request_id: Optional[str] = None


class RecallRosterRequest(BaseModel):
    """Request to recall (un-publish) a roster."""
    reason: str = Field(
        ...,
        description="Reason for recalling the roster",
        min_length=1,
        max_length=500,
    )


class ArchiveRosterRequest(BaseModel):
    """Request to archive a roster."""
    pass


class StateTransitionRequest(BaseModel):
    """Request to manually transition roster state (admin)."""
    new_state: str = Field(
        ...,
        description="New state (draft, pending_review, approved, published, archived, rejected, failed)",
    )
    reason: str = Field(
        ...,
        description="Reason for manual transition",
        min_length=1,
        max_length=500,
    )


class RosterStateResponse(BaseModel):
    """Response with roster state and history."""
    roster_id: str
    current_state: str
    state_history: List[tuple] = Field(
        default_factory=list,
        description="List of (state, timestamp, actor_id) tuples",
    )
    created_at: str = ""
    last_modified_by: str = ""
    last_modified_at: str = ""


class PublicationEventResponse(BaseModel):
    """Response with publication event details."""
    roster_id: str
    venue_id: str
    state: str
    publisher_id: str
    timestamp: str
    details: dict = Field(default_factory=dict)


class PublicationHistoryResponse(BaseModel):
    """Response with publication history."""
    venue_id: str
    events: List[PublicationEventResponse]
    total_count: int


# ============================================================================
# Route Handlers
# ============================================================================


@router.post(
    "/rosters/{roster_id}/publish",
    response_model=PublishRosterResponse,
    summary="Publish a roster",
    description="Initiate the roster publishing workflow including conflict detection, approval, and Tanda push.",
)
async def publish_roster(
    roster_id: str = Path(..., description="Roster ID"),
    body: PublishRosterRequest = None,
    user: UserContext = Depends(get_current_user),
) -> PublishRosterResponse:
    """
    Publish a roster.

    Orchestrates the complete workflow:
    1. Validate roster state
    2. Detect conflicts
    3. Submit for approval (unless skip_approval=True)
    4. Push to Tanda (if configured)
    5. Notify staff

    Args:
        roster_id: ID of roster to publish
        body: Request body with optional skip_approval flag
        user: Current authenticated user

    Returns:
        PublishRosterResponse with outcome

    Raises:
        404: Roster not found
        400: Roster in invalid state for publishing
    """
    if body is None:
        body = PublishRosterRequest()

    logger.info(
        f"Publishing roster {roster_id} by user {user.user_id} "
        f"(skip_approval={body.skip_approval})"
    )

    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Verify permissions
    if user.role not in ("owner", "manager") and body.skip_approval:
        raise HTTPException(
            status_code=403,
            detail="Only managers and owners can skip approval workflow",
        )

    # Publish
    result = await publisher.publish_roster(
        roster_id=roster_id,
        publisher_id=user.user_id,
        skip_approval=body.skip_approval and user.role in ("owner", "manager"),
    )

    return PublishRosterResponse(
        success=result.success,
        roster_id=result.roster_id,
        state=result.state,
        conflicts_found=result.conflicts_found,
        critical_conflicts=result.critical_conflicts,
        tanda_push_result=result.tanda_push_result,
        notifications_sent=result.notifications_sent,
        message=result.message,
        timestamp=result.timestamp,
        approval_request_id=result.approval_request_id,
    )


@router.post(
    "/rosters/{roster_id}/recall",
    response_model=dict,
    summary="Recall a published roster",
    description="Un-publish a roster, returning it to DRAFT state for revision.",
)
async def recall_roster(
    roster_id: str = Path(..., description="Roster ID"),
    body: RecallRosterRequest = None,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """
    Recall (un-publish) a published roster.

    Only published rosters can be recalled. Recalled rosters return to DRAFT
    state and can be re-edited and re-published.

    Args:
        roster_id: ID of roster to recall
        body: Request body with recall reason
        user: Current authenticated user

    Returns:
        Success status with message

    Raises:
        404: Roster not found
        400: Roster not in PUBLISHED state
        403: User lacks permission
    """
    if body is None:
        raise HTTPException(status_code=400, detail="Request body required")

    logger.info(f"Recalling roster {roster_id} by user {user.user_id}: {body.reason}")

    # Verify user is manager or owner
    if user.role not in ("owner", "manager"):
        raise HTTPException(
            status_code=403,
            detail="Only managers and owners can recall rosters",
        )

    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Recall
    success = publisher.recall_roster(roster_id=roster_id, reason=body.reason)

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot recall roster {roster_id} (must be in PUBLISHED state)",
        )

    return {
        "success": True,
        "roster_id": roster_id,
        "message": f"Roster recalled: {body.reason}",
    }


@router.post(
    "/rosters/{roster_id}/archive",
    response_model=dict,
    summary="Archive a roster",
    description="Archive a published roster. Archived rosters are read-only.",
)
async def archive_roster(
    roster_id: str = Path(..., description="Roster ID"),
    user: UserContext = Depends(get_current_user),
) -> dict:
    """
    Archive a published roster.

    Archived rosters are read-only and do not appear in active roster views.
    Can only archive PUBLISHED rosters.

    Args:
        roster_id: ID of roster to archive
        user: Current authenticated user

    Returns:
        Success status with message

    Raises:
        404: Roster not found
        400: Roster not in PUBLISHED state
        403: User lacks permission
    """
    logger.info(f"Archiving roster {roster_id} by user {user.user_id}")

    # Verify user is manager or owner
    if user.role not in ("owner", "manager"):
        raise HTTPException(
            status_code=403,
            detail="Only managers and owners can archive rosters",
        )

    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Archive
    success = publisher.archive_roster(roster_id=roster_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot archive roster {roster_id} (must be in PUBLISHED state)",
        )

    return {
        "success": True,
        "roster_id": roster_id,
        "message": "Roster archived",
    }


@router.get(
    "/rosters/{roster_id}/state",
    response_model=RosterStateResponse,
    summary="Get roster state",
    description="Get current publishing state and full state transition history.",
)
async def get_roster_state(
    roster_id: str = Path(..., description="Roster ID"),
    user: UserContext = Depends(get_current_user),
) -> RosterStateResponse:
    """
    Get the current state and history of a roster.

    Returns the current publishing state (DRAFT, PENDING_REVIEW, APPROVED, etc.)
    and the complete state transition history.

    Args:
        roster_id: ID of roster
        user: Current authenticated user

    Returns:
        RosterStateResponse with current state and history

    Raises:
        404: Roster not found
    """
    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Get state
    state = publisher.get_roster_state(roster_id=roster_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"State not found for {roster_id}")

    return RosterStateResponse(
        roster_id=state.roster_id,
        current_state=state.current_state,
        state_history=state.state_history,
        created_at=state.created_at,
        last_modified_by=state.last_modified_by,
        last_modified_at=state.last_modified_at,
    )


@router.post(
    "/rosters/{roster_id}/transition",
    response_model=dict,
    summary="Manual state transition (admin)",
    description="Manually transition roster to a specific state (admin only).",
)
async def transition_roster_state(
    roster_id: str = Path(..., description="Roster ID"),
    body: StateTransitionRequest = None,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """
    Manually transition a roster to a specific state (admin only).

    This endpoint is for administrative purposes and allows direct state
    transitions without normal workflow constraints.

    Args:
        roster_id: ID of roster
        body: Request body with new state and reason
        user: Current authenticated user (must be owner)

    Returns:
        Success status with message

    Raises:
        404: Roster not found
        400: Invalid state
        403: User lacks permission
    """
    if body is None:
        raise HTTPException(status_code=400, detail="Request body required")

    logger.info(
        f"Manual state transition for roster {roster_id} by user {user.user_id}: "
        f"{body.new_state} ({body.reason})"
    )

    # Verify user is owner
    if user.role != "owner":
        raise HTTPException(
            status_code=403,
            detail="Only owners can perform manual state transitions",
        )

    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Validate state
    valid_states = [s.value for s in RosterPublishState]
    if body.new_state not in valid_states:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid state: {body.new_state}. Valid: {', '.join(valid_states)}",
        )

    # Transition
    success = publisher.transition_state(
        roster_id=roster_id,
        new_state=body.new_state,
        reason=body.reason,
        actor_id=user.user_id,
    )

    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"State transition failed",
        )

    return {
        "success": True,
        "roster_id": roster_id,
        "new_state": body.new_state,
        "message": f"Transitioned to {body.new_state}: {body.reason}",
    }


@router.get(
    "/venues/{venue_id}/publication-history",
    response_model=PublicationHistoryResponse,
    summary="Get publication history",
    description="Get publication history for a venue.",
)
async def get_publication_history(
    venue_id: str = Path(..., description="Venue ID"),
    limit: int = Query(20, ge=1, le=100, description="Max events to return"),
    user: UserContext = Depends(get_current_user),
) -> PublicationHistoryResponse:
    """
    Get publication history for a venue.

    Returns the most recent roster publication events for a venue, including
    state transitions and publication details.

    Args:
        venue_id: ID of venue
        limit: Max events to return (default 20, max 100)
        user: Current authenticated user

    Returns:
        PublicationHistoryResponse with events list

    Raises:
        404: Venue not found
        403: User lacks permission
    """
    # Verify user can access this venue (tenant gate: 403 for other tenants' venues)
    enforce_venue_access(venue_id)

    db = get_db()
    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    # Verify permissions
    if user.role == "staff":
        raise HTTPException(
            status_code=403,
            detail="Staff cannot view publication history",
        )

    # Get history
    events = publisher.get_publication_history(venue_id=venue_id, limit=limit)

    return PublicationHistoryResponse(
        venue_id=venue_id,
        events=[
            PublicationEventResponse(
                roster_id=e.roster_id,
                venue_id=e.venue_id,
                state=e.state,
                publisher_id=e.publisher_id,
                timestamp=e.timestamp,
                details=e.details,
            )
            for e in events
        ],
        total_count=len(events),
    )


@router.post(
    "/rosters/{roster_id}/auto-publish",
    response_model=PublishRosterResponse,
    summary="Auto-publish if no conflicts",
    description="Automatically publish a roster if it has no critical conflicts.",
)
async def auto_publish_roster(
    roster_id: str = Path(..., description="Roster ID"),
    user: UserContext = Depends(get_current_user),
) -> PublishRosterResponse:
    """
    Auto-publish a roster if it has no conflicts.

    This is a convenience endpoint for programmatically-generated rosters
    that are guaranteed to be clean. If any conflicts are detected, the
    operation fails.

    Args:
        roster_id: ID of roster to auto-publish
        user: Current authenticated user

    Returns:
        PublishRosterResponse with outcome

    Raises:
        404: Roster not found
        400: Roster has conflicts
    """
    logger.info(f"Auto-publishing roster {roster_id} by user {user.user_id}")

    # Verify user is manager or owner
    if user.role not in ("owner", "manager"):
        raise HTTPException(
            status_code=403,
            detail="Only managers and owners can auto-publish",
        )

    # Verify roster exists AND belongs to one of the caller's venues (404 otherwise)
    roster = _load_roster_scoped(roster_id)

    # Auto-publish
    result = await publisher.auto_publish_if_no_conflicts(roster_id=roster_id)

    if not result.success and result.conflicts_found > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Auto-publish failed: {result.message}",
        )

    return PublishRosterResponse(
        success=result.success,
        roster_id=result.roster_id,
        state=result.state,
        conflicts_found=result.conflicts_found,
        critical_conflicts=result.critical_conflicts,
        tanda_push_result=result.tanda_push_result,
        notifications_sent=result.notifications_sent,
        message=result.message,
        timestamp=result.timestamp,
        approval_request_id=result.approval_request_id,
    )
