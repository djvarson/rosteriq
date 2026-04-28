"""
Shift Handover Notes API routes for RosterIQ.

Endpoints for creating, retrieving, and acknowledging shift handover notes
that facilitate communication between outgoing and incoming staff.

Endpoints:
- POST /api/v1/shifts/{id}/handover — create handover note
- GET /api/v1/shifts/{id}/handover — get handover note
- POST /api/v1/handover/{id}/acknowledge — acknowledge receipt
- GET /api/v1/employees/{id}/incoming-handovers — my incoming handovers
- GET /api/v1/venues/{id}/handovers/{date} — venue handovers for date
- GET /api/v1/venues/{id}/handovers/unacknowledged — outstanding handovers
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Path, Query, Body
from pydantic import BaseModel, Field

from rosteriq.services.handover_notes import (
    get_handover_service,
    HandoverNote,
    HandoverPriority,
    PrepStatus,
    PrepItem,
    VIPInfo,
    VIPTable,
    MaintenanceIssues,
    Issue,
    StockAlerts,
    StockItem,
    GeneralNotes,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["handover"])


# ============================================================================
# Request/Response Models
# ============================================================================


class PrepItemRequest(BaseModel):
    """Request model for a prep item."""
    name: str = Field(..., description="Name of prep item")
    status: str = Field(..., description="Status: done, in_progress, pending")
    notes: str = Field("", description="Additional notes")
    due_time: Optional[str] = Field(None, description="Due time (HH:MM format)")


class PrepStatusRequest(BaseModel):
    """Request model for prep status section."""
    items: List[PrepItemRequest] = Field(default_factory=list)


class VIPTableRequest(BaseModel):
    """Request model for VIP table info."""
    table_number: str = Field(..., description="Table number or identifier")
    guest_name: str = Field(..., description="Name of guest/party")
    party_size: int = Field(..., description="Number of guests")
    arrival_time: Optional[str] = Field(None, description="Expected arrival time")
    dietary_restrictions: str = Field("", description="Dietary restrictions")
    special_requests: str = Field("", description="Special requests or preferences")
    seating_preference: str = Field("", description="Seating preferences")


class VIPInfoRequest(BaseModel):
    """Request model for VIP info section."""
    tables: List[VIPTableRequest] = Field(default_factory=list)


class IssueRequest(BaseModel):
    """Request model for a maintenance/operational issue."""
    category: str = Field(..., description="equipment, plumbing, electrical, other")
    description: str = Field(..., description="Description of issue")
    severity: str = Field(..., description="Severity: low, medium, high")
    location: str = Field(..., description="Location of issue")
    reported_time: Optional[str] = Field(None, description="Time issue was reported")
    action_required: str = Field("", description="Action required to resolve")


class MaintenanceIssuesRequest(BaseModel):
    """Request model for maintenance issues section."""
    issues: List[IssueRequest] = Field(default_factory=list)


class StockItemRequest(BaseModel):
    """Request model for a stock alert."""
    item_name: str = Field(..., description="Name of item")
    current_level: str = Field(..., description="low, critical, sufficient")
    quantity_remaining: Optional[str] = Field(None, description="Quantity remaining")
    reorder_needed: bool = Field(False, description="Whether reorder is needed")
    notes: str = Field("", description="Additional notes")


class StockAlertsRequest(BaseModel):
    """Request model for stock alerts section."""
    items: List[StockItemRequest] = Field(default_factory=list)


class GeneralNotesRequest(BaseModel):
    """Request model for general notes section."""
    text: str = Field("", description="Free-form general notes")


class CreateHandoverRequest(BaseModel):
    """Request to create a handover note."""
    author_id: str = Field(..., description="ID of outgoing staff member")
    author_name: str = Field(..., description="Name of outgoing staff member")
    venue_id: str = Field(..., description="ID of venue")
    priority: str = Field("normal", description="Priority: normal, important, urgent")
    prep_status: Optional[PrepStatusRequest] = None
    vip_info: Optional[VIPInfoRequest] = None
    maintenance_issues: Optional[MaintenanceIssuesRequest] = None
    stock_alerts: Optional[StockAlertsRequest] = None
    general_notes: Optional[GeneralNotesRequest] = None


class PrepItemResponse(BaseModel):
    """Response model for a prep item."""
    name: str
    status: str
    notes: str
    due_time: Optional[str]


class PrepStatusResponse(BaseModel):
    """Response model for prep status."""
    items: List[PrepItemResponse]


class VIPTableResponse(BaseModel):
    """Response model for VIP table info."""
    table_number: str
    guest_name: str
    party_size: int
    arrival_time: Optional[str]
    dietary_restrictions: str
    special_requests: str
    seating_preference: str


class VIPInfoResponse(BaseModel):
    """Response model for VIP info."""
    tables: List[VIPTableResponse]


class IssueResponse(BaseModel):
    """Response model for an issue."""
    category: str
    description: str
    severity: str
    location: str
    reported_time: Optional[str]
    action_required: str


class MaintenanceIssuesResponse(BaseModel):
    """Response model for maintenance issues."""
    issues: List[IssueResponse]


class StockItemResponse(BaseModel):
    """Response model for a stock item."""
    item_name: str
    current_level: str
    quantity_remaining: Optional[str]
    reorder_needed: bool
    notes: str


class StockAlertsResponse(BaseModel):
    """Response model for stock alerts."""
    items: List[StockItemResponse]


class GeneralNotesResponse(BaseModel):
    """Response model for general notes."""
    text: str


class HandoverNoteResponse(BaseModel):
    """Response model for a complete handover note."""
    id: str
    shift_id: str
    venue_id: str
    author_id: str
    author_name: str
    created_at: str
    priority: str
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None
    prep_status: Optional[PrepStatusResponse] = None
    vip_info: Optional[VIPInfoResponse] = None
    maintenance_issues: Optional[MaintenanceIssuesResponse] = None
    stock_alerts: Optional[StockAlertsResponse] = None
    general_notes: Optional[GeneralNotesResponse] = None


class AcknowledgeHandoverRequest(BaseModel):
    """Request to acknowledge a handover note."""
    employee_id: str = Field(..., description="ID of incoming staff member")


# ============================================================================
# Helper Functions
# ============================================================================


def _to_sections_dict(req: CreateHandoverRequest) -> Dict[str, Any]:
    """Convert CreateHandoverRequest sections to dictionary format.

    Args:
        req: CreateHandoverRequest object

    Returns:
        Dictionary with section data
    """
    sections = {}

    if req.prep_status:
        sections["prep_status"] = {
            "items": [
                {
                    "name": item.name,
                    "status": item.status,
                    "notes": item.notes,
                    "due_time": item.due_time,
                }
                for item in req.prep_status.items
            ]
        }

    if req.vip_info:
        sections["vip_info"] = {
            "tables": [
                {
                    "table_number": table.table_number,
                    "guest_name": table.guest_name,
                    "party_size": table.party_size,
                    "arrival_time": table.arrival_time,
                    "dietary_restrictions": table.dietary_restrictions,
                    "special_requests": table.special_requests,
                    "seating_preference": table.seating_preference,
                }
                for table in req.vip_info.tables
            ]
        }

    if req.maintenance_issues:
        sections["maintenance_issues"] = {
            "issues": [
                {
                    "category": issue.category,
                    "description": issue.description,
                    "severity": issue.severity,
                    "location": issue.location,
                    "reported_time": issue.reported_time,
                    "action_required": issue.action_required,
                }
                for issue in req.maintenance_issues.issues
            ]
        }

    if req.stock_alerts:
        sections["stock_alerts"] = {
            "items": [
                {
                    "item_name": item.item_name,
                    "current_level": item.current_level,
                    "quantity_remaining": item.quantity_remaining,
                    "reorder_needed": item.reorder_needed,
                    "notes": item.notes,
                }
                for item in req.stock_alerts.items
            ]
        }

    if req.general_notes:
        sections["general_notes"] = {"text": req.general_notes.text}

    return sections


def _to_response(note: HandoverNote) -> HandoverNoteResponse:
    """Convert HandoverNote to response model.

    Args:
        note: HandoverNote object

    Returns:
        HandoverNoteResponse
    """
    sections = note.sections or {}

    prep_status = None
    if "prep_status" in sections and sections["prep_status"]:
        prep_status = PrepStatusResponse(
            items=[PrepItemResponse(**item) for item in sections["prep_status"].get("items", [])]
        )

    vip_info = None
    if "vip_info" in sections and sections["vip_info"]:
        vip_info = VIPInfoResponse(
            tables=[VIPTableResponse(**table) for table in sections["vip_info"].get("tables", [])]
        )

    maintenance_issues = None
    if "maintenance_issues" in sections and sections["maintenance_issues"]:
        maintenance_issues = MaintenanceIssuesResponse(
            issues=[IssueResponse(**issue) for issue in sections["maintenance_issues"].get("issues", [])]
        )

    stock_alerts = None
    if "stock_alerts" in sections and sections["stock_alerts"]:
        stock_alerts = StockAlertsResponse(
            items=[StockItemResponse(**item) for item in sections["stock_alerts"].get("items", [])]
        )

    general_notes = None
    if "general_notes" in sections and sections["general_notes"]:
        general_notes = GeneralNotesResponse(**sections["general_notes"])

    return HandoverNoteResponse(
        id=note.id,
        shift_id=note.shift_id,
        venue_id=note.venue_id,
        author_id=note.author_id,
        author_name=note.author_name,
        created_at=note.created_at,
        priority=note.priority,
        acknowledged_by=note.acknowledged_by,
        acknowledged_at=note.acknowledged_at,
        prep_status=prep_status,
        vip_info=vip_info,
        maintenance_issues=maintenance_issues,
        stock_alerts=stock_alerts,
        general_notes=general_notes,
    )


# ============================================================================
# Routes
# ============================================================================


@router.post(
    "/shifts/{shift_id}/handover",
    response_model=HandoverNoteResponse,
    status_code=201,
    summary="Create handover note",
    description="Create a new shift handover note with sections for prep status, VIP info, maintenance issues, stock alerts, and general notes.",
)
async def create_handover_note(
    shift_id: str = Path(..., description="ID of the shift"),
    request: CreateHandoverRequest = Body(...),
) -> HandoverNoteResponse:
    """Create a handover note for a shift.

    The outgoing staff member documents the shift status and critical information
    for the incoming staff to review.

    Args:
        shift_id: ID of the shift
        request: Handover note creation request

    Returns:
        Created HandoverNoteResponse

    Raises:
        HTTPException: If shift not found or invalid priority
    """
    try:
        service = get_handover_service()

        sections = _to_sections_dict(request)

        note = service.create_note(
            shift_id=shift_id,
            author_id=request.author_id,
            author_name=request.author_name,
            venue_id=request.venue_id,
            sections=sections,
            priority=request.priority,
        )

        return _to_response(note)
    except ValueError as e:
        logger.error(f"Error creating handover note: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating handover note: {e}")
        raise HTTPException(status_code=500, detail="Failed to create handover note")


@router.get(
    "/shifts/{shift_id}/handover",
    response_model=Optional[HandoverNoteResponse],
    summary="Get handover note",
    description="Retrieve the handover note for a specific shift, if one exists.",
)
async def get_handover_note(
    shift_id: str = Path(..., description="ID of the shift"),
) -> Optional[HandoverNoteResponse]:
    """Get the handover note for a shift.

    Args:
        shift_id: ID of the shift

    Returns:
        HandoverNoteResponse or None if no note exists

    Raises:
        HTTPException: If error retrieving note
    """
    try:
        service = get_handover_service()
        note = service.get_note_by_shift(shift_id)

        if not note:
            return None

        return _to_response(note)
    except Exception as e:
        logger.error(f"Error retrieving handover note: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve handover note")


@router.post(
    "/handover/{note_id}/acknowledge",
    response_model=HandoverNoteResponse,
    summary="Acknowledge handover",
    description="Mark a handover note as acknowledged by the incoming staff member.",
)
async def acknowledge_handover(
    note_id: str = Path(..., description="ID of the handover note"),
    request: AcknowledgeHandoverRequest = Body(...),
) -> HandoverNoteResponse:
    """Acknowledge receipt of a handover note.

    The incoming staff member marks the handover as read, recording their ID
    and the time of acknowledgment.

    Args:
        note_id: ID of the handover note
        request: Acknowledgment request with employee ID

    Returns:
        Updated HandoverNoteResponse

    Raises:
        HTTPException: If note not found
    """
    try:
        service = get_handover_service()
        note = service.acknowledge_note(note_id, request.employee_id)
        return _to_response(note)
    except ValueError as e:
        logger.error(f"Error acknowledging handover: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error acknowledging handover: {e}")
        raise HTTPException(status_code=500, detail="Failed to acknowledge handover")


@router.get(
    "/employees/{employee_id}/incoming-handovers",
    response_model=List[HandoverNoteResponse],
    summary="Get incoming handovers",
    description="Get all unacknowledged handover notes for an employee's upcoming shifts (next 7 days).",
)
async def get_incoming_handovers(
    employee_id: str = Path(..., description="ID of the employee"),
    venue_id: str = Query(..., description="ID of the venue"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of handovers to return"),
) -> List[HandoverNoteResponse]:
    """Get handover notes for my upcoming shifts.

    Returns unacknowledged handover notes for shifts where the employee is scheduled
    in the next 7 days.

    Args:
        employee_id: ID of the employee
        venue_id: ID of the venue
        limit: Maximum number of handovers (default 10, max 50)

    Returns:
        List of HandoverNoteResponse objects

    Raises:
        HTTPException: If error retrieving handovers
    """
    try:
        service = get_handover_service()
        notes = service.get_incoming_handovers(employee_id, venue_id, limit=limit)
        return [_to_response(note) for note in notes]
    except Exception as e:
        logger.error(f"Error retrieving incoming handovers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve incoming handovers")


@router.get(
    "/venues/{venue_id}/handovers/{date}",
    response_model=List[HandoverNoteResponse],
    summary="Get venue handovers for date",
    description="Get all handover notes for a venue on a specific date (YYYY-MM-DD).",
)
async def get_venue_handovers_for_date(
    venue_id: str = Path(..., description="ID of the venue"),
    date: str = Path(..., description="Date in YYYY-MM-DD format"),
) -> List[HandoverNoteResponse]:
    """Get all handover notes for a venue on a specific date.

    Args:
        venue_id: ID of the venue
        date: ISO date string (YYYY-MM-DD)

    Returns:
        List of HandoverNoteResponse objects

    Raises:
        HTTPException: If invalid date format or error retrieving handovers
    """
    try:
        # Validate date format
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    try:
        service = get_handover_service()
        notes = service.get_venue_handovers(venue_id, date)
        return [_to_response(note) for note in notes]
    except Exception as e:
        logger.error(f"Error retrieving venue handovers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve venue handovers")


@router.get(
    "/venues/{venue_id}/handovers/unacknowledged",
    response_model=List[HandoverNoteResponse],
    summary="Get unacknowledged handovers",
    description="Get all unacknowledged handover notes for a venue.",
)
async def get_unacknowledged_handovers(
    venue_id: str = Path(..., description="ID of the venue"),
) -> List[HandoverNoteResponse]:
    """Get all unacknowledged handover notes for a venue.

    Returns outstanding handovers from the past 3 days and next 7 days that haven't
    been acknowledged by incoming staff.

    Args:
        venue_id: ID of the venue

    Returns:
        List of HandoverNoteResponse objects

    Raises:
        HTTPException: If error retrieving handovers
    """
    try:
        service = get_handover_service()
        notes = service.get_unacknowledged(venue_id)
        return [_to_response(note) for note in notes]
    except Exception as e:
        logger.error(f"Error retrieving unacknowledged handovers: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve unacknowledged handovers")
