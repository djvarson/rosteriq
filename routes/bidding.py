"""
Shift bidding marketplace routes.

Endpoints for posting open shifts, placing bids, and managing awards.
"""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional, List
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.models import UserRole
from rosteriq.services.shift_bidding import (
    ShiftBiddingService, OpenShift, Bid, OpenShiftStatus, BidStatus
)
from rosteriq.services.notifications import get_notification_service

router = APIRouter(prefix="/api/bidding", tags=["bidding"])


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================


class PostOpenShiftRequest(BaseModel):
    """Request to post an open shift."""
    date: date
    start_time: time
    end_time: time
    role_required: str
    skills_required: List[str] = []
    min_rate: Decimal = Field(gt=0, description="Minimum acceptable rate")
    max_rate: Optional[Decimal] = Field(None, description="Maximum rate cap")
    deadline: Optional[datetime] = None
    notes: Optional[str] = None


class OpenShiftResponse(BaseModel):
    """Response for open shift."""
    id: str
    venue_id: str
    date: date
    start_time: time
    end_time: time
    role_required: str
    skills_required: List[str]
    min_rate: Decimal
    max_rate: Optional[Decimal]
    posted_by: str
    posted_at: datetime
    deadline: datetime
    status: str
    notes: Optional[str]

    @staticmethod
    def from_model(shift: OpenShift) -> "OpenShiftResponse":
        return OpenShiftResponse(
            id=shift.id,
            venue_id=shift.venue_id,
            date=shift.date,
            start_time=shift.start_time,
            end_time=shift.end_time,
            role_required=shift.role_required,
            skills_required=shift.skills_required,
            min_rate=shift.min_rate,
            max_rate=shift.max_rate,
            posted_by=shift.posted_by,
            posted_at=shift.posted_at,
            deadline=shift.deadline,
            status=shift.status.value,
            notes=shift.notes,
        )


class PlaceBidRequest(BaseModel):
    """Request to place a bid."""
    offered_rate: Decimal = Field(gt=0, description="Rate offered by employee")
    message: Optional[str] = None


class BidResponse(BaseModel):
    """Response for a bid."""
    id: str
    open_shift_id: str
    employee_id: str
    offered_rate: Decimal
    message: Optional[str]
    seniority_years: float
    preference_score: float
    submitted_at: datetime
    status: str

    @staticmethod
    def from_model(bid: Bid) -> "BidResponse":
        return BidResponse(
            id=bid.id,
            open_shift_id=bid.open_shift_id,
            employee_id=bid.employee_id,
            offered_rate=bid.offered_rate,
            message=bid.message,
            seniority_years=bid.seniority_years,
            preference_score=bid.preference_score,
            submitted_at=bid.submitted_at,
            status=bid.status.value,
        )


class BidWithScoreResponse(BidResponse):
    """Bid response with calculated score."""
    score: float


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str


# ============================================================================
# DEPENDENCIES
# ============================================================================


def get_bidding_service(db=Depends(get_db)) -> ShiftBiddingService:
    """Get bidding service instance."""
    notifications = get_notification_service()
    return ShiftBiddingService(db, notifications)


def _load_shift_in_scope(
    bidding_service: ShiftBiddingService, shift_id: str
) -> OpenShift:
    """
    Load an open shift by id and enforce that the CURRENT request's user may
    act on the shift's venue.

    Owners pass; managers/staff must have the shift's venue in their venue_ids.
    A cross-venue request gets the SAME 404 as a missing shift so shift ids are
    not an oracle for other tenants' data.
    """
    shift = bidding_service.get_open_shift(shift_id) if shift_id else None
    if not shift:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Open shift not found",
        )
    try:
        enforce_venue_access(shift.venue_id)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Open shift not found",
            )
        raise
    return shift


# ============================================================================
# ENDPOINTS
# ============================================================================


@router.post(
    "/shifts",
    response_model=OpenShiftResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post an open shift",
)
async def post_open_shift(
    venue_id: str = Query(..., description="Venue ID"),
    request: PostOpenShiftRequest = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Post an open shift for employees to bid on.

    Only managers can post shifts, and only for a venue they can access.
    """
    # Tenant scope: manager/staff limited to their venue_ids (owner passes).
    enforce_venue_access(venue_id)

    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can post shifts",
        )

    try:
        shift = bidding_service.post_open_shift(
            venue_id=venue_id,
            shift_details={
                "date": request.date,
                "start_time": request.start_time,
                "end_time": request.end_time,
                "role_required": request.role_required,
                "skills_required": request.skills_required,
                "min_rate": request.min_rate,
                "max_rate": request.max_rate,
                "deadline": request.deadline,
                "notes": request.notes,
            },
            posted_by=user.user_id,
        )
        return OpenShiftResponse.from_model(shift)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get(
    "/shifts",
    response_model=List[OpenShiftResponse],
    summary="List open shifts",
)
async def list_open_shifts(
    venue_id: str = Query(..., description="Venue ID"),
    role: Optional[str] = Query(None, description="Filter by role"),
    start_date: Optional[date] = Query(None, description="Start date filter"),
    end_date: Optional[date] = Query(None, description="End date filter"),
    status_filter: Optional[str] = Query(None, description="Filter by status (open/filled/expired/cancelled)"),
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """List open shifts for a venue the caller can access."""
    # Tenant scope: manager/staff limited to their venue_ids (owner passes).
    enforce_venue_access(venue_id)

    try:
        shift_status = None
        if status_filter:
            shift_status = OpenShiftStatus(status_filter)

        date_range = None
        if start_date and end_date:
            date_range = (start_date, end_date)

        shifts = bidding_service.list_open_shifts(
            venue_id=venue_id,
            role=role,
            date_range=date_range,
            status=shift_status,
        )
        return [OpenShiftResponse.from_model(s) for s in shifts]
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get(
    "/shifts/{shift_id}",
    response_model=dict,
    summary="Get open shift details with bids",
)
async def get_open_shift_with_bids(
    shift_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """Get details of an open shift and all bids (manager view only)."""
    # 404 for missing OR cross-venue (ids are not an oracle).
    shift = _load_shift_in_scope(bidding_service, shift_id)

    # Only managers can see all bids
    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can view bids",
        )

    bids = bidding_service.list_bids(shift_id)

    return {
        "shift": OpenShiftResponse.from_model(shift),
        "bids": [BidResponse.from_model(b) for b in bids],
        "bid_count": len(bids),
    }


@router.post(
    "/bids",
    response_model=BidResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Place a bid on a shift",
)
async def place_bid(
    open_shift_id: str = Query(..., description="Open shift ID"),
    request: PlaceBidRequest = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Place a bid on an open shift.

    Staff can only bid with their own ID, and only on shifts in a venue they
    belong to.
    """
    if user.role != UserRole.staff:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can place bids",
        )

    # 404 for missing OR cross-venue shift (ids are not an oracle).
    _load_shift_in_scope(bidding_service, open_shift_id)

    try:
        bid = bidding_service.place_bid(
            open_shift_id=open_shift_id,
            employee_id=user.user_id,
            offered_rate=request.offered_rate,
            message=request.message,
        )
        return BidResponse.from_model(bid)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete(
    "/bids/{bid_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Withdraw a bid",
)
async def withdraw_bid(
    bid_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Withdraw a bid.

    Staff can only withdraw their own bids.
    """
    try:
        bidding_service.withdraw_bid(bid_id, user.user_id)
        return None
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(
    "/shifts/{shift_id}/assign",
    response_model=BidResponse,
    summary="Auto-assign shift to best bid",
)
async def auto_assign_shift(
    shift_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Automatically assign shift to best-scoring bid.

    Only managers can auto-assign, and only within a venue they can access.
    """
    # 404 for missing OR cross-venue (ids are not an oracle).
    _load_shift_in_scope(bidding_service, shift_id)

    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can auto-assign shifts",
        )

    try:
        winning_bid = bidding_service.auto_assign(shift_id)
        if not winning_bid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No eligible bids to assign",
            )
        return BidResponse.from_model(winning_bid)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post(
    "/shifts/{shift_id}/award/{bid_id}",
    response_model=dict,
    summary="Manually award shift to a bid",
)
async def award_shift(
    shift_id: str = None,
    bid_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Manager manually awards shift to a bidder.

    Creates actual shift in roster and rejects other bids.
    Only managers can award, and only within a venue they can access.
    """
    # 404 for missing OR cross-venue (ids are not an oracle).
    _load_shift_in_scope(bidding_service, shift_id)

    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can award shifts",
        )

    try:
        shift = bidding_service.award_shift(shift_id, bid_id, user.user_id)
        return {
            "id": shift.id,
            "employee_id": shift.employee_id,
            "date": shift.date,
            "start_time": shift.start_time,
            "end_time": shift.end_time,
            "role": shift.role,
            "status": shift.status.value,
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete(
    "/shifts/{shift_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel an open shift",
)
async def cancel_open_shift(
    shift_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Cancel an open shift posting.

    Only managers who posted the shift can cancel it, and only within a venue
    they can access.
    """
    # 404 for missing OR cross-venue (ids are not an oracle).
    _load_shift_in_scope(bidding_service, shift_id)

    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can cancel shifts",
        )

    try:
        bidding_service.cancel_open_shift(shift_id, user.user_id)
        return None
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get(
    "/my-bids",
    response_model=List[dict],
    summary="Get employee's active bids",
)
async def get_my_bids(
    user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get all active bids for the current employee.

    Staff only. Returns bids with shift details.
    """
    if user.role != UserRole.staff:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view their bids",
        )

    # TODO: Query bids by employee_id when database method exists
    # For now, return empty list
    return []


@router.get(
    "/shifts/{shift_id}/eligible-employees",
    response_model=List[dict],
    summary="Get eligible employees for a shift",
)
async def get_eligible_employees(
    shift_id: str = None,
    user: UserContext = Depends(get_current_user),
    bidding_service: ShiftBiddingService = Depends(get_bidding_service),
):
    """
    Get list of employees eligible to bid on a shift.

    Only managers can view this, and only within a venue they can access.
    """
    # 404 for missing OR cross-venue (ids are not an oracle).
    _load_shift_in_scope(bidding_service, shift_id)

    if user.role != UserRole.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only managers can view eligible employees",
        )

    employees = bidding_service.get_eligible_employees(shift_id)
    return [
        {
            "id": emp.id,
            "name": emp.name,
            "role": getattr(emp, "role", None),
            "skills": emp.skills,
        }
        for emp in employees
    ]
