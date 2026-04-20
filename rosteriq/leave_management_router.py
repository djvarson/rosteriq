"""FastAPI router for leave management endpoints.

Provides REST API for leave request management:
- POST /api/v1/leave/request - Submit a leave request
- GET /api/v1/leave/{venue_id} - List leave requests
- POST /api/v1/leave/{request_id}/approve - Approve a leave request
- POST /api/v1/leave/{request_id}/reject - Reject a leave request
- POST /api/v1/leave/{request_id}/cancel - Cancel a leave request
- GET /api/v1/leave/{venue_id}/calendar - Leave calendar view
- GET /api/v1/leave/{venue_id}/balances/{employee_id} - Leave balances
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.leave_management import (
    get_leave_store,
    LeaveType,
    LeaveStatus,
    LeaveConflict,
)

logger = logging.getLogger("rosteriq.leave_management_router")

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


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class SubmitLeaveRequest(BaseModel):
    """Request to submit leave."""
    employee_id: str = Field(..., description="Employee ID")
    employee_name: str = Field(..., description="Employee name")
    leave_type: str = Field(..., description="Leave type: annual, personal_carer, compassionate, etc")
    start_date: str = Field(..., description="ISO date YYYY-MM-DD")
    end_date: str = Field(..., description="ISO date YYYY-MM-DD")
    hours_requested: float = Field(..., description="Hours requested")
    reason: str = Field(..., description="Reason for leave")


class ApproveLeaveRequest(BaseModel):
    """Request to approve leave."""
    decided_by: Optional[str] = Field(None, description="Manager/approver ID if not from auth")


class RejectLeaveRequest(BaseModel):
    """Request to reject leave."""
    reason: str = Field(..., description="Reason for rejection")
    decided_by: Optional[str] = Field(None, description="Manager/approver ID if not from auth")


class CancelLeaveRequest(BaseModel):
    """Request to cancel leave."""
    pass


class LeaveRequestResponse(BaseModel):
    """Response containing a leave request."""
    request_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    leave_type: str
    start_date: str
    end_date: str
    hours_requested: float
    reason: str
    status: str
    decided_by: Optional[str] = None
    decided_at: Optional[str] = None
    created_at: str
    notes: Optional[str] = None

    @classmethod
    def from_request(cls, req: Any) -> LeaveRequestResponse:
        """Convert a LeaveRequest dataclass to response."""
        return cls(**req.to_dict())


class LeaveBalanceResponse(BaseModel):
    """Response containing leave balance."""
    employee_id: str
    employee_name: str
    venue_id: str
    leave_type: str
    accrued_hours: float
    used_hours: float
    pending_hours: float
    available_hours: float

    @classmethod
    def from_balance(cls, bal: Any) -> LeaveBalanceResponse:
        """Convert a LeaveBalance dataclass to response."""
        return cls(**bal.to_dict())


class LeaveConflictResponse(BaseModel):
    """Response indicating conflicts."""
    request_id: str
    conflicting_shifts: List[Dict[str, Any]]
    minimum_staff_warning: bool
    message: str


class ListLeaveResponse(BaseModel):
    """Response containing a list of leave requests."""
    count: int = Field(..., description="Number of requests")
    requests: List[LeaveRequestResponse]


class ListBalancesResponse(BaseModel):
    """Response containing leave balances."""
    employee_id: str
    venue_id: str
    count: int = Field(..., description="Number of balance records")
    balances: List[LeaveBalanceResponse]


class CalendarResponse(BaseModel):
    """Response containing leave calendar."""
    venue_id: str
    date_from: str
    date_to: str
    count: int = Field(..., description="Number of calendar entries")
    entries: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v1/leave", tags=["leave"])


@router.post("/request", response_model=LeaveRequestResponse, status_code=201)
async def submit_leave(
    venue_id: str,
    req: SubmitLeaveRequest,
    request: Request,
) -> LeaveRequestResponse:
    """
    Submit a new leave request.

    Staff (L1+) can submit leave requests. Requires sufficient balance
    of the requested leave type.

    Args:
        venue_id: Venue ID
        req: Leave request details
        request: HTTP request for auth context

    Returns:
        Created leave request in PENDING status

    Raises:
        400: Invalid input or insufficient balance
        403: Not authorized
    """
    await _gate(request, "L1_STAFF")

    try:
        leave_type = LeaveType(req.leave_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid leave type: {req.leave_type}")

    store = get_leave_store()
    try:
        leave_req = store.submit_leave_request(
            employee_id=req.employee_id,
            employee_name=req.employee_name,
            venue_id=venue_id,
            leave_type=leave_type,
            start_date=req.start_date,
            end_date=req.end_date,
            hours_requested=req.hours_requested,
            reason=req.reason,
        )
        return LeaveRequestResponse.from_request(leave_req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{venue_id}", response_model=ListLeaveResponse)
async def list_leave_requests(
    venue_id: str,
    employee_id: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    request: Optional[Request] = None,
) -> ListLeaveResponse:
    """
    List leave requests for a venue.

    Staff (L1+) can view requests. Optionally filter by employee, status, or date range.

    Args:
        venue_id: Venue ID to filter by
        employee_id: Filter by employee ID (optional)
        status: Filter by status (optional)
        date_from: Filter by start date >= (optional, ISO string)
        date_to: Filter by end date <= (optional, ISO string)
        request: HTTP request for auth context

    Returns:
        List of matching leave requests

    Raises:
        403: Not authorized
    """
    if request:
        await _gate(request, "L1_STAFF")

    store = get_leave_store()
    status_enum = None
    if status:
        try:
            status_enum = LeaveStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    requests = store._list_requests(
        venue_id=venue_id,
        employee_id=employee_id,
        status=status_enum,
        date_from=date_from,
        date_to=date_to,
    )

    return ListLeaveResponse(
        count=len(requests),
        requests=[LeaveRequestResponse.from_request(r) for r in requests],
    )


@router.post("/{request_id}/approve", response_model=LeaveRequestResponse)
async def approve_leave(
    request_id: str,
    req: ApproveLeaveRequest,
    request: Request,
) -> LeaveRequestResponse:
    """
    Approve a leave request.

    Managers (L2+) can approve pending leave requests. Hours are deducted
    from the employee's leave balance.

    Args:
        request_id: Leave request ID
        req: Approval details
        request: HTTP request for auth context

    Returns:
        Approved leave request

    Raises:
        400: Request not in PENDING status or not found
        403: Not authorized
    """
    await _gate(request, "L2_ROSTER_MAKER")

    decided_by = req.decided_by or getattr(request.state, "user_id", "unknown")

    store = get_leave_store()
    try:
        leave_req = store.approve_leave(request_id, decided_by)
        return LeaveRequestResponse.from_request(leave_req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{request_id}/reject", response_model=LeaveRequestResponse)
async def reject_leave(
    request_id: str,
    req: RejectLeaveRequest,
    request: Request,
) -> LeaveRequestResponse:
    """
    Reject a leave request.

    Managers (L2+) can reject pending leave requests with a reason.
    Pending hours are released back to the employee.

    Args:
        request_id: Leave request ID
        req: Rejection details
        request: HTTP request for auth context

    Returns:
        Rejected leave request

    Raises:
        400: Request not in PENDING status or not found
        403: Not authorized
    """
    await _gate(request, "L2_ROSTER_MAKER")

    decided_by = req.decided_by or getattr(request.state, "user_id", "unknown")

    store = get_leave_store()
    try:
        leave_req = store.reject_leave(request_id, decided_by, req.reason)
        return LeaveRequestResponse.from_request(leave_req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{request_id}/cancel", response_model=LeaveRequestResponse)
async def cancel_leave(
    request_id: str,
    req: CancelLeaveRequest,
    request: Request,
) -> LeaveRequestResponse:
    """
    Cancel a leave request.

    Staff (L1+) can cancel their own leave requests. If approved, hours
    are restored to the balance.

    Args:
        request_id: Leave request ID
        req: Cancellation details
        request: HTTP request for auth context

    Returns:
        Cancelled leave request

    Raises:
        400: Request already cancelled or not found
        403: Not authorized
    """
    await _gate(request, "L1_STAFF")

    store = get_leave_store()
    try:
        leave_req = store.cancel_leave(request_id)
        return LeaveRequestResponse.from_request(leave_req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{venue_id}/calendar", response_model=CalendarResponse)
async def get_leave_calendar(
    venue_id: str,
    date_from: str,
    date_to: str,
    request: Request,
) -> CalendarResponse:
    """
    Get a calendar view of who's on leave when.

    Staff (L1+) can view leave calendars. Shows all approved leave
    for the venue within the date range.

    Args:
        venue_id: Venue ID
        date_from: Start date (ISO string, inclusive)
        date_to: End date (ISO string, inclusive)
        request: HTTP request for auth context

    Returns:
        Calendar entries with employee leave information

    Raises:
        403: Not authorized
    """
    await _gate(request, "L1_STAFF")

    store = get_leave_store()
    entries = store.get_leave_calendar(venue_id, date_from, date_to)

    return CalendarResponse(
        venue_id=venue_id,
        date_from=date_from,
        date_to=date_to,
        count=len(entries),
        entries=entries,
    )


@router.get("/{venue_id}/balances/{employee_id}", response_model=ListBalancesResponse)
async def get_leave_balances(
    venue_id: str,
    employee_id: str,
    request: Request,
) -> ListBalancesResponse:
    """
    Get all leave balances for an employee.

    Staff (L1+) can view balances. Returns accrued, used, pending, and
    available hours for each leave type.

    Args:
        venue_id: Venue ID
        employee_id: Employee ID
        request: HTTP request for auth context

    Returns:
        Leave balances for all leave types

    Raises:
        403: Not authorized
    """
    await _gate(request, "L1_STAFF")

    store = get_leave_store()
    balances = store.get_balances(employee_id, venue_id)

    # If no balances exist, create placeholder response
    if not balances:
        return ListBalancesResponse(
            employee_id=employee_id,
            venue_id=venue_id,
            count=0,
            balances=[],
        )

    employee_name = balances[0].employee_name if balances else "Unknown"

    return ListBalancesResponse(
        employee_id=employee_id,
        venue_id=venue_id,
        count=len(balances),
        balances=[LeaveBalanceResponse.from_balance(b) for b in balances],
    )
