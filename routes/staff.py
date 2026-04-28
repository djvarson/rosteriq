"""
Staff self-service portal routes.

Provides endpoints for staff members to view shifts, manage availability,
track pay estimates, and request shift swaps.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.models import Employee, Shift, ShiftStatus
from rosteriq.award_rules import get_day_type, get_penalty_multiplier
from rosteriq.services.ws_events import get_dispatcher

router = APIRouter(prefix="/api/staff", tags=["staff"])


# ============================================================================
# Request/Response Models
# ============================================================================

class AvailabilityUpdateRequest(BaseModel):
    """Request to update availability preferences."""
    availability: Dict[str, Dict[str, bool]]
    preferred_hours_per_week: float


class ProfileUpdateRequest(BaseModel):
    """Request to update profile information."""
    name: Optional[str] = None
    phone: Optional[str] = None


class OfferShiftRequest(BaseModel):
    """Request to offer a shift for swap."""
    shift_id: str


class SwapRequestBody(BaseModel):
    """Request to request a shift swap."""
    my_shift_id: str
    swap_id: str
    message: Optional[str] = ""


# ============================================================================
# Helper functions
# ============================================================================


def _parse_date(date_str: str) -> date:
    """Parse ISO date string."""
    return datetime.fromisoformat(date_str).date()


def _get_week_range(week_start: date):
    """Get the full week range (Monday-Sunday)."""
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


@router.get("/my-shifts")
async def get_my_shifts(
    week_start: str = Query(..., description="Week start date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get shifts for the authenticated staff member for a given week.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view their shifts",
        )

    try:
        week_start_date = _parse_date(week_start)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )

    week_end = week_start_date + timedelta(days=6)

    # Get employee by user
    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    # Get rosters for the week
    rosters = db.list_rosters()
    shifts = []

    for roster in rosters:
        if roster.week_start <= week_end and roster.week_end >= week_start_date:
            for shift in roster.shifts:
                if shift.employee_id == emp.id and week_start_date <= shift.date <= week_end:
                    # Get venue info
                    venue = db.get_venue(roster.venue_id)
                    venue_name = venue.name if venue else "Unknown"

                    shifts.append({
                        "id": shift.id,
                        "date": shift.date.isoformat(),
                        "start_time": shift.start_time.isoformat(),
                        "end_time": shift.end_time.isoformat(),
                        "role": shift.role,
                        "venue": venue_name,
                        "status": shift.status.value,
                        "duration_hours": shift.net_hours,
                    })

    # Sort by date and time
    shifts.sort(key=lambda x: (x["date"], x["start_time"]))

    return {
        "week_start": week_start,
        "week_end": (week_start_date + timedelta(days=6)).isoformat(),
        "employee_id": emp.id,
        "shifts": shifts,
        "total_hours": sum(s["duration_hours"] for s in shifts),
        "shift_count": len(shifts),
    }


@router.get("/pay-estimate")
async def get_pay_estimate(
    week_start: str = Query(..., description="Week start date (YYYY-MM-DD)"),
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Calculate estimated pay for the week including penalty rates.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view their pay",
        )

    try:
        week_start_date = _parse_date(week_start)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )

    week_end = week_start_date + timedelta(days=6)

    # Get employee
    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    # Get shifts for the week
    rosters = db.list_rosters()
    shifts = []
    for roster in rosters:
        if roster.week_start <= week_end and roster.week_end >= week_start_date:
            for shift in roster.shifts:
                if shift.employee_id == emp.id and week_start_date <= shift.date <= week_end:
                    shifts.append(shift)

    # Calculate pay
    base_rate = float(emp.hourly_base_rate)
    total_hours = 0
    base_pay = Decimal("0")
    penalty_pay = Decimal("0")
    shift_breakdown = []

    for shift in shifts:
        hours = shift.net_hours
        total_hours += hours

        # Get day type for penalty calculation
        day_type = get_day_type(shift.date, state=emp.state)
        multiplier = get_penalty_multiplier(emp.employment_type, day_type)

        # Calculate pay for this shift
        shift_base = Decimal(str(hours)) * Decimal(str(base_rate))
        shift_penalty = shift_base * (multiplier - Decimal("1"))
        shift_total = shift_base + shift_penalty

        base_pay += shift_base
        penalty_pay += shift_penalty

        venue = db.get_venue(shift.role.split(':')[0]) if ':' in shift.role else None
        venue_name = venue.name if venue else "Unknown"

        shift_breakdown.append({
            "date": shift.date.isoformat(),
            "start_time": shift.start_time.isoformat(),
            "end_time": shift.end_time.isoformat(),
            "hours": hours,
            "role": shift.role,
            "base_pay": float(shift_base),
            "penalty_pay": float(shift_penalty),
            "total_pay": float(shift_total),
            "multiplier": float(multiplier),
        })

    total_pay = base_pay + penalty_pay

    return {
        "week_start": week_start,
        "employee_id": emp.id,
        "total_hours": total_hours,
        "base_rate": float(base_rate),
        "base_pay": float(base_pay),
        "penalty_pay": float(penalty_pay),
        "total_pay": float(total_pay),
        "shift_breakdown": shift_breakdown,
    }


@router.get("/availability")
async def get_availability(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get the staff member's availability preferences.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view their availability",
        )

    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    return {
        "employee_id": emp.id,
        "availability": emp.availability or {},
        "max_hours_per_week": emp.max_hours_per_week,
    }


@router.put("/availability")
async def update_availability(
    request: AvailabilityUpdateRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Update staff member's availability preferences and max hours.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can update their availability",
        )

    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    # Update availability
    emp.availability = request.availability
    emp.max_hours_per_week = request.preferred_hours_per_week
    emp.updated_at = datetime.utcnow()

    db.save_employee(emp)

    return {
        "message": "Availability updated",
        "employee_id": emp.id,
        "availability": emp.availability,
    }


@router.get("/profile")
async def get_profile(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get staff member's profile information.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view their profile",
        )

    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    return {
        "employee_id": emp.id,
        "name": emp.name,
        "email": emp.email,
        "phone": emp.phone,
        "employment_type": emp.employment_type.value,
        "award_level": emp.award_level.value,
        "state": emp.state.value,
        "skills": emp.skills,
        "certifications": [],  # Placeholder for cert data
    }


@router.put("/profile")
async def update_profile(
    request: ProfileUpdateRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Update staff member's profile information.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can update their profile",
        )

    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if not emp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee record not found",
        )

    # Update allowed fields
    if request.name:
        emp.name = request.name
    if request.phone:
        emp.phone = request.phone

    emp.updated_at = datetime.utcnow()
    db.save_employee(emp)

    return {
        "message": "Profile updated",
        "employee_id": emp.id,
    }


@router.get("/swap-board")
async def get_swap_board(
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Get available shifts for swapping on the swap board.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can view the swap board",
        )

    swaps = db.list_shift_swaps() if hasattr(db, 'list_shift_swaps') else []

    return {
        "swaps": swaps,
        "count": len(swaps),
    }


@router.post("/swap/offer")
async def offer_shift_for_swap(
    request: OfferShiftRequest,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Offer one of your shifts for swap with another staff member.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can offer shifts",
        )

    shift_id = request.shift_id
    if not shift_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="shift_id is required",
        )

    # Find the shift
    rosters = db.list_rosters()
    shift = None
    venue = None
    for roster in rosters:
        for s in roster.shifts:
            if s.id == shift_id:
                shift = s
                venue = db.get_venue(roster.venue_id)
                break

    if not shift:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Shift not found",
        )

    # Verify ownership
    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if shift.employee_id != emp.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only offer your own shifts",
        )

    # Create swap record
    swap = {
        "id": str(uuid.uuid4()),
        "shift_id": shift_id,
        "offered_by": emp.id,
        "date": shift.date.isoformat(),
        "start_time": shift.start_time.isoformat(),
        "end_time": shift.end_time.isoformat(),
        "role": shift.role,
        "venue": venue.name if venue else "Unknown",
        "venue_id": venue.id if venue else None,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }

    db.save_shift_swap(swap)

    return {
        "message": "Shift offered for swap",
        "swap_id": swap["id"],
    }


@router.post("/swap/request")
async def request_shift_swap(
    request: SwapRequestBody,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Request to swap your shift with another staff member's offered shift.
    """
    if current_user.role not in ["staff", "manager"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only staff can request swaps",
        )

    my_shift_id = request.my_shift_id
    swap_id = request.swap_id
    message = request.message

    if not my_shift_id or not swap_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="my_shift_id and swap_id are required",
        )

    # Verify both shifts exist and are owned correctly
    rosters = db.list_rosters()
    my_shift = None
    for roster in rosters:
        for s in roster.shifts:
            if s.id == my_shift_id:
                my_shift = s
                break

    if not my_shift:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Your shift not found",
        )

    # Get the swap being requested
    swap = db.get_shift_swap(swap_id) if hasattr(db, 'get_shift_swap') else None
    if not swap:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Swap not found",
        )

    # Get current employee
    employees = db.list_employees()
    emp = None
    for e in employees:
        if e.email == current_user.email:
            emp = e
            break

    if my_shift.employee_id != emp.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only request swaps with your own shifts",
        )

    # Create swap request record
    swap_request = {
        "id": str(uuid.uuid4()),
        "swap_id": swap_id,
        "requested_by": emp.id,
        "my_shift_id": my_shift_id,
        "offered_shift_id": swap["shift_id"],
        "message": message,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }

    db.save_shift_swap(swap_request)

    return {
        "message": "Swap request created",
        "request_id": swap_request["id"],
    }


@router.post("/swap/{swap_id}/accept")
async def accept_swap(
    swap_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Accept a swap offer (if you are the offered recipient).
    """
    swap = db.get_shift_swap(swap_id) if hasattr(db, 'get_shift_swap') else None
    if not swap:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Swap not found",
        )

    swap["status"] = "approved"
    db.save_shift_swap(swap)

    # Broadcast shift swap completion (non-fatal)
    try:
        dispatcher = get_dispatcher()
        from_employee = swap.get("from_employee_id") or swap.get("from_employee")
        to_employee = swap.get("to_employee_id") or swap.get("to_employee") or current_user.user_id
        await dispatcher.shift_swapped(
            swap_id=swap_id,
            from_employee=from_employee,
            to_employee=to_employee,
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to broadcast shift swap: {e}")

    return {
        "message": "Swap accepted",
        "swap_id": swap_id,
    }


@router.post("/swap/{swap_id}/reject")
async def reject_swap(
    swap_id: str,
    current_user: UserContext = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Reject a swap offer.
    """
    swap = db.get_shift_swap(swap_id) if hasattr(db, 'get_shift_swap') else None
    if not swap:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Swap not found",
        )

    swap["status"] = "rejected"
    db.save_shift_swap(swap)

    return {
        "message": "Swap rejected",
        "swap_id": swap_id,
    }
