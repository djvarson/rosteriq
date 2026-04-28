"""
Cross-Venue Roster Synchronisation API endpoints for RosterIQ.

Provides REST endpoints for managing scheduling across multiple venues:
- GET /api/v1/employees/{id}/cross-venue-shifts — all shifts across venues
- GET /api/v1/employees/{id}/cross-venue-conflicts — overlapping shifts
- GET /api/v1/multi-venue/conflicts — scan all conflicts in venues
- GET /api/v1/multi-venue/shared-employees — employees in multiple venues
- POST /api/v1/multi-venue/check-schedule — pre-check before scheduling
- GET /api/v1/employees/{id}/cross-venue-hours — weekly hours across venues
- GET /api/v1/employees/{id}/cross-venue-availability/{date} — free slots

Used by managers to prevent double-booking and manage multi-venue staff.
"""

import logging
from typing import List, Optional
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.models import Employee
from rosteriq.services.cross_venue_sync import (
    CrossVenueSync,
    CrossVenueConflict,
    SharedEmployee,
    CrossVenueHours,
    TimeSlot,
    ScheduleCheckResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["cross-venue"],
)

# Instantiate sync service
_sync_service = CrossVenueSync()


# ============================================================================
# Response Models
# ============================================================================


class CrossVenueShiftResponse(BaseModel):
    """Response model for a cross-venue shift."""

    shift_id: str
    venue_id: str
    venue_name: str
    date: str
    start_time: str
    end_time: str
    duration_hours: float
    status: str

    class Config:
        from_attributes = True


class CrossVenueScheduleResponse(BaseModel):
    """Response model for cross-venue schedule."""

    employee_id: str
    employee_name: str
    date_range: dict
    total_hours: float
    venues_count: int
    shifts_by_venue: dict

    class Config:
        from_attributes = True


class CrossVenueConflictResponse(BaseModel):
    """Response model for a cross-venue conflict."""

    employee_id: str
    employee_name: str
    shift_a: dict
    shift_b: dict
    overlap_minutes: int
    severity: str

    class Config:
        from_attributes = True


class SharedEmployeeResponse(BaseModel):
    """Response model for a shared employee."""

    employee_id: str
    name: str
    venues: List[str]
    venue_names: List[str]
    total_weekly_hours: float
    conflict_count: int

    class Config:
        from_attributes = True


class CrossVenueHoursResponse(BaseModel):
    """Response model for cross-venue hours."""

    employee_id: str
    total_hours: float
    per_venue: dict
    over_limit: bool
    max_hours: float
    compliance_warning: Optional[str] = None

    class Config:
        from_attributes = True


class TimeSlotResponse(BaseModel):
    """Response model for an available time slot."""

    date: str
    start_time: str
    end_time: str
    duration_hours: float

    class Config:
        from_attributes = True


class ScheduleCheckResultResponse(BaseModel):
    """Response model for schedule check result."""

    can_schedule: bool
    conflicts: List[CrossVenueConflictResponse]
    total_hours_after: float
    warnings: List[str]
    errors: List[str]

    class Config:
        from_attributes = True


class ProposedShiftRequest(BaseModel):
    """Request model for proposing a shift."""

    venue_id: str
    date: str
    start_time: str
    end_time: str
    break_minutes: int = 0


# ============================================================================
# Request Handlers - Individual Employee
# ============================================================================


@router.get(
    "/employees/{employee_id}/cross-venue-shifts",
    response_model=CrossVenueScheduleResponse,
    summary="Get cross-venue shifts for employee",
    description="Aggregate all shifts across all venues for one employee",
)
def get_employee_cross_venue_shifts(
    employee_id: str = Path(..., description="Employee ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """
    Get all shifts for an employee across all venues in a date range.

    Returns shifts grouped by venue with total hours calculation.
    """
    try:
        schedule = _sync_service.get_cross_venue_shifts(
            employee_id, start_date, end_date
        )
        return schedule.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting cross-venue shifts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/employees/{employee_id}/cross-venue-conflicts",
    response_model=List[CrossVenueConflictResponse],
    summary="Get conflicts for employee",
    description="Find overlapping shifts at different venues for one employee",
)
def get_employee_conflicts(
    employee_id: str = Path(..., description="Employee ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """
    Detect overlapping shifts at different venues for a specific employee.

    Returns a list of conflicts with overlap duration and severity.
    """
    try:
        conflicts = _sync_service.detect_conflicts(
            employee_id, start_date, end_date
        )
        return [c.to_dict() for c in conflicts]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error detecting conflicts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/employees/{employee_id}/cross-venue-hours",
    response_model=CrossVenueHoursResponse,
    summary="Get weekly hours across venues",
    description="Calculate total hours worked across all venues for a week",
)
def get_employee_cross_venue_hours(
    employee_id: str = Path(..., description="Employee ID"),
    week_start: str = Query(..., description="Week start date (YYYY-MM-DD, must be Monday)"),
):
    """
    Get total hours worked across all venues for a specific week.

    Includes per-venue breakdown and compliance warnings if over limit.
    """
    try:
        hours = _sync_service.get_cross_venue_hours(employee_id, week_start)
        return hours.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting cross-venue hours: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/employees/{employee_id}/cross-venue-availability/{target_date}",
    response_model=List[TimeSlotResponse],
    summary="Get available time slots",
    description="Find free time slots considering all venue commitments",
)
def get_employee_availability_across_venues(
    employee_id: str = Path(..., description="Employee ID"),
    target_date: str = Path(..., description="Date to check (YYYY-MM-DD)"),
):
    """
    Find available time slots for an employee on a given day.

    Considers all existing shifts and required rest periods between venues.
    """
    try:
        slots = _sync_service.get_availability_across_venues(employee_id, target_date)
        return [
            {
                "date": s.date.isoformat(),
                "start_time": s.start_time.isoformat(),
                "end_time": s.end_time.isoformat(),
                "duration_hours": s.duration_hours(),
            }
            for s in slots
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting availability: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Request Handlers - Multi-Venue
# ============================================================================


@router.get(
    "/multi-venue/conflicts",
    response_model=List[CrossVenueConflictResponse],
    summary="Scan conflicts across venues",
    description="Detect all conflicts for shared employees across venues",
)
def get_multi_venue_conflicts(
    venue_ids: str = Query(..., description="Comma-separated venue IDs"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """
    Scan all employees working across multiple venues and find conflicts.

    Useful for compliance audits and multi-venue scheduling reviews.
    """
    try:
        venue_list = [v.strip() for v in venue_ids.split(",")]
        conflicts = _sync_service.detect_all_conflicts(
            venue_list, start_date, end_date
        )
        return [c.to_dict() for c in conflicts]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error detecting multi-venue conflicts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/multi-venue/shared-employees",
    response_model=List[SharedEmployeeResponse],
    summary="Get shared employees",
    description="List employees working at multiple venues",
)
def get_multi_venue_shared_employees(
    venue_ids: str = Query(..., description="Comma-separated venue IDs"),
):
    """
    Get all employees working across multiple specified venues.

    Includes total weekly hours and recent conflict counts.
    """
    try:
        venue_list = [v.strip() for v in venue_ids.split(",")]
        shared = _sync_service.get_shared_employees(venue_list)
        return [s.to_dict() for s in shared]
    except Exception as e:
        logger.error(f"Error getting shared employees: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Request Handlers - Pre-Check
# ============================================================================


@router.post(
    "/multi-venue/check-schedule",
    response_model=ScheduleCheckResultResponse,
    summary="Pre-check scheduling",
    description="Validate a proposed shift before adding to roster",
)
def check_schedule_before_booking(
    employee_id: str = Query(..., description="Employee ID"),
    proposed_shift: ProposedShiftRequest = Body(..., description="Proposed shift details"),
):
    """
    Check if a proposed shift would create conflicts or violations.

    Returns validation status with any conflicts, warnings, or errors.
    Useful for preventing invalid shifts before they're added to the roster.
    """
    try:
        result = _sync_service.check_before_scheduling(
            employee_id, proposed_shift.dict()
        )
        return result.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error checking schedule: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Health Check
# ============================================================================


@router.get(
    "/cross-venue/health",
    summary="Health check",
    description="Verify cross-venue sync service is operational",
)
def health_check():
    """
    Health check endpoint for the cross-venue sync service.

    Returns operational status.
    """
    return {
        "status": "ok",
        "service": "cross-venue-sync",
        "version": "1.0.0",
    }
