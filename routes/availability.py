"""
Availability conflict resolver API endpoints for RosterIQ.

Provides REST endpoints for resolving employee availability conflicts:
- GET /api/v1/shifts/{id}/alternatives — ranked alternatives for a shift
- POST /api/v1/shifts/{id}/time-adjustment — suggest time adjustments
- POST /api/v1/shifts/swap-suggestions — find beneficial swaps
- POST /api/v1/conflicts/bulk-resolve — resolve multiple conflicts
- GET /api/v1/venues/{id}/coverage/{date} — availability coverage for a day

Used by managers to quickly resolve conflicts or find optimal reassignments.
"""

import logging
from typing import Optional, List
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.models import Shift, Employee, VenueConfig
from rosteriq.services.availability_resolver import (
    AvailabilityResolver,
    AlternativeOption,
    TimeAdjustment,
    SwapSuggestion,
    ResolutionPlan,
    CoverageReport,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["availability"],
)

# Instantiate resolver
_resolver = AvailabilityResolver()


# ============================================================================
# Utility functions
# ============================================================================


def _get_shift_from_db(shift_id: str) -> Optional[Shift]:
    """Helper to find a shift by ID across all rosters."""
    db = get_db()
    all_rosters = db.list_rosters()
    for roster in all_rosters:
        shift = next((s for s in roster.shifts if s.id == shift_id), None)
        if shift:
            return shift
    return None


# ============================================================================
# Response models
# ============================================================================


class AlternativeOptionResponse(BaseModel):
    """Response model for an alternative employee option."""

    employee_id: str
    employee_name: str
    overall_score: float
    cost_impact: float
    skill_match_score: float
    fairness_score: float
    preference_score: float
    availability_fit_score: float
    hours_this_week: float
    hours_after: float
    consecutive_days_after: int
    warnings: List[str]

    class Config:
        from_attributes = True


class TimeAdjustmentResponse(BaseModel):
    """Response model for a time adjustment suggestion."""

    original_start: str
    original_end: str
    adjusted_start: str
    adjusted_end: str
    adjusted_net_hours: float
    availability_conflict: str
    score: float

    class Config:
        from_attributes = True


class SwapSuggestionResponse(BaseModel):
    """Response model for a shift swap suggestion."""

    shift_a_id: str
    shift_a_employee_id: str
    shift_b_id: str
    shift_b_employee_id: str
    benefit_a: str
    benefit_b: str
    total_score: float
    cost_delta: float

    class Config:
        from_attributes = True


class FindAlternativesResponse(BaseModel):
    """Response for finding alternatives."""

    shift_id: str
    employee_id: str
    total_alternatives: int
    alternatives: List[AlternativeOptionResponse]


class TimeAdjustmentSuggestionsResponse(BaseModel):
    """Response for time adjustment suggestions."""

    shift_id: str
    employee_id: str
    total_suggestions: int
    suggestions: List[TimeAdjustmentResponse]


class ShiftSwapSuggestionsResponse(BaseModel):
    """Response for shift swap suggestions."""

    shift_id: str
    employee_id: str
    total_suggestions: int
    suggestions: List[SwapSuggestionResponse]


class ResolutionPlanResponse(BaseModel):
    """Response model for a single resolution plan."""

    shift_id: str
    original_employee_id: str
    recommended_employee_id: Optional[str] = None
    reason: str
    alternatives: List[AlternativeOptionResponse]


class BulkResolveResponse(BaseModel):
    """Response for bulk conflict resolution."""

    venue_id: str
    total_conflicts: int
    plans: List[ResolutionPlanResponse]


class CoverageHourlyBreakdown(BaseModel):
    """Hourly breakdown of coverage."""

    hour: int
    staff_count: int
    staff_ids: List[str]
    understaffed: bool

    class Config:
        from_attributes = True


class AvailableStaffMember(BaseModel):
    """Available staff member with time windows."""

    employee_id: str
    employee_name: str
    availability_windows: List[dict]  # [{start_hour, end_hour}, ...]

    class Config:
        from_attributes = True


class CoverageReportResponse(BaseModel):
    """Response for availability coverage analysis."""

    venue_id: str
    date: str
    min_required: int
    total_scheduled: int
    understaffed_hours: List[int]
    gaps: List[dict]  # [{start_hour, end_hour}, ...]
    hourly_breakdown: List[CoverageHourlyBreakdown]
    available_staff: List[AvailableStaffMember]

    class Config:
        from_attributes = True


# ============================================================================
# Request models
# ============================================================================


class BulkConflictInput(BaseModel):
    """Input for bulk conflict resolution."""

    conflicts: List[dict]  # [{shift_id, employee_id, reason}, ...]


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/shifts/{shift_id}/alternatives")
async def find_alternatives(
    shift_id: str = Path(..., description="Shift ID"),
    venue_id: str = Query(..., description="Venue ID"),
) -> FindAlternativesResponse:
    """
    Find and rank alternative employees for a conflicting shift.

    Returns alternatives sorted by overall suitability score (0-100).

    Scoring factors:
    - Cost impact (30%): Preference for cheaper options
    - Fairness (25%): Preference for underutilized staff
    - Preference (20%): Employee's stated/learned preferences
    - Skill match (15%): Required skills
    - Availability fit (10%): How well their availability aligns

    Args:
        shift_id: ID of the conflicting shift
        venue_id: ID of the venue

    Returns:
        Ranked list of alternative employees with detailed scoring
    """
    logger.info(f"GET /shifts/{shift_id}/alternatives?venue_id={venue_id}")

    db = get_db()
    shift = _get_shift_from_db(shift_id)
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")

    employee = db.get_employee(shift.employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee {shift.employee_id} not found")

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    alternatives = _resolver.find_alternatives(shift, employee, venue_id)

    return FindAlternativesResponse(
        shift_id=shift_id,
        employee_id=shift.employee_id,
        total_alternatives=len(alternatives),
        alternatives=[
            AlternativeOptionResponse(
                employee_id=alt.employee.id,
                employee_name=alt.employee.name,
                overall_score=alt.overall_score,
                cost_impact=float(alt.cost_impact),
                skill_match_score=alt.skill_match_score,
                fairness_score=alt.fairness_score,
                preference_score=alt.preference_score,
                availability_fit_score=alt.availability_fit_score,
                hours_this_week=alt.hours_this_week,
                hours_after=alt.hours_after,
                consecutive_days_after=alt.consecutive_days_after,
                warnings=alt.warnings,
            )
            for alt in alternatives
        ],
    )


@router.post("/shifts/{shift_id}/time-adjustment")
async def suggest_time_adjustment(
    shift_id: str = Path(..., description="Shift ID"),
    employee_id: str = Query(..., description="Employee ID"),
) -> TimeAdjustmentSuggestionsResponse:
    """
    Suggest time adjustments for an employee with partial availability.

    If an employee is not fully available for a shift but has some overlapping
    availability, suggests adjusted start/end times that would fit their
    availability while maintaining minimum engagement requirements.

    Args:
        shift_id: ID of the shift
        employee_id: ID of the employee

    Returns:
        List of time adjustment suggestions sorted by score (0-100)
    """
    logger.info(f"POST /shifts/{shift_id}/time-adjustment?employee_id={employee_id}")

    db = get_db()
    shift = _get_shift_from_db(shift_id)
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")

    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")

    adjustments = _resolver.suggest_time_adjustment(shift, employee)

    return TimeAdjustmentSuggestionsResponse(
        shift_id=shift_id,
        employee_id=employee_id,
        total_suggestions=len(adjustments),
        suggestions=[
            TimeAdjustmentResponse(
                original_start=f"{adj.original_start.hour:02d}:{adj.original_start.minute:02d}",
                original_end=f"{adj.original_end.hour:02d}:{adj.original_end.minute:02d}",
                adjusted_start=f"{adj.adjusted_start.hour:02d}:{adj.adjusted_start.minute:02d}",
                adjusted_end=f"{adj.adjusted_end.hour:02d}:{adj.adjusted_end.minute:02d}",
                adjusted_net_hours=adj.adjusted_net_hours,
                availability_conflict=adj.availability_conflict,
                score=adj.score,
            )
            for adj in adjustments
        ],
    )


@router.post("/shifts/swap-suggestions")
async def suggest_shift_swaps(
    shift_id: str = Query(..., description="Shift ID"),
    venue_id: str = Query(..., description="Venue ID"),
) -> ShiftSwapSuggestionsResponse:
    """
    Find beneficial shift swaps where both employees win.

    Looks for shifts assigned to other employees where:
    - Current employee can work the other shift (has skills, available)
    - Other employee can work current shift (has skills, available)
    - Both parties benefit (prefer their new shift, or reduce fairness issues)

    Args:
        shift_id: ID of the problematic shift
        venue_id: ID of the venue

    Returns:
        List of swap suggestions sorted by total benefit score (0-100)
    """
    logger.info(f"POST /shifts/swap-suggestions?shift_id={shift_id}&venue_id={venue_id}")

    db = get_db()
    shift = _get_shift_from_db(shift_id)
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")

    employee = db.get_employee(shift.employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee {shift.employee_id} not found")

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    suggestions = _resolver.suggest_shift_swap(shift, employee, venue_id)

    return ShiftSwapSuggestionsResponse(
        shift_id=shift_id,
        employee_id=shift.employee_id,
        total_suggestions=len(suggestions),
        suggestions=[
            SwapSuggestionResponse(
                shift_a_id=sugg.shift_a_id,
                shift_a_employee_id=sugg.shift_a_employee_id,
                shift_b_id=sugg.shift_b_id,
                shift_b_employee_id=sugg.shift_b_employee_id,
                benefit_a=sugg.benefit_a,
                benefit_b=sugg.benefit_b,
                total_score=sugg.total_score,
                cost_delta=float(sugg.cost_delta),
            )
            for sugg in suggestions
        ],
    )


@router.post("/conflicts/bulk-resolve")
async def bulk_resolve_conflicts(
    venue_id: str = Query(..., description="Venue ID"),
    request: BulkConflictInput = Body(...),
) -> BulkResolveResponse:
    """
    Resolve multiple conflicts optimally without reassigning same person twice.

    Provides resolution plans for a batch of conflicts, ensuring no employee
    is reassigned to multiple shifts in a single batch.

    Args:
        venue_id: ID of the venue
        request: List of conflicts with shift_id, employee_id, reason

    Returns:
        Resolution plan for each conflict with top alternatives
    """
    logger.info(f"POST /conflicts/bulk-resolve (venue: {venue_id}, {len(request.conflicts)} conflicts)")

    venue = get_db().get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    plans = _resolver.bulk_resolve(request.conflicts, venue_id)

    return BulkResolveResponse(
        venue_id=venue_id,
        total_conflicts=len(request.conflicts),
        plans=[
            ResolutionPlanResponse(
                shift_id=plan.shift_id,
                original_employee_id=plan.original_employee_id,
                recommended_employee_id=plan.recommended_employee_id,
                reason=plan.reason,
                alternatives=[
                    AlternativeOptionResponse(
                        employee_id=alt.employee.id,
                        employee_name=alt.employee.name,
                        overall_score=alt.overall_score,
                        cost_impact=float(alt.cost_impact),
                        skill_match_score=alt.skill_match_score,
                        fairness_score=alt.fairness_score,
                        preference_score=alt.preference_score,
                        availability_fit_score=alt.availability_fit_score,
                        hours_this_week=alt.hours_this_week,
                        hours_after=alt.hours_after,
                        consecutive_days_after=alt.consecutive_days_after,
                        warnings=alt.warnings,
                    )
                    for alt in plan.alternatives
                ],
            )
            for plan in plans
        ],
    )


@router.get("/venues/{venue_id}/coverage/{coverage_date}")
async def get_availability_coverage(
    venue_id: str = Path(..., description="Venue ID"),
    coverage_date: date = Path(..., description="Date (YYYY-MM-DD)"),
) -> CoverageReportResponse:
    """
    For a given day, show coverage gaps and available staff per hour.

    Returns hourly breakdown of:
    - Currently scheduled staff
    - Coverage gaps (hours below minimum required)
    - Available staff not yet scheduled (with time windows)

    Useful for identifying understaffed periods and finding available backups.

    Args:
        venue_id: ID of the venue
        coverage_date: Date to analyze (YYYY-MM-DD format)

    Returns:
        Detailed hourly coverage report with gaps and available staff
    """
    logger.info(f"GET /venues/{venue_id}/coverage/{coverage_date}")

    db = get_db()
    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    report = _resolver.get_availability_coverage(venue_id, coverage_date)

    # Build hourly breakdown
    hourly = []
    for hour in range(24):
        staff_ids = report.hours_coverage.get(hour, [])
        understaffed = len(staff_ids) < report.min_required
        hourly.append(
            CoverageHourlyBreakdown(
                hour=hour,
                staff_count=len(staff_ids),
                staff_ids=staff_ids,
                understaffed=understaffed,
            )
        )

    # Build available staff
    available_staff_list = []
    for emp_id, windows in report.available_staff.items():
        emp = db.get_employee(emp_id)
        if emp:
            available_staff_list.append(
                AvailableStaffMember(
                    employee_id=emp_id,
                    employee_name=emp.name,
                    availability_windows=[
                        {"start_hour": s, "end_hour": e}
                        for s, e in windows
                    ],
                )
            )

    return CoverageReportResponse(
        venue_id=venue_id,
        date=coverage_date.isoformat(),
        min_required=report.min_required,
        total_scheduled=sum(len(ids) for ids in report.hours_coverage.values()),
        understaffed_hours=report.understaffed_hours,
        gaps=[{"start_hour": s, "end_hour": e} for s, e in report.gaps],
        hourly_breakdown=hourly,
        available_staff=available_staff_list,
    )
