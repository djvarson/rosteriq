"""
Auto-Scheduling API routes for RosterIQ.

Endpoints for generating full-week rosters from demand forecasts using the
smart auto-scheduler engine.

Routes:
    POST /api/v1/venues/{venue_id}/auto-schedule — generate full week roster
    GET /api/v1/venues/{venue_id}/schedule-preview — preview demand + availability
    POST /api/v1/rosters/{roster_id}/fill-gaps — fill coverage gaps in roster
    GET /api/v1/venues/{venue_id}/hiring-suggestions — hiring recommendations

The auto-schedule endpoint accepts:
    - week_start: Start date of the week (ISO 8601)
    - strategy: "balanced" (default), "cost_optimized", or "coverage_first"
    - covers_per_staff: Optional override for covers-to-staff ratio (default: 15)

Returns ScheduleResult with:
    - roster: The generated Roster object
    - schedule_quality: Quality score (0-100)
    - coverage_gaps: List of unfilled slots
    - total_cost: Decimal total cost
    - cost_breakdown: Breakdown by type/day/role
    - warnings: List of warning messages
    - employees_used: Number of employees scheduled
    - total_shifts: Number of shifts created
    - total_hours: Total hours scheduled
    - strategy_used: Strategy that was used
    - generation_time_ms: Time taken to generate
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field, field_validator

from rosteriq.database import get_db
from rosteriq.middleware.tenant import (
    enforce_venue_access, enforce_venue_manager, load_roster_in_scope,
)
from rosteriq.models import Roster, Employee
from rosteriq.auth import get_current_user
from rosteriq.services.auto_scheduler import (
    AutoScheduler, ScheduleResult, CoverageGap, HiringRecommendation,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["auto-schedule"])


# ============================================================================
# Request/Response Models
# ============================================================================

class AutoScheduleRequest(BaseModel):
    """Request to generate a weekly schedule."""
    week_start: str = Field(..., description="Start date (ISO 8601, must be Monday)")
    strategy: str = Field(
        default="balanced",
        description="Strategy: 'balanced', 'cost_optimized', or 'coverage_first'",
    )
    covers_per_staff: Optional[float] = Field(
        default=None,
        description="Covers per staff ratio (override venue default)",
    )

    @field_validator("week_start")
    @classmethod
    def validate_week_start(cls, v: str) -> str:
        """Ensure week_start is a valid ISO date and is a Monday."""
        try:
            week_date = date.fromisoformat(v)
            # ISO 8601: Monday is 0
            if week_date.weekday() != 0:
                raise ValueError("week_start must be a Monday (ISO 8601)")
            return v
        except ValueError as e:
            raise ValueError(f"Invalid week_start: {e}")

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: str) -> str:
        """Ensure strategy is valid."""
        valid = ["balanced", "cost_optimized", "coverage_first"]
        if v not in valid:
            raise ValueError(f"strategy must be one of {valid}")
        return v


class CoverageGapResponse(BaseModel):
    """Coverage gap in response."""
    date: str
    hour: int
    role: str
    reason: str


class CostBreakdownResponse(BaseModel):
    """Cost breakdown in response."""
    by_employment_type: dict = Field(default_factory=dict)
    by_day: dict = Field(default_factory=dict)
    by_role: dict = Field(default_factory=dict)
    total: str  # Decimal as string


class ScheduleResultResponse(BaseModel):
    """Response from schedule generation."""
    roster_id: str
    week_start: str
    week_end: str
    schedule_quality: float
    coverage_gaps: List[CoverageGapResponse]
    total_cost: str  # Decimal as string
    cost_breakdown: CostBreakdownResponse
    warnings: List[str]
    employees_used: int
    total_shifts: int
    total_hours: float
    strategy_used: str
    generation_time_ms: float

    @classmethod
    def from_result(cls, result: ScheduleResult) -> "ScheduleResultResponse":
        """Convert ScheduleResult to response."""
        # Serialize cost_breakdown
        cost_bd = {}
        for key, val in result.cost_breakdown.items():
            if isinstance(val, dict):
                cost_bd[key] = {k: str(v) for k, v in val.items()}
            else:
                cost_bd[key] = str(val)

        return cls(
            roster_id=result.roster.id,
            week_start=result.roster.week_start.isoformat(),
            week_end=result.roster.week_end.isoformat(),
            schedule_quality=result.schedule_quality,
            coverage_gaps=[
                CoverageGapResponse(
                    date=gap.date.isoformat(),
                    hour=gap.hour,
                    role=gap.role,
                    reason=gap.reason,
                )
                for gap in result.coverage_gaps
            ],
            total_cost=str(result.total_cost),
            cost_breakdown=CostBreakdownResponse(**cost_bd),
            warnings=result.warnings,
            employees_used=result.employees_used,
            total_shifts=result.total_shifts,
            total_hours=result.total_hours,
            strategy_used=result.strategy_used,
            generation_time_ms=result.generation_time_ms,
        )


class PreviewResponse(BaseModel):
    """Response from schedule preview."""
    week_start: str
    demand_grid: dict
    available_staff: dict


class HiringRecommendationResponse(BaseModel):
    """Hiring recommendation in response."""
    role: str
    priority: str
    gap_days: int
    estimated_hours_per_week: float
    reason: str


class HiringRecommendationsResponse(BaseModel):
    """Response with hiring recommendations."""
    venue_id: str
    week_start: str
    recommendations: List[HiringRecommendationResponse]


class FillGapsResponse(BaseModel):
    """Response from filling gaps."""
    roster_id: str
    shifts_added: int
    total_cost_added: str  # Decimal as string


# ============================================================================
# Endpoints
# ============================================================================

@router.post(
    "/venues/{venue_id}/auto-schedule",
    response_model=ScheduleResultResponse,
    summary="Generate weekly roster",
    description="Generate a full week's roster from demand forecasts using auto-scheduler",
)
async def generate_schedule(
    venue_id: str,
    request: AutoScheduleRequest,
    db=Depends(get_db),
    current_user=Depends(get_current_user),
) -> ScheduleResultResponse:
    """
    Generate a full week's roster from demand forecasts.

    The scheduler uses a multi-step algorithm:
    1. Converts demand forecasts to required staff per hour
    2. Builds shift templates from demand patterns
    3. Assigns employees to shifts using multi-factor scoring
    4. Validates for compliance conflicts
    5. Returns detailed schedule with quality metrics

    Query Parameters:
        - week_start: ISO 8601 date (must be Monday)
        - strategy: "balanced", "cost_optimized", or "coverage_first"
        - covers_per_staff: Optional covers-per-staff ratio override

    Returns 200 with ScheduleResultResponse on success.
    Returns 400 if week_start is invalid or not a Monday.
    Returns 404 if venue not found.
    """
    enforce_venue_manager(venue_id)
    try:
        # Verify venue exists
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Parse week_start
        try:
            week_start = date.fromisoformat(request.week_start)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid week_start format (use ISO 8601)",
            )

        # Generate schedule
        scheduler = AutoScheduler(db)
        covers = request.covers_per_staff or 15.0

        result = scheduler.generate_week(
            venue_id=venue_id,
            week_start=week_start,
            strategy=request.strategy,
            covers_per_staff=covers,
        )

        # Save roster to database
        db.save_roster(result.roster)

        # Return response
        return ScheduleResultResponse.from_result(result)

    except HTTPException:
        raise
    except ValueError as e:
        logger.warning(f"Schedule generation validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Schedule generation error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Schedule generation failed")


@router.get(
    "/venues/{venue_id}/schedule-preview",
    response_model=PreviewResponse,
    summary="Preview demand and availability",
    description="Preview demand patterns and available staff without generating schedule",
)
async def preview_schedule(
    venue_id: str,
    week_start: str = Query(..., description="Start date (ISO 8601)"),
    covers_per_staff: Optional[float] = Query(None, description="Optional ratio override"),
    db=Depends(get_db),
    current_user=Depends(get_current_user),
) -> PreviewResponse:
    """
    Preview demand patterns and available staff for a week.

    Returns demand grid and available staff counts without generating a schedule.

    Returns 200 with PreviewResponse.
    Returns 400 if week_start is invalid.
    Returns 404 if venue not found.
    """
    try:
        # Verify venue exists
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Verify user has access (owners pass)
        enforce_venue_access(venue_id)

        # Parse date
        try:
            week_date = date.fromisoformat(week_start)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

        # Get preview
        scheduler = AutoScheduler(db)
        covers = covers_per_staff or 15.0

        preview_data = scheduler.preview_week(
            venue_id=venue_id,
            week_start=week_date,
            covers_per_staff=covers,
        )

        return PreviewResponse(**preview_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Preview error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Preview failed")


@router.post(
    "/rosters/{roster_id}/fill-gaps",
    response_model=FillGapsResponse,
    summary="Fill coverage gaps",
    description="Find and fill remaining coverage gaps in an existing roster",
)
async def fill_gaps(
    roster_id: str,
    venue_id: str = Query(..., description="Venue ID"),
    db=Depends(get_db),
    current_user=Depends(get_current_user),
) -> FillGapsResponse:
    """
    Fill remaining coverage gaps in an existing roster.

    Identifies unfilled shift slots and attempts to fill them by finding
    available employees.

    Returns 200 with number of shifts added.
    Returns 404 if roster not found.
    """
    # Gate on the ROSTER's own venue, not the caller-supplied venue_id (that was
    # an IDOR: any venue_id the caller could access passed the check while
    # roster_id targeted another tenant). fill-gaps mutates the roster -> manager.
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager(getattr(roster, "venue_id", None))
    if venue_id != getattr(roster, "venue_id", None):
        raise HTTPException(
            status_code=400, detail="venue_id does not match the roster's venue")
    try:
        # Fill gaps
        scheduler = AutoScheduler(db)
        new_shifts = scheduler.fill_gaps(roster_id, venue_id)

        # Calculate cost of new shifts
        total_new_cost = sum(s.cost or Decimal("0") for s in new_shifts)

        # Update roster in database
        roster.shifts.extend(new_shifts)
        db.save_roster(roster)

        return FillGapsResponse(
            roster_id=roster_id,
            shifts_added=len(new_shifts),
            total_cost_added=str(total_new_cost),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fill gaps error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Gap filling failed")


@router.get(
    "/venues/{venue_id}/hiring-suggestions",
    response_model=HiringRecommendationsResponse,
    summary="Get hiring recommendations",
    description="Analyze scheduling patterns to suggest hiring needs",
)
async def get_hiring_suggestions(
    venue_id: str,
    week_start: str = Query(..., description="Start date (ISO 8601)"),
    db=Depends(get_db),
    current_user=Depends(get_current_user),
) -> HiringRecommendationsResponse:
    """
    Analyze 4-week scheduling patterns to identify hiring needs.

    Based on chronic coverage gaps, recommends roles and priority levels for hiring.

    Returns 200 with HiringRecommendationsResponse.
    Returns 400 if week_start is invalid.
    Returns 404 if venue not found.
    """
    try:
        # Verify venue exists
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Verify user has access (owners pass)
        enforce_venue_access(venue_id)

        # Parse date
        try:
            week_date = date.fromisoformat(week_start)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

        # Get recommendations
        scheduler = AutoScheduler(db)
        recommendations = scheduler.suggest_hiring(venue_id, week_date)

        return HiringRecommendationsResponse(
            venue_id=venue_id,
            week_start=week_start,
            recommendations=[
                HiringRecommendationResponse(
                    role=rec.role,
                    priority=rec.priority,
                    gap_days=rec.gap_days,
                    estimated_hours_per_week=rec.estimated_hours_per_week,
                    reason=rec.reason,
                )
                for rec in recommendations
            ],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Hiring suggestions error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Hiring analysis failed")
