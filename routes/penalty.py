"""
Penalty rate calculator API routes for RosterIQ.

Real-time penalty breakdown endpoints for instant UI feedback during roster editing.

Endpoints:
- POST /api/v1/calculate-penalty — full breakdown (employee_id + shift details)
- POST /api/v1/quick-estimate — no employee lookup needed
- POST /api/v1/compare-shift-times — compare costs at different times
- POST /api/v1/compare-shift-days — compare costs on different days
- POST /api/v1/cheapest-slot — find cheapest time window
"""

import logging
from typing import Dict, List, Optional
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.services.penalty_calculator import PenaltyCalculator, PenaltyBreakdown
from rosteriq.models import EmploymentType, State

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["penalties"])


# ============================================================================
# Request/Response Models
# ============================================================================


class PenaltyDetailResponse(BaseModel):
    """Response model for a single penalty detail."""
    name: str
    multiplier: float
    hours_applicable: float
    additional_cost: str  # Decimal as string for JSON
    description: str


class CostComparisonResponse(BaseModel):
    """Response model for cost comparison."""
    weekday_cost: str
    actual_cost: str
    premium: str
    premium_pct: float


class PenaltyBreakdownResponse(BaseModel):
    """Response model for full penalty breakdown."""
    employee_name: str
    base_hourly_rate: str
    date: str
    day_type: str
    start_time: str
    end_time: str
    total_hours: float
    break_minutes: int
    net_hours: float
    penalties_applied: List[PenaltyDetailResponse]
    base_cost: str
    penalty_cost: str
    casual_loading: str
    super_contribution: str
    total_hourly_cost: str
    total_shift_cost: str
    cost_comparison: Optional[CostComparisonResponse] = None


class CalculatePenaltyRequest(BaseModel):
    """Request to calculate penalty for a shift."""
    employee_id: str = Field(..., description="Employee ID")
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    start_time: str = Field(..., description="Start time in HH:MM format")
    end_time: str = Field(..., description="End time in HH:MM format")
    role: Optional[str] = Field(None, description="Optional role name")
    break_minutes: int = Field(default=0, description="Unpaid break duration")


class QuickEstimateRequest(BaseModel):
    """Request for quick cost estimate without employee lookup."""
    award_level: str = Field(..., description="Award level")
    employment_type: str = Field(
        ...,
        description="Employment type: full_time, part_time, or casual"
    )
    state: str = Field(..., description="State code (e.g. vic, nsw, qld)")
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    start_time: str = Field(..., description="Start time in HH:MM format")
    end_time: str = Field(..., description="End time in HH:MM format")
    hourly_rate: str = Field(..., description="Base hourly rate")
    break_minutes: int = Field(default=0, description="Unpaid break duration")


class TimeOption(BaseModel):
    """Single time option for comparison."""
    start_time: str = Field(..., description="Start time in HH:MM format")
    end_time: str = Field(..., description="End time in HH:MM format")


class CompareShiftTimesRequest(BaseModel):
    """Request to compare shift costs at different times."""
    employee_id: str = Field(..., description="Employee ID")
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    options: List[TimeOption] = Field(
        ...,
        description="List of time options to compare"
    )


class CompareShiftDaysRequest(BaseModel):
    """Request to compare shift costs on different days."""
    employee_id: str = Field(..., description="Employee ID")
    start_time: str = Field(..., description="Start time in HH:MM format")
    end_time: str = Field(..., description="End time in HH:MM format")
    dates: List[str] = Field(
        ...,
        description="List of dates in YYYY-MM-DD format"
    )


class CheapestSlotRequest(BaseModel):
    """Request to find cheapest time slot for a duration."""
    employee_id: str = Field(..., description="Employee ID")
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    duration_hours: float = Field(..., description="Desired shift duration in hours")


class CheapestSlotResponse(BaseModel):
    """Response with cheapest time slot."""
    start_time: str
    end_time: str
    total_cost: str


# ============================================================================
# Helper Functions
# ============================================================================


def _penalty_breakdown_to_response(breakdown: PenaltyBreakdown) -> PenaltyBreakdownResponse:
    """Convert internal PenaltyBreakdown to API response."""
    return PenaltyBreakdownResponse(
        employee_name=breakdown.employee_name,
        base_hourly_rate=str(breakdown.base_hourly_rate),
        date=breakdown.date,
        day_type=breakdown.day_type,
        start_time=breakdown.start_time,
        end_time=breakdown.end_time,
        total_hours=breakdown.total_hours,
        break_minutes=breakdown.break_minutes,
        net_hours=breakdown.net_hours,
        penalties_applied=[
            PenaltyDetailResponse(
                name=p.name,
                multiplier=p.multiplier,
                hours_applicable=p.hours_applicable,
                additional_cost=str(p.additional_cost),
                description=p.description,
            )
            for p in breakdown.penalties_applied
        ],
        base_cost=str(breakdown.base_cost),
        penalty_cost=str(breakdown.penalty_cost),
        casual_loading=str(breakdown.casual_loading),
        super_contribution=str(breakdown.super_contribution),
        total_hourly_cost=str(breakdown.total_hourly_cost),
        total_shift_cost=str(breakdown.total_shift_cost),
        cost_comparison=(
            CostComparisonResponse(
                weekday_cost=str(breakdown.cost_comparison.weekday_cost),
                actual_cost=str(breakdown.cost_comparison.actual_cost),
                premium=str(breakdown.cost_comparison.premium),
                premium_pct=breakdown.cost_comparison.premium_pct,
            )
            if breakdown.cost_comparison
            else None
        ),
    )


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/calculate-penalty", response_model=PenaltyBreakdownResponse)
async def calculate_penalty(request: CalculatePenaltyRequest) -> PenaltyBreakdownResponse:
    """
    Calculate full penalty breakdown for a shift.

    Performs instant rate calculation for real-time UI feedback during
    roster editing. Includes base cost, penalties, casual loading,
    superannuation, and cost comparison.

    Args:
        request: CalculatePenaltyRequest with employee_id and shift details

    Returns:
        PenaltyBreakdownResponse with complete cost analysis
    """
    try:
        calculator = PenaltyCalculator()
        breakdown = calculator.calculate(
            employee_id=request.employee_id,
            date_str=request.date,
            start_time_str=request.start_time,
            end_time_str=request.end_time,
            role=request.role,
            break_minutes=request.break_minutes,
        )
        return _penalty_breakdown_to_response(breakdown)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating penalty: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate penalty: {str(e)}",
        )


@router.post("/quick-estimate", response_model=PenaltyBreakdownResponse)
async def quick_estimate(request: QuickEstimateRequest) -> PenaltyBreakdownResponse:
    """
    Quick cost estimate without employee lookup.

    For "what would it cost?" calculator. No database access — just rate
    calculation based on provided parameters. Useful for quick comparisons
    and scenario planning.

    Args:
        request: QuickEstimateRequest with award level, employment type, and rate

    Returns:
        PenaltyBreakdownResponse with estimated cost
    """
    try:
        calculator = PenaltyCalculator()
        hourly_rate = Decimal(request.hourly_rate)
        breakdown = calculator.quick_estimate(
            award_level=request.award_level,
            employment_type=request.employment_type,
            state=request.state,
            date_str=request.date,
            start_time_str=request.start_time,
            end_time_str=request.end_time,
            hourly_rate=hourly_rate,
            break_minutes=request.break_minutes,
        )
        return _penalty_breakdown_to_response(breakdown)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error estimating penalty: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to estimate penalty: {str(e)}",
        )


@router.post(
    "/compare-shift-times",
    response_model=List[PenaltyBreakdownResponse]
)
async def compare_shift_times(
    request: CompareShiftTimesRequest,
) -> List[PenaltyBreakdownResponse]:
    """
    Compare cost of same shift at different times.

    Returns penalty breakdown for each time option, making it easy to
    visually compare costs and identify cheapest times to schedule.

    Args:
        request: CompareShiftTimesRequest with employee_id, date, and time options

    Returns:
        List of PenaltyBreakdownResponse, one per time option
    """
    try:
        calculator = PenaltyCalculator()
        options = [(opt.start_time, opt.end_time) for opt in request.options]
        results = calculator.compare_times(
            employee_id=request.employee_id,
            date_str=request.date,
            options=options,
        )
        return [_penalty_breakdown_to_response(b) for b in results]
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error comparing shift times: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare shift times: {str(e)}",
        )


@router.post(
    "/compare-shift-days",
    response_model=List[PenaltyBreakdownResponse]
)
async def compare_shift_days(
    request: CompareShiftDaysRequest,
) -> List[PenaltyBreakdownResponse]:
    """
    Compare cost of same shift on different days.

    Useful for identifying the cheapest days to schedule a given shift,
    accounting for weekend and public holiday penalties.

    Args:
        request: CompareShiftDaysRequest with employee_id, times, and dates

    Returns:
        List of PenaltyBreakdownResponse, one per date
    """
    try:
        calculator = PenaltyCalculator()
        results = calculator.compare_days(
            employee_id=request.employee_id,
            start_time_str=request.start_time,
            end_time_str=request.end_time,
            dates=request.dates,
        )
        return [_penalty_breakdown_to_response(b) for b in results]
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error comparing shift days: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare shift days: {str(e)}",
        )


@router.post("/cheapest-slot", response_model=CheapestSlotResponse)
async def find_cheapest_slot(
    request: CheapestSlotRequest,
) -> CheapestSlotResponse:
    """
    Find the cheapest time window for a given duration on a given day.

    Scans through all hourly slots and identifies the time window that
    minimizes cost for the requested shift duration.

    Args:
        request: CheapestSlotRequest with employee_id, date, and duration

    Returns:
        CheapestSlotResponse with start_time, end_time, and total_cost
    """
    try:
        calculator = PenaltyCalculator()
        start_time, end_time, total_cost = calculator.cheapest_time_slot(
            employee_id=request.employee_id,
            date_str=request.date,
            duration_hours=request.duration_hours,
        )
        return CheapestSlotResponse(
            start_time=start_time,
            end_time=end_time,
            total_cost=str(total_cost),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error finding cheapest slot: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to find cheapest slot: {str(e)}",
        )
