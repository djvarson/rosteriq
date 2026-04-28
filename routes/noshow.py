"""
No-show risk prediction and mitigation routes.

Provides endpoints for:
- Predicting no-show risks for upcoming shifts
- Retrieving employee reliability profiles
- Recording shift outcomes for model feedback
- Getting venue-level risk summaries
- Finding backup staff for high-risk shifts
"""

from datetime import date
from typing import List, Dict, Optional, Any
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.services.noshow_predictor import (
    NoShowPredictor, ShiftRisk, ReliabilityProfile, VenueRiskSummary,
    BackupEmployee, RiskFactor,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["noshow"])


# ============================================================================
# Request/Response Models
# ============================================================================


class RiskFactorResponse(BaseModel):
    """Response model for a single risk factor."""
    name: str
    score: float = Field(..., ge=0, le=100)
    weight: float = Field(..., ge=0, le=1)
    description: str


class BackupEmployeeResponse(BaseModel):
    """Response model for a backup staff recommendation."""
    employee_id: str
    name: str
    availability_match: bool
    cost_delta: Optional[float] = None
    skills_match: bool


class ShiftRiskResponse(BaseModel):
    """Response model for a shift risk assessment."""
    shift_id: str
    employee_id: str
    employee_name: str
    date: str
    start_time: str
    end_time: str
    role: str
    risk_score: float = Field(..., ge=0, le=100)
    risk_level: str = Field(..., pattern="^(low|medium|high|critical)$")
    risk_factors: List[RiskFactorResponse]
    suggested_backups: List[BackupEmployeeResponse]
    mitigation: str


class NoShowRiskListResponse(BaseModel):
    """Response model for list of shifts at risk."""
    date_from: str
    date_to: str
    total_shifts: int
    shifts_at_risk: int
    shifts_high_risk: int
    shifts_critical_risk: int
    avg_risk_score: float
    shifts: List[ShiftRiskResponse]


class EmployeeReliabilityResponse(BaseModel):
    """Response model for employee reliability profile."""
    employee_id: str
    employee_name: str
    total_shifts: int
    completed: int
    no_shows: int
    late_arrivals: int
    reliability_pct: float
    trend: str = Field(..., pattern="^(improving|stable|declining)$")
    risk_days: List[str]
    days_since_last_noshow: int


class OutcomeRecordRequest(BaseModel):
    """Request to record shift outcome."""
    outcome: str = Field(..., pattern="^(completed|no_show|late_arrival|cancelled)$")


class VenueRiskSummaryResponse(BaseModel):
    """Response model for venue risk summary."""
    venue_id: str
    date_from: str
    date_to: str
    total_shifts_at_risk: int
    shifts_high_risk: int
    shifts_critical_risk: int
    total_risk_exposure: float
    avg_risk_score: float
    highest_risk_date: Optional[str] = None
    highest_risk_role: Optional[str] = None
    recommendations: List[str]


class BackupListResponse(BaseModel):
    """Response model for suggested backup staff."""
    shift_id: str
    suggested_count: int
    backups: List[BackupEmployeeResponse]


# ============================================================================
# Route handlers
# ============================================================================


@router.get(
    "/venues/{venue_id}/noshow-risk",
    response_model=NoShowRiskListResponse,
    status_code=status.HTTP_200_OK,
    summary="Predict no-show risks",
    description="Get risk-scored upcoming shifts for a venue with mitigation suggestions",
)
async def get_noshow_risks(
    venue_id: str,
    date_from: str = Query(
        ...,
        description="Start date in ISO format (YYYY-MM-DD)",
        example="2026-04-28"
    ),
    date_to: str = Query(
        ...,
        description="End date in ISO format (YYYY-MM-DD)",
        example="2026-05-04"
    ),
    risk_level: Optional[str] = Query(
        None,
        description="Filter by risk level (low|medium|high|critical)",
        example="high"
    ),
    current_user: UserContext = Depends(get_current_user),
) -> NoShowRiskListResponse:
    """
    Predict no-show risks for all upcoming shifts in a venue during a date range.

    Risk factors considered:
    - Historical no-show rate (30%)
    - Day-of-week patterns (15%)
    - Shift timing (10%)
    - Consecutive days worked (10%)
    - Fatigue level (10%)
    - Weather severity (5%)
    - Recent 2-week pattern (10%)
    - Shift confirmation status (10%)

    Risk levels:
    - low: <25
    - medium: 25-50
    - high: 50-75
    - critical: >75
    """
    try:
        predictor = NoShowPredictor()
        shifts = predictor.predict_risks(venue_id, date_from, date_to)

        # Filter by risk level if specified
        if risk_level:
            shifts = [s for s in shifts if s.risk_level == risk_level]

        # Calculate summary stats
        at_risk = [s for s in shifts if s.risk_level != "low"]
        high_risk = [s for s in shifts if s.risk_level == "high"]
        critical_risk = [s for s in shifts if s.risk_level == "critical"]

        avg_score = (
            sum(s.risk_score for s in shifts) / len(shifts)
            if shifts else 0.0
        )

        # Convert to response models
        shift_responses = [
            ShiftRiskResponse(
                shift_id=s.shift_id,
                employee_id=s.employee_id,
                employee_name=s.employee_name,
                date=s.date,
                start_time=s.start_time,
                end_time=s.end_time,
                role=s.role,
                risk_score=s.risk_score,
                risk_level=s.risk_level,
                risk_factors=[
                    RiskFactorResponse(
                        name=f.name,
                        score=f.score,
                        weight=f.weight,
                        description=f.description
                    )
                    for f in s.risk_factors
                ],
                suggested_backups=[
                    BackupEmployeeResponse(
                        employee_id=b.employee_id,
                        name=b.name,
                        availability_match=b.availability_match,
                        cost_delta=float(b.cost_delta) if b.cost_delta else None,
                        skills_match=b.skills_match
                    )
                    for b in s.suggested_backups
                ],
                mitigation=s.mitigation,
            )
            for s in shifts
        ]

        return NoShowRiskListResponse(
            date_from=date_from,
            date_to=date_to,
            total_shifts=len(shifts),
            shifts_at_risk=len(at_risk),
            shifts_high_risk=len(high_risk),
            shifts_critical_risk=len(critical_risk),
            avg_risk_score=round(avg_score, 1),
            shifts=shift_responses,
        )

    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error predicting risks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to predict no-show risks"
        )


@router.get(
    "/employees/{employee_id}/reliability",
    response_model=EmployeeReliabilityResponse,
    status_code=status.HTTP_200_OK,
    summary="Get employee reliability profile",
    description="Retrieve historical reliability stats for an employee (last 90 days)",
)
async def get_employee_reliability(
    employee_id: str,
    current_user: UserContext = Depends(get_current_user),
) -> EmployeeReliabilityResponse:
    """
    Get employee's reliability profile based on last 90 days of history.

    Returns:
    - Total shifts and completion rate
    - No-shows and late arrivals
    - Trend (improving/stable/declining)
    - Risk days (days with >40% no-show rate)
    - Days since last no-show
    """
    try:
        predictor = NoShowPredictor()
        profile = predictor.get_employee_reliability(employee_id)

        return EmployeeReliabilityResponse(
            employee_id=profile.employee_id,
            employee_name=profile.employee_name,
            total_shifts=profile.total_shifts,
            completed=profile.completed,
            no_shows=profile.no_shows,
            late_arrivals=profile.late_arrivals,
            reliability_pct=profile.reliability_pct,
            trend=profile.trend,
            risk_days=profile.risk_days,
            days_since_last_noshow=profile.days_since_last_noshow,
        )

    except Exception as e:
        logger.error(f"Error retrieving reliability profile: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve reliability profile"
        )


@router.post(
    "/shifts/{shift_id}/record-outcome",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Record shift outcome",
    description="Record the actual outcome of a shift for model feedback (completed|no_show|late_arrival|cancelled)",
)
async def record_shift_outcome(
    shift_id: str,
    request: OutcomeRecordRequest,
    current_user: UserContext = Depends(get_current_user),
) -> None:
    """
    Record the actual outcome of a shift.

    This allows the model to learn from real outcomes and improve predictions.
    Outcomes: completed, no_show, late_arrival, cancelled
    """
    try:
        predictor = NoShowPredictor()
        predictor.record_outcome(shift_id, request.outcome)

    except Exception as e:
        logger.error(f"Error recording outcome: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record shift outcome"
        )


@router.get(
    "/venues/{venue_id}/risk-summary",
    response_model=VenueRiskSummaryResponse,
    status_code=status.HTTP_200_OK,
    summary="Get venue risk summary",
    description="Get aggregate no-show risk summary for a venue",
)
async def get_venue_risk_summary(
    venue_id: str,
    date_from: str = Query(
        ...,
        description="Start date in ISO format (YYYY-MM-DD)",
        example="2026-04-28"
    ),
    date_to: str = Query(
        ...,
        description="End date in ISO format (YYYY-MM-DD)",
        example="2026-05-04"
    ),
    current_user: UserContext = Depends(get_current_user),
) -> VenueRiskSummaryResponse:
    """
    Get venue-level risk summary for a date range.

    Returns:
    - Total shifts at risk and breakdown by severity
    - Total risk exposure and average risk score
    - Highest risk date and role
    - Recommendations for venue management
    """
    try:
        predictor = NoShowPredictor()
        summary = predictor.get_venue_risk_summary(venue_id, date_from, date_to)

        return VenueRiskSummaryResponse(
            venue_id=summary.venue_id,
            date_from=summary.date_from,
            date_to=summary.date_to,
            total_shifts_at_risk=summary.total_shifts_at_risk,
            shifts_high_risk=summary.shifts_high_risk,
            shifts_critical_risk=summary.shifts_critical_risk,
            total_risk_exposure=summary.total_risk_exposure,
            avg_risk_score=summary.avg_risk_score,
            highest_risk_date=summary.highest_risk_date,
            highest_risk_role=summary.highest_risk_role,
            recommendations=summary.recommendations,
        )

    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error retrieving risk summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve risk summary"
        )


@router.get(
    "/shifts/{shift_id}/backups",
    response_model=BackupListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get suggested backup staff",
    description="Find available backup staff for a high-risk shift",
)
async def get_shift_backups(
    shift_id: str,
    current_user: UserContext = Depends(get_current_user),
) -> BackupListResponse:
    """
    Find available backup staff for a shift.

    Considers:
    - Availability on the shift date/time
    - Skill match with the role
    - Employment type (casuals preferred for flexibility)
    - Cost differential

    Returns top 5 backup candidates sorted by suitability.
    """
    try:
        predictor = NoShowPredictor()
        backups = predictor.suggest_backups(shift_id)

        return BackupListResponse(
            shift_id=shift_id,
            suggested_count=len(backups),
            backups=[
                BackupEmployeeResponse(
                    employee_id=b.employee_id,
                    name=b.name,
                    availability_match=b.availability_match,
                    cost_delta=float(b.cost_delta) if b.cost_delta else None,
                    skills_match=b.skills_match,
                )
                for b in backups
            ],
        )

    except Exception as e:
        logger.error(f"Error retrieving backups: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve backup suggestions"
        )
