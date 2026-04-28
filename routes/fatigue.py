"""
Fatigue prediction and burnout risk assessment routes.

Provides endpoints for:
- Individual fatigue assessments
- Team fatigue reports
- Clopening shift identification
- Burnout date predictions
- Recovery roster suggestions
"""

from datetime import date
from typing import Optional, List, Dict, Any
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.services.fatigue_predictor import FatiguePredictor
from rosteriq.models import Shift

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["fatigue"])


# ============================================================================
# Request/Response Models
# ============================================================================


class FatigueFactorScores(BaseModel):
    """Individual factor scores for fatigue assessment."""
    consecutive_days: float
    weekly_hours_trend: float
    hours_vs_contract: float
    clopening: float
    weekend_ratio: float
    overtime_ratio: float
    leave_balance: float
    shift_variety: float


class FatigueAssessmentResponse(BaseModel):
    """Response for individual fatigue assessment."""
    employee_id: str
    employee_name: str
    overall_score: float
    risk_level: str
    factor_scores: Dict[str, float]
    consecutive_days_current: int
    avg_weekly_hours_4w: float
    clopening_count_4w: int
    weekend_ratio: float
    overtime_hours_4w: float
    days_since_last_leave: int
    recommendations: List[str]
    trend: str


class ClopeningShiftPair(BaseModel):
    """A pair of late-to-early shifts (clopening)."""
    late_shift_date: date
    late_shift_end_time: str
    early_shift_date: date
    early_shift_start_time: str
    gap_hours: float


class ClopeningListResponse(BaseModel):
    """Response for clopening shifts."""
    employee_id: str
    employee_name: str
    clopening_count: int
    clopenings: List[ClopeningShiftPair]


class BurnoutPredictionResponse(BaseModel):
    """Response for burnout date prediction."""
    employee_id: str
    employee_name: str
    current_fatigue_score: float
    current_risk_level: str
    trend: str
    predicted_critical_date: Optional[date] = None
    days_to_critical: Optional[int] = None
    recommendation: str


class RecoveryRosterResponse(BaseModel):
    """Response for recovery roster suggestions."""
    employee_id: str
    recommended_total_hours: float
    recommended_shifts: int
    avoid_patterns: List[str]
    days_off: int
    ideal_shift_timing: str
    reason: str


class EmployeeFatigueItem(BaseModel):
    """Summary of one employee's fatigue status."""
    employee_id: str
    employee_name: str
    overall_score: float
    risk_level: str
    trend: str
    top_concern: str


class TeamFatigueReportResponse(BaseModel):
    """Response for team fatigue report."""
    venue_id: str
    assessed_count: int
    avg_score: float
    high_risk_count: int
    risk_distribution: Dict[str, int]
    employees: List[EmployeeFatigueItem]
    team_recommendations: List[str]


# ============================================================================
# Helper Functions
# ============================================================================


def _get_predictor() -> FatiguePredictor:
    """Get a fatigue predictor instance."""
    return FatiguePredictor()


def _get_top_concern(factor_scores: Dict[str, float]) -> str:
    """Get the top concern from factor scores."""
    concern_map = {
        "consecutive_days": "Consecutive days",
        "weekly_hours_trend": "Hours increasing",
        "hours_vs_contract": "Hours vs contract",
        "clopening": "Late-early shifts",
        "weekend_ratio": "Weekend work",
        "overtime_ratio": "Overtime hours",
        "leave_balance": "Leave balance",
        "shift_variety": "Shift variety",
    }

    if not factor_scores:
        return "Unknown"

    top_factor = max(factor_scores.items(), key=lambda x: x[1])
    return concern_map.get(top_factor[0], "Unknown")


# ============================================================================
# Routes
# ============================================================================


@router.get("/employees/{employee_id}/fatigue-risk", response_model=FatigueAssessmentResponse)
async def get_fatigue_risk(
    employee_id: str,
    lookback_weeks: int = Query(4, ge=1, le=12, description="Weeks to analyze (1-12)"),
    current_user: UserContext = Depends(get_current_user),
):
    """
    Get fatigue risk assessment for an individual employee.

    Returns scores for 8 fatigue factors and an overall burnout risk score.
    Risk levels: green (0-30), yellow (31-50), orange (51-70), red (71-100).

    Args:
        employee_id: ID of the employee to assess
        lookback_weeks: Number of weeks to analyze (default 4)

    Returns:
        FatigueAssessmentResponse with detailed scores and recommendations
    """
    predictor = _get_predictor()
    assessment = predictor.assess_fatigue(employee_id, lookback_weeks=lookback_weeks)

    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No fatigue data available for employee {employee_id}"
        )

    return FatigueAssessmentResponse(
        employee_id=assessment.employee_id,
        employee_name=assessment.employee_name,
        overall_score=assessment.overall_score,
        risk_level=assessment.risk_level,
        factor_scores=assessment.factor_scores,
        consecutive_days_current=assessment.consecutive_days_current,
        avg_weekly_hours_4w=assessment.avg_weekly_hours_4w,
        clopening_count_4w=assessment.clopening_count_4w,
        weekend_ratio=assessment.weekend_ratio,
        overtime_hours_4w=assessment.overtime_hours_4w,
        days_since_last_leave=assessment.days_since_last_leave,
        recommendations=assessment.recommendations,
        trend=assessment.trend,
    )


@router.get("/venues/{venue_id}/team-fatigue", response_model=TeamFatigueReportResponse)
async def get_team_fatigue(
    venue_id: str,
    current_user: UserContext = Depends(get_current_user),
):
    """
    Get team-wide fatigue report for a venue.

    Assesses all employees at a venue and provides:
    - Individual scores sorted by risk
    - Team averages and high-risk counts
    - Team-wide recommendations

    Args:
        venue_id: ID of the venue

    Returns:
        TeamFatigueReportResponse with all employees and team metrics
    """
    predictor = _get_predictor()
    report = predictor.assess_team(venue_id)

    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No fatigue data available for venue {venue_id}"
        )

    # Build risk distribution
    risk_dist = {}
    for assessment in report.employees:
        level = assessment.risk_level
        risk_dist[level] = risk_dist.get(level, 0) + 1

    # Convert assessments to response items
    employee_items = [
        EmployeeFatigueItem(
            employee_id=a.employee_id,
            employee_name=a.employee_name,
            overall_score=a.overall_score,
            risk_level=a.risk_level,
            trend=a.trend,
            top_concern=_get_top_concern(a.factor_scores),
        )
        for a in report.employees
    ]

    return TeamFatigueReportResponse(
        venue_id=report.venue_id,
        assessed_count=report.assessed_count,
        avg_score=report.avg_score,
        high_risk_count=report.high_risk_count,
        risk_distribution=risk_dist,
        employees=employee_items,
        team_recommendations=report.team_recommendations,
    )


@router.get("/employees/{employee_id}/clopenings", response_model=ClopeningListResponse)
async def get_clopenings(
    employee_id: str,
    weeks: int = Query(4, ge=1, le=12, description="Weeks to analyze (1-12)"),
    current_user: UserContext = Depends(get_current_user),
):
    """
    Get late-to-early shift pairs (clopenings) for an employee.

    A clopening is a late shift (e.g., closing at 10pm) followed by an early
    shift the next day (e.g., opening at 6am) with <10 hours between them.

    Args:
        employee_id: ID of the employee
        weeks: Number of weeks to look back (default 4)

    Returns:
        ClopeningListResponse with all clopening pairs
    """
    db = get_db()
    predictor = _get_predictor()

    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found"
        )

    clopening_pairs = predictor.get_clopening_shifts(employee_id, weeks=weeks)

    # Convert to response format
    clopenings = []
    for late_shift, early_shift in clopening_pairs:
        gap_hours = (
            (
                int(early_shift.start_time.hour * 60 + early_shift.start_time.minute)
                - int(late_shift.end_time.hour * 60 + late_shift.end_time.minute)
            ) / 60.0
        )
        if gap_hours < 0:
            gap_hours += 24

        clopenings.append(
            ClopeningShiftPair(
                late_shift_date=late_shift.date,
                late_shift_end_time=late_shift.end_time.isoformat(),
                early_shift_date=early_shift.date,
                early_shift_start_time=early_shift.start_time.isoformat(),
                gap_hours=round(gap_hours, 1),
            )
        )

    return ClopeningListResponse(
        employee_id=employee_id,
        employee_name=employee.name,
        clopening_count=len(clopenings),
        clopenings=clopenings,
    )


@router.get("/employees/{employee_id}/burnout-prediction", response_model=BurnoutPredictionResponse)
async def get_burnout_prediction(
    employee_id: str,
    current_user: UserContext = Depends(get_current_user),
):
    """
    Predict when an employee might reach critical burnout.

    Uses historical fatigue trends to extrapolate when the employee's
    fatigue score will reach critical (71+) threshold.

    Only provides prediction if trend is worsening.

    Args:
        employee_id: ID of the employee

    Returns:
        BurnoutPredictionResponse with estimated critical date
    """
    db = get_db()
    predictor = _get_predictor()

    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found"
        )

    assessment = predictor.assess_fatigue(employee_id)
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No fatigue data available for employee {employee_id}"
        )

    predicted_date = predictor.predict_burnout_date(employee_id)

    recommendation = "Continue monitoring fatigue levels"
    if assessment.risk_level == "red":
        recommendation = "URGENT: Critical burnout risk - immediate intervention needed"
    elif assessment.risk_level == "orange":
        recommendation = "Elevated risk - implement recovery measures"
    elif assessment.trend == "worsening":
        recommendation = "Trend worsening - monitor closely and plan recovery"

    days_to_critical = None
    if predicted_date:
        days_to_critical = (predicted_date - date.today()).days

    return BurnoutPredictionResponse(
        employee_id=employee_id,
        employee_name=employee.name,
        current_fatigue_score=assessment.overall_score,
        current_risk_level=assessment.risk_level,
        trend=assessment.trend,
        predicted_critical_date=predicted_date,
        days_to_critical=days_to_critical,
        recommendation=recommendation,
    )


@router.get("/employees/{employee_id}/recovery-plan", response_model=RecoveryRosterResponse)
async def get_recovery_plan(
    employee_id: str,
    current_user: UserContext = Depends(get_current_user),
):
    """
    Get suggested recovery roster for next week.

    Recommends:
    - Reduced hours (25% less than current average)
    - No closing shifts
    - At least one full day off
    - Spread shifts to avoid consecutive days

    Args:
        employee_id: ID of the employee

    Returns:
        RecoveryRosterResponse with recovery suggestions
    """
    db = get_db()
    predictor = _get_predictor()

    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found"
        )

    suggestions = predictor.suggest_recovery_roster(employee_id)

    if not suggestions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No recovery data available for employee {employee_id}"
        )

    return RecoveryRosterResponse(
        employee_id=suggestions["employee_id"],
        recommended_total_hours=suggestions["recommended_total_hours"],
        recommended_shifts=suggestions["recommended_shifts"],
        avoid_patterns=suggestions["avoid_patterns"],
        days_off=suggestions["days_off"],
        ideal_shift_timing=suggestions["ideal_shift_timing"],
        reason=suggestions["reason"],
    )
