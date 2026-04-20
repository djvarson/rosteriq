"""Fatigue Manager Router — REST endpoints for fatigue management.

Provides:
- GET /api/v1/fatigue/{venue_id}/assess/{employee_id} — individual fatigue assessment
- GET /api/v1/fatigue/{venue_id}/roster-check — bulk roster fatigue check
- POST /api/v1/fatigue/would-exceed — pre-check if proposed shift would exceed limits
- GET /api/v1/fatigue/{venue_id}/alerts — fatigue alerts (filterable by risk_level, date_from)
- GET /api/v1/fatigue/{venue_id}/high-risk — employees currently at HIGH/CRITICAL risk

All data endpoints require auth level L1_SUPERVISOR or higher.
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

try:
    from fastapi import APIRouter, HTTPException, status, Query, Request
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    status = None
    Query = None
    BaseModel = None
    Field = None
    Request = None

from rosteriq import fatigue_manager as fm

# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(prefix="/api/v1/fatigue", tags=["fatigue"])


# ============================================================================
# Pydantic Models
# ============================================================================


class FatigueRiskLevelEnum(str):
    """Fatigue risk level enum."""
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class ShiftInput(BaseModel):
    """Shift data for fatigue assessment."""
    date: str = Field(..., description="Shift date (ISO 8601 YYYY-MM-DD)")
    start: str = Field(..., description="Start time (HH:MM)")
    end: str = Field(..., description="End time (HH:MM)")
    employee_id: Optional[str] = Field(None, description="Employee ID")
    employee_name: Optional[str] = Field(None, description="Employee name")


class FatigueAssessmentResponse(BaseModel):
    """Fatigue assessment response."""
    employee_id: str
    employee_name: str
    venue_id: str
    assessment_date: str
    risk_level: str
    weekly_hours: float
    fortnightly_hours: float
    consecutive_days: int
    last_day_off: Optional[str]
    night_shift_count: int
    violations: List[str]
    recommendations: List[str]
    score: int


class FatigueAlertResponse(BaseModel):
    """Fatigue alert response."""
    alert_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    risk_level: str
    trigger: str
    hours_worked: float
    created_at: str
    acknowledged_at: Optional[str]


class WouldExceedRequest(BaseModel):
    """Request for would-exceed pre-check."""
    employee_id: str = Field(..., description="Employee ID")
    employee_name: str = Field(..., description="Employee name")
    venue_id: str = Field(..., description="Venue ID")
    proposed_shift: ShiftInput = Field(..., description="Proposed shift")
    existing_shifts: List[ShiftInput] = Field(
        ..., description="List of existing shifts in current window"
    )


class WouldExceedResponse(BaseModel):
    """Response from would-exceed check."""
    would_exceed: bool
    reasons: List[str]


class RosterCheckRequest(BaseModel):
    """Request for roster-wide fatigue check."""
    venue_id: str = Field(..., description="Venue ID")
    shifts_by_employee: Dict[str, List[ShiftInput]] = Field(
        ..., description="Shifts organized by employee_id"
    )


class RosterCheckResponse(BaseModel):
    """Response from roster check."""
    venue_id: str
    checked_at: str
    total_employees: int
    at_risk_count: int
    assessments: List[FatigueAssessmentResponse]


class HighRiskEmployeesResponse(BaseModel):
    """Response for high-risk employees."""
    venue_id: str
    checked_at: str
    high_risk_count: int
    critical_count: int
    employees: List[FatigueAssessmentResponse]


# ============================================================================
# Helper Functions
# ============================================================================


def _convert_assessment_to_response(a: fm.FatigueAssessment) -> FatigueAssessmentResponse:
    """Convert FatigueAssessment to API response."""
    return FatigueAssessmentResponse(
        employee_id=a.employee_id,
        employee_name=a.employee_name,
        venue_id=a.venue_id,
        assessment_date=a.assessment_date.isoformat(),
        risk_level=a.risk_level.value,
        weekly_hours=a.weekly_hours,
        fortnightly_hours=a.fortnightly_hours,
        consecutive_days=a.consecutive_days,
        last_day_off=a.last_day_off.isoformat() if a.last_day_off else None,
        night_shift_count=a.night_shift_count,
        violations=a.violations,
        recommendations=a.recommendations,
        score=a.score,
    )


def _convert_alert_to_response(a: fm.FatigueAlert) -> FatigueAlertResponse:
    """Convert FatigueAlert to API response."""
    return FatigueAlertResponse(
        alert_id=a.alert_id,
        employee_id=a.employee_id,
        employee_name=a.employee_name,
        venue_id=a.venue_id,
        risk_level=a.risk_level.value,
        trigger=a.trigger,
        hours_worked=a.hours_worked,
        created_at=a.created_at.isoformat(),
        acknowledged_at=a.acknowledged_at.isoformat() if a.acknowledged_at else None,
    )


def _shift_input_to_dict(shift: ShiftInput) -> Dict[str, Any]:
    """Convert ShiftInput to dict for processing."""
    return {
        "date": shift.date,
        "start": shift.start,
        "end": shift.end,
        "employee_id": shift.employee_id,
        "employee_name": shift.employee_name,
    }


# ============================================================================
# Endpoints
# ============================================================================


@router.get(
    "/{venue_id}/assess/{employee_id}",
    response_model=FatigueAssessmentResponse,
    summary="Assess fatigue for an employee",
    description="Get comprehensive fatigue risk assessment for a single employee",
)
def get_employee_fatigue_assessment(
    venue_id: str,
    employee_id: str,
    shifts_7_days: Optional[str] = Query(None, description="JSON list of shifts (last 7 days)"),
    shifts_14_days: Optional[str] = Query(None, description="JSON list of shifts (last 14 days)"),
) -> FatigueAssessmentResponse:
    """Get fatigue assessment for an employee.

    Requires shifts data in JSON format:
    [{"date": "2026-04-20", "start": "09:00", "end": "17:00"}, ...]
    """
    # Parse shifts from query params (JSON strings)
    import json

    shifts_7 = []
    shifts_14 = []

    if shifts_7_days:
        try:
            shifts_7 = json.loads(shifts_7_days)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in shifts_7_days")

    if shifts_14_days:
        try:
            shifts_14 = json.loads(shifts_14_days)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in shifts_14_days")

    if not shifts_7 and not shifts_14:
        # Return a baseline assessment with no shifts
        assessment = fm.assess_fatigue(
            employee_id=employee_id,
            employee_name=employee_id,
            venue_id=venue_id,
            shifts_7_days=[],
            shifts_14_days=[],
        )
    else:
        assessment = fm.assess_fatigue(
            employee_id=employee_id,
            employee_name=employee_id,
            venue_id=venue_id,
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_14,
        )

    return _convert_assessment_to_response(assessment)


@router.post(
    "/{venue_id}/roster-check",
    response_model=RosterCheckResponse,
    summary="Check fatigue for entire roster",
    description="Bulk fatigue assessment for all employees in a venue",
)
def check_venue_roster_fatigue(
    venue_id: str,
    request: RosterCheckRequest,
) -> RosterCheckResponse:
    """Check fatigue for entire roster."""
    # Convert ShiftInput to dict format
    shifts_by_employee = {}
    for emp_id, shifts in request.shifts_by_employee.items():
        shifts_by_employee[emp_id] = [_shift_input_to_dict(s) for s in shifts]

    assessments = fm.check_roster_fatigue(
        venue_id=venue_id,
        all_shifts_by_employee=shifts_by_employee,
    )

    at_risk = sum(
        1 for a in assessments
        if a.risk_level in (fm.FatigueRiskLevel.HIGH, fm.FatigueRiskLevel.CRITICAL)
    )

    return RosterCheckResponse(
        venue_id=venue_id,
        checked_at=datetime.now().isoformat(),
        total_employees=len(assessments),
        at_risk_count=at_risk,
        assessments=[_convert_assessment_to_response(a) for a in assessments],
    )


@router.post(
    "/would-exceed",
    response_model=WouldExceedResponse,
    summary="Pre-check proposed shift",
    description="Check if a proposed shift would exceed fatigue limits",
)
def check_would_exceed_limits(
    request: WouldExceedRequest,
) -> WouldExceedResponse:
    """Pre-check if a proposed shift would exceed limits."""
    proposed_dict = _shift_input_to_dict(request.proposed_shift)
    existing_dicts = [_shift_input_to_dict(s) for s in request.existing_shifts]

    would_exceed, reasons = fm.would_exceed_limits(
        employee_id=request.employee_id,
        proposed_shift=proposed_dict,
        existing_shifts=existing_dicts,
    )

    return WouldExceedResponse(would_exceed=would_exceed, reasons=reasons)


@router.get(
    "/{venue_id}/alerts",
    response_model=List[FatigueAlertResponse],
    summary="Get fatigue alerts",
    description="Get fatigue alerts for a venue, optionally filtered",
)
def get_fatigue_alerts(
    venue_id: str,
    risk_level: Optional[str] = Query(None, description="Filter by risk level (low/moderate/high/critical)"),
    date_from: Optional[str] = Query(None, description="Filter alerts from date (ISO 8601)"),
) -> List[FatigueAlertResponse]:
    """Get fatigue alerts for a venue."""
    store = fm.get_fatigue_store()

    # Get all alerts for venue
    risk_filter = None
    if risk_level:
        try:
            risk_filter = fm.FatigueRiskLevel(risk_level)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid risk_level: {risk_level}. Must be one of: low, moderate, high, critical",
            )

    alerts = store.get_alerts(venue_id, risk_level=risk_filter)

    # Filter by date if provided
    if date_from:
        try:
            from_date = datetime.fromisoformat(date_from)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date_from: {date_from}")

        alerts = [a for a in alerts if a.created_at >= from_date]

    return [_convert_alert_to_response(a) for a in alerts]


@router.get(
    "/{venue_id}/high-risk",
    response_model=HighRiskEmployeesResponse,
    summary="Get high-risk employees",
    description="Get employees currently at HIGH or CRITICAL fatigue risk",
)
def get_high_risk_employees(venue_id: str) -> HighRiskEmployeesResponse:
    """Get employees at HIGH or CRITICAL risk."""
    store = fm.get_fatigue_store()
    assessments = store.get_venue_assessments(venue_id)

    high_risk = [
        a for a in assessments
        if a.risk_level == fm.FatigueRiskLevel.HIGH
    ]
    critical = [
        a for a in assessments
        if a.risk_level == fm.FatigueRiskLevel.CRITICAL
    ]

    all_at_risk = high_risk + critical

    return HighRiskEmployeesResponse(
        venue_id=venue_id,
        checked_at=datetime.now().isoformat(),
        high_risk_count=len(high_risk),
        critical_count=len(critical),
        employees=[_convert_assessment_to_response(a) for a in all_at_risk],
    )
