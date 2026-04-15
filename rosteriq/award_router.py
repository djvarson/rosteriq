"""
Award Engine Router for RosterIQ

Provides API endpoints for:
- Award rule evaluation
- Roster compliance checking
- Wage cost breakdown

Uses the award_engine.py module to evaluate shifts and rosters against
Australian Hospitality Industry Award 2020 (MA000009) rules.
"""

from datetime import datetime, date, time
from decimal import Decimal
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.award_engine import (
    AwardEngine, EmploymentType, ShiftClassification, ComplianceWarning
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)

# ── Router Setup ───────────────────────────────────────────────────────────
router = APIRouter(prefix="/api/v1/award", tags=["award"])

# Initialize engine (shared across requests)
_engine = AwardEngine(award_year=2025)


# ── Pydantic Models ────────────────────────────────────────────────────────

class ShiftInput(BaseModel):
    """Single shift in a roster evaluation request."""
    employee_id: str
    role: int = Field(..., description="Award level (1-6)")
    shift_start: str = Field(..., description="ISO 8601 datetime")
    shift_end: str = Field(..., description="ISO 8601 datetime")
    hourly_rate: Optional[float] = Field(None, description="Optional explicit rate (overrides award level)")
    employment_type: str = Field("full_time", description="full_time, part_time, casual, junior")
    age: Optional[int] = Field(None, description="Age of worker (for junior rates)")


class AwardEvaluateRequest(BaseModel):
    """Request body for POST /api/v1/award/evaluate"""
    venue_id: str
    roster: List[ShiftInput]


class PenaltyBreakdown(BaseModel):
    """Penalty rate applied to a shift."""
    employee_id: str
    shift_date: str
    shift_start: str
    shift_end: str
    loading_type: str  # e.g., "saturday", "sunday", "public_holiday", "evening"
    loading_percent: float
    base_cost: float
    loading_cost: float


class ComplianceIssue(BaseModel):
    """Compliance violation in roster."""
    severity: str  # "error", "warning", "info"
    rule: str
    detail: str
    employee_id: str


class AwardEvaluateResponse(BaseModel):
    """Response from POST /api/v1/award/evaluate"""
    total_base_cost: float = Field(..., description="Base cost without penalties")
    total_loading_cost: float = Field(..., description="Additional cost from penalties/loadings")
    total_cost: float = Field(..., description="Base + loading cost")
    penalty_breakdown: List[PenaltyBreakdown] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    compliance_issues: List[ComplianceIssue] = Field(default_factory=list)


class AwardRule(BaseModel):
    """Single award rule currently enforced."""
    name: str
    description: str
    applies_to: str  # e.g., "all", "casual", "full_time"


class AwardRulesResponse(BaseModel):
    """Response from GET /api/v1/award/rules"""
    rules: List[AwardRule]
    description: str


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.post("/evaluate", response_model=AwardEvaluateResponse)
async def evaluate_roster(req: AwardEvaluateRequest, request: Request) -> AwardEvaluateResponse:
    """
    Evaluate a roster against Australian Hospitality Award rules.

    Takes a draft roster (list of shifts), calculates total wage cost including
    penalty rates, and returns compliance warnings.

    Returns:
    - total_base_cost: Cost if all shifts were ordinary time
    - total_loading_cost: Additional cost from Saturday/Sunday/PH/evening penalties
    - total_cost: Base + loading
    - penalty_breakdown: Per-shift loading details
    - warnings: Text warnings (e.g., "Shift exceeds 12 hours")
    - compliance_issues: Compliance rule violations
    """
    await _gate(request, "L2_ROSTER_MAKER")

    total_base_cost = Decimal("0")
    total_loading_cost = Decimal("0")
    penalty_breakdown: List[PenaltyBreakdown] = []
    warnings: List[str] = []
    compliance_issues: List[ComplianceIssue] = []

    # Parse and evaluate each shift
    for shift in req.roster:
        try:
            # Parse ISO 8601 datetime strings
            shift_start_dt = datetime.fromisoformat(shift.shift_start)
            shift_end_dt = datetime.fromisoformat(shift.shift_end)

            shift_date = shift_start_dt.date()
            start_time = shift_start_dt.time()
            end_time = shift_end_dt.time()

            # Determine employment type
            try:
                emp_type = EmploymentType[shift.employment_type.upper()]
            except KeyError:
                emp_type = EmploymentType.FULL_TIME

            # Check if date is public holiday
            is_public_holiday = _engine.is_public_holiday(shift_date)

            # Calculate shift cost using award engine
            calc = _engine.calculate_shift_cost(
                employee_id=shift.employee_id,
                award_level=shift.role,
                employment_type=emp_type,
                shift_date=shift_date,
                start_time=start_time,
                end_time=end_time,
                is_public_holiday=is_public_holiday,
                age=shift.age
            )

            # Accumulate costs
            # Base cost is hours * base_rate
            base_cost = calc.base_hours * calc.base_rate
            loading_cost = calc.gross_pay - base_cost

            total_base_cost += base_cost
            total_loading_cost += loading_cost

            # Determine loading type for display
            loading_type = "ordinary"
            if is_public_holiday:
                loading_type = "public_holiday"
            elif shift_date.weekday() == 6:
                loading_type = "sunday"
            elif shift_date.weekday() == 5:
                loading_type = "saturday"
            elif start_time.hour >= 19:
                loading_type = "evening"

            # Calculate loading percentage
            loading_percent = 0.0
            if loading_type == "ordinary":
                loading_percent = 0.0
            elif loading_type == "public_holiday":
                loading_percent = 125.0 if emp_type == EmploymentType.CASUAL else 125.0
            elif loading_type == "sunday":
                loading_percent = 75.0 if emp_type == EmploymentType.CASUAL else 50.0
            elif loading_type == "saturday":
                loading_percent = 50.0 if emp_type == EmploymentType.CASUAL else 25.0
            elif loading_type == "evening":
                loading_percent = 15.0

            penalty_breakdown.append(PenaltyBreakdown(
                employee_id=shift.employee_id,
                shift_date=shift_date.isoformat(),
                shift_start=shift_start_dt.isoformat(),
                shift_end=shift_end_dt.isoformat(),
                loading_type=loading_type,
                loading_percent=loading_percent,
                base_cost=float(base_cost),
                loading_cost=float(loading_cost)
            ))

            # Add shift warnings
            warnings.extend(calc.warnings)

        except Exception as e:
            warnings.append(f"Error evaluating shift for {shift.employee_id}: {str(e)}")

    # Check compliance for all shifts (grouped by employee)
    shifts_by_employee: Dict[str, List[tuple]] = {}
    for shift in req.roster:
        try:
            shift_start_dt = datetime.fromisoformat(shift.shift_start)
            shift_end_dt = datetime.fromisoformat(shift.shift_end)

            if shift.employee_id not in shifts_by_employee:
                shifts_by_employee[shift.employee_id] = []

            shifts_by_employee[shift.employee_id].append(
                (shift_start_dt.date(), shift_start_dt.time(), shift_end_dt.time())
            )
        except Exception:
            pass

    # Run compliance checks per employee
    for emp_id, shifts_list in shifts_by_employee.items():
        try:
            emp_type = EmploymentType.FULL_TIME
            age = None

            # Find employment type and age from roster
            for shift in req.roster:
                if shift.employee_id == emp_id:
                    try:
                        emp_type = EmploymentType[shift.employment_type.upper()]
                    except KeyError:
                        pass
                    age = shift.age
                    break

            compliance_warnings = _engine.check_compliance(
                employee_id=emp_id,
                shifts=shifts_list,
                employment_type=emp_type,
                age=age
            )

            for comp_warn in compliance_warnings:
                compliance_issues.append(ComplianceIssue(
                    severity=comp_warn.severity,
                    rule=comp_warn.rule,
                    detail=comp_warn.message,
                    employee_id=comp_warn.employee_id
                ))
        except Exception as e:
            warnings.append(f"Compliance check error for {emp_id}: {str(e)}")

    total_cost = total_base_cost + total_loading_cost

    return AwardEvaluateResponse(
        total_base_cost=float(total_base_cost),
        total_loading_cost=float(total_loading_cost),
        total_cost=float(total_cost),
        penalty_breakdown=penalty_breakdown,
        warnings=warnings,
        compliance_issues=compliance_issues
    )


@router.get("/rules", response_model=AwardRulesResponse)
async def get_award_rules() -> AwardRulesResponse:
    """
    Get a summary of award rules currently enforced by the engine.

    Useful for the dashboard to show a "verified against" badge.

    Public endpoint — award rules are reference data, no auth required.
    """

    rules = [
        AwardRule(
            name="base_rates",
            description="Hospitality Industry Award 2020 base rates (Levels 1-6)",
            applies_to="all"
        ),
        AwardRule(
            name="saturday_penalty",
            description="Saturday work: 125% for full-time/part-time, 150% for casual",
            applies_to="all"
        ),
        AwardRule(
            name="sunday_penalty",
            description="Sunday work: 150% for full-time/part-time, 175% for casual",
            applies_to="all"
        ),
        AwardRule(
            name="public_holiday_penalty",
            description="Public holiday work: 225% for full-time/part-time, 250% for casual",
            applies_to="all"
        ),
        AwardRule(
            name="evening_loading",
            description="Evening work (after 7pm): 115% on weekdays",
            applies_to="all"
        ),
        AwardRule(
            name="casual_loading",
            description="Casual employment: 25% base loading on ordinary rates",
            applies_to="casual"
        ),
        AwardRule(
            name="junior_rates",
            description="Junior minimum wages: age-based percentages (16-18 years)",
            applies_to="junior"
        ),
        AwardRule(
            name="overtime",
            description="Overtime: 150% for first 2 hours over 38/week, 200% thereafter",
            applies_to="full_time"
        ),
        AwardRule(
            name="min_break_between_shifts",
            description="Minimum 11-hour break between consecutive shifts",
            applies_to="all"
        ),
        AwardRule(
            name="max_consecutive_work_days",
            description="Maximum 6 consecutive days of work",
            applies_to="all"
        ),
        AwardRule(
            name="junior_hour_restriction",
            description="Junior workers (under 18): maximum 30 hours per week",
            applies_to="junior"
        ),
    ]

    return AwardRulesResponse(
        rules=rules,
        description="Hospitality Industry (General) Award 2020 (MA000009) effective 1 July 2025"
    )
