"""RDO Manager Router — REST endpoints for RDO management.

Provides:
- POST /api/v1/rdo/policies/{venue_id} — Create policy (OWNER)
- GET /api/v1/rdo/policies/{venue_id} — List policies (L1+)
- POST /api/v1/rdo/enrol/{venue_id}/{employee_id} — Enrol employee (L2+)
- GET /api/v1/rdo/balance/{venue_id}/{employee_id} — Get balance (L1+)
- POST /api/v1/rdo/accrue/{venue_id} — Accrue hours (L2+)
- POST /api/v1/rdo/schedule/{venue_id}/{employee_id} — Schedule RDO (L2+)
- POST /api/v1/rdo/{schedule_id}/take — Mark as taken (L2+)
- POST /api/v1/rdo/{schedule_id}/cancel — Cancel RDO (L2+)
- POST /api/v1/rdo/{schedule_id}/swap — Swap date (L2+)
- GET /api/v1/rdo/calendar/{venue_id} — Team calendar (L1+)
- GET /api/v1/rdo/upcoming/{venue_id} — Upcoming RDOs (L1+)
- GET /api/v1/rdo/eligibility/{venue_id}/{employee_id} — Check eligibility (L1+)

All endpoints follow RosterIQ auth patterns.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Query, Request, status
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    Query = None
    Request = None
    status = None
    BaseModel = None
    Field = None

from rosteriq.rdo_manager import (
    get_rdo_manager_store,
    RDOPolicy,
    RDOBalance,
    RDOSchedule,
    RDOStatus,
)

logger = logging.getLogger("rosteriq.rdo_manager_router")

# ─────────────────────────────────────────────────────────────────────────────
# Router Setup
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/v1/rdo", tags=["rdo"])

# Try to import auth
try:
    from rosteriq.auth import require_access
except ImportError:
    require_access = None


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Auth gating
# ─────────────────────────────────────────────────────────────────────────────


def _gate(request: Request, level_name: str) -> None:
    """
    Gate access based on auth level (if auth is enabled).

    Args:
        request: FastAPI Request object
        level_name: Access level name ("L1" for read, "L2" for write, "OWNER" for admin)

    Raises:
        HTTPException(403) if access is denied
    """
    if require_access:
        try:
            require_access(request, level_name)
        except Exception as e:
            logger.warning(f"Access gate failed for level {level_name}: {e}")
            raise HTTPException(status_code=403, detail="Access denied")


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Models
# ─────────────────────────────────────────────────────────────────────────────


class RDOPolicyRequest(BaseModel):
    """Request to create/update RDO policy."""
    name: str = Field(..., description="Policy name")
    cycle_days: int = Field(..., description="Accrual cycle length (14 or 28)")
    accrual_hours_per_day: float = Field(..., description="Hours accrued per day worked")
    rdo_length_hours: float = Field(default=7.6, description="Standard RDO length")
    eligible_employment_types: List[str] = Field(
        default_factory=lambda: ["FULL_TIME"],
        description="Eligible employment types"
    )
    min_service_days: int = Field(default=0, description="Minimum service days")


class RDOPolicyResponse(BaseModel):
    """RDO policy response."""
    id: str
    venue_id: str
    name: str
    cycle_days: int
    accrual_hours_per_day: float
    rdo_length_hours: float
    eligible_employment_types: List[str]
    min_service_days: int
    is_active: bool
    created_at: str


class RDOEnrolRequest(BaseModel):
    """Request to enrol employee in RDO policy."""
    policy_id: str = Field(..., description="Policy ID")
    employment_start_date: str = Field(..., description="Employment start date (YYYY-MM-DD)")
    employment_type: str = Field(default="FULL_TIME", description="Employment type")


class RDOBalanceResponse(BaseModel):
    """RDO balance response."""
    id: str
    venue_id: str
    employee_id: str
    policy_id: str
    accrued_hours: float
    taken_hours: float
    balance_hours: float
    employment_start_date: str
    employment_type: str
    last_accrual_date: Optional[str]
    updated_at: str


class RDOAccrualRequest(BaseModel):
    """Request to accrue hours."""
    employee_hours: Dict[str, float] = Field(
        ...,
        description="Map of employee_id -> hours_worked"
    )
    work_date: str = Field(..., description="Work date (YYYY-MM-DD)")


class RDOScheduleRequest(BaseModel):
    """Request to schedule RDO."""
    date: str = Field(..., description="RDO date (YYYY-MM-DD)")
    approved_by: Optional[str] = Field(None, description="Approver ID")
    notes: Optional[str] = Field(None, description="Optional notes")


class RDOScheduleResponse(BaseModel):
    """RDO schedule response."""
    id: str
    venue_id: str
    employee_id: str
    date: str
    hours: float
    status: str
    swap_date: Optional[str]
    approved_by: Optional[str]
    notes: Optional[str]
    created_at: str


class RDOSwapRequest(BaseModel):
    """Request to swap RDO date."""
    new_date: str = Field(..., description="New RDO date (YYYY-MM-DD)")


class RDOEligibilityResponse(BaseModel):
    """RDO eligibility check response."""
    eligible: bool
    employee_id: str
    reason: Optional[str] = None
    balance_hours: Optional[float] = None
    policy_name: Optional[str] = None
    current_balance: Optional[float] = None
    required: Optional[float] = None


class RDOForecastResponse(BaseModel):
    """RDO accrual forecast."""
    employee_id: str
    current_balance: float
    days_ahead: int
    taken_in_period: float
    scheduled_in_period: float
    expected_accrual: float
    forecast_balance: float


class RDOCalendarResponse(BaseModel):
    """Team RDO calendar."""
    venue_id: str
    month: int
    year: int
    first_day: str
    last_day: str
    by_employee: Dict[str, List[Dict[str, Any]]]
    total_scheduled: int


# ─────────────────────────────────────────────────────────────────────────────
# Policy endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/policies/{venue_id}", response_model=RDOPolicyResponse)
async def create_policy(
    venue_id: str,
    req: RDOPolicyRequest,
    request: Request,
) -> Dict[str, Any]:
    """Create a new RDO policy for a venue."""
    _gate(request, "OWNER")
    store = get_rdo_manager_store()
    policy = store.create_policy(
        venue_id=venue_id,
        name=req.name,
        cycle_days=req.cycle_days,
        accrual_hours_per_day=req.accrual_hours_per_day,
        rdo_length_hours=req.rdo_length_hours,
        eligible_employment_types=req.eligible_employment_types,
        min_service_days=req.min_service_days,
    )
    return policy.to_dict()


@router.get("/policies/{venue_id}", response_model=List[RDOPolicyResponse])
async def list_policies(
    venue_id: str,
    request: Request,
) -> List[Dict[str, Any]]:
    """List all RDO policies for a venue."""
    _gate(request, "L1")
    store = get_rdo_manager_store()
    policies = store.list_policies(venue_id)
    return [p.to_dict() for p in policies]


# ─────────────────────────────────────────────────────────────────────────────
# Enrolment & Balance endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/enrol/{venue_id}/{employee_id}", response_model=RDOBalanceResponse)
async def enrol_employee(
    venue_id: str,
    employee_id: str,
    req: RDOEnrolRequest,
    request: Request,
) -> Dict[str, Any]:
    """Enrol employee in an RDO policy."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        employment_start = date.fromisoformat(req.employment_start_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid employment_start_date format",
        )

    balance = store.enrol_employee(
        venue_id=venue_id,
        employee_id=employee_id,
        policy_id=req.policy_id,
        employment_start_date=employment_start,
        employment_type=req.employment_type,
    )
    return balance.to_dict()


@router.get("/balance/{venue_id}/{employee_id}", response_model=RDOBalanceResponse)
async def get_balance(
    venue_id: str,
    employee_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Get RDO balance for an employee."""
    _gate(request, "L1")
    store = get_rdo_manager_store()
    balance = store.get_balance(venue_id, employee_id)
    if not balance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Balance not found",
        )
    return balance.to_dict()


# ─────────────────────────────────────────────────────────────────────────────
# Accrual endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/accrue/{venue_id}")
async def accrue_hours(
    venue_id: str,
    req: RDOAccrualRequest,
    request: Request,
) -> Dict[str, Any]:
    """Accrue RDO hours for one or more employees."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        work_date = date.fromisoformat(req.work_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid work_date format",
        )

    results = store.bulk_accrue(venue_id, work_date, req.employee_hours)
    return {
        "venue_id": venue_id,
        "work_date": req.work_date,
        "accrued_count": len(results),
        "balances": [b.to_dict() for b in results],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Schedule endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/schedule/{venue_id}/{employee_id}",
    response_model=RDOScheduleResponse,
)
async def schedule_rdo(
    venue_id: str,
    employee_id: str,
    req: RDOScheduleRequest,
    request: Request,
) -> Dict[str, Any]:
    """Schedule an RDO for an employee."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        rdo_date = date.fromisoformat(req.date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format",
        )

    try:
        schedule = store.schedule_rdo(
            venue_id=venue_id,
            employee_id=employee_id,
            date_=rdo_date,
            approved_by=req.approved_by,
            notes=req.notes,
        )
        return schedule.to_dict()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/{schedule_id}/take", response_model=RDOScheduleResponse)
async def take_rdo(
    schedule_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Mark RDO as taken."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        schedule = store.take_rdo(schedule_id)
        return schedule.to_dict()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/{schedule_id}/cancel", response_model=RDOScheduleResponse)
async def cancel_rdo(
    schedule_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Cancel an RDO."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        schedule = store.cancel_rdo(schedule_id)
        return schedule.to_dict()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


@router.post("/{schedule_id}/swap", response_model=RDOScheduleResponse)
async def swap_rdo(
    schedule_id: str,
    req: RDOSwapRequest,
    request: Request,
) -> Dict[str, Any]:
    """Swap RDO to a new date."""
    _gate(request, "L2")
    store = get_rdo_manager_store()

    try:
        new_date = date.fromisoformat(req.new_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid new_date format",
        )

    try:
        schedule = store.swap_rdo(schedule_id, new_date)
        return schedule.to_dict()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Query endpoints
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/calendar/{venue_id}", response_model=RDOCalendarResponse)
async def get_calendar(
    venue_id: str,
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2100),
    request: Request = None,
) -> Dict[str, Any]:
    """Get team RDO calendar for a month."""
    if request:
        _gate(request, "L1")
    store = get_rdo_manager_store()
    calendar = store.get_team_rdo_calendar(venue_id, month, year)
    return calendar


@router.get("/upcoming/{venue_id}")
async def get_upcoming(
    venue_id: str,
    days_ahead: int = Query(default=28, ge=1, le=365),
    request: Request = None,
) -> Dict[str, Any]:
    """Get upcoming RDOs."""
    if request:
        _gate(request, "L1")
    store = get_rdo_manager_store()
    schedules = store.get_upcoming_rdos(venue_id, days_ahead)
    return {
        "venue_id": venue_id,
        "days_ahead": days_ahead,
        "count": len(schedules),
        "schedules": [s.to_dict() for s in schedules],
    }


@router.get("/eligibility/{venue_id}/{employee_id}", response_model=RDOEligibilityResponse)
async def check_eligibility(
    venue_id: str,
    employee_id: str,
    request: Request,
) -> Dict[str, Any]:
    """Check if employee is eligible for RDO."""
    _gate(request, "L1")
    store = get_rdo_manager_store()
    result = store.check_eligibility(venue_id, employee_id)
    return result


@router.get("/forecast/{venue_id}/{employee_id}", response_model=RDOForecastResponse)
async def get_forecast(
    venue_id: str,
    employee_id: str,
    days_ahead: int = Query(default=28, ge=1, le=365),
    request: Request = None,
) -> Dict[str, Any]:
    """Get accrual forecast for employee."""
    if request:
        _gate(request, "L1")
    store = get_rdo_manager_store()
    forecast = store.get_accrual_forecast(venue_id, employee_id, days_ahead)
    if not forecast:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Employee or balance not found",
        )
    return forecast
