"""
Employee costing and labour cost projection routes for RosterIQ.

REST endpoints for:
- Individual employee cost projections
- Employee cost comparisons
- Cheapest employee selection for shifts
- Annual cost estimates
- Detailed roster labour cost breakdowns
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    State,
)
from rosteriq.services.employee_costing import (
    EmployeeCostingService,
    EmployeeCostProjection,
    RosterCostSummary,
    ShiftCostDetail,
)
from rosteriq.database import get_db


# ============================================================================
# SETUP
# ============================================================================

router = APIRouter(prefix="/api/v1/costing", tags=["costing"])
service = EmployeeCostingService()


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class ShiftCostDetailJSON(BaseModel):
    """JSON representation of shift cost detail."""
    shift_id: str
    date: str
    start_time: str
    end_time: str
    hours: str
    base_pay: str
    penalty_pay: str
    casual_loading: str
    super_contribution: str
    workcover_component: str
    leave_accrual: str
    total_cost: str
    effective_hourly_rate: str
    penalty_multiplier: str

    @staticmethod
    def from_detail(detail: ShiftCostDetail) -> "ShiftCostDetailJSON":
        return ShiftCostDetailJSON(
            shift_id=detail.shift_id,
            date=detail.date.isoformat(),
            start_time=detail.start_time.isoformat() if detail.start_time else None,
            end_time=detail.end_time.isoformat() if detail.end_time else None,
            hours=str(detail.hours),
            base_pay=str(detail.base_pay),
            penalty_pay=str(detail.penalty_pay),
            casual_loading=str(detail.casual_loading),
            super_contribution=str(detail.super_contribution),
            workcover_component=str(detail.workcover_component),
            leave_accrual=str(detail.leave_accrual),
            total_cost=str(detail.total_cost),
            effective_hourly_rate=str(detail.effective_hourly_rate),
            penalty_multiplier=str(detail.penalty_multiplier),
        )


class EmployeeCostProjectionJSON(BaseModel):
    """JSON representation of employee cost projection."""
    employee_id: str
    employee_name: str
    employment_type: str
    state: str
    total_hours: str
    total_shifts: int
    base_pay: str
    penalty_pay: str
    casual_loading: str
    super_contribution: str
    workcover_levy: str
    payroll_tax_component: str
    leave_accrual: str
    total_cost: str
    effective_hourly_rate: str
    warnings: List[str]
    per_shift_breakdown: List[ShiftCostDetailJSON]

    @staticmethod
    def from_projection(proj: EmployeeCostProjection) -> "EmployeeCostProjectionJSON":
        return EmployeeCostProjectionJSON(
            employee_id=proj.employee_id,
            employee_name=proj.employee_name,
            employment_type=proj.employment_type.value,
            state=proj.state.value,
            total_hours=str(proj.total_hours),
            total_shifts=proj.total_shifts,
            base_pay=str(proj.base_pay),
            penalty_pay=str(proj.penalty_pay),
            casual_loading=str(proj.casual_loading),
            super_contribution=str(proj.super_contribution),
            workcover_levy=str(proj.workcover_levy),
            payroll_tax_component=str(proj.payroll_tax_component),
            leave_accrual=str(proj.leave_accrual),
            total_cost=str(proj.total_cost),
            effective_hourly_rate=str(proj.effective_hourly_rate),
            warnings=proj.warnings,
            per_shift_breakdown=[
                ShiftCostDetailJSON.from_detail(d)
                for d in proj.per_shift_breakdown
            ],
        )


class RosterCostSummaryJSON(BaseModel):
    """JSON representation of roster cost summary."""
    roster_id: str
    week_start: str
    week_end: str
    total_hours: str
    total_shifts: int
    total_employees: int
    total_base_pay: str
    total_penalty_pay: str
    total_casual_loading: str
    total_super_contribution: str
    total_workcover_levy: str
    total_payroll_tax: str
    total_leave_accrual: str
    total_cost: str
    average_hourly_rate: str
    warnings: List[str]
    employee_costs: List[EmployeeCostProjectionJSON]

    @staticmethod
    def from_summary(summary: RosterCostSummary) -> "RosterCostSummaryJSON":
        return RosterCostSummaryJSON(
            roster_id=summary.roster_id,
            week_start=summary.week_start.isoformat(),
            week_end=summary.week_end.isoformat(),
            total_hours=str(summary.total_hours),
            total_shifts=summary.total_shifts,
            total_employees=summary.total_employees,
            total_base_pay=str(summary.total_base_pay),
            total_penalty_pay=str(summary.total_penalty_pay),
            total_casual_loading=str(summary.total_casual_loading),
            total_super_contribution=str(summary.total_super_contribution),
            total_workcover_levy=str(summary.total_workcover_levy),
            total_payroll_tax=str(summary.total_payroll_tax),
            total_leave_accrual=str(summary.total_leave_accrual),
            total_cost=str(summary.total_cost),
            average_hourly_rate=str(summary.average_hourly_rate),
            warnings=summary.warnings,
            employee_costs=[
                EmployeeCostProjectionJSON.from_projection(ec)
                for ec in summary.employee_costs
            ],
        )


class CompareEmployeesRequest(BaseModel):
    """Request to compare costs across multiple employees."""
    employee_ids: List[str]
    shift_ids: List[str]
    venue_annual_payroll: Optional[str] = None


class FindCheapestRequest(BaseModel):
    """Request to find cheapest employee for a shift."""
    eligible_employee_ids: List[str]
    shift_id: str
    venue_annual_payroll: Optional[str] = None


class AnnualCostRequest(BaseModel):
    """Request to estimate annual cost."""
    avg_weekly_hours: float
    weeks_per_year: int = 52
    venue_annual_payroll: Optional[str] = None


class RosterCostRequest(BaseModel):
    """Request to calculate roster labour cost."""
    shift_ids: List[str]
    venue_annual_payroll: Optional[str] = None


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.get("/employees/{employee_id}/cost-projection")
async def get_employee_cost_projection(
    employee_id: str,
    days_ahead: int = Query(7, ge=1, le=365, description="Number of days to project"),
    venue_annual_payroll: Optional[str] = Query(None, description="Annual payroll for tax calc"),
) -> EmployeeCostProjectionJSON:
    """
    Project cost for an employee's upcoming shifts.

    Args:
        employee_id: The employee ID
        days_ahead: Number of days to look ahead (default 7)
        venue_annual_payroll: Venue's annual payroll estimate

    Returns:
        EmployeeCostProjectionJSON with detailed breakdown
    """
    db = get_db()

    # Fetch employee
    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")

    # Fetch upcoming shifts
    today = date.today()
    future_date = today + timedelta(days=days_ahead)
    shifts = db.get_shifts_for_employee_in_period(employee_id, today, future_date)

    if not shifts:
        raise HTTPException(
            status_code=404,
            detail=f"No shifts found for {employee_id} in next {days_ahead} days"
        )

    # Parse payroll if provided
    venue_payroll = None
    if venue_annual_payroll:
        try:
            venue_payroll = Decimal(venue_annual_payroll)
        except (ValueError, TypeError, ArithmeticError):
            raise HTTPException(status_code=400, detail="Invalid venue_annual_payroll")

    # Calculate projection
    projection = service.project_cost(employee, shifts, venue_payroll)

    return EmployeeCostProjectionJSON.from_projection(projection)


@router.post("/compare")
async def compare_employees(request: CompareEmployeesRequest) -> List[EmployeeCostProjectionJSON]:
    """
    Compare cost of assigning the same shifts to different employees.

    Args:
        request: CompareEmployeesRequest with employee_ids and shift_ids

    Returns:
        List of cost projections sorted by total cost (cheapest first)
    """
    db = get_db()

    # Fetch employees
    employees = []
    for emp_id in request.employee_ids:
        emp = db.get_employee(emp_id)
        if not emp:
            raise HTTPException(status_code=404, detail=f"Employee {emp_id} not found")
        employees.append(emp)

    # Fetch shifts
    shifts = []
    for shift_id in request.shift_ids:
        shift = db.get_shift(shift_id)
        if not shift:
            raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")
        shifts.append(shift)

    if not shifts:
        raise HTTPException(status_code=400, detail="No shifts provided")

    # Parse payroll
    venue_payroll = None
    if request.venue_annual_payroll:
        try:
            venue_payroll = Decimal(request.venue_annual_payroll)
        except (ValueError, TypeError, ArithmeticError):
            raise HTTPException(status_code=400, detail="Invalid venue_annual_payroll")

    # Compare
    projections = service.compare_employees(employees, shifts, venue_payroll)

    return [EmployeeCostProjectionJSON.from_projection(p) for p in projections]


@router.post("/cheapest")
async def find_cheapest_employee(request: FindCheapestRequest) -> List[dict]:
    """
    Find the cheapest employee option for a shift.

    Args:
        request: FindCheapestRequest with eligible_employee_ids and shift_id

    Returns:
        List of (employee, cost) tuples sorted by cost
    """
    db = get_db()

    # Fetch employees
    employees = []
    for emp_id in request.eligible_employee_ids:
        emp = db.get_employee(emp_id)
        if not emp:
            raise HTTPException(status_code=404, detail=f"Employee {emp_id} not found")
        employees.append(emp)

    # Fetch shift
    shift = db.get_shift(request.shift_id)
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {request.shift_id} not found")

    # Parse payroll
    venue_payroll = None
    if request.venue_annual_payroll:
        try:
            venue_payroll = Decimal(request.venue_annual_payroll)
        except (ValueError, TypeError, ArithmeticError):
            raise HTTPException(status_code=400, detail="Invalid venue_annual_payroll")

    # Find cheapest
    results = service.find_cheapest_option(employees, shift, venue_payroll)

    return [
        {
            "employee_id": emp.id,
            "employee_name": emp.name,
            "employment_type": emp.employment_type.value,
            "total_cost": str(cost),
        }
        for emp, cost in results
    ]


@router.get("/employees/{employee_id}/annual-cost")
async def get_annual_cost_estimate(
    employee_id: str,
    avg_weekly_hours: float = Query(38.0, ge=1, le=60),
    weeks_per_year: int = Query(52, ge=1, le=52),
    venue_annual_payroll: Optional[str] = Query(None),
) -> dict:
    """
    Project annual employment cost based on average weekly hours.

    Args:
        employee_id: The employee ID
        avg_weekly_hours: Average hours per week (default 38)
        weeks_per_year: Weeks worked per year (default 52)
        venue_annual_payroll: Venue's annual payroll estimate

    Returns:
        Dict with annual cost breakdown
    """
    db = get_db()

    # Fetch employee
    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")

    # Parse payroll
    venue_payroll = None
    if venue_annual_payroll:
        try:
            venue_payroll = Decimal(venue_annual_payroll)
        except (ValueError, TypeError, ArithmeticError):
            raise HTTPException(status_code=400, detail="Invalid venue_annual_payroll")

    # Calculate
    annual_estimate = service.annual_cost_estimate(
        employee, avg_weekly_hours, weeks_per_year, venue_payroll
    )

    # Convert Decimals to strings for JSON
    return {
        key: str(value)
        for key, value in annual_estimate.items()
    }


@router.post("/rosters/{roster_id}/labour-cost")
async def calculate_roster_labour_cost(
    roster_id: str,
    request: RosterCostRequest,
) -> RosterCostSummaryJSON:
    """
    Calculate detailed roster labour cost breakdown.

    Args:
        roster_id: The roster ID
        request: RosterCostRequest with shift_ids

    Returns:
        RosterCostSummaryJSON with comprehensive breakdown
    """
    db = get_db()

    # Fetch roster
    roster = db.get_roster(roster_id)
    if not roster:
        raise HTTPException(status_code=404, detail=f"Roster {roster_id} not found")

    # Fetch shifts
    roster_shifts = []
    for shift_id in request.shift_ids:
        shift = db.get_shift(shift_id)
        if not shift:
            raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")

        # Fetch employee for the shift
        employee = db.get_employee(shift.employee_id)
        if not employee:
            raise HTTPException(
                status_code=404,
                detail=f"Employee {shift.employee_id} for shift {shift_id} not found"
            )

        roster_shifts.append((employee, shift))

    # Parse payroll
    venue_payroll = None
    if request.venue_annual_payroll:
        try:
            venue_payroll = Decimal(request.venue_annual_payroll)
        except (ValueError, TypeError, ArithmeticError):
            raise HTTPException(status_code=400, detail="Invalid venue_annual_payroll")

    # Calculate
    summary = service.calculate_roster_labour_cost(roster_shifts, venue_payroll)
    summary.roster_id = roster_id

    return RosterCostSummaryJSON.from_summary(summary)


@router.get("/health")
async def health_check() -> dict:
    """Health check endpoint for costing service."""
    return {"status": "ok", "service": "employee_costing"}
