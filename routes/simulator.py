"""
Roster cost simulator API routes for RosterIQ.

REST endpoints for:
- Running what-if roster scenarios
- Comparing multiple scenarios side by side
- Auto-suggesting cost reduction changes
- Simulating employee leave impact
"""

from datetime import date, time, datetime
from decimal import Decimal
from typing import List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from rosteriq.models import (
    Employee,
    Roster,
    State,
    AwardLevel,
)
from rosteriq.services.cost_simulator import (
    CostSimulator,
    SimulationResult,
    AddShift,
    RemoveShift,
    ChangeEmployee,
    ChangeTime,
    AddCasual,
    RemoveDay,
    EmployeeLeave,
    ScenarioChange,
)
from rosteriq.database import get_db


# ============================================================================
# SETUP
# ============================================================================

router = APIRouter(prefix="/api/v1/rosters", tags=["simulator"])


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================


class ShiftChangeRequest(BaseModel):
    """Base for shift-related changes."""
    type: str = Field(..., description="Change type: add_shift, remove_shift, etc.")


class AddShiftRequest(BaseModel):
    """Request to add a shift."""
    type: str = "add_shift"
    employee_id: str
    date: str
    start_time: str
    end_time: str
    role: str
    break_minutes: int = 0


class RemoveShiftRequest(BaseModel):
    """Request to remove a shift."""
    type: str = "remove_shift"
    shift_id: str


class ChangeEmployeeRequest(BaseModel):
    """Request to change employee on a shift."""
    type: str = "change_employee"
    shift_id: str
    new_employee_id: str


class ChangeTimeRequest(BaseModel):
    """Request to change shift times."""
    type: str = "change_time"
    shift_id: str
    new_start_time: str
    new_end_time: str
    break_minutes: Optional[int] = None


class AddCasualRequest(BaseModel):
    """Request to add a hypothetical casual shift."""
    type: str = "add_casual"
    date: str
    start_time: str
    end_time: str
    role: str
    award_level: str
    hourly_rate: str
    break_minutes: int = 0


class RemoveDayRequest(BaseModel):
    """Request to remove all shifts from a date."""
    type: str = "remove_day"
    date: str


class EmployeeLeaveRequest(BaseModel):
    """Request to simulate employee leave."""
    type: str = "employee_leave"
    employee_id: str
    start_date: str
    end_date: str


class SimulateRequest(BaseModel):
    """Request to simulate roster changes."""
    scenario_name: str = "What-if Scenario"
    changes: List[dict] = Field(..., description="List of scenario changes")


class CompareScenarioRequest(BaseModel):
    """Request to compare multiple scenarios."""
    scenarios: List[dict] = Field(..., description="List of {name, changes} objects")


class FindSavingsRequest(BaseModel):
    """Request to find cost reduction opportunities."""
    target_savings_pct: float = Field(..., ge=0, le=100, description="Target savings percentage")
    max_iterations: int = 10


class LeaveImpactRequest(BaseModel):
    """Request to simulate leave impact."""
    employee_id: str
    leave_dates: List[str]


class SimulationResultJSON(BaseModel):
    """JSON response for simulation result."""
    scenario_name: str
    original_cost: str
    simulated_cost: str
    cost_delta: str
    cost_delta_pct: float
    original_hours: float
    simulated_hours: float
    hours_delta: float
    original_shifts: int
    simulated_shifts: int
    shifts_delta: int
    conflicts_introduced: List[str]
    coverage_gaps: List[str]
    warnings: List[str]
    is_compliant: bool
    per_day_comparison: List[dict]

    @staticmethod
    def from_result(result: SimulationResult) -> "SimulationResultJSON":
        return SimulationResultJSON(
            scenario_name=result.scenario_name,
            original_cost=str(result.original_cost),
            simulated_cost=str(result.simulated_cost),
            cost_delta=str(result.cost_delta),
            cost_delta_pct=result.cost_delta_pct,
            original_hours=result.original_hours,
            simulated_hours=result.simulated_hours,
            hours_delta=result.hours_delta,
            original_shifts=result.original_shifts,
            simulated_shifts=result.simulated_shifts,
            shifts_delta=result.shifts_delta,
            conflicts_introduced=result.conflicts_introduced,
            coverage_gaps=result.coverage_gaps,
            warnings=result.warnings,
            is_compliant=result.is_compliant,
            per_day_comparison=[
                {
                    "date": str(dc.date),
                    "original_cost": str(dc.original_cost),
                    "simulated_cost": str(dc.simulated_cost),
                    "cost_delta": str(dc.cost_delta),
                    "original_staff_count": dc.original_staff_count,
                    "simulated_staff_count": dc.simulated_staff_count,
                    "staff_delta": dc.staff_delta,
                }
                for dc in result.per_day_comparison
            ],
        )


class FindSavingsResponse(BaseModel):
    """Response for cost savings suggestions."""
    found: bool
    savings_suggestions: Optional[List[dict]] = None
    message: str


class CompareScenarioResponse(BaseModel):
    """Response for scenario comparison."""
    scenarios: List[SimulationResultJSON]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _parse_change_request(change_dict: dict) -> ScenarioChange:
    """Parse a change dictionary into a ScenarioChange object."""
    change_type = change_dict.get("type")

    if change_type == "add_shift":
        return AddShift(
            employee_id=change_dict["employee_id"],
            shift_date=date.fromisoformat(change_dict["date"]),
            start_time=time.fromisoformat(change_dict["start_time"]),
            end_time=time.fromisoformat(change_dict["end_time"]),
            role=change_dict["role"],
            break_minutes=change_dict.get("break_minutes", 0),
        )
    elif change_type == "remove_shift":
        return RemoveShift(shift_id=change_dict["shift_id"])
    elif change_type == "change_employee":
        return ChangeEmployee(
            shift_id=change_dict["shift_id"],
            new_employee_id=change_dict["new_employee_id"],
        )
    elif change_type == "change_time":
        return ChangeTime(
            shift_id=change_dict["shift_id"],
            new_start_time=time.fromisoformat(change_dict["new_start_time"]),
            new_end_time=time.fromisoformat(change_dict["new_end_time"]),
            break_minutes=change_dict.get("break_minutes"),
        )
    elif change_type == "add_casual":
        return AddCasual(
            shift_date=date.fromisoformat(change_dict["date"]),
            start_time=time.fromisoformat(change_dict["start_time"]),
            end_time=time.fromisoformat(change_dict["end_time"]),
            role=change_dict["role"],
            award_level=AwardLevel(change_dict["award_level"]),
            hourly_rate=Decimal(change_dict["hourly_rate"]),
            state=State(change_dict.get("state", "vic")),
            break_minutes=change_dict.get("break_minutes", 0),
        )
    elif change_type == "remove_day":
        return RemoveDay(shift_date=date.fromisoformat(change_dict["date"]))
    elif change_type == "employee_leave":
        return EmployeeLeave(
            employee_id=change_dict["employee_id"],
            start_date=date.fromisoformat(change_dict["start_date"]),
            end_date=date.fromisoformat(change_dict["end_date"]),
        )
    else:
        raise ValueError(f"Unknown change type: {change_type}")


def _get_simulator(roster_id: str, db=None) -> Tuple[Roster, CostSimulator, State]:
    """
    Get a roster and initialize a simulator for it.

    Args:
        roster_id: ID of the roster to simulate
        db: Database connection (if None, uses get_db())

    Returns:
        Tuple of (Roster, CostSimulator, State)

    Raises:
        HTTPException if roster not found
    """
    if db is None:
        db = get_db()

    roster = db.get_roster(roster_id)
    if not roster:
        raise HTTPException(status_code=404, detail=f"Roster {roster_id} not found")

    # Get all employees from the database
    employees_list = db.list_employees() or []
    employees = {emp.id: emp for emp in employees_list}

    # Infer state from venue config
    venue = db.get_venue(roster.venue_id)
    state = venue.state if venue else State.vic

    simulator = CostSimulator(employees, state)
    return roster, simulator, state


# ============================================================================
# ROUTES
# ============================================================================


@router.post("/{roster_id}/simulate")
async def simulate_roster(
    roster_id: str,
    request: SimulateRequest,
) -> SimulationResultJSON:
    """
    Simulate a roster with a set of changes.

    Runs a what-if scenario against the given roster without modifying it.
    Returns detailed cost and compliance analysis.

    Args:
        roster_id: ID of the roster to simulate
        request: Scenario request with list of changes

    Returns:
        Detailed simulation result
    """
    try:
        roster, simulator, state = _get_simulator(roster_id)

        # Parse all changes
        changes = [_parse_change_request(change_dict) for change_dict in request.changes]

        # Run simulation
        result = simulator.simulate(
            roster,
            changes,
            scenario_name=request.scenario_name,
        )

        return SimulationResultJSON.from_result(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simulation error: {str(e)}")


@router.post("/{roster_id}/compare-scenarios")
async def compare_scenarios(
    roster_id: str,
    request: CompareScenarioRequest,
) -> CompareScenarioResponse:
    """
    Compare multiple what-if scenarios side by side.

    Simulates all provided scenarios and returns comparison data.

    Args:
        roster_id: ID of the roster to simulate against
        request: List of scenarios with names and changes

    Returns:
        Comparison results for all scenarios
    """
    try:
        roster, simulator, state = _get_simulator(roster_id)

        # Parse scenarios
        scenarios: List[Tuple[str, List[ScenarioChange]]] = []
        for scenario_dict in request.scenarios:
            name = scenario_dict.get("name", "Unnamed Scenario")
            changes_list = scenario_dict.get("changes", [])
            changes = [_parse_change_request(change_dict) for change_dict in changes_list]
            scenarios.append((name, changes))

        # Run comparisons
        results = simulator.compare_scenarios(roster, scenarios)

        return CompareScenarioResponse(
            scenarios=[SimulationResultJSON.from_result(result) for result in results]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison error: {str(e)}")


@router.post("/{roster_id}/find-savings")
async def find_savings(
    roster_id: str,
    request: FindSavingsRequest,
) -> FindSavingsResponse:
    """
    Auto-suggest roster changes to achieve target cost reduction.

    Uses a greedy algorithm to identify high-cost shifts that can be removed
    to reach the target savings percentage.

    Args:
        roster_id: ID of the roster to optimize
        request: Target savings percentage and max iterations

    Returns:
        List of suggested changes, or message if not found
    """
    try:
        roster, simulator, state = _get_simulator(roster_id)

        # Find savings
        suggestions = simulator.find_savings(
            roster,
            request.target_savings_pct,
            max_iterations=request.max_iterations,
        )

        if suggestions:
            # Convert to dicts
            suggestions_dicts = []
            for change in suggestions:
                if isinstance(change, RemoveShift):
                    suggestions_dicts.append({
                        "type": "remove_shift",
                        "shift_id": change.shift_id,
                    })
            return FindSavingsResponse(
                found=True,
                savings_suggestions=suggestions_dicts,
                message=f"Found {len(suggestions)} changes to achieve {request.target_savings_pct}% savings",
            )
        else:
            return FindSavingsResponse(
                found=False,
                message=f"Could not find changes to achieve {request.target_savings_pct}% savings "
                        f"within {request.max_iterations} iterations",
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Optimization error: {str(e)}")


@router.post("/{roster_id}/leave-impact")
async def leave_impact(
    roster_id: str,
    request: LeaveImpactRequest,
) -> SimulationResultJSON:
    """
    Simulate the impact of employee leave on roster costs.

    Quick shortcut for simulating what happens when an employee goes on leave.

    Args:
        roster_id: ID of the roster
        request: Employee ID and leave dates

    Returns:
        Simulation result showing leave impact
    """
    try:
        roster, simulator, state = _get_simulator(roster_id)

        # Convert string dates to date objects
        leave_dates = [date.fromisoformat(d) for d in request.leave_dates]

        # Run simulation
        result = simulator.impact_of_leave(
            roster,
            request.employee_id,
            leave_dates,
        )

        return SimulationResultJSON.from_result(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Leave impact error: {str(e)}")
