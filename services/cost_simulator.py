"""
Roster cost simulator for "what if" scenario modelling in RosterIQ.

Provides simulation and comparison of roster changes with detailed cost impact analysis.
Includes scenario generation, conflict detection, and cost reduction optimization.

All monetary values use Decimal for precision.
Complies with Fair Work Australia regulations.
"""

from decimal import Decimal, ROUND_HALF_UP
from datetime import date, time, datetime, timedelta
from typing import List, Optional, Dict, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from copy import deepcopy

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    EmploymentType,
    ShiftStatus,
    AwardLevel,
    State,
    DayType,
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown,
    calculate_roster_cost,
)
from rosteriq.award_rules import (
    get_day_type,
    get_penalty_multiplier,
    validate_shift_compliance,
)


# ============================================================================
# SCENARIO CHANGE TYPES
# ============================================================================


class ChangeType(str, Enum):
    """Types of scenario changes."""
    ADD_SHIFT = "add_shift"
    REMOVE_SHIFT = "remove_shift"
    CHANGE_EMPLOYEE = "change_employee"
    CHANGE_TIME = "change_time"
    ADD_CASUAL = "add_casual"
    REMOVE_DAY = "remove_day"
    EMPLOYEE_LEAVE = "employee_leave"


@dataclass
class AddShift:
    """Add a new shift to the roster."""
    employee_id: str
    shift_date: date
    start_time: time
    end_time: time
    role: str
    break_minutes: int = 0

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.ADD_SHIFT


@dataclass
class RemoveShift:
    """Remove a shift from the roster."""
    shift_id: str

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.REMOVE_SHIFT


@dataclass
class ChangeEmployee:
    """Swap employee on an existing shift."""
    shift_id: str
    new_employee_id: str

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.CHANGE_EMPLOYEE


@dataclass
class ChangeTime:
    """Modify shift start/end times."""
    shift_id: str
    new_start_time: time
    new_end_time: time
    break_minutes: Optional[int] = None

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.CHANGE_TIME


@dataclass
class AddCasual:
    """Add a hypothetical new casual hire for a shift."""
    shift_date: date
    start_time: time
    end_time: time
    role: str
    award_level: AwardLevel
    hourly_rate: Decimal
    state: State
    break_minutes: int = 0

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.ADD_CASUAL


@dataclass
class RemoveDay:
    """Remove all shifts from a specific date (cut the day to skeleton)."""
    shift_date: date

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.REMOVE_DAY


@dataclass
class EmployeeLeave:
    """Simulate employee absence during a date range."""
    employee_id: str
    start_date: date
    end_date: date

    @property
    def change_type(self) -> ChangeType:
        return ChangeType.EMPLOYEE_LEAVE


ScenarioChange = Union[
    AddShift,
    RemoveShift,
    ChangeEmployee,
    ChangeTime,
    AddCasual,
    RemoveDay,
    EmployeeLeave,
]


# ============================================================================
# SIMULATION RESULT DATACLASSES
# ============================================================================


@dataclass
class DayComparison:
    """Comparison of costs and staffing for a single day."""
    date: date
    original_cost: Decimal
    simulated_cost: Decimal
    cost_delta: Decimal
    original_staff_count: int
    simulated_staff_count: int
    staff_delta: int


@dataclass
class SimulationResult:
    """Result of a roster simulation."""
    scenario_name: str
    original_cost: Decimal
    simulated_cost: Decimal
    cost_delta: Decimal
    cost_delta_pct: float
    original_hours: float
    simulated_hours: float
    hours_delta: float
    original_shifts: int
    simulated_shifts: int
    shifts_delta: int
    conflicts_introduced: List[str] = field(default_factory=list)
    coverage_gaps: List[str] = field(default_factory=list)
    per_day_comparison: List[DayComparison] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    is_compliant: bool = True

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "scenario_name": self.scenario_name,
            "original_cost": str(self.original_cost),
            "simulated_cost": str(self.simulated_cost),
            "cost_delta": str(self.cost_delta),
            "cost_delta_pct": self.cost_delta_pct,
            "original_hours": self.original_hours,
            "simulated_hours": self.simulated_hours,
            "hours_delta": self.hours_delta,
            "original_shifts": self.original_shifts,
            "simulated_shifts": self.simulated_shifts,
            "shifts_delta": self.shifts_delta,
            "conflicts_introduced": self.conflicts_introduced,
            "coverage_gaps": self.coverage_gaps,
            "per_day_comparison": [
                {
                    "date": str(dc.date),
                    "original_cost": str(dc.original_cost),
                    "simulated_cost": str(dc.simulated_cost),
                    "cost_delta": str(dc.cost_delta),
                    "original_staff_count": dc.original_staff_count,
                    "simulated_staff_count": dc.simulated_staff_count,
                    "staff_delta": dc.staff_delta,
                }
                for dc in self.per_day_comparison
            ],
            "warnings": self.warnings,
            "is_compliant": self.is_compliant,
        }


# ============================================================================
# COST SIMULATOR
# ============================================================================


class CostSimulator:
    """
    Simulator for roster cost "what if" scenarios.

    Enables deep simulation of roster changes with detailed cost and compliance analysis.
    All operations clone the roster, never modifying the original.
    """

    def __init__(self, employees: Dict[str, Employee], state: State):
        """
        Initialize the cost simulator.

        Args:
            employees: Dict mapping employee IDs to Employee objects.
            state: The state for award rule application.
        """
        self.employees = employees
        self.state = state

    def simulate(
        self,
        roster: Roster,
        changes: List[ScenarioChange],
        scenario_name: str = "Custom Scenario",
    ) -> SimulationResult:
        """
        Simulate a roster with a set of changes.

        Args:
            roster: The original roster.
            changes: List of scenario changes to apply.
            scenario_name: Name for this scenario.

        Returns:
            SimulationResult with detailed comparison and analysis.
        """
        # Clone the original roster to avoid mutation
        simulated_roster = deepcopy(roster)

        # Apply all changes to the clone
        for change in changes:
            self._apply_change(simulated_roster, change)

        # Calculate costs for both rosters
        original_cost = calculate_roster_cost(roster, self.employees, self.state)
        simulated_cost = calculate_roster_cost(simulated_roster, self.employees, self.state)

        # Calculate cost delta
        cost_delta = simulated_cost - original_cost
        cost_delta_pct = float(
            (cost_delta / original_cost * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if original_cost > 0
            else Decimal("0")
        )

        # Calculate hours delta
        original_hours = roster.total_hours
        simulated_hours = simulated_roster.total_hours
        hours_delta = simulated_hours - original_hours

        # Calculate shifts delta
        original_shifts = len(roster.shifts)
        simulated_shifts = len(simulated_roster.shifts)
        shifts_delta = simulated_shifts - original_shifts

        # Run conflict detection
        conflicts = self._detect_conflicts(simulated_roster)
        coverage_gaps = self._detect_coverage_gaps(roster, simulated_roster)

        # Validate compliance
        is_compliant = len(conflicts) == 0
        warnings = self._generate_warnings(simulated_roster, conflicts)

        # Generate per-day comparison
        per_day_comparison = self._compare_days(roster, simulated_roster)

        return SimulationResult(
            scenario_name=scenario_name,
            original_cost=original_cost,
            simulated_cost=simulated_cost,
            cost_delta=cost_delta,
            cost_delta_pct=cost_delta_pct,
            original_hours=original_hours,
            simulated_hours=simulated_hours,
            hours_delta=hours_delta,
            original_shifts=original_shifts,
            simulated_shifts=simulated_shifts,
            shifts_delta=shifts_delta,
            conflicts_introduced=conflicts,
            coverage_gaps=coverage_gaps,
            per_day_comparison=per_day_comparison,
            warnings=warnings,
            is_compliant=is_compliant,
        )

    def compare_scenarios(
        self,
        roster: Roster,
        scenarios: List[Tuple[str, List[ScenarioChange]]],
    ) -> List[SimulationResult]:
        """
        Run and compare multiple scenarios side by side.

        Args:
            roster: The original roster.
            scenarios: List of (name, changes) tuples.

        Returns:
            List of SimulationResults, one per scenario.
        """
        results = []
        for scenario_name, changes in scenarios:
            result = self.simulate(roster, changes, scenario_name)
            results.append(result)
        return results

    def find_savings(
        self,
        roster: Roster,
        target_savings_pct: float,
        max_iterations: int = 10,
    ) -> Optional[List[ScenarioChange]]:
        """
        Auto-suggest changes to achieve a target cost reduction.

        Uses a greedy algorithm to identify cost-saving opportunities.
        Works by progressively removing shifts with high penalty multipliers.

        Args:
            roster: The original roster.
            target_savings_pct: Target savings as percentage (e.g., 10.0 for 10%).
            max_iterations: Maximum attempts to find savings.

        Returns:
            List of scenario changes that achieve target savings, or None if not found.
        """
        original_cost = calculate_roster_cost(roster, self.employees, self.state)
        target_cost = original_cost * Decimal(str(1 - (target_savings_pct / 100)))

        suggestions: List[ScenarioChange] = []
        simulated_roster = deepcopy(roster)
        current_cost = original_cost

        # Identify high-cost shifts sorted by cost desc
        shift_costs: List[Tuple[str, Decimal, float]] = []
        for shift in simulated_roster.shifts:
            if shift.employee_id in self.employees:
                breakdown = calculate_shift_cost_breakdown(
                    self.employees[shift.employee_id], shift, self.state
                )
                shift_costs.append((shift.id, breakdown.total_cost, shift.net_hours))

        # Sort by cost descending
        shift_costs.sort(key=lambda x: x[1], reverse=True)

        # Greedily remove shifts until target is reached
        for shift_id, shift_cost, hours in shift_costs:
            if current_cost <= target_cost or len(suggestions) >= max_iterations:
                break

            suggestions.append(RemoveShift(shift_id=shift_id))
            # Simulate removal
            simulated_roster.shifts = [s for s in simulated_roster.shifts if s.id != shift_id]
            current_cost = calculate_roster_cost(simulated_roster, self.employees, self.state)

        # Check if we achieved the target
        if current_cost <= target_cost:
            return suggestions
        return None

    def impact_of_leave(
        self,
        roster: Roster,
        employee_id: str,
        leave_dates: List[date],
    ) -> SimulationResult:
        """
        Quick simulation of employee leave impact.

        Args:
            roster: The original roster.
            employee_id: ID of employee going on leave.
            leave_dates: List of dates for the leave.

        Returns:
            SimulationResult showing the impact of the leave.
        """
        if not leave_dates:
            return SimulationResult(
                scenario_name=f"Leave impact: {employee_id}",
                original_cost=Decimal("0"),
                simulated_cost=Decimal("0"),
                cost_delta=Decimal("0"),
                cost_delta_pct=0.0,
                original_hours=0.0,
                simulated_hours=0.0,
                hours_delta=0.0,
                original_shifts=0,
                simulated_shifts=0,
                shifts_delta=0,
            )

        start_date = min(leave_dates)
        end_date = max(leave_dates)
        change = EmployeeLeave(
            employee_id=employee_id,
            start_date=start_date,
            end_date=end_date,
        )

        return self.simulate(
            roster,
            [change],
            scenario_name=f"Leave impact: {employee_id} ({start_date} to {end_date})",
        )

    # ========================================================================
    # PRIVATE METHODS
    # ========================================================================

    def _apply_change(self, roster: Roster, change: ScenarioChange) -> None:
        """Apply a single scenario change to a roster in-place."""
        if isinstance(change, AddShift):
            self._apply_add_shift(roster, change)
        elif isinstance(change, RemoveShift):
            self._apply_remove_shift(roster, change)
        elif isinstance(change, ChangeEmployee):
            self._apply_change_employee(roster, change)
        elif isinstance(change, ChangeTime):
            self._apply_change_time(roster, change)
        elif isinstance(change, AddCasual):
            self._apply_add_casual(roster, change)
        elif isinstance(change, RemoveDay):
            self._apply_remove_day(roster, change)
        elif isinstance(change, EmployeeLeave):
            self._apply_employee_leave(roster, change)

    def _apply_add_shift(self, roster: Roster, change: AddShift) -> None:
        """Add a new shift to the roster."""
        new_shift = Shift(
            id=f"sim_{len(roster.shifts)}_{datetime.now().timestamp()}",
            employee_id=change.employee_id,
            date=change.shift_date,
            start_time=change.start_time,
            end_time=change.end_time,
            break_minutes=change.break_minutes,
            status=ShiftStatus.scheduled,
            role=change.role,
            cost=None,
            penalty_multiplier=1.0,
        )
        roster.shifts.append(new_shift)

    def _apply_remove_shift(self, roster: Roster, change: RemoveShift) -> None:
        """Remove a shift from the roster."""
        roster.shifts = [s for s in roster.shifts if s.id != change.shift_id]

    def _apply_change_employee(self, roster: Roster, change: ChangeEmployee) -> None:
        """Change the employee assigned to a shift."""
        for shift in roster.shifts:
            if shift.id == change.shift_id:
                shift.employee_id = change.new_employee_id
                break

    def _apply_change_time(self, roster: Roster, change: ChangeTime) -> None:
        """Change shift start/end times."""
        for shift in roster.shifts:
            if shift.id == change.shift_id:
                shift.start_time = change.new_start_time
                shift.end_time = change.new_end_time
                if change.break_minutes is not None:
                    shift.break_minutes = change.break_minutes
                break

    def _apply_add_casual(self, roster: Roster, change: AddCasual) -> None:
        """Add a hypothetical casual hire shift."""
        # Create a temporary casual employee for this shift
        casual_id = f"casual_{len(roster.shifts)}_{datetime.now().timestamp()}"
        casual_employee = Employee(
            id=casual_id,
            name="Hypothetical Casual",
            employment_type=EmploymentType.casual,
            award_level=change.award_level,
            state=change.state,
            hourly_base_rate=change.hourly_rate,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        self.employees[casual_id] = casual_employee

        new_shift = Shift(
            id=f"sim_{len(roster.shifts)}_{datetime.now().timestamp()}",
            employee_id=casual_id,
            date=change.shift_date,
            start_time=change.start_time,
            end_time=change.end_time,
            break_minutes=change.break_minutes,
            status=ShiftStatus.scheduled,
            role=change.role,
            cost=None,
            penalty_multiplier=1.0,
        )
        roster.shifts.append(new_shift)

    def _apply_remove_day(self, roster: Roster, change: RemoveDay) -> None:
        """Remove all shifts from a specific date."""
        roster.shifts = [s for s in roster.shifts if s.date != change.shift_date]

    def _apply_employee_leave(self, roster: Roster, change: EmployeeLeave) -> None:
        """Remove shifts for an employee during a date range."""
        roster.shifts = [
            s
            for s in roster.shifts
            if not (
                s.employee_id == change.employee_id
                and change.start_date <= s.date <= change.end_date
            )
        ]

    def _detect_conflicts(self, roster: Roster) -> List[str]:
        """Detect compliance conflicts in the roster."""
        conflicts = []

        # Check each shift for compliance
        for shift in roster.shifts:
            if shift.employee_id not in self.employees:
                conflicts.append(f"Shift {shift.id}: Employee {shift.employee_id} not found")
                continue

            employee = self.employees[shift.employee_id]
            violations = validate_shift_compliance(employee, shift)
            conflicts.extend(violations)

        # Check for consecutive days violations per employee
        employee_shifts: Dict[str, List[Shift]] = {}
        for shift in roster.shifts:
            if shift.employee_id not in employee_shifts:
                employee_shifts[shift.employee_id] = []
            employee_shifts[shift.employee_id].append(shift)

        for emp_id, shifts in employee_shifts.items():
            sorted_shifts = sorted(shifts, key=lambda s: s.date)
            consecutive_count = 1
            for i in range(1, len(sorted_shifts)):
                if sorted_shifts[i].date == sorted_shifts[i - 1].date + timedelta(days=1):
                    consecutive_count += 1
                    if consecutive_count > 6:
                        conflicts.append(
                            f"Employee {emp_id} scheduled {consecutive_count} consecutive days"
                        )
                else:
                    consecutive_count = 1

        return conflicts

    def _detect_coverage_gaps(self, original: Roster, simulated: Roster) -> List[str]:
        """Detect coverage gaps introduced by simulation."""
        gaps = []

        # Calculate staff count by date
        original_staff_by_date: Dict[date, int] = {}
        simulated_staff_by_date: Dict[date, int] = {}

        for shift in original.shifts:
            original_staff_by_date[shift.date] = original_staff_by_date.get(shift.date, 0) + 1

        for shift in simulated.shifts:
            simulated_staff_by_date[shift.date] = simulated_staff_by_date.get(shift.date, 0) + 1

        # Check for significant drops
        for d, orig_count in original_staff_by_date.items():
            sim_count = simulated_staff_by_date.get(d, 0)
            drop = orig_count - sim_count
            if drop > 2:  # More than 2 staff removed
                gaps.append(
                    f"Coverage gap on {d}: {orig_count} -> {sim_count} staff "
                    f"({drop} staff removed)"
                )

        return gaps

    def _compare_days(self, original: Roster, simulated: Roster) -> List[DayComparison]:
        """Generate per-day comparison between rosters."""
        comparisons: Dict[date, DayComparison] = {}

        # Process original roster
        for shift in original.shifts:
            if shift.date not in comparisons:
                comparisons[shift.date] = DayComparison(
                    date=shift.date,
                    original_cost=Decimal("0"),
                    simulated_cost=Decimal("0"),
                    cost_delta=Decimal("0"),
                    original_staff_count=0,
                    simulated_staff_count=0,
                    staff_delta=0,
                )
            comp = comparisons[shift.date]
            comp.original_staff_count += 1
            if shift.employee_id in self.employees:
                breakdown = calculate_shift_cost_breakdown(
                    self.employees[shift.employee_id], shift, self.state
                )
                comp.original_cost += breakdown.total_cost

        # Process simulated roster
        for shift in simulated.shifts:
            if shift.date not in comparisons:
                comparisons[shift.date] = DayComparison(
                    date=shift.date,
                    original_cost=Decimal("0"),
                    simulated_cost=Decimal("0"),
                    cost_delta=Decimal("0"),
                    original_staff_count=0,
                    simulated_staff_count=0,
                    staff_delta=0,
                )
            comp = comparisons[shift.date]
            comp.simulated_staff_count += 1
            if shift.employee_id in self.employees:
                breakdown = calculate_shift_cost_breakdown(
                    self.employees[shift.employee_id], shift, self.state
                )
                comp.simulated_cost += breakdown.total_cost

        # Calculate deltas
        for comp in comparisons.values():
            comp.cost_delta = comp.simulated_cost - comp.original_cost
            comp.staff_delta = comp.simulated_staff_count - comp.original_staff_count

        return sorted(comparisons.values(), key=lambda x: x.date)

    def _generate_warnings(self, roster: Roster, conflicts: List[str]) -> List[str]:
        """Generate warnings about the simulated roster."""
        warnings = []

        if conflicts:
            warnings.append(f"{len(conflicts)} compliance issues detected")

        # Check for thin staffing
        staff_by_date: Dict[date, int] = {}
        for shift in roster.shifts:
            staff_by_date[shift.date] = staff_by_date.get(shift.date, 0) + 1

        for d, count in staff_by_date.items():
            if count <= 1:
                warnings.append(f"Only 1 staff member scheduled on {d}")

        # Check for high-cost days
        daily_costs: Dict[date, Decimal] = {}
        for shift in roster.shifts:
            if shift.employee_id in self.employees:
                breakdown = calculate_shift_cost_breakdown(
                    self.employees[shift.employee_id], shift, self.state
                )
                daily_costs[shift.date] = daily_costs.get(shift.date, Decimal("0")) + breakdown.total_cost

        if daily_costs:
            avg_daily_cost = sum(daily_costs.values()) / len(daily_costs)
            for d, cost in daily_costs.items():
                if cost > avg_daily_cost * Decimal("1.5"):
                    warnings.append(f"High cost day on {d}: ${cost:.2f} (avg: ${avg_daily_cost:.2f})")

        return warnings
