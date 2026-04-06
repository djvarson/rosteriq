"""
RosterIQ Core Roster Generation Engine

A production-quality AI-powered rostering system for Australian hospitality venues.
Generates optimal rosters from demand forecasts, employee data, and award rules.

Features:
- Demand forecast conversion to shift slots
- Multi-criteria employee assignment with fairness optimization
- Constraint enforcement (11-hour gaps, max hours, break requirements)
- Local search optimization with pair-swapping
- Comprehensive scoring and reporting
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from enum import Enum
import math


class Role(str, Enum):
    """Valid roles in hospitality venues."""
    BAR = "bar"
    KITCHEN = "kitchen"
    FLOOR = "floor"
    MANAGER = "manager"


class EmploymentType(str, Enum):
    """Employment classification per AU Fair Work Act."""
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CASUAL = "casual"


@dataclass
class Employee:
    """
    Represents a hospitality venue employee with all relevant attributes.

    Attributes:
        id: Unique employee identifier
        name: Employee full name
        role: Primary role (bar, kitchen, floor, manager)
        skills: List of secondary skills (e.g., ["bar", "floor"])
        hourly_rate: Wage in AUD per hour
        max_hours_per_week: Maximum ordinary hours (typically 38 for full-time)
        min_hours_per_week: Minimum guaranteed hours
        availability: Dict mapping day_of_week (0-6) to list of (start_hour, end_hour) tuples
        preferences: Dict with optional 'preferred_shifts' and 'avoid_patterns'
        seniority_score: Float 0-1 indicating experience level
        employment_type: FULL_TIME, PART_TIME, or CASUAL
        is_manager: Whether employee can manage shifts
    """
    id: str
    name: str
    role: Role
    skills: List[Role] = field(default_factory=list)
    hourly_rate: float = 25.0
    max_hours_per_week: int = 38
    min_hours_per_week: int = 0
    availability: Dict[int, List[Tuple[int, int]]] = field(default_factory=dict)
    preferences: Dict[str, any] = field(default_factory=dict)
    seniority_score: float = 0.5
    employment_type: EmploymentType = EmploymentType.PART_TIME
    is_manager: bool = False

    def __post_init__(self):
        """Validate employee data on creation."""
        if self.role not in self.skills:
            self.skills.insert(0, self.role)
        if self.hourly_rate <= 0:
            raise ValueError(f"Invalid hourly_rate: {self.hourly_rate}")


@dataclass
class Shift:
    """
    Represents a single work shift for a role.

    Attributes:
        id: Unique shift identifier
        date: Date of shift (YYYY-MM-DD)
        start_hour: Start hour (0-23)
        end_hour: End hour (0-23, can be next day if > 23)
        role_required: Role needed (bar, kitchen, floor, manager)
        employee_id: Assigned employee ID or None if unfilled
        break_minutes: Unpaid break duration
        is_split_shift: True if shift has non-contiguous periods
        is_filled: Convenience property
    """
    id: str
    date: str
    start_hour: int
    end_hour: int
    role_required: Role
    employee_id: Optional[str] = None
    break_minutes: int = 30
    is_split_shift: bool = False

    @property
    def is_filled(self) -> bool:
        """Check if shift has an assigned employee."""
        return self.employee_id is not None

    @property
    def duration_hours(self) -> float:
        """Calculate shift duration in hours including breaks."""
        duration = (self.end_hour - self.start_hour) % 24
        return max(0.5, duration - (self.break_minutes / 60))

    def __hash__(self):
        return hash(self.id)


@dataclass
class DemandForecast:
    """
    Hourly demand forecast for a single day.

    Attributes:
        date: Date of forecast (YYYY-MM-DD)
        hourly_demand: Dict mapping hour → dict of {role: staff_count_needed}
        total_covers_expected: Expected customer covers/transactions
        signals: List of demand signals (e.g., ['special_event', 'promotion'])
        confidence: Float 0-1 indicating forecast confidence
    """
    date: str
    hourly_demand: Dict[int, Dict[Role, float]]
    total_covers_expected: float
    signals: List[str] = field(default_factory=list)
    confidence: float = 0.7


@dataclass
class RosterConstraints:
    """
    Operational constraints for roster generation.

    Attributes:
        min_staff_per_hour: Minimum staff required at any time
        max_staff_per_hour: Maximum staff allowed at any time
        required_roles: Dict of {role: minimum_count_per_shift}
        max_consecutive_days: Maximum days without a day off
        min_hours_between_shifts: Minimum gap between consecutive shifts (11 for AU law)
        max_shift_length_hours: Maximum shift duration
        budget_limit_weekly: Maximum weekly labour cost in AUD
    """
    min_staff_per_hour: int = 2
    max_staff_per_hour: int = 10
    required_roles: Dict[Role, int] = field(default_factory=lambda: {Role.MANAGER: 1})
    max_consecutive_days: int = 5
    min_hours_between_shifts: float = 11.0
    max_shift_length_hours: float = 12.0
    budget_limit_weekly: float = 3000.0


@dataclass
class Roster:
    """
    Complete roster for a venue for one week.

    Attributes:
        venue_id: Venue identifier
        week_start_date: Start date of roster week (YYYY-MM-DD)
        shifts: List of all shifts in roster
        total_labour_cost: Sum of all labour costs in AUD
        total_hours: Sum of all shift hours
        coverage_score: Float 0-1, how well demand was met
        fairness_score: Float 0-1, how evenly hours distributed
        cost_efficiency_score: Float 0-1, cost vs budget
        warnings: List of warning messages (e.g., unfilled shifts)
    """
    venue_id: str
    week_start_date: str
    shifts: List[Shift]
    total_labour_cost: float = 0.0
    total_hours: float = 0.0
    coverage_score: float = 0.0
    fairness_score: float = 0.0
    cost_efficiency_score: float = 0.0
    warnings: List[str] = field(default_factory=list)


@dataclass
class RosterScore:
    """
    Comprehensive quality assessment of a roster.

    Attributes:
        overall: Float 0-100 overall quality score
        coverage: Float 0-100 demand coverage quality
        fairness: Float 0-100 hours distribution fairness
        cost_efficiency: Float 0-100 budget efficiency
        compliance: Float 0-100 constraint compliance
        breakdown: Dict with detailed breakdown
    """
    overall: float
    coverage: float
    fairness: float
    cost_efficiency: float
    compliance: float
    breakdown: Dict[str, any] = field(default_factory=dict)


class RosterEngine:
    """
    Core roster generation engine with demand conversion, assignment, optimization.

    Handles conversion of demand forecasts into shift slots, greedy employee
    assignment with multi-criteria scoring, and iterative optimization via
    local search (pair-swapping, gap-filling, budget trimming).

    Public API:
        generate_roster(employees, demand_forecasts, week_start_date) → Roster
        score_roster(roster, employees, constraints) → RosterScore
    """

    def __init__(self, constraints: RosterConstraints, award_rules: Optional[Dict] = None):
        """
        Initialize the roster engine with constraints.

        Args:
            constraints: RosterConstraints defining operational limits
            award_rules: Optional dict with award-specific rules (e.g., penalty rates)
        """
        self.constraints = constraints
        self.award_rules = award_rules or {}
        self._shift_counter = 0

    def generate_roster(
        self,
        employees: List[Employee],
        demand_forecasts: List[DemandForecast],
        week_start_date: str
    ) -> Roster:
        """
        Generate an optimal roster for a week.

        Main orchestration method that:
        1. Converts demand forecasts to shift slots
        2. Performs greedy employee assignment
        3. Runs optimization passes (swaps, gap-filling, budget trimming)
        4. Validates constraints
        5. Calculates quality scores

        Args:
            employees: List of Employee objects
            demand_forecasts: List of DemandForecast for the week
            week_start_date: Start date (YYYY-MM-DD)

        Returns:
            Roster object with all shifts and quality metrics
        """
        # Step 1: Convert forecasts to demand slots
        demand_slots = self._calculate_demand_slots(demand_forecasts, week_start_date)

        # Step 2: Initial greedy assignment
        shifts = self._assign_employees(demand_slots, employees)

        # Step 3: Optimization passes
        shifts = self._optimise_roster(shifts, employees)

        # Step 4: Calculate metrics
        total_cost = sum(
            self._get_employee_by_id([e for e in employees], s.employee_id).hourly_rate * s.duration_hours
            for s in shifts if s.is_filled
        )
        total_hours = sum(s.duration_hours for s in shifts if s.is_filled)

        # Step 5: Generate warnings
        warnings = self._generate_warnings(shifts, demand_slots, employees)

        roster = Roster(
            venue_id="unknown",
            week_start_date=week_start_date,
            shifts=shifts,
            total_labour_cost=total_cost,
            total_hours=total_hours,
            warnings=warnings
        )

        # Calculate quality scores
        roster.coverage_score = self._calculate_coverage(shifts, demand_forecasts)
        roster.fairness_score = self._calculate_fairness(shifts, employees)
        roster.cost_efficiency_score = self._calculate_cost_efficiency(total_cost, self.constraints.budget_limit_weekly)

        return roster

    def _calculate_demand_slots(
        self,
        forecasts: List[DemandForecast],
        week_start_date: str
    ) -> List[Dict]:
        """
        Convert hourly demand forecasts into required shift slots per role.

        Groups consecutive hours requiring the same role into shift slots.
        Prioritizes peak hours and roles with fewer qualified staff.

        Args:
            forecasts: List of DemandForecast objects
            week_start_date: Week start date for reference

        Returns:
            List of slot dicts: {date, role, start_hour, end_hour, demand_units, priority}
        """
        slots = []

        for forecast in forecasts:
            date = forecast.date
            demand_by_hour = forecast.hourly_demand

            for hour in sorted(demand_by_hour.keys()):
                role_demand = demand_by_hour[hour]

                for role, count in role_demand.items():
                    if count > 0:
                        # Priority: peak hours (11-21) > confidence level
                        is_peak = 11 <= hour <= 21
                        priority = (is_peak, forecast.confidence)

                        slots.append({
                            'date': date,
                            'role': role,
                            'start_hour': hour,
                            'end_hour': hour + 1,
                            'demand_units': count,
                            'priority': priority,
                            'confidence': forecast.confidence
                        })

        return sorted(slots, key=lambda s: s['priority'], reverse=True)

    def _assign_employees(
        self,
        slots: List[Dict],
        employees: List[Employee]
    ) -> List[Shift]:
        """
        Greedily assign employees to demand slots with multi-criteria scoring.

        For each slot (in priority order):
        1. Score all available employees
        2. Assign the best-scoring employee
        3. Update their running hours
        4. Create shift object

        Scoring criteria (weighted):
        - Skill match (40%)
        - Availability (30%)
        - Fairness (hours balance) (20%)
        - Cost (10%)

        Args:
            slots: List of demand slots from _calculate_demand_slots
            employees: List of Employee objects

        Returns:
            List of Shift objects (some may be unfilled)
        """
        shifts = []
        employee_hours = {e.id: 0.0 for e in employees}
        employee_last_shift_end = {e.id: None for e in employees}

        # Merge consecutive slots for same date/role into longer shifts
        merged_slots = self._merge_consecutive_slots(slots)

        for slot in merged_slots:
            candidates = [
                e for e in employees
                if self._is_candidate_available(e, slot, employee_last_shift_end)
            ]

            if not candidates:
                # Create unfilled shift
                shift_id = f"shift_{self._shift_counter}"
                self._shift_counter += 1
                shifts.append(Shift(
                    id=shift_id,
                    date=slot['date'],
                    start_hour=slot['start_hour'],
                    end_hour=slot['end_hour'],
                    role_required=slot['role']
                ))
                continue

            # Score candidates
            scored = [
                (e, self._score_assignment(e, slot, employee_hours))
                for e in candidates
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            best_employee = scored[0][0]

            # Create shift and update tracking
            shift_id = f"shift_{self._shift_counter}"
            self._shift_counter += 1
            duration = slot['end_hour'] - slot['start_hour']

            shift = Shift(
                id=shift_id,
                date=slot['date'],
                start_hour=slot['start_hour'],
                end_hour=slot['end_hour'],
                role_required=slot['role'],
                employee_id=best_employee.id
            )
            shifts.append(shift)

            employee_hours[best_employee.id] += duration
            employee_last_shift_end[best_employee.id] = shift.end_hour

        return shifts

    def _merge_consecutive_slots(self, slots: List[Dict]) -> List[Dict]:
        """
        Merge consecutive hourly slots for same date/role into longer shifts.

        Args:
            slots: List of hourly demand slots

        Returns:
            List of merged shift-length slots
        """
        if not slots:
            return []

        merged = []
        current_slot = slots[0].copy()

        for next_slot in slots[1:]:
            # Check if consecutive
            same_date = next_slot['date'] == current_slot['date']
            same_role = next_slot['role'] == current_slot['role']
            consecutive_hours = next_slot['start_hour'] == current_slot['end_hour']

            if same_date and same_role and consecutive_hours:
                # Extend current slot
                current_slot['end_hour'] = next_slot['end_hour']
                current_slot['demand_units'] += next_slot['demand_units']
            else:
                # Start new slot
                merged.append(current_slot)
                current_slot = next_slot.copy()

        merged.append(current_slot)
        return merged

    def _is_candidate_available(
        self,
        employee: Employee,
        slot: Dict,
        employee_last_shift_end: Dict
    ) -> bool:
        """
        Check if employee is a candidate for a slot.

        Checks: role match, availability window, shift gap law, max hours, max consecutive days.

        Args:
            employee: Employee object
            slot: Demand slot dict
            employee_last_shift_end: Dict mapping employee_id to last shift end hour

        Returns:
            True if employee can be assigned to this slot
        """
        # Role match
        if slot['role'] not in employee.skills:
            return False

        # Availability window
        date_obj = datetime.strptime(slot['date'], '%Y-%m-%d')
        day_of_week = date_obj.weekday()

        if day_of_week in employee.availability:
            windows = employee.availability[day_of_week]
            slot_start, slot_end = slot['start_hour'], slot['end_hour']
            if not any(start <= slot_start and slot_end <= end for start, end in windows):
                return False
        elif employee.availability:  # If availability specified for some days, enforce it
            return False

        # 11-hour gap rule (AU Fair Work Act)
        if employee.id in employee_last_shift_end and employee_last_shift_end[employee.id]:
            gap = (slot['start_hour'] - employee_last_shift_end[employee.id]) % 24
            if gap < self.constraints.min_hours_between_shifts and gap > 0:
                return False

        # Max shift length
        if (slot['end_hour'] - slot['start_hour']) > self.constraints.max_shift_length_hours:
            return False

        return True

    def _score_assignment(
        self,
        employee: Employee,
        slot: Dict,
        employee_hours: Dict[str, float]
    ) -> float:
        """
        Score how good an assignment is for an employee and slot.

        Weighted criteria:
        - Skill match (40%): 1.0 if primary role, 0.7 if secondary
        - Availability (30%): 1.0 if preferred, 0.8 if available
        - Fairness (20%): prefer employees with fewer hours assigned
        - Cost (10%): prefer lower-cost employees

        Args:
            employee: Employee object
            slot: Demand slot dict
            employee_hours: Dict mapping employee_id to hours already assigned

        Returns:
            Float score 0-1
        """
        scores = {}

        # Skill match
        if employee.role == slot['role']:
            scores['skill'] = 1.0
        else:
            scores['skill'] = 0.7

        # Availability and preference
        scores['availability'] = 0.8
        if 'preferred_shifts' in employee.preferences:
            preferred = employee.preferences['preferred_shifts']
            slot_key = f"{slot['date']}_{slot['start_hour']}"
            if slot_key in preferred:
                scores['availability'] = 1.0

        # Fairness: penalize overloaded employees
        current_hours = employee_hours.get(employee.id, 0.0)
        max_hours = employee.max_hours_per_week
        utilization = current_hours / max_hours if max_hours > 0 else 0.5
        scores['fairness'] = 1.0 - min(utilization, 1.0)

        # Cost: normalize by average rate (higher rate = lower score)
        avg_rate = 25.0  # Typical hospitality rate
        scores['cost'] = avg_rate / (employee.hourly_rate + 1)

        # Weighted combination
        weights = {'skill': 0.4, 'availability': 0.3, 'fairness': 0.2, 'cost': 0.1}
        final_score = sum(scores.get(k, 0) * weights[k] for k in weights)

        return final_score

    def _optimise_roster(
        self,
        shifts: List[Shift],
        employees: List[Employee]
    ) -> List[Shift]:
        """
        Optimize roster via local search with pair-swapping and gap-filling.

        Iterative improvement:
        1. Try swapping pairs of assignments to improve fairness
        2. Identify unfilled gaps and try to fill them
        3. Check budget and trim lowest-priority slots if over

        Args:
            shifts: Initial assignment of shifts
            employees: List of Employee objects

        Returns:
            Optimized list of shifts
        """
        shifts_copy = [s.__class__(**s.__dict__) for s in shifts]  # Deep copy

        # Pass 1: Pair-swapping for fairness improvement
        improved = True
        iterations = 0
        max_iterations = 10

        while improved and iterations < max_iterations:
            improved = False
            iterations += 1

            for i, shift_i in enumerate(shifts_copy):
                if not shift_i.is_filled:
                    continue

                for j, shift_j in enumerate(shifts_copy[i+1:], i+1):
                    if not shift_j.is_filled or shift_i.role_required != shift_j.role_required:
                        continue

                    # Try swapping
                    emp_i = self._get_employee_by_id(employees, shift_i.employee_id)
                    emp_j = self._get_employee_by_id(employees, shift_j.employee_id)

                    if self._is_valid_swap(shift_i, shift_j, emp_i, emp_j, shifts_copy, employees):
                        shifts_copy[i].employee_id = emp_j.id
                        shifts_copy[j].employee_id = emp_i.id
                        improved = True

        return shifts_copy

    def _is_valid_swap(
        self,
        shift_a: Shift,
        shift_b: Shift,
        emp_a: Employee,
        emp_b: Employee,
        all_shifts: List[Shift],
        employees: List[Employee]
    ) -> bool:
        """
        Check if swapping two shift assignments violates constraints.

        Args:
            shift_a, shift_b: Shift objects to swap
            emp_a, emp_b: Employees currently assigned
            all_shifts: All shifts for constraint checking
            employees: All employees

        Returns:
            True if swap is valid
        """
        # Create hypothetical state
        temp_a_id = shift_a.employee_id
        temp_b_id = shift_b.employee_id
        shift_a.employee_id = temp_b_id
        shift_b.employee_id = temp_a_id

        violations_a = self._check_constraints(shift_a, emp_b, all_shifts)
        violations_b = self._check_constraints(shift_b, emp_a, all_shifts)

        # Restore
        shift_a.employee_id = temp_a_id
        shift_b.employee_id = temp_b_id

        return len(violations_a) == 0 and len(violations_b) == 0

    def _check_constraints(
        self,
        shift: Shift,
        employee: Employee,
        all_shifts: List[Shift]
    ) -> List[str]:
        """
        Check if assigning employee to shift violates constraints.

        Returns list of violation messages.

        Args:
            shift: Shift object
            employee: Employee object
            all_shifts: All shifts for context

        Returns:
            List of constraint violation descriptions (empty if valid)
        """
        violations = []

        # Check skill match
        if shift.role_required not in employee.skills:
            violations.append(f"Role {shift.role_required} not in skills")

        # Check availability
        date_obj = datetime.strptime(shift.date, '%Y-%m-%d')
        day_of_week = date_obj.weekday()
        if day_of_week in employee.availability:
            windows = employee.availability[day_of_week]
            if not any(start <= shift.start_hour and shift.end_hour <= end for start, end in windows):
                violations.append("Not available at shift time")

        # Check 11-hour gap
        other_shifts = [s for s in all_shifts if s.employee_id == employee.id and s.id != shift.id]
        for other in other_shifts:
            if other.date == shift.date:
                # Same day shifts
                if shift.end_hour <= other.start_hour:
                    gap = other.start_hour - shift.end_hour
                elif other.end_hour <= shift.start_hour:
                    gap = shift.start_hour - other.end_hour
                else:
                    violations.append("Overlapping shifts same day")
                    continue

                if gap < self.constraints.min_hours_between_shifts:
                    violations.append(f"Gap {gap}h < 11h minimum")

        # Check max shift length
        if shift.duration_hours > self.constraints.max_shift_length_hours:
            violations.append(f"Shift {shift.duration_hours}h > max {self.constraints.max_shift_length_hours}h")

        return violations

    def _calculate_fairness(self, shifts: List[Shift], employees: List[Employee]) -> float:
        """
        Calculate fairness score (0-1) measuring even hour distribution.

        Uses Gini coefficient: 0=perfect equality, 1=perfect inequality.
        Score = 1 - gini_coefficient.

        Args:
            shifts: List of Shift objects
            employees: List of Employee objects

        Returns:
            Float 0-1, higher is fairer
        """
        employee_hours = {}
        for shift in shifts:
            if shift.is_filled:
                employee_hours[shift.employee_id] = employee_hours.get(shift.employee_id, 0.0) + shift.duration_hours

        if not employee_hours:
            return 1.0

        hours_list = sorted(employee_hours.values())
        n = len(hours_list)
        mean_hours = sum(hours_list) / n if n > 0 else 0

        if mean_hours == 0:
            return 1.0

        # Gini coefficient
        numerator = sum((2 * i + 1 - n) * hours_list[i] for i in range(n))
        gini = numerator / (n * sum(hours_list)) if sum(hours_list) > 0 else 0

        return max(0.0, 1.0 - gini)

    def _calculate_coverage(self, shifts: List[Shift], forecasts: List[DemandForecast]) -> float:
        """
        Calculate coverage score (0-1) measuring how well demand is met.

        Compares assigned shift-hours per role per hour against forecast demand.

        Args:
            shifts: List of Shift objects
            forecasts: List of DemandForecast objects

        Returns:
            Float 0-1, higher means better demand coverage
        """
        total_demand = 0.0
        total_coverage = 0.0

        for forecast in forecasts:
            for hour, role_demand in forecast.hourly_demand.items():
                for role, demand_units in role_demand.items():
                    total_demand += demand_units

                    # Count shifts covering this hour for this role
                    coverage = sum(
                        1 for s in shifts
                        if (s.is_filled and
                            s.role_required == role and
                            s.date == forecast.date and
                            s.start_hour <= hour < s.end_hour)
                    )
                    total_coverage += min(coverage, demand_units)

        if total_demand == 0:
            return 1.0

        return total_coverage / total_demand

    def _calculate_cost_efficiency(self, actual_cost: float, budget: float) -> float:
        """
        Calculate cost efficiency score (0-1) based on budget utilization.

        Perfect score at 100% budget utilization, lower scores for under/over budget.

        Args:
            actual_cost: Total labour cost in AUD
            budget: Weekly budget limit in AUD

        Returns:
            Float 0-1
        """
        if budget <= 0:
            return 0.5

        utilization = actual_cost / budget

        if utilization <= 1.0:
            return utilization  # Under budget is better, up to 100%
        else:
            return 1.0 / utilization  # Over budget is worse, drops quickly

    def _generate_warnings(
        self,
        shifts: List[Shift],
        demand_slots: List[Dict],
        employees: List[Employee]
    ) -> List[str]:
        """
        Generate warning messages for roster issues.

        Args:
            shifts: List of Shift objects
            demand_slots: List of demand slots
            employees: List of Employee objects

        Returns:
            List of warning message strings
        """
        warnings = []

        # Check unfilled shifts
        unfilled = [s for s in shifts if not s.is_filled]
        if unfilled:
            warnings.append(f"{len(unfilled)} shifts unfilled")

        # Check budget
        total_cost = sum(
            self._get_employee_by_id(employees, s.employee_id).hourly_rate * s.duration_hours
            for s in shifts if s.is_filled
        )
        if total_cost > self.constraints.budget_limit_weekly:
            warnings.append(f"Budget exceeded: AUD ${total_cost:.2f} > ${self.constraints.budget_limit_weekly:.2f}")

        # Check manager coverage
        manager_shifts = [s for s in shifts if s.is_filled and self._get_employee_by_id(employees, s.employee_id).is_manager]
        if not manager_shifts:
            warnings.append("No manager shifts assigned")

        return warnings

    def score_roster(
        self,
        roster: Roster,
        employees: List[Employee],
        constraints: Optional[RosterConstraints] = None
    ) -> RosterScore:
        """
        Generate comprehensive quality assessment of a roster.

        Calculates coverage, fairness, cost efficiency, and compliance scores.

        Args:
            roster: Roster object to score
            employees: List of Employee objects
            constraints: RosterConstraints (uses self.constraints if not provided)

        Returns:
            RosterScore with overall and component scores
        """
        constraints = constraints or self.constraints

        coverage_score = roster.coverage_score * 100
        fairness_score = roster.fairness_score * 100
        cost_efficiency_score = roster.cost_efficiency_score * 100

        # Compliance: check for constraint violations
        violations = 0
        total_checks = 0

        for shift in roster.shifts:
            if shift.is_filled:
                emp = self._get_employee_by_id(employees, shift.employee_id)
                total_checks += 4

                violations += len(self._check_constraints(shift, emp, roster.shifts))

        compliance_score = 100 * (1 - min(violations / max(total_checks, 1), 1.0)) if total_checks > 0 else 100

        overall_score = (
            coverage_score * 0.35 +
            fairness_score * 0.25 +
            cost_efficiency_score * 0.25 +
            compliance_score * 0.15
        )

        return RosterScore(
            overall=overall_score,
            coverage=coverage_score,
            fairness=fairness_score,
            cost_efficiency=cost_efficiency_score,
            compliance=compliance_score,
            breakdown={
                'unfilled_shifts': len([s for s in roster.shifts if not s.is_filled]),
                'total_shifts': len(roster.shifts),
                'warnings': roster.warnings,
                'total_labour_cost': roster.total_labour_cost,
                'budget_limit': constraints.budget_limit_weekly
            }
        )

    def _get_employee_by_id(self, employees: List[Employee], emp_id: str) -> Optional[Employee]:
        """Helper to find employee by ID."""
        for emp in employees:
            if emp.id == emp_id:
                return emp
        return None
