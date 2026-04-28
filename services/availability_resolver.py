"""
Employee availability conflict resolver for RosterIQ.

Automatically suggests alternative employees for conflicting shifts, with intelligent
ranking based on cost, fairness, preferences, and constraints.

Features:
- Alternative employee ranking with multi-factor scoring
- Time adjustment suggestions for partial availability
- Beneficial shift swap detection (both parties win)
- Bulk conflict resolution with constraint optimization
- Hourly coverage gap analysis and availability mapping

Used by the conflict resolver and roster optimization engine.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Set, Tuple
from collections import defaultdict

from rosteriq.database import get_db
from rosteriq.models import (
    Shift, Roster, Employee, VenueConfig, EmploymentType, ShiftStatus,
)
from rosteriq.services.preference_learner import PreferenceLearner
from rosteriq.services.employee_costing import EmployeeCostingService
from rosteriq.award_rules import (
    get_penalty_multiplier, get_minimum_engagement_hours,
    MAX_SHIFT_LENGTH_HOURS, MINIMUM_HOURS_BETWEEN_SHIFTS, MAX_CONSECUTIVE_DAYS,
    get_day_type, DayType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass
class AlternativeOption:
    """Represents an alternative employee option for a conflicting shift."""

    employee: Employee
    overall_score: float  # 0-100, weighted composite
    cost_impact: Decimal  # positive = more expensive
    skill_match_score: float  # 0-100
    fairness_score: float  # 0-100
    preference_score: float  # 0-100
    availability_fit_score: float  # 0-100
    hours_this_week: float  # current hours worked
    hours_after: float  # hours if assigned
    consecutive_days_after: int  # consecutive days if assigned
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "employee_id": self.employee.id,
            "employee_name": self.employee.name,
            "overall_score": round(self.overall_score, 2),
            "cost_impact": float(self.cost_impact),
            "skill_match_score": round(self.skill_match_score, 2),
            "fairness_score": round(self.fairness_score, 2),
            "preference_score": round(self.preference_score, 2),
            "availability_fit_score": round(self.availability_fit_score, 2),
            "hours_this_week": round(self.hours_this_week, 2),
            "hours_after": round(self.hours_after, 2),
            "consecutive_days_after": self.consecutive_days_after,
            "warnings": self.warnings,
        }


@dataclass
class TimeAdjustment:
    """Suggested time adjustment for partial availability."""

    original_start: time
    original_end: time
    adjusted_start: time
    adjusted_end: time
    adjusted_net_hours: float
    availability_conflict: str  # Description of what conflicts
    score: float  # How well it works (0-100)


@dataclass
class SwapSuggestion:
    """Suggested shift swap where both parties benefit."""

    shift_a_id: str
    shift_a_employee_id: str
    shift_b_id: str
    shift_b_employee_id: str
    benefit_a: str  # Why it's good for A
    benefit_b: str  # Why it's good for B
    total_score: float  # Combined benefit (0-100)
    cost_delta: Decimal  # Cost change (negative = savings)


@dataclass
class ResolutionPlan:
    """A plan for resolving a single conflict."""

    shift_id: str
    original_employee_id: str
    recommended_employee_id: Optional[str] = None
    reason: str = ""
    alternatives: List[AlternativeOption] = field(default_factory=list)
    suggested_swap: Optional[SwapSuggestion] = None
    suggested_time_adjustment: Optional[TimeAdjustment] = None


@dataclass
class CoverageReport:
    """Availability coverage for a specific day."""

    venue_id: str
    date: date
    hours_coverage: Dict[int, List[str]] = field(default_factory=dict)  # hour -> [emp_ids]
    gaps: List[Tuple[int, int]] = field(default_factory=list)  # [(start_hour, end_hour), ...]
    available_staff: Dict[str, List[Tuple[int, int]]] = field(default_factory=dict)  # emp_id -> [(start, end), ...]
    understaffed_hours: List[int] = field(default_factory=list)
    min_required: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "venue_id": self.venue_id,
            "date": self.date.isoformat(),
            "hours_coverage": {str(h): emp_ids for h, emp_ids in self.hours_coverage.items()},
            "gaps": [[s, e] for s, e in self.gaps],
            "available_staff": {
                emp_id: [[s, e] for s, e in ranges]
                for emp_id, ranges in self.available_staff.items()
            },
            "understaffed_hours": self.understaffed_hours,
            "min_required": self.min_required,
        }


# ============================================================================
# AvailabilityResolver
# ============================================================================


class AvailabilityResolver:
    """Resolves availability conflicts by suggesting alternative employees."""

    # Scoring weights (sum to 1.0)
    WEIGHTS = {
        "cost": 0.30,
        "fairness": 0.25,
        "preference": 0.20,
        "skill": 0.15,
        "availability_fit": 0.10,
    }

    def __init__(self):
        self.db = get_db()
        self.preference_learner = PreferenceLearner()
        self.costing = EmployeeCostingService()

    # ========================================================================
    # Main resolution methods
    # ========================================================================

    def find_alternatives(
        self,
        shift: Shift,
        current_employee: Employee,
        venue_id: str,
        roster_context: Optional[Roster] = None,
    ) -> List[AlternativeOption]:
        """
        Find and rank alternative employees for a conflicting shift.

        Scoring factors:
        - Skill match (required role in employee.skills): mandatory filter
        - Availability (employee available during shift time): mandatory filter
        - Cost impact: Decimal difference vs current assignment (lower = better)
        - Fairness score: how balanced are hours across team (prefer underutilised)
        - Preference score: employee's preference for this shift pattern
        - Consecutive days: won't breach limit
        - Max hours: won't exceed weekly max

        Args:
            shift: The shift needing reassignment
            current_employee: Current (conflicting) employee
            venue_id: Venue ID to search within
            roster_context: Optional roster for weekly context

        Returns:
            List of AlternativeOption sorted by overall_score (descending)
        """
        logger.info(f"Finding alternatives for shift {shift.id}, current: {current_employee.id}")

        # Get all employees
        all_employees = self.db.list_employees()
        alternatives: List[AlternativeOption] = []

        # Calculate current cost
        current_cost = shift.cost or self._estimate_shift_cost(shift, current_employee)

        for candidate in all_employees:
            # Skip current employee
            if candidate.id == current_employee.id:
                continue

            # Mandatory filter: skill match
            if shift.role not in candidate.skills:
                continue

            # Mandatory filter: availability
            if not self._is_available(candidate, shift):
                continue

            # Calculate scores
            skill_score = self._score_skill_match(candidate, shift)
            availability_score = self._score_availability_fit(candidate, shift)

            # Get employee's shifts this week for context
            employee_shifts = self._get_employee_shifts_this_week(
                candidate.id,
                shift.date,
                roster_context,
            )

            hours_this_week = sum(s.net_hours for s in employee_shifts)
            hours_after = hours_this_week + shift.net_hours

            # Cost impact
            candidate_cost = self._estimate_shift_cost(shift, candidate)
            cost_impact = candidate_cost - current_cost
            cost_score = self._score_cost(cost_impact)

            # Fairness score
            fairness_score = self._score_fairness(
                candidate,
                venue_id,
                hours_after,
            )

            # Preference score
            temp_shift = Shift(
                id=shift.id,
                employee_id=candidate.id,
                date=shift.date,
                start_time=shift.start_time,
                end_time=shift.end_time,
                break_minutes=shift.break_minutes,
                status=shift.status,
                role=shift.role,
                cost=candidate_cost,
            )
            preference_score = self._score_preference(candidate.id, temp_shift)

            # Check constraints
            warnings = self._check_assignment_constraints(
                candidate,
                shift,
                hours_after,
                employee_shifts,
            )

            consecutive_days = self._count_consecutive_days(candidate, shift, employee_shifts)

            # Weighted overall score
            overall_score = (
                cost_score * self.WEIGHTS["cost"] +
                fairness_score * self.WEIGHTS["fairness"] +
                preference_score * self.WEIGHTS["preference"] +
                skill_score * self.WEIGHTS["skill"] +
                availability_score * self.WEIGHTS["availability_fit"]
            )

            # Penalize for warnings
            if warnings:
                overall_score *= (1.0 - (len(warnings) * 0.05))

            option = AlternativeOption(
                employee=candidate,
                overall_score=max(0, min(100, overall_score)),
                cost_impact=cost_impact,
                skill_match_score=skill_score,
                fairness_score=fairness_score,
                preference_score=preference_score,
                availability_fit_score=availability_score,
                hours_this_week=hours_this_week,
                hours_after=hours_after,
                consecutive_days_after=consecutive_days,
                warnings=warnings,
            )
            alternatives.append(option)

        # Sort by overall score descending
        alternatives.sort(key=lambda x: x.overall_score, reverse=True)
        logger.info(f"Found {len(alternatives)} valid alternatives")

        return alternatives

    def suggest_time_adjustment(
        self,
        shift: Shift,
        employee: Employee,
    ) -> List[TimeAdjustment]:
        """
        Suggest time adjustments for an employee with partial availability.

        If employee is available outside the shift window but has gaps within it,
        suggest adjusted start/end times.

        Args:
            shift: The shift to adjust
            employee: The employee with partial availability

        Returns:
            List of TimeAdjustment options, sorted by score
        """
        logger.info(f"Suggesting time adjustments for shift {shift.id}, employee {employee.id}")

        if not employee.availability:
            return []

        day_name = shift.date.strftime("%A").lower()
        available_ranges = employee.availability.get(day_name, [])

        if not available_ranges:
            return []

        adjustments: List[TimeAdjustment] = []

        for avail_range in available_ranges:
            avail_start = self._parse_time(avail_range.get("start"))
            avail_end = self._parse_time(avail_range.get("end"))

            if not avail_start or not avail_end:
                continue

            # Try to fit within available range
            adjusted_start = max(shift.start_time, avail_start)
            adjusted_end = min(shift.end_time, avail_end)

            # Skip if no overlap
            if adjusted_start >= adjusted_end:
                continue

            # Calculate new net hours
            duration_mins = (adjusted_end.hour * 60 + adjusted_end.minute) - \
                           (adjusted_start.hour * 60 + adjusted_start.minute)
            adjusted_net_hours = max(0, (duration_mins - shift.break_minutes) / 60.0)

            # Skip if too short
            min_hours = get_minimum_engagement_hours(employee.employment_type)
            if adjusted_net_hours < min_hours:
                continue

            # Describe conflicts
            conflicts = []
            if adjusted_start > shift.start_time:
                conflicts.append(f"start {(adjusted_start.hour * 60 + adjusted_start.minute - shift.start_time.hour * 60 - shift.start_time.minute) // 60}min later")
            if adjusted_end < shift.end_time:
                conflicts.append(f"end {(shift.end_time.hour * 60 + shift.end_time.minute - adjusted_end.hour * 60 - adjusted_end.minute) // 60}min earlier")

            availability_conflict = ", ".join(conflicts) if conflicts else "none"

            # Score: prefer minimal adjustment
            total_adjustment = abs((adjusted_start.hour * 60 + adjusted_start.minute) - (shift.start_time.hour * 60 + shift.start_time.minute)) + \
                             abs((adjusted_end.hour * 60 + adjusted_end.minute) - (shift.end_time.hour * 60 + shift.end_time.minute))
            score = max(0, 100 - (total_adjustment / 5))  # 5min adjustment = -1 point

            adjustment = TimeAdjustment(
                original_start=shift.start_time,
                original_end=shift.end_time,
                adjusted_start=adjusted_start,
                adjusted_end=adjusted_end,
                adjusted_net_hours=adjusted_net_hours,
                availability_conflict=availability_conflict,
                score=score,
            )
            adjustments.append(adjustment)

        # Sort by score descending
        adjustments.sort(key=lambda x: x.score, reverse=True)
        return adjustments

    def suggest_shift_swap(
        self,
        shift_a: Shift,
        employee_a: Employee,
        venue_id: str,
    ) -> List[SwapSuggestion]:
        """
        Find beneficial shift swaps where both employees win.

        Looks for shifts assigned to other employees where:
        - Employee A can work shift B (has skills, available)
        - Employee B can work shift A (has skills, available)
        - Both parties benefit (prefer their new shift more, or reduce fairness violation)

        Args:
            shift_a: The problematic shift
            employee_a: Current employee on shift A
            venue_id: Venue to search

        Returns:
            List of SwapSuggestion sorted by total_score
        """
        logger.info(f"Suggesting swaps for shift {shift_a.id}")

        suggestions: List[SwapSuggestion] = []

        # Get all rosters and extract shifts for the venue
        all_rosters = self.db.list_rosters()
        all_rosters = [r for r in all_rosters if r.venue_id == venue_id]
        all_shifts = []
        for roster in all_rosters:
            all_shifts.extend(roster.shifts)

        for shift_b in all_shifts:
            # Skip same shift
            if shift_b.id == shift_a.id:
                continue

            # Skip cancelled/completed shifts
            if shift_b.status in [ShiftStatus.completed, ShiftStatus.cancelled]:
                continue

            employee_b = self.db.get_employee(shift_b.employee_id)
            if not employee_b:
                continue

            # Check if A can do B and B can do A
            if shift_a.role not in employee_b.skills:
                continue
            if shift_b.role not in employee_a.skills:
                continue

            if not self._is_available(employee_b, shift_a):
                continue
            if not self._is_available(employee_a, shift_b):
                continue

            # Calculate benefits
            a_happiness_before = self.preference_learner.predict_happiness(employee_a.id, shift_a)
            b_happiness_before = self.preference_learner.predict_happiness(employee_b.id, shift_b)

            a_shift_b = Shift(
                id=shift_b.id,
                employee_id=employee_a.id,
                date=shift_b.date,
                start_time=shift_b.start_time,
                end_time=shift_b.end_time,
                break_minutes=shift_b.break_minutes,
                status=shift_b.status,
                role=shift_b.role,
                cost=shift_b.cost,
            )

            b_shift_a = Shift(
                id=shift_a.id,
                employee_id=employee_b.id,
                date=shift_a.date,
                start_time=shift_a.start_time,
                end_time=shift_a.end_time,
                break_minutes=shift_a.break_minutes,
                status=shift_a.status,
                role=shift_a.role,
                cost=shift_a.cost,
            )

            a_happiness_after = self.preference_learner.predict_happiness(employee_a.id, a_shift_b)
            b_happiness_after = self.preference_learner.predict_happiness(employee_b.id, b_shift_a)

            a_improvement = (a_happiness_after - a_happiness_before) * 100
            b_improvement = (b_happiness_after - b_happiness_before) * 100

            # Both must benefit or at least not hurt
            if a_improvement < -5 or b_improvement < -5:
                continue

            # Cost delta
            cost_a = shift_a.cost or self._estimate_shift_cost(shift_a, employee_a)
            cost_b = shift_b.cost or self._estimate_shift_cost(shift_b, employee_b)
            cost_a_new = self._estimate_shift_cost(a_shift_b, employee_a)
            cost_b_new = self._estimate_shift_cost(b_shift_a, employee_b)
            cost_delta = (cost_a_new + cost_b_new) - (cost_a + cost_b)

            total_score = (a_improvement + b_improvement) / 2

            # Boost score if saves cost
            if cost_delta < 0:
                total_score += min(10, abs(float(cost_delta)))

            suggestion = SwapSuggestion(
                shift_a_id=shift_a.id,
                shift_a_employee_id=employee_a.id,
                shift_b_id=shift_b.id,
                shift_b_employee_id=employee_b.id,
                benefit_a=f"Happiness: {a_improvement:+.0f}%",
                benefit_b=f"Happiness: {b_improvement:+.0f}%",
                total_score=min(100, total_score),
                cost_delta=cost_delta,
            )
            suggestions.append(suggestion)

        suggestions.sort(key=lambda x: x.total_score, reverse=True)
        return suggestions

    def bulk_resolve(
        self,
        conflicts: List[dict],
        venue_id: str,
    ) -> List[ResolutionPlan]:
        """
        Resolve multiple conflicts optimally without reassigning same person twice.

        Args:
            conflicts: List of {shift_id, employee_id, reason}
            venue_id: Venue ID

        Returns:
            List of ResolutionPlan objects
        """
        logger.info(f"Bulk resolving {len(conflicts)} conflicts for venue {venue_id}")

        plans: List[ResolutionPlan] = []
        assigned_in_batch: Set[str] = set()  # employee_ids already reassigned

        for conflict in conflicts:
            shift_id = conflict.get("shift_id")
            employee_id = conflict.get("employee_id")
            reason = conflict.get("reason", "conflict detected")

            shift = self.db.get_shift(shift_id)
            employee = self.db.get_employee(employee_id)

            if not shift or not employee:
                logger.warning(f"Shift {shift_id} or employee {employee_id} not found")
                continue

            # Find alternatives, excluding those already reassigned
            alternatives = self.find_alternatives(shift, employee, venue_id)
            alternatives = [
                alt for alt in alternatives
                if alt.employee.id not in assigned_in_batch
            ]

            recommended_employee = None
            if alternatives:
                recommended_employee = alternatives[0].employee.id
                assigned_in_batch.add(recommended_employee)

            plan = ResolutionPlan(
                shift_id=shift_id,
                original_employee_id=employee_id,
                recommended_employee_id=recommended_employee,
                reason=reason,
                alternatives=alternatives[:5],  # Top 5
            )
            plans.append(plan)

        logger.info(f"Generated {len(plans)} resolution plans")
        return plans

    def get_availability_coverage(
        self,
        venue_id: str,
        target_date: date,
    ) -> CoverageReport:
        """
        For a given day, show coverage gaps and available staff per hour.

        Args:
            venue_id: Venue ID
            target_date: Date to analyze

        Returns:
            CoverageReport with hourly breakdown
        """
        logger.info(f"Analyzing coverage for venue {venue_id} on {target_date}")

        report = CoverageReport(venue_id=venue_id, date=target_date)

        # Get all rosters for this venue and extract shifts for the day
        all_rosters = self.db.list_rosters()
        venue_rosters = [r for r in all_rosters if r.venue_id == venue_id]
        shifts_today = []
        for roster in venue_rosters:
            shifts_today.extend([
                s for s in roster.shifts
                if s.date == target_date and s.status != ShiftStatus.cancelled
            ])

        # Get all employees
        employees = self.db.list_employees()
        employee_dict = {e.id: e for e in employees}

        # Get venue min staffing
        venue = self.db.get_venue(venue_id)
        report.min_required = min(venue.min_staff.values()) if venue and venue.min_staff else 1

        # Build hourly coverage
        for hour in range(24):
            report.hours_coverage[hour] = []
            for shift in shifts_today:
                # Check if shift covers this hour
                start_min = shift.start_time.hour * 60 + shift.start_time.minute
                end_min = shift.end_time.hour * 60 + shift.end_time.minute
                hour_min = hour * 60

                if end_min < start_min:  # overnight
                    end_min += 24 * 60

                if start_min <= hour_min < min(end_min, hour_min + 60):
                    report.hours_coverage[hour].append(shift.employee_id)

        # Identify gaps
        gap_start = None
        for hour in range(24):
            coverage_count = len(report.hours_coverage.get(hour, []))
            if coverage_count < report.min_required:
                if gap_start is None:
                    gap_start = hour
            else:
                if gap_start is not None:
                    report.gaps.append((gap_start, hour))
                    gap_start = None

        if gap_start is not None:
            report.gaps.append((gap_start, 24))

        # Find available staff (not scheduled)
        scheduled_ids = {shift.employee_id for shift in shifts_today}

        for employee in employees:
            if employee.id in scheduled_ids:
                continue  # Already scheduled

            # Check availability for each hour
            available_ranges: List[Tuple[int, int]] = []
            day_name = target_date.strftime("%A").lower()
            avail_ranges = employee.availability.get(day_name, [])

            for avail_range in avail_ranges:
                start = self._parse_time(avail_range.get("start"))
                end = self._parse_time(avail_range.get("end"))

                if start and end:
                    start_hour = start.hour
                    end_hour = end.hour
                    available_ranges.append((start_hour, end_hour))

            if available_ranges:
                report.available_staff[employee.id] = available_ranges

        report.understaffed_hours = [
            hour for hour in range(24)
            if len(report.hours_coverage.get(hour, [])) < report.min_required
        ]

        return report

    # ========================================================================
    # Scoring methods
    # ========================================================================

    def _score_skill_match(self, employee: Employee, shift: Shift) -> float:
        """Score how well employee's skills match the shift (0-100)."""
        if shift.role in employee.skills:
            return 100.0
        return 0.0

    def _score_availability_fit(self, employee: Employee, shift: Shift) -> float:
        """Score how well employee's availability fits the shift (0-100)."""
        if not self._is_available(employee, shift):
            return 0.0

        # Perfect fit if exact availability match
        if employee.availability:
            day_name = shift.date.strftime("%A").lower()
            ranges = employee.availability.get(day_name, [])
            for r in ranges:
                start = self._parse_time(r.get("start"))
                end = self._parse_time(r.get("end"))
                if start and end and start <= shift.start_time and shift.end_time <= end:
                    return 100.0

        return 80.0  # Good fit if available but not perfect match

    def _score_cost(self, cost_impact: Decimal) -> float:
        """
        Score cost impact.

        0 = no cost difference (100 points)
        Negative values (savings) = bonus
        Positive values (more expensive) = penalty
        """
        cost_float = float(cost_impact)
        if cost_float < 0:
            # Savings: up to 20 bonus points
            return min(120, 100 + (abs(cost_float) / 10))
        else:
            # Extra cost: penalty
            return max(0, 100 - (cost_float / 10))

    def _score_fairness(
        self,
        employee: Employee,
        venue_id: str,
        hours_after: float,
    ) -> float:
        """
        Score fairness based on hour distribution.

        Employees with fewer hours are preferred (more fair distribution).
        """
        team_employees = self.db.list_employees_by_venue(venue_id)
        if not team_employees:
            return 50.0

        # Get average hours
        all_hours = []
        for emp in team_employees:
            shifts = self.db.list_shifts_by_employee(emp.id)
            hours = sum(s.net_hours for s in shifts)
            all_hours.append(hours)

        avg_hours = sum(all_hours) / len(all_hours) if all_hours else 0

        # Score: prefer employees closer to average
        diff = abs(hours_after - avg_hours)
        return max(0, 100 - diff * 5)

    def _score_preference(self, employee_id: str, shift: Shift) -> float:
        """Score employee's preference for this shift (0-100)."""
        happiness = self.preference_learner.predict_happiness(employee_id, shift)
        return happiness * 100

    # ========================================================================
    # Helper methods
    # ========================================================================

    def _is_available(self, employee: Employee, shift: Shift) -> bool:
        """Check if employee is available for the shift."""
        if not employee.availability:
            return True  # No constraints

        day_name = shift.date.strftime("%A").lower()
        available_ranges = employee.availability.get(day_name, [])

        if not available_ranges:
            return False  # Not available on this day

        for avail_range in available_ranges:
            start = self._parse_time(avail_range.get("start"))
            end = self._parse_time(avail_range.get("end"))

            if start and end and start <= shift.start_time and shift.end_time <= end:
                return True

        return False

    def _check_assignment_constraints(
        self,
        employee: Employee,
        shift: Shift,
        hours_after: float,
        employee_shifts: List[Shift],
    ) -> List[str]:
        """Check constraints and return list of warnings."""
        warnings = []

        # Check max hours
        if hours_after > employee.max_hours_per_week:
            warnings.append(
                f"Exceeds max {employee.max_hours_per_week}h/week by "
                f"{hours_after - employee.max_hours_per_week:.1f}h"
            )

        # Check consecutive days
        consecutive = self._count_consecutive_days(employee, shift, employee_shifts)
        if consecutive > employee.consecutive_days_limit:
            warnings.append(
                f"Would reach {consecutive} consecutive days "
                f"(limit: {employee.consecutive_days_limit})"
            )

        # Check fatigue (rest between shifts)
        for emp_shift in employee_shifts:
            if emp_shift.date == shift.date:
                # Same day shift
                if not (emp_shift.end_time <= shift.start_time or shift.end_time <= emp_shift.start_time):
                    warnings.append(f"Overlaps with shift {emp_shift.id}")
            elif emp_shift.date == shift.date - timedelta(days=1):
                # Previous day
                hours_rest = (shift.start_time.hour + shift.start_time.minute / 60) - \
                            (emp_shift.end_time.hour + emp_shift.end_time.minute / 60)
                if hours_rest < MINIMUM_HOURS_BETWEEN_SHIFTS:
                    warnings.append(
                        f"Only {hours_rest:.1f}h rest from previous shift "
                        f"(minimum {MINIMUM_HOURS_BETWEEN_SHIFTS}h)"
                    )

        return warnings

    def _count_consecutive_days(
        self,
        employee: Employee,
        new_shift: Shift,
        employee_shifts: List[Shift],
    ) -> int:
        """Count consecutive days including the new shift."""
        all_shift_dates = {s.date for s in employee_shifts}
        all_shift_dates.add(new_shift.date)

        sorted_dates = sorted(all_shift_dates)

        # Find the streak containing the new shift date
        max_consecutive = 1
        current_consecutive = 1

        for i, d in enumerate(sorted_dates):
            if i > 0 and d == sorted_dates[i-1] + timedelta(days=1):
                current_consecutive += 1
            else:
                current_consecutive = 1

            max_consecutive = max(max_consecutive, current_consecutive)

        return max_consecutive

    def _get_employee_shifts_this_week(
        self,
        employee_id: str,
        reference_date: date,
        roster_context: Optional[Roster] = None,
    ) -> List[Shift]:
        """Get employee's shifts for the week containing reference_date."""
        if roster_context:
            return [
                s for s in roster_context.shifts
                if s.employee_id == employee_id
            ]

        # Find week bounds
        week_start = reference_date - timedelta(days=reference_date.weekday())
        week_end = week_start + timedelta(days=6)

        # Get all rosters and extract employee's shifts for the week
        all_rosters = self.db.list_rosters()
        employee_shifts = []
        for roster in all_rosters:
            if week_start <= roster.week_end and roster.week_start <= week_end:
                employee_shifts.extend([
                    s for s in roster.shifts
                    if s.employee_id == employee_id and week_start <= s.date <= week_end
                ])
        return employee_shifts

    def _estimate_shift_cost(self, shift: Shift, employee: Employee) -> Decimal:
        """Estimate shift cost for an employee."""
        if shift.cost:
            return shift.cost

        # Simple estimate: base rate * hours * penalty multiplier
        hours = shift.net_hours
        base = employee.hourly_base_rate * Decimal(str(hours))

        # Apply penalty multiplier if not base rate
        penalty_multiplier = shift.penalty_multiplier or 1.0
        total = base * Decimal(str(penalty_multiplier))

        # Add casual loading if applicable
        if employee.employment_type == EmploymentType.casual:
            total *= Decimal("1.25")

        return total

    @staticmethod
    def _parse_time(time_str: Optional[str]) -> Optional[time]:
        """Parse time string (HH:MM format) to time object."""
        if not time_str:
            return None
        try:
            parts = time_str.split(":")
            hours, minutes = int(parts[0]), int(parts[1])
            return time(hour=hours, minute=minutes)
        except (ValueError, AttributeError, IndexError):
            return None
