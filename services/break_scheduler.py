"""
Intelligent Break Scheduler for RosterIQ.

Implements MA000009 hospitality award break rules and optimises break timing
to maintain minimum staffing coverage while ensuring compliance.

Break Rules (MA000009):
- Shifts > 5 hours: 30-min unpaid meal break, must be taken within first 5 hours
- Shifts > 7.5 hours: additional 20-min paid rest break
- Shifts > 10 hours: second meal break (30-min unpaid)
- Break cannot start in first hour or last hour of shift
- Minimum 10-minute break between work periods
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, time
from decimal import Decimal
from typing import Optional, List, Dict, Tuple
import logging

from rosteriq.models import Shift, Roster, Employee

logger = logging.getLogger(__name__)


@dataclass
class BreakConfig:
    """Configuration for break scheduling."""
    meal_break_threshold_hours: float = 5.0
    meal_break_minutes: int = 30
    rest_break_threshold_hours: float = 7.5
    rest_break_minutes: int = 20
    second_meal_threshold_hours: float = 10.0
    second_meal_minutes: int = 30
    min_coverage_during_breaks: int = 1
    max_concurrent_breaks_at_coverage: int = None  # None = unlimited


@dataclass
class Break:
    """Represents a scheduled break."""
    employee_id: str
    shift_id: str
    break_type: str  # 'meal' or 'rest'
    start_time: datetime
    end_time: datetime
    is_paid: bool
    duration_minutes: int
    position_in_shift: str  # 'first', 'second', 'third'


@dataclass
class BreakViolation:
    """Represents a break compliance violation."""
    shift_id: str
    employee_id: str
    violation_type: str  # 'missing_break', 'break_too_late', 'insufficient_gap'
    details: str
    severity: str  # 'critical', 'warning'


@dataclass
class BreakComplianceReport:
    """Report on break compliance for a roster."""
    total_shifts: int = 0
    compliant_shifts: int = 0
    violations: List[BreakViolation] = field(default_factory=list)
    compliance_score: float = 0.0  # 0.0 to 1.0
    violations_by_type: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if self.total_shifts > 0:
            self.compliance_score = self.compliant_shifts / self.total_shifts


class BreakScheduler:
    """Schedules and validates breaks according to MA000009 rules."""

    def __init__(self, config: Optional[BreakConfig] = None):
        self.config = config or BreakConfig()

    def schedule_breaks(self, roster: Roster) -> Tuple[Roster, List[Break]]:
        """
        Insert breaks into roster shifts.

        Args:
            roster: The roster to process

        Returns:
            Tuple of (updated_roster, list_of_breaks)
        """
        breaks_scheduled: List[Break] = []

        for shift in roster.shifts:
            shift_breaks = self._calculate_required_breaks(shift)
            for break_obj in shift_breaks:
                breaks_scheduled.append(break_obj)

        # Check coverage constraints
        breaks_scheduled = self._stagger_breaks_for_coverage(
            roster, breaks_scheduled
        )

        # Update shifts with break information
        for shift in roster.shifts:
            shift_break_list = [b for b in breaks_scheduled if b.shift_id == shift.id]
            if shift_break_list:
                shift.breaks = shift_break_list

        roster.breaks = breaks_scheduled
        return roster, breaks_scheduled

    def _calculate_required_breaks(self, shift: Shift) -> List[Break]:
        """
        Determine required breaks for a single shift based on duration.

        Args:
            shift: The shift to calculate breaks for

        Returns:
            List of required breaks for this shift
        """
        breaks_list: List[Break] = []

        if shift.start_time is None or shift.end_time is None:
            return breaks_list

        shift_duration_hours = shift.duration_hours or 0

        # First meal break: required for shifts > 5 hours
        if shift_duration_hours > self.config.meal_break_threshold_hours:
            meal_break = Break(
                employee_id=shift.employee_id,
                shift_id=shift.id,
                break_type="meal",
                start_time=self._calculate_optimal_meal_break_time(shift),
                end_time=None,  # Will be set based on start_time
                is_paid=False,
                duration_minutes=self.config.meal_break_minutes,
                position_in_shift="first",
            )
            meal_break.end_time = meal_break.start_time + timedelta(
                minutes=meal_break.duration_minutes
            )
            breaks_list.append(meal_break)

        # Rest break: required for shifts > 7.5 hours
        if shift_duration_hours > self.config.rest_break_threshold_hours:
            rest_break = Break(
                employee_id=shift.employee_id,
                shift_id=shift.id,
                break_type="rest",
                start_time=self._calculate_optimal_rest_break_time(shift),
                end_time=None,
                is_paid=True,
                duration_minutes=self.config.rest_break_minutes,
                position_in_shift="first",
            )
            rest_break.end_time = rest_break.start_time + timedelta(
                minutes=rest_break.duration_minutes
            )
            breaks_list.append(rest_break)

        # Second meal break: required for shifts > 10 hours
        if shift_duration_hours > self.config.second_meal_threshold_hours:
            second_meal = Break(
                employee_id=shift.employee_id,
                shift_id=shift.id,
                break_type="meal",
                start_time=self._calculate_optimal_second_meal_break_time(shift),
                end_time=None,
                is_paid=False,
                duration_minutes=self.config.meal_break_minutes,
                position_in_shift="second",
            )
            second_meal.end_time = second_meal.start_time + timedelta(
                minutes=second_meal.duration_minutes
            )
            breaks_list.append(second_meal)

        return breaks_list

    def _calculate_optimal_meal_break_time(self, shift: Shift) -> datetime:
        """
        Calculate optimal time for meal break (within first 5 hours of shift).

        Break cannot start in first hour or last hour of shift.
        """
        if shift.start_time is None:
            return shift.start_time

        # Earliest: 1 hour after shift start
        earliest = shift.start_time + timedelta(hours=1)

        # Latest: first 5 hours of shift or 1 hour before end, whichever comes first
        five_hour_point = shift.start_time + timedelta(hours=5)
        one_hour_before_end = (shift.end_time or shift.start_time) - timedelta(hours=1)
        latest = min(five_hour_point, one_hour_before_end)

        # Return midpoint between earliest and latest
        if earliest < latest:
            return earliest + (latest - earliest) / 2
        else:
            # Fallback to earliest if invalid
            return earliest

    def _calculate_optimal_rest_break_time(self, shift: Shift) -> datetime:
        """
        Calculate optimal time for rest break (typically mid-shift).

        Rest break is usually taken after meal break.
        """
        if shift.start_time is None or shift.end_time is None:
            return shift.start_time

        # Aim for 2/3 through the shift
        duration = shift.end_time - shift.start_time
        two_thirds_point = shift.start_time + (duration * 2 / 3)

        # Ensure not in first or last hour
        earliest = shift.start_time + timedelta(hours=1)
        latest = shift.end_time - timedelta(hours=1)

        if two_thirds_point < earliest:
            return earliest
        elif two_thirds_point > latest:
            return latest
        else:
            return two_thirds_point

    def _calculate_optimal_second_meal_break_time(self, shift: Shift) -> datetime:
        """
        Calculate optimal time for second meal break (late in shift).

        Second meal break typically in final portion of shift.
        """
        if shift.start_time is None or shift.end_time is None:
            return shift.start_time

        # Aim for 4/5 through the shift
        duration = shift.end_time - shift.start_time
        four_fifths_point = shift.start_time + (duration * 4 / 5)

        # Ensure not in first or last hour
        earliest = shift.start_time + timedelta(hours=1)
        latest = shift.end_time - timedelta(hours=1)

        if four_fifths_point < earliest:
            return earliest
        elif four_fifths_point > latest:
            return latest
        else:
            return four_fifths_point

    def _stagger_breaks_for_coverage(
        self, roster: Roster, breaks_list: List[Break]
    ) -> List[Break]:
        """
        Adjust break timing to ensure minimum coverage is maintained.

        Greedy algorithm: stagger breaks to avoid 2+ simultaneous breaks
        if that would drop below minimum coverage.
        """
        if not breaks_list:
            return breaks_list

        # Group breaks by time period and count concurrent staff
        break_windows: Dict[datetime, List[Break]] = {}

        for brk in breaks_list:
            key = brk.start_time
            if key not in break_windows:
                break_windows[key] = []
            break_windows[key].append(brk)

        # For each time window with multiple concurrent breaks,
        # stagger them slightly
        adjusted_breaks: List[Break] = []
        for window_time, window_breaks in break_windows.items():
            if len(window_breaks) <= self.config.min_coverage_during_breaks:
                adjusted_breaks.extend(window_breaks)
            else:
                # Stagger breaks in 5-minute increments
                for i, brk in enumerate(window_breaks):
                    staggered_time = window_time + timedelta(minutes=i * 5)
                    # Ensure we don't go past the safe window
                    shift = next(s for s in roster.shifts if s.id == brk.shift_id)
                    if shift.end_time and (
                        staggered_time + timedelta(minutes=brk.duration_minutes)
                        < shift.end_time - timedelta(minutes=59)
                    ):
                        brk.start_time = staggered_time
                        brk.end_time = staggered_time + timedelta(
                            minutes=brk.duration_minutes
                        )
                    adjusted_breaks.append(brk)

        return adjusted_breaks

    def validate_breaks(self, roster: Roster) -> BreakComplianceReport:
        """
        Validate break compliance for all shifts in roster.

        Returns:
            BreakComplianceReport with violations and compliance score
        """
        report = BreakComplianceReport()
        report.total_shifts = len(roster.shifts)

        for shift in roster.shifts:
            violations = self._validate_shift_breaks(shift)
            if not violations:
                report.compliant_shifts += 1
            else:
                report.violations.extend(violations)
                for v in violations:
                    report.violations_by_type[v.violation_type] = (
                        report.violations_by_type.get(v.violation_type, 0) + 1
                    )

        report.compliance_score = (
            report.compliant_shifts / report.total_shifts
            if report.total_shifts > 0
            else 0.0
        )

        return report

    def _validate_shift_breaks(self, shift: Shift) -> List[BreakViolation]:
        """Check if a single shift meets break requirements."""
        violations: List[BreakViolation] = []

        if shift.start_time is None or shift.end_time is None:
            return violations

        shift_duration_hours = shift.duration_hours or 0
        shift_breaks = shift.breaks or []

        # Check meal break (> 5 hours)
        if shift_duration_hours > self.config.meal_break_threshold_hours:
            meal_breaks = [b for b in shift_breaks if b.break_type == "meal"]
            if not meal_breaks:
                violations.append(
                    BreakViolation(
                        shift_id=shift.id,
                        employee_id=shift.employee_id,
                        violation_type="missing_break",
                        details=f"Shift {shift_duration_hours:.1f}h requires meal break",
                        severity="critical",
                    )
                )
            else:
                # Validate meal break is within first 5 hours
                for meal_break in meal_breaks:
                    if meal_break.start_time is None:
                        continue
                    hours_in = (meal_break.start_time - shift.start_time).total_seconds() / 3600
                    if hours_in > 5:
                        violations.append(
                            BreakViolation(
                                shift_id=shift.id,
                                employee_id=shift.employee_id,
                                violation_type="break_too_late",
                                details=f"Meal break at {hours_in:.1f}h exceeds 5h threshold",
                                severity="critical",
                            )
                        )

        # Check rest break (> 7.5 hours)
        if shift_duration_hours > self.config.rest_break_threshold_hours:
            rest_breaks = [b for b in shift_breaks if b.break_type == "rest"]
            if not rest_breaks:
                violations.append(
                    BreakViolation(
                        shift_id=shift.id,
                        employee_id=shift.employee_id,
                        violation_type="missing_break",
                        details=f"Shift {shift_duration_hours:.1f}h requires rest break",
                        severity="critical",
                    )
                )

        # Check second meal break (> 10 hours)
        if shift_duration_hours > self.config.second_meal_threshold_hours:
            meal_breaks = [b for b in shift_breaks if b.break_type == "meal"]
            if len(meal_breaks) < 2:
                violations.append(
                    BreakViolation(
                        shift_id=shift.id,
                        employee_id=shift.employee_id,
                        violation_type="missing_break",
                        details=f"Shift {shift_duration_hours:.1f}h requires two meal breaks",
                        severity="critical",
                    )
                )

        # Check minimum gap between breaks (10 minutes)
        sorted_breaks = sorted(shift_breaks, key=lambda b: b.start_time or shift.start_time)
        for i in range(len(sorted_breaks) - 1):
            current_end = sorted_breaks[i].end_time or shift.start_time
            next_start = sorted_breaks[i + 1].start_time or shift.start_time
            gap_minutes = (next_start - current_end).total_seconds() / 60
            if gap_minutes < self.config.min_coverage_during_breaks * 10:
                violations.append(
                    BreakViolation(
                        shift_id=shift.id,
                        employee_id=shift.employee_id,
                        violation_type="insufficient_gap",
                        details=f"Only {gap_minutes:.0f}min gap between breaks",
                        severity="warning",
                    )
                )

        return violations

    def optimise_break_timing(
        self,
        shifts: List[Shift],
        demand_curve: Dict[int, float],
    ) -> Dict[str, datetime]:
        """
        Optimise break timing given concurrent shifts and hourly demand forecast.

        Algorithm:
        1. Identify low-demand hours in the demand_curve
        2. Assign breaks to lowest-demand windows first (greedy)
        3. Stagger breaks to maintain minimum coverage

        Args:
            shifts: List of concurrent shifts to schedule breaks for
            demand_curve: Dict mapping hour (0-23) to demand (0.0-1.0)

        Returns:
            Dict mapping shift_id to optimal break start time
        """
        break_times: Dict[str, datetime] = {}

        if not shifts or not demand_curve:
            return break_times

        # Sort hours by demand (lowest first)
        sorted_hours = sorted(demand_curve.items(), key=lambda x: x[1])

        # Assign each shift to a break time in a low-demand hour
        for shift, (hour, _demand) in zip(shifts, sorted_hours):
            if shift.start_time is None or shift.end_time is None:
                continue

            # Convert hour to actual datetime within the shift
            shift_date = shift.start_time.date()
            hour_time = datetime.combine(shift_date, time(hour=hour, minute=0))

            # Ensure the break time is within shift bounds and not in first/last hour
            earliest = shift.start_time + timedelta(hours=1)
            latest = shift.end_time - timedelta(hours=1)

            if earliest <= hour_time <= latest:
                break_times[shift.id] = hour_time
            else:
                # Fallback to calculated optimal time
                break_times[shift.id] = self._calculate_optimal_meal_break_time(shift)

        return break_times
