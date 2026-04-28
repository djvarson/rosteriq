"""
Conflict Detection Engine for RosterIQ.

Detects compliance and operational conflicts in rosters, including:
- Double bookings
- Availability violations
- Overtime breaches
- Consecutive days violations
- Skill mismatches
- Minimum engagement violations
- Shift length violations
- Break requirement violations
- Understaffing/overstaffing by hour
- Fatigue risk (insufficient rest between shifts)

Used by the roster optimiser and compliance checker.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Set, Tuple
import logging
from collections import defaultdict

from rosteriq.models import (
    Shift, Roster, Employee, VenueConfig, EmploymentType, ShiftStatus, State
)
from rosteriq.award_rules import (
    get_minimum_break_minutes, get_minimum_engagement_hours,
    MAX_SHIFT_LENGTH_HOURS, MINIMUM_HOURS_BETWEEN_SHIFTS, MAX_CONSECUTIVE_DAYS,
    get_day_type, DayType,
)

logger = logging.getLogger(__name__)


class ConflictType(str, Enum):
    """Types of roster conflicts detected."""
    DOUBLE_BOOKING = "double_booking"
    AVAILABILITY_VIOLATION = "availability_violation"
    OVERTIME_BREACH = "overtime_breach"
    CONSECUTIVE_DAYS_VIOLATION = "consecutive_days_violation"
    SKILL_MISMATCH = "skill_mismatch"
    MINIMUM_ENGAGEMENT = "minimum_engagement"
    MAX_SHIFT_LENGTH = "max_shift_length"
    BREAK_VIOLATION = "break_violation"
    UNDERSTAFFED_HOUR = "understaffed_hour"
    OVERSTAFFED_HOUR = "overstaffed_hour"
    FATIGUE_RISK = "fatigue_risk"


class ConflictSeverity(str, Enum):
    """Severity levels for conflicts."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class RosterConflict:
    """Represents a detected conflict in a roster."""
    conflict_type: ConflictType
    severity: ConflictSeverity
    message: str
    employee_ids: List[str] = field(default_factory=list)
    shift_ids: List[str] = field(default_factory=list)
    hour: Optional[int] = None
    date: Optional[date] = None
    suggestion: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert conflict to dictionary for JSON serialization."""
        return {
            "conflict_type": self.conflict_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "employee_ids": self.employee_ids,
            "shift_ids": self.shift_ids,
            "hour": self.hour,
            "date": self.date.isoformat() if self.date else None,
            "suggestion": self.suggestion,
        }


class ConflictDetector:
    """Detects various types of conflicts in a roster."""

    def __init__(self):
        self.conflicts: List[RosterConflict] = []

    def detect_conflicts(
        self,
        roster: Roster,
        venue: VenueConfig,
        employees: List[Employee],
    ) -> List[RosterConflict]:
        """
        Detect all conflicts in the roster.

        Args:
            roster: The roster to check
            venue: Venue configuration with staffing requirements
            employees: List of all employees for reference

        Returns:
            List of detected conflicts
        """
        self.conflicts = []
        employees_dict = {e.id: e for e in employees}

        shifts_by_employee: Dict[str, List[Shift]] = {}
        for shift in roster.shifts:
            if shift.employee_id not in shifts_by_employee:
                shifts_by_employee[shift.employee_id] = []
            shifts_by_employee[shift.employee_id].append(shift)

        for shift in roster.shifts:
            employee = employees_dict.get(shift.employee_id)
            if not employee:
                logger.warning(f"Employee {shift.employee_id} not found")
                continue

            emp_shifts = shifts_by_employee[shift.employee_id]

            self._check_double_booking(shift, emp_shifts)
            self._check_availability_violation(shift, employee)
            self._check_skill_mismatch(shift, employee)
            self._check_minimum_engagement(shift, employee)
            self._check_max_shift_length(shift)
            self._check_break_violation(shift)
            self._check_fatigue_risk(shift, emp_shifts)

        for employee_id, emp_shifts in shifts_by_employee.items():
            employee = employees_dict.get(employee_id)
            if employee:
                self._check_overtime_breach(employee, emp_shifts, roster)
                self._check_consecutive_days_violation(employee, emp_shifts)

        self._check_staffing_levels(roster, venue, employees_dict)

        return self.conflicts

    def _check_double_booking(self, shift: Shift, employee_shifts: List[Shift]) -> None:
        """Check if employee is assigned overlapping shifts on the same day."""
        same_day_shifts = [s for s in employee_shifts if s.date == shift.date and s.id != shift.id]

        for other in same_day_shifts:
            if self._shifts_overlap(shift, other):
                self.conflicts.append(RosterConflict(
                    conflict_type=ConflictType.DOUBLE_BOOKING,
                    severity=ConflictSeverity.CRITICAL,
                    message=(
                        f"Employee {shift.employee_id} double-booked on {shift.date}: "
                        f"{shift.start_time}-{shift.end_time} overlaps with {other.start_time}-{other.end_time}"
                    ),
                    employee_ids=[shift.employee_id],
                    shift_ids=[shift.id, other.id],
                    date=shift.date,
                    suggestion=f"Remove one of the overlapping shifts or reschedule"
                ))

    def _check_availability_violation(self, shift: Shift, employee: Employee) -> None:
        """Check if shift falls outside employee's stated availability."""
        if not employee.availability:
            return

        day_name = shift.date.strftime("%A").lower()
        available_ranges = employee.availability.get(day_name, [])

        if not available_ranges:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.AVAILABILITY_VIOLATION,
                severity=ConflictSeverity.CRITICAL,
                message=(
                    f"Employee {employee.id} not available on {day_name}. "
                    f"Assigned shift: {shift.start_time}-{shift.end_time}"
                ),
                employee_ids=[employee.id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Reschedule to a day when employee is available"
            ))
            return

        shift_in_available_range = False
        for avail_range in available_ranges:
            start_time = self._parse_time(avail_range.get("start"))
            end_time = self._parse_time(avail_range.get("end"))

            if start_time and end_time:
                if start_time <= shift.start_time and shift.end_time <= end_time:
                    shift_in_available_range = True
                    break

        if not shift_in_available_range:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.AVAILABILITY_VIOLATION,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Employee {employee.id} shift {shift.start_time}-{shift.end_time} "
                    f"falls outside availability on {day_name}"
                ),
                employee_ids=[employee.id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Adjust shift times to match availability or confirm with employee"
            ))

    def _check_skill_mismatch(self, shift: Shift, employee: Employee) -> None:
        """Check if employee has the required skill for the shift role."""
        if not employee.skills or not shift.role:
            return

        if shift.role not in employee.skills:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.SKILL_MISMATCH,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Employee {employee.id} lacks required skill '{shift.role}'. "
                    f"Has skills: {', '.join(employee.skills)}"
                ),
                employee_ids=[employee.id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Assign employee with skill '{shift.role}' or train employee"
            ))

    def _check_minimum_engagement(self, shift: Shift, employee: Employee) -> None:
        """Check if shift meets minimum engagement hours for employment type."""
        min_hours = get_minimum_engagement_hours(employee.employment_type)
        if shift.net_hours < min_hours:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.MINIMUM_ENGAGEMENT,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Shift {shift.id} is {shift.net_hours:.1f}h, "
                    f"below {min_hours}h minimum for {employee.employment_type.value}"
                ),
                employee_ids=[employee.id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Extend shift by at least {min_hours - shift.net_hours:.1f}h"
            ))

    def _check_max_shift_length(self, shift: Shift) -> None:
        """Check if shift exceeds maximum length (11.5 hours)."""
        if shift.duration_hours > MAX_SHIFT_LENGTH_HOURS:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.MAX_SHIFT_LENGTH,
                severity=ConflictSeverity.CRITICAL,
                message=(
                    f"Shift {shift.id} is {shift.duration_hours:.1f}h, "
                    f"exceeds maximum {MAX_SHIFT_LENGTH_HOURS}h"
                ),
                employee_ids=[shift.employee_id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Reduce shift length by at least {shift.duration_hours - MAX_SHIFT_LENGTH_HOURS:.1f}h"
            ))

    def _check_break_violation(self, shift: Shift) -> None:
        """Check if shift has required breaks based on duration."""
        required_break = get_minimum_break_minutes(shift.duration_hours)
        if shift.break_minutes < required_break:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.BREAK_VIOLATION,
                severity=ConflictSeverity.CRITICAL,
                message=(
                    f"Shift {shift.id} ({shift.duration_hours:.1f}h) has {shift.break_minutes}min break, "
                    f"needs {required_break}min"
                ),
                employee_ids=[shift.employee_id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Add {required_break - shift.break_minutes}min break"
            ))

    def _check_fatigue_risk(self, shift: Shift, employee_shifts: List[Shift]) -> None:
        """Check if employee has at least 10 hours rest between consecutive shifts."""
        previous_shifts = [s for s in employee_shifts if (s.date < shift.date) or (s.date == shift.date and s.end_time < shift.start_time)]

        if not previous_shifts:
            return

        previous_shift = max(previous_shifts, key=lambda s: (s.date, s.end_time))

        if previous_shift.date == shift.date:
            hours_rest = (shift.start_time.hour + shift.start_time.minute / 60) - (
                previous_shift.end_time.hour + previous_shift.end_time.minute / 60
            )
        else:
            hours_rest = 24 - (previous_shift.end_time.hour + previous_shift.end_time.minute / 60)
            hours_rest += shift.start_time.hour + shift.start_time.minute / 60

        if hours_rest < MINIMUM_HOURS_BETWEEN_SHIFTS:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.FATIGUE_RISK,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Only {hours_rest:.1f}h rest between shifts for {shift.employee_id}, "
                    f"minimum {MINIMUM_HOURS_BETWEEN_SHIFTS}h required"
                ),
                employee_ids=[shift.employee_id],
                shift_ids=[previous_shift.id, shift.id],
                date=shift.date,
                suggestion=f"Move shift later or reschedule previous shift earlier"
            ))

    def _check_overtime_breach(
        self,
        employee: Employee,
        shifts: List[Shift],
        roster: Roster
    ) -> None:
        """Check if employee exceeds max_hours_per_week."""
        week_shifts = [s for s in shifts if roster.week_start <= s.date <= roster.week_end]
        total_hours = sum(s.net_hours for s in week_shifts)

        if total_hours > employee.max_hours_per_week:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.OVERTIME_BREACH,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Employee {employee.id} scheduled {total_hours:.1f}h, "
                    f"exceeds max {employee.max_hours_per_week}h per week"
                ),
                employee_ids=[employee.id],
                shift_ids=[s.id for s in week_shifts],
                suggestion=f"Reduce by {total_hours - employee.max_hours_per_week:.1f}h"
            ))

    def _check_consecutive_days_violation(
        self,
        employee: Employee,
        shifts: List[Shift]
    ) -> None:
        """Check if employee exceeds consecutive days limit."""
        if not shifts:
            return

        sorted_shifts = sorted(shifts, key=lambda s: s.date)

        max_consecutive = 1
        current_consecutive = 1
        current_date = sorted_shifts[0].date

        for shift in sorted_shifts[1:]:
            if shift.date == current_date + timedelta(days=1):
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
                current_date = shift.date
            else:
                current_consecutive = 1
                current_date = shift.date

        if max_consecutive > MAX_CONSECUTIVE_DAYS:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.CONSECUTIVE_DAYS_VIOLATION,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Employee {employee.id} scheduled {max_consecutive} consecutive days, "
                    f"exceeds limit of {MAX_CONSECUTIVE_DAYS}"
                ),
                employee_ids=[employee.id],
                shift_ids=[s.id for s in sorted_shifts],
                suggestion=f"Provide at least {max_consecutive - MAX_CONSECUTIVE_DAYS} days off"
            ))

    def _check_staffing_levels(
        self,
        roster: Roster,
        venue: VenueConfig,
        employees_dict: Dict[str, Employee]
    ) -> None:
        """Check for understaffing/overstaffing by hour."""
        if not venue.min_staff:
            return

        staffing_by_hour: Dict[Tuple[date, int], List[str]] = {}

        for shift in roster.shifts:
            current_time = shift.start_time
            current_date = shift.date

            while current_time < shift.end_time:
                hour_key = (current_date, current_time.hour)
                if hour_key not in staffing_by_hour:
                    staffing_by_hour[hour_key] = []
                staffing_by_hour[hour_key].append(shift.employee_id)

                current_time = time(hour=(current_time.hour + 1) % 24)
                if current_time.hour == 0:
                    current_date = current_date + timedelta(days=1)

        for (check_date, hour), staff_ids in staffing_by_hour.items():
            total_min = min(venue.min_staff.values()) if venue.min_staff else 0

            if total_min > 0 and len(staff_ids) < total_min:
                self.conflicts.append(RosterConflict(
                    conflict_type=ConflictType.UNDERSTAFFED_HOUR,
                    severity=ConflictSeverity.WARNING,
                    message=(
                        f"Only {len(staff_ids)} staff at {check_date} {hour:02d}:00, "
                        f"minimum {total_min} required"
                    ),
                    employee_ids=staff_ids,
                    shift_ids=[],
                    hour=hour,
                    date=check_date,
                    suggestion=f"Add {total_min - len(staff_ids)} staff for that hour"
                ))

            if total_min > 0 and len(staff_ids) > total_min * 1.5:
                self.conflicts.append(RosterConflict(
                    conflict_type=ConflictType.OVERSTAFFED_HOUR,
                    severity=ConflictSeverity.INFO,
                    message=(
                        f"Over-staffed at {check_date} {hour:02d}:00: {len(staff_ids)} staff, "
                        f"target is {total_min} ({total_min * 1.5:.0f} acceptable)"
                    ),
                    employee_ids=staff_ids,
                    shift_ids=[],
                    hour=hour,
                    date=check_date,
                    suggestion=f"Consider removing {len(staff_ids) - int(total_min * 1.5)} staff"
                ))

    @staticmethod
    def _shifts_overlap(shift1: Shift, shift2: Shift) -> bool:
        """Check if two shifts on the same day overlap."""
        return not (shift1.end_time <= shift2.start_time or shift2.end_time <= shift1.start_time)

    @staticmethod
    def _parse_time(time_str: Optional[str]) -> Optional[time]:
        """Parse time string (HH:MM format) to time object."""
        if not time_str:
            return None
        try:
            hours, minutes = map(int, time_str.split(":"))
            return time(hour=hours, minute=minutes)
        except (ValueError, AttributeError):
            return None


class ConflictSummary:
    """Summary of conflicts grouped by type and severity."""

    def __init__(self, conflicts: List[RosterConflict]):
        self.conflicts = conflicts
        self.by_type: Dict[ConflictType, List[RosterConflict]] = {}
        self.by_severity: Dict[ConflictSeverity, List[RosterConflict]] = {}
        self.count_by_type: Dict[ConflictType, int] = {}
        self.count_by_severity: Dict[ConflictSeverity, int] = {}

        self._summarize()

    def _summarize(self) -> None:
        """Group conflicts by type and severity."""
        for conflict in self.conflicts:
            if conflict.conflict_type not in self.by_type:
                self.by_type[conflict.conflict_type] = []
            self.by_type[conflict.conflict_type].append(conflict)

            if conflict.severity not in self.by_severity:
                self.by_severity[conflict.severity] = []
            self.by_severity[conflict.severity].append(conflict)

        for ctype in ConflictType:
            self.count_by_type[ctype] = len(self.by_type.get(ctype, []))

        for csev in ConflictSeverity:
            self.count_by_severity[csev] = len(self.by_severity.get(csev, []))

    def to_dict(self) -> Dict:
        """Convert summary to dictionary for JSON serialization."""
        return {
            "total_conflicts": len(self.conflicts),
            "count_by_type": {k.value: v for k, v in self.count_by_type.items()},
            "count_by_severity": {k.value: v for k, v in self.count_by_severity.items()},
            "conflicts": [c.to_dict() for c in self.conflicts]
        }
