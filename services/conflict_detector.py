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
    Shift, Roster, Employee, VenueConfig, EmploymentType, ShiftStatus, State,
    DemandForecast, holds_skill, normalise_area,
)
from rosteriq.award_rules import (
    get_minimum_break_minutes, get_paid_rest_break_minutes, get_minimum_engagement_hours,
    MAX_SHIFT_LENGTH_HOURS, MINIMUM_HOURS_BETWEEN_SHIFTS, MAX_CONSECUTIVE_DAYS,
    get_day_type, DayType,
)
from rosteriq.services.availability_rules import is_available
from rosteriq.services.venue_areas import min_staff_for, trading_window, venue_areas

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
        forecasts: Optional[List[DemandForecast]] = None,
    ) -> List[RosterConflict]:
        """
        Detect all conflicts in the roster.

        Args:
            roster: The roster to check
            venue: Venue configuration with staffing requirements
            employees: List of all employees for reference
            forecasts: The week's demand forecasts; their hours with demand are
                the trading hours the staffing check covers

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
                if shift.employee_id:
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

        self._check_staffing_levels(roster, venue, employees_dict, forecasts)

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
        # The shared rule (services/availability_rules.py): unavailable that
        # day = CRITICAL; available that day but outside every window = WARNING.
        availability = employee.availability
        if is_available(availability, shift.date, shift.start_time, shift.end_time):
            return

        day_name = shift.date.strftime("%A").lower()

        if not is_available(availability, shift.date):
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

        # Available that day, but no window holds the whole shift.
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

    # Generic roles that any hospitality employee can fill — not a specific skill.
    _GENERIC_ROLES = {"general", "any", "staff", "floor_general"}

    def _check_skill_mismatch(self, shift: Shift, employee: Employee) -> None:
        """Check if employee has the required skill for the shift role."""
        if not employee.skills or not shift.role:
            return

        # A generic/catch-all role isn't a specialised skill, so don't flag it.
        if shift.role.lower() in self._GENERIC_ROLES:
            return

        if not holds_skill(employee.skills, shift.role):
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
        """Check the recorded unpaid meal break against the break table.

        Paid rest breaks are worked time, so they are never expected in
        break_minutes. The table is advisor-to-confirm, so a short break is a
        warning for the manager, never a publish block.
        """
        required_break = get_minimum_break_minutes(shift.duration_hours)
        if shift.break_minutes < required_break:
            paid_rest = get_paid_rest_break_minutes(shift.duration_hours)
            rest_note = f", plus {paid_rest}min paid rest (not deducted)" if paid_rest else ""
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.BREAK_VIOLATION,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Shift {shift.id} ({shift.duration_hours:.1f}h) records a "
                    f"{shift.break_minutes}min unpaid meal break; the break table expects "
                    f"{required_break}min{rest_note} (advisor to confirm)"
                ),
                employee_ids=[shift.employee_id],
                shift_ids=[shift.id],
                date=shift.date,
                suggestion=f"Record a {required_break}min unpaid meal break"
            ))

    @staticmethod
    def _shift_end_dt(s: Shift) -> datetime:
        """Actual end datetime, accounting for crossing midnight (overnight shifts)."""
        end_dt = datetime.combine(s.date, s.end_time)
        if s.end_time <= s.start_time:
            end_dt += timedelta(days=1)
        return end_dt

    def _check_fatigue_risk(self, shift: Shift, employee_shifts: List[Shift]) -> None:
        """Check if employee has at least 10 hours rest between consecutive shifts."""
        this_start = datetime.combine(shift.date, shift.start_time)
        # A "previous" shift is any OTHER shift that actually ends at or before this
        # one starts — using real datetimes so an overnight previous shift's
        # next-day end (e.g. Mon 22:00->Tue 06:00) is compared correctly.
        previous_shifts = [
            s for s in employee_shifts
            if s.id != shift.id and self._shift_end_dt(s) <= this_start
        ]

        if not previous_shifts:
            return

        previous_shift = max(previous_shifts, key=self._shift_end_dt)
        hours_rest = (this_start - self._shift_end_dt(previous_shift)).total_seconds() / 3600

        if 0 <= hours_rest < MINIMUM_HOURS_BETWEEN_SHIFTS:
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

        # Respect the employee's own consecutive-days limit when set; fall back
        # to the award-wide maximum otherwise. (Previously this always compared
        # against the global MAX_CONSECUTIVE_DAYS, so a per-employee limit
        # stricter than the award cap was never enforced.)
        limit = getattr(employee, "consecutive_days_limit", None) or MAX_CONSECUTIVE_DAYS

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

        if max_consecutive > limit:
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.CONSECUTIVE_DAYS_VIOLATION,
                severity=ConflictSeverity.WARNING,
                message=(
                    f"Employee {employee.id} scheduled {max_consecutive} consecutive days, "
                    f"exceeds limit of {limit}"
                ),
                employee_ids=[employee.id],
                shift_ids=[s.id for s in sorted_shifts],
                suggestion=f"Provide at least {max_consecutive - limit} days off"
            ))

    def _check_staffing_levels(
        self,
        roster: Roster,
        venue: VenueConfig,
        employees_dict: Dict[str, Employee],
        forecasts: Optional[List[DemandForecast]] = None,
    ) -> None:
        """Check each area's minimum across every trading hour, empty hours too.

        Trading hours are the venue's recorded hours for the day (a close past
        midnight runs into the next date); a day without them uses its forecast
        hours with demand, and a day with neither the span from its first
        rostered start to its last rostered end. Each hour is held to the
        minimums of the trading day it belongs to, so a sports bar that needs
        one on Monday and three on Saturday is checked as such. Shortfalls are
        warnings in plain words, one per run of hours ("Front bar: nobody
        21:00-24:00 Mon"), never a publish block.
        """
        areas = sorted(a["name"] for a in venue_areas(venue) if any(a["min_by_day"].values()))
        has_hours = bool(getattr(venue, "trading_hours", None))
        if not areas and not forecasts and not has_hours:
            return

        on_by_cell: Dict[Tuple[date, int], List[str]] = defaultdict(list)
        on_by_area: Dict[str, Dict[Tuple[date, int], List[str]]] = defaultdict(lambda: defaultdict(list))
        span_by_day: Dict[date, Tuple[datetime, datetime]] = {}
        for shift in roster.shifts:
            if shift.status in (ShiftStatus.cancelled, ShiftStatus.no_show):
                continue
            area = normalise_area(shift.role)
            for cell in shift.clock_hours():
                if not shift.employee_id:
                    continue  # an open shift is a gap, not cover
                on_by_cell[cell].append(shift.employee_id)
                on_by_area[area][cell].append(shift.employee_id)
            start_dt = datetime.combine(shift.date, shift.start_time)
            end_dt = start_dt + timedelta(hours=shift.duration_hours)
            lo, hi = span_by_day.get(shift.date, (start_dt, end_dt))
            span_by_day[shift.date] = (min(lo, start_dt), max(hi, end_dt))

        # (date, hour) -> the trading day whose minimums apply to it
        trading: Dict[Tuple[date, int], date] = {}

        def _add_span(day: date, lo: datetime, hi: datetime) -> None:
            cur = lo.replace(minute=0, second=0, microsecond=0)
            for _ in range(48):
                if cur >= hi:
                    break
                trading.setdefault((cur.date(), cur.hour), day)
                cur += timedelta(hours=1)

        recorded_days: Set[date] = set()
        for offset in range((roster.week_end - roster.week_start).days + 1):
            day = roster.week_start + timedelta(days=offset)
            window = trading_window(venue, day)
            if window:
                recorded_days.add(day)
                _add_span(day, *window)

        forecast_days: Set[date] = set()
        for fc in forecasts or []:
            if fc.venue_id != roster.venue_id or fc.predicted_covers <= 0:
                continue
            if roster.week_start <= fc.date <= roster.week_end and fc.date not in recorded_days:
                forecast_days.add(fc.date)
                trading.setdefault((fc.date, fc.hour), fc.date)
        if areas:
            for day, (lo, hi) in span_by_day.items():
                if day not in recorded_days and day not in forecast_days:
                    _add_span(day, lo, hi)
        cells = sorted(trading)

        minimums: Dict[date, Dict[str, int]] = {}

        def _mins(cell) -> Dict[str, int]:
            day = trading.get(cell, cell[0])
            if day not in minimums:
                minimums[day] = min_staff_for(venue, day)
            return minimums[day]

        if areas:
            for area in areas:
                label = area[:1].upper() + area[1:]
                self._flag_short_runs(cells, on_by_area.get(area, {}),
                                      lambda cell, a=area: _mins(cell).get(a, 0), label)
        else:
            self._flag_short_runs(cells, on_by_cell, 1, None)

        for (check_date, hour), staff_ids in sorted(on_by_cell.items()):
            total_min = sum(_mins((check_date, hour)).values())
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

    def _flag_short_runs(
        self,
        cells: List[Tuple[date, int]],
        on: Dict[Tuple[date, int], List[str]],
        need,
        label: Optional[str],
    ) -> None:
        """One UNDERSTAFFED_HOUR warning per run of consecutive short hours with
        the same head count and need. need is a count, or a function of the
        cell. label=None means the venue as a whole."""
        need_of = need if callable(need) else (lambda cell: need)
        run: List[Tuple[date, int]] = []
        run_count = None
        run_need = 0

        def _flush():
            if not run:
                return
            first, last = run[0], run[-1]
            start_dt = datetime.combine(first[0], time(first[1]))
            end_dt = datetime.combine(last[0], time(last[1])) + timedelta(hours=1)
            end_label = "24:00" if end_dt == datetime.combine(first[0] + timedelta(days=1), time(0)) \
                else f"{end_dt.hour:02d}:00"
            when = f"{start_dt.hour:02d}:00-{end_label} {first[0].strftime('%a')}"
            ids: List[str] = []
            for cell in run:
                ids.extend(i for i in on.get(cell, []) if i not in ids)
            if label is None:
                message = f"Nobody rostered {when}"
                suggestion = f"Roster someone {when}, or confirm the venue is closed"
            elif run_count == 0:
                message = f"{label}: nobody {when}"
                suggestion = f"Add {run_need} {label.lower()} staff {when}"
            else:
                message = f"{label}: {run_count} of {run_need} on {when}"
                suggestion = f"Add {run_need - run_count} {label.lower()} staff {when}"
            self.conflicts.append(RosterConflict(
                conflict_type=ConflictType.UNDERSTAFFED_HOUR,
                severity=ConflictSeverity.WARNING,
                message=message,
                employee_ids=ids,
                shift_ids=[],
                hour=first[1],
                date=first[0],
                suggestion=suggestion,
            ))

        prev_dt = None
        for cell in cells:
            count = len(on.get(cell, []))
            cell_need = need_of(cell)
            cell_dt = datetime.combine(cell[0], time(cell[1]))
            if count >= cell_need:
                _flush()
                run, run_count, prev_dt = [], None, None
                continue
            if run and (count != run_count or cell_need != run_need
                        or cell_dt - prev_dt != timedelta(hours=1)):
                _flush()
                run = []
            run.append(cell)
            run_count = count
            run_need = cell_need
            prev_dt = cell_dt
        _flush()

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


def detect_conflicts(
    roster: Roster,
    venue: VenueConfig,
    employees: List[Employee],
    forecasts: Optional[List[DemandForecast]] = None,
) -> List[RosterConflict]:
    """Module-level convenience wrapper around ConflictDetector.detect_conflicts."""
    return ConflictDetector().detect_conflicts(roster, venue, employees, forecasts)


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
