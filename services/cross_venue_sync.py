"""
Cross-Venue Roster Synchroniser for RosterIQ.

Prevents double-booking by detecting and managing scheduling conflicts
when employees work across multiple venues.

Provides:
- Detection of overlapping shifts at different venues for same employee
- Aggregation of hours across all venues
- Pre-check validation before scheduling new shifts
- Identification of shared employees across venues
- Availability computation considering all commitments
- Weekly hours tracking for compliance

Used by managers to enforce fairness and compliance across multi-venue operations.
"""

from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from typing import Optional, List, Dict, Set, Tuple
from decimal import Decimal
import logging
from collections import defaultdict

from rosteriq.database import get_db
from rosteriq.models import (
    Employee, Shift, Roster, VenueConfig, ShiftStatus
)

logger = logging.getLogger(__name__)

# Maximum hours per week for compliance (typically 38 for full-time, can be exceeded for casual)
DEFAULT_MAX_HOURS = 38.0

# Minimum rest period between consecutive shifts (hours)
MINIMUM_REST_HOURS = 10.0


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class TimeSlot:
    """Represents a free time slot during a day."""
    date: date
    start_time: time
    end_time: time

    def duration_hours(self) -> float:
        """Calculate duration in hours."""
        start_minutes = self.start_time.hour * 60 + self.start_time.minute
        end_minutes = self.end_time.hour * 60 + self.end_time.minute
        if end_minutes < start_minutes:
            end_minutes += 24 * 60
        return (end_minutes - start_minutes) / 60.0


@dataclass
class CrossVenueConflict:
    """Represents a scheduling conflict across venues."""
    employee_id: str
    employee_name: str
    shift_a_venue_id: str
    shift_a_venue_name: str
    shift_a_date: date
    shift_a_start: time
    shift_a_end: time
    shift_a_id: str
    shift_b_venue_id: str
    shift_b_venue_name: str
    shift_b_date: date
    shift_b_start: time
    shift_b_end: time
    shift_b_id: str
    overlap_minutes: int
    severity: str  # "critical" | "warning"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "shift_a": {
                "venue_id": self.shift_a_venue_id,
                "venue_name": self.shift_a_venue_name,
                "date": self.shift_a_date.isoformat(),
                "start_time": self.shift_a_start.isoformat(),
                "end_time": self.shift_a_end.isoformat(),
                "shift_id": self.shift_a_id,
            },
            "shift_b": {
                "venue_id": self.shift_b_venue_id,
                "venue_name": self.shift_b_venue_name,
                "date": self.shift_b_date.isoformat(),
                "start_time": self.shift_b_start.isoformat(),
                "end_time": self.shift_b_end.isoformat(),
                "shift_id": self.shift_b_id,
            },
            "overlap_minutes": self.overlap_minutes,
            "severity": self.severity,
        }


@dataclass
class SharedEmployee:
    """Represents an employee working at multiple venues."""
    employee_id: str
    name: str
    venues: List[str]
    venue_names: List[str]
    total_weekly_hours: float
    conflict_count: int

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "employee_id": self.employee_id,
            "name": self.name,
            "venues": self.venues,
            "venue_names": self.venue_names,
            "total_weekly_hours": self.total_weekly_hours,
            "conflict_count": self.conflict_count,
        }


@dataclass
class CrossVenueHours:
    """Represents total hours worked across venues."""
    employee_id: str
    total_hours: float
    per_venue: Dict[str, float]
    over_limit: bool
    max_hours: float
    compliance_warning: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "employee_id": self.employee_id,
            "total_hours": self.total_hours,
            "per_venue": self.per_venue,
            "over_limit": self.over_limit,
            "max_hours": self.max_hours,
            "compliance_warning": self.compliance_warning,
        }


@dataclass
class CrossVenueSchedule:
    """Aggregated schedule for an employee across all venues."""
    employee_id: str
    employee_name: str
    date_range: Tuple[date, date]
    shifts_by_venue: Dict[str, List[Shift]]
    all_shifts: List[Shift]
    total_hours: float
    venues_count: int

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "shifts_by_venue": {
                venue_id: [
                    {
                        "id": s.id,
                        "date": s.date.isoformat(),
                        "start_time": s.start_time.isoformat(),
                        "end_time": s.end_time.isoformat(),
                        "duration_hours": s.net_hours,
                        "status": s.status.value,
                    }
                    for s in shifts
                ]
                for venue_id, shifts in self.shifts_by_venue.items()
            },
            "total_hours": self.total_hours,
            "venues_count": self.venues_count,
        }


@dataclass
class ScheduleCheckResult:
    """Result of pre-check before scheduling a shift."""
    can_schedule: bool
    conflicts: List[CrossVenueConflict] = field(default_factory=list)
    total_hours_after: float = 0.0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "can_schedule": self.can_schedule,
            "conflicts": [c.to_dict() for c in self.conflicts],
            "total_hours_after": self.total_hours_after,
            "warnings": self.warnings,
            "errors": self.errors,
        }


# ============================================================================
# Main Service Class
# ============================================================================


class CrossVenueSync:
    """
    Multi-venue roster synchroniser.

    Detects and manages scheduling conflicts for employees working across
    multiple venues to prevent double-booking and ensure compliance.
    """

    def __init__(self):
        """Initialize the cross-venue synchroniser."""
        self.db = get_db()

    def get_cross_venue_shifts(
        self, employee_id: str, start_date: str, end_date: str
    ) -> CrossVenueSchedule:
        """
        Aggregate all shifts across all venues for one employee.

        Args:
            employee_id: ID of the employee
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            CrossVenueSchedule with all shifts grouped by venue
        """
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")

        # Get employee
        employee = self.db.get_employee(employee_id)
        if not employee:
            raise ValueError(f"Employee {employee_id} not found")

        # Aggregate shifts from all rosters
        shifts_by_venue: Dict[str, List[Shift]] = defaultdict(list)
        all_shifts: List[Shift] = []
        total_hours = 0.0

        for roster in self.db.list_rosters():
            if start <= roster.week_start <= end or start <= roster.week_end <= end:
                for shift in roster.shifts:
                    if (
                        shift.employee_id == employee_id
                        and start <= shift.date <= end
                    ):
                        shifts_by_venue[roster.venue_id].append(shift)
                        all_shifts.append(shift)
                        total_hours += shift.net_hours

        # Sort shifts by date
        all_shifts.sort(key=lambda s: (s.date, s.start_time))
        for venue_shifts in shifts_by_venue.values():
            venue_shifts.sort(key=lambda s: (s.date, s.start_time))

        return CrossVenueSchedule(
            employee_id=employee_id,
            employee_name=employee.name,
            date_range=(start, end),
            shifts_by_venue=dict(shifts_by_venue),
            all_shifts=all_shifts,
            total_hours=total_hours,
            venues_count=len(shifts_by_venue),
        )

    def detect_conflicts(
        self, employee_id: str, start_date: str, end_date: str
    ) -> List[CrossVenueConflict]:
        """
        Find overlapping shifts at different venues for one employee.

        Args:
            employee_id: ID of the employee
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of CrossVenueConflict objects
        """
        schedule = self.get_cross_venue_shifts(employee_id, start_date, end_date)
        conflicts: List[CrossVenueConflict] = []

        # Check all pairs of shifts
        for i, shift_a in enumerate(schedule.all_shifts):
            for shift_b in schedule.all_shifts[i + 1 :]:
                # Only flag if different venues
                venue_a = self._get_shift_venue_id(shift_a)
                venue_b = self._get_shift_venue_id(shift_b)

                if venue_a == venue_b:
                    continue

                # Check for overlap
                overlap = self._calculate_overlap_minutes(shift_a, shift_b)
                if overlap > 0:
                    venue_a_config = self.db.get_venue(venue_a)
                    venue_b_config = self.db.get_venue(venue_b)

                    conflict = CrossVenueConflict(
                        employee_id=employee_id,
                        employee_name=schedule.employee_name,
                        shift_a_venue_id=venue_a,
                        shift_a_venue_name=venue_a_config.name if venue_a_config else venue_a,
                        shift_a_date=shift_a.date,
                        shift_a_start=shift_a.start_time,
                        shift_a_end=shift_a.end_time,
                        shift_a_id=shift_a.id,
                        shift_b_venue_id=venue_b,
                        shift_b_venue_name=venue_b_config.name if venue_b_config else venue_b,
                        shift_b_date=shift_b.date,
                        shift_b_start=shift_b.start_time,
                        shift_b_end=shift_b.end_time,
                        shift_b_id=shift_b.id,
                        overlap_minutes=overlap,
                        severity="critical",
                    )
                    conflicts.append(conflict)

        return conflicts

    def detect_all_conflicts(
        self, venue_ids: List[str], start_date: str, end_date: str
    ) -> List[CrossVenueConflict]:
        """
        Scan all shared employees across multiple venues for conflicts.

        Args:
            venue_ids: List of venue IDs to check
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of CrossVenueConflict objects
        """
        all_conflicts: List[CrossVenueConflict] = []

        # Get shared employees
        shared_employees = self.get_shared_employees(venue_ids)

        # Check each shared employee
        for shared_emp in shared_employees:
            conflicts = self.detect_conflicts(
                shared_emp.employee_id, start_date, end_date
            )
            all_conflicts.extend(conflicts)

        return all_conflicts

    def get_shared_employees(self, venue_ids: List[str]) -> List[SharedEmployee]:
        """
        List employees working at multiple venues.

        Args:
            venue_ids: List of venue IDs to check

        Returns:
            List of SharedEmployee objects
        """
        # Map employee IDs to venues they work at
        emp_to_venues: Dict[str, Set[str]] = defaultdict(set)
        emp_to_name: Dict[str, str] = {}

        for venue_id in venue_ids:
            for roster in self.db.list_rosters():
                if roster.venue_id == venue_id:
                    for shift in roster.shifts:
                        emp_to_venues[shift.employee_id].add(venue_id)
                        if shift.employee_id not in emp_to_name:
                            emp = self.db.get_employee(shift.employee_id)
                            if emp:
                                emp_to_name[shift.employee_id] = emp.name

        # Filter to only those in multiple venues
        shared_employees: List[SharedEmployee] = []

        for emp_id, venues in emp_to_venues.items():
            if len(venues) > 1:
                # Get total weekly hours
                total_hours = self._get_total_weekly_hours(emp_id, venues)

                # Count conflicts
                try:
                    conflicts = self.detect_conflicts(
                        emp_id,
                        (date.today() - timedelta(days=7)).isoformat(),
                        date.today().isoformat(),
                    )
                except Exception:
                    conflicts = []

                venue_names = []
                for vid in venues:
                    vc = self.db.get_venue(vid)
                    venue_names.append(vc.name if vc else vid)

                shared_employees.append(
                    SharedEmployee(
                        employee_id=emp_id,
                        name=emp_to_name.get(emp_id, "Unknown"),
                        venues=sorted(list(venues)),
                        venue_names=venue_names,
                        total_weekly_hours=total_hours,
                        conflict_count=len(conflicts),
                    )
                )

        return shared_employees

    def check_before_scheduling(
        self, employee_id: str, proposed_shift: dict
    ) -> ScheduleCheckResult:
        """
        Pre-check if adding a shift would create a conflict.

        Args:
            employee_id: ID of the employee
            proposed_shift: Dictionary with keys:
                - venue_id: str
                - date: str (YYYY-MM-DD)
                - start_time: str (HH:MM:SS)
                - end_time: str (HH:MM:SS)
                - break_minutes: int (optional)

        Returns:
            ScheduleCheckResult with validation status
        """
        result = ScheduleCheckResult(can_schedule=True)

        try:
            # Parse proposed shift
            venue_id = proposed_shift.get("venue_id")
            shift_date = date.fromisoformat(proposed_shift.get("date"))
            start_time = time.fromisoformat(proposed_shift.get("start_time"))
            end_time = time.fromisoformat(proposed_shift.get("end_time"))
            break_minutes = proposed_shift.get("break_minutes", 0)

            if not venue_id:
                result.errors.append("venue_id is required")
                result.can_schedule = False
                return result

            # Create temporary shift object
            temp_shift = Shift(
                id="temp_proposed",
                employee_id=employee_id,
                date=shift_date,
                start_time=start_time,
                end_time=end_time,
                break_minutes=break_minutes,
                status=ShiftStatus.scheduled,
                role="",
            )

            # Get current shifts for the employee
            schedule = self.get_cross_venue_shifts(
                employee_id,
                (shift_date - timedelta(days=7)).isoformat(),
                (shift_date + timedelta(days=7)).isoformat(),
            )

            # Check for time overlaps with existing shifts
            conflicts_found = []
            for existing_shift in schedule.all_shifts:
                # Skip shifts in the same venue (roster engine handles those)
                existing_venue = self._get_shift_venue_id(existing_shift)
                if existing_venue == venue_id:
                    continue

                overlap = self._calculate_overlap_minutes(temp_shift, existing_shift)
                if overlap > 0:
                    existing_config = self.db.get_venue(existing_venue)
                    proposed_config = self.db.get_venue(venue_id)

                    conflict = CrossVenueConflict(
                        employee_id=employee_id,
                        employee_name=schedule.employee_name,
                        shift_a_venue_id=venue_id,
                        shift_a_venue_name=proposed_config.name if proposed_config else venue_id,
                        shift_a_date=shift_date,
                        shift_a_start=start_time,
                        shift_a_end=end_time,
                        shift_a_id="proposed",
                        shift_b_venue_id=existing_venue,
                        shift_b_venue_name=existing_config.name if existing_config else existing_venue,
                        shift_b_date=existing_shift.date,
                        shift_b_start=existing_shift.start_time,
                        shift_b_end=existing_shift.end_time,
                        shift_b_id=existing_shift.id,
                        overlap_minutes=overlap,
                        severity="critical",
                    )
                    conflicts_found.append(conflict)

            if conflicts_found:
                result.conflicts = conflicts_found
                result.can_schedule = False
                result.errors.append(f"Shift overlaps with {len(conflicts_found)} existing shift(s)")

            # Check rest period between shifts
            rest_warnings = self._check_rest_period(
                temp_shift, schedule.all_shifts, venue_id
            )
            if rest_warnings:
                result.warnings.extend(rest_warnings)

            # Calculate total hours after adding this shift
            new_hours = temp_shift.net_hours
            result.total_hours_after = schedule.total_hours + new_hours

            # Check max hours compliance
            employee = self.db.get_employee(employee_id)
            if employee:
                max_hours = employee.max_hours_per_week
                week_start = shift_date - timedelta(days=shift_date.weekday())
                week_end = week_start + timedelta(days=6)

                # Calculate hours in this week (rough estimate)
                week_hours = sum(
                    s.net_hours for s in schedule.all_shifts
                    if week_start <= s.date <= week_end
                )
                week_hours_after = week_hours + new_hours

                if week_hours_after > max_hours:
                    result.warnings.append(
                        f"Weekly hours would reach {week_hours_after:.1f}h "
                        f"(limit: {max_hours}h)"
                    )

        except (ValueError, KeyError, AttributeError) as e:
            result.errors.append(f"Invalid proposed shift data: {str(e)}")
            result.can_schedule = False

        return result

    def get_cross_venue_hours(
        self, employee_id: str, week_start: str
    ) -> CrossVenueHours:
        """
        Total hours worked across all venues for a week.

        Args:
            employee_id: ID of the employee
            week_start: Start of week (YYYY-MM-DD), must be a Monday

        Returns:
            CrossVenueHours with breakdown by venue
        """
        try:
            start = date.fromisoformat(week_start)
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")

        end = start + timedelta(days=6)

        schedule = self.get_cross_venue_shifts(
            employee_id, start.isoformat(), end.isoformat()
        )

        employee = self.db.get_employee(employee_id)
        max_hours = DEFAULT_MAX_HOURS
        if employee:
            max_hours = employee.max_hours_per_week

        # Calculate hours per venue
        per_venue: Dict[str, float] = {}
        for venue_id, shifts in schedule.shifts_by_venue.items():
            per_venue[venue_id] = sum(s.net_hours for s in shifts)

        over_limit = schedule.total_hours > max_hours
        compliance_warning = None

        if over_limit:
            compliance_warning = (
                f"Weekly hours exceed limit: {schedule.total_hours:.1f}h > {max_hours}h"
            )

        return CrossVenueHours(
            employee_id=employee_id,
            total_hours=schedule.total_hours,
            per_venue=per_venue,
            over_limit=over_limit,
            max_hours=max_hours,
            compliance_warning=compliance_warning,
        )

    def get_availability_across_venues(
        self, employee_id: str, target_date: str
    ) -> List[TimeSlot]:
        """
        Free time slots considering all venue commitments.

        Args:
            employee_id: ID of the employee
            target_date: Date to check (YYYY-MM-DD)

        Returns:
            List of available TimeSlot objects
        """
        try:
            target = date.fromisoformat(target_date)
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")

        schedule = self.get_cross_venue_shifts(
            employee_id,
            target.isoformat(),
            target.isoformat(),
        )

        # Get shifts for this day sorted by time
        day_shifts = [s for s in schedule.all_shifts if s.date == target]
        day_shifts.sort(key=lambda s: s.start_time)

        if not day_shifts:
            # Entire day is available
            return [TimeSlot(target, time(0, 0), time(23, 59))]

        available_slots: List[TimeSlot] = []

        # Before first shift
        if day_shifts[0].start_time > time(0, 0):
            available_slots.append(
                TimeSlot(target, time(0, 0), day_shifts[0].start_time)
            )

        # Between shifts (with rest period buffer)
        for i in range(len(day_shifts) - 1):
            current_shift = day_shifts[i]
            next_shift = day_shifts[i + 1]

            gap_start = current_shift.end_time
            gap_end = next_shift.start_time

            # Add buffer for rest
            rest_minutes = int(MINIMUM_REST_HOURS * 60)
            gap_start_with_buffer = self._add_minutes_to_time(gap_start, rest_minutes)

            if gap_start_with_buffer < gap_end:
                available_slots.append(
                    TimeSlot(target, gap_start_with_buffer, gap_end)
                )

        # After last shift
        if day_shifts[-1].end_time < time(23, 59):
            available_slots.append(
                TimeSlot(target, day_shifts[-1].end_time, time(23, 59))
            )

        return available_slots

    # ========================================================================
    # Private Helper Methods
    # ========================================================================

    def _get_shift_venue_id(self, shift: Shift) -> str:
        """Find which venue a shift belongs to."""
        for roster in self.db.list_rosters():
            if any(s.id == shift.id for s in roster.shifts):
                return roster.venue_id
        return ""

    def _calculate_overlap_minutes(self, shift_a: Shift, shift_b: Shift) -> int:
        """Calculate overlap in minutes between two shifts on the same date."""
        if shift_a.date != shift_b.date:
            return 0

        start_a = self._time_to_minutes(shift_a.start_time)
        end_a = self._time_to_minutes(shift_a.end_time)
        start_b = self._time_to_minutes(shift_b.start_time)
        end_b = self._time_to_minutes(shift_b.end_time)

        # Handle overnight shifts
        if end_a < start_a:
            end_a += 24 * 60
        if end_b < start_b:
            end_b += 24 * 60

        overlap_start = max(start_a, start_b)
        overlap_end = min(end_a, end_b)

        if overlap_end > overlap_start:
            return overlap_end - overlap_start
        return 0

    def _time_to_minutes(self, t: time) -> int:
        """Convert time to minutes since midnight."""
        return t.hour * 60 + t.minute

    def _add_minutes_to_time(self, t: time, minutes: int) -> time:
        """Add minutes to a time, handling day wrap."""
        total_minutes = self._time_to_minutes(t) + minutes
        hour = (total_minutes // 60) % 24
        minute = total_minutes % 60
        return time(hour, minute)

    def _check_rest_period(
        self, proposed_shift: Shift, existing_shifts: List[Shift], venue_id: str
    ) -> List[str]:
        """Check if proposed shift violates rest period requirements."""
        warnings = []

        # Get shifts on adjacent days
        prev_day = proposed_shift.date - timedelta(days=1)
        next_day = proposed_shift.date + timedelta(days=1)

        # Check rest after shifts on previous day
        for shift in existing_shifts:
            if shift.date == prev_day:
                # Check rest period between end of previous shift and start of proposed
                rest_hours = self._hours_between(shift.end_time, proposed_shift.start_time)
                if rest_hours < MINIMUM_REST_HOURS:
                    warnings.append(
                        f"Only {rest_hours:.1f}h rest after previous shift "
                        f"(minimum: {MINIMUM_REST_HOURS}h)"
                    )

        # Check rest before shifts on next day
        for shift in existing_shifts:
            if shift.date == next_day:
                # Check rest between end of proposed and start of next
                rest_hours = self._hours_between(proposed_shift.end_time, shift.start_time)
                if rest_hours < MINIMUM_REST_HOURS:
                    warnings.append(
                        f"Only {rest_hours:.1f}h rest before next shift "
                        f"(minimum: {MINIMUM_REST_HOURS}h)"
                    )

        return warnings

    def _hours_between(self, time_a: time, time_b: time) -> float:
        """Calculate hours between two times (spanning midnight)."""
        minutes_a = self._time_to_minutes(time_a)
        minutes_b = self._time_to_minutes(time_b)

        if minutes_b >= minutes_a:
            return (minutes_b - minutes_a) / 60.0
        else:
            # Spans midnight
            return ((24 * 60) - minutes_a + minutes_b) / 60.0

    def _get_total_weekly_hours(
        self, employee_id: str, venue_ids: Set[str]
    ) -> float:
        """Get total hours for an employee this week across specified venues."""
        week_start = date.today() - timedelta(days=date.today().weekday())
        week_end = week_start + timedelta(days=6)

        total = 0.0

        for roster in self.db.list_rosters():
            if roster.venue_id in venue_ids:
                if week_start <= roster.week_start <= week_end:
                    for shift in roster.shifts:
                        if shift.employee_id == employee_id:
                            total += shift.net_hours

        return total
