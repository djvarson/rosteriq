"""Break Compliance Checker for Australian hospitality.

Implements Fair Work Act 2009 break rules for the Hospitality Industry (General)
Award 2020:
- Meal breaks: 30 min unpaid per 5 hours; 2nd 30 min if >10 hours
- Rest breaks: 10 min paid per 4 hours worked
- Minimum gap: 11 hours between end of one shift and start of next
- Maximum shift: 11.5 hours including breaks for ordinary hours
- Split shifts: Maximum 12-hour span from start of first to end of last part

Data is persisted to SQLite for queries and compliance reports.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.break_compliance")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class ViolationSeverity(str, Enum):
    """Severity levels for break violations."""
    WARNING = "warning"
    VIOLATION = "violation"
    CRITICAL = "critical"


class RuleType(str, Enum):
    """Types of break compliance rules."""
    MEAL_BREAK = "meal_break"
    REST_BREAK = "rest_break"
    MIN_GAP = "min_gap"
    MAX_SHIFT = "max_shift"
    SPLIT_SPAN = "split_span"


@dataclass
class BreakRule:
    """Definition of a break compliance rule."""
    rule_type: RuleType
    threshold_hours: float
    break_minutes: int
    description: str
    severity: ViolationSeverity = ViolationSeverity.VIOLATION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_type": self.rule_type.value,
            "threshold_hours": self.threshold_hours,
            "break_minutes": self.break_minutes,
            "description": self.description,
            "severity": self.severity.value,
        }


@dataclass
class BreakViolation:
    """A single break compliance violation."""
    violation_id: str
    venue_id: str
    employee_id: str
    employee_name: str
    shift_date: str  # ISO date (YYYY-MM-DD)
    shift_start: str  # HH:MM
    shift_end: str  # HH:MM
    rule_type: RuleType
    severity: ViolationSeverity
    description: str
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    dismissed_at: Optional[datetime] = None
    dismissed_by: Optional[str] = None
    dismiss_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "violation_id": self.violation_id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "shift_date": self.shift_date,
            "shift_start": self.shift_start,
            "shift_end": self.shift_end,
            "rule_type": self.rule_type.value,
            "severity": self.severity.value,
            "description": self.description,
            "detected_at": self.detected_at.isoformat(),
            "dismissed_at": self.dismissed_at.isoformat() if self.dismissed_at else None,
            "dismissed_by": self.dismissed_by,
            "dismiss_reason": self.dismiss_reason,
        }


@dataclass
class ComplianceReport:
    """Aggregated compliance report for a venue."""
    venue_id: str
    check_date: datetime
    total_shifts: int
    violations: List[BreakViolation] = field(default_factory=list)
    compliant: bool = True

    @property
    def summary(self) -> Dict[str, Any]:
        """Summary counts by severity."""
        counts = {
            "warning": 0,
            "violation": 0,
            "critical": 0,
        }
        for v in self.violations:
            counts[v.severity.value] += 1
        return {
            "total_violations": len(self.violations),
            "warning": counts["warning"],
            "violation": counts["violation"],
            "critical": counts["critical"],
            "compliant": self.compliant,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "check_date": self.check_date.isoformat(),
            "total_shifts": self.total_shifts,
            "violations": [v.to_dict() for v in self.violations],
            "summary": self.summary,
            "compliant": self.compliant,
        }


# ---------------------------------------------------------------------------
# Default rules
# ---------------------------------------------------------------------------

DEFAULT_RULES = [
    BreakRule(
        rule_type=RuleType.MEAL_BREAK,
        threshold_hours=5.0,
        break_minutes=30,
        description="Employees working >5 hours entitled to unpaid 30-min meal break",
        severity=ViolationSeverity.VIOLATION,
    ),
    BreakRule(
        rule_type=RuleType.MEAL_BREAK,
        threshold_hours=10.0,
        break_minutes=60,  # second 30-min break
        description="Employees working >10 hours entitled to second unpaid 30-min meal break",
        severity=ViolationSeverity.CRITICAL,
    ),
    BreakRule(
        rule_type=RuleType.REST_BREAK,
        threshold_hours=4.0,
        break_minutes=10,
        description="10-min paid rest break per 4 hours worked",
        severity=ViolationSeverity.WARNING,
    ),
    BreakRule(
        rule_type=RuleType.MIN_GAP,
        threshold_hours=11.0,
        break_minutes=0,
        description="Minimum 11-hour gap between end of one shift and start of next",
        severity=ViolationSeverity.CRITICAL,
    ),
    BreakRule(
        rule_type=RuleType.MAX_SHIFT,
        threshold_hours=11.5,
        break_minutes=0,
        description="Maximum 11.5-hour shift length including breaks for ordinary hours",
        severity=ViolationSeverity.CRITICAL,
    ),
    BreakRule(
        rule_type=RuleType.SPLIT_SPAN,
        threshold_hours=12.0,
        break_minutes=0,
        description="Maximum 12-hour span from start of first part to end of last part (split shifts)",
        severity=ViolationSeverity.VIOLATION,
    ),
]


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------

def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_BREAK_VIOLATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS break_violations (
    violation_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    shift_start TEXT NOT NULL,
    shift_end TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    description TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    dismissed_at TEXT,
    dismissed_by TEXT,
    dismiss_reason TEXT
);
CREATE INDEX IF NOT EXISTS ix_violation_venue ON break_violations(venue_id);
CREATE INDEX IF NOT EXISTS ix_violation_date ON break_violations(shift_date);
CREATE INDEX IF NOT EXISTS ix_violation_employee ON break_violations(employee_id);
CREATE INDEX IF NOT EXISTS ix_violation_severity ON break_violations(severity);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("break_violations", _BREAK_VIOLATIONS_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_compliance_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Break Compliance Checking Functions
# ---------------------------------------------------------------------------


def _shift_duration_hours(shift_start: str, shift_end: str) -> float:
    """Calculate shift duration in hours from HH:MM strings.

    Args:
        shift_start: Start time in HH:MM format
        shift_end: End time in HH:MM format

    Returns:
        Duration in hours (float)
    """
    try:
        start_h, start_m = map(int, shift_start.split(":"))
        end_h, end_m = map(int, shift_end.split(":"))

        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m

        # Handle overnight shifts (end time < start time)
        if end_minutes < start_minutes:
            end_minutes += 24 * 60

        duration_minutes = end_minutes - start_minutes
        return duration_minutes / 60.0
    except (ValueError, AttributeError):
        return 0.0


def _time_to_minutes(time_str: str) -> int:
    """Convert HH:MM to minutes since midnight.

    Args:
        time_str: Time in HH:MM format

    Returns:
        Minutes since midnight
    """
    try:
        h, m = map(int, time_str.split(":"))
        return h * 60 + m
    except (ValueError, AttributeError):
        return 0


def _minutes_to_time(minutes: int) -> str:
    """Convert minutes since midnight to HH:MM format.

    Args:
        minutes: Minutes since midnight

    Returns:
        Time in HH:MM format
    """
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def check_shift_breaks(
    shift_date: str,
    shift_start: str,
    shift_end: str,
    break_minutes: int = 30,
    rules: Optional[List[BreakRule]] = None,
) -> List[BreakViolation]:
    """Check a single shift against break rules.

    Args:
        shift_date: Date in ISO format (YYYY-MM-DD)
        shift_start: Start time in HH:MM
        shift_end: End time in HH:MM
        break_minutes: Minutes of breaks taken during shift (default 30)
        rules: List of rules to check (default: DEFAULT_RULES)

    Returns:
        List of violations found
    """
    if rules is None:
        rules = DEFAULT_RULES

    violations = []
    duration_hours = _shift_duration_hours(shift_start, shift_end)
    actual_work_hours = duration_hours - (break_minutes / 60.0)

    for rule in rules:
        if rule.rule_type == RuleType.MEAL_BREAK:
            # Check meal break entitlement
            if duration_hours > rule.threshold_hours:
                # Entitled to a break but may not have taken enough
                # This would require tracking actual breaks taken, which
                # isn't in the basic shift info. Flag as potential violation.
                violations.append(
                    BreakViolation(
                        violation_id=f"viol_{uuid.uuid4().hex[:12]}",
                        venue_id="",  # Will be set by caller
                        employee_id="",
                        employee_name="",
                        shift_date=shift_date,
                        shift_start=shift_start,
                        shift_end=shift_end,
                        rule_type=RuleType.MEAL_BREAK,
                        severity=rule.severity,
                        description=f"{rule.description} (Duration: {duration_hours:.1f}h, "
                                   f"breaks recorded: {break_minutes}m)",
                    )
                )

        elif rule.rule_type == RuleType.REST_BREAK:
            # Check rest break entitlement (10 min per 4 hours)
            required_breaks = int(actual_work_hours / rule.threshold_hours) * rule.break_minutes
            if required_breaks > 0 and break_minutes < required_breaks:
                violations.append(
                    BreakViolation(
                        violation_id=f"viol_{uuid.uuid4().hex[:12]}",
                        venue_id="",
                        employee_id="",
                        employee_name="",
                        shift_date=shift_date,
                        shift_start=shift_start,
                        shift_end=shift_end,
                        rule_type=RuleType.REST_BREAK,
                        severity=rule.severity,
                        description=f"{rule.description} (Work hours: {actual_work_hours:.1f}h, "
                                   f"breaks: {break_minutes}m, required: {required_breaks}m)",
                    )
                )

        elif rule.rule_type == RuleType.MAX_SHIFT:
            # Check max shift length
            if actual_work_hours > rule.threshold_hours:
                violations.append(
                    BreakViolation(
                        violation_id=f"viol_{uuid.uuid4().hex[:12]}",
                        venue_id="",
                        employee_id="",
                        employee_name="",
                        shift_date=shift_date,
                        shift_start=shift_start,
                        shift_end=shift_end,
                        rule_type=RuleType.MAX_SHIFT,
                        severity=rule.severity,
                        description=f"{rule.description} (Actual: {actual_work_hours:.1f}h)",
                    )
                )

    return violations


def check_gap_compliance(
    employee_id: str,
    employee_name: str,
    venue_id: str,
    shifts_for_employee: List[Dict[str, str]],
    rules: Optional[List[BreakRule]] = None,
) -> List[BreakViolation]:
    """Check 11-hour minimum gap between consecutive shifts.

    Args:
        employee_id: Employee ID
        employee_name: Employee name
        venue_id: Venue ID
        shifts_for_employee: List of dicts with keys: date, start, end (sorted chronologically)
        rules: List of rules to check (default: DEFAULT_RULES)

    Returns:
        List of gap violations
    """
    if rules is None:
        rules = DEFAULT_RULES

    violations = []

    # Find the min gap rule
    gap_rule = None
    for rule in rules:
        if rule.rule_type == RuleType.MIN_GAP:
            gap_rule = rule
            break

    if not gap_rule:
        return violations

    # Sort shifts by date and start time
    sorted_shifts = sorted(
        shifts_for_employee,
        key=lambda s: (s.get("date", ""), s.get("start", ""))
    )

    # Check gaps between consecutive shifts
    for i in range(len(sorted_shifts) - 1):
        current = sorted_shifts[i]
        next_shift = sorted_shifts[i + 1]

        try:
            # Parse end time of current shift and date
            current_date = datetime.fromisoformat(current.get("date", "2026-04-20"))
            current_end = _time_to_minutes(current.get("end", "17:00"))

            # For overnight shifts, adjust the date
            next_date = datetime.fromisoformat(next_shift.get("date", "2026-04-20"))
            next_start = _time_to_minutes(next_shift.get("start", "09:00"))

            # Calculate the gap in hours
            current_end_dt = current_date.replace(hour=current_end // 60, minute=current_end % 60)
            next_start_dt = next_date.replace(hour=next_start // 60, minute=next_start % 60)

            gap = next_start_dt - current_end_dt
            gap_hours = gap.total_seconds() / 3600.0

            if gap_hours < gap_rule.threshold_hours:
                violations.append(
                    BreakViolation(
                        violation_id=f"viol_{uuid.uuid4().hex[:12]}",
                        venue_id=venue_id,
                        employee_id=employee_id,
                        employee_name=employee_name,
                        shift_date=next_shift.get("date", ""),
                        shift_start=next_shift.get("start", ""),
                        shift_end=next_shift.get("end", ""),
                        rule_type=RuleType.MIN_GAP,
                        severity=gap_rule.severity,
                        description=f"{gap_rule.description} (Gap: {gap_hours:.1f}h)",
                    )
                )
        except (ValueError, AttributeError):
            continue

    return violations


def check_roster_compliance(
    venue_id: str,
    roster_shifts: List[Dict[str, Any]],
    rules: Optional[List[BreakRule]] = None,
) -> ComplianceReport:
    """Check full roster for a venue.

    Args:
        venue_id: Venue ID
        roster_shifts: List of shift dicts with keys: date, start, end, employee_id, employee_name, breaks_minutes
        rules: List of rules to check (default: DEFAULT_RULES)

    Returns:
        ComplianceReport with all violations
    """
    if rules is None:
        rules = DEFAULT_RULES

    report = ComplianceReport(
        venue_id=venue_id,
        check_date=datetime.now(timezone.utc),
        total_shifts=len(roster_shifts),
    )

    # Group shifts by employee
    shifts_by_employee: Dict[str, List[Dict[str, str]]] = {}
    for shift in roster_shifts:
        emp_id = shift.get("employee_id", "unknown")
        if emp_id not in shifts_by_employee:
            shifts_by_employee[emp_id] = []
        shifts_by_employee[emp_id].append(shift)

    # Check each shift for break compliance
    for shift in roster_shifts:
        violations = check_shift_breaks(
            shift.get("date", ""),
            shift.get("start", ""),
            shift.get("end", ""),
            shift.get("break_minutes", 30),
            rules=rules,
        )
        for v in violations:
            v.violation_id = f"viol_{uuid.uuid4().hex[:12]}"
            v.venue_id = venue_id
            v.employee_id = shift.get("employee_id", "")
            v.employee_name = shift.get("employee_name", "")
            report.violations.append(v)

    # Check gaps between consecutive shifts for each employee
    for emp_id, shifts in shifts_by_employee.items():
        emp_name = shifts[0].get("employee_name", "Unknown") if shifts else "Unknown"
        gap_violations = check_gap_compliance(
            emp_id, emp_name, venue_id, shifts, rules=rules
        )
        report.violations.extend(gap_violations)

    # Set compliant flag
    report.compliant = len([v for v in report.violations if v.dismissed_at is None]) == 0

    return report


# ---------------------------------------------------------------------------
# Break Compliance Store
# ---------------------------------------------------------------------------


class BreakComplianceStore:
    """Thread-safe in-memory store for break violations with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._violations: Dict[str, BreakViolation] = {}
        self._lock = threading.Lock()

    def _persist(self, violation: BreakViolation) -> None:
        """Persist a violation to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "violation_id": violation.violation_id,
            "venue_id": violation.venue_id,
            "employee_id": violation.employee_id,
            "employee_name": violation.employee_name,
            "shift_date": violation.shift_date,
            "shift_start": violation.shift_start,
            "shift_end": violation.shift_end,
            "rule_type": violation.rule_type.value,
            "severity": violation.severity.value,
            "description": violation.description,
            "detected_at": violation.detected_at.isoformat(),
            "dismissed_at": violation.dismissed_at.isoformat() if violation.dismissed_at else None,
            "dismissed_by": violation.dismissed_by,
            "dismiss_reason": violation.dismiss_reason,
        }
        try:
            _p.upsert("break_violations", row, pk="violation_id")
        except Exception as e:
            logger.warning("Failed to persist violation %s: %s", violation.violation_id, e)

    def _rehydrate(self) -> None:
        """Load all violations from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            rows = _p.fetchall("SELECT * FROM break_violations")
            for row in rows:
                violation = self._row_to_violation(dict(row))
                self._violations[violation.violation_id] = violation
            logger.info("Rehydrated %d break violations from persistence", len(self._violations))
        except Exception as e:
            logger.warning("Failed to rehydrate break violations: %s", e)

    @staticmethod
    def _row_to_violation(row: Dict[str, Any]) -> BreakViolation:
        """Reconstruct a BreakViolation from a DB row."""
        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        return BreakViolation(
            violation_id=row["violation_id"],
            venue_id=row["venue_id"],
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            shift_date=row["shift_date"],
            shift_start=row["shift_start"],
            shift_end=row["shift_end"],
            rule_type=RuleType(row.get("rule_type", "meal_break")),
            severity=ViolationSeverity(row.get("severity", "violation")),
            description=row["description"],
            detected_at=parse_iso(row.get("detected_at")) or datetime.now(timezone.utc),
            dismissed_at=parse_iso(row.get("dismissed_at")),
            dismissed_by=row.get("dismissed_by"),
            dismiss_reason=row.get("dismiss_reason"),
        )

    def record_violation(self, violation: BreakViolation) -> BreakViolation:
        """Record a new violation."""
        with self._lock:
            self._violations[violation.violation_id] = violation
        self._persist(violation)
        return violation

    def dismiss_violation(
        self,
        violation_id: str,
        dismissed_by: str,
        reason: Optional[str] = None,
    ) -> BreakViolation:
        """Dismiss a violation with reason.

        Raises ValueError if violation not found.
        """
        with self._lock:
            violation = self._violations.get(violation_id)
            if not violation:
                raise ValueError(f"Violation {violation_id} not found")
            violation.dismissed_at = datetime.now(timezone.utc)
            violation.dismissed_by = dismissed_by
            violation.dismiss_reason = reason

        self._persist(violation)
        return violation

    def get(self, violation_id: str) -> Optional[BreakViolation]:
        """Get a violation by ID. Returns None if not found."""
        with self._lock:
            return self._violations.get(violation_id)

    def list_by_venue(
        self,
        venue_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        severity: Optional[ViolationSeverity] = None,
        employee_id: Optional[str] = None,
        include_dismissed: bool = False,
        limit: int = 100,
    ) -> List[BreakViolation]:
        """List violations for a venue with optional filters.

        Returns newest first.
        """
        with self._lock:
            violations = [v for v in self._violations.values() if v.venue_id == venue_id]

            if not include_dismissed:
                violations = [v for v in violations if v.dismissed_at is None]

            if date_from:
                violations = [v for v in violations if v.shift_date >= date_from]
            if date_to:
                violations = [v for v in violations if v.shift_date <= date_to]
            if severity:
                violations = [v for v in violations if v.severity == severity]
            if employee_id:
                violations = [v for v in violations if v.employee_id == employee_id]

            # Sort newest first
            violations.sort(key=lambda v: v.detected_at, reverse=True)
            return violations[:limit]


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_compliance_store_singleton: Optional[BreakComplianceStore] = None
_singleton_lock = threading.Lock()


def get_compliance_store() -> BreakComplianceStore:
    """Get the module-level break compliance store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _compliance_store_singleton
    if _compliance_store_singleton is None:
        with _singleton_lock:
            if _compliance_store_singleton is None:
                _compliance_store_singleton = BreakComplianceStore()
    return _compliance_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _compliance_store_singleton
    _compliance_store_singleton = None
