"""Timesheet Reconciliation Engine for Australian hospitality venues (Round 30).

Compares rostered shifts against actual timesheets to detect discrepancies,
no-shows, over-rostering, and unrostered work. Persists reconciliation records
to SQLite for reporting and pattern analysis.

Implements:
- Shift-by-shift reconciliation with status classification
- Variance calculations (hours and costs)
- Recurring pattern detection (no-shows, over-rostering, etc.)
- Period summaries with match rates and cost variance
- Support for penalty rates (weekends, public holidays, evening shifts)
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.timesheet_recon")


# ---------------------------------------------------------------------------
# Schemas — register with persistence layer
# ---------------------------------------------------------------------------

_SHIFT_RECON_SCHEMA = """
CREATE TABLE IF NOT EXISTS shift_recon (
    recon_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    rostered_start TEXT,
    rostered_end TEXT,
    rostered_hours REAL NOT NULL,
    actual_start TEXT,
    actual_end TEXT,
    actual_hours REAL NOT NULL,
    variance_hours REAL NOT NULL,
    variance_pct REAL,
    rostered_cost REAL,
    actual_cost REAL,
    cost_variance REAL,
    status TEXT NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_shift_recon_venue ON shift_recon(venue_id);
CREATE INDEX IF NOT EXISTS ix_shift_recon_date ON shift_recon(shift_date);
CREATE INDEX IF NOT EXISTS ix_shift_recon_employee ON shift_recon(employee_id);
CREATE INDEX IF NOT EXISTS ix_shift_recon_status ON shift_recon(status);
"""
_p.register_schema("shift_recon", _SHIFT_RECON_SCHEMA)

_RECON_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS recon_summary (
    summary_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    total_rostered_hours REAL NOT NULL,
    total_actual_hours REAL NOT NULL,
    total_variance_hours REAL NOT NULL,
    total_rostered_cost REAL,
    total_actual_cost REAL,
    total_cost_variance REAL,
    match_rate_pct REAL,
    no_show_count INTEGER NOT NULL,
    late_start_count INTEGER NOT NULL,
    early_finish_count INTEGER NOT NULL,
    over_roster_count INTEGER NOT NULL,
    under_roster_count INTEGER NOT NULL,
    unrostered_count INTEGER NOT NULL,
    shifts_reconciled INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_recon_summary_venue ON recon_summary(venue_id);
CREATE INDEX IF NOT EXISTS ix_recon_summary_period ON recon_summary(period_start, period_end);
"""
_p.register_schema("recon_summary", _RECON_SUMMARY_SCHEMA)


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class ReconStatus(str, Enum):
    """Reconciliation status for a shift."""
    MATCHED = "matched"
    UNDER_ROSTERED = "under_rostered"
    OVER_ROSTERED = "over_rostered"
    NO_SHOW = "no_show"
    UNROSTERED_CLOCK_IN = "unrostered_clock_in"
    LATE_START = "late_start"
    EARLY_FINISH = "early_finish"


@dataclass
class ShiftRecon:
    """Reconciliation record for a single shift."""
    recon_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    shift_date: str  # ISO date (YYYY-MM-DD)
    rostered_start: Optional[str]  # HH:MM
    rostered_end: Optional[str]  # HH:MM
    rostered_hours: float
    actual_start: Optional[str]  # HH:MM
    actual_end: Optional[str]  # HH:MM
    actual_hours: float
    variance_hours: float  # actual - rostered
    variance_pct: Optional[float]  # (actual - rostered) / rostered * 100
    rostered_cost: Optional[float]  # AUD
    actual_cost: Optional[float]  # AUD
    cost_variance: Optional[float]  # actual - rostered
    status: ReconStatus
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recon_id": self.recon_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "shift_date": self.shift_date,
            "rostered_start": self.rostered_start,
            "rostered_end": self.rostered_end,
            "rostered_hours": self.rostered_hours,
            "actual_start": self.actual_start,
            "actual_end": self.actual_end,
            "actual_hours": self.actual_hours,
            "variance_hours": self.variance_hours,
            "variance_pct": self.variance_pct,
            "rostered_cost": self.rostered_cost,
            "actual_cost": self.actual_cost,
            "cost_variance": self.cost_variance,
            "status": self.status.value,
            "notes": self.notes,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ReconSummary:
    """Aggregated reconciliation summary for a venue over a period."""
    summary_id: str
    venue_id: str
    period_start: str  # ISO date
    period_end: str  # ISO date
    total_rostered_hours: float
    total_actual_hours: float
    total_variance_hours: float
    total_rostered_cost: Optional[float]
    total_actual_cost: Optional[float]
    total_cost_variance: Optional[float]
    match_rate_pct: float  # percentage of shifts matched without variance
    no_show_count: int
    late_start_count: int
    early_finish_count: int
    over_roster_count: int
    under_roster_count: int
    unrostered_count: int
    shifts_reconciled: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary_id": self.summary_id,
            "venue_id": self.venue_id,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "total_rostered_hours": self.total_rostered_hours,
            "total_actual_hours": self.total_actual_hours,
            "total_variance_hours": self.total_variance_hours,
            "total_rostered_cost": self.total_rostered_cost,
            "total_actual_cost": self.total_actual_cost,
            "total_cost_variance": self.total_cost_variance,
            "match_rate_pct": self.match_rate_pct,
            "no_show_count": self.no_show_count,
            "late_start_count": self.late_start_count,
            "early_finish_count": self.early_finish_count,
            "over_roster_count": self.over_roster_count,
            "under_roster_count": self.under_roster_count,
            "unrostered_count": self.unrostered_count,
            "shifts_reconciled": self.shifts_reconciled,
            "created_at": self.created_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Shift cost calculation with AU penalty rates
# ---------------------------------------------------------------------------


def calculate_shift_cost_simple(
    hours: float,
    hourly_rate: float,
    day_of_week: int,
    start_hour: int,
) -> float:
    """Calculate shift cost with simplified AU penalty rates.

    Args:
        hours: Total hours worked
        hourly_rate: Base hourly rate (AUD)
        day_of_week: 0=Monday, 6=Sunday
        start_hour: Start hour of shift (0-23)

    Returns:
        Total cost in AUD including penalty multipliers
    """
    if hours <= 0 or hourly_rate <= 0:
        return 0.0

    # Base multiplier starts at 1.0
    multiplier = 1.0

    # Saturday +25%, Sunday +50%
    if day_of_week == 5:  # Saturday
        multiplier = 1.25
    elif day_of_week == 6:  # Sunday
        multiplier = 1.50

    # Evening shift (6pm-midnight) +15% if not already a weekend
    if 18 <= start_hour < 24 and day_of_week < 5:
        multiplier = 1.15

    return hours * hourly_rate * multiplier


# ---------------------------------------------------------------------------
# Core reconciliation logic
# ---------------------------------------------------------------------------


def reconcile_shift(
    rostered: Dict[str, Any],
    actual: Dict[str, Any],
) -> ShiftRecon:
    """Reconcile a single rostered shift against actual timesheet data.

    Args:
        rostered: dict with keys: employee_id, employee_name, venue_id,
                  shift_date, start, end, hours, hourly_rate
        actual: dict with keys: employee_id, shift_date, start, end, hours, hourly_rate

    Returns:
        ShiftRecon record with status and variance calculations
    """
    recon_id = f"recon_{uuid.uuid4().hex[:12]}"
    created_at = datetime.now(timezone.utc)

    employee_id = rostered.get("employee_id", "")
    employee_name = rostered.get("employee_name", "")
    venue_id = rostered.get("venue_id", "")
    shift_date = rostered.get("shift_date", "")

    rostered_start = rostered.get("start")
    rostered_end = rostered.get("end")
    rostered_hours = float(rostered.get("hours", 0.0))
    rostered_rate = float(rostered.get("hourly_rate", 0.0))

    actual_start = actual.get("start")
    actual_end = actual.get("end")
    actual_hours = float(actual.get("hours", 0.0))
    actual_rate = float(actual.get("hourly_rate", 0.0))

    # Calculate variance
    variance_hours = actual_hours - rostered_hours
    variance_pct = None
    if rostered_hours > 0:
        variance_pct = (variance_hours / rostered_hours) * 100

    # Cost calculations
    rostered_cost = None
    actual_cost = None
    cost_variance = None

    if rostered_hours > 0 and rostered_rate > 0 and rostered_start:
        # Parse day of week from shift_date (YYYY-MM-DD)
        try:
            date_obj = datetime.strptime(shift_date, "%Y-%m-%d").date()
            day_of_week = date_obj.weekday()
            start_h = int(rostered_start.split(":")[0])
            rostered_cost = calculate_shift_cost_simple(
                rostered_hours, rostered_rate, day_of_week, start_h
            )
        except (ValueError, IndexError):
            rostered_cost = rostered_hours * rostered_rate

    if actual_hours > 0 and actual_rate > 0 and actual_start:
        try:
            date_obj = datetime.strptime(shift_date, "%Y-%m-%d").date()
            day_of_week = date_obj.weekday()
            start_h = int(actual_start.split(":")[0])
            actual_cost = calculate_shift_cost_simple(
                actual_hours, actual_rate, day_of_week, start_h
            )
        except (ValueError, IndexError):
            actual_cost = actual_hours * actual_rate

    if rostered_cost is not None and actual_cost is not None:
        cost_variance = actual_cost - rostered_cost

    # Determine status
    status = ReconStatus.MATCHED
    notes = None

    # No actual clock in/out
    if actual_hours == 0 or not actual_start:
        status = ReconStatus.NO_SHOW
        notes = "Employee did not clock in or work recorded"

    # No roster entry but actual hours worked
    elif rostered_hours == 0 and actual_hours > 0:
        status = ReconStatus.UNROSTERED_CLOCK_IN
        notes = f"Worked {actual_hours}h but not rostered"

    # Check for late start (>15 min) or early finish (>15 min)
    elif actual_start and rostered_start:
        try:
            r_start = datetime.strptime(rostered_start, "%H:%M")
            a_start = datetime.strptime(actual_start, "%H:%M")
            diff_min = (a_start - r_start).total_seconds() / 60
            if diff_min > 15:
                status = ReconStatus.LATE_START
                notes = f"Started {int(diff_min)} min late"
        except ValueError:
            pass

    # Check early finish only if not already marked as late start
    if status == ReconStatus.MATCHED and actual_end and rostered_end:
        try:
            r_end = datetime.strptime(rostered_end, "%H:%M")
            a_end = datetime.strptime(actual_end, "%H:%M")
            diff_min = (r_end - a_end).total_seconds() / 60
            if diff_min > 15:
                status = ReconStatus.EARLY_FINISH
                notes = f"Finished {int(diff_min)} min early"
        except ValueError:
            pass

    # Variance threshold checks (only if no show/unrostered/late/early)
    if status == ReconStatus.MATCHED:
        if variance_pct is not None:
            if variance_pct > 10:
                status = ReconStatus.OVER_ROSTERED
                notes = f"Worked {variance_pct:.1f}% more than rostered"
            elif variance_pct < -10:
                status = ReconStatus.UNDER_ROSTERED
                notes = f"Worked {abs(variance_pct):.1f}% less than rostered"

    return ShiftRecon(
        recon_id=recon_id,
        employee_id=employee_id,
        employee_name=employee_name,
        venue_id=venue_id,
        shift_date=shift_date,
        rostered_start=rostered_start,
        rostered_end=rostered_end,
        rostered_hours=rostered_hours,
        actual_start=actual_start,
        actual_end=actual_end,
        actual_hours=actual_hours,
        variance_hours=variance_hours,
        variance_pct=variance_pct,
        rostered_cost=rostered_cost,
        actual_cost=actual_cost,
        cost_variance=cost_variance,
        status=status,
        notes=notes,
        created_at=created_at,
    )


def reconcile_day(
    venue_id: str,
    shift_date: str,
    rostered_shifts: List[Dict[str, Any]],
    actual_shifts: List[Dict[str, Any]],
) -> List[ShiftRecon]:
    """Reconcile all shifts for a venue on a given date.

    Matches rostered shifts to actual timesheets by employee_id, then
    reconciles each pair.

    Args:
        venue_id: Venue identifier
        shift_date: ISO date string (YYYY-MM-DD)
        rostered_shifts: List of rostered shift dicts
        actual_shifts: List of actual timesheet dicts

    Returns:
        List of ShiftRecon records
    """
    recons = []

    # Index actual shifts by employee_id for quick lookup
    actual_by_emp = {}
    for shift in actual_shifts:
        emp_id = shift.get("employee_id", "")
        if emp_id not in actual_by_emp:
            actual_by_emp[emp_id] = []
        actual_by_emp[emp_id].append(shift)

    # Reconcile each rostered shift
    for rostered in rostered_shifts:
        emp_id = rostered.get("employee_id", "")
        actuals = actual_by_emp.get(emp_id, [])

        if actuals:
            # For now, pair with the first actual shift
            # (simple 1-to-1 matching; could be enhanced for split shifts)
            actual = actuals[0]
        else:
            # No actual shift for this employee
            actual = {
                "employee_id": emp_id,
                "shift_date": shift_date,
                "start": None,
                "end": None,
                "hours": 0.0,
                "hourly_rate": rostered.get("hourly_rate", 0.0),
            }

        recon = reconcile_shift(rostered, actual)
        recons.append(recon)

    # Unrostered actuals (employee clocked in but not on roster)
    rostered_emps = {r.get("employee_id", "") for r in rostered_shifts}
    for emp_id, actuals in actual_by_emp.items():
        if emp_id not in rostered_emps:
            for actual in actuals:
                rostered = {
                    "employee_id": emp_id,
                    "employee_name": actual.get("employee_name", ""),
                    "venue_id": venue_id,
                    "shift_date": shift_date,
                    "start": None,
                    "end": None,
                    "hours": 0.0,
                    "hourly_rate": 0.0,
                }
                recon = reconcile_shift(rostered, actual)
                recons.append(recon)

    return recons


# ---------------------------------------------------------------------------
# Summary and pattern detection
# ---------------------------------------------------------------------------


def build_recon_summary(
    recons: List[ShiftRecon],
    venue_id: str,
    period_start: str,
    period_end: str,
) -> ReconSummary:
    """Build an aggregated reconciliation summary from shift records.

    Args:
        recons: List of ShiftRecon records
        venue_id: Venue identifier
        period_start: ISO date (YYYY-MM-DD)
        period_end: ISO date (YYYY-MM-DD)

    Returns:
        ReconSummary with aggregated stats
    """
    summary_id = f"summary_{uuid.uuid4().hex[:12]}"

    total_rostered_hours = sum(r.rostered_hours for r in recons)
    total_actual_hours = sum(r.actual_hours for r in recons)
    total_variance_hours = total_actual_hours - total_rostered_hours

    total_rostered_cost = sum(r.rostered_cost or 0.0 for r in recons)
    total_actual_cost = sum(r.actual_cost or 0.0 for r in recons)
    total_cost_variance = total_actual_cost - total_rostered_cost

    # Count statuses
    matched = sum(1 for r in recons if r.status == ReconStatus.MATCHED)
    match_rate_pct = (matched / len(recons) * 100) if recons else 0.0

    status_counts = {}
    for status in ReconStatus:
        status_counts[status] = sum(1 for r in recons if r.status == status)

    return ReconSummary(
        summary_id=summary_id,
        venue_id=venue_id,
        period_start=period_start,
        period_end=period_end,
        total_rostered_hours=total_rostered_hours,
        total_actual_hours=total_actual_hours,
        total_variance_hours=total_variance_hours,
        total_rostered_cost=total_rostered_cost if total_rostered_cost > 0 else None,
        total_actual_cost=total_actual_cost if total_actual_cost > 0 else None,
        total_cost_variance=total_cost_variance if total_cost_variance != 0 else None,
        match_rate_pct=match_rate_pct,
        no_show_count=status_counts.get(ReconStatus.NO_SHOW, 0),
        late_start_count=status_counts.get(ReconStatus.LATE_START, 0),
        early_finish_count=status_counts.get(ReconStatus.EARLY_FINISH, 0),
        over_roster_count=status_counts.get(ReconStatus.OVER_ROSTERED, 0),
        under_roster_count=status_counts.get(ReconStatus.UNDER_ROSTERED, 0),
        unrostered_count=status_counts.get(ReconStatus.UNROSTERED_CLOCK_IN, 0),
        shifts_reconciled=len(recons),
    )


def detect_patterns(recons: List[ShiftRecon]) -> Dict[str, Any]:
    """Detect recurring issues and patterns in reconciliation records.

    Identifies:
    - Employees with frequent no-shows
    - Venues/departments consistently over-rostered
    - Recurring late arrivals
    - Cost variance trends

    Args:
        recons: List of ShiftRecon records

    Returns:
        Dict with pattern categories and details
    """
    patterns = {
        "frequent_no_shows": {},
        "frequent_late_starts": {},
        "frequent_early_finishes": {},
        "over_rostered_trend": {},
        "under_rostered_trend": {},
        "high_cost_variance": [],
    }

    # Count by employee
    emp_no_shows = {}
    emp_late = {}
    emp_early = {}
    emp_over = {}
    emp_under = {}

    for recon in recons:
        emp_id = recon.employee_id
        emp_name = recon.employee_name

        if recon.status == ReconStatus.NO_SHOW:
            if emp_id not in emp_no_shows:
                emp_no_shows[emp_id] = {"name": emp_name, "count": 0}
            emp_no_shows[emp_id]["count"] += 1

        elif recon.status == ReconStatus.LATE_START:
            if emp_id not in emp_late:
                emp_late[emp_id] = {"name": emp_name, "count": 0}
            emp_late[emp_id]["count"] += 1

        elif recon.status == ReconStatus.EARLY_FINISH:
            if emp_id not in emp_early:
                emp_early[emp_id] = {"name": emp_name, "count": 0}
            emp_early[emp_id]["count"] += 1

        elif recon.status == ReconStatus.OVER_ROSTERED:
            if emp_id not in emp_over:
                emp_over[emp_id] = {"name": emp_name, "count": 0}
            emp_over[emp_id]["count"] += 1

        elif recon.status == ReconStatus.UNDER_ROSTERED:
            if emp_id not in emp_under:
                emp_under[emp_id] = {"name": emp_name, "count": 0}
            emp_under[emp_id]["count"] += 1

        # Track high cost variance
        if recon.cost_variance and abs(recon.cost_variance) > 50:  # >$50
            patterns["high_cost_variance"].append({
                "employee_id": emp_id,
                "employee_name": emp_name,
                "shift_date": recon.shift_date,
                "variance_aud": round(recon.cost_variance, 2),
            })

    # Filter for recurring (2+) occurrences
    patterns["frequent_no_shows"] = {
        k: v for k, v in emp_no_shows.items() if v["count"] >= 2
    }
    patterns["frequent_late_starts"] = {
        k: v for k, v in emp_late.items() if v["count"] >= 2
    }
    patterns["frequent_early_finishes"] = {
        k: v for k, v in emp_early.items() if v["count"] >= 2
    }
    patterns["over_rostered_trend"] = {
        k: v for k, v in emp_over.items() if v["count"] >= 2
    }
    patterns["under_rostered_trend"] = {
        k: v for k, v in emp_under.items() if v["count"] >= 2
    }

    return patterns


# ---------------------------------------------------------------------------
# Persistence store
# ---------------------------------------------------------------------------


class ReconStore:
    """Thread-safe store for reconciliation records with SQLite persistence."""

    def __init__(self):
        self._lock = threading.Lock()
        self._recons: Dict[str, ShiftRecon] = {}
        self._summaries: Dict[str, ReconSummary] = {}
        self._rehydrate()

    def _rehydrate(self):
        """Load existing records from SQLite on startup."""
        if not _p.is_persistence_enabled():
            return

        try:
            conn = _p.connection()

            # Load shift reconciliations
            cursor = conn.execute(
                "SELECT * FROM shift_recon ORDER BY created_at DESC LIMIT 1000"
            )
            for row in cursor.fetchall():
                recon = ShiftRecon(
                    recon_id=row["recon_id"],
                    employee_id=row["employee_id"],
                    employee_name=row["employee_name"],
                    venue_id=row["venue_id"],
                    shift_date=row["shift_date"],
                    rostered_start=row["rostered_start"],
                    rostered_end=row["rostered_end"],
                    rostered_hours=row["rostered_hours"],
                    actual_start=row["actual_start"],
                    actual_end=row["actual_end"],
                    actual_hours=row["actual_hours"],
                    variance_hours=row["variance_hours"],
                    variance_pct=row["variance_pct"],
                    rostered_cost=row["rostered_cost"],
                    actual_cost=row["actual_cost"],
                    cost_variance=row["cost_variance"],
                    status=ReconStatus(row["status"]),
                    notes=row["notes"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                self._recons[recon.recon_id] = recon

            # Load summaries
            cursor = conn.execute(
                "SELECT * FROM recon_summary ORDER BY created_at DESC LIMIT 100"
            )
            for row in cursor.fetchall():
                summary = ReconSummary(
                    summary_id=row["summary_id"],
                    venue_id=row["venue_id"],
                    period_start=row["period_start"],
                    period_end=row["period_end"],
                    total_rostered_hours=row["total_rostered_hours"],
                    total_actual_hours=row["total_actual_hours"],
                    total_variance_hours=row["total_variance_hours"],
                    total_rostered_cost=row["total_rostered_cost"],
                    total_actual_cost=row["total_actual_cost"],
                    total_cost_variance=row["total_cost_variance"],
                    match_rate_pct=row["match_rate_pct"],
                    no_show_count=row["no_show_count"],
                    late_start_count=row["late_start_count"],
                    early_finish_count=row["early_finish_count"],
                    over_roster_count=row["over_roster_count"],
                    under_roster_count=row["under_roster_count"],
                    unrostered_count=row["unrostered_count"],
                    shifts_reconciled=row["shifts_reconciled"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                self._summaries[summary.summary_id] = summary

        except Exception as e:
            logger.error("rehydrate failed: %s", e)

    def persist_shift_recon(self, recon: ShiftRecon) -> None:
        """Store a shift reconciliation to SQLite."""
        with self._lock:
            self._recons[recon.recon_id] = recon

            if not _p.is_persistence_enabled():
                return

            try:
                conn = _p.connection()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO shift_recon (
                        recon_id, employee_id, employee_name, venue_id,
                        shift_date, rostered_start, rostered_end, rostered_hours,
                        actual_start, actual_end, actual_hours,
                        variance_hours, variance_pct,
                        rostered_cost, actual_cost, cost_variance,
                        status, notes, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recon.recon_id,
                        recon.employee_id,
                        recon.employee_name,
                        recon.venue_id,
                        recon.shift_date,
                        recon.rostered_start,
                        recon.rostered_end,
                        recon.rostered_hours,
                        recon.actual_start,
                        recon.actual_end,
                        recon.actual_hours,
                        recon.variance_hours,
                        recon.variance_pct,
                        recon.rostered_cost,
                        recon.actual_cost,
                        recon.cost_variance,
                        recon.status.value,
                        recon.notes,
                        recon.created_at.isoformat(),
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.error("persist_shift_recon failed: %s", e)

    def persist_summary(self, summary: ReconSummary) -> None:
        """Store a reconciliation summary to SQLite."""
        with self._lock:
            self._summaries[summary.summary_id] = summary

            if not _p.is_persistence_enabled():
                return

            try:
                conn = _p.connection()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO recon_summary (
                        summary_id, venue_id, period_start, period_end,
                        total_rostered_hours, total_actual_hours, total_variance_hours,
                        total_rostered_cost, total_actual_cost, total_cost_variance,
                        match_rate_pct,
                        no_show_count, late_start_count, early_finish_count,
                        over_roster_count, under_roster_count, unrostered_count,
                        shifts_reconciled, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        summary.summary_id,
                        summary.venue_id,
                        summary.period_start,
                        summary.period_end,
                        summary.total_rostered_hours,
                        summary.total_actual_hours,
                        summary.total_variance_hours,
                        summary.total_rostered_cost,
                        summary.total_actual_cost,
                        summary.total_cost_variance,
                        summary.match_rate_pct,
                        summary.no_show_count,
                        summary.late_start_count,
                        summary.early_finish_count,
                        summary.over_roster_count,
                        summary.under_roster_count,
                        summary.unrostered_count,
                        summary.shifts_reconciled,
                        summary.created_at.isoformat(),
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.error("persist_summary failed: %s", e)

    def get_recon(self, recon_id: str) -> Optional[ShiftRecon]:
        """Retrieve a shift reconciliation by ID."""
        with self._lock:
            return self._recons.get(recon_id)

    def query_recons(
        self,
        venue_id: Optional[str] = None,
        shift_date: Optional[str] = None,
        employee_id: Optional[str] = None,
        status: Optional[ReconStatus] = None,
    ) -> List[ShiftRecon]:
        """Query shift reconciliations with optional filters."""
        results = []
        with self._lock:
            for recon in self._recons.values():
                if venue_id and recon.venue_id != venue_id:
                    continue
                if shift_date and recon.shift_date != shift_date:
                    continue
                if employee_id and recon.employee_id != employee_id:
                    continue
                if status and recon.status != status:
                    continue
                results.append(recon)
        return results

    def query_summaries(
        self,
        venue_id: Optional[str] = None,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> List[ReconSummary]:
        """Query reconciliation summaries with optional filters."""
        results = []
        with self._lock:
            for summary in self._summaries.values():
                if venue_id and summary.venue_id != venue_id:
                    continue
                if period_start and summary.period_start < period_start:
                    continue
                if period_end and summary.period_end > period_end:
                    continue
                results.append(summary)
        return results

    def clear(self) -> None:
        """Clear all in-memory records (for testing)."""
        with self._lock:
            self._recons.clear()
            self._summaries.clear()


# Singleton store
_store = ReconStore()


def store() -> ReconStore:
    """Get the global reconciliation store."""
    return _store
