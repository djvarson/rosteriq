"""Labour Budget Guardrails for Australian hospitality venues (Round 26).

Provides real-time visibility into wage costs vs labour percentage budgets.
Implements:
- Budget threshold management (target %, warning %, critical %)
- Shift cost projections with penalty rates (Sat +25%, Sun +50%, evening +15%, public holiday +125%)
- Roster cost calculation with hourly rates
- Budget snapshot generation with alerts
- Hours-remaining projection for budget planning
- What-if scenario analysis

Data is SQLite-persisted for venue budgets and alerts.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.labour_budget")


# ---------------------------------------------------------------------------
# Schemas — register with persistence layer
# ---------------------------------------------------------------------------

_BUDGET_THRESHOLDS_SCHEMA = """
CREATE TABLE IF NOT EXISTS budget_thresholds (
    venue_id TEXT PRIMARY KEY,
    target_labour_pct REAL NOT NULL,
    warning_labour_pct REAL NOT NULL,
    critical_labour_pct REAL NOT NULL,
    max_wage_cost_per_hour REAL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""
_p.register_schema("budget_thresholds", _BUDGET_THRESHOLDS_SCHEMA)

_BUDGET_ALERTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS budget_alerts (
    alert_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    current_labour_pct REAL NOT NULL,
    target_labour_pct REAL NOT NULL,
    current_wage_cost REAL NOT NULL,
    projected_revenue REAL,
    message TEXT,
    shift_date TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_budget_alerts_venue ON budget_alerts(venue_id);
CREATE INDEX IF NOT EXISTS ix_budget_alerts_date ON budget_alerts(shift_date);
"""
_p.register_schema("budget_alerts", _BUDGET_ALERTS_SCHEMA)


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class AlertType(str, Enum):
    """Budget alert severity levels."""
    ON_TRACK = "on_track"
    WARNING = "warning"
    OVER_BUDGET = "over_budget"
    CRITICAL = "critical"


@dataclass
class BudgetThreshold:
    """Budget threshold configuration for a venue."""
    venue_id: str
    target_labour_pct: float  # e.g. 30.0 for 30%
    warning_labour_pct: float  # e.g. 28.0 for 28%
    critical_labour_pct: float  # e.g. 33.0 for 33%
    max_wage_cost_per_hour: Optional[float] = None  # optional AUD cap per hour
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "target_labour_pct": self.target_labour_pct,
            "warning_labour_pct": self.warning_labour_pct,
            "critical_labour_pct": self.critical_labour_pct,
            "max_wage_cost_per_hour": self.max_wage_cost_per_hour,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class BudgetAlert:
    """A budget alert generated when thresholds are crossed."""
    alert_id: str
    venue_id: str
    alert_type: AlertType
    current_labour_pct: float
    target_labour_pct: float
    current_wage_cost: float
    projected_revenue: Optional[float] = None
    message: Optional[str] = None
    shift_date: Optional[str] = None  # ISO date YYYY-MM-DD
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "venue_id": self.venue_id,
            "alert_type": self.alert_type.value,
            "current_labour_pct": self.current_labour_pct,
            "target_labour_pct": self.target_labour_pct,
            "current_wage_cost": self.current_wage_cost,
            "projected_revenue": self.projected_revenue,
            "message": self.message,
            "shift_date": self.shift_date,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class ShiftCostProjection:
    """Cost projection for a single shift including penalties."""
    employee_id: str
    employee_name: str
    shift_start: str  # HH:MM format
    shift_end: str  # HH:MM format
    base_cost: float  # cost at ordinary rate
    penalty_cost: float  # additional cost from penalties
    total_cost: float  # base_cost + penalty_cost
    hourly_rate: float  # ordinary hourly rate
    is_overtime: bool = False
    is_penalty_rate: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "shift_start": self.shift_start,
            "shift_end": self.shift_end,
            "base_cost": self.base_cost,
            "penalty_cost": self.penalty_cost,
            "total_cost": self.total_cost,
            "hourly_rate": self.hourly_rate,
            "is_overtime": self.is_overtime,
            "is_penalty_rate": self.is_penalty_rate,
        }


@dataclass
class BudgetSnapshot:
    """Complete budget picture for a venue at a point in time."""
    venue_id: str
    snapshot_date: str  # ISO date YYYY-MM-DD
    total_wage_cost: float
    projected_revenue: float
    labour_pct: float  # (total_wage_cost / projected_revenue) * 100
    headcount: int  # number of shifts
    avg_hourly_cost: float  # average cost per shift hour
    shift_costs: List[ShiftCostProjection] = field(default_factory=list)
    alerts: List[BudgetAlert] = field(default_factory=list)
    hours_remaining_in_budget: Optional[float] = None  # hours at avg rate before hitting target
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "snapshot_date": self.snapshot_date,
            "total_wage_cost": self.total_wage_cost,
            "projected_revenue": self.projected_revenue,
            "labour_pct": self.labour_pct,
            "headcount": self.headcount,
            "avg_hourly_cost": self.avg_hourly_cost,
            "shift_costs": [s.to_dict() for s in self.shift_costs],
            "alerts": [a.to_dict() for a in self.alerts],
            "hours_remaining_in_budget": self.hours_remaining_in_budget,
            "created_at": self.created_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Default thresholds
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS = BudgetThreshold(
    venue_id="__default__",
    target_labour_pct=30.0,
    warning_labour_pct=28.0,
    critical_labour_pct=35.0,
)


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def _shift_duration_hours(shift_start: str, shift_end: str) -> float:
    """Calculate shift duration in hours from HH:MM strings.

    Handles overnight shifts (e.g. 22:00 to 06:00 = 8 hours).
    """
    try:
        start_parts = shift_start.split(":")
        end_parts = shift_end.split(":")

        start_mins = int(start_parts[0]) * 60 + int(start_parts[1])
        end_mins = int(end_parts[0]) * 60 + int(end_parts[1])

        # If end time is before start time, assume it's the next day
        if end_mins <= start_mins:
            end_mins += 24 * 60

        duration_mins = end_mins - start_mins
        return duration_mins / 60.0
    except (ValueError, IndexError):
        return 0.0


def _get_penalty_multiplier(shift_start: str, shift_end: str, shift_date: str, day_of_week: int = 0, is_public_holiday: bool = False) -> float:
    """Calculate penalty multiplier for shift.

    Applies:
    - Saturday: +25% (multiplier 1.25)
    - Sunday: +50% (multiplier 1.50)
    - After 7pm: +15% (multiplier 1.15)
    - Public holiday: +125% (multiplier 2.25)

    If multiple apply, use the highest. day_of_week: 0=Monday, 5=Saturday, 6=Sunday.
    """
    multipliers = [1.0]  # base

    # Day of week penalties
    if is_public_holiday:
        multipliers.append(2.25)
    elif day_of_week == 5:  # Saturday
        multipliers.append(1.25)
    elif day_of_week == 6:  # Sunday
        multipliers.append(1.50)

    # Evening penalty (after 7pm / 19:00)
    try:
        end_parts = shift_end.split(":")
        end_hour = int(end_parts[0])
        if end_hour >= 19:
            multipliers.append(1.15)
    except (ValueError, IndexError):
        pass

    # Return highest multiplier
    return max(multipliers)


# ---------------------------------------------------------------------------
# Core Calculations
# ---------------------------------------------------------------------------


def calculate_shift_cost(
    shift: Dict[str, Any],
    hourly_rate: float,
    employment_type: str = "casual",
    day_of_week: Optional[int] = None,
) -> ShiftCostProjection:
    """Compute cost for a single shift including penalty rates.

    Args:
        shift: dict with keys shift_start, shift_end, shift_date, employee_id, employee_name
        hourly_rate: ordinary hourly rate in AUD
        employment_type: "casual", "part_time", or "full_time"
        day_of_week: 0=Monday, 6=Sunday (parsed from shift_date if not provided)

    Returns:
        ShiftCostProjection with base, penalty, and total costs.
    """
    employee_id = shift.get("employee_id", "unknown")
    employee_name = shift.get("employee_name", "Unknown")
    shift_start = shift.get("shift_start", "00:00")
    shift_end = shift.get("shift_end", "00:00")
    shift_date = shift.get("shift_date", "")

    # Calculate duration
    hours = _shift_duration_hours(shift_start, shift_end)

    # Get day of week (default 0 = Monday; shift_date used if available)
    if day_of_week is None:
        day_of_week = 0
        is_public_holiday = False
        if shift_date:
            try:
                from datetime import datetime as dt
                parsed = dt.strptime(shift_date, "%Y-%m-%d")
                day_of_week = parsed.weekday()  # 0=Monday, 6=Sunday
                # Could check against a public holiday list; for now just flag available
            except (ValueError, ImportError):
                pass
    else:
        is_public_holiday = False

    # Calculate penalty multiplier
    penalty_mult = _get_penalty_multiplier(
        shift_start, shift_end, shift_date, day_of_week, is_public_holiday
    )

    # Base cost at ordinary rate
    base_cost = hours * hourly_rate

    # If penalty applies, calculate the additional cost
    penalty_cost = base_cost * (penalty_mult - 1.0) if penalty_mult > 1.0 else 0.0

    total_cost = base_cost + penalty_cost

    return ShiftCostProjection(
        employee_id=employee_id,
        employee_name=employee_name,
        shift_start=shift_start,
        shift_end=shift_end,
        base_cost=round(base_cost, 2),
        penalty_cost=round(penalty_cost, 2),
        total_cost=round(total_cost, 2),
        hourly_rate=hourly_rate,
        is_overtime=hours > 8.0,
        is_penalty_rate=penalty_mult > 1.0,
    )


def calculate_roster_cost(shifts: List[Dict[str, Any]], rates_map: Dict[str, float]) -> float:
    """Calculate total cost for a set of shifts.

    Args:
        shifts: list of shift dicts
        rates_map: dict mapping employee_id to hourly_rate

    Returns:
        Total wage cost as float.
    """
    total = 0.0
    for shift in shifts:
        employee_id = shift.get("employee_id", "")
        rate = rates_map.get(employee_id, 25.0)  # default $25/hr
        projection = calculate_shift_cost(shift, rate)
        total += projection.total_cost
    return round(total, 2)


def check_budget_alerts(
    snapshot: BudgetSnapshot, thresholds: Optional[BudgetThreshold] = None
) -> List[BudgetAlert]:
    """Generate alerts based on threshold crossings.

    Args:
        snapshot: BudgetSnapshot to check
        thresholds: BudgetThreshold to compare against; uses DEFAULT if None

    Returns:
        List of BudgetAlert objects.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    alerts = []
    labour_pct = snapshot.labour_pct

    # Determine alert type and message
    alert_type = AlertType.ON_TRACK
    message = None

    if labour_pct >= thresholds.critical_labour_pct:
        alert_type = AlertType.CRITICAL
        message = (
            f"CRITICAL: Labour is at {labour_pct:.1f}%, exceeding critical threshold "
            f"of {thresholds.critical_labour_pct:.1f}%"
        )
    elif labour_pct > thresholds.target_labour_pct:
        alert_type = AlertType.OVER_BUDGET
        message = (
            f"Over budget: Labour is at {labour_pct:.1f}%, exceeding target "
            f"of {thresholds.target_labour_pct:.1f}%"
        )
    elif labour_pct < thresholds.warning_labour_pct:
        alert_type = AlertType.WARNING
        message = (
            f"Warning: Labour is at {labour_pct:.1f}%, below minimum "
            f"of {thresholds.warning_labour_pct:.1f}%"
        )
    else:
        alert_type = AlertType.ON_TRACK
        message = f"On track: Labour is at {labour_pct:.1f}%"

    alert = BudgetAlert(
        alert_id=uuid.uuid4().hex[:12],
        venue_id=snapshot.venue_id,
        alert_type=alert_type,
        current_labour_pct=round(labour_pct, 2),
        target_labour_pct=thresholds.target_labour_pct,
        current_wage_cost=snapshot.total_wage_cost,
        projected_revenue=snapshot.projected_revenue,
        message=message,
        shift_date=snapshot.snapshot_date,
    )

    alerts.append(alert)
    return alerts


def project_hours_remaining(
    thresholds: BudgetThreshold,
    current_cost: float,
    projected_revenue: float,
    blended_rate: float,
) -> float:
    """Project how many more hours can be rostered before hitting target %.

    Args:
        thresholds: BudgetThreshold with target %
        current_cost: current wage cost so far
        projected_revenue: total forecast revenue
        blended_rate: average hourly rate for additional hours

    Returns:
        Hours remaining at blended rate before hitting target %, or 0 if already over.
    """
    if projected_revenue <= 0 or blended_rate <= 0:
        return 0.0

    target_wage_cost = projected_revenue * (thresholds.target_labour_pct / 100.0)
    remaining_budget = target_wage_cost - current_cost

    if remaining_budget <= 0:
        return 0.0

    hours_remaining = remaining_budget / blended_rate
    return max(0.0, hours_remaining)


def build_budget_snapshot(
    venue_id: str,
    shifts: List[Dict[str, Any]],
    rates_map: Dict[str, float],
    projected_revenue: float,
    snapshot_date: str,
    thresholds: Optional[BudgetThreshold] = None,
) -> BudgetSnapshot:
    """Build a complete budget snapshot including shifts and alerts.

    Args:
        venue_id: venue identifier
        shifts: list of shift dicts with employee_id, shift_start, shift_end, etc.
        rates_map: dict mapping employee_id to hourly_rate
        projected_revenue: forecast revenue for the period
        snapshot_date: ISO date string YYYY-MM-DD
        thresholds: BudgetThreshold to use; uses DEFAULT if None

    Returns:
        BudgetSnapshot with full cost analysis and alerts.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    # Calculate individual shift costs
    shift_costs = []
    total_wage_cost = 0.0
    total_hours = 0.0

    for shift in shifts:
        employee_id = shift.get("employee_id", "")
        rate = rates_map.get(employee_id, 25.0)
        projection = calculate_shift_cost(shift, rate)
        shift_costs.append(projection)
        total_wage_cost += projection.total_cost

        hours = _shift_duration_hours(
            shift.get("shift_start", "00:00"),
            shift.get("shift_end", "00:00"),
        )
        total_hours += hours

    # Calculate labour percentage
    labour_pct = (total_wage_cost / projected_revenue * 100) if projected_revenue > 0 else 0.0

    # Calculate average hourly cost
    avg_hourly_cost = (total_wage_cost / total_hours) if total_hours > 0 else 0.0

    # Generate alerts
    snapshot = BudgetSnapshot(
        venue_id=venue_id,
        snapshot_date=snapshot_date,
        total_wage_cost=round(total_wage_cost, 2),
        projected_revenue=projected_revenue,
        labour_pct=round(labour_pct, 2),
        headcount=len(shifts),
        avg_hourly_cost=round(avg_hourly_cost, 2),
        shift_costs=shift_costs,
    )

    # Check alerts
    alerts = check_budget_alerts(snapshot, thresholds)
    snapshot.alerts = alerts

    # Project hours remaining
    snapshot.hours_remaining_in_budget = round(
        project_hours_remaining(
            thresholds, total_wage_cost, projected_revenue, avg_hourly_cost
        ),
        2,
    )

    return snapshot


# ---------------------------------------------------------------------------
# Store: Threshold Management
# ---------------------------------------------------------------------------


class BudgetThresholdStore:
    """Thread-safe registry of budget thresholds per venue."""

    def __init__(self) -> None:
        self._thresholds: Dict[str, BudgetThreshold] = {}
        self._lock = threading.Lock()
        self._load_from_db()

    def _load_from_db(self) -> None:
        """Load thresholds from SQLite on init."""
        if not _p.is_persistence_enabled():
            return
        try:
            conn = _p.connection()
            cursor = conn.execute(
                "SELECT venue_id, target_labour_pct, warning_labour_pct, "
                "critical_labour_pct, max_wage_cost_per_hour, created_at, updated_at "
                "FROM budget_thresholds"
            )
            for row in cursor.fetchall():
                threshold = BudgetThreshold(
                    venue_id=row["venue_id"],
                    target_labour_pct=row["target_labour_pct"],
                    warning_labour_pct=row["warning_labour_pct"],
                    critical_labour_pct=row["critical_labour_pct"],
                    max_wage_cost_per_hour=row["max_wage_cost_per_hour"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )
                self._thresholds[row["venue_id"]] = threshold
        except Exception as e:
            logger.warning("Failed to load thresholds from DB: %s", e)

    def get(self, venue_id: str) -> BudgetThreshold:
        """Get thresholds for a venue, or DEFAULT."""
        with self._lock:
            return self._thresholds.get(venue_id, DEFAULT_THRESHOLDS)

    def set(self, threshold: BudgetThreshold) -> BudgetThreshold:
        """Create or update venue thresholds."""
        threshold.updated_at = datetime.now(timezone.utc)
        with self._lock:
            self._thresholds[threshold.venue_id] = threshold

        # Persist
        if _p.is_persistence_enabled():
            self._persist(threshold)

        return threshold

    def _persist(self, threshold: BudgetThreshold) -> None:
        """Persist threshold to SQLite."""
        try:
            conn = _p.connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO budget_thresholds
                (venue_id, target_labour_pct, warning_labour_pct, critical_labour_pct,
                 max_wage_cost_per_hour, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    threshold.venue_id,
                    threshold.target_labour_pct,
                    threshold.warning_labour_pct,
                    threshold.critical_labour_pct,
                    threshold.max_wage_cost_per_hour,
                    threshold.created_at.isoformat(),
                    threshold.updated_at.isoformat(),
                ),
            )
            conn.commit()
        except Exception as e:
            logger.warning("Failed to persist threshold: %s", e)


# ---------------------------------------------------------------------------
# Store: Alert History
# ---------------------------------------------------------------------------


class BudgetAlertStore:
    """Thread-safe registry of budget alerts."""

    def __init__(self) -> None:
        self._alerts: Dict[str, BudgetAlert] = {}  # keyed by alert_id
        self._lock = threading.Lock()
        self._load_from_db()

    def _load_from_db(self) -> None:
        """Load alerts from SQLite on init."""
        if not _p.is_persistence_enabled():
            return
        try:
            conn = _p.connection()
            # Only load the most recent 100 alerts on startup to avoid memory issues
            cursor = conn.execute(
                "SELECT alert_id, venue_id, alert_type, current_labour_pct, "
                "target_labour_pct, current_wage_cost, projected_revenue, "
                "message, shift_date, created_at FROM budget_alerts ORDER BY created_at DESC LIMIT 100"
            )
            for row in cursor.fetchall():
                alert = BudgetAlert(
                    alert_id=row["alert_id"],
                    venue_id=row["venue_id"],
                    alert_type=AlertType(row["alert_type"]),
                    current_labour_pct=row["current_labour_pct"],
                    target_labour_pct=row["target_labour_pct"],
                    current_wage_cost=row["current_wage_cost"],
                    projected_revenue=row["projected_revenue"],
                    message=row["message"],
                    shift_date=row["shift_date"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
                self._alerts[row["alert_id"]] = alert
        except Exception as e:
            logger.warning("Failed to load alerts from DB: %s", e)

    def record(self, alert: BudgetAlert) -> BudgetAlert:
        """Record a new alert."""
        with self._lock:
            self._alerts[alert.alert_id] = alert

        # Persist
        if _p.is_persistence_enabled():
            self._persist(alert)

        return alert

    def get_by_venue(
        self, venue_id: str, limit: int = 50, alert_type: Optional[str] = None
    ) -> List[BudgetAlert]:
        """Query alerts for a venue, optionally filtered by type."""
        with self._lock:
            alerts = [a for a in self._alerts.values() if a.venue_id == venue_id]

        if alert_type:
            alerts = [a for a in alerts if a.alert_type.value == alert_type]

        # Return newest first
        alerts.sort(key=lambda a: a.created_at, reverse=True)
        return alerts[:limit]

    def get_by_date_range(
        self, venue_id: str, date_from: str, date_to: str, limit: int = 50
    ) -> List[BudgetAlert]:
        """Query alerts for a venue within a date range."""
        with self._lock:
            alerts = [
                a
                for a in self._alerts.values()
                if a.venue_id == venue_id
                and a.shift_date
                and date_from <= a.shift_date <= date_to
            ]

        alerts.sort(key=lambda a: a.created_at, reverse=True)
        return alerts[:limit]

    def _persist(self, alert: BudgetAlert) -> None:
        """Persist alert to SQLite."""
        try:
            conn = _p.connection()
            conn.execute(
                """
                INSERT INTO budget_alerts
                (alert_id, venue_id, alert_type, current_labour_pct,
                 target_labour_pct, current_wage_cost, projected_revenue,
                 message, shift_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert.alert_id,
                    alert.venue_id,
                    alert.alert_type.value,
                    alert.current_labour_pct,
                    alert.target_labour_pct,
                    alert.current_wage_cost,
                    alert.projected_revenue,
                    alert.message,
                    alert.shift_date,
                    alert.created_at.isoformat(),
                ),
            )
            conn.commit()
        except Exception as e:
            logger.warning("Failed to persist alert: %s", e)


# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

_threshold_store: Optional[BudgetThresholdStore] = None
_alert_store: Optional[BudgetAlertStore] = None
_stores_lock = threading.Lock()


def get_threshold_store() -> BudgetThresholdStore:
    """Get or create the global threshold store."""
    global _threshold_store
    if _threshold_store is None:
        with _stores_lock:
            if _threshold_store is None:
                _threshold_store = BudgetThresholdStore()
    return _threshold_store


def get_alert_store() -> BudgetAlertStore:
    """Get or create the global alert store."""
    global _alert_store
    if _alert_store is None:
        with _stores_lock:
            if _alert_store is None:
                _alert_store = BudgetAlertStore()
    return _alert_store


def _reset_for_tests() -> None:
    """Test helper to reset stores."""
    global _threshold_store, _alert_store
    _threshold_store = None
    _alert_store = None
