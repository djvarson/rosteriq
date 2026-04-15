"""Accountability Engine — decision logging and variance analysis.

Tracks who built/approved each roster, who cut/kept staff during a shift,
and the variance between forecasted and actual outcomes.

Pure-stdlib module. No FastAPI/Pydantic here; the router delegates to these helpers.

Data shapes:
- VarianceRecord: venue_id, shift_id, shift_date, forecast_revenue, actual_revenue,
  forecast_headcount_peak, actual_headcount_peak, forecast_staff_hours, actual_staff_hours,
  variance_revenue_pct, variance_staff_hours_pct, computed_at
- DecisionLog: decision_id, venue_id, shift_id, manager_id, manager_name, decision_type,
  taken_at, signals_available (dict), outcome_variance (dict), notes
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, date, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DecisionType(str, Enum):
    """Types of manager decisions that affect staffing and costs."""

    KEPT_STAFF_ON = "kept_staff_on"
    CUT_STAFF = "cut_staff"
    CALLED_IN_STAFF = "called_in_staff"
    IGNORED_ALERT = "ignored_alert"
    PUBLISHED_ROSTER = "published_roster"
    MODIFIED_ROSTER = "modified_roster"


# ---------------------------------------------------------------------------
# Data Models (pure dataclasses)
# ---------------------------------------------------------------------------


@dataclass
class VarianceRecord:
    """Variance between forecast and actual outcomes for a shift."""

    venue_id: str
    shift_id: str
    shift_date: date
    forecast_revenue: Optional[float] = None  # AUD, forecast at shift start
    actual_revenue: Optional[float] = None  # AUD, from POS
    forecast_headcount_peak: Optional[int] = None  # forecast peak staff
    actual_headcount_peak: Optional[int] = None  # actual peak from headcount clicker
    forecast_staff_hours: Optional[float] = None  # total forecast staff-hours
    actual_staff_hours: Optional[float] = None  # total actual staff-hours
    variance_revenue_pct: Optional[float] = None  # (actual - forecast) / forecast * 100
    variance_staff_hours_pct: Optional[float] = None  # (actual - forecast) / forecast * 100
    computed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for API responses."""
        d = asdict(self)
        # Convert date to ISO string
        if isinstance(d.get("shift_date"), date):
            d["shift_date"] = d["shift_date"].isoformat()
        return d


@dataclass
class DecisionLog:
    """A manager decision made during or before a shift."""

    decision_id: str
    venue_id: str
    shift_id: str
    manager_id: str
    manager_name: str
    decision_type: DecisionType
    taken_at: str  # ISO 8601 timestamp
    signals_available: Dict[str, Any]  # snapshot of forecasts/alerts at decision time
    outcome_variance: Dict[str, Any]  # filled once shift ends
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for API responses."""
        return {
            "decision_id": self.decision_id,
            "venue_id": self.venue_id,
            "shift_id": self.shift_id,
            "manager_id": self.manager_id,
            "manager_name": self.manager_name,
            "decision_type": self.decision_type.value,
            "taken_at": self.taken_at,
            "signals_available": self.signals_available,
            "outcome_variance": self.outcome_variance,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------

_DECISIONS_STORE: Dict[str, List[DecisionLog]] = {}  # venue_id -> list of decisions
_VARIANCE_STORE: Dict[str, List[VarianceRecord]] = {}  # venue_id -> list of variances

MAX_DECISIONS = 500  # per venue
MAX_VARIANCE = 200  # per venue


def _now_iso() -> str:
    """Return current time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def clear() -> None:
    """Wipe the entire store. Used by tests."""
    _DECISIONS_STORE.clear()
    _VARIANCE_STORE.clear()


def store() -> Dict[str, Any]:
    """Return the raw store dicts. For tests and diagnostics only."""
    return {
        "decisions": _DECISIONS_STORE,
        "variance": _VARIANCE_STORE,
    }


# ---------------------------------------------------------------------------
# Decision Recording
# ---------------------------------------------------------------------------


def record_decision(
    venue_id: str,
    shift_id: str,
    manager_id: str,
    manager_name: str,
    decision_type: DecisionType,
    signals_available: Dict[str, Any],
    notes: Optional[str] = None,
) -> DecisionLog:
    """Record a manager decision and its context.

    Args:
        venue_id: Venue ID
        shift_id: Shift ID (e.g., "shift_2026-04-15_0900")
        manager_id: ID of manager making the decision
        manager_name: Human-readable name of manager
        decision_type: Type of decision (enum)
        signals_available: Dict snapshot of forecasts/alerts/headcount at decision time
        notes: Optional human-readable notes about the decision

    Returns:
        DecisionLog record

    """
    if venue_id not in _DECISIONS_STORE:
        _DECISIONS_STORE[venue_id] = []

    decision_id = f"dec_{uuid.uuid4().hex[:12]}"
    decision = DecisionLog(
        decision_id=decision_id,
        venue_id=venue_id,
        shift_id=shift_id,
        manager_id=manager_id,
        manager_name=manager_name,
        decision_type=decision_type,
        taken_at=_now_iso(),
        signals_available=signals_available,
        outcome_variance={},
        notes=notes,
    )

    hist = _DECISIONS_STORE[venue_id]
    hist.append(decision)

    # Truncate to max
    if len(hist) > MAX_DECISIONS:
        del hist[: len(hist) - MAX_DECISIONS]

    return decision


def list_decisions(
    venue_id: str,
    manager_id: Optional[str] = None,
    since: Optional[datetime] = None,
    limit: int = 50,
) -> List[DecisionLog]:
    """List decisions for a venue, optionally filtered.

    Args:
        venue_id: Venue ID
        manager_id: Optional filter by manager
        since: Optional filter by datetime
        limit: Max results

    Returns:
        List of DecisionLog records, newest first

    """
    if venue_id not in _DECISIONS_STORE:
        return []

    hist = _DECISIONS_STORE[venue_id]
    results = list(hist)

    # Filter by manager if provided
    if manager_id:
        results = [d for d in results if d.manager_id == manager_id]

    # Filter by since if provided
    if since:
        since_iso = since.isoformat()
        results = [d for d in results if d.taken_at >= since_iso]

    # Newest first
    results.sort(key=lambda d: d.taken_at, reverse=True)

    return results[:limit]


def update_decision_variance(
    venue_id: str,
    decision_id: str,
    outcome_variance: Dict[str, Any],
) -> Optional[DecisionLog]:
    """Update a decision with its outcome variance (once shift ends).

    Args:
        venue_id: Venue ID
        decision_id: Decision ID
        outcome_variance: Dict with actual vs forecast for the decision

    Returns:
        Updated DecisionLog, or None if not found

    """
    if venue_id not in _DECISIONS_STORE:
        return None

    for decision in _DECISIONS_STORE[venue_id]:
        if decision.decision_id == decision_id:
            decision.outcome_variance = outcome_variance
            return decision

    return None


# ---------------------------------------------------------------------------
# Variance Analysis
# ---------------------------------------------------------------------------


def compute_variance(
    venue_id: str,
    shift_id: str,
    shift_date: date,
    forecast_revenue: Optional[float] = None,
    actual_revenue: Optional[float] = None,
    forecast_headcount_peak: Optional[int] = None,
    actual_headcount_peak: Optional[int] = None,
    forecast_staff_hours: Optional[float] = None,
    actual_staff_hours: Optional[float] = None,
) -> VarianceRecord:
    """Compute and store variance record for a shift.

    Variance percentages are computed as: (actual - forecast) / forecast * 100
    - Negative = better than forecast (lower cost, fewer staff needed)
    - Positive = worse than forecast (higher cost, more staff needed)

    Args:
        venue_id: Venue ID
        shift_id: Shift ID
        shift_date: Date of shift
        forecast_revenue: Forecasted revenue (AUD)
        actual_revenue: Actual revenue from POS (AUD)
        forecast_headcount_peak: Forecasted peak headcount
        actual_headcount_peak: Actual peak headcount
        forecast_staff_hours: Forecasted total staff-hours
        actual_staff_hours: Actual total staff-hours

    Returns:
        VarianceRecord with computed percentages

    """
    if venue_id not in _VARIANCE_STORE:
        _VARIANCE_STORE[venue_id] = []

    # Compute variance percentages (safe division)
    variance_revenue_pct = None
    if forecast_revenue and actual_revenue is not None and forecast_revenue != 0:
        variance_revenue_pct = round(
            ((actual_revenue - forecast_revenue) / forecast_revenue) * 100, 2
        )

    variance_staff_hours_pct = None
    if forecast_staff_hours and actual_staff_hours is not None and forecast_staff_hours != 0:
        variance_staff_hours_pct = round(
            ((actual_staff_hours - forecast_staff_hours) / forecast_staff_hours) * 100, 2
        )

    record = VarianceRecord(
        venue_id=venue_id,
        shift_id=shift_id,
        shift_date=shift_date,
        forecast_revenue=forecast_revenue,
        actual_revenue=actual_revenue,
        forecast_headcount_peak=forecast_headcount_peak,
        actual_headcount_peak=actual_headcount_peak,
        forecast_staff_hours=forecast_staff_hours,
        actual_staff_hours=actual_staff_hours,
        variance_revenue_pct=variance_revenue_pct,
        variance_staff_hours_pct=variance_staff_hours_pct,
        computed_at=_now_iso(),
    )

    hist = _VARIANCE_STORE[venue_id]
    # Check if we already have a variance record for this shift
    for i, existing in enumerate(hist):
        if existing.shift_id == shift_id:
            hist[i] = record
            return record

    hist.append(record)

    # Truncate to max
    if len(hist) > MAX_VARIANCE:
        del hist[: len(hist) - MAX_VARIANCE]

    return record


def list_variance(
    venue_id: str,
    shift_id: Optional[str] = None,
    since: Optional[date] = None,
    limit: int = 50,
) -> List[VarianceRecord]:
    """List variance records for a venue, optionally filtered.

    Args:
        venue_id: Venue ID
        shift_id: Optional filter by shift
        since: Optional filter by date
        limit: Max results

    Returns:
        List of VarianceRecord records, newest first

    """
    if venue_id not in _VARIANCE_STORE:
        return []

    hist = _VARIANCE_STORE[venue_id]
    results = list(hist)

    # Filter by shift if provided
    if shift_id:
        results = [v for v in results if v.shift_id == shift_id]

    # Filter by since if provided
    if since:
        results = [v for v in results if v.shift_date >= since]

    # Newest first
    results.sort(key=lambda v: v.computed_at, reverse=True)

    return results[:limit]


# ---------------------------------------------------------------------------
# Manager Scoring
# ---------------------------------------------------------------------------


@dataclass
class ManagerScore:
    """Manager accountability score."""

    manager_id: str
    manager_name: str
    venue_id: str
    decisions_total: int
    alerts_actioned_pct: float  # % of alerts they actioned vs total pending
    avg_variance_revenue: float  # average variance % on revenue
    avg_variance_staff_hours: float  # average variance % on staff hours
    decisions_against_signals: int  # times they made decision counter to data

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for API responses."""
        return asdict(self)


def score_manager(
    venue_id: str,
    manager_id: str,
    since: Optional[datetime] = None,
) -> ManagerScore:
    """Compute accountability score for a manager.

    Args:
        venue_id: Venue ID
        manager_id: Manager ID
        since: Optional filter by datetime

    Returns:
        ManagerScore with metrics

    """
    # Get decisions for this manager
    decisions = list_decisions(venue_id, manager_id=manager_id, since=since)

    if not decisions:
        return ManagerScore(
            manager_id=manager_id,
            manager_name="Unknown",
            venue_id=venue_id,
            decisions_total=0,
            alerts_actioned_pct=0.0,
            avg_variance_revenue=0.0,
            avg_variance_staff_hours=0.0,
            decisions_against_signals=0,
        )

    manager_name = decisions[0].manager_name if decisions else "Unknown"

    # Count decisions against signals
    decisions_against_signals = 0
    for decision in decisions:
        # If decision_type is IGNORED_ALERT, it's against signals
        if decision.decision_type == DecisionType.IGNORED_ALERT:
            decisions_against_signals += 1
        # If signals_available suggested cut but they KEPT_STAFF_ON, that's against signals
        elif decision.decision_type == DecisionType.KEPT_STAFF_ON:
            if decision.signals_available.get("suggested_action") == "cut":
                decisions_against_signals += 1

    # Compute alerts_actioned_pct — count actioned vs total decisions
    actioned = len(
        [
            d
            for d in decisions
            if d.decision_type
            in (
                DecisionType.CUT_STAFF,
                DecisionType.CALLED_IN_STAFF,
            )
        ]
    )
    alerts_actioned_pct = round((actioned / len(decisions) * 100), 2) if decisions else 0.0

    # Compute average variance from outcome_variance
    revenue_variances = []
    hours_variances = []
    for decision in decisions:
        if decision.outcome_variance:
            if "variance_revenue_pct" in decision.outcome_variance:
                revenue_variances.append(decision.outcome_variance["variance_revenue_pct"])
            if "variance_staff_hours_pct" in decision.outcome_variance:
                hours_variances.append(
                    decision.outcome_variance["variance_staff_hours_pct"]
                )

    avg_variance_revenue = (
        round(sum(revenue_variances) / len(revenue_variances), 2)
        if revenue_variances
        else 0.0
    )
    avg_variance_staff_hours = (
        round(sum(hours_variances) / len(hours_variances), 2)
        if hours_variances
        else 0.0
    )

    return ManagerScore(
        manager_id=manager_id,
        manager_name=manager_name,
        venue_id=venue_id,
        decisions_total=len(decisions),
        alerts_actioned_pct=alerts_actioned_pct,
        avg_variance_revenue=avg_variance_revenue,
        avg_variance_staff_hours=avg_variance_staff_hours,
        decisions_against_signals=decisions_against_signals,
    )


def venue_leaderboard(
    venue_ids: List[str],
    since: Optional[datetime] = None,
    limit: int = 100,
) -> List[ManagerScore]:
    """Compute leaderboard of managers across venues.

    Sorted by alerts_actioned_pct descending (best actors first).

    Args:
        venue_ids: List of venue IDs to include
        since: Optional filter by datetime
        limit: Max managers to return

    Returns:
        List of ManagerScore sorted by alerts_actioned_pct desc

    """
    managers: Dict[str, ManagerScore] = {}

    for venue_id in venue_ids:
        if venue_id not in _DECISIONS_STORE:
            continue

        # Extract unique manager_ids from decisions
        unique_managers = set()
        for decision in _DECISIONS_STORE[venue_id]:
            unique_managers.add(decision.manager_id)

        # Score each manager
        for manager_id in unique_managers:
            score = score_manager(venue_id, manager_id, since=since)
            # Key is manager_id globally (might collide across venues, but that's ok)
            key = f"{venue_id}:{manager_id}"
            managers[key] = score

    # Convert to list and sort by alerts_actioned_pct desc
    results = list(managers.values())
    results.sort(key=lambda m: m.alerts_actioned_pct, reverse=True)

    return results[:limit]


# ---------------------------------------------------------------------------
# Demo Data Seeding
# ---------------------------------------------------------------------------


def _seed_demo_data():
    """Seed the store with realistic demo data if empty.

    Called on first router load to ensure leaderboard isn't empty.
    """
    # Only seed if store is completely empty
    if _DECISIONS_STORE or _VARIANCE_STORE:
        return

    now = datetime.now(timezone.utc)
    two_days_ago = now - timedelta(days=2)
    one_week_ago = now - timedelta(days=7)

    # Create demo managers across venues
    venues = ["mojos", "earls", "burning-palms"]
    managers = [
        ("mgr_alice", "Alice Smith"),
        ("mgr_bob", "Bob Jones"),
        ("mgr_charlie", "Charlie Brown"),
    ]

    for venue_id in venues:
        # Alice: excellent actor (90% alerts actioned)
        for i in range(10):
            decision = record_decision(
                venue_id=venue_id,
                shift_id=f"shift_{venue_id}_{i}",
                manager_id="mgr_alice",
                manager_name="Alice Smith",
                decision_type=DecisionType.CUT_STAFF if i % 10 < 9 else DecisionType.KEPT_STAFF_ON,
                signals_available={"forecast_revenue": 5000 + i * 100, "suggested_action": "cut"},
                notes=f"Demo decision {i}" if i % 3 == 0 else None,
            )
            # Add outcome variance
            update_decision_variance(
                venue_id=venue_id,
                decision_id=decision.decision_id,
                outcome_variance={
                    "variance_revenue_pct": -5.0 - (i % 5),
                    "variance_staff_hours_pct": -3.0 - (i % 4),
                },
            )

        # Bob: moderate actor (60% alerts actioned)
        for i in range(10):
            decision = record_decision(
                venue_id=venue_id,
                shift_id=f"shift_bob_{venue_id}_{i}",
                manager_id="mgr_bob",
                manager_name="Bob Jones",
                decision_type=DecisionType.CUT_STAFF if i % 10 < 6 else DecisionType.IGNORED_ALERT,
                signals_available={"forecast_revenue": 4500 + i * 80, "suggested_action": "cut"},
            )
            update_decision_variance(
                venue_id=venue_id,
                decision_id=decision.decision_id,
                outcome_variance={
                    "variance_revenue_pct": -2.0 - (i % 3),
                    "variance_staff_hours_pct": -1.5 - (i % 2),
                },
            )

        # Charlie: poor actor (40% alerts actioned)
        for i in range(8):
            decision = record_decision(
                venue_id=venue_id,
                shift_id=f"shift_charlie_{venue_id}_{i}",
                manager_id="mgr_charlie",
                manager_name="Charlie Brown",
                decision_type=DecisionType.CUT_STAFF if i % 10 < 4 else DecisionType.IGNORED_ALERT,
                signals_available={"forecast_revenue": 5200 + i * 50, "suggested_action": "cut"},
            )
            update_decision_variance(
                venue_id=venue_id,
                decision_id=decision.decision_id,
                outcome_variance={
                    "variance_revenue_pct": 2.0 + (i % 4),
                    "variance_staff_hours_pct": 1.5 + (i % 3),
                },
            )

    # Add some variance records for demo shifts
    for venue_id in venues:
        for day_offset in range(7):
            shift_date = (now - timedelta(days=day_offset)).date()
            compute_variance(
                venue_id=venue_id,
                shift_id=f"shift_{venue_id}_{day_offset}",
                shift_date=shift_date,
                forecast_revenue=5000.0,
                actual_revenue=4850.0 + (day_offset * 20),
                forecast_headcount_peak=10,
                actual_headcount_peak=9 + (day_offset % 2),
                forecast_staff_hours=80.0,
                actual_staff_hours=75.0 + (day_offset % 3),
            )
