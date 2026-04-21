"""KPI Dashboard Data API for RosterIQ (Round 53).

Aggregated KPI metrics for venue dashboards with trend analysis, alerts, and targets.

Tracks performance indicators:
- labour_cost_pct: Labour costs as % of revenue
- revenue_per_labour_hour: Revenue generated per labour hour
- avg_hourly_cost: Average cost per labour hour
- covers_per_staff_hour: Covers served per staff hour
- roster_fill_rate: % of scheduled shifts filled
- no_show_rate: % of assigned shifts with no-shows
- break_compliance_rate: % of shifts with compliant breaks
- avg_shift_length: Average duration of shifts

Alert thresholds:
- labour_cost_pct: 25-35% good, >40% critical
- no_show_rate: <5% good, >10% critical
- break_compliance_rate: >95% good, <85% critical
- roster_fill_rate: >90% good, <80% critical
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.kpi_dashboard")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class KPIPeriod(str, Enum):
    """Time period for KPI snapshot."""
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


@dataclass
class KPISnapshot:
    """KPI snapshot for a venue at a point in time."""
    id: str
    venue_id: str
    date: str  # ISO date string YYYY-MM-DD
    period: KPIPeriod
    metrics: Dict[str, Any]  # labour_cost_pct, revenue_per_labour_hour, etc.
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "date": self.date,
            "period": self.period.value,
            "metrics": self.metrics,
            "created_at": self.created_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Core KPI calculation functions
# ---------------------------------------------------------------------------


def calculate_labour_cost_pct(labour_cost: float, revenue: float) -> float:
    """Calculate labour cost as percentage of revenue.

    Args:
        labour_cost: Total labour cost for period
        revenue: Total revenue for period

    Returns:
        Labour cost percentage (0-100)
    """
    if revenue <= 0:
        return 0.0
    return round((labour_cost / revenue) * 100, 2)


def calculate_revenue_per_labour_hour(revenue: float, hours_worked: float) -> float:
    """Calculate revenue per labour hour.

    Args:
        revenue: Total revenue
        hours_worked: Total labour hours

    Returns:
        Revenue per hour
    """
    if hours_worked <= 0:
        return 0.0
    return round(revenue / hours_worked, 2)


def calculate_avg_hourly_cost(labour_cost: float, hours_worked: float) -> float:
    """Calculate average hourly labour cost.

    Args:
        labour_cost: Total labour cost
        hours_worked: Total hours worked

    Returns:
        Average cost per hour
    """
    if hours_worked <= 0:
        return 0.0
    return round(labour_cost / hours_worked, 2)


def calculate_covers_per_staff_hour(covers: int, hours_worked: float) -> float:
    """Calculate covers served per staff hour.

    Args:
        covers: Total covers/customers served
        hours_worked: Total labour hours

    Returns:
        Covers per labour hour
    """
    if hours_worked <= 0:
        return 0.0
    return round(covers / hours_worked, 2)


def calculate_roster_fill_rate(shifts_filled: int, shifts_scheduled: int) -> float:
    """Calculate roster fill rate (% of scheduled shifts filled).

    Args:
        shifts_filled: Number of shifts filled
        shifts_scheduled: Total shifts scheduled

    Returns:
        Fill rate percentage (0-100)
    """
    if shifts_scheduled <= 0:
        return 0.0
    return round((shifts_filled / shifts_scheduled) * 100, 2)


def calculate_no_show_rate(no_shows: int, shifts_scheduled: int) -> float:
    """Calculate no-show rate.

    Args:
        no_shows: Number of no-shows
        shifts_scheduled: Total shifts scheduled

    Returns:
        No-show percentage (0-100)
    """
    if shifts_scheduled <= 0:
        return 0.0
    return round((no_shows / shifts_scheduled) * 100, 2)


def calculate_break_compliance_rate(total_breaks: int, break_violations: int) -> float:
    """Calculate break compliance rate.

    Args:
        total_breaks: Total breaks scheduled
        break_violations: Number of violations

    Returns:
        Compliance percentage (0-100)
    """
    if total_breaks <= 0:
        return 100.0
    compliant = total_breaks - break_violations
    return round((compliant / total_breaks) * 100, 2)


def calculate_avg_shift_length(hours_worked: float, shifts_filled: int) -> float:
    """Calculate average shift length.

    Args:
        hours_worked: Total hours worked
        shifts_filled: Total shifts filled

    Returns:
        Average hours per shift
    """
    if shifts_filled <= 0:
        return 0.0
    return round(hours_worked / shifts_filled, 2)


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


_KPI_SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS kpi_snapshots (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date TEXT NOT NULL,
    period TEXT NOT NULL,
    metrics TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_kpi_venue ON kpi_snapshots(venue_id);
CREATE INDEX IF NOT EXISTS ix_kpi_date ON kpi_snapshots(date);
CREATE INDEX IF NOT EXISTS ix_kpi_period ON kpi_snapshots(period);
CREATE INDEX IF NOT EXISTS ix_kpi_venue_date ON kpi_snapshots(venue_id, date);

CREATE TABLE IF NOT EXISTS kpi_targets (
    venue_id TEXT PRIMARY KEY,
    targets TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kpi_alerts (
    alert_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    actual_value REAL NOT NULL,
    threshold_min REAL,
    threshold_max REAL,
    severity TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_kpi_alert_venue ON kpi_alerts(venue_id);
CREATE INDEX IF NOT EXISTS ix_kpi_alert_date ON kpi_alerts(date);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("kpi_dashboard", _KPI_SNAPSHOTS_SCHEMA)
            def _rehydrate_on_init():
                store = get_kpi_dashboard_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# KPI Dashboard Store
# ---------------------------------------------------------------------------


class KPIDashboardStore:
    """Thread-safe in-memory store for KPI snapshots with persistence."""

    def __init__(self):
        self._snapshots: Dict[str, KPISnapshot] = {}
        self._targets: Dict[str, Dict[str, Any]] = {}
        self._alerts: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def _persist_snapshot(self, snapshot: KPISnapshot) -> None:
        """Persist a KPI snapshot to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        row = {
            "id": snapshot.id,
            "venue_id": snapshot.venue_id,
            "date": snapshot.date,
            "period": snapshot.period.value,
            "metrics": json.dumps(snapshot.metrics),
            "created_at": snapshot.created_at.isoformat(),
        }
        try:
            _p.upsert("kpi_snapshots", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist KPI snapshot %s: %s", snapshot.id, e)

    def _persist_targets(self, venue_id: str, targets: Dict[str, Any]) -> None:
        """Persist KPI targets for a venue."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        row = {
            "venue_id": venue_id,
            "targets": json.dumps(targets),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            _p.upsert("kpi_targets", row, pk="venue_id")
        except Exception as e:
            logger.warning("Failed to persist KPI targets for %s: %s", venue_id, e)

    def _persist_alert(self, alert: Dict[str, Any]) -> None:
        """Persist a KPI alert."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            _p.upsert("kpi_alerts", alert, pk="alert_id")
        except Exception as e:
            logger.warning("Failed to persist KPI alert: %s", e)

    def _rehydrate(self) -> None:
        """Load all KPI data from SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        try:
            # Load snapshots
            rows = _p.fetchall("SELECT * FROM kpi_snapshots")
            for row in rows:
                d = dict(row)
                metrics = json.loads(d.get("metrics", "{}"))
                snapshot = KPISnapshot(
                    id=d["id"],
                    venue_id=d["venue_id"],
                    date=d["date"],
                    period=KPIPeriod(d["period"]),
                    metrics=metrics,
                    created_at=datetime.fromisoformat(d["created_at"]),
                )
                self._snapshots[snapshot.id] = snapshot

            # Load targets
            rows = _p.fetchall("SELECT * FROM kpi_targets")
            for row in rows:
                d = dict(row)
                targets = json.loads(d.get("targets", "{}"))
                self._targets[d["venue_id"]] = targets

            logger.info("Rehydrated %d KPI snapshots and %d target sets",
                       len(self._snapshots), len(self._targets))
        except Exception as e:
            logger.warning("Failed to rehydrate KPI data: %s", e)

    def calculate_daily_kpis(
        self,
        venue_id: str,
        date_str: str,
        revenue: float,
        labour_cost: float,
        hours_worked: float,
        covers: int,
        shifts_scheduled: int,
        shifts_filled: int,
        no_shows: int,
        break_violations: int,
        total_breaks: int,
    ) -> KPISnapshot:
        """Calculate and store daily KPI snapshot.

        Args:
            venue_id: Venue identifier
            date_str: ISO date string YYYY-MM-DD
            revenue: Daily revenue
            labour_cost: Daily labour cost
            hours_worked: Total labour hours
            covers: Number of covers served
            shifts_scheduled: Shifts scheduled
            shifts_filled: Shifts filled
            no_shows: No-show count
            break_violations: Break violation count
            total_breaks: Total breaks scheduled

        Returns:
            KPISnapshot with calculated metrics
        """
        metrics = {
            "labour_cost_pct": calculate_labour_cost_pct(labour_cost, revenue),
            "revenue_per_labour_hour": calculate_revenue_per_labour_hour(revenue, hours_worked),
            "avg_hourly_cost": calculate_avg_hourly_cost(labour_cost, hours_worked),
            "covers_per_staff_hour": calculate_covers_per_staff_hour(covers, hours_worked),
            "roster_fill_rate": calculate_roster_fill_rate(shifts_filled, shifts_scheduled),
            "no_show_rate": calculate_no_show_rate(no_shows, shifts_scheduled),
            "break_compliance_rate": calculate_break_compliance_rate(total_breaks, break_violations),
            "avg_shift_length": calculate_avg_shift_length(hours_worked, shifts_filled),
        }

        snapshot = KPISnapshot(
            id=f"kpi_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            date=date_str,
            period=KPIPeriod.DAILY,
            metrics=metrics,
        )

        with self._lock:
            self._snapshots[snapshot.id] = snapshot

        self._persist_snapshot(snapshot)

        # Generate alerts
        self._check_and_create_alerts(venue_id, date_str, metrics)

        return snapshot

    def calculate_weekly_kpis(self, venue_id: str, week_start: str) -> Optional[KPISnapshot]:
        """Aggregate daily KPIs into weekly snapshot.

        Args:
            venue_id: Venue identifier
            week_start: ISO date of week start (Monday)

        Returns:
            KPISnapshot for the week or None if no data
        """
        with self._lock:
            # Find all daily snapshots for this week
            week_snapshots = []
            try:
                week_date = date.fromisoformat(week_start)
                week_end = week_date + timedelta(days=6)
                for snapshot in self._snapshots.values():
                    if (snapshot.venue_id == venue_id and
                        snapshot.period == KPIPeriod.DAILY):
                        snap_date = date.fromisoformat(snapshot.date)
                        if week_date <= snap_date <= week_end:
                            week_snapshots.append(snapshot)
            except (ValueError, TypeError):
                return None

        if not week_snapshots:
            return None

        # Aggregate metrics
        aggregated = self._aggregate_metrics(week_snapshots)

        snapshot = KPISnapshot(
            id=f"kpi_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            date=week_start,
            period=KPIPeriod.WEEKLY,
            metrics=aggregated,
        )

        with self._lock:
            self._snapshots[snapshot.id] = snapshot

        self._persist_snapshot(snapshot)
        return snapshot

    def calculate_monthly_kpis(self, venue_id: str, year: int, month: int) -> Optional[KPISnapshot]:
        """Aggregate daily KPIs into monthly snapshot.

        Args:
            venue_id: Venue identifier
            year: Year
            month: Month (1-12)

        Returns:
            KPISnapshot for the month or None if no data
        """
        with self._lock:
            # Find all daily snapshots for this month
            month_snapshots = []
            for snapshot in self._snapshots.values():
                if (snapshot.venue_id == venue_id and
                    snapshot.period == KPIPeriod.DAILY):
                    try:
                        snap_date = date.fromisoformat(snapshot.date)
                        if snap_date.year == year and snap_date.month == month:
                            month_snapshots.append(snapshot)
                    except (ValueError, TypeError):
                        pass

        if not month_snapshots:
            return None

        # Aggregate metrics
        aggregated = self._aggregate_metrics(month_snapshots)

        # Use first day of month as snapshot date
        month_start = date(year, month, 1).isoformat()

        snapshot = KPISnapshot(
            id=f"kpi_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            date=month_start,
            period=KPIPeriod.MONTHLY,
            metrics=aggregated,
        )

        with self._lock:
            self._snapshots[snapshot.id] = snapshot

        self._persist_snapshot(snapshot)
        return snapshot

    @staticmethod
    def _aggregate_metrics(snapshots: List[KPISnapshot]) -> Dict[str, Any]:
        """Average metrics across multiple snapshots."""
        if not snapshots:
            return {}

        # Extract all metric keys from first snapshot
        metric_keys = list(snapshots[0].metrics.keys())
        aggregated = {}

        for key in metric_keys:
            values = [s.metrics.get(key, 0) for s in snapshots if key in s.metrics]
            if values:
                aggregated[key] = round(sum(values) / len(values), 2)

        return aggregated

    def _check_and_create_alerts(
        self,
        venue_id: str,
        date_str: str,
        metrics: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Check metrics against thresholds and create alerts."""
        alerts = []
        alert_config = {
            "labour_cost_pct": {"min": 25, "max": 35, "critical_max": 40},
            "no_show_rate": {"min": 0, "max": 5, "critical_max": 10},
            "break_compliance_rate": {"min": 95, "critical_min": 85},
            "roster_fill_rate": {"min": 90, "critical_min": 80},
        }

        for metric_name, config in alert_config.items():
            if metric_name not in metrics:
                continue

            value = metrics[metric_name]
            alert = None

            if "critical_max" in config and value > config["critical_max"]:
                alert = {
                    "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
                    "venue_id": venue_id,
                    "date": date_str,
                    "metric_name": metric_name,
                    "actual_value": value,
                    "threshold_min": config.get("min"),
                    "threshold_max": config.get("max"),
                    "severity": "CRITICAL",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            elif "critical_min" in config and value < config["critical_min"]:
                alert = {
                    "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
                    "venue_id": venue_id,
                    "date": date_str,
                    "metric_name": metric_name,
                    "actual_value": value,
                    "threshold_min": config.get("critical_min"),
                    "threshold_max": None,
                    "severity": "CRITICAL",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            elif "max" in config and value > config["max"]:
                alert = {
                    "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
                    "venue_id": venue_id,
                    "date": date_str,
                    "metric_name": metric_name,
                    "actual_value": value,
                    "threshold_min": config.get("min"),
                    "threshold_max": config.get("max"),
                    "severity": "WARNING",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            elif "min" in config and value < config["min"]:
                alert = {
                    "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
                    "venue_id": venue_id,
                    "date": date_str,
                    "metric_name": metric_name,
                    "actual_value": value,
                    "threshold_min": config.get("min"),
                    "threshold_max": None,
                    "severity": "WARNING",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }

            if alert:
                alerts.append(alert)
                self._persist_alert(alert)

        return alerts

    def get_snapshot(
        self,
        venue_id: str,
        date_str: str,
        period: str = "DAILY",
    ) -> Optional[KPISnapshot]:
        """Get a specific KPI snapshot.

        Args:
            venue_id: Venue identifier
            date_str: ISO date string
            period: KPI period (DAILY/WEEKLY/MONTHLY)

        Returns:
            KPISnapshot or None
        """
        with self._lock:
            for snapshot in self._snapshots.values():
                if (snapshot.venue_id == venue_id and
                    snapshot.date == date_str and
                    snapshot.period.value == period):
                    return snapshot
        return None

    def get_snapshots(
        self,
        venue_id: str,
        date_from: str,
        date_to: str,
        period: str = "DAILY",
    ) -> List[KPISnapshot]:
        """Get KPI snapshots within date range.

        Args:
            venue_id: Venue identifier
            date_from: Start ISO date
            date_to: End ISO date (inclusive)
            period: KPI period filter

        Returns:
            List of KPISnapshot objects sorted by date
        """
        with self._lock:
            try:
                from_date = date.fromisoformat(date_from)
                to_date = date.fromisoformat(date_to)
            except (ValueError, TypeError):
                return []

            snapshots = []
            for snapshot in self._snapshots.values():
                if snapshot.venue_id != venue_id:
                    continue
                if snapshot.period.value != period:
                    continue

                try:
                    snap_date = date.fromisoformat(snapshot.date)
                    if from_date <= snap_date <= to_date:
                        snapshots.append(snapshot)
                except (ValueError, TypeError):
                    continue

        # Sort by date, newest first
        snapshots.sort(key=lambda s: s.date, reverse=True)
        return snapshots

    def get_current_kpis(self, venue_id: str) -> Dict[str, Any]:
        """Get latest daily KPIs and trends.

        Args:
            venue_id: Venue identifier

        Returns:
            Dict with latest metrics and 7-day trends
        """
        with self._lock:
            # Find most recent daily snapshot
            daily_snapshots = [
                s for s in self._snapshots.values()
                if s.venue_id == venue_id and s.period == KPIPeriod.DAILY
            ]

        if not daily_snapshots:
            return {}

        daily_snapshots.sort(key=lambda s: s.date, reverse=True)
        latest = daily_snapshots[0]

        # Calculate trends (7-day)
        today = date.fromisoformat(latest.date)
        week_ago = today - timedelta(days=7)

        older_snapshots = [
            s for s in daily_snapshots
            if date.fromisoformat(s.date) == week_ago
        ]

        trends = {}
        if older_snapshots:
            older = older_snapshots[0]
            for key in latest.metrics:
                if key in older.metrics:
                    old_val = older.metrics[key]
                    new_val = latest.metrics[key]
                    if old_val != 0:
                        pct_change = ((new_val - old_val) / old_val) * 100
                        trends[key] = round(pct_change, 2)

        return {
            "latest": latest.to_dict(),
            "trends": trends,
        }

    def get_trends(
        self,
        venue_id: str,
        date_str: str,
        lookback_days: int = 7,
    ) -> Dict[str, Any]:
        """Get percentage change vs prior period.

        Args:
            venue_id: Venue identifier
            date_str: Reference date (ISO)
            lookback_days: Days to look back

        Returns:
            Dict with trend percentages for each metric
        """
        try:
            today = date.fromisoformat(date_str)
            prior_date = today - timedelta(days=lookback_days)
        except (ValueError, TypeError):
            return {}

        current = self.get_snapshot(venue_id, date_str)
        prior = self.get_snapshot(venue_id, prior_date.isoformat())

        if not current or not prior:
            return {}

        trends = {}
        for key in current.metrics:
            if key in prior.metrics:
                old_val = prior.metrics[key]
                new_val = current.metrics[key]
                if old_val != 0:
                    pct_change = ((new_val - old_val) / old_val) * 100
                    trends[key] = round(pct_change, 2)
                else:
                    trends[key] = 0.0

        return trends

    def get_alerts(self, venue_id: str) -> List[Dict[str, Any]]:
        """Get all active alerts for a venue.

        Args:
            venue_id: Venue identifier

        Returns:
            List of alert dicts
        """
        _p = _get_persistence()
        if _p and _p.is_persistence_enabled():
            try:
                rows = _p.fetchall(
                    "SELECT * FROM kpi_alerts WHERE venue_id = ? ORDER BY created_at DESC",
                    (venue_id,)
                )
                return [dict(row) for row in rows]
            except Exception:
                pass

        return []

    def set_targets(self, venue_id: str, targets: Dict[str, Any]) -> Dict[str, Any]:
        """Set KPI targets for a venue.

        Args:
            venue_id: Venue identifier
            targets: Dict mapping metric names to target values

        Returns:
            Updated targets dict
        """
        with self._lock:
            self._targets[venue_id] = targets

        self._persist_targets(venue_id, targets)
        return targets

    def get_targets(self, venue_id: str) -> Dict[str, Any]:
        """Get KPI targets for a venue.

        Args:
            venue_id: Venue identifier

        Returns:
            Dict of targets or empty dict
        """
        with self._lock:
            return self._targets.get(venue_id, {})

    def get_target_progress(
        self,
        venue_id: str,
        date_str: str,
    ) -> Dict[str, Any]:
        """Get actual vs target metrics.

        Args:
            venue_id: Venue identifier
            date_str: ISO date

        Returns:
            Dict with actual, target, and variance
        """
        snapshot = self.get_snapshot(venue_id, date_str)
        targets = self.get_targets(venue_id)

        if not snapshot or not targets:
            return {}

        progress = {
            "date": date_str,
            "metrics": {},
        }

        for metric_name, target_value in targets.items():
            actual = snapshot.metrics.get(metric_name, 0)
            variance = actual - target_value
            variance_pct = 0.0
            if target_value != 0:
                variance_pct = (variance / target_value) * 100

            progress["metrics"][metric_name] = {
                "actual": actual,
                "target": target_value,
                "variance": round(variance, 2),
                "variance_pct": round(variance_pct, 2),
            }

        return progress

    def compare_periods(
        self,
        venue_id: str,
        date1: str,
        date2: str,
        period: str = "DAILY",
    ) -> Dict[str, Any]:
        """Compare KPI metrics between two periods.

        Args:
            venue_id: Venue identifier
            date1: First ISO date
            date2: Second ISO date
            period: KPI period

        Returns:
            Dict with comparison metrics
        """
        snap1 = self.get_snapshot(venue_id, date1, period)
        snap2 = self.get_snapshot(venue_id, date2, period)

        if not snap1 or not snap2:
            return {}

        comparison = {
            "period_1": date1,
            "period_2": date2,
            "metrics": {},
        }

        for key in snap1.metrics:
            if key in snap2.metrics:
                val1 = snap1.metrics[key]
                val2 = snap2.metrics[key]
                variance = val2 - val1
                variance_pct = 0.0
                if val1 != 0:
                    variance_pct = (variance / val1) * 100

                comparison["metrics"][key] = {
                    "period_1": val1,
                    "period_2": val2,
                    "variance": round(variance, 2),
                    "variance_pct": round(variance_pct, 2),
                }

        return comparison

    def get_venue_ranking(
        self,
        venue_ids: List[str],
        date_str: str,
        metric_name: str,
    ) -> List[Dict[str, Any]]:
        """Rank venues by a specific metric on a date.

        Args:
            venue_ids: List of venue IDs to rank
            date_str: ISO date
            metric_name: Metric to rank by

        Returns:
            List of venues ranked by metric (highest first)
        """
        ranking = []

        for venue_id in venue_ids:
            snapshot = self.get_snapshot(venue_id, date_str)
            if snapshot and metric_name in snapshot.metrics:
                ranking.append({
                    "venue_id": venue_id,
                    "metric_name": metric_name,
                    "value": snapshot.metrics[metric_name],
                })

        # Sort by metric value descending
        ranking.sort(key=lambda x: x["value"], reverse=True)

        # Add rank
        for i, item in enumerate(ranking, 1):
            item["rank"] = i

        return ranking


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_kpi_store_singleton: Optional[KPIDashboardStore] = None
_singleton_lock = threading.Lock()


def get_kpi_dashboard_store() -> KPIDashboardStore:
    """Get the module-level KPI dashboard store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _kpi_store_singleton
    if _kpi_store_singleton is None:
        with _singleton_lock:
            if _kpi_store_singleton is None:
                _kpi_store_singleton = KPIDashboardStore()
    return _kpi_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _kpi_store_singleton
    _kpi_store_singleton = None
