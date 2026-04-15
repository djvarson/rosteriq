"""Tanda historical data ingestion + warehouse (Round 8 Track B).

Pulls historical rosters, timesheets and forecast revenue from Tanda and
stores rolled-up daily and hourly aggregates in an in-memory warehouse
keyed by venue_id. Pure stdlib — no FastAPI/Pydantic/httpx hard deps.

Why this matters: the forecast/roster engines and the Ask agent need
access to actuals (what was rostered, what was worked, what was earned)
so they can compare predictions to ground truth, surface variance, and
answer questions like "what did I spend on labour last Friday".

How to apply: an ingest job calls TandaHistoryIngestor.ingest_range()
periodically (or on-demand from /api/v1/tanda/history/ingest). Aggregates
are queryable via TandaHistoryStore which is also exposed over REST in
tanda_history_router.py.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.tanda_history")

AU_TZ = timezone(timedelta(hours=10))


# Round 12 — SQLite schema
_SCHEMA = """
CREATE TABLE IF NOT EXISTS tanda_daily_actuals (
    venue_id           TEXT NOT NULL,
    day                TEXT NOT NULL,
    rostered_hours     REAL NOT NULL DEFAULT 0,
    worked_hours       REAL NOT NULL DEFAULT 0,
    rostered_cost      REAL NOT NULL DEFAULT 0,
    worked_cost        REAL NOT NULL DEFAULT 0,
    forecast_revenue   REAL NOT NULL DEFAULT 0,
    actual_revenue     REAL NOT NULL DEFAULT 0,
    shift_count        INTEGER NOT NULL DEFAULT 0,
    employee_count     INTEGER NOT NULL DEFAULT 0,
    notes              TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (venue_id, day)
);
CREATE INDEX IF NOT EXISTS ix_tanda_daily_venue ON tanda_daily_actuals(venue_id);

CREATE TABLE IF NOT EXISTS tanda_hourly_actuals (
    venue_id           TEXT NOT NULL,
    day                TEXT NOT NULL,
    hour               INTEGER NOT NULL,
    rostered_heads     INTEGER NOT NULL DEFAULT 0,
    worked_heads       INTEGER NOT NULL DEFAULT 0,
    forecast_revenue   REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (venue_id, day, hour)
);

CREATE TABLE IF NOT EXISTS tanda_last_ingest (
    venue_id           TEXT PRIMARY KEY,
    ingested_at        TEXT NOT NULL
);
"""
_p.register_schema("tanda_history", _SCHEMA)


# ---------------------------------------------------------------------------
# Aggregate dataclasses
# ---------------------------------------------------------------------------


@dataclass
class DailyActuals:
    """One day of rolled-up actuals for a venue."""

    venue_id: str
    day: date
    rostered_hours: float = 0.0
    worked_hours: float = 0.0
    rostered_cost: float = 0.0
    worked_cost: float = 0.0
    forecast_revenue: float = 0.0
    actual_revenue: float = 0.0
    shift_count: int = 0
    employee_count: int = 0
    notes: List[str] = field(default_factory=list)

    @property
    def labour_pct(self) -> Optional[float]:
        """Worked-cost as a % of actual revenue (None if no revenue)."""
        if self.actual_revenue <= 0:
            return None
        return round((self.worked_cost / self.actual_revenue) * 100.0, 2)

    @property
    def variance_hours(self) -> float:
        """worked - rostered hours. Positive = staff worked over plan."""
        return round(self.worked_hours - self.rostered_hours, 2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "day": self.day.isoformat(),
            "rostered_hours": round(self.rostered_hours, 2),
            "worked_hours": round(self.worked_hours, 2),
            "rostered_cost": round(self.rostered_cost, 2),
            "worked_cost": round(self.worked_cost, 2),
            "forecast_revenue": round(self.forecast_revenue, 2),
            "actual_revenue": round(self.actual_revenue, 2),
            "shift_count": self.shift_count,
            "employee_count": self.employee_count,
            "labour_pct": self.labour_pct,
            "variance_hours": self.variance_hours,
            "notes": list(self.notes),
        }


@dataclass
class HourlyActuals:
    """One hour bucket of rolled-up actuals (for intraday analysis)."""

    venue_id: str
    day: date
    hour: int  # 0..23
    rostered_heads: int = 0
    worked_heads: int = 0
    forecast_revenue: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "day": self.day.isoformat(),
            "hour": self.hour,
            "rostered_heads": self.rostered_heads,
            "worked_heads": self.worked_heads,
            "forecast_revenue": round(self.forecast_revenue, 2),
        }


# ---------------------------------------------------------------------------
# Thread-safe warehouse
# ---------------------------------------------------------------------------


class TandaHistoryStore:
    """In-memory warehouse for Tanda historical aggregates.

    Keyed first by venue_id, then by ISO day for daily aggregates and
    by (day, hour) for hourly aggregates. Always returns snapshots so
    callers can iterate without holding the lock.
    """

    def __init__(self) -> None:
        self._daily: Dict[str, Dict[str, DailyActuals]] = {}
        self._hourly: Dict[str, Dict[Tuple[str, int], HourlyActuals]] = {}
        self._lock = threading.Lock()
        self._last_ingest: Dict[str, datetime] = {}

    # -- persistence helpers --

    def _persist_hourly(self, agg: HourlyActuals) -> None:
        if not _p.is_persistence_enabled():
            return
        try:
            with _p.write_txn() as c:
                c.execute(
                    "INSERT OR REPLACE INTO tanda_hourly_actuals "
                    "(venue_id, day, hour, rostered_heads, worked_heads, forecast_revenue) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [agg.venue_id, agg.day.isoformat(), agg.hour,
                     agg.rostered_heads, agg.worked_heads, agg.forecast_revenue],
                )
        except Exception as e:
            logger.warning("hourly persist failed: %s", e)

    def _persist_daily_manual(self, agg: DailyActuals) -> None:
        """Composite-key UPSERT — sqlite INSERT OR REPLACE works because of
        the composite primary key on (venue_id, day)."""
        if not _p.is_persistence_enabled():
            return
        try:
            with _p.write_txn() as c:
                c.execute(
                    "INSERT OR REPLACE INTO tanda_daily_actuals "
                    "(venue_id, day, rostered_hours, worked_hours, rostered_cost, "
                    " worked_cost, forecast_revenue, actual_revenue, shift_count, "
                    " employee_count, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [agg.venue_id, agg.day.isoformat(), agg.rostered_hours,
                     agg.worked_hours, agg.rostered_cost, agg.worked_cost,
                     agg.forecast_revenue, agg.actual_revenue, agg.shift_count,
                     agg.employee_count, _p.json_dumps(list(agg.notes))],
                )
        except Exception as e:
            logger.warning("daily persist failed: %s", e)

    def rehydrate(self) -> None:
        if not _p.is_persistence_enabled():
            return
        with self._lock:
            for r in _p.fetchall("SELECT * FROM tanda_daily_actuals"):
                try:
                    agg = DailyActuals(
                        venue_id=r["venue_id"],
                        day=date.fromisoformat(r["day"]),
                        rostered_hours=r["rostered_hours"],
                        worked_hours=r["worked_hours"],
                        rostered_cost=r["rostered_cost"],
                        worked_cost=r["worked_cost"],
                        forecast_revenue=r["forecast_revenue"],
                        actual_revenue=r["actual_revenue"],
                        shift_count=r["shift_count"],
                        employee_count=r["employee_count"],
                        notes=_p.json_loads(r["notes"], default=[]) or [],
                    )
                    self._daily.setdefault(agg.venue_id, {})[agg.day.isoformat()] = agg
                except Exception as e:
                    logger.warning("rehydrate daily row failed: %s", e)
            for r in _p.fetchall("SELECT * FROM tanda_hourly_actuals"):
                try:
                    agg = HourlyActuals(
                        venue_id=r["venue_id"],
                        day=date.fromisoformat(r["day"]),
                        hour=r["hour"],
                        rostered_heads=r["rostered_heads"],
                        worked_heads=r["worked_heads"],
                        forecast_revenue=r["forecast_revenue"],
                    )
                    self._hourly.setdefault(agg.venue_id, {})[(agg.day.isoformat(), agg.hour)] = agg
                except Exception as e:
                    logger.warning("rehydrate hourly row failed: %s", e)
            for r in _p.fetchall("SELECT * FROM tanda_last_ingest"):
                try:
                    self._last_ingest[r["venue_id"]] = datetime.fromisoformat(r["ingested_at"])
                except Exception as e:
                    logger.warning("rehydrate last_ingest row failed: %s", e)
        logger.info(
            "Tanda history rehydrated: %d daily, %d hourly across %d venues",
            sum(len(v) for v in self._daily.values()),
            sum(len(v) for v in self._hourly.values()),
            len(self._daily),
        )

    # -- writes --

    def upsert_daily(self, agg: DailyActuals) -> None:
        with self._lock:
            self._daily.setdefault(agg.venue_id, {})[agg.day.isoformat()] = agg
        self._persist_daily_manual(agg)

    def upsert_hourly(self, agg: HourlyActuals) -> None:
        with self._lock:
            self._hourly.setdefault(agg.venue_id, {})[(agg.day.isoformat(), agg.hour)] = agg
        self._persist_hourly(agg)

    def mark_ingested(self, venue_id: str) -> None:
        with self._lock:
            self._last_ingest[venue_id] = datetime.now(AU_TZ)
        if _p.is_persistence_enabled():
            try:
                _p.upsert(
                    "tanda_last_ingest",
                    {"venue_id": venue_id, "ingested_at": self._last_ingest[venue_id].isoformat()},
                    pk="venue_id",
                )
            except Exception as e:
                logger.warning("last_ingest persist failed: %s", e)

    # -- reads --

    def daily_range(
        self,
        venue_id: str,
        start: date,
        end: date,
    ) -> List[DailyActuals]:
        """Return daily aggregates inclusive of start and end."""
        with self._lock:
            venue_map = dict(self._daily.get(venue_id, {}))
        out: List[DailyActuals] = []
        cur = start
        while cur <= end:
            agg = venue_map.get(cur.isoformat())
            if agg is not None:
                out.append(agg)
            cur += timedelta(days=1)
        return out

    def hourly_for_day(self, venue_id: str, day: date) -> List[HourlyActuals]:
        with self._lock:
            venue_map = dict(self._hourly.get(venue_id, {}))
        return [
            agg for (k_day, _hr), agg in sorted(venue_map.items())
            if k_day == day.isoformat()
        ]

    def last_ingested(self, venue_id: str) -> Optional[datetime]:
        with self._lock:
            return self._last_ingest.get(venue_id)

    def venues(self) -> List[str]:
        with self._lock:
            return sorted(set(self._daily.keys()) | set(self._hourly.keys()))

    def clear(self) -> None:
        """Test helper."""
        with self._lock:
            self._daily.clear()
            self._hourly.clear()
            self._last_ingest.clear()


# Module-level singleton
_store: Optional[TandaHistoryStore] = None


def get_history_store() -> TandaHistoryStore:
    global _store
    if _store is None:
        _store = TandaHistoryStore()
    return _store


@_p.on_init
def _rehydrate_tanda_history_on_init() -> None:
    get_history_store().rehydrate()


# ---------------------------------------------------------------------------
# Ingestor
# ---------------------------------------------------------------------------


def _shift_hours(start_t: time, end_t: time, break_minutes: int = 0) -> float:
    """Compute paid hours from start/end times (handles same-day shifts).

    For overnight shifts (end < start) we add 24h. break_minutes is
    subtracted from gross hours but never below zero.
    """
    start_min = start_t.hour * 60 + start_t.minute
    end_min = end_t.hour * 60 + end_t.minute
    if end_min <= start_min:
        end_min += 24 * 60
    gross = (end_min - start_min) / 60.0
    paid = gross - (break_minutes / 60.0)
    return max(0.0, round(paid, 4))


def _shift_hour_buckets(start_t: time, end_t: time) -> List[int]:
    """Return the hour-of-day buckets a shift spans (rounded to whole hours).

    A shift 09:30-13:15 spans hours [9, 10, 11, 12, 13].
    """
    start_h = start_t.hour
    end_h = end_t.hour
    if end_h < start_h or (end_h == start_h and end_t.minute <= start_t.minute):
        # treat as next-day end → cap at 23 to keep things bounded
        end_h = 23
    if end_t.minute > 0 and end_h < 23:
        end_h += 1
    return list(range(start_h, max(start_h + 1, end_h + 1)))


class TandaHistoryIngestor:
    """Pull historicals from a TandaClient/adapter into the warehouse.

    The adapter argument should expose:
      - get_shifts(org_id, (start, end)) -> List[Shift]
      - get_timesheets(org_id, (start, end)) -> List[Timesheet]
      - get_forecast_revenue(org_id, (start, end)) -> List[ForecastRevenue]
      - get_employees(org_id) -> List[Employee]  (used for hourly rate lookup)

    Missing methods or runtime errors are tolerated and logged — partial
    ingest is better than no ingest.
    """

    def __init__(
        self,
        adapter: Any,
        store: Optional[TandaHistoryStore] = None,
        default_hourly_rate: float = 32.0,
    ) -> None:
        self.adapter = adapter
        self.store = store or get_history_store()
        self.default_hourly_rate = default_hourly_rate

    async def _employee_rates(self, org_id: str) -> Dict[str, float]:
        try:
            employees = await self.adapter.get_employees(org_id)
        except Exception as e:  # pragma: no cover - depends on live adapter
            logger.warning("get_employees failed for %s: %s", org_id, e)
            return {}
        rates: Dict[str, float] = {}
        for emp in employees:
            rate = getattr(emp, "hourly_rate", None) or self.default_hourly_rate
            rates[getattr(emp, "id", "")] = float(rate)
        return rates

    async def ingest_range(
        self,
        venue_id: str,
        org_id: str,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        """Pull rosters + timesheets + forecast revenue for [start, end].

        Returns a summary dict with counts so callers can verify the run.
        """
        if start > end:
            raise ValueError("start must be <= end")

        rates = await self._employee_rates(org_id)

        # Fetch source data — degrade gracefully if any call fails
        try:
            shifts = await self.adapter.get_shifts(org_id, (start, end))
        except Exception as e:
            logger.warning("get_shifts failed: %s", e)
            shifts = []

        try:
            timesheets = await self.adapter.get_timesheets(org_id, (start, end))
        except Exception as e:
            logger.warning("get_timesheets failed: %s", e)
            timesheets = []

        try:
            forecasts = await self.adapter.get_forecast_revenue(org_id, (start, end))
        except Exception as e:
            logger.warning("get_forecast_revenue failed: %s", e)
            forecasts = []

        # Build daily aggregates
        days: Dict[date, DailyActuals] = {}
        cur = start
        while cur <= end:
            days[cur] = DailyActuals(venue_id=venue_id, day=cur)
            cur += timedelta(days=1)

        employees_per_day: Dict[date, set] = {d: set() for d in days}
        hourly: Dict[Tuple[date, int], HourlyActuals] = {}

        # Roll up rostered shifts
        for sh in shifts:
            d = getattr(sh, "date", None)
            if d not in days:
                continue
            hours = _shift_hours(sh.start_time, sh.end_time, getattr(sh, "break_minutes", 0))
            rate = rates.get(getattr(sh, "employee_id", ""), self.default_hourly_rate)
            agg = days[d]
            agg.rostered_hours += hours
            agg.rostered_cost += hours * rate
            agg.shift_count += 1
            employees_per_day[d].add(getattr(sh, "employee_id", ""))
            for hr in _shift_hour_buckets(sh.start_time, sh.end_time):
                key = (d, hr)
                if key not in hourly:
                    hourly[key] = HourlyActuals(venue_id=venue_id, day=d, hour=hr)
                hourly[key].rostered_heads += 1

        # Roll up worked timesheets
        for ts in timesheets:
            d = getattr(ts, "date", None)
            if d not in days:
                continue
            hours = float(getattr(ts, "hours", 0.0) or 0.0)
            rate = rates.get(getattr(ts, "employee_id", ""), self.default_hourly_rate)
            agg = days[d]
            agg.worked_hours += hours
            agg.worked_cost += hours * rate
            employees_per_day[d].add(getattr(ts, "employee_id", ""))
            # Walk per-shift entries if present to populate hourly worked heads
            for entry in getattr(ts, "shifts", []) or []:
                start_t = entry.get("start_time") if isinstance(entry, dict) else None
                end_t = entry.get("end_time") if isinstance(entry, dict) else None
                if isinstance(start_t, time) and isinstance(end_t, time):
                    for hr in _shift_hour_buckets(start_t, end_t):
                        key = (d, hr)
                        if key not in hourly:
                            hourly[key] = HourlyActuals(venue_id=venue_id, day=d, hour=hr)
                        hourly[key].worked_heads += 1

        # Roll up forecast revenue (and actual where present)
        for fc in forecasts:
            fc_dt = getattr(fc, "datetime", None) or getattr(fc, "date", None)
            if isinstance(fc_dt, datetime):
                fc_day = fc_dt.date()
                fc_hour = fc_dt.hour
            elif isinstance(fc_dt, date):
                fc_day = fc_dt
                fc_hour = None
            else:
                continue
            if fc_day not in days:
                continue
            forecast_amt = float(getattr(fc, "forecast", 0.0) or 0.0)
            actual_amt = float(getattr(fc, "actual", 0.0) or 0.0)
            agg = days[fc_day]
            agg.forecast_revenue += forecast_amt
            agg.actual_revenue += actual_amt
            if fc_hour is not None:
                key = (fc_day, fc_hour)
                if key not in hourly:
                    hourly[key] = HourlyActuals(venue_id=venue_id, day=fc_day, hour=fc_hour)
                hourly[key].forecast_revenue += forecast_amt

        # Finalise + persist
        for d, agg in days.items():
            agg.employee_count = len(employees_per_day[d])
            self.store.upsert_daily(agg)
        for h_agg in hourly.values():
            self.store.upsert_hourly(h_agg)

        self.store.mark_ingested(venue_id)

        summary = {
            "venue_id": venue_id,
            "org_id": org_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "days_processed": len(days),
            "shift_count": sum(d.shift_count for d in days.values()),
            "timesheet_hours": round(sum(d.worked_hours for d in days.values()), 2),
            "rostered_hours": round(sum(d.rostered_hours for d in days.values()), 2),
            "forecast_revenue": round(sum(d.forecast_revenue for d in days.values()), 2),
            "actual_revenue": round(sum(d.actual_revenue for d in days.values()), 2),
            "hourly_buckets": len(hourly),
            "ingested_at": datetime.now(AU_TZ).isoformat(),
        }
        logger.info("Tanda history ingest complete: %s", summary)
        return summary


# ---------------------------------------------------------------------------
# Convenience query helpers (used by Ask agent + REST router)
# ---------------------------------------------------------------------------


def variance_summary(
    venue_id: str,
    days: int = 14,
    store: Optional[TandaHistoryStore] = None,
) -> Dict[str, Any]:
    """Roll up the last `days` days into a single variance digest."""
    s = store or get_history_store()
    end = date.today()
    start = end - timedelta(days=days - 1)
    rows = s.daily_range(venue_id, start, end)
    if not rows:
        return {
            "venue_id": venue_id,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "rows": 0,
            "message": "no historical data ingested yet",
        }
    total_rost = sum(r.rostered_hours for r in rows)
    total_work = sum(r.worked_hours for r in rows)
    total_rev = sum(r.actual_revenue for r in rows)
    total_cost = sum(r.worked_cost for r in rows)
    return {
        "venue_id": venue_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "rows": len(rows),
        "rostered_hours": round(total_rost, 2),
        "worked_hours": round(total_work, 2),
        "variance_hours": round(total_work - total_rost, 2),
        "labour_cost": round(total_cost, 2),
        "actual_revenue": round(total_rev, 2),
        "labour_pct": round((total_cost / total_rev) * 100.0, 2) if total_rev > 0 else None,
    }
