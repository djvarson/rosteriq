"""Forecast-vs-actual accuracy reporting (Round 13).

Reads historical DailyActuals (already populated by the Tanda history
warehouse) and produces an accuracy digest:

- MAPE (mean absolute percentage error) of revenue forecast
- Bias (signed average error — are we systematically over or under?)
- Worst/best days
- A rolling trend (7-day rolling MAPE)
- Labour-hour variance alongside revenue variance

Why this matters: "the forecast was wrong" is not actionable. The
accuracy digest is what turns raw numbers into coaching — e.g. "your
Friday forecast has been 18% too high for four weeks running", which
points directly at a pattern the forecast engine should learn.

How to apply: the Ask agent + the dashboard both consume this digest
via GET /api/v1/reports/forecast-accuracy. No new storage — we build
on the DailyActuals rows that already have both forecast_revenue and
actual_revenue.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from rosteriq.tanda_history import (
    DailyActuals,
    TandaHistoryStore,
    get_history_store,
)

logger = logging.getLogger("rosteriq.forecast_accuracy")


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------


def _pct_err(forecast: float, actual: float) -> Optional[float]:
    """Signed percentage error: (forecast - actual) / actual * 100.

    Returns None if actual is zero (no ground truth to compare against).
    Positive = forecast was high; negative = forecast was low.
    """
    if actual <= 0:
        return None
    return ((forecast - actual) / actual) * 100.0


@dataclass
class AccuracyRow:
    day: date
    forecast_revenue: float
    actual_revenue: float
    pct_err: Optional[float]  # signed; None when actual=0
    labour_variance_hours: float  # worked - rostered

    def to_dict(self) -> Dict[str, Any]:
        return {
            "day": self.day.isoformat(),
            "forecast_revenue": round(self.forecast_revenue, 2),
            "actual_revenue": round(self.actual_revenue, 2),
            "pct_err": round(self.pct_err, 2) if self.pct_err is not None else None,
            "labour_variance_hours": round(self.labour_variance_hours, 2),
        }


def _to_rows(daily: List[DailyActuals]) -> List[AccuracyRow]:
    return [
        AccuracyRow(
            day=d.day,
            forecast_revenue=d.forecast_revenue,
            actual_revenue=d.actual_revenue,
            pct_err=_pct_err(d.forecast_revenue, d.actual_revenue),
            labour_variance_hours=d.variance_hours,
        )
        for d in daily
    ]


def _mape(rows: List[AccuracyRow]) -> Optional[float]:
    """Mean absolute percentage error over rows with a computable pct_err."""
    errs = [abs(r.pct_err) for r in rows if r.pct_err is not None]
    if not errs:
        return None
    return round(sum(errs) / len(errs), 2)


def _bias(rows: List[AccuracyRow]) -> Optional[float]:
    """Signed average pct_err — positive = chronically over-forecast."""
    errs = [r.pct_err for r in rows if r.pct_err is not None]
    if not errs:
        return None
    return round(sum(errs) / len(errs), 2)


def _rolling_mape(rows: List[AccuracyRow], window: int = 7) -> List[Dict[str, Any]]:
    """Sliding-window MAPE over the last `window` days at each point."""
    out: List[Dict[str, Any]] = []
    for i in range(len(rows)):
        start = max(0, i - window + 1)
        slice_ = rows[start : i + 1]
        m = _mape(slice_)
        out.append({
            "day": rows[i].day.isoformat(),
            "rolling_mape": m,
            "window_size": len(slice_),
        })
    return out


def _pick_worst_best(rows: List[AccuracyRow]) -> Dict[str, Any]:
    scored = [r for r in rows if r.pct_err is not None]
    if not scored:
        return {"worst": None, "best": None}
    worst = max(scored, key=lambda r: abs(r.pct_err))
    best = min(scored, key=lambda r: abs(r.pct_err))
    return {
        "worst": worst.to_dict(),
        "best": best.to_dict(),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_accuracy_report(
    venue_id: str,
    days: int = 28,
    store: Optional[TandaHistoryStore] = None,
) -> Dict[str, Any]:
    """Assemble a full forecast-accuracy digest for a venue.

    Args:
        venue_id: Venue to report on.
        days: Look-back window (default 28 = 4 weeks).
        store: Optional override — defaults to the global history store.

    Returns:
        Dict with overall metrics (mape, bias), worst/best day, rolling
        trend, and per-day rows. Always returns a well-formed dict even
        when there is no data, so the UI can render a graceful empty
        state.
    """
    s = store or get_history_store()
    end = date.today()
    start = end - timedelta(days=days - 1)
    daily = s.daily_range(venue_id, start, end)
    rows = _to_rows(daily)

    scoreable = [r for r in rows if r.pct_err is not None]

    return {
        "venue_id": venue_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "window_days": days,
        "rows_total": len(rows),
        "rows_scoreable": len(scoreable),
        "mape": _mape(rows),
        "bias": _bias(rows),
        "direction": _direction(_bias(rows)),
        "labour_variance_hours_total": round(
            sum(r.labour_variance_hours for r in rows), 2
        ),
        "extremes": _pick_worst_best(rows),
        "rolling": _rolling_mape(rows),
        "rows": [r.to_dict() for r in rows],
    }


def _direction(bias: Optional[float]) -> str:
    """Human-readable direction tag derived from bias."""
    if bias is None:
        return "no_data"
    if abs(bias) < 5:
        return "on_target"
    return "over_forecasting" if bias > 0 else "under_forecasting"
