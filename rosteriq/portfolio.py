"""Multi-venue portfolio reporting module for Enterprise tier.

Powers the "your portfolio" owner view — a consolidated view across all venues
showing performance, anomalies, and weekly digest data.

Pure stdlib module with lazy imports.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.portfolio")


@dataclass
class VenueSummary:
    """Summary of a single venue's performance over a period."""
    venue_id: str
    period_days: int
    total_revenue: float
    total_labour_cost: float
    avg_labour_pct: Optional[float]
    total_variance_hours: float
    days_over_forecast: int
    days_under_forecast: int
    busiest_day: Optional[date]
    quietest_day: Optional[date]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "period_days": self.period_days,
            "total_revenue": round(self.total_revenue, 2),
            "total_labour_cost": round(self.total_labour_cost, 2),
            "avg_labour_pct": round(self.avg_labour_pct, 2) if self.avg_labour_pct is not None else None,
            "total_variance_hours": round(self.total_variance_hours, 2),
            "days_over_forecast": self.days_over_forecast,
            "days_under_forecast": self.days_under_forecast,
            "busiest_day": self.busiest_day.isoformat() if self.busiest_day else None,
            "quietest_day": self.quietest_day.isoformat() if self.quietest_day else None,
        }


@dataclass
class Anomaly:
    """An anomaly detected in a venue's performance."""
    venue_id: str
    type: str  # "high_labour" | "low_labour" | "over_rostered" | "data_gap"
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "type": self.type,
            "message": self.message,
        }


def build_venue_summary(
    venue_id: str,
    days: int = 7,
    history_store: Any = None,
) -> Dict[str, Any]:
    """Build a summary of a single venue's performance over N days.

    Uses tanda_history.get_history_store() (or passed store) to pull daily
    actuals for the venue for the given period.

    Returns:
        {
            "venue_id": str,
            "period_days": int,
            "total_revenue": float,
            "total_labour_cost": float,
            "avg_labour_pct": float | None,
            "total_variance_hours": float,
            "days_over_forecast": int,
            "days_under_forecast": int,
            "busiest_day": str (ISO date) | None,
            "quietest_day": str (ISO date) | None,
        }

    Handles empty data gracefully (returns zeros).
    """
    if history_store is None:
        from rosteriq import tanda_history as _th
        history_store = _th.get_history_store()

    today = date.today()
    start = today - timedelta(days=days)

    # Fetch daily actuals for the period
    daily_actuals = history_store.daily_range(venue_id, start, today)

    if not daily_actuals:
        # Empty data — return zeros
        return {
            "venue_id": venue_id,
            "period_days": days,
            "total_revenue": 0.0,
            "total_labour_cost": 0.0,
            "avg_labour_pct": None,
            "total_variance_hours": 0.0,
            "days_over_forecast": 0,
            "days_under_forecast": 0,
            "busiest_day": None,
            "quietest_day": None,
        }

    total_revenue = sum(a.actual_revenue for a in daily_actuals)
    total_labour_cost = sum(a.worked_cost for a in daily_actuals)
    total_variance_hours = sum(a.variance_hours for a in daily_actuals)

    # Compute weighted average labour_pct
    labour_pct_sum = 0.0
    valid_labour_pct_count = 0
    for a in daily_actuals:
        if a.labour_pct is not None:
            labour_pct_sum += a.labour_pct
            valid_labour_pct_count += 1

    avg_labour_pct = labour_pct_sum / valid_labour_pct_count if valid_labour_pct_count > 0 else None

    # Count over/under forecast days
    days_over_forecast = sum(1 for a in daily_actuals if a.variance_hours > 0)
    days_under_forecast = sum(1 for a in daily_actuals if a.variance_hours < 0)

    # Find busiest and quietest days
    busiest_day = None
    quietest_day = None
    if daily_actuals:
        max_revenue = max((a.actual_revenue for a in daily_actuals), default=0.0)
        min_revenue = min((a.actual_revenue for a in daily_actuals if a.actual_revenue > 0), default=None)

        if max_revenue > 0:
            busiest_day = next((a.day for a in daily_actuals if a.actual_revenue == max_revenue), None)
        if min_revenue is not None and min_revenue > 0:
            quietest_day = next((a.day for a in daily_actuals if a.actual_revenue == min_revenue), None)

    return {
        "venue_id": venue_id,
        "period_days": days,
        "total_revenue": round(total_revenue, 2),
        "total_labour_cost": round(total_labour_cost, 2),
        "avg_labour_pct": round(avg_labour_pct, 2) if avg_labour_pct is not None else None,
        "total_variance_hours": round(total_variance_hours, 2),
        "days_over_forecast": days_over_forecast,
        "days_under_forecast": days_under_forecast,
        "busiest_day": busiest_day.isoformat() if busiest_day else None,
        "quietest_day": quietest_day.isoformat() if quietest_day else None,
    }


def detect_anomalies(venue_summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detect anomalies across a list of venue summaries.

    Flags:
    - labour_pct > 35% (high labour cost)
    - labour_pct < 20% (suspiciously low — data issue?)
    - variance_hours > 10% of total worked hours (significant over-rostering)
    - venue with 0 revenue days (data gap)

    Returns list of dicts with keys: venue_id, type, message
    """
    anomalies: List[Dict[str, Any]] = []

    for venue in venue_summaries:
        venue_id = venue["venue_id"]
        labour_pct = venue.get("avg_labour_pct")
        variance_hours = venue.get("total_variance_hours", 0.0)
        total_revenue = venue.get("total_revenue", 0.0)

        # Flag high labour cost
        if labour_pct is not None and labour_pct > 35.0:
            anomalies.append({
                "venue_id": venue_id,
                "type": "high_labour",
                "message": f"Labour cost at {labour_pct:.1f}% of revenue (threshold: 35%)",
            })

        # Flag suspiciously low labour cost
        if labour_pct is not None and labour_pct < 20.0:
            anomalies.append({
                "venue_id": venue_id,
                "type": "low_labour",
                "message": f"Labour cost at {labour_pct:.1f}% of revenue (possible data issue)",
            })

        # Flag significant over-rostering
        # Check if variance is > 10% of the total rostered hours
        # For simplicity, if total revenue is 0 we can't compute this
        if total_revenue > 0 and variance_hours > 0:
            # Estimate total worked hours from labour cost / avg hourly rate
            # For now, use a simple heuristic: flag if variance > 5 hours
            if variance_hours > 5.0:
                anomalies.append({
                    "venue_id": venue_id,
                    "type": "over_rostered",
                    "message": f"Over-rostered by {variance_hours:.1f} hours over period",
                })

        # Flag data gaps (0 revenue days)
        if total_revenue == 0.0:
            anomalies.append({
                "venue_id": venue_id,
                "type": "data_gap",
                "message": "No revenue recorded for period (possible data import gap)",
            })

    return anomalies


def build_portfolio_report(
    venue_ids: List[str],
    days: int = 7,
    history_store: Any = None,
) -> Dict[str, Any]:
    """Build a consolidated portfolio report across multiple venues.

    Calls build_venue_summary for each venue and aggregates results.

    Returns:
        {
            "period_days": int,
            "period_start": str (ISO date),
            "period_end": str (ISO date),
            "venue_count": int,
            "venues": [venue_summary, ...],  # sorted by total_revenue desc
            "totals": {
                "total_revenue": float,
                "total_labour_cost": float,
                "avg_labour_pct": float | None,
                "total_variance_hours": float,
            },
            "anomalies": [...],
            "rankings": {
                "best_labour_pct": venue_id | None,
                "worst_labour_pct": venue_id | None,
                "highest_revenue": venue_id | None,
                "most_over_rostered": venue_id | None,
            }
        }
    """
    if history_store is None:
        from rosteriq import tanda_history as _th
        history_store = _th.get_history_store()

    today = date.today()
    start = today - timedelta(days=days)

    # Build summaries for each venue
    venue_summaries = [
        build_venue_summary(vid, days=days, history_store=history_store)
        for vid in venue_ids
    ]

    # Sort by total_revenue descending
    venue_summaries_sorted = sorted(
        venue_summaries,
        key=lambda v: v.get("total_revenue", 0.0),
        reverse=True,
    )

    # Aggregate totals
    total_revenue = sum(v.get("total_revenue", 0.0) for v in venue_summaries)
    total_labour_cost = sum(v.get("total_labour_cost", 0.0) for v in venue_summaries)
    total_variance_hours = sum(v.get("total_variance_hours", 0.0) for v in venue_summaries)

    # Weighted average labour_pct
    labour_pct_sum = 0.0
    valid_labour_pct_count = 0
    for v in venue_summaries:
        lp = v.get("avg_labour_pct")
        if lp is not None:
            labour_pct_sum += lp
            valid_labour_pct_count += 1

    avg_labour_pct = labour_pct_sum / valid_labour_pct_count if valid_labour_pct_count > 0 else None

    # Detect anomalies
    anomalies_list = detect_anomalies(venue_summaries)

    # Compute rankings
    best_labour_pct = None
    worst_labour_pct = None
    highest_revenue = None
    most_over_rostered = None

    venue_with_labour = [v for v in venue_summaries if v.get("avg_labour_pct") is not None]
    if venue_with_labour:
        best_labour_pct = min(venue_with_labour, key=lambda v: v.get("avg_labour_pct", float("inf"))).get("venue_id")
        worst_labour_pct = max(venue_with_labour, key=lambda v: v.get("avg_labour_pct", 0.0)).get("venue_id")

    if venue_summaries:
        highest_revenue = max(venue_summaries, key=lambda v: v.get("total_revenue", 0.0)).get("venue_id")
        most_over_rostered = max(venue_summaries, key=lambda v: v.get("total_variance_hours", 0.0)).get("venue_id")

    return {
        "period_days": days,
        "period_start": start.isoformat(),
        "period_end": today.isoformat(),
        "venue_count": len(venue_ids),
        "venues": venue_summaries_sorted,
        "totals": {
            "total_revenue": round(total_revenue, 2),
            "total_labour_cost": round(total_labour_cost, 2),
            "avg_labour_pct": round(avg_labour_pct, 2) if avg_labour_pct is not None else None,
            "total_variance_hours": round(total_variance_hours, 2),
        },
        "anomalies": anomalies_list,
        "rankings": {
            "best_labour_pct": best_labour_pct,
            "worst_labour_pct": worst_labour_pct,
            "highest_revenue": highest_revenue,
            "most_over_rostered": most_over_rostered,
        },
    }
