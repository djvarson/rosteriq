"""POS Sales Curve Forecaster for RosterIQ.

Builds hourly and daily sales profiles from historical POS data.
Uses actual trade patterns to predict staffing demand by hour,
giving dollar-backed headcount recommendations.

E.g. "Based on last 8 Tuesdays, you average $1,200/hr at 6pm and
need 4 floor staff to maintain your $300/staff-hour target."

Persistence: SQLite via rosteriq.persistence.
"""

from __future__ import annotations

import logging
import math
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("rosteriq.sales_curve")

# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------

try:
    from rosteriq.persistence import get_persistence as _get_persistence
except ImportError:
    _get_persistence = None


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday"]


@dataclass
class HourlySalesRecord:
    """A single hour of POS sales data."""
    id: str
    venue_id: str
    date: str  # ISO date
    hour: int  # 0-23
    revenue: float
    transaction_count: int
    covers: int  # patrons served
    avg_transaction: float  # revenue / transaction_count
    source: str  # "pos_swiftpos", "pos_tanda", "manual"
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "date": self.date,
            "hour": self.hour,
            "revenue": self.revenue,
            "transaction_count": self.transaction_count,
            "covers": self.covers,
            "avg_transaction": round(self.avg_transaction, 2),
            "source": self.source,
            "created_at": self.created_at,
        }


@dataclass
class SalesCurve:
    """Aggregated sales profile for a day-of-week or date range."""
    venue_id: str
    day_of_week: Optional[int]  # 0=Monday, None if custom range
    label: str  # e.g. "Tuesday", "2026-04-14 to 2026-04-20"
    hourly_profile: Dict[int, Dict[str, float]]  # hour -> {avg_revenue, avg_covers, ...}
    total_avg_revenue: float
    total_avg_covers: float
    peak_hour: int
    peak_revenue: float
    quiet_hours: List[int]  # hours where revenue < 10% of peak
    sample_count: int  # number of days averaged
    confidence: float  # 0-1, based on sample size + variance

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "day_of_week": self.day_of_week,
            "label": self.label,
            "hourly_profile": self.hourly_profile,
            "total_avg_revenue": round(self.total_avg_revenue, 2),
            "total_avg_covers": round(self.total_avg_covers, 2),
            "peak_hour": self.peak_hour,
            "peak_revenue": round(self.peak_revenue, 2),
            "quiet_hours": self.quiet_hours,
            "sample_count": self.sample_count,
            "confidence": round(self.confidence, 2),
        }


@dataclass
class StaffingRecommendation:
    """Dollar-backed staffing recommendation for a time slot."""
    venue_id: str
    date: str
    hour: int
    predicted_revenue: float
    predicted_covers: float
    target_revenue_per_staff_hour: float
    recommended_staff: int
    confidence: float
    reasoning: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "date": self.date,
            "hour": self.hour,
            "predicted_revenue": round(self.predicted_revenue, 2),
            "predicted_covers": round(self.predicted_covers, 2),
            "target_revenue_per_staff_hour": round(
                self.target_revenue_per_staff_hour, 2),
            "recommended_staff": self.recommended_staff,
            "confidence": round(self.confidence, 2),
            "reasoning": self.reasoning,
        }


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

_store: Optional["SalesCurveStore"] = None
_store_lock = threading.Lock()


def get_sales_curve_store() -> "SalesCurveStore":
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = SalesCurveStore()
    return _store


def _reset_for_tests():
    """Test helper: clear store without persistence reload."""
    global _store
    with _store_lock:
        _store = SalesCurveStore.__new__(SalesCurveStore)
        _store._lock = threading.Lock()
        _store._records = {}
        _store._targets = {}


class SalesCurveStore:
    """Thread-safe store for POS sales data and curve generation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._records: Dict[str, HourlySalesRecord] = {}
        self._targets: Dict[str, Dict[str, float]] = {}  # venue_id -> targets

        if _get_persistence is not None:
            try:
                _p = _get_persistence()
                _p.register_schema("sales_curve_records", [
                    "id TEXT PRIMARY KEY",
                    "venue_id TEXT NOT NULL",
                    "date TEXT NOT NULL",
                    "hour INTEGER NOT NULL",
                    "data TEXT NOT NULL",
                ])
                logger.info("Sales curve persistence schema registered")
            except Exception as exc:
                logger.warning("Sales curve persistence unavailable: %s", exc)

    # ------------------------------------------------------------------
    # Ingest POS data
    # ------------------------------------------------------------------

    def add_hourly_record(self, rec_dict: Dict[str, Any]) -> HourlySalesRecord:
        """Add a single hour of POS sales data."""
        with self._lock:
            rec_id = rec_dict.get("id", str(uuid.uuid4()))
            revenue = float(rec_dict.get("revenue", 0))
            txn_count = int(rec_dict.get("transaction_count", 0))
            avg_txn = (revenue / txn_count) if txn_count > 0 else 0

            rec = HourlySalesRecord(
                id=rec_id,
                venue_id=rec_dict["venue_id"],
                date=rec_dict["date"],
                hour=int(rec_dict["hour"]),
                revenue=revenue,
                transaction_count=txn_count,
                covers=int(rec_dict.get("covers", 0)),
                avg_transaction=round(avg_txn, 2),
                source=rec_dict.get("source", "manual"),
            )
            self._records[rec.id] = rec
            return rec

    def bulk_ingest(self, records: List[Dict[str, Any]]) -> int:
        """Ingest multiple hourly records at once."""
        count = 0
        for rec in records:
            self.add_hourly_record(rec)
            count += 1
        return count

    def ingest_daily_pos(self, venue_id: str, pos_date: str,
                         hourly_data: List[Dict[str, Any]],
                         source: str = "pos") -> int:
        """Ingest a full day of POS data (list of hourly breakdowns).

        Each dict: {hour, revenue, transaction_count, covers}
        """
        count = 0
        for hd in hourly_data:
            self.add_hourly_record({
                "venue_id": venue_id,
                "date": pos_date,
                "hour": hd["hour"],
                "revenue": hd.get("revenue", 0),
                "transaction_count": hd.get("transaction_count", 0),
                "covers": hd.get("covers", 0),
                "source": source,
            })
            count += 1
        return count

    # ------------------------------------------------------------------
    # Query raw data
    # ------------------------------------------------------------------

    def get_records(self, venue_id: str, date_from: Optional[str] = None,
                    date_to: Optional[str] = None,
                    hour: Optional[int] = None) -> List[HourlySalesRecord]:
        """Get hourly records with filters."""
        with self._lock:
            results = [r for r in self._records.values()
                       if r.venue_id == venue_id]
            if date_from:
                results = [r for r in results if r.date >= date_from]
            if date_to:
                results = [r for r in results if r.date <= date_to]
            if hour is not None:
                results = [r for r in results if r.hour == hour]
            return sorted(results, key=lambda r: (r.date, r.hour))

    def get_daily_total(self, venue_id: str, target_date: str) -> Dict[str, Any]:
        """Get total revenue/covers for a specific date."""
        records = self.get_records(venue_id, target_date, target_date)
        total_rev = sum(r.revenue for r in records)
        total_covers = sum(r.covers for r in records)
        total_txns = sum(r.transaction_count for r in records)
        return {
            "date": target_date,
            "venue_id": venue_id,
            "total_revenue": round(total_rev, 2),
            "total_covers": total_covers,
            "total_transactions": total_txns,
            "hours_with_data": len(records),
        }

    # ------------------------------------------------------------------
    # Sales curve generation
    # ------------------------------------------------------------------

    def build_day_of_week_curve(self, venue_id: str,
                                day_of_week: int,
                                weeks_back: int = 8) -> SalesCurve:
        """Build an average hourly sales curve for a specific day of week.

        Averages the last N occurrences of that weekday.
        """
        # Collect all records, group by date
        with self._lock:
            all_records = [r for r in self._records.values()
                           if r.venue_id == venue_id]

        # Filter to matching weekday
        day_records: Dict[str, List[HourlySalesRecord]] = defaultdict(list)
        for r in all_records:
            try:
                d = date.fromisoformat(r.date)
                if d.weekday() == day_of_week:
                    day_records[r.date].append(r)
            except ValueError:
                continue

        # Sort dates descending and take last N weeks
        sorted_dates = sorted(day_records.keys(), reverse=True)[:weeks_back]
        sample_count = len(sorted_dates)

        # Build hourly averages
        hourly_sums: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {"revenue": [], "covers": [], "transactions": []})

        for d in sorted_dates:
            for r in day_records[d]:
                hourly_sums[r.hour]["revenue"].append(r.revenue)
                hourly_sums[r.hour]["covers"].append(float(r.covers))
                hourly_sums[r.hour]["transactions"].append(
                    float(r.transaction_count))

        hourly_profile: Dict[int, Dict[str, float]] = {}
        total_rev = 0.0
        total_covers = 0.0
        peak_hour = 0
        peak_revenue = 0.0

        for hour in range(24):
            if hour in hourly_sums:
                revs = hourly_sums[hour]["revenue"]
                covs = hourly_sums[hour]["covers"]
                txns = hourly_sums[hour]["transactions"]

                avg_rev = sum(revs) / len(revs) if revs else 0
                avg_cov = sum(covs) / len(covs) if covs else 0
                avg_txn = sum(txns) / len(txns) if txns else 0

                # Standard deviation for confidence
                if len(revs) > 1:
                    mean = avg_rev
                    variance = sum((x - mean) ** 2 for x in revs) / len(revs)
                    std_dev = math.sqrt(variance)
                    cv = (std_dev / mean) if mean > 0 else 1.0
                else:
                    cv = 1.0

                hourly_profile[hour] = {
                    "avg_revenue": round(avg_rev, 2),
                    "avg_covers": round(avg_cov, 1),
                    "avg_transactions": round(avg_txn, 1),
                    "std_dev": round(std_dev if len(revs) > 1 else 0, 2),
                    "coefficient_of_variation": round(cv, 3),
                    "sample_count": len(revs),
                }

                total_rev += avg_rev
                total_covers += avg_cov

                if avg_rev > peak_revenue:
                    peak_revenue = avg_rev
                    peak_hour = hour
            else:
                hourly_profile[hour] = {
                    "avg_revenue": 0, "avg_covers": 0,
                    "avg_transactions": 0, "std_dev": 0,
                    "coefficient_of_variation": 0, "sample_count": 0,
                }

        # Quiet hours: < 10% of peak
        quiet_threshold = peak_revenue * 0.1
        quiet_hours = [h for h in range(24)
                       if hourly_profile[h]["avg_revenue"] < quiet_threshold
                       and hourly_profile[h]["avg_revenue"] >= 0]

        # Confidence based on sample size (8 weeks = ideal)
        confidence = min(1.0, sample_count / 8) * 0.7
        # Boost if low variance
        avg_cv = 0.0
        cv_count = 0
        for h in hourly_profile.values():
            if h["sample_count"] > 0:
                avg_cv += h["coefficient_of_variation"]
                cv_count += 1
        if cv_count > 0:
            avg_cv /= cv_count
            confidence += max(0, 0.3 * (1 - avg_cv))
        confidence = min(1.0, confidence)

        label = DAYS_OF_WEEK[day_of_week] if 0 <= day_of_week <= 6 else f"Day {day_of_week}"

        return SalesCurve(
            venue_id=venue_id,
            day_of_week=day_of_week,
            label=label,
            hourly_profile=hourly_profile,
            total_avg_revenue=round(total_rev, 2),
            total_avg_covers=round(total_covers, 1),
            peak_hour=peak_hour,
            peak_revenue=round(peak_revenue, 2),
            quiet_hours=quiet_hours,
            sample_count=sample_count,
            confidence=round(confidence, 2),
        )

    def build_weekly_curves(self, venue_id: str,
                            weeks_back: int = 8) -> List[SalesCurve]:
        """Build curves for all 7 days of the week."""
        return [self.build_day_of_week_curve(venue_id, dow, weeks_back)
                for dow in range(7)]

    def build_custom_curve(self, venue_id: str,
                           date_from: str, date_to: str) -> SalesCurve:
        """Build a curve from a specific date range (averaging all days)."""
        records = self.get_records(venue_id, date_from, date_to)

        dates_seen = set()
        hourly_sums: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {"revenue": [], "covers": []})

        for r in records:
            dates_seen.add(r.date)
            hourly_sums[r.hour]["revenue"].append(r.revenue)
            hourly_sums[r.hour]["covers"].append(float(r.covers))

        sample_count = len(dates_seen)

        hourly_profile: Dict[int, Dict[str, float]] = {}
        total_rev = 0.0
        total_covers = 0.0
        peak_hour = 0
        peak_revenue = 0.0

        for hour in range(24):
            if hour in hourly_sums:
                revs = hourly_sums[hour]["revenue"]
                covs = hourly_sums[hour]["covers"]
                avg_rev = sum(revs) / len(revs) if revs else 0
                avg_cov = sum(covs) / len(covs) if covs else 0
                hourly_profile[hour] = {
                    "avg_revenue": round(avg_rev, 2),
                    "avg_covers": round(avg_cov, 1),
                    "sample_count": len(revs),
                }
                total_rev += avg_rev
                total_covers += avg_cov
                if avg_rev > peak_revenue:
                    peak_revenue = avg_rev
                    peak_hour = hour
            else:
                hourly_profile[hour] = {
                    "avg_revenue": 0, "avg_covers": 0, "sample_count": 0}

        quiet_threshold = peak_revenue * 0.1
        quiet_hours = [h for h in range(24)
                       if hourly_profile[h]["avg_revenue"] < quiet_threshold]

        confidence = min(1.0, sample_count / 14) * 0.8

        return SalesCurve(
            venue_id=venue_id,
            day_of_week=None,
            label=f"{date_from} to {date_to}",
            hourly_profile=hourly_profile,
            total_avg_revenue=round(total_rev, 2),
            total_avg_covers=round(total_covers, 1),
            peak_hour=peak_hour,
            peak_revenue=round(peak_revenue, 2),
            quiet_hours=quiet_hours,
            sample_count=sample_count,
            confidence=round(confidence, 2),
        )

    # ------------------------------------------------------------------
    # Staffing recommendations
    # ------------------------------------------------------------------

    def set_targets(self, venue_id: str, targets: Dict[str, float]):
        """Set venue staffing targets.

        targets: {
            "revenue_per_staff_hour": 300.0,  # target $/staff-hour
            "min_staff": 2,  # minimum staff at any time
            "max_staff": 15,  # cap
            "covers_per_staff_hour": 8,  # alternative metric
        }
        """
        with self._lock:
            self._targets[venue_id] = targets
            return targets

    def get_targets(self, venue_id: str) -> Dict[str, float]:
        """Get venue staffing targets with defaults."""
        with self._lock:
            defaults = {
                "revenue_per_staff_hour": 300.0,
                "min_staff": 2,
                "max_staff": 15,
                "covers_per_staff_hour": 8,
            }
            stored = self._targets.get(venue_id, {})
            defaults.update(stored)
            return defaults

    def recommend_staffing(self, venue_id: str,
                           target_date: str,
                           hours: Optional[List[int]] = None,
                           use_curve: bool = True) -> List[StaffingRecommendation]:
        """Generate staffing recommendations for a date.

        Uses the day-of-week sales curve to predict revenue,
        then divides by target $/staff-hour.
        """
        try:
            d = date.fromisoformat(target_date)
            dow = d.weekday()
        except ValueError:
            return []

        targets = self.get_targets(venue_id)
        rev_target = targets["revenue_per_staff_hour"]
        min_staff = int(targets["min_staff"])
        max_staff = int(targets["max_staff"])

        if use_curve:
            curve = self.build_day_of_week_curve(venue_id, dow)
        else:
            curve = None

        if hours is None:
            hours = list(range(24))

        recommendations = []
        for hour in hours:
            if curve and hour in curve.hourly_profile:
                hp = curve.hourly_profile[hour]
                predicted_rev = hp.get("avg_revenue", 0)
                predicted_cov = hp.get("avg_covers", 0)
                conf = curve.confidence
            else:
                predicted_rev = 0
                predicted_cov = 0
                conf = 0

            # Calculate recommended staff
            if rev_target > 0 and predicted_rev > 0:
                raw_staff = predicted_rev / rev_target
                staff = max(min_staff, min(max_staff, math.ceil(raw_staff)))
            else:
                staff = min_staff

            reasoning = (
                f"Predicted ${predicted_rev:.0f}/hr revenue "
                f"({curve.sample_count if curve else 0} {DAYS_OF_WEEK[dow]}s averaged). "
                f"At ${rev_target:.0f}/staff-hr target → "
                f"{predicted_rev/rev_target:.1f} staff needed."
                if predicted_rev > 0 else
                f"No historical data for {DAYS_OF_WEEK[dow]} {hour:02d}:00. "
                f"Using minimum staffing ({min_staff})."
            )

            recommendations.append(StaffingRecommendation(
                venue_id=venue_id,
                date=target_date,
                hour=hour,
                predicted_revenue=predicted_rev,
                predicted_covers=predicted_cov,
                target_revenue_per_staff_hour=rev_target,
                recommended_staff=staff,
                confidence=conf,
                reasoning=reasoning,
            ))

        return recommendations

    def get_daily_staffing_plan(self, venue_id: str,
                                target_date: str,
                                operating_hours: Optional[Tuple[int, int]] = None
                                ) -> Dict[str, Any]:
        """Generate a full day staffing plan.

        operating_hours: (open_hour, close_hour) e.g. (10, 23)
        """
        if operating_hours:
            hours = list(range(operating_hours[0], operating_hours[1] + 1))
        else:
            hours = list(range(6, 24))  # default 6am-midnight

        recs = self.recommend_staffing(venue_id, target_date, hours)

        total_staff_hours = sum(r.recommended_staff for r in recs)
        total_predicted_rev = sum(r.predicted_revenue for r in recs)
        peak_rec = max(recs, key=lambda r: r.recommended_staff) if recs else None

        return {
            "venue_id": venue_id,
            "date": target_date,
            "operating_hours": hours,
            "hourly_plan": [r.to_dict() for r in recs],
            "summary": {
                "total_staff_hours": total_staff_hours,
                "total_predicted_revenue": round(total_predicted_rev, 2),
                "peak_hour": peak_rec.hour if peak_rec else None,
                "peak_staff_needed": peak_rec.recommended_staff if peak_rec else 0,
                "avg_staff": round(total_staff_hours / len(hours), 1) if hours else 0,
            },
        }

    # ------------------------------------------------------------------
    # Trend analysis
    # ------------------------------------------------------------------

    def get_weekly_revenue_trend(self, venue_id: str,
                                 weeks: int = 8) -> List[Dict[str, Any]]:
        """Get weekly revenue totals for trend analysis."""
        today = date.today()
        results = []

        for w in range(weeks):
            week_end = today - timedelta(days=today.weekday()) - timedelta(weeks=w)
            week_start = week_end - timedelta(days=6)

            records = self.get_records(
                venue_id, week_start.isoformat(), week_end.isoformat())
            total_rev = sum(r.revenue for r in records)
            total_covers = sum(r.covers for r in records)
            days_with_data = len(set(r.date for r in records))

            results.append({
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "total_revenue": round(total_rev, 2),
                "total_covers": total_covers,
                "days_with_data": days_with_data,
                "avg_daily_revenue": round(total_rev / max(1, days_with_data), 2),
            })

        results.reverse()  # chronological order
        return results

    def get_hour_comparison(self, venue_id: str, hour: int,
                            weeks: int = 4) -> Dict[str, Any]:
        """Compare a specific hour's performance across recent weeks."""
        today = date.today()
        weekly_data = []

        for w in range(weeks):
            week_date = today - timedelta(weeks=w)
            records = self.get_records(
                venue_id, date_from=week_date.isoformat(),
                date_to=week_date.isoformat(), hour=hour)
            if records:
                rev = sum(r.revenue for r in records)
                weekly_data.append({
                    "date": week_date.isoformat(),
                    "revenue": round(rev, 2),
                    "covers": sum(r.covers for r in records),
                })

        return {
            "venue_id": venue_id,
            "hour": hour,
            "weeks_compared": weeks,
            "data": weekly_data,
            "avg_revenue": round(
                sum(d["revenue"] for d in weekly_data) / max(1, len(weekly_data)), 2),
        }
