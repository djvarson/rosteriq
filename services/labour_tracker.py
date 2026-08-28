"""
Real-time revenue vs labour tracker with colour-coded alerts.

Combines live POS revenue, current staff costs, and forecasts to provide
real-time labour percentage monitoring with configurable thresholds.

Components:
- LabourTracker: Main service combining revenue and costing
- LabourStatus: Current live status snapshot with alerts
- HourlySnapshot: Hour-by-hour labour efficiency tracking
- EODPrediction: End-of-day labour cost projection
- ForecastComparison: Actual vs forecasted labour %
- WeeklyLabourSummary: Week-to-date labour performance
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict
from collections import defaultdict

from rosteriq.models import (
    Employee, Shift, VenueConfig, EmploymentType, State,
)
from rosteriq.database import get_db
from rosteriq.cost_calculator import calculate_shift_cost_breakdown

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class HourlySnapshot:
    """Hour-by-hour labour efficiency snapshot."""

    hour: int  # 0-23
    revenue: Decimal = Decimal("0.00")
    labour_cost: Decimal = Decimal("0.00")
    staff_count: int = 0
    labour_pct: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def calculate_labour_pct(self) -> float:
        """Calculate labour % for this hour."""
        if self.revenue <= 0:
            return 0.0
        pct = (self.labour_cost / self.revenue * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        return float(pct)


@dataclass
class LabourStatus:
    """Current live labour status snapshot."""

    venue_id: str
    timestamp: str  # ISO format
    revenue_today: Decimal
    labour_cost_today: Decimal
    labour_percentage: float
    status: str  # "green" | "amber" | "red" | "critical"
    staff_on_shift: int
    average_cost_per_hour: Decimal
    revenue_per_staff_hour: Decimal
    hourly_breakdown: List[HourlySnapshot] = field(default_factory=list)
    projected_eod_labour_pct: float = 0.0
    projected_eod_revenue: Decimal = Decimal("0.00")
    projected_eod_cost: Decimal = Decimal("0.00")
    alert_message: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "venue_id": self.venue_id,
            "timestamp": self.timestamp,
            "revenue_today": str(self.revenue_today),
            "labour_cost_today": str(self.labour_cost_today),
            "labour_percentage": round(self.labour_percentage, 2),
            "status": self.status,
            "staff_on_shift": self.staff_on_shift,
            "average_cost_per_hour": str(self.average_cost_per_hour),
            "revenue_per_staff_hour": str(self.revenue_per_staff_hour),
            "hourly_breakdown": [
                {
                    "hour": h.hour,
                    "revenue": str(h.revenue),
                    "labour_cost": str(h.labour_cost),
                    "staff_count": h.staff_count,
                    "labour_pct": round(h.labour_pct, 2),
                }
                for h in self.hourly_breakdown
            ],
            "projected_eod_labour_pct": round(self.projected_eod_labour_pct, 2),
            "projected_eod_revenue": str(self.projected_eod_revenue),
            "projected_eod_cost": str(self.projected_eod_cost),
            "alert_message": self.alert_message,
        }


@dataclass
class EODPrediction:
    """End-of-day labour cost projection."""

    predicted_revenue: Decimal
    predicted_cost: Decimal
    predicted_labour_pct: float
    confidence: float  # 0.0-1.0
    method: str  # "linear" | "weighted" | "trend"
    hours_extrapolated: int = 0
    last_update: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "predicted_revenue": str(self.predicted_revenue),
            "predicted_cost": str(self.predicted_cost),
            "predicted_labour_pct": round(self.predicted_labour_pct, 2),
            "confidence": round(self.confidence, 3),
            "method": self.method,
            "hours_extrapolated": self.hours_extrapolated,
            "last_update": self.last_update.isoformat(),
        }


@dataclass
class ForecastComparison:
    """Actual vs forecasted labour % comparison."""

    forecast_revenue: Decimal
    actual_revenue: Decimal
    variance_pct: float  # Positive = worse than forecast
    forecast_labour_pct: float
    actual_labour_pct: float
    variance_labour_pct: float  # Positive = over budget
    hours_completed: int
    hours_remaining: int

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "forecast_revenue": str(self.forecast_revenue),
            "actual_revenue": str(self.actual_revenue),
            "variance_pct": round(self.variance_pct, 2),
            "forecast_labour_pct": round(self.forecast_labour_pct, 2),
            "actual_labour_pct": round(self.actual_labour_pct, 2),
            "variance_labour_pct": round(self.variance_labour_pct, 2),
            "hours_completed": self.hours_completed,
            "hours_remaining": self.hours_remaining,
        }


@dataclass
class DailyLabourSummary:
    """Daily labour summary with date."""

    date: str  # YYYY-MM-DD
    revenue: Decimal
    labour_cost: Decimal
    labour_pct: float
    status: str
    staff_hours: Decimal


@dataclass
class WeeklyLabourSummary:
    """Week-to-date labour performance summary."""

    week_start: str  # YYYY-MM-DD
    week_end: str  # YYYY-MM-DD
    daily_summaries: List[DailyLabourSummary] = field(default_factory=list)
    week_avg_labour_pct: float = 0.0
    week_total_revenue: Decimal = Decimal("0.00")
    week_total_cost: Decimal = Decimal("0.00")
    best_day: Optional[str] = None
    worst_day: Optional[str] = None
    trend: str = "stable"  # "improving" | "stable" | "declining"

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "week_start": self.week_start,
            "week_end": self.week_end,
            "daily_summaries": [
                {
                    "date": d.date,
                    "revenue": str(d.revenue),
                    "labour_cost": str(d.labour_cost),
                    "labour_pct": round(d.labour_pct, 2),
                    "status": d.status,
                    "staff_hours": str(d.staff_hours),
                }
                for d in self.daily_summaries
            ],
            "week_avg_labour_pct": round(self.week_avg_labour_pct, 2),
            "week_total_revenue": str(self.week_total_revenue),
            "week_total_cost": str(self.week_total_cost),
            "best_day": self.best_day,
            "worst_day": self.worst_day,
            "trend": self.trend,
        }


# ============================================================================
# Labour Tracker
# ============================================================================


class LabourTracker:
    """
    Real-time revenue vs labour tracker with colour-coded alerts.

    Combines POS revenue, current staff costs, and forecasts to monitor
    labour percentage in real-time.
    """

    # Default alert thresholds (percentage)
    DEFAULT_THRESHOLDS = {
        "green_max": 28.0,
        "amber_max": 33.0,
        "red_max": 38.0,
        # Critical is anything > 38%
    }

    def __init__(self):
        """Initialize the labour tracker."""
        self.db = get_db()
        self.threshold_configs: Dict[str, Dict[str, float]] = defaultdict(
            lambda: self.DEFAULT_THRESHOLDS.copy()
        )
        logger.info("LabourTracker initialized")

    # ========================================================================
    # Core Methods
    # ========================================================================

    def get_live_status(self, venue_id: str) -> LabourStatus:
        """
        Get current live labour status for a venue.

        Combines:
        - Live POS revenue (today so far)
        - Current on-shift staff costs (who's clocked in right now)
        - Calculate real-time labour percentage
        """
        venue = self.db.get_venue(venue_id)
        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        today = date.today()
        now = datetime.now()
        current_hour = now.hour

        # Get POS revenue accumulator for today
        revenue_today = self._get_revenue_today(venue_id)

        # Get current staff on shift
        staff_on_shift = self._get_staff_on_shift(venue_id, now)
        labour_cost_today = self._calculate_labour_cost_today(
            venue_id, staff_on_shift, today, venue.state
        )

        # Calculate labour percentage
        if revenue_today <= 0:
            labour_pct = 0.0
        else:
            labour_pct = float(
                (labour_cost_today / revenue_today * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )

        # Determine status based on thresholds
        thresholds = self.threshold_configs[venue_id]
        status = self._determine_status(labour_pct, thresholds)

        # Calculate metrics
        avg_cost_per_hour = (
            labour_cost_today / Decimal(current_hour + 1)
            if current_hour >= 0
            else Decimal("0.00")
        )
        revenue_per_staff_hour = (
            revenue_today / Decimal(len(staff_on_shift))
            if len(staff_on_shift) > 0
            else Decimal("0.00")
        )

        # Get hourly breakdown
        hourly_breakdown = self.get_hourly_trend(venue_id)

        # Get end-of-day projection
        eod_pred = self.predict_end_of_day(venue_id)

        # Generate alert message if needed
        alert_message = self._generate_alert_message(
            labour_pct, status, len(staff_on_shift), revenue_today
        )

        return LabourStatus(
            venue_id=venue_id,
            timestamp=now.isoformat(),
            revenue_today=revenue_today,
            labour_cost_today=labour_cost_today,
            labour_percentage=labour_pct,
            status=status,
            staff_on_shift=len(staff_on_shift),
            average_cost_per_hour=avg_cost_per_hour,
            revenue_per_staff_hour=revenue_per_staff_hour,
            hourly_breakdown=hourly_breakdown,
            projected_eod_labour_pct=eod_pred.predicted_labour_pct,
            projected_eod_revenue=eod_pred.predicted_revenue,
            projected_eod_cost=eod_pred.predicted_cost,
            alert_message=alert_message,
        )

    def get_hourly_trend(self, venue_id: str) -> List[HourlySnapshot]:
        """
        Get today's hour-by-hour labour efficiency breakdown.

        Returns data for each hour of the day so far.
        """
        today = date.today()
        now = datetime.now()
        current_hour = now.hour

        hourly_data: Dict[int, HourlySnapshot] = {}

        # Get hourly revenue from POS accumulator
        revenue_by_hour = self._get_revenue_by_hour(venue_id, today)

        # Get hourly labour costs
        labour_by_hour = self._get_labour_cost_by_hour(venue_id, today)

        # Get staff count by hour
        staff_by_hour = self._get_staff_count_by_hour(venue_id, today)

        # Compile hourly snapshots
        for hour in range(current_hour + 1):
            revenue = revenue_by_hour.get(hour, Decimal("0.00"))
            labour_cost = labour_by_hour.get(hour, Decimal("0.00"))
            staff_count = staff_by_hour.get(hour, 0)

            labour_pct = 0.0
            if revenue > 0:
                labour_pct = float(
                    (labour_cost / revenue * Decimal("100")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                )

            snapshot = HourlySnapshot(
                hour=hour,
                revenue=revenue,
                labour_cost=labour_cost,
                staff_count=staff_count,
                labour_pct=labour_pct,
            )
            hourly_data[hour] = snapshot

        return [hourly_data[h] for h in sorted(hourly_data.keys())]

    def predict_end_of_day(self, venue_id: str) -> EODPrediction:
        """
        Predict end-of-day revenue and labour costs.

        Uses linear extrapolation from current trends.
        """
        today = date.today()
        now = datetime.now()
        current_hour = now.hour
        hours_into_day = current_hour + 1

        # Get actual so far
        actual_revenue = self._get_revenue_today(venue_id)
        actual_cost = self._calculate_labour_cost_today(venue_id, [], today, State.nsw)

        # Hours remaining in operating day (assume 23:00 closing)
        hours_remaining = max(0, 23 - current_hour)

        if hours_into_day == 0 or actual_revenue <= 0:
            return EODPrediction(
                predicted_revenue=actual_revenue,
                predicted_cost=actual_cost,
                predicted_labour_pct=0.0,
                confidence=0.1,
                method="no_data",
                hours_extrapolated=0,
            )

        # Linear extrapolation
        avg_revenue_per_hour = actual_revenue / Decimal(hours_into_day)
        avg_cost_per_hour = actual_cost / Decimal(hours_into_day)

        projected_revenue = actual_revenue + (avg_revenue_per_hour * Decimal(hours_remaining))
        projected_cost = actual_cost + (avg_cost_per_hour * Decimal(hours_remaining))

        if projected_revenue <= 0:
            projected_labour_pct = 0.0
        else:
            projected_labour_pct = float(
                (projected_cost / projected_revenue * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )

        # Confidence decreases as we get further from actual data
        confidence = min(1.0, 0.9 - (hours_remaining * 0.02))

        return EODPrediction(
            predicted_revenue=projected_revenue,
            predicted_cost=projected_cost,
            predicted_labour_pct=projected_labour_pct,
            confidence=max(0.0, confidence),
            method="linear",
            hours_extrapolated=hours_remaining,
        )

    def get_threshold_config(self, venue_id: str) -> Dict[str, float]:
        """Get alert threshold configuration for a venue."""
        if venue_id not in self.threshold_configs:
            self.threshold_configs[venue_id] = self.DEFAULT_THRESHOLDS.copy()
        return self.threshold_configs[venue_id]

    def update_thresholds(self, venue_id: str, thresholds: Dict[str, float]) -> Dict[str, float]:
        """Update alert thresholds for a venue."""
        # Validate thresholds
        if not all(k in ["green_max", "amber_max", "red_max"] for k in thresholds.keys()):
            raise ValueError("Invalid threshold keys")

        # Ensure green < amber < red
        green = thresholds.get("green_max", self.DEFAULT_THRESHOLDS["green_max"])
        amber = thresholds.get("amber_max", self.DEFAULT_THRESHOLDS["amber_max"])
        red = thresholds.get("red_max", self.DEFAULT_THRESHOLDS["red_max"])

        if not (green < amber < red):
            raise ValueError("Thresholds must be in order: green < amber < red")

        # Update config
        self.threshold_configs[venue_id].update(thresholds)
        logger.info(f"Updated thresholds for venue {venue_id}: {thresholds}")

        return self.threshold_configs[venue_id]

    def compare_to_forecast(self, venue_id: str, forecast_revenue: Decimal, forecast_labour_pct: float) -> ForecastComparison:
        """
        Compare actual vs forecasted labour performance.

        Used to track forecast accuracy.
        """
        today = date.today()
        now = datetime.now()
        current_hour = now.hour
        hours_completed = current_hour + 1

        # Get actual
        actual_revenue = self._get_revenue_today(venue_id)
        actual_cost = self._calculate_labour_cost_today(venue_id, [], today, State.nsw)

        if actual_revenue <= 0:
            actual_labour_pct = 0.0
        else:
            actual_labour_pct = float(
                (actual_cost / actual_revenue * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )

        # Calculate variances
        if forecast_revenue > 0:
            revenue_variance_pct = float(
                ((forecast_revenue - actual_revenue) / forecast_revenue * Decimal("100"))
                .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )
        else:
            revenue_variance_pct = 0.0

        labour_variance_pct = actual_labour_pct - forecast_labour_pct

        hours_remaining = max(0, 23 - current_hour)

        return ForecastComparison(
            forecast_revenue=forecast_revenue,
            actual_revenue=actual_revenue,
            variance_pct=revenue_variance_pct,
            forecast_labour_pct=forecast_labour_pct,
            actual_labour_pct=actual_labour_pct,
            variance_labour_pct=labour_variance_pct,
            hours_completed=hours_completed,
            hours_remaining=hours_remaining,
        )

    def get_weekly_summary(self, venue_id: str) -> WeeklyLabourSummary:
        """
        Get week-to-date labour performance summary.

        Returns daily labour % breakdown for the past 7 days.
        """
        today = date.today()
        week_start = today - timedelta(days=today.weekday())  # Monday
        week_end = week_start + timedelta(days=6)

        daily_summaries: List[DailyLabourSummary] = []
        total_revenue = Decimal("0.00")
        total_cost = Decimal("0.00")
        labour_pcts = []

        for offset in range(7):
            check_date = week_start + timedelta(days=offset)
            if check_date > today:
                break

            day_revenue = self._get_revenue_for_date(venue_id, check_date)
            day_cost = self._get_labour_cost_for_date(venue_id, check_date)
            day_staff_hours = self._get_staff_hours_for_date(venue_id, check_date)

            if day_revenue > 0:
                day_labour_pct = float(
                    (day_cost / day_revenue * Decimal("100")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                )
            else:
                day_labour_pct = 0.0

            # Determine daily status
            thresholds = self.threshold_configs[venue_id]
            day_status = self._determine_status(day_labour_pct, thresholds)

            summary = DailyLabourSummary(
                date=check_date.isoformat(),
                revenue=day_revenue,
                labour_cost=day_cost,
                labour_pct=day_labour_pct,
                status=day_status,
                staff_hours=day_staff_hours,
            )
            daily_summaries.append(summary)

            total_revenue += day_revenue
            total_cost += day_cost
            labour_pcts.append(day_labour_pct)

        # Calculate week averages
        week_avg_labour_pct = (
            sum(labour_pcts) / len(labour_pcts) if labour_pcts else 0.0
        )

        # Find best/worst days
        if labour_pcts:
            best_idx = labour_pcts.index(min(labour_pcts))
            worst_idx = labour_pcts.index(max(labour_pcts))
            best_day = daily_summaries[best_idx].date
            worst_day = daily_summaries[worst_idx].date
        else:
            best_day = None
            worst_day = None

        # Determine trend (first 3 days vs last 3 days)
        trend = "stable"
        if len(labour_pcts) >= 6:
            early_avg = sum(labour_pcts[:3]) / 3
            late_avg = sum(labour_pcts[-3:]) / 3
            if late_avg < early_avg - 1.0:
                trend = "improving"
            elif late_avg > early_avg + 1.0:
                trend = "declining"

        return WeeklyLabourSummary(
            week_start=week_start.isoformat(),
            week_end=week_end.isoformat(),
            daily_summaries=daily_summaries,
            week_avg_labour_pct=week_avg_labour_pct,
            week_total_revenue=total_revenue,
            week_total_cost=total_cost,
            best_day=best_day,
            worst_day=worst_day,
            trend=trend,
        )

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _get_revenue_today(self, venue_id: str) -> Decimal:
        """Get POS revenue accumulated so far today."""
        try:
            # This would integrate with the POS revenue accumulator
            # For now, return placeholder from database if available
            revenue = self.db.get_venue_revenue_today(venue_id)
            return revenue if revenue else Decimal("0.00")
        except Exception as e:
            logger.warning(f"Could not fetch revenue for {venue_id}: {e}")
            return Decimal("0.00")

    def _get_revenue_by_hour(self, venue_id: str, day: date) -> Dict[int, Decimal]:
        """Get hourly revenue breakdown for a day."""
        try:
            hourly = self.db.get_hourly_revenue(venue_id, day)
            return hourly if hourly else {}
        except Exception as e:
            logger.warning(f"Could not fetch hourly revenue: {e}")
            return {}

    def _get_revenue_for_date(self, venue_id: str, day: date) -> Decimal:
        """Get total revenue for a specific date."""
        try:
            revenue = self.db.get_venue_revenue_for_date(venue_id, day)
            return revenue if revenue else Decimal("0.00")
        except Exception as e:
            logger.warning(f"Could not fetch revenue for {day}: {e}")
            return Decimal("0.00")

    def _todays_shifts(self, venue_id: str, day: date) -> list:
        """Today's rostered shifts via the ONE query the store actually has.

        This tracker used to call get_active_shifts / get_shifts_for_date /
        get_staff_count_by_hour / get_total_staff_hours — none of which exist
        on any store. Every call raised AttributeError, every except swallowed
        it, and live labour ops reported an empty venue and 0% labour all
        day while looking perfectly healthy.
        """
        try:
            return list(self.db.get_shifts(venue_id, day, day) or [])
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Could not fetch today's shifts: {e}")
            return []

    def _get_staff_on_shift(self, venue_id: str, now: datetime) -> List[Employee]:
        """Get list of employees currently on shift."""
        try:
            current = now.time()
            employees = []
            for shift in self._todays_shifts(venue_id, now.date()):
                if shift.start_time <= current <= shift.end_time:
                    emp = self.db.get_employee(shift.employee_id)
                    if emp:
                        employees.append(emp)
            return employees
        except Exception as e:
            logger.warning(f"Could not fetch staff on shift: {e}")
            return []

    def _get_staff_count_by_hour(self, venue_id: str, day: date) -> Dict[int, int]:
        """Get number of staff on shift for each hour."""
        counts: Dict[int, int] = {}
        for shift in self._todays_shifts(venue_id, day):
            end_hour = shift.end_time.hour + (1 if shift.end_time.minute else 0)
            for hour in range(shift.start_time.hour, max(end_hour, shift.start_time.hour + 1)):
                counts[hour] = counts.get(hour, 0) + 1
        return counts

    def _get_staff_hours_for_date(self, venue_id: str, day: date) -> Decimal:
        """Get total staff hours for a date."""
        total = Decimal("0.00")
        for shift in self._todays_shifts(venue_id, day):
            minutes = ((shift.end_time.hour - shift.start_time.hour) * 60
                       + (shift.end_time.minute - shift.start_time.minute)
                       - (getattr(shift, "break_minutes", 0) or 0))
            if minutes > 0:
                total += Decimal(minutes) / Decimal(60)
        return total

    def _calculate_labour_cost_today(
        self, venue_id: str, staff_on_shift: List[Employee], today: date, state: State
    ) -> Decimal:
        """
        Calculate total labour cost for today so far.

        Includes base wages, penalty multipliers, and casual loading.
        """
        try:
            shifts = self._todays_shifts(venue_id, today)
            total_cost = Decimal("0.00")

            employees = {s.employee_id: e for e in staff_on_shift for s in [shifts[0]]}
            # Load all employees from DB
            all_employees = self.db.list_employees(venue_id)
            emp_map = {e.id: e for e in all_employees}

            for shift in shifts:
                if shift.employee_id in emp_map:
                    emp = emp_map[shift.employee_id]
                    breakdown = calculate_shift_cost_breakdown(emp, shift, state)
                    total_cost += breakdown.total_cost

            return total_cost
        except Exception as e:
            logger.warning(f"Could not calculate labour cost: {e}")
            return Decimal("0.00")

    def _get_labour_cost_by_hour(self, venue_id: str, day: date) -> Dict[int, Decimal]:
        """Get hourly labour cost breakdown."""
        try:
            hourly = self.db.get_hourly_labour_cost(venue_id, day)
            return hourly if hourly else {}
        except Exception as e:
            logger.warning(f"Could not fetch hourly labour costs: {e}")
            return {}

    def _get_labour_cost_for_date(self, venue_id: str, day: date) -> Decimal:
        """Get total labour cost for a date."""
        try:
            cost = self.db.get_venue_labour_cost_for_date(venue_id, day)
            return cost if cost else Decimal("0.00")
        except Exception as e:
            logger.warning(f"Could not fetch labour cost: {e}")
            return Decimal("0.00")

    def _determine_status(self, labour_pct: float, thresholds: Dict[str, float]) -> str:
        """Determine status based on labour % and thresholds."""
        if labour_pct <= thresholds.get("green_max", 28.0):
            return "green"
        elif labour_pct <= thresholds.get("amber_max", 33.0):
            return "amber"
        elif labour_pct <= thresholds.get("red_max", 38.0):
            return "red"
        else:
            return "critical"

    def _generate_alert_message(
        self, labour_pct: float, status: str, staff_count: int, revenue: Decimal
    ) -> Optional[str]:
        """Generate alert message if status warrants one."""
        if status == "green":
            return None

        if status == "critical":
            return f"CRITICAL: Labour at {labour_pct:.1f}% (>38%). Consider reducing staff or improving revenue."

        if status == "red":
            return f"Red alert: Labour at {labour_pct:.1f}% (33-38%). Monitor closely."

        if status == "amber":
            if staff_count > 5:
                return f"Amber: Labour at {labour_pct:.1f}% with {staff_count} staff. Consider early finish for lower-paid roles."

        return None
