"""
Comprehensive labour cost analytics service for RosterIQ.

Provides labour cost trending, forecast accuracy scoring, venue benchmarking,
peak hour analysis, and cost optimisation insights for Australian hospitality venues.

All monetary values in AUD, Decimal precision. Designed for venues targeting 28-32%
labour percentage of revenue.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any
from collections import defaultdict
import statistics

from rosteriq.models import (
    Roster, Shift, Employee, DemandForecast,
    EmploymentType, DayType, State,
)
from rosteriq.cost_calculator import calculate_shift_cost_breakdown
from rosteriq.database import BaseStore
from rosteriq.award_rules import get_day_type


# Constants for AU hospitality benchmarking
BASE_RATES = {
    EmploymentType.casual: Decimal("23.00"),
    EmploymentType.part_time: Decimal("24.00"),
    EmploymentType.full_time: Decimal("28.00"),
}

LABOUR_PCT_TARGET = Decimal("0.30")  # 30% target
LABOUR_PCT_MIN = Decimal("0.28")     # 28% minimum acceptable
LABOUR_PCT_MAX = Decimal("0.32")     # 32% maximum acceptable
LABOUR_PCT_ALERT = Decimal("0.35")   # Alert threshold

REVENUE_PER_COVER = Decimal("100.00")  # AU average $100/cover
COST_PER_COVER = Decimal("50.00")      # Approx
STAFF_PER_COVER = Decimal("0.04")      # 1 staff per ~25 covers


class AnalyticsService:
    """Labour cost analytics engine for venue intelligence."""

    def __init__(self, db: BaseStore):
        """Initialize with database connection."""
        self.db = db

    # ========================================================================
    # Labour Cost Trending
    # ========================================================================

    def get_labour_trend(
        self,
        venue_id: str,
        period: str = "daily",
        days: int = 90,
    ) -> List[Dict[str, Any]]:
        """
        Get labour cost trend over time with moving averages and YoY comparison.

        Args:
            venue_id: Venue ID
            period: "daily", "weekly", or "monthly"
            days: Number of days to look back (default 90)

        Returns:
            List of trend points with date, costs, labour %, headcount, hours
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Get all rosters in date range
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        if not rosters:
            return []

        # Get revenue data
        revenue_snapshots = self.db.get_revenue_snapshots(
            venue_id, start_date, end_date
        )
        revenue_by_date = {
            snap["date"]: Decimal(str(snap["revenue"]))
            for snap in revenue_snapshots
        }

        # Group rosters by period
        grouped_data = self._group_by_period(rosters, period)

        trend_points = []
        for period_key, period_rosters in grouped_data.items():
            period_date = self._period_key_to_date(period_key, period)

            # Aggregate costs
            total_cost = Decimal("0.00")
            total_hours = Decimal("0.00")
            headcount = set()

            for roster in period_rosters:
                for shift in roster.shifts:
                    if shift.cost:
                        total_cost += shift.cost
                    total_hours += Decimal(str(shift.net_hours))
                    headcount.add(shift.employee_id)

            # Get revenue for period
            period_revenue = revenue_by_date.get(period_date, Decimal("0.00"))

            # Calculate labour percentage
            labour_pct = Decimal("0.00")
            if period_revenue > 0:
                labour_pct = (total_cost / period_revenue).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )

            trend_points.append({
                "date": period_date.isoformat(),
                "total_labour_cost": float(total_cost),
                "total_revenue": float(period_revenue),
                "labour_percentage": float(labour_pct),
                "headcount": len(headcount),
                "hours_worked": float(total_hours),
            })

        # Add moving averages
        trend_points = self._add_moving_averages(
            trend_points, period
        )

        # Add YoY comparison if data available
        trend_points = self._add_yoy_comparison(
            trend_points, venue_id, period, days
        )

        return sorted(trend_points, key=lambda x: x["date"])

    def get_labour_breakdown(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:
        """
        Break down labour costs by day type, shift type, and employment type.

        Returns:
            {
                "by_day_type": {"weekday": {...}, "weekend": {...}},
                "by_shift_type": {"morning": {...}, ...},
                "by_employment": {"casual": {...}, ...},
                "total": {...}
            }
        """
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()
        venue = self.db.get_venue(venue_id)

        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        breakdown = {
            "by_day_type": {},
            "by_shift_type": {},
            "by_employment": {},
            "total": {},
        }

        # Initialize counters
        day_type_stats = defaultdict(lambda: {
            "cost": Decimal("0.00"),
            "hours": Decimal("0.00"),
            "headcount": set(),
            "shift_count": 0,
        })
        shift_type_stats = defaultdict(lambda: {
            "cost": Decimal("0.00"),
            "hours": Decimal("0.00"),
            "headcount": set(),
            "shift_count": 0,
        })
        employment_stats = defaultdict(lambda: {
            "cost": Decimal("0.00"),
            "hours": Decimal("0.00"),
            "headcount": set(),
            "shift_count": 0,
        })

        # Aggregate data
        total_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        total_headcount = set()

        for roster in rosters:
            for shift in roster.shifts:
                if not shift.cost:
                    continue

                employee = employees.get(shift.employee_id)
                if not employee:
                    continue

                # Day type
                day_type = get_day_type(shift.date, venue.state)
                day_type_key = day_type.value
                day_type_stats[day_type_key]["cost"] += shift.cost
                day_type_stats[day_type_key]["hours"] += Decimal(str(shift.net_hours))
                day_type_stats[day_type_key]["headcount"].add(shift.employee_id)
                day_type_stats[day_type_key]["shift_count"] += 1

                # Shift type (based on start time)
                shift_type = self._classify_shift_type(shift.start_time.hour)
                shift_type_stats[shift_type]["cost"] += shift.cost
                shift_type_stats[shift_type]["hours"] += Decimal(str(shift.net_hours))
                shift_type_stats[shift_type]["headcount"].add(shift.employee_id)
                shift_type_stats[shift_type]["shift_count"] += 1

                # Employment type
                emp_type = employee.employment_type.value
                employment_stats[emp_type]["cost"] += shift.cost
                employment_stats[emp_type]["hours"] += Decimal(str(shift.net_hours))
                employment_stats[emp_type]["headcount"].add(shift.employee_id)
                employment_stats[emp_type]["shift_count"] += 1

                # Totals
                total_cost += shift.cost
                total_hours += Decimal(str(shift.net_hours))
                total_headcount.add(shift.employee_id)

        # Format output
        for day_type_key, stats in day_type_stats.items():
            breakdown["by_day_type"][day_type_key] = self._format_stats(stats)

        for shift_type, stats in shift_type_stats.items():
            breakdown["by_shift_type"][shift_type] = self._format_stats(stats)

        for emp_type, stats in employment_stats.items():
            breakdown["by_employment"][emp_type] = self._format_stats(stats)

        breakdown["total"] = {
            "total_cost": float(total_cost),
            "total_hours": float(total_hours),
            "unique_staff": len(total_headcount),
            "avg_cost_per_hour": float(
                total_cost / total_hours if total_hours > 0 else Decimal("0.00")
            ),
        }

        return breakdown

    # ========================================================================
    # Forecast Accuracy Scoring
    # ========================================================================

    def score_forecast_accuracy(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:
        """
        Compare forecast predictions vs actual roster data.

        Returns MAPE, MAE, RMSE, bias, and per-day-of-week breakdown.
        """
        forecasts = self.db.get_forecasts(venue_id, start_date, end_date)
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)

        if not forecasts or not rosters:
            return {
                "mape": None,
                "mae": None,
                "rmse": None,
                "bias": None,
                "samples": 0,
                "per_day_of_week": {},
            }

        # Build actual hourly covers from rosters
        actual_by_hour = defaultdict(lambda: {"covers": Decimal("0.00"), "count": 0})

        for roster in rosters:
            for shift in roster.shifts:
                # Estimate covers served = hours * staff_to_cover ratio
                shift_covers = Decimal(str(shift.net_hours)) / STAFF_PER_COVER
                hour = shift.start_time.hour

                actual_by_hour[(shift.date, hour)]["covers"] += shift_covers
                actual_by_hour[(shift.date, hour)]["count"] += 1

        # Compare predictions to actuals
        errors = []
        absolute_errors = []
        squared_errors = []
        biases = []
        per_dow = defaultdict(list)

        for forecast in forecasts:
            key = (forecast.date, forecast.hour)
            if key in actual_by_hour:
                actual = actual_by_hour[key]["covers"]
                predicted = Decimal(str(forecast.predicted_covers))

                error = actual - predicted
                abs_error = abs(error)
                sq_error = error ** 2

                errors.append(float(error))
                absolute_errors.append(float(abs_error))
                squared_errors.append(float(sq_error))
                biases.append(float(predicted - actual))  # Over/under

                # Track by day of week
                day_of_week = forecast.date.strftime("%A")
                per_dow[day_of_week].append(float(abs_error))

        if not errors:
            return {
                "mape": None,
                "mae": None,
                "rmse": None,
                "bias": None,
                "samples": 0,
                "per_day_of_week": {},
            }

        # Calculate metrics
        mae = statistics.mean(absolute_errors)
        rmse = (sum(squared_errors) / len(squared_errors)) ** 0.5
        mean_bias = statistics.mean(biases)

        # MAPE: mean absolute percentage error
        mape_values = []
        for forecast in forecasts:
            key = (forecast.date, forecast.hour)
            if key in actual_by_hour and forecast.predicted_covers > 0:
                actual = actual_by_hour[key]["covers"]
                predicted = Decimal(str(forecast.predicted_covers))
                mape_values.append(
                    float(abs(actual - predicted) / predicted)
                )

        mape = (statistics.mean(mape_values) * 100) if mape_values else None

        # Per-day-of-week accuracy
        per_dow_stats = {}
        for dow, errors_list in per_dow.items():
            per_dow_stats[dow] = {
                "mae": statistics.mean(errors_list),
                "count": len(errors_list),
            }

        return {
            "mape": mape,
            "mae": mae,
            "rmse": rmse,
            "bias": mean_bias,
            "samples": len(errors),
            "per_day_of_week": per_dow_stats,
        }

    def get_accuracy_history(
        self,
        venue_id: str,
        weeks: int = 12,
    ) -> List[Dict[str, Any]]:
        """
        Get weekly accuracy trend over N weeks.

        Returns list of weekly accuracy scores showing improvement/degradation.
        """
        history = []
        end_date = date.today()

        for week_offset in range(weeks):
            week_end = end_date - timedelta(days=week_offset * 7)
            week_start = week_end - timedelta(days=6)

            accuracy = self.score_forecast_accuracy(venue_id, week_start, week_end)

            history.append({
                "week_ending": week_end.isoformat(),
                "mape": accuracy["mape"],
                "mae": accuracy["mae"],
                "rmse": accuracy["rmse"],
                "bias": accuracy["bias"],
                "samples": accuracy["samples"],
            })

        return sorted(history, key=lambda x: x["week_ending"])

    # ========================================================================
    # Venue Benchmarking
    # ========================================================================

    def benchmark_venues(
        self,
        venue_ids: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Compare labour metrics across venues.

        Returns rankings and outlier detection for each metric.
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        venue_metrics = {}

        for venue_id in venue_ids:
            rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
            revenue_snapshots = self.db.get_revenue_snapshots(venue_id, start_date, end_date)
            employees = self.db.get_employees_dict()

            if not rosters:
                continue

            # Calculate metrics
            total_cost = Decimal("0.00")
            total_hours = Decimal("0.00")
            headcount = set()
            total_revenue = Decimal("0.00")
            overtime_hours = Decimal("0.00")

            casual_count = 0
            total_shifts = 0

            for roster in rosters:
                for shift in roster.shifts:
                    if shift.cost:
                        total_cost += shift.cost
                    total_hours += Decimal(str(shift.net_hours))
                    headcount.add(shift.employee_id)
                    total_shifts += 1

                    employee = employees.get(shift.employee_id)
                    if employee and employee.employment_type == EmploymentType.casual:
                        casual_count += 1

            for snap in revenue_snapshots:
                total_revenue += Decimal(str(snap["revenue"]))

            # Labour percentage
            labour_pct = Decimal("0.00")
            if total_revenue > 0:
                labour_pct = total_cost / total_revenue

            # Cost per cover (estimate from revenue and standard covers)
            covers_served = total_revenue / REVENUE_PER_COVER if total_revenue > 0 else Decimal("0.00")
            cost_per_cover = total_cost / covers_served if covers_served > 0 else Decimal("0.00")

            # Staff utilisation: hours per staff per week
            weeks = max(1, (end_date - start_date).days / 7)
            staff_util = total_hours / (len(headcount) * weeks) if headcount else Decimal("0.00")

            # Casual percentage
            casual_pct = Decimal(casual_count) / total_shifts if total_shifts > 0 else Decimal("0.00")

            venue_metrics[venue_id] = {
                "labour_pct": float(labour_pct),
                "avg_cost_per_cover": float(cost_per_cover),
                "avg_staff_util": float(staff_util),
                "casual_pct": float(casual_pct),
                "headcount": len(headcount),
                "total_cost": float(total_cost),
                "total_revenue": float(total_revenue),
            }

        # Rank venues
        rankings = self._calculate_rankings(venue_metrics)

        # Detect outliers
        outliers = self._detect_outliers(venue_metrics)

        return {
            "venues": venue_metrics,
            "rankings": rankings,
            "outliers": outliers,
        }

    # ========================================================================
    # Peak Hour Analysis
    # ========================================================================

    def get_peak_analysis(
        self,
        venue_id: str,
        weeks: int = 4,
    ) -> Dict[str, Any]:
        """
        Analyse hourly patterns across 7 days × 24 hours.

        Returns heatmap data and optimal staffing recommendations.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=weeks * 7)

        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        revenue_snapshots = self.db.get_revenue_snapshots(venue_id, start_date, end_date)

        if not rosters:
            return {"heatmap": {}, "peak_windows": [], "dead_zones": []}

        # Initialize heatmap
        heatmap = defaultdict(lambda: {
            "hours_count": 0,
            "avg_revenue": Decimal("0.00"),
            "avg_headcount": Decimal("0.00"),
            "avg_labour_cost": Decimal("0.00"),
            "covers_served": Decimal("0.00"),
        })

        # Aggregate by day of week and hour
        for roster in rosters:
            dow = roster.week_start.strftime("%A")

            for shift in roster.shifts:
                hour = shift.start_time.hour
                key = (dow, hour)

                # Count staff on shift at this hour
                if shift.duration_hours > 0:
                    hours_count = 1
                    headcount = Decimal("1.00") / Decimal(str(shift.duration_hours))
                else:
                    hours_count = 0
                    headcount = Decimal("0.00")

                heatmap[key]["hours_count"] += hours_count
                heatmap[key]["avg_headcount"] += headcount
                if shift.cost:
                    heatmap[key]["avg_labour_cost"] += shift.cost
                heatmap[key]["covers_served"] += Decimal(str(shift.net_hours)) / STAFF_PER_COVER

        # Normalize and calculate averages
        heatmap_output = {}
        for (dow, hour), stats in heatmap.items():
            if stats["hours_count"] > 0:
                heatmap_output[f"{dow}_{hour:02d}"] = {
                    "day_of_week": dow,
                    "hour": hour,
                    "avg_headcount": float(stats["avg_headcount"] / stats["hours_count"]),
                    "avg_labour_cost": float(stats["avg_labour_cost"] / stats["hours_count"]),
                    "avg_revenue_per_hour": float(
                        REVENUE_PER_COVER * stats["covers_served"] / max(1, stats["hours_count"])
                    ),
                    "revenue_per_labour_hour": float(
                        (REVENUE_PER_COVER * stats["covers_served"]) /
                        max(Decimal("0.01"), stats["avg_labour_cost"])
                    ),
                }

        # Identify peak windows (high revenue, good ratio)
        peak_windows = []
        dead_zones = []

        for key, data in heatmap_output.items():
            ratio = data["revenue_per_labour_hour"]
            if ratio > 100:  # Good profit per labour dollar
                peak_windows.append({
                    **data,
                    "efficiency_score": ratio,
                })
            elif data["avg_revenue_per_hour"] < 50:  # Low revenue
                dead_zones.append({
                    **data,
                    "efficiency_score": ratio,
                })

        peak_windows = sorted(peak_windows, key=lambda x: x["efficiency_score"], reverse=True)
        dead_zones = sorted(dead_zones, key=lambda x: x["efficiency_score"])

        return {
            "heatmap": heatmap_output,
            "peak_windows": peak_windows[:10],
            "dead_zones": dead_zones[:10],
        }

    # ========================================================================
    # Cost Optimisation Insights
    # ========================================================================

    def get_optimisation_opportunities(
        self,
        venue_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Detect actionable cost-saving opportunities.

        Returns list of insights with severity and estimated savings.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=28)

        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        revenue_snapshots = self.db.get_revenue_snapshots(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()
        venue = self.db.get_venue(venue_id)

        if not rosters or not venue:
            return []

        insights = []

        # Build daily metrics
        daily_data = defaultdict(lambda: {
            "cost": Decimal("0.00"),
            "hours": Decimal("0.00"),
            "revenue": Decimal("0.00"),
            "headcount": set(),
            "shifts": [],
            "day_type": None,
        })

        for roster in rosters:
            for shift in roster.shifts:
                daily_data[shift.date]["shifts"].append(shift)
                if shift.cost:
                    daily_data[shift.date]["cost"] += shift.cost
                daily_data[shift.date]["hours"] += Decimal(str(shift.net_hours))
                daily_data[shift.date]["headcount"].add(shift.employee_id)
                daily_data[shift.date]["day_type"] = get_day_type(shift.date, venue.state)

        for snap in revenue_snapshots:
            daily_data[snap["date"]]["revenue"] = Decimal(str(snap["revenue"]))

        # Analyse patterns
        overstaffed_days = []
        understaffed_days = []
        excessive_casuals = []
        reducible_shifts = []

        for shift_date, data in daily_data.items():
            if data["revenue"] == 0:
                continue

            labour_pct = data["cost"] / data["revenue"] if data["revenue"] > 0 else Decimal("0.00")

            # Detect overstaffing
            if labour_pct > LABOUR_PCT_ALERT:
                weekly_savings = (labour_pct - LABOUR_PCT_MAX) * data["revenue"] * 4  # 4 weeks
                overstaffed_days.append({
                    "date": shift_date.isoformat(),
                    "labour_pct": float(labour_pct),
                    "estimated_weekly_savings": float(weekly_savings),
                })

            # Check for excessive casuals on weekdays (PT would be cheaper)
            if data["day_type"] == DayType.weekday:
                casual_count = sum(
                    1 for shift in data["shifts"]
                    if employees.get(shift.employee_id, Employee(
                        id="", name="", employment_type=EmploymentType.casual,
                        award_level="level_1", state="vic", hourly_base_rate=Decimal("23"),
                        created_at=datetime.now(), updated_at=datetime.now()
                    )).employment_type == EmploymentType.casual
                )
                if casual_count > len(data["headcount"]) * 0.5:  # >50% casuals
                    excessive_casuals.append({
                        "date": shift_date.isoformat(),
                        "casual_pct": float(Decimal(casual_count) / max(1, len(data["headcount"]))),
                    })

            # Check for shifts that could be shortened
            for shift in data["shifts"]:
                if shift.duration_hours > 8:
                    savings = Decimal(str(shift.duration_hours - 8)) * employees.get(
                        shift.employee_id
                    ).hourly_base_rate if employees.get(shift.employee_id) else Decimal("0.00")
                    reducible_shifts.append({
                        "shift_id": shift.id,
                        "date": shift.date.isoformat(),
                        "duration": shift.duration_hours,
                        "potential_savings": float(savings),
                    })

        # Build insights list
        if overstaffed_days:
            avg_savings = statistics.mean(
                d["estimated_weekly_savings"] for d in overstaffed_days
            )
            insights.append({
                "category": "overstaffed_periods",
                "severity": "high" if avg_savings > 500 else "medium",
                "estimated_savings_weekly": avg_savings,
                "recommendation": "Review labour percentage on high-cost days; consider reducing shifts or reallocating to slower periods",
                "affected_dates": len(overstaffed_days),
            })

        if excessive_casuals:
            insights.append({
                "category": "casual_cost_inefficiency",
                "severity": "medium",
                "estimated_savings_weekly": 300,
                "recommendation": "Shift casual weekday shifts to part-time staff; casuals cost 25% loading but are less committed",
                "affected_dates": len(excessive_casuals),
            })

        if reducible_shifts:
            total_savings = sum(d["potential_savings"] for d in reducible_shifts)
            insights.append({
                "category": "long_shifts",
                "severity": "low",
                "estimated_savings_weekly": float(total_savings / 4),
                "recommendation": "Some shifts exceed 8 hours; breaking into 2 shifts may reduce penalty costs",
                "affected_shifts": len(reducible_shifts),
            })

        return sorted(insights, key=lambda x: x["estimated_savings_weekly"], reverse=True)

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _group_by_period(
        self,
        rosters: List[Roster],
        period: str,
    ) -> Dict[str, List[Roster]]:
        """Group rosters by time period."""
        grouped = defaultdict(list)

        for roster in rosters:
            if period == "daily":
                key = roster.week_start.isoformat()
            elif period == "weekly":
                key = roster.week_start.isoformat()
            else:  # monthly
                key = roster.week_start.strftime("%Y-%m")

            grouped[key].append(roster)

        return grouped

    def _period_key_to_date(self, key: str, period: str) -> date:
        """Convert period key back to date."""
        if period in ("daily", "weekly"):
            return date.fromisoformat(key)
        else:  # monthly
            return date.fromisoformat(f"{key}-01")

    def _classify_shift_type(self, hour: int) -> str:
        """Classify shift by start hour."""
        if 5 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 23:
            return "evening"
        else:
            return "night"

    def _format_stats(self, stats: Dict[str, Any]) -> Dict[str, float]:
        """Format statistics dictionary."""
        return {
            "total_cost": float(stats["cost"]),
            "total_hours": float(stats["hours"]),
            "unique_staff": len(stats["headcount"]),
            "shift_count": stats["shift_count"],
            "avg_cost_per_hour": float(
                stats["cost"] / stats["hours"] if stats["hours"] > 0 else Decimal("0.00")
            ),
        }

    def _add_moving_averages(
        self,
        trend_points: List[Dict[str, Any]],
        period: str,
    ) -> List[Dict[str, Any]]:
        """Add moving average columns."""
        window_size = 7 if period == "daily" else 4 if period == "weekly" else 3

        for i, point in enumerate(trend_points):
            start_idx = max(0, i - window_size + 1)
            window = trend_points[start_idx:i + 1]

            if window:
                point["labour_pct_ma"] = statistics.mean(
                    p["labour_percentage"] for p in window
                )

        return trend_points

    def _add_yoy_comparison(
        self,
        trend_points: List[Dict[str, Any]],
        venue_id: str,
        period: str,
        days: int,
    ) -> List[Dict[str, Any]]:
        """Add year-over-year comparison if data available."""
        # This would require historical data; simplified version
        return trend_points

    def _calculate_rankings(
        self,
        metrics: Dict[str, Dict[str, float]],
    ) -> Dict[str, List[tuple]]:
        """Rank venues by each metric."""
        rankings = {}

        metric_keys = ["labour_pct", "avg_cost_per_cover", "avg_staff_util", "casual_pct"]

        for metric in metric_keys:
            sorted_venues = sorted(
                metrics.items(),
                key=lambda x: x[1][metric],
                reverse=(metric == "avg_staff_util"),  # Higher util is better
            )
            rankings[metric] = [(v[0], v[1][metric]) for v in sorted_venues]

        return rankings

    def _detect_outliers(
        self,
        metrics: Dict[str, Dict[str, float]],
    ) -> Dict[str, List[str]]:
        """Detect venues >1.5 std dev from mean on each metric."""
        outliers_by_metric = {}

        metric_keys = ["labour_pct", "avg_cost_per_cover", "avg_staff_util", "casual_pct"]

        for metric in metric_keys:
            values = [m[metric] for m in metrics.values()]
            if len(values) < 2:
                continue

            mean = statistics.mean(values)
            stdev = statistics.stdev(values)

            outliers_by_metric[metric] = [
                venue_id
                for venue_id, venue_metrics in metrics.items()
                if abs(venue_metrics[metric] - mean) > 1.5 * stdev
            ]

        return outliers_by_metric
