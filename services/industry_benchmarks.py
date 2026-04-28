"""
Industry benchmarking engine for RosterIQ.

Compares venue labour costs against Australian hospitality industry benchmarks
using embedded ABS and industry average data. Provides percentile rankings,
gap analysis, and actionable recommendations.

All monetary values in AUD (Decimal). Percentiles in 0-100 range.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
import statistics

from pydantic import BaseModel

from rosteriq.models import (
    Roster, Shift, Employee, DemandForecast,
    EmploymentType, DayType, State,
)
from rosteriq.database import BaseStore
from rosteriq.award_rules import get_day_type


# ============================================================================
# Embedded Industry Benchmark Data (AU Hospitality 2024-2026)
# ============================================================================
# Source: ABS, industry surveys, hospitality awards


INDUSTRY_BENCHMARKS = {
    "cafe": {
        "labour_pct_range": (0.28, 0.32),
        "labour_pct_target": 0.30,
        "avg_hourly_cost": Decimal("28.00"),
        "covers_per_staff_hour": 18,
        "description": "Café - espresso, takeaway focused",
    },
    "restaurant_casual": {
        "labour_pct_range": (0.30, 0.34),
        "labour_pct_target": 0.32,
        "avg_hourly_cost": Decimal("30.00"),
        "covers_per_staff_hour": 15,
        "description": "Casual Dining Restaurant",
    },
    "restaurant_fine_dining": {
        "labour_pct_range": (0.35, 0.40),
        "labour_pct_target": 0.37,
        "avg_hourly_cost": Decimal("34.00"),
        "covers_per_staff_hour": 10,
        "description": "Fine Dining Restaurant",
    },
    "bar_pub": {
        "labour_pct_range": (0.25, 0.30),
        "labour_pct_target": 0.27,
        "avg_hourly_cost": Decimal("27.00"),
        "covers_per_staff_hour": 20,
        "description": "Bar / Pub",
    },
    "hotel": {
        "labour_pct_range": (0.33, 0.38),
        "labour_pct_target": 0.35,
        "avg_hourly_cost": Decimal("32.00"),
        "covers_per_staff_hour": 12,
        "description": "Hotel (F&B operations)",
    },
    "fast_food_qsr": {
        "labour_pct_range": (0.22, 0.28),
        "labour_pct_target": 0.25,
        "avg_hourly_cost": Decimal("25.00"),
        "covers_per_staff_hour": 25,
        "description": "Fast Food / Quick Service Restaurant",
    },
    "catering": {
        "labour_pct_range": (0.30, 0.35),
        "labour_pct_target": 0.32,
        "avg_hourly_cost": Decimal("29.00"),
        "covers_per_staff_hour": 14,
        "description": "Catering / Events",
    },
}


# ============================================================================
# Pydantic Models for Industry Benchmarking
# ============================================================================


class IndustryBenchmark(BaseModel):
    """Industry benchmark data for a venue type."""
    venue_type: str
    description: str
    labour_pct_min: float
    labour_pct_target: float
    labour_pct_max: float
    avg_hourly_cost: Decimal
    covers_per_staff_hour: int


class BenchmarkComparison(BaseModel):
    """Venue comparison against industry benchmark."""
    venue_id: str
    venue_type: str
    period_start: str
    period_end: str

    # Actual metrics
    actual_labour_pct: float
    benchmark_labour_pct: float
    labour_pct_gap: float  # positive = above benchmark

    actual_cost_per_cover: Decimal
    benchmark_cost_per_cover: Decimal

    staff_utilisation: float
    covers_per_staff_hour_actual: float
    covers_per_staff_hour_benchmark: int

    percentile_rank: float  # 0-100, 100=best performer
    status: str  # "exceeds", "at_target", "below", "critical"
    gap_amount: Decimal  # AUD amount of labour cost above/below benchmark

    total_revenue: Decimal
    total_labour_cost: Decimal


class BenchmarkRecommendation(BaseModel):
    """Actionable recommendation from industry benchmarking."""
    area: str  # "labour_percentage", "cost_per_cover", "staff_utilisation", "casual_dependency"
    current_value: float
    target_value: float
    potential_savings: Decimal
    priority: str  # "high", "medium", "low"
    action: str  # Actionable recommendation


class VenueTypeMapping(BaseModel):
    """Mapping of venue to industry type."""
    venue_id: str
    venue_name: str
    mapped_type: str  # e.g., "restaurant_casual"
    confidence: float  # 0-1, confidence in mapping


# ============================================================================
# Service Class
# ============================================================================


class IndustryBenchmarkService:
    """Industry benchmarking engine for venue labour cost analysis."""

    def __init__(self, db: BaseStore):
        """Initialize with database connection."""
        self.db = db

    # ========================================================================
    # Core Benchmarking Methods
    # ========================================================================

    def compare_venue(
        self,
        venue_id: str,
        venue_type: str,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> BenchmarkComparison:
        """
        Compare a venue's labour costs against industry benchmark.

        Args:
            venue_id: Venue ID
            venue_type: One of keys in INDUSTRY_BENCHMARKS (e.g., "restaurant_casual")
            period_start: Start date (ISO format), default 90 days ago
            period_end: End date (ISO format), default today

        Returns:
            BenchmarkComparison with gap analysis and percentile rank
        """
        if venue_type not in INDUSTRY_BENCHMARKS:
            raise ValueError(
                f"Unknown venue type '{venue_type}'. "
                f"Valid types: {list(INDUSTRY_BENCHMARKS.keys())}"
            )

        # Parse dates
        if period_start:
            start_date = date.fromisoformat(period_start)
        else:
            start_date = date.today() - timedelta(days=90)

        if period_end:
            end_date = date.fromisoformat(period_end)
        else:
            end_date = date.today()

        # Get venue and rosters
        venue = self.db.get_venue(venue_id)
        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        revenue_snapshots = self.db.get_revenue_snapshots(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()

        # Calculate actual metrics
        total_labour_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        scheduled_hours = Decimal("0.00")
        unique_staff = set()
        total_revenue = Decimal("0.00")

        for roster in rosters:
            for shift in roster.shifts:
                if shift.cost:
                    total_labour_cost += shift.cost

                net_hours = Decimal(str(shift.net_hours))
                duration_hours = Decimal(str(shift.duration_hours))
                total_hours += net_hours
                scheduled_hours += duration_hours
                unique_staff.add(shift.employee_id)

        for snap in revenue_snapshots:
            total_revenue += Decimal(str(snap["revenue"]))

        # Calculate actual labour percentage
        actual_labour_pct = 0.0
        if total_revenue > 0:
            actual_labour_pct = float((total_labour_cost / total_revenue))

        # Get industry benchmark for this type
        benchmark_data = INDUSTRY_BENCHMARKS[venue_type]
        benchmark_labour_pct = benchmark_data["labour_pct_target"]
        labour_pct_min, labour_pct_max = benchmark_data["labour_pct_range"]

        # Calculate gap
        labour_pct_gap = actual_labour_pct - benchmark_labour_pct
        gap_amount = total_revenue * Decimal(str(labour_pct_gap))

        # Calculate cost per cover
        revenue_per_cover = Decimal("100.00")  # AU average
        covers_served = total_revenue / revenue_per_cover if total_revenue > 0 else Decimal("0.00")
        actual_cost_per_cover = (
            total_labour_cost / covers_served if covers_served > 0 else Decimal("0.00")
        )

        benchmark_cost_per_cover = (
            benchmark_data["avg_hourly_cost"] / benchmark_data["covers_per_staff_hour"]
        )

        # Calculate covers per staff hour
        covers_per_staff_actual = (
            float(covers_served / total_hours) if total_hours > 0 else 0.0
        )

        # Staff utilisation
        staff_utilisation = 0.0
        if scheduled_hours > 0:
            staff_utilisation = float((total_hours / scheduled_hours) * 100)

        # Determine status and percentile
        if actual_labour_pct < labour_pct_min:
            status = "exceeds"
        elif actual_labour_pct <= benchmark_labour_pct:
            status = "at_target"
        elif actual_labour_pct <= labour_pct_max:
            status = "below"
        else:
            status = "critical"

        # Percentile: 100 = best (lowest labour %), 0 = worst (highest labour %)
        percentile_rank = self._calculate_industry_percentile(
            actual_labour_pct, venue_type
        )

        return BenchmarkComparison(
            venue_id=venue_id,
            venue_type=venue_type,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            actual_labour_pct=actual_labour_pct,
            benchmark_labour_pct=benchmark_labour_pct,
            labour_pct_gap=labour_pct_gap,
            actual_cost_per_cover=actual_cost_per_cover.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            benchmark_cost_per_cover=benchmark_cost_per_cover.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            staff_utilisation=staff_utilisation,
            covers_per_staff_hour_actual=covers_per_staff_actual,
            covers_per_staff_hour_benchmark=benchmark_data["covers_per_staff_hour"],
            percentile_rank=percentile_rank,
            status=status,
            gap_amount=gap_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            total_revenue=total_revenue.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            total_labour_cost=total_labour_cost.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
        )

    def get_percentile(
        self,
        venue_id: str,
        venue_type: str,
        metric: str = "labour_pct",
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> float:
        """
        Get percentile ranking (0-100) for a venue metric vs industry.

        Args:
            venue_id: Venue ID
            venue_type: Industry type
            metric: "labour_pct", "cost_per_cover", "covers_per_staff"
            period_start: Period start
            period_end: Period end

        Returns:
            Percentile (0=worst, 100=best)
        """
        comparison = self.compare_venue(venue_id, venue_type, period_start, period_end)

        if metric == "labour_pct":
            return comparison.percentile_rank
        elif metric == "cost_per_cover":
            benchmark_data = INDUSTRY_BENCHMARKS[venue_type]
            min_cost = benchmark_data["avg_hourly_cost"] / benchmark_data["covers_per_staff_hour"]
            # For cost, lower is better, so invert the calculation
            percentile = 100 * (1 - min(float(comparison.actual_cost_per_cover) / float(min_cost), 1.0))
            return percentile
        elif metric == "covers_per_staff":
            benchmark_cph = INDUSTRY_BENCHMARKS[venue_type]["covers_per_staff_hour"]
            # Higher is better, so direct calculation
            percentile = 100 * min(comparison.covers_per_staff_hour_actual / benchmark_cph, 1.0)
            return percentile
        else:
            raise ValueError(f"Unknown metric '{metric}'")

    def get_recommendations(
        self,
        venue_id: str,
        venue_type: str,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> List[BenchmarkRecommendation]:
        """
        Generate actionable recommendations based on industry benchmarking.

        Args:
            venue_id: Venue ID
            venue_type: Industry type
            period_start: Period start
            period_end: Period end

        Returns:
            List of prioritised recommendations
        """
        comparison = self.compare_venue(venue_id, venue_type, period_start, period_end)
        benchmark_data = INDUSTRY_BENCHMARKS[venue_type]
        recommendations = []

        # 1. Labour percentage analysis
        if comparison.labour_pct_gap > 0.02:  # More than 2% above
            savings = comparison.total_revenue * Decimal(str(comparison.labour_pct_gap))
            recommendations.append(BenchmarkRecommendation(
                area="labour_percentage",
                current_value=comparison.actual_labour_pct,
                target_value=benchmark_data["labour_pct_target"],
                potential_savings=savings.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                priority="high",
                action=f"Reduce labour costs from {comparison.actual_labour_pct*100:.1f}% to "
                       f"{benchmark_data['labour_pct_target']*100:.1f}% of revenue. "
                       f"Focus on: optimising staff-to-cover ratio, reducing overtime, "
                       f"reviewing casual dependency. Potential monthly saving: "
                       f"${savings/4:.0f}.",
            ))
        elif comparison.labour_pct_gap > 0:
            savings = comparison.total_revenue * Decimal(str(comparison.labour_pct_gap))
            recommendations.append(BenchmarkRecommendation(
                area="labour_percentage",
                current_value=comparison.actual_labour_pct,
                target_value=benchmark_data["labour_pct_target"],
                potential_savings=savings.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                priority="medium",
                action=f"Fine-tune labour costs: currently {comparison.actual_labour_pct*100:.1f}%, "
                       f"target {benchmark_data['labour_pct_target']*100:.1f}%. "
                       f"Small gains in scheduling efficiency could yield "
                       f"${savings/4:.0f} monthly savings.",
            ))

        # 2. Cost per cover analysis
        if comparison.actual_cost_per_cover > comparison.benchmark_cost_per_cover * Decimal("1.10"):
            cost_diff = comparison.actual_cost_per_cover - comparison.benchmark_cost_per_cover
            covers = comparison.total_revenue / Decimal("100.00")
            savings = covers * cost_diff
            recommendations.append(BenchmarkRecommendation(
                area="cost_per_cover",
                current_value=float(comparison.actual_cost_per_cover),
                target_value=float(comparison.benchmark_cost_per_cover),
                potential_savings=savings.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                priority="high",
                action=f"Improve cost per cover from ${comparison.actual_cost_per_cover} to "
                       f"${comparison.benchmark_cost_per_cover}. Target: achieve "
                       f"{comparison.covers_per_staff_hour_benchmark} covers per staff hour. "
                       f"Potential saving: ${savings/4:.0f} per month.",
            ))

        # 3. Staff utilisation analysis
        if comparison.staff_utilisation < 85.0:
            recommendations.append(BenchmarkRecommendation(
                area="staff_utilisation",
                current_value=comparison.staff_utilisation,
                target_value=90.0,
                potential_savings=Decimal("0.00"),
                priority="medium",
                action=f"Staff utilisation at {comparison.staff_utilisation:.1f}% is below optimal. "
                       f"Review shift patterns to reduce idle time and improve scheduling accuracy. "
                       f"Target: 90%+ utilisation.",
            ))

        # 4. Covers per staff hour analysis
        if comparison.covers_per_staff_hour_actual < comparison.covers_per_staff_hour_benchmark * 0.9:
            shortfall = (comparison.covers_per_staff_hour_benchmark -
                        comparison.covers_per_staff_hour_actual)
            recommendations.append(BenchmarkRecommendation(
                area="covers_per_staff",
                current_value=comparison.covers_per_staff_hour_actual,
                target_value=comparison.covers_per_staff_hour_benchmark,
                potential_savings=Decimal("0.00"),
                priority="medium",
                action=f"Covers per staff hour: {comparison.covers_per_staff_hour_actual:.1f} vs "
                       f"benchmark {comparison.covers_per_staff_hour_benchmark}. "
                       f"Analyse peak vs off-peak demand; adjust scheduling to match demand curves.",
            ))

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda x: priority_order.get(x.priority, 3))

        return recommendations

    def get_benchmarks(self, venue_type: str) -> IndustryBenchmark:
        """
        Get industry benchmark data for a venue type.

        Args:
            venue_type: One of keys in INDUSTRY_BENCHMARKS

        Returns:
            IndustryBenchmark with targets and ranges
        """
        if venue_type not in INDUSTRY_BENCHMARKS:
            raise ValueError(
                f"Unknown venue type '{venue_type}'. "
                f"Valid types: {list(INDUSTRY_BENCHMARKS.keys())}"
            )

        data = INDUSTRY_BENCHMARKS[venue_type]
        min_pct, max_pct = data["labour_pct_range"]

        return IndustryBenchmark(
            venue_type=venue_type,
            description=data["description"],
            labour_pct_min=min_pct,
            labour_pct_target=data["labour_pct_target"],
            labour_pct_max=max_pct,
            avg_hourly_cost=data["avg_hourly_cost"],
            covers_per_staff_hour=data["covers_per_staff_hour"],
        )

    def compare_venues(
        self,
        venue_configs: List[Dict[str, str]],
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
    ) -> List[BenchmarkComparison]:
        """
        Compare multiple venues against industry benchmarks.

        Args:
            venue_configs: List of dicts with "venue_id" and "venue_type" keys
            period_start: Period start (ISO format)
            period_end: Period end (ISO format)

        Returns:
            List of BenchmarkComparison results
        """
        results = []
        for config in venue_configs:
            try:
                comparison = self.compare_venue(
                    config["venue_id"],
                    config["venue_type"],
                    period_start,
                    period_end,
                )
                results.append(comparison)
            except Exception:
                # Skip venues with missing data
                continue

        return results

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _calculate_industry_percentile(
        self,
        actual_labour_pct: float,
        venue_type: str,
    ) -> float:
        """
        Calculate percentile within industry type.

        Assumes normal distribution around benchmark.
        Higher percentile = lower labour % = better performance.
        """
        benchmark_data = INDUSTRY_BENCHMARKS[venue_type]
        min_pct, max_pct = benchmark_data["labour_pct_range"]
        target_pct = benchmark_data["labour_pct_target"]

        # If at or below target, calculate upside percentile (50-100)
        if actual_labour_pct <= target_pct:
            percentile = 50 + (50 * (target_pct - actual_labour_pct) / (target_pct - min_pct))
            return min(100.0, max(50.0, percentile))

        # If above target, calculate downside percentile (0-50)
        else:
            percentile = 50 - (50 * (actual_labour_pct - target_pct) / (max_pct - target_pct))
            return max(0.0, min(50.0, percentile))

    def get_all_venue_types(self) -> List[str]:
        """Get list of all supported venue types."""
        return list(INDUSTRY_BENCHMARKS.keys())

    def get_venue_type_descriptions(self) -> Dict[str, str]:
        """Get descriptions for all venue types."""
        return {
            venue_type: data["description"]
            for venue_type, data in INDUSTRY_BENCHMARKS.items()
        }
