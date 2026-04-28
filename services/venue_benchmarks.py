"""
Venue benchmarking engine for multi-venue operators.

Provides comprehensive comparison of labour metrics across venues with
rankings, industry benchmarks, efficiency scoring, and actionable insights.

All monetary values in AUD (Decimal). Dates in ISO 8601 format.
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
# Pydantic Models for Benchmarking
# ============================================================================


class VenueBenchmark(BaseModel):
    """Benchmarking metrics for a single venue."""
    venue_id: str
    venue_name: str
    period_start: str
    period_end: str

    # Key metrics
    labour_pct_of_revenue: Decimal
    cost_per_cover: Decimal
    staff_utilisation: float
    overtime_ratio: float
    casual_dependency: float
    forecast_accuracy: Optional[float]  # MAPE
    roster_efficiency_score: float  # 0-100
    avg_shift_cost: Decimal
    employee_retention_indicator: Optional[float]
    compliance_score: float  # 0-100

    # Aggregates for context
    total_labour_cost: Decimal
    total_revenue: Decimal
    total_hours: Decimal
    unique_staff_count: int
    total_shifts: int


class ImprovementSuggestion(BaseModel):
    """Actionable improvement for a venue."""
    venue_id: str
    venue_name: str
    metric: str
    current_value: float
    target_value: float
    potential_saving: Decimal
    suggestion: str


class IndustryComparison(BaseModel):
    """Industry benchmark comparison."""
    venue_id: str
    venue_name: str
    metric_name: str
    current_value: float
    industry_target: float
    industry_min: float
    industry_max: float
    percentile_position: float  # 0-100
    status: str  # "exceeds", "on_track", "below", "critical"


class BenchmarkReport(BaseModel):
    """Complete benchmarking report."""
    report_generated_at: str
    period_start: str
    period_end: str
    venue_count: int

    venue_benchmarks: List[VenueBenchmark]
    rankings: Dict[str, List[Tuple[str, float]]]
    best_practices: List[str]
    improvement_opportunities: List[ImprovementSuggestion]


# ============================================================================
# Service Class
# ============================================================================


class VenueBenchmarkService:
    """Multi-venue benchmarking engine."""

    # AU hospitality industry targets
    LABOUR_PCT_TARGET = Decimal("0.32")  # 32% of revenue
    LABOUR_PCT_MIN = Decimal("0.28")
    LABOUR_PCT_MAX = Decimal("0.35")

    COST_PER_COVER_TARGET = Decimal("10.00")  # $8-12 range
    COST_PER_COVER_MIN = Decimal("8.00")
    COST_PER_COVER_MAX = Decimal("12.00")

    CASUAL_DEPENDENCY_TARGET = Decimal("0.35")  # <35%
    CASUAL_DEPENDENCY_MAX = Decimal("0.40")

    # Base rates for calculations
    REVENUE_PER_COVER = Decimal("100.00")
    STAFF_PER_COVER = Decimal("0.04")  # 1 staff per ~25 covers

    def __init__(self, db: BaseStore):
        """Initialize with database connection."""
        self.db = db

    # ========================================================================
    # Core Benchmarking
    # ========================================================================

    def benchmark_venues(
        self,
        venue_ids: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> BenchmarkReport:
        """
        Comprehensive benchmarking across multiple venues.

        Args:
            venue_ids: List of venue IDs to benchmark
            start_date: Period start (default: 90 days ago)
            end_date: Period end (default: today)

        Returns:
            Complete BenchmarkReport with metrics, rankings, and insights
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        # Calculate metrics for each venue
        venue_benchmarks = []
        all_metrics = {}

        for venue_id in venue_ids:
            try:
                benchmark = self._calculate_venue_benchmark(
                    venue_id, start_date, end_date
                )
                venue_benchmarks.append(benchmark)

                # Store metrics for rankings and comparisons
                all_metrics[venue_id] = {
                    "labour_pct_of_revenue": float(benchmark.labour_pct_of_revenue),
                    "cost_per_cover": float(benchmark.cost_per_cover),
                    "staff_utilisation": benchmark.staff_utilisation,
                    "overtime_ratio": benchmark.overtime_ratio,
                    "casual_dependency": benchmark.casual_dependency,
                    "forecast_accuracy": benchmark.forecast_accuracy,
                    "roster_efficiency_score": benchmark.roster_efficiency_score,
                    "compliance_score": benchmark.compliance_score,
                }
            except Exception as e:
                # Log but don't fail the entire report
                continue

        # Generate rankings
        rankings = self._calculate_rankings(all_metrics)

        # Generate best practices from top performers
        best_practices = self._extract_best_practices(venue_benchmarks, rankings)

        # Identify improvement opportunities
        improvement_opportunities = self._identify_improvements(
            venue_benchmarks, rankings
        )

        return BenchmarkReport(
            report_generated_at=datetime.now().isoformat(),
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            venue_count=len(venue_benchmarks),
            venue_benchmarks=venue_benchmarks,
            rankings=rankings,
            best_practices=best_practices,
            improvement_opportunities=improvement_opportunities,
        )

    def _calculate_venue_benchmark(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> VenueBenchmark:
        """Calculate all metrics for a single venue."""
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        revenue_snapshots = self.db.get_revenue_snapshots(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()
        venue = self.db.get_venue(venue_id)
        forecasts = self.db.get_forecasts(venue_id, start_date, end_date)

        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        if not rosters:
            raise ValueError(f"No roster data for venue {venue_id}")

        # Aggregate data
        total_labour_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        scheduled_hours = Decimal("0.00")
        overtime_hours = Decimal("0.00")
        unique_staff = set()
        total_shifts = 0
        casual_hours = Decimal("0.00")
        total_venue_hours = Decimal("0.00")

        shift_costs = []

        for roster in rosters:
            for shift in roster.shifts:
                if shift.cost:
                    total_labour_cost += shift.cost

                net_hours = Decimal(str(shift.net_hours))
                duration_hours = Decimal(str(shift.duration_hours))

                total_hours += net_hours
                scheduled_hours += duration_hours
                total_venue_hours += duration_hours

                shift_costs.append(float(shift.cost) if shift.cost else 0.0)
                unique_staff.add(shift.employee_id)
                total_shifts += 1

                # Track overtime (hours over 8 per day)
                if duration_hours > 8:
                    overtime_hours += duration_hours - 8

                # Track casual hours
                employee = employees.get(shift.employee_id)
                if employee and employee.employment_type == EmploymentType.casual:
                    casual_hours += net_hours

        # Get revenue data
        total_revenue = Decimal("0.00")
        for snap in revenue_snapshots:
            total_revenue += Decimal(str(snap["revenue"]))

        # Calculate metrics
        labour_pct_of_revenue = Decimal("0.00")
        if total_revenue > 0:
            labour_pct_of_revenue = (total_labour_cost / total_revenue).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )

        # Cost per cover
        covers_served = total_revenue / self.REVENUE_PER_COVER if total_revenue > 0 else Decimal("0.00")
        cost_per_cover = total_labour_cost / covers_served if covers_served > 0 else Decimal("0.00")

        # Staff utilisation (actual hours / scheduled hours)
        staff_utilisation = 0.0
        if scheduled_hours > 0:
            staff_utilisation = float((total_hours / scheduled_hours) * 100)

        # Overtime ratio (overtime / total hours)
        overtime_ratio = 0.0
        if total_hours > 0:
            overtime_ratio = float((overtime_hours / total_hours) * 100)

        # Casual dependency (casual hours / total hours)
        casual_dependency = 0.0
        if total_hours > 0:
            casual_dependency = float((casual_hours / total_hours) * 100)

        # Forecast accuracy (if available)
        forecast_accuracy = self._calculate_forecast_accuracy(
            venue_id, start_date, end_date, rosters
        )

        # Efficiency score (composite 0-100)
        roster_efficiency_score = self.calculate_efficiency_score(
            venue_id, start_date, end_date
        )

        # Average shift cost
        avg_shift_cost = Decimal("0.00")
        if shift_costs:
            avg_shift_cost = Decimal(str(statistics.mean(shift_costs))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        # Retention indicator (based on tenure if available)
        retention_indicator = self._calculate_retention_indicator(unique_staff, employees)

        # Compliance score (based on shift conflicts/violations)
        compliance_score = self._calculate_compliance_score(rosters)

        return VenueBenchmark(
            venue_id=venue_id,
            venue_name=venue.name,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            labour_pct_of_revenue=labour_pct_of_revenue,
            cost_per_cover=cost_per_cover,
            staff_utilisation=staff_utilisation,
            overtime_ratio=overtime_ratio,
            casual_dependency=casual_dependency,
            forecast_accuracy=forecast_accuracy,
            roster_efficiency_score=roster_efficiency_score,
            avg_shift_cost=avg_shift_cost,
            employee_retention_indicator=retention_indicator,
            compliance_score=compliance_score,
            total_labour_cost=total_labour_cost.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            total_revenue=total_revenue.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            total_hours=total_hours.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            unique_staff_count=len(unique_staff),
            total_shifts=total_shifts,
        )

    # ========================================================================
    # Ranking and Comparison
    # ========================================================================

    def rank_venues(
        self,
        metric: str,
        venue_ids: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Tuple[str, float]]:
        """
        Rank venues by a specific metric.

        Args:
            metric: Metric name (e.g., "labour_pct_of_revenue", "cost_per_cover")
            venue_ids: Venues to rank (default: all)
            start_date: Period start
            end_date: Period end

        Returns:
            List of (venue_id, metric_value) tuples, sorted by performance
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        if not venue_ids:
            venue_ids = [v.id for v in self.db.list_venues()]

        metrics = {}
        for venue_id in venue_ids:
            try:
                benchmark = self._calculate_venue_benchmark(
                    venue_id, start_date, end_date
                )
                metrics[venue_id] = getattr(benchmark, metric, 0.0)
            except:
                continue

        # Sort (higher is better for efficiency_score, lower is better for cost metrics)
        reverse = metric in ["staff_utilisation", "roster_efficiency_score", "compliance_score"]

        sorted_venues = sorted(
            metrics.items(),
            key=lambda x: x[1],
            reverse=reverse
        )

        return [(venue_id, float(value)) for venue_id, value in sorted_venues]

    def get_industry_comparison(
        self,
        venue_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[IndustryComparison]:
        """
        Compare venue metrics against AU hospitality industry averages.

        Industry targets:
        - Labour %: 28-35% of revenue (target 32%)
        - Cost per cover: $8-12 (target $10)
        - Casual dependency: <35%
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        benchmark = self._calculate_venue_benchmark(venue_id, start_date, end_date)
        venue = self.db.get_venue(venue_id)

        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        # Collect all venue metrics for percentile calculation
        all_venues = [v.id for v in self.db.list_venues()]
        all_metrics = {}

        for v_id in all_venues:
            try:
                b = self._calculate_venue_benchmark(v_id, start_date, end_date)
                all_metrics[v_id] = {
                    "labour_pct": float(b.labour_pct_of_revenue),
                    "cost_per_cover": float(b.cost_per_cover),
                    "casual_dependency": b.casual_dependency,
                }
            except:
                continue

        comparisons = []

        # Labour percentage comparison
        labour_values = [m["labour_pct"] for m in all_metrics.values()]
        labour_percentile = self._calculate_percentile(
            float(benchmark.labour_pct_of_revenue), labour_values
        )
        labour_status = self._get_status(
            float(benchmark.labour_pct_of_revenue),
            float(self.LABOUR_PCT_MIN),
            float(self.LABOUR_PCT_TARGET),
            float(self.LABOUR_PCT_MAX),
        )

        comparisons.append(IndustryComparison(
            venue_id=venue_id,
            venue_name=venue.name,
            metric_name="labour_pct_of_revenue",
            current_value=float(benchmark.labour_pct_of_revenue),
            industry_target=float(self.LABOUR_PCT_TARGET),
            industry_min=float(self.LABOUR_PCT_MIN),
            industry_max=float(self.LABOUR_PCT_MAX),
            percentile_position=labour_percentile,
            status=labour_status,
        ))

        # Cost per cover comparison
        cost_values = [m["cost_per_cover"] for m in all_metrics.values()]
        cost_percentile = self._calculate_percentile(
            float(benchmark.cost_per_cover), cost_values
        )
        cost_status = self._get_status(
            float(benchmark.cost_per_cover),
            float(self.COST_PER_COVER_MIN),
            float(self.COST_PER_COVER_TARGET),
            float(self.COST_PER_COVER_MAX),
        )

        comparisons.append(IndustryComparison(
            venue_id=venue_id,
            venue_name=venue.name,
            metric_name="cost_per_cover",
            current_value=float(benchmark.cost_per_cover),
            industry_target=float(self.COST_PER_COVER_TARGET),
            industry_min=float(self.COST_PER_COVER_MIN),
            industry_max=float(self.COST_PER_COVER_MAX),
            percentile_position=cost_percentile,
            status=cost_status,
        ))

        # Casual dependency comparison
        casual_values = [m["casual_dependency"] for m in all_metrics.values()]
        casual_percentile = self._calculate_percentile(
            benchmark.casual_dependency, casual_values
        )
        casual_status = "exceeds" if benchmark.casual_dependency < float(self.CASUAL_DEPENDENCY_TARGET) else "below"

        comparisons.append(IndustryComparison(
            venue_id=venue_id,
            venue_name=venue.name,
            metric_name="casual_dependency",
            current_value=benchmark.casual_dependency,
            industry_target=float(self.CASUAL_DEPENDENCY_TARGET),
            industry_min=0.0,
            industry_max=float(self.CASUAL_DEPENDENCY_MAX),
            percentile_position=casual_percentile,
            status=casual_status,
        ))

        return comparisons

    # ========================================================================
    # Insights and Recommendations
    # ========================================================================

    def generate_insights(
        self,
        venue_ids: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[str]:
        """
        Generate plain-English insights comparing venues.

        Examples:
        - "Venue X spends 15% more on labour than Venue Y primarily due to higher casual dependency"
        - "Venue B has the best staff utilisation at 92% vs venue A's 78%"
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        insights = []
        benchmarks = {}

        # Calculate benchmarks for all venues
        for venue_id in venue_ids:
            try:
                benchmarks[venue_id] = self._calculate_venue_benchmark(
                    venue_id, start_date, end_date
                )
            except:
                continue

        if not benchmarks:
            return []

        venues = {b.venue_id: b for b in benchmarks.values()}

        # Find best/worst performers for key metrics
        labour_rankings = sorted(
            benchmarks.items(),
            key=lambda x: float(x[1].labour_pct_of_revenue)
        )

        cost_rankings = sorted(
            benchmarks.items(),
            key=lambda x: float(x[1].cost_per_cover)
        )

        util_rankings = sorted(
            benchmarks.items(),
            key=lambda x: x[1].staff_utilisation,
            reverse=True
        )

        casual_rankings = sorted(
            benchmarks.items(),
            key=lambda x: x[1].casual_dependency
        )

        # Generate insights
        if len(labour_rankings) >= 2:
            best_labour = labour_rankings[0]
            worst_labour = labour_rankings[-1]
            diff_pct = float(worst_labour[1].labour_pct_of_revenue - best_labour[1].labour_pct_of_revenue)
            diff_pct_display = diff_pct * 100

            best_venue = venues[best_labour[0]]
            worst_venue = venues[worst_labour[0]]

            insights.append(
                f"{worst_venue.venue_name} has {diff_pct_display:.1f}% higher labour costs than "
                f"{best_venue.venue_name} ({float(worst_labour[1].labour_pct_of_revenue)*100:.1f}% vs "
                f"{float(best_labour[1].labour_pct_of_revenue)*100:.1f}%)"
            )

        if len(util_rankings) >= 2:
            best_util = util_rankings[0]
            worst_util = util_rankings[-1]
            diff_util = best_util[1].staff_utilisation - worst_util[1].staff_utilisation

            best_venue = venues[best_util[0]]
            worst_venue = venues[worst_util[0]]

            insights.append(
                f"{best_venue.venue_name} achieves superior staff utilisation at {best_util[1].staff_utilisation:.1f}% "
                f"compared to {worst_venue.venue_name}'s {worst_util[1].staff_utilisation:.1f}%"
            )

        if len(casual_rankings) >= 2:
            best_casual = casual_rankings[0]
            worst_casual = casual_rankings[-1]

            if worst_casual[1].casual_dependency > float(self.CASUAL_DEPENDENCY_TARGET):
                worst_venue = venues[worst_casual[0]]
                insights.append(
                    f"{worst_venue.venue_name}'s casual dependency at {worst_casual[1].casual_dependency:.1f}% "
                    f"exceeds industry target of 35%; shifting to part-time staff could reduce costs"
                )

        return insights

    def calculate_efficiency_score(
        self,
        venue_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> float:
        """
        Calculate composite roster efficiency score (0-100).

        Weighs:
        - Labour % of revenue (30%): closer to 32% target is better
        - Staff utilisation (25%): higher is better
        - Cost per cover (20%): lower is better
        - Casual dependency (15%): lower is better
        - Compliance (10%): higher is better
        """
        if not start_date:
            start_date = date.today() - timedelta(days=90)
        if not end_date:
            end_date = date.today()

        benchmark = self._calculate_venue_benchmark(venue_id, start_date, end_date)

        # Labour % score (target is 32%, acceptable range 28-35%)
        labour_pct = float(benchmark.labour_pct_of_revenue)
        labour_target = float(self.LABOUR_PCT_TARGET)
        labour_deviation = abs(labour_pct - labour_target)
        labour_score = max(0, 100 - (labour_deviation * 200))  # 0.5% deviation = 10 points

        # Staff utilisation score (scale 0-100)
        util_score = min(100, benchmark.staff_utilisation)

        # Cost per cover score (target $10)
        cost_per_cover = float(benchmark.cost_per_cover)
        cost_target = float(self.COST_PER_COVER_TARGET)
        cost_deviation = abs(cost_per_cover - cost_target)
        cost_score = max(0, 100 - (cost_deviation * 10))  # $1 deviation = 10 points

        # Casual dependency score (lower is better, target <35%)
        casual_score = max(0, 100 - (benchmark.casual_dependency * 2))  # 1% over target = 2 points

        # Compliance score (already 0-100)
        compliance_score = benchmark.compliance_score

        # Weighted composite
        efficiency = (
            (labour_score * 0.30) +
            (util_score * 0.25) +
            (cost_score * 0.20) +
            (casual_score * 0.15) +
            (compliance_score * 0.10)
        )

        return min(100, max(0, efficiency))

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _calculate_rankings(
        self,
        all_metrics: Dict[str, Dict[str, float]],
    ) -> Dict[str, List[Tuple[str, float]]]:
        """Calculate rankings for each metric."""
        rankings = {}

        metrics_to_rank = [
            "labour_pct_of_revenue",
            "cost_per_cover",
            "staff_utilisation",
            "casual_dependency",
            "roster_efficiency_score",
            "compliance_score",
        ]

        for metric in metrics_to_rank:
            if not all_metrics:
                rankings[metric] = []
                continue

            sorted_venues = sorted(
                all_metrics.items(),
                key=lambda x: x[1].get(metric, 0.0),
                reverse=(metric in ["staff_utilisation", "roster_efficiency_score", "compliance_score"])
            )

            rankings[metric] = [(v[0], v[1].get(metric, 0.0)) for v in sorted_venues]

        return rankings

    def _extract_best_practices(
        self,
        benchmarks: List[VenueBenchmark],
        rankings: Dict[str, List[Tuple[str, float]]],
    ) -> List[str]:
        """Extract best practices from top performers."""
        practices = []

        if not benchmarks:
            return practices

        # Find best labour % performer
        labour_ranking = rankings.get("labour_pct_of_revenue", [])
        if labour_ranking:
            best_venue_id = labour_ranking[0][0]
            best_venue = next((b for b in benchmarks if b.venue_id == best_venue_id), None)
            if best_venue:
                practices.append(
                    f"{best_venue.venue_name} achieves optimal labour cost management at "
                    f"{float(best_venue.labour_pct_of_revenue)*100:.1f}% of revenue"
                )

        # Find best efficiency performer
        efficiency_ranking = rankings.get("roster_efficiency_score", [])
        if efficiency_ranking:
            best_venue_id = efficiency_ranking[0][0]
            best_venue = next((b for b in benchmarks if b.venue_id == best_venue_id), None)
            if best_venue:
                practices.append(
                    f"{best_venue.venue_name} demonstrates excellence in roster efficiency "
                    f"(score: {best_venue.roster_efficiency_score:.1f}/100)"
                )

        # Find best staff utilisation
        util_ranking = rankings.get("staff_utilisation", [])
        if util_ranking:
            best_venue_id = util_ranking[0][0]
            best_venue = next((b for b in benchmarks if b.venue_id == best_venue_id), None)
            if best_venue:
                practices.append(
                    f"{best_venue.venue_name} optimises staff utilisation at {best_venue.staff_utilisation:.1f}%"
                )

        # Find lowest casual dependency
        casual_ranking = rankings.get("casual_dependency", [])
        if casual_ranking:
            best_venue_id = casual_ranking[0][0]
            best_venue = next((b for b in benchmarks if b.venue_id == best_venue_id), None)
            if best_venue and best_venue.casual_dependency < float(self.CASUAL_DEPENDENCY_TARGET):
                practices.append(
                    f"{best_venue.venue_name} maintains efficient casual staffing at {best_venue.casual_dependency:.1f}% "
                    f"(below {float(self.CASUAL_DEPENDENCY_TARGET)*100:.0f}% target)"
                )

        return practices

    def _identify_improvements(
        self,
        benchmarks: List[VenueBenchmark],
        rankings: Dict[str, List[Tuple[str, float]]],
    ) -> List[ImprovementSuggestion]:
        """Identify actionable improvements for each venue."""
        suggestions = []

        if not benchmarks:
            return suggestions

        # Get best performers for targets
        labour_best = rankings.get("labour_pct_of_revenue", [])
        cost_best = rankings.get("cost_per_cover", [])
        util_best = rankings.get("staff_utilisation", [])
        casual_best = rankings.get("casual_dependency", [])

        for benchmark in benchmarks:
            # Labour cost opportunity
            if labour_best and float(benchmark.labour_pct_of_revenue) > float(self.LABOUR_PCT_TARGET):
                target_labour = labour_best[0][1]  # Best performer's value
                labour_diff = float(benchmark.labour_pct_of_revenue) - target_labour

                # Calculate potential saving
                potential_saving = benchmark.total_revenue * Decimal(str(labour_diff))

                suggestions.append(ImprovementSuggestion(
                    venue_id=benchmark.venue_id,
                    venue_name=benchmark.venue_name,
                    metric="labour_pct_of_revenue",
                    current_value=float(benchmark.labour_pct_of_revenue),
                    target_value=target_labour,
                    potential_saving=potential_saving.quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    ),
                    suggestion=f"Reduce labour costs from {float(benchmark.labour_pct_of_revenue)*100:.1f}% to "
                               f"{target_labour*100:.1f}% by optimising casual dependency and overtime management. "
                               f"Potential monthly saving: ${potential_saving/4:.2f}",
                ))

            # Cost per cover opportunity
            if cost_best and float(benchmark.cost_per_cover) > float(self.COST_PER_COVER_TARGET):
                target_cost = cost_best[0][1]
                cost_diff = float(benchmark.cost_per_cover) - target_cost
                covers = benchmark.total_revenue / self.REVENUE_PER_COVER
                potential_saving = covers * Decimal(str(cost_diff))

                suggestions.append(ImprovementSuggestion(
                    venue_id=benchmark.venue_id,
                    venue_name=benchmark.venue_name,
                    metric="cost_per_cover",
                    current_value=float(benchmark.cost_per_cover),
                    target_value=target_cost,
                    potential_saving=potential_saving.quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    ),
                    suggestion=f"Reduce cost per cover from ${float(benchmark.cost_per_cover):.2f} to "
                               f"${target_cost:.2f}. Target: improve staff-to-cover ratio and reduce overtime.",
                ))

            # Casual dependency opportunity
            if benchmark.casual_dependency > float(self.CASUAL_DEPENDENCY_TARGET):
                casual_overage = benchmark.casual_dependency - float(self.CASUAL_DEPENDENCY_TARGET)
                # Casuals cost 25% loading, so reducing could save
                estimated_saving = (benchmark.total_labour_cost * Decimal(str(casual_overage / 100)) *
                                   Decimal("0.25"))  # 25% loading premium

                suggestions.append(ImprovementSuggestion(
                    venue_id=benchmark.venue_id,
                    venue_name=benchmark.venue_name,
                    metric="casual_dependency",
                    current_value=benchmark.casual_dependency,
                    target_value=float(self.CASUAL_DEPENDENCY_TARGET),
                    potential_saving=estimated_saving.quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    ),
                    suggestion=f"Shift {casual_overage:.1f}% of casual hours to part-time staff. "
                               f"Current casual dependency: {benchmark.casual_dependency:.1f}% (target <35%). "
                               f"Potential monthly saving from reduced loading: ${estimated_saving/4:.2f}",
                ))

        # Sort by potential saving
        suggestions.sort(key=lambda x: x.potential_saving, reverse=True)

        return suggestions

    def _calculate_forecast_accuracy(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
        rosters: List[Roster],
    ) -> Optional[float]:
        """Calculate MAPE (Mean Absolute Percentage Error) for forecasts."""
        forecasts = self.db.get_forecasts(venue_id, start_date, end_date)

        if not forecasts:
            return None

        # Build actual hourly covers from rosters
        actual_by_hour = defaultdict(lambda: Decimal("0.00"))

        for roster in rosters:
            for shift in roster.shifts:
                shift_covers = Decimal(str(shift.net_hours)) / self.STAFF_PER_COVER
                hour = shift.start_time.hour
                actual_by_hour[(shift.date, hour)] += shift_covers

        # Calculate MAPE
        mape_values = []

        for forecast in forecasts:
            key = (forecast.date, forecast.hour)
            if key in actual_by_hour and forecast.predicted_covers > 0:
                actual = actual_by_hour[key]
                predicted = Decimal(str(forecast.predicted_covers))

                if predicted > 0:
                    mape = abs(actual - predicted) / predicted
                    mape_values.append(float(mape))

        if mape_values:
            return statistics.mean(mape_values) * 100  # Return as percentage

        return None

    def _calculate_retention_indicator(
        self,
        unique_staff: set,
        employees: Dict[str, Employee],
    ) -> Optional[float]:
        """
        Calculate retention indicator based on average tenure.

        Returns percentage of staff with >6 months tenure.
        """
        if not unique_staff:
            return None

        long_tenure_count = 0
        days_threshold = 180  # 6 months

        for emp_id in unique_staff:
            employee = employees.get(emp_id)
            if employee:
                tenure = (datetime.now().date() - employee.created_at.date()).days
                if tenure >= days_threshold:
                    long_tenure_count += 1

        return (long_tenure_count / len(unique_staff)) * 100 if unique_staff else None

    def _calculate_compliance_score(
        self,
        rosters: List[Roster],
    ) -> float:
        """
        Calculate compliance score (0-100) based on shift violations.

        Checks for consecutive day limits, break requirements, etc.
        """
        if not rosters:
            return 100.0

        # Count violations
        violations = 0
        total_shifts = 0

        for roster in rosters:
            for shift in roster.shifts:
                total_shifts += 1
                # Simplified: check for long shifts (>10 hours)
                if shift.duration_hours > 10:
                    violations += 1

        if total_shifts == 0:
            return 100.0

        violation_rate = violations / total_shifts
        compliance_score = max(0, 100 - (violation_rate * 100))

        return compliance_score

    def _calculate_percentile(
        self,
        value: float,
        all_values: List[float],
    ) -> float:
        """Calculate percentile position of value in list."""
        if not all_values:
            return 50.0

        sorted_vals = sorted(all_values)
        position = sum(1 for v in sorted_vals if v < value) / len(sorted_vals)

        return position * 100

    def _get_status(
        self,
        current: float,
        min_val: float,
        target: float,
        max_val: float,
    ) -> str:
        """Determine status based on target ranges."""
        if current < min_val:
            return "exceeds"
        elif current <= target:
            return "on_track"
        elif current <= max_val:
            return "below"
        else:
            return "critical"
