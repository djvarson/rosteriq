"""
Weekly Digest Generator for RosterIQ

Compiles comprehensive venue performance summaries including labour costs,
forecast accuracy, staffing metrics, compliance, and activity summaries.
Generates actionable insights and sends to venue managers via email.

Features:
- Labour cost analysis with budget comparison
- Forecast accuracy metrics (MAPE, bias detection)
- Staffing utilisation and overtime tracking
- Compliance violation detection
- Auto-generated insights
- HTML and JSON rendering
- Multi-venue portfolio digests
"""

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from collections import defaultdict
import statistics
import logging

from rosteriq.database import get_db
from rosteriq.models import (
    Roster, Shift, Employee, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus,
)
from rosteriq.services.analytics import AnalyticsService
from rosteriq.services.notifications import NotificationService
from rosteriq.award_rules import validate_shift_compliance
from rosteriq.cost_calculator import calculate_shift_cost_breakdown, SUPER_RATE

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models for Weekly Digest
# ============================================================================

@dataclass
class LabourCostSection:
    """Labour cost summary for the week."""
    total_cost: Decimal
    budget_cost: Optional[Decimal] = None
    budget_variance: Optional[Decimal] = None  # positive = over budget
    cost_breakdown: Dict[str, Decimal] = field(default_factory=dict)  # base, penalties, loading, super
    cost_per_cover: Decimal = Decimal("0.00")
    total_covers: int = 0
    week_over_week_change_pct: Decimal = Decimal("0.00")
    trend: str = "stable"  # "up", "down", "stable"


@dataclass
class ForecastSection:
    """Forecast accuracy metrics."""
    mape: Decimal  # Mean Absolute Percentage Error
    rmse: Decimal  # Root Mean Square Error
    best_day: str = ""  # e.g., "Monday"
    worst_day: str = ""
    bias: str = "neutral"  # "over", "under", "neutral"
    bias_pct: Decimal = Decimal("0.00")
    predictions_count: int = 0


@dataclass
class StaffingSection:
    """Staffing utilisation metrics."""
    total_hours: Decimal
    unique_headcount: int
    overtime_hours: Decimal
    overtime_cost: Decimal
    casual_ratio: Decimal  # 0.0 to 1.0
    permanent_ratio: Decimal
    avg_shift_length: Decimal
    coverage_gaps_hours: Decimal
    no_shows: int


@dataclass
class ComplianceSection:
    """Compliance violations and status."""
    violations_found: int
    violations_types: Dict[str, int] = field(default_factory=dict)  # break_violation, consecutive_days, etc
    resolution_status: str = "pending"  # "pending", "resolved", "at_risk"
    details: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ActivitySection:
    """Team activity metrics."""
    shift_swaps_completed: int
    bids_placed: int
    bids_resolved: int
    approvals_processed: int
    staff_no_shows: int
    shift_cancellations: int


@dataclass
class WeeklyDigest:
    """Complete weekly digest for a venue."""
    venue_id: str
    venue_name: str
    week_start: date
    week_end: date
    generated_at: datetime

    labour_cost: LabourCostSection
    forecast_accuracy: ForecastSection
    staffing: StaffingSection
    compliance: ComplianceSection
    activity: ActivitySection
    insights: List[str] = field(default_factory=list)
    score: float = 0.0  # 0-100 overall health


@dataclass
class MultiVenueDigest:
    """Portfolio digest for owners with multiple venues."""
    owner_id: str
    week_start: date
    week_end: date
    generated_at: datetime
    venue_digests: List[WeeklyDigest]
    portfolio_totals: Dict[str, Any] = field(default_factory=dict)
    comparison_insights: List[str] = field(default_factory=list)


# ============================================================================
# Weekly Digest Generator
# ============================================================================

class WeeklyDigestGenerator:
    """Generates comprehensive weekly performance digests for venues."""

    def __init__(self):
        """Initialize with database and services."""
        self.db = get_db()
        self.analytics = AnalyticsService(self.db)
        self.notifications = NotificationService()

    def generate_digest(
        self, venue_id: str, week_start: date
    ) -> WeeklyDigest:
        """
        Generate complete weekly digest for a venue.

        Args:
            venue_id: Venue identifier
            week_start: Start date of week (Monday)

        Returns:
            WeeklyDigest with all sections populated
        """
        week_end = week_start + timedelta(days=6)
        venue = self.db.get_venue(venue_id)
        if not venue:
            raise ValueError(f"Venue {venue_id} not found")

        # Compile each section
        labour_cost = self._compile_labour_cost(venue_id, week_start, week_end)
        forecast_accuracy = self._compile_forecast_accuracy(venue_id, week_start, week_end)
        staffing = self._compile_staffing_metrics(venue_id, week_start, week_end)
        compliance = self._compile_compliance(venue_id, week_start, week_end)
        activity = self._compile_activity(venue_id, week_start, week_end)

        # Generate insights
        insights = self._generate_insights(
            labour_cost, forecast_accuracy, staffing, compliance, activity, venue
        )

        # Calculate overall health score
        score = self._calculate_health_score(
            labour_cost, forecast_accuracy, staffing, compliance, activity
        )

        return WeeklyDigest(
            venue_id=venue_id,
            venue_name=venue.name,
            week_start=week_start,
            week_end=week_end,
            generated_at=datetime.now(),
            labour_cost=labour_cost,
            forecast_accuracy=forecast_accuracy,
            staffing=staffing,
            compliance=compliance,
            activity=activity,
            insights=insights,
            score=score,
        )

    # ========================================================================
    # Section Compilation Methods
    # ========================================================================

    def _compile_labour_cost(
        self, venue_id: str, week_start: date, week_end: date
    ) -> LabourCostSection:
        """Compile labour cost metrics for the week."""
        rosters = self.db.get_rosters_by_date_range(venue_id, week_start, week_end)
        if not rosters:
            return LabourCostSection(total_cost=Decimal("0.00"))

        total_cost = Decimal("0.00")
        base_cost = Decimal("0.00")
        penalty_cost = Decimal("0.00")
        casual_loading = Decimal("0.00")
        super_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        venue = self.db.get_venue(venue_id)

        for roster in rosters:
            for shift in roster.shifts:
                if shift.status in (ShiftStatus.scheduled, ShiftStatus.completed, ShiftStatus.in_progress):
                    employee = self.db.get_employee(shift.employee_id)
                    if employee:
                        breakdown = calculate_shift_cost_breakdown(
                            employee, shift, venue.state
                        )
                        total_cost += breakdown.total_cost
                        base_cost += breakdown.base_cost
                        penalty_cost += breakdown.penalty_cost
                        casual_loading += breakdown.casual_loading
                        super_cost += breakdown.super_contribution
                        total_hours += Decimal(str(shift.net_hours))

        # Get covers for the week
        total_covers = self._get_total_covers(venue_id, week_start, week_end)
        cost_per_cover = (
            total_cost / Decimal(str(total_covers))
            if total_covers > 0
            else Decimal("0.00")
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Compare to previous week
        prev_week_start = week_start - timedelta(days=7)
        prev_week_end = week_end - timedelta(days=7)
        prev_cost = self._get_total_labour_cost(venue_id, prev_week_start, prev_week_end)

        week_over_week_change = Decimal("0.00")
        if prev_cost > 0:
            week_over_week_change = (
                ((total_cost - prev_cost) / prev_cost * 100)
                .quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            )

        trend = "stable"
        if week_over_week_change > Decimal("5.0"):
            trend = "up"
        elif week_over_week_change < Decimal("-5.0"):
            trend = "down"

        # Budget variance (if set)
        budget_cost = None
        budget_variance = None
        if hasattr(venue, 'weekly_budget_cost') and venue.weekly_budget_cost:
            budget_cost = Decimal(str(venue.weekly_budget_cost))
            budget_variance = (total_cost - budget_cost).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        return LabourCostSection(
            total_cost=total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            budget_cost=budget_cost,
            budget_variance=budget_variance,
            cost_breakdown={
                "base": base_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "penalties": penalty_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "casual_loading": casual_loading.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "super": super_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            },
            cost_per_cover=cost_per_cover,
            total_covers=total_covers,
            week_over_week_change_pct=week_over_week_change,
            trend=trend,
        )

    def _compile_forecast_accuracy(
        self, venue_id: str, week_start: date, week_end: date
    ) -> ForecastSection:
        """Compile forecast accuracy metrics."""
        forecasts = self.db.get_forecasts(venue_id, week_start, week_end)
        if not forecasts:
            return ForecastSection(
                mape=Decimal("0.00"),
                rmse=Decimal("0.00"),
            )

        errors_by_day = defaultdict(list)
        all_errors = []

        for forecast in forecasts:
            if forecast.actual_covers is not None and forecast.actual_covers > 0:
                error = abs(forecast.predicted_covers - forecast.actual_covers)
                mape_error = (error / forecast.actual_covers * 100)
                all_errors.append(mape_error)

                day_name = forecast.date.strftime("%A")
                errors_by_day[day_name].append(mape_error)

        if not all_errors:
            return ForecastSection(
                mape=Decimal("0.00"),
                rmse=Decimal("0.00"),
            )

        mape = Decimal(str(statistics.mean(all_errors))).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )
        rmse = Decimal(str(
            (sum(e ** 2 for e in all_errors) / len(all_errors)) ** 0.5
        )).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)

        # Find best and worst days
        best_day = min(errors_by_day.items(), key=lambda x: statistics.mean(x[1]))[0] if errors_by_day else ""
        worst_day = max(errors_by_day.items(), key=lambda x: statistics.mean(x[1]))[0] if errors_by_day else ""

        # Detect bias (systematic over/under prediction)
        bias = "neutral"
        bias_pct = Decimal("0.00")
        actual_totals = sum(f.actual_covers or 0 for f in forecasts)
        predicted_totals = sum(f.predicted_covers for f in forecasts)
        if actual_totals > 0:
            bias_ratio = (predicted_totals - actual_totals) / actual_totals
            bias_pct = (Decimal(str(bias_ratio)) * 100).quantize(
                Decimal("0.1"), rounding=ROUND_HALF_UP
            )
            if bias_ratio > 0.05:
                bias = "over"
            elif bias_ratio < -0.05:
                bias = "under"

        return ForecastSection(
            mape=mape,
            rmse=rmse,
            best_day=best_day,
            worst_day=worst_day,
            bias=bias,
            bias_pct=bias_pct,
            predictions_count=len([f for f in forecasts if f.actual_covers is not None]),
        )

    def _compile_staffing_metrics(
        self, venue_id: str, week_start: date, week_end: date
    ) -> StaffingSection:
        """Compile staffing utilisation metrics."""
        rosters = self.db.get_rosters_by_date_range(venue_id, week_start, week_end)
        if not rosters:
            return StaffingSection(
                total_hours=Decimal("0.00"),
                unique_headcount=0,
                overtime_hours=Decimal("0.00"),
                overtime_cost=Decimal("0.00"),
                casual_ratio=Decimal("0.00"),
                permanent_ratio=Decimal("1.00"),
                avg_shift_length=Decimal("0.00"),
                coverage_gaps_hours=Decimal("0.00"),
                no_shows=0,
            )

        total_hours = Decimal("0.00")
        unique_employees = set()
        overtime_hours = Decimal("0.00")
        overtime_cost = Decimal("0.00")
        employment_types = defaultdict(int)
        shift_lengths = []
        no_shows = 0
        coverage_gaps = Decimal("0.00")
        venue = self.db.get_venue(venue_id)

        for roster in rosters:
            for shift in roster.shifts:
                if shift.status in (ShiftStatus.scheduled, ShiftStatus.completed, ShiftStatus.in_progress):
                    employee = self.db.get_employee(shift.employee_id)
                    if employee:
                        unique_employees.add(shift.employee_id)
                        employment_types[employee.employment_type] += 1

                        net_hours = Decimal(str(shift.net_hours))
                        total_hours += net_hours
                        shift_lengths.append(float(net_hours))

                        # Track overtime
                        if hasattr(shift, 'overtime_hours') and shift.overtime_hours > 0:
                            overtime_hours += Decimal(str(shift.overtime_hours))
                            breakdown = calculate_shift_cost_breakdown(employee, shift, venue.state)
                            overtime_cost += breakdown.penalty_cost

                elif shift.status == ShiftStatus.no_show:
                    no_shows += 1
                    coverage_gaps += Decimal(str(shift.net_hours))

        # Employment type ratios
        total_shifts = sum(employment_types.values())
        casual_ratio = Decimal(str(employment_types[EmploymentType.casual] / total_shifts)) if total_shifts > 0 else Decimal("0.00")
        permanent_ratio = Decimal(str((employment_types[EmploymentType.full_time] + employment_types[EmploymentType.part_time]) / total_shifts)) if total_shifts > 0 else Decimal("1.00")

        avg_shift = Decimal(str(statistics.mean(shift_lengths))) if shift_lengths else Decimal("0.00")

        return StaffingSection(
            total_hours=total_hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            unique_headcount=len(unique_employees),
            overtime_hours=overtime_hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            overtime_cost=overtime_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            casual_ratio=casual_ratio.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            permanent_ratio=permanent_ratio.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            avg_shift_length=avg_shift.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            coverage_gaps_hours=coverage_gaps.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            no_shows=no_shows,
        )

    def _compile_compliance(
        self, venue_id: str, week_start: date, week_end: date
    ) -> ComplianceSection:
        """Compile compliance violations."""
        rosters = self.db.get_rosters_by_date_range(venue_id, week_start, week_end)
        if not rosters:
            return ComplianceSection(violations_found=0)

        violations = []
        violation_types = defaultdict(int)
        venue = self.db.get_venue(venue_id)

        for roster in rosters:
            for shift in roster.shifts:
                if shift.status in (ShiftStatus.scheduled, ShiftStatus.completed, ShiftStatus.in_progress):
                    employee = self.db.get_employee(shift.employee_id)
                    if employee:
                        compliance_result = validate_shift_compliance(
                            employee, shift, venue.state
                        )
                        if not compliance_result.get("compliant", True):
                            violation_type = compliance_result.get("issue_type", "unknown")
                            violation_types[violation_type] += 1
                            violations.append({
                                "date": str(shift.date),
                                "employee": employee.name,
                                "type": violation_type,
                                "details": compliance_result.get("details", ""),
                            })

        resolution_status = "resolved" if len(violations) == 0 else "pending"

        return ComplianceSection(
            violations_found=len(violations),
            violations_types=dict(violation_types),
            resolution_status=resolution_status,
            details=violations[:10],  # Limit to 10 for summary
        )

    def _compile_activity(
        self, venue_id: str, week_start: date, week_end: date
    ) -> ActivitySection:
        """Compile team activity metrics."""
        # These would come from activity log tables in real implementation
        # For now, return defaults; actual implementation would query activity logs
        return ActivitySection(
            shift_swaps_completed=0,
            bids_placed=0,
            bids_resolved=0,
            approvals_processed=0,
            staff_no_shows=0,
            shift_cancellations=0,
        )

    # ========================================================================
    # Insight Generation
    # ========================================================================

    def _generate_insights(
        self,
        labour_cost: LabourCostSection,
        forecast: ForecastSection,
        staffing: StaffingSection,
        compliance: ComplianceSection,
        activity: ActivitySection,
        venue: VenueConfig,
    ) -> List[str]:
        """Generate top 3 actionable insights."""
        insights = []

        # Insight 1: Cost trends
        if labour_cost.budget_variance is not None and labour_cost.budget_variance > Decimal("500"):
            variance_pct = (labour_cost.budget_variance / labour_cost.budget_cost * 100) if labour_cost.budget_cost else Decimal("0")
            insights.append(
                f"Labour costs exceeded budget by ${labour_cost.budget_variance:.2f} "
                f"({variance_pct:.0f}%). Review overtime and penalty costs."
            )
        elif labour_cost.trend == "up" and labour_cost.week_over_week_change_pct > Decimal("10"):
            insights.append(
                f"Labour costs increased {labour_cost.week_over_week_change_pct:.1f}% week-over-week. "
                f"Check for unusual penalty occurrences or overtime."
            )

        # Insight 2: Forecast accuracy
        if forecast.mape > Decimal("25"):
            insights.append(
                f"Forecast accuracy low (MAPE: {forecast.mape:.1f}%). "
                f"Worst day: {forecast.worst_day}. Review demand signals and event calendar."
            )
        elif forecast.bias == "over" and forecast.bias_pct > Decimal("10"):
            insights.append(
                f"Systematically over-predicting demand by {forecast.bias_pct:.1f}%. "
                f"Consider adjusting forecast models or base demand assumptions."
            )

        # Insight 3: Staffing issues
        if staffing.no_shows > 2:
            insights.append(
                f"{staffing.no_shows} no-shows this week created {staffing.coverage_gaps_hours:.1f}h coverage gaps. "
                f"Review attendance patterns and consider backup scheduling."
            )
        elif staffing.overtime_hours > Decimal("20"):
            overtime_employees = min(staffing.unique_headcount, 3)
            insights.append(
                f"High overtime: {staffing.overtime_hours:.1f}h at ${staffing.overtime_cost:.2f} cost. "
                f"Redistribute hours across {overtime_employees}+ staff next week."
            )

        # Compliance insight
        if compliance.violations_found > 0:
            top_violation = max(compliance.violations_types.items(), key=lambda x: x[1])[0]
            count = compliance.violations_types[top_violation]
            insights.append(
                f"{compliance.violations_found} compliance violation(s) detected. "
                f"Most common: {top_violation} ({count}x). Address before next week."
            )

        return insights[:3]

    def _calculate_health_score(
        self,
        labour_cost: LabourCostSection,
        forecast: ForecastSection,
        staffing: StaffingSection,
        compliance: ComplianceSection,
        activity: ActivitySection,
    ) -> float:
        """Calculate overall venue health score (0-100)."""
        score = 100.0

        # Cost management (max -25 points)
        if labour_cost.budget_variance is not None and labour_cost.budget_cost:
            variance_pct = float(labour_cost.budget_variance / labour_cost.budget_cost * 100)
            if variance_pct > 10:
                score -= min(25, variance_pct / 2)
        elif labour_cost.week_over_week_change_pct > Decimal("15"):
            score -= min(15, float(labour_cost.week_over_week_change_pct) / 2)

        # Forecast accuracy (max -20 points)
        mape_float = float(forecast.mape)
        if mape_float > 30:
            score -= min(20, (mape_float - 30) / 2)

        # Staffing issues (max -20 points)
        if staffing.no_shows > 2:
            score -= min(10, staffing.no_shows * 3)
        if staffing.overtime_hours > Decimal("30"):
            score -= min(10, float(staffing.overtime_hours) / 5)

        # Compliance (max -20 points)
        if compliance.violations_found > 0:
            score -= min(20, compliance.violations_found * 2)

        # Positive boost for good activity engagement
        activity_count = (
            activity.shift_swaps_completed +
            activity.bids_resolved +
            activity.approvals_processed
        )
        if activity_count > 10:
            score = min(100, score + 5)

        return max(0.0, score)

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _get_total_covers(self, venue_id: str, start: date, end: date) -> int:
        """Get total covers for period."""
        forecasts = self.db.get_forecasts(venue_id, start, end)
        return sum(f.actual_covers or f.predicted_covers for f in forecasts)

    def _get_total_labour_cost(self, venue_id: str, start: date, end: date) -> Decimal:
        """Get total labour cost for period."""
        rosters = self.db.get_rosters_by_date_range(venue_id, start, end)
        if not rosters:
            return Decimal("0.00")

        total = Decimal("0.00")
        venue = self.db.get_venue(venue_id)
        for roster in rosters:
            for shift in roster.shifts:
                if shift.status in (ShiftStatus.scheduled, ShiftStatus.completed, ShiftStatus.in_progress):
                    employee = self.db.get_employee(shift.employee_id)
                    if employee:
                        breakdown = calculate_shift_cost_breakdown(employee, shift, venue.state)
                        total += breakdown.total_cost

        return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # ========================================================================
    # Rendering
    # ========================================================================

    def render_html(self, digest: WeeklyDigest) -> str:
        """Render digest as HTML email template."""
        # Helper variables to avoid f-string backslash issues
        score_class = "positive" if digest.score >= 80 else "negative"
        variance_class = "negative" if digest.labour_cost.budget_variance and digest.labour_cost.budget_variance > 0 else "positive"
        change_class = "negative" if digest.labour_cost.week_over_week_change_pct > 0 else "positive"
        coverage_class = "alert" if digest.staffing.coverage_gaps_hours > 0 else ""

        # Build optional sections
        budget_section = ""
        if digest.labour_cost.budget_cost:
            budget_section += f'<div class="metric"><span class="metric-label">Budget:</span><span class="metric-value">${digest.labour_cost.budget_cost:,.2f}</span></div>'

        variance_section = ""
        if digest.labour_cost.budget_variance is not None:
            variance_section += f'<div class="metric"><span class="metric-label">Variance:</span><span class="metric-value {variance_class}">${digest.labour_cost.budget_variance:,.2f}</span></div>'

        violations_section = ""
        if digest.compliance.violations_types:
            violations_list = '<br/>'.join(f'{k}: {v}' for k, v in digest.compliance.violations_types.items())
            violations_section = f'<div style="margin-top: 10px; font-size: 14px;"><strong>Types:</strong><br/>{violations_list}</div>'

        insights_html = ''.join(f'<div class="insight">{i}</div>' for i in digest.insights)
        newline = '\n'

        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #1e40af; color: white; padding: 20px; border-radius: 8px; }}
        .score {{ font-size: 36px; font-weight: bold; margin: 10px 0; }}
        .section {{ margin: 20px 0; padding: 15px; background: #f5f5f5; border-left: 4px solid #1e40af; }}
        .section h3 {{ margin-top: 0; color: #1e40af; }}
        .metric {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #ddd; }}
        .metric-label {{ font-weight: bold; }}
        .metric-value {{ text-align: right; }}
        .insight {{ padding: 10px; margin: 5px 0; background: #e3f2fd; border-left: 3px solid #1e40af; }}
        .alert {{ padding: 10px; margin: 5px 0; background: #fff3cd; border-left: 3px solid #ff9800; }}
        .positive {{ color: #4caf50; }}
        .negative {{ color: #f44336; }}
        .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #999; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Weekly Digest: {digest.venue_name}</h1>
            <p>Week of {digest.week_start.strftime('%B %d')} - {digest.week_end.strftime('%B %d, %Y')}</p>
            <div class="score" style="text-align: center;">
                Health Score: <span class="{score_class}">{digest.score:.0f}/100</span>
            </div>
        </div>

        <div class="section">
            <h3>Labour Cost Summary</h3>
            <div class="metric">
                <span class="metric-label">Total Cost:</span>
                <span class="metric-value">${digest.labour_cost.total_cost:,.2f}</span>
            </div>
            {budget_section}
            {variance_section}
            <div class="metric">
                <span class="metric-label">Cost Per Cover:</span>
                <span class="metric-value">${digest.labour_cost.cost_per_cover:.2f}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Week-over-Week Change:</span>
                <span class="metric-value {change_class}">{digest.labour_cost.week_over_week_change_pct:+.1f}%</span>
            </div>
            <div class="metric">
                <span class="metric-label">Cost Breakdown:</span>
            </div>
            <div style="margin-left: 20px; font-size: 14px;">
                <div>Base: ${digest.labour_cost.cost_breakdown['base']:,.2f}</div>
                <div>Penalties: ${digest.labour_cost.cost_breakdown['penalties']:,.2f}</div>
                <div>Casual Loading: ${digest.labour_cost.cost_breakdown['casual_loading']:,.2f}</div>
                <div>Superannuation: ${digest.labour_cost.cost_breakdown['super']:,.2f}</div>
            </div>
        </div>

        <div class="section">
            <h3>Forecast Accuracy</h3>
            <div class="metric">
                <span class="metric-label">MAPE (Mean Absolute %):</span>
                <span class="metric-value">{digest.forecast_accuracy.mape:.1f}%</span>
            </div>
            <div class="metric">
                <span class="metric-label">RMSE:</span>
                <span class="metric-value">{digest.forecast_accuracy.rmse:.1f}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Best Day:</span>
                <span class="metric-value">{digest.forecast_accuracy.best_day}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Worst Day:</span>
                <span class="metric-value">{digest.forecast_accuracy.worst_day}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Systematic Bias:</span>
                <span class="metric-value">{digest.forecast_accuracy.bias} ({digest.forecast_accuracy.bias_pct:+.1f}%)</span>
            </div>
        </div>

        <div class="section">
            <h3>Staffing Metrics</h3>
            <div class="metric">
                <span class="metric-label">Total Hours Rostered:</span>
                <span class="metric-value">{digest.staffing.total_hours:.1f}h</span>
            </div>
            <div class="metric">
                <span class="metric-label">Unique Staff:</span>
                <span class="metric-value">{digest.staffing.unique_headcount}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Overtime:</span>
                <span class="metric-value">{digest.staffing.overtime_hours:.1f}h (${digest.staffing.overtime_cost:,.2f})</span>
            </div>
            <div class="metric">
                <span class="metric-label">Employment Mix:</span>
                <span class="metric-value">Casual {(digest.staffing.casual_ratio * 100):.0f}% / Permanent {(digest.staffing.permanent_ratio * 100):.0f}%</span>
            </div>
            <div class="metric">
                <span class="metric-label">Avg Shift Length:</span>
                <span class="metric-value">{digest.staffing.avg_shift_length:.1f}h</span>
            </div>
            <div class="metric">
                <span class="metric-label">Coverage Gaps:</span>
                <span class="metric-value {coverage_class}">{digest.staffing.coverage_gaps_hours:.1f}h</span>
            </div>
            <div class="metric">
                <span class="metric-label">No-Shows:</span>
                <span class="metric-value">{digest.staffing.no_shows}</span>
            </div>
        </div>

        <div class="section">
            <h3>Compliance</h3>
            <div class="metric">
                <span class="metric-label">Violations Found:</span>
                <span class="metric-value">{digest.compliance.violations_found}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Status:</span>
                <span class="metric-value">{digest.compliance.resolution_status.title()}</span>
            </div>
            {violations_section}
        </div>

        <div class="section">
            <h3>Key Insights</h3>
            {insights_html}
        </div>

        <div class="footer">
            <p>Generated: {digest.generated_at.strftime('%Y-%m-%d %H:%M')} AEST</p>
            <p>RosterIQ Weekly Digest</p>
        </div>
    </div>
</body>
</html>
"""
        return html

    def render_json(self, digest: WeeklyDigest) -> dict:
        """Render digest as JSON-friendly dict."""
        return {
            "venue_id": digest.venue_id,
            "venue_name": digest.venue_name,
            "week_start": digest.week_start.isoformat(),
            "week_end": digest.week_end.isoformat(),
            "generated_at": digest.generated_at.isoformat(),
            "score": digest.score,
            "labour_cost": {
                "total_cost": float(digest.labour_cost.total_cost),
                "budget_cost": float(digest.labour_cost.budget_cost) if digest.labour_cost.budget_cost else None,
                "budget_variance": float(digest.labour_cost.budget_variance) if digest.labour_cost.budget_variance else None,
                "cost_per_cover": float(digest.labour_cost.cost_per_cover),
                "total_covers": digest.labour_cost.total_covers,
                "week_over_week_change_pct": float(digest.labour_cost.week_over_week_change_pct),
                "trend": digest.labour_cost.trend,
                "breakdown": {k: float(v) for k, v in digest.labour_cost.cost_breakdown.items()},
            },
            "forecast_accuracy": {
                "mape": float(digest.forecast_accuracy.mape),
                "rmse": float(digest.forecast_accuracy.rmse),
                "best_day": digest.forecast_accuracy.best_day,
                "worst_day": digest.forecast_accuracy.worst_day,
                "bias": digest.forecast_accuracy.bias,
                "bias_pct": float(digest.forecast_accuracy.bias_pct),
                "predictions_count": digest.forecast_accuracy.predictions_count,
            },
            "staffing": {
                "total_hours": float(digest.staffing.total_hours),
                "unique_headcount": digest.staffing.unique_headcount,
                "overtime_hours": float(digest.staffing.overtime_hours),
                "overtime_cost": float(digest.staffing.overtime_cost),
                "casual_ratio": float(digest.staffing.casual_ratio),
                "permanent_ratio": float(digest.staffing.permanent_ratio),
                "avg_shift_length": float(digest.staffing.avg_shift_length),
                "coverage_gaps_hours": float(digest.staffing.coverage_gaps_hours),
                "no_shows": digest.staffing.no_shows,
            },
            "compliance": {
                "violations_found": digest.compliance.violations_found,
                "violations_types": digest.compliance.violations_types,
                "resolution_status": digest.compliance.resolution_status,
            },
            "activity": {
                "shift_swaps_completed": digest.activity.shift_swaps_completed,
                "bids_placed": digest.activity.bids_placed,
                "bids_resolved": digest.activity.bids_resolved,
                "approvals_processed": digest.activity.approvals_processed,
                "staff_no_shows": digest.activity.staff_no_shows,
                "shift_cancellations": digest.activity.shift_cancellations,
            },
            "insights": digest.insights,
        }

    # ========================================================================
    # Email & Scheduling
    # ========================================================================

    async def send_digest(
        self, venue_id: str, week_start: date, recipients: List[str]
    ) -> bool:
        """
        Generate and send digest to venue managers.

        Args:
            venue_id: Venue identifier
            week_start: Start of week
            recipients: List of email addresses

        Returns:
            True if sent successfully
        """
        try:
            digest = self.generate_digest(venue_id, week_start)
            html_content = self.render_html(digest)
            subject = f"RosterIQ Weekly Digest: {digest.venue_name} ({digest.week_start.strftime('%b %d')})"

            for recipient in recipients:
                await self.notifications.send_email(
                    to_email=recipient,
                    subject=subject,
                    html_content=html_content,
                )

            logger.info(f"Sent digest for {venue_id} to {len(recipients)} recipients")
            return True
        except Exception as e:
            logger.error(f"Failed to send digest: {e}")
            return False

    def schedule_weekly_digests(self) -> None:
        """
        Register weekly digest scheduling with task scheduler.
        Runs Monday 6am AEST for all active venues.
        """
        try:
            from rosteriq.services.task_scheduler import get_scheduler
            scheduler = get_scheduler()

            # Get all active venues
            venues = self.db.list_venues()
            for venue in venues:
                if getattr(venue, 'active', True):
                    # Schedule digest for this venue
                    job_config = {
                        "job_type": "weekly_digest",
                        "venue_id": venue.id,
                        "run_time": "06:00",  # 6am AEST
                        "day_of_week": "monday",
                    }
                    scheduler.register_job(job_config)
                    logger.info(f"Scheduled weekly digest for {venue.id}")
        except Exception as e:
            logger.error(f"Failed to schedule digests: {e}")

    def generate_multi_venue_digest(
        self, venue_ids: List[str], week_start: date
    ) -> MultiVenueDigest:
        """
        Generate portfolio digest for owners with multiple venues.

        Args:
            venue_ids: List of venue identifiers
            week_start: Start of week

        Returns:
            MultiVenueDigest with comparison table
        """
        digests = []
        portfolio_totals = {
            "total_cost": Decimal("0.00"),
            "total_hours": Decimal("0.00"),
            "total_headcount": 0,
            "avg_health_score": 0.0,
        }
        scores = []

        for venue_id in venue_ids:
            try:
                digest = self.generate_digest(venue_id, week_start)
                digests.append(digest)

                portfolio_totals["total_cost"] += digest.labour_cost.total_cost
                portfolio_totals["total_hours"] += digest.staffing.total_hours
                portfolio_totals["total_headcount"] += digest.staffing.unique_headcount
                scores.append(digest.score)
            except Exception as e:
                logger.error(f"Failed to generate digest for {venue_id}: {e}")

        if scores:
            portfolio_totals["avg_health_score"] = sum(scores) / len(scores)

        # Generate comparison insights
        comparison_insights = self._generate_portfolio_insights(digests)

        return MultiVenueDigest(
            owner_id="",  # Would be set from context
            week_start=week_start,
            week_end=week_start + timedelta(days=6),
            generated_at=datetime.now(),
            venue_digests=digests,
            portfolio_totals=portfolio_totals,
            comparison_insights=comparison_insights,
        )

    def _generate_portfolio_insights(self, digests: List[WeeklyDigest]) -> List[str]:
        """Generate comparison insights across venues."""
        insights = []

        if not digests:
            return insights

        # Find venue with highest cost
        highest_cost_venue = max(digests, key=lambda d: d.labour_cost.total_cost)
        lowest_cost_venue = min(digests, key=lambda d: d.labour_cost.total_cost)

        if highest_cost_venue.venue_id != lowest_cost_venue.venue_id:
            insights.append(
                f"{highest_cost_venue.venue_name} spent ${highest_cost_venue.labour_cost.total_cost:,.0f} "
                f"vs {lowest_cost_venue.venue_name} ${lowest_cost_venue.labour_cost.total_cost:,.0f} — "
                f"review cost drivers at {highest_cost_venue.venue_name}."
            )

        # Find best and worst performing
        best_health = max(digests, key=lambda d: d.score)
        worst_health = min(digests, key=lambda d: d.score)

        if best_health.score - worst_health.score > 15:
            insights.append(
                f"{best_health.venue_name} leads health score ({best_health.score:.0f}). "
                f"{worst_health.venue_name} lagging ({worst_health.score:.0f}). "
                f"Share best practices."
            )

        return insights[:3]
