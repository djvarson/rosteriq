"""
RosterIQ Reporting and Analytics Module

Comprehensive reporting system for Australian hospitality rostering.
Generates labour cost reports, forecast accuracy metrics, roster efficiency analysis,
and exportable data in CSV and JSON formats.

Features:
- Labour cost analysis with penalty and superannuation tracking
- Forecast accuracy measurement against actual covers
- Roster efficiency scoring (coverage, fairness, cost)
- Employee performance and utilisation reports
- Weekly digest with highlights and recommendations
- Period-over-period comparison analysis
- Multiple export formats (CSV, JSON)
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from enum import Enum
import json
import csv
from io import StringIO


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class ReportPeriod:
    """
    Represents the time period for a report.

    Attributes:
        start_date: Period start date (YYYY-MM-DD)
        end_date: Period end date (YYYY-MM-DD)
        venue_id: Unique venue identifier
        venue_name: Human-readable venue name
    """
    start_date: str
    end_date: str
    venue_id: str
    venue_name: str


@dataclass
class LabourCostReport:
    """
    Complete labour cost analysis for a period.

    Attributes:
        period: ReportPeriod defining the date range
        total_cost: Total labour cost in AUD
        total_hours: Total hours rostered
        avg_hourly_cost: Average cost per hour
        cost_by_day: Dict mapping date (YYYY-MM-DD) to daily cost
        cost_by_role: Dict mapping role name to total cost
        cost_by_employee: Dict mapping employee_id to total cost
        overtime_hours: Total overtime hours worked
        overtime_cost: Total overtime cost
        penalty_cost: Total penalty rates (weekend, public holiday)
        super_cost: Superannuation contribution cost
        budget: Budgeted labour cost (if provided)
        budget_variance: Actual - Budget (positive = over)
        budget_variance_pct: Variance as percentage
    """
    period: ReportPeriod
    total_cost: float
    total_hours: float
    avg_hourly_cost: float
    cost_by_day: Dict[str, float]
    cost_by_role: Dict[str, float]
    cost_by_employee: Dict[str, float]
    overtime_hours: float
    overtime_cost: float
    penalty_cost: float
    super_cost: float
    budget: Optional[float] = None
    budget_variance: Optional[float] = None
    budget_variance_pct: Optional[float] = None


@dataclass
class ForecastAccuracyReport:
    """
    Measures forecast accuracy against actual demand.

    Attributes:
        period: ReportPeriod defining the date range
        overall_accuracy_pct: Overall forecast accuracy (0-100)
        by_day: Dict mapping date to accuracy percentage
        by_hour: Dict mapping hour (0-23) to accuracy percentage
        by_signal_source: Dict mapping data source to accuracy
        mean_absolute_error: MAE of forecast vs actual
        total_forecast_covers: Sum of all forecast covers
        total_actual_covers: Sum of all actual covers
        overforecast_hours: Hours where we overestimated demand
        underforecast_hours: Hours where we underestimated demand
    """
    period: ReportPeriod
    overall_accuracy_pct: float
    by_day: Dict[str, float]
    by_hour: Dict[int, float]
    by_signal_source: Dict[str, float]
    mean_absolute_error: float
    total_forecast_covers: int
    total_actual_covers: int
    overforecast_hours: int
    underforecast_hours: int


@dataclass
class RosterEfficiencyReport:
    """
    Evaluates roster quality and efficiency.

    Attributes:
        period: ReportPeriod defining the date range
        coverage_score: 0-100 score for demand coverage
        fairness_score: 0-100 score for hour distribution fairness
        cost_efficiency_score: 0-100 score for cost efficiency
        overall_score: Weighted average of above scores
        staff_utilisation_pct: % of rostered hours that are productive
        idle_hours: Hours rostered but not needed
        overstaffed_hours: Total hours over-rostered
        understaffed_hours: Total hours under-rostered
        recommendations: List of improvement suggestions
    """
    period: ReportPeriod
    coverage_score: float
    fairness_score: float
    cost_efficiency_score: float
    overall_score: float
    staff_utilisation_pct: float
    idle_hours: float
    overstaffed_hours: float
    understaffed_hours: float
    recommendations: List[str] = field(default_factory=list)


@dataclass
class EmployeeReport:
    """
    Individual employee performance and utilisation report.

    Attributes:
        employee_id: Unique employee identifier
        name: Employee full name
        total_hours: Total hours worked in period
        total_shifts: Number of shifts worked
        avg_shift_length: Average shift duration in hours
        overtime_hours: Total overtime hours
        weekend_shifts: Number of weekend shifts
        evening_shifts: Number of evening shifts (after 6pm)
        total_earnings: Total pay earned
        swap_requests: Number of shift swaps requested
        swap_approvals: Number of swaps approved
    """
    employee_id: str
    name: str
    total_hours: float
    total_shifts: int
    avg_shift_length: float
    overtime_hours: float
    weekend_shifts: int
    evening_shifts: int
    total_earnings: float
    swap_requests: int = 0
    swap_approvals: int = 0


@dataclass
class WeeklyDigest:
    """
    High-level weekly summary for venue managers.

    Attributes:
        period: ReportPeriod (typically one week)
        highlights: List of positive observations
        warnings: List of concerns or anomalies
        labour_summary: Dict with key labour metrics
        top_performers: List of employee names/IDs
        recommendations: List of actionable suggestions
        comparison_to_last_week: Dict of metric comparisons
    """
    period: ReportPeriod
    highlights: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    labour_summary: Dict[str, Any] = field(default_factory=dict)
    top_performers: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    comparison_to_last_week: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# REPORT GENERATOR CLASS
# ============================================================================

class ReportGenerator:
    """
    Generates all report types for a venue.

    Encapsulates analysis logic for labour costs, forecast accuracy,
    roster efficiency, and employee performance.
    """

    def __init__(self, venue_id: str, venue_name: str):
        """
        Initialize report generator for a venue.

        Args:
            venue_id: Unique venue identifier
            venue_name: Human-readable venue name
        """
        self.venue_id = venue_id
        self.venue_name = venue_name

    def generate_labour_cost_report(
        self,
        roster: List[Dict],
        pay_calculations: List[Dict],
        budget: Optional[float] = None
    ) -> LabourCostReport:
        """
        Generate comprehensive labour cost analysis.

        Args:
            roster: List of shift dicts with employee_id, date, start_hour, end_hour
            pay_calculations: List of pay calc dicts with employee_id, total, overtime, penalties, super
            budget: Optional budgeted labour cost

        Returns:
            LabourCostReport with full cost breakdown
        """
        if not roster or not pay_calculations:
            return self._empty_labour_report()

        # Extract date range
        dates = [r.get('date') for r in roster if r.get('date')]
        if not dates:
            return self._empty_labour_report()

        start_date = min(dates)
        end_date = max(dates)
        period = ReportPeriod(start_date, end_date, self.venue_id, self.venue_name)

        # Aggregate costs
        total_cost = sum(p.get('total', 0) for p in pay_calculations)
        overtime_cost = sum(p.get('overtime_cost', 0) for p in pay_calculations)
        penalty_cost = sum(p.get('penalty_cost', 0) for p in pay_calculations)
        super_cost = sum(p.get('super_cost', 0) for p in pay_calculations)

        # Calculate hours
        total_hours = 0.0
        for shift in roster:
            start_hour = shift.get('start_hour', 0)
            end_hour = shift.get('end_hour', 0)
            break_mins = shift.get('break_minutes', 30)
            duration = (end_hour - start_hour) - (break_mins / 60)
            total_hours += max(0, duration)

        avg_hourly_cost = total_cost / total_hours if total_hours > 0 else 0

        # Cost by day
        cost_by_day: Dict[str, float] = {}
        for shift in roster:
            shift_date = shift.get('date', '')
            emp_id = shift.get('employee_id', '')
            cost = next((p.get('total', 0) for p in pay_calculations
                        if p.get('employee_id') == emp_id), 0)
            cost_by_day[shift_date] = cost_by_day.get(shift_date, 0) + cost

        # Cost by role
        cost_by_role: Dict[str, float] = {}
        for shift in roster:
            role = shift.get('role_required', 'unknown')
            emp_id = shift.get('employee_id', '')
            cost = next((p.get('total', 0) for p in pay_calculations
                        if p.get('employee_id') == emp_id), 0)
            cost_by_role[role] = cost_by_role.get(role, 0) + cost

        # Cost by employee
        cost_by_employee: Dict[str, float] = {}
        for pay in pay_calculations:
            emp_id = pay.get('employee_id', '')
            cost_by_employee[emp_id] = pay.get('total', 0)

        # Budget variance
        budget_variance = None
        budget_variance_pct = None
        if budget is not None:
            budget_variance = total_cost - budget
            budget_variance_pct = (budget_variance / budget * 100) if budget > 0 else 0

        return LabourCostReport(
            period=period,
            total_cost=round(total_cost, 2),
            total_hours=round(total_hours, 2),
            avg_hourly_cost=round(avg_hourly_cost, 2),
            cost_by_day={k: round(v, 2) for k, v in cost_by_day.items()},
            cost_by_role={k: round(v, 2) for k, v in cost_by_role.items()},
            cost_by_employee={k: round(v, 2) for k, v in cost_by_employee.items()},
            overtime_hours=round(sum(p.get('overtime_hours', 0) for p in pay_calculations), 2),
            overtime_cost=round(overtime_cost, 2),
            penalty_cost=round(penalty_cost, 2),
            super_cost=round(super_cost, 2),
            budget=budget,
            budget_variance=round(budget_variance, 2) if budget_variance else None,
            budget_variance_pct=round(budget_variance_pct, 2) if budget_variance_pct else None
        )

    def generate_forecast_accuracy(
        self,
        forecasts: List[Dict],
        actuals: List[Dict]
    ) -> ForecastAccuracyReport:
        """
        Measure forecast accuracy against actual covers.

        Args:
            forecasts: List of forecast dicts with date, hour, predicted_covers, source
            actuals: List of actual dicts with date, hour, actual_covers

        Returns:
            ForecastAccuracyReport with accuracy metrics
        """
        if not forecasts or not actuals:
            return self._empty_forecast_report()

        dates = [f.get('date') for f in forecasts if f.get('date')]
        if not dates:
            return self._empty_forecast_report()

        start_date = min(dates)
        end_date = max(dates)
        period = ReportPeriod(start_date, end_date, self.venue_id, self.venue_name)

        # Build lookup for actuals
        actuals_map = {}
        for actual in actuals:
            key = (actual.get('date'), actual.get('hour'))
            actuals_map[key] = actual.get('actual_covers', 0)

        # Calculate accuracy metrics
        total_error = 0
        total_points = 0
        by_day: Dict[str, float] = {}
        by_hour: Dict[int, float] = {}
        by_source: Dict[str, float] = {}
        overforecast = 0
        underforecast = 0
        total_forecast_covers = 0
        total_actual_covers = 0

        for forecast in forecasts:
            f_date = forecast.get('date')
            f_hour = forecast.get('hour', 0)
            f_covers = forecast.get('predicted_covers', 0)
            source = forecast.get('source', 'unknown')

            actual = actuals_map.get((f_date, f_hour), 0)
            error = abs(f_covers - actual)

            total_error += error
            total_points += 1
            total_forecast_covers += f_covers
            total_actual_covers += actual

            if f_covers > actual:
                overforecast += 1
            elif f_covers < actual:
                underforecast += 1

            # Accuracy for this point (100% if exact, 0% if way off)
            accuracy = max(0, 100 - (error / max(1, actual) * 100)) if actual > 0 else 100

            by_day[f_date] = (by_day.get(f_date, 0) + accuracy) / 2 if f_date in by_day else accuracy
            by_hour[f_hour] = (by_hour.get(f_hour, 0) + accuracy) / 2 if f_hour in by_hour else accuracy
            by_source[source] = (by_source.get(source, 0) + accuracy) / 2 if source in by_source else accuracy

        mae = total_error / total_points if total_points > 0 else 0
        overall_accuracy = 100 - (mae / max(1, total_actual_covers / total_points) * 100) if total_points > 0 else 0
        overall_accuracy = max(0, min(100, overall_accuracy))

        return ForecastAccuracyReport(
            period=period,
            overall_accuracy_pct=round(overall_accuracy, 1),
            by_day={k: round(v, 1) for k, v in by_day.items()},
            by_hour={k: round(v, 1) for k, v in by_hour.items()},
            by_signal_source={k: round(v, 1) for k, v in by_source.items()},
            mean_absolute_error=round(mae, 2),
            total_forecast_covers=int(total_forecast_covers),
            total_actual_covers=int(total_actual_covers),
            overforecast_hours=overforecast,
            underforecast_hours=underforecast
        )

    def generate_roster_efficiency(
        self,
        roster: List[Dict],
        demand_forecasts: List[Dict]
    ) -> RosterEfficiencyReport:
        """
        Evaluate roster quality and efficiency.

        Args:
            roster: List of shift dicts with date, start_hour, end_hour, employee_id
            demand_forecasts: List of forecast dicts with date, hour, required_staff

        Returns:
            RosterEfficiencyReport with efficiency scores
        """
        if not roster or not demand_forecasts:
            return self._empty_efficiency_report()

        dates = [r.get('date') for r in roster if r.get('date')]
        if not dates:
            return self._empty_efficiency_report()

        start_date = min(dates)
        end_date = max(dates)
        period = ReportPeriod(start_date, end_date, self.venue_id, self.venue_name)

        # Build rostered staff by hour
        rostered_by_hour: Dict[Tuple[str, int], int] = {}
        for shift in roster:
            s_date = shift.get('date')
            start_h = shift.get('start_hour', 0)
            end_h = shift.get('end_hour', 0)
            for hour in range(start_h, end_h):
                key = (s_date, hour)
                rostered_by_hour[key] = rostered_by_hour.get(key, 0) + 1

        # Build required staff by hour
        required_by_hour: Dict[Tuple[str, int], int] = {}
        for forecast in demand_forecasts:
            f_date = forecast.get('date')
            f_hour = forecast.get('hour', 0)
            required = forecast.get('required_staff', 0)
            key = (f_date, f_hour)
            required_by_hour[key] = required

        # Calculate metrics
        total_coverage = 0
        coverage_count = 0
        total_fairness = 0
        fairness_count = 0
        overstaffed = 0
        understaffed = 0

        for key, required in required_by_hour.items():
            rostered = rostered_by_hour.get(key, 0)

            # Coverage score (0-100: how well do we meet demand)
            if required > 0:
                coverage = min(100, (rostered / required) * 100)
            else:
                coverage = 100 if rostered == 0 else 50

            total_coverage += coverage
            coverage_count += 1

            # Track staffing deviations
            if rostered > required:
                overstaffed += rostered - required
            elif rostered < required:
                understaffed += required - rostered

        # Fairness score based on hour distribution
        employee_hours: Dict[str, float] = {}
        for shift in roster:
            emp_id = shift.get('employee_id')
            start_h = shift.get('start_hour', 0)
            end_h = shift.get('end_hour', 0)
            hours = end_h - start_h - (shift.get('break_minutes', 30) / 60)
            if emp_id:
                employee_hours[emp_id] = employee_hours.get(emp_id, 0) + hours

        if employee_hours:
            avg_hours = sum(employee_hours.values()) / len(employee_hours)
            variance = sum((h - avg_hours) ** 2 for h in employee_hours.values()) / len(employee_hours)
            fairness_score = max(0, 100 - (variance / (avg_hours ** 2 + 1) * 100))
        else:
            fairness_score = 100

        # Cost efficiency (lower cost per unit of coverage)
        total_rostered_hours = sum(
            (s.get('end_hour', 0) - s.get('start_hour', 0) - (s.get('break_minutes', 30) / 60))
            for s in roster
        )

        idle_hours = max(0, total_rostered_hours - sum(
            (required * (end_date != "start_date"))
            for required in required_by_hour.values()
        ))

        staff_util = ((total_rostered_hours - idle_hours) / total_rostered_hours * 100) if total_rostered_hours > 0 else 0
        cost_efficiency = min(100, staff_util)

        coverage_score = (total_coverage / coverage_count) if coverage_count > 0 else 0
        overall_score = (coverage_score * 0.4 + fairness_score * 0.3 + cost_efficiency * 0.3)

        report = RosterEfficiencyReport(
            period=period,
            coverage_score=round(coverage_score, 1),
            fairness_score=round(fairness_score, 1),
            cost_efficiency_score=round(cost_efficiency, 1),
            overall_score=round(overall_score, 1),
            staff_utilisation_pct=round(staff_util, 1),
            idle_hours=round(idle_hours, 2),
            overstaffed_hours=overstaffed,
            understaffed_hours=understaffed
        )

        report.recommendations = self.generate_recommendations(report)
        return report

    def generate_employee_report(
        self,
        employee: Dict,
        shifts: List[Dict],
        pay_calculations: List[Dict]
    ) -> EmployeeReport:
        """
        Generate individual employee performance report.

        Args:
            employee: Employee dict with id, name
            shifts: List of shift dicts for this employee
            pay_calculations: Pay calculation dicts

        Returns:
            EmployeeReport with utilisation and earnings
        """
        emp_id = employee.get('id', '')
        emp_name = employee.get('name', 'Unknown')

        total_hours = 0
        total_shifts = len(shifts)
        weekend_shifts = 0
        evening_shifts = 0

        for shift in shifts:
            start_h = shift.get('start_hour', 0)
            end_h = shift.get('end_hour', 0)
            break_mins = shift.get('break_minutes', 30)
            hours = (end_h - start_h) - (break_mins / 60)
            total_hours += hours

            # Check if weekend
            shift_date = shift.get('date', '')
            if shift_date:
                try:
                    dt = datetime.strptime(shift_date, '%Y-%m-%d')
                    if dt.weekday() >= 5:  # Saturday=5, Sunday=6
                        weekend_shifts += 1
                except:
                    pass

            # Check if evening
            if start_h >= 18:
                evening_shifts += 1

        avg_shift = total_hours / total_shifts if total_shifts > 0 else 0

        # Get pay info
        pay_info = next((p for p in pay_calculations if p.get('employee_id') == emp_id), {})
        total_earnings = pay_info.get('total', 0)
        overtime_hours = pay_info.get('overtime_hours', 0)

        return EmployeeReport(
            employee_id=emp_id,
            name=emp_name,
            total_hours=round(total_hours, 2),
            total_shifts=total_shifts,
            avg_shift_length=round(avg_shift, 2),
            overtime_hours=round(overtime_hours, 2),
            weekend_shifts=weekend_shifts,
            evening_shifts=evening_shifts,
            total_earnings=round(total_earnings, 2)
        )

    def generate_weekly_digest(
        self,
        roster: List[Dict],
        forecasts: List[Dict],
        actuals: List[Dict],
        pay_calculations: List[Dict]
    ) -> WeeklyDigest:
        """
        Generate high-level weekly summary for managers.

        Args:
            roster: List of shifts
            forecasts: Forecast data
            actuals: Actual covers data
            pay_calculations: Pay calculations

        Returns:
            WeeklyDigest with highlights and recommendations
        """
        dates = [r.get('date') for r in roster if r.get('date')]
        if not dates:
            return self._empty_digest()

        start_date = min(dates)
        end_date = max(dates)
        period = ReportPeriod(start_date, end_date, self.venue_id, self.venue_name)

        cost_report = self.generate_labour_cost_report(roster, pay_calculations)
        accuracy_report = self.generate_forecast_accuracy(forecasts, actuals)

        digest = WeeklyDigest(period=period)

        # Labour summary
        digest.labour_summary = {
            'total_cost': cost_report.total_cost,
            'total_hours': cost_report.total_hours,
            'overtime_hours': cost_report.overtime_hours,
            'penalty_cost': cost_report.penalty_cost
        }

        # Highlights
        if accuracy_report.overall_accuracy_pct > 85:
            digest.highlights.append(f"Excellent forecast accuracy: {accuracy_report.overall_accuracy_pct}%")
        if cost_report.budget_variance_pct and cost_report.budget_variance_pct < 5:
            digest.highlights.append("Under budget - excellent cost control")
        if cost_report.overtime_cost < cost_report.total_cost * 0.05:
            digest.highlights.append("Low overtime costs")

        # Warnings
        if accuracy_report.overall_accuracy_pct < 70:
            digest.warnings.append(f"Low forecast accuracy: {accuracy_report.overall_accuracy_pct}%")
        if cost_report.budget_variance_pct and cost_report.budget_variance_pct > 10:
            digest.warnings.append(f"Over budget by {cost_report.budget_variance_pct}%")
        if cost_report.overtime_hours > 10:
            digest.warnings.append(f"High overtime: {cost_report.overtime_hours} hours")

        # Top performers (by hours)
        emp_hours = {}
        for shift in roster:
            emp_id = shift.get('employee_id', '')
            if emp_id:
                start_h = shift.get('start_hour', 0)
                end_h = shift.get('end_hour', 0)
                hours = end_h - start_h - (shift.get('break_minutes', 30) / 60)
                emp_hours[emp_id] = emp_hours.get(emp_id, 0) + hours

        digest.top_performers = sorted(emp_hours.items(), key=lambda x: x[1], reverse=True)[:3]
        digest.top_performers = [k for k, v in digest.top_performers]

        digest.recommendations = [
            "Review forecast accuracy trends",
            "Monitor penalty rate exposure",
            "Optimise staffing levels during slow periods"
        ]

        return digest

    def compare_periods(
        self,
        report_a: LabourCostReport,
        report_b: LabourCostReport
    ) -> Dict[str, Any]:
        """
        Compare two labour cost reports period-over-period.

        Args:
            report_a: First report (typically earlier period)
            report_b: Second report (typically later period)

        Returns:
            Dict with comparison metrics and percent changes
        """
        cost_change_pct = ((report_b.total_cost - report_a.total_cost) / report_a.total_cost * 100) if report_a.total_cost > 0 else 0
        hours_change_pct = ((report_b.total_hours - report_a.total_hours) / report_a.total_hours * 100) if report_a.total_hours > 0 else 0
        avg_cost_change_pct = ((report_b.avg_hourly_cost - report_a.avg_hourly_cost) / report_a.avg_hourly_cost * 100) if report_a.avg_hourly_cost > 0 else 0

        return {
            'cost_change': round(report_b.total_cost - report_a.total_cost, 2),
            'cost_change_pct': round(cost_change_pct, 1),
            'hours_change': round(report_b.total_hours - report_a.total_hours, 2),
            'hours_change_pct': round(hours_change_pct, 1),
            'avg_cost_change': round(report_b.avg_hourly_cost - report_a.avg_hourly_cost, 2),
            'avg_cost_change_pct': round(avg_cost_change_pct, 1),
            'overtime_change': round(report_b.overtime_hours - report_a.overtime_hours, 2),
            'penalty_change': round(report_b.penalty_cost - report_a.penalty_cost, 2)
        }

    def generate_recommendations(self, efficiency_report: RosterEfficiencyReport) -> List[str]:
        """
        Generate AI-like optimisation recommendations from efficiency metrics.

        Args:
            efficiency_report: RosterEfficiencyReport to analyse

        Returns:
            List of actionable recommendations
        """
        recommendations = []

        if efficiency_report.coverage_score < 80:
            recommendations.append(
                f"Coverage score is {efficiency_report.coverage_score}%. Consider increasing staffing during peak hours."
            )

        if efficiency_report.fairness_score < 70:
            recommendations.append(
                "Hour distribution is uneven. Review employee preferences and availability to balance fairness."
            )

        if efficiency_report.cost_efficiency_score < 75:
            recommendations.append(
                f"Cost efficiency is {efficiency_report.cost_efficiency_score}%. Reduce overstaffing during slow periods."
            )

        if efficiency_report.overstaffed_hours > 20:
            recommendations.append(
                f"{efficiency_report.overstaffed_hours} overstaffed hours detected. Review demand forecasts accuracy."
            )

        if efficiency_report.understaffed_hours > 20:
            recommendations.append(
                f"{efficiency_report.understaffed_hours} understaffed hours. Risk of poor service quality."
            )

        if efficiency_report.staff_utilisation_pct < 60:
            recommendations.append(
                "Staff utilisation is low. Consider shorter shifts or flexible scheduling."
            )

        if not recommendations:
            recommendations.append("Roster is running efficiently. Continue monitoring metrics.")

        return recommendations

    def _empty_labour_report(self) -> LabourCostReport:
        """Create empty labour report."""
        period = ReportPeriod("", "", self.venue_id, self.venue_name)
        return LabourCostReport(
            period=period, total_cost=0, total_hours=0, avg_hourly_cost=0,
            cost_by_day={}, cost_by_role={}, cost_by_employee={},
            overtime_hours=0, overtime_cost=0, penalty_cost=0, super_cost=0
        )

    def _empty_forecast_report(self) -> ForecastAccuracyReport:
        """Create empty forecast report."""
        period = ReportPeriod("", "", self.venue_id, self.venue_name)
        return ForecastAccuracyReport(
            period=period, overall_accuracy_pct=0, by_day={}, by_hour={},
            by_signal_source={}, mean_absolute_error=0,
            total_forecast_covers=0, total_actual_covers=0,
            overforecast_hours=0, underforecast_hours=0
        )

    def _empty_efficiency_report(self) -> RosterEfficiencyReport:
        """Create empty efficiency report."""
        period = ReportPeriod("", "", self.venue_id, self.venue_name)
        return RosterEfficiencyReport(
            period=period, coverage_score=0, fairness_score=0,
            cost_efficiency_score=0, overall_score=0, staff_utilisation_pct=0,
            idle_hours=0, overstaffed_hours=0, understaffed_hours=0
        )

    def _empty_digest(self) -> WeeklyDigest:
        """Create empty weekly digest."""
        period = ReportPeriod("", "", self.venue_id, self.venue_name)
        return WeeklyDigest(period=period)


# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def export_to_csv(report: Any, filepath: str) -> str:
    """
    Export any report to CSV format.

    Supports all report types. Nested dicts are flattened with dot notation.

    Args:
        report: Any report dataclass instance
        filepath: Path to write CSV file

    Returns:
        Path to created file
    """
    data = asdict(report)

    # Flatten nested structures
    flat_data = {}
    _flatten_dict(data, flat_data)

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_data.keys())
        writer.writeheader()
        writer.writerow(flat_data)

    return filepath


def export_to_json(report: Any, filepath: str) -> str:
    """
    Export any report to JSON format.

    Args:
        report: Any report dataclass instance
        filepath: Path to write JSON file

    Returns:
        Path to created file
    """
    data = asdict(report)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)

    return filepath


def export_roster_to_csv(roster: List[Dict], employees: List[Dict], filepath: str) -> str:
    """
    Export full roster with employee details to CSV.

    Args:
        roster: List of shift dicts
        employees: List of employee dicts with id, name
        filepath: Path to write CSV file

    Returns:
        Path to created file
    """
    emp_map = {e.get('id'): e.get('name', 'Unknown') for e in employees}

    rows = []
    for shift in roster:
        emp_id = shift.get('employee_id', '')
        emp_name = emp_map.get(emp_id, 'Unassigned')

        duration = (shift.get('end_hour', 0) - shift.get('start_hour', 0) -
                   (shift.get('break_minutes', 30) / 60))

        rows.append({
            'date': shift.get('date', ''),
            'employee_id': emp_id,
            'employee_name': emp_name,
            'role': shift.get('role_required', ''),
            'start_time': f"{shift.get('start_hour', 0):02d}:00",
            'end_time': f"{shift.get('end_hour', 0):02d}:00",
            'break_minutes': shift.get('break_minutes', 30),
            'hours': round(duration, 2)
        })

    if not rows:
        with open(filepath, 'w') as f:
            f.write('date,employee_id,employee_name,role,start_time,end_time,break_minutes,hours\n')
    else:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    return filepath


def format_currency(amount: float) -> str:
    """
    Format amount as Australian currency.

    Args:
        amount: Amount in AUD

    Returns:
        Formatted string like "$1,234.56"
    """
    return f"${amount:,.2f}"


def format_percentage(value: float) -> str:
    """
    Format value as percentage.

    Args:
        value: Value 0-100

    Returns:
        Formatted string like "85.2%"
    """
    return f"{value:.1f}%"


def format_hours(hours: float) -> str:
    """
    Format hours with unit.

    Args:
        hours: Number of hours

    Returns:
        Formatted string like "38.5 hrs"
    """
    return f"{hours:.1f} hrs"


# ============================================================================
# ANALYTICS FUNCTIONS
# ============================================================================

def calculate_labour_percentage(labour_cost: float, revenue: float) -> float:
    """
    Calculate labour cost as percentage of revenue.

    Target for Australian hospitality: 25-35%.

    Args:
        labour_cost: Total labour cost in AUD
        revenue: Total revenue in AUD

    Returns:
        Labour cost as percentage of revenue
    """
    if revenue <= 0:
        return 0
    return (labour_cost / revenue) * 100


def calculate_staff_utilisation(rostered_hours: float, productive_hours: float) -> float:
    """
    Calculate percentage of rostered hours that are productive.

    Args:
        rostered_hours: Total rostered hours
        productive_hours: Hours of actual productive work

    Returns:
        Utilisation percentage (0-100)
    """
    if rostered_hours <= 0:
        return 0
    return min(100, (productive_hours / rostered_hours) * 100)


def identify_overstaffed_periods(roster: List[Dict], demand: List[Dict]) -> List[Tuple[str, int, int]]:
    """
    Identify time periods with excess staff.

    Args:
        roster: List of shifts
        demand: List of demand forecasts with date, hour, required_staff

    Returns:
        List of (date, hour, excess_count) tuples
    """
    # Build rostered staff by hour
    rostered_by_hour = {}
    for shift in roster:
        date = shift.get('date', '')
        start_h = shift.get('start_hour', 0)
        end_h = shift.get('end_hour', 0)
        for hour in range(start_h, end_h):
            key = (date, hour)
            rostered_by_hour[key] = rostered_by_hour.get(key, 0) + 1

    # Find excesses
    overstaffed = []
    for d in demand:
        date = d.get('date', '')
        hour = d.get('hour', 0)
        required = d.get('required_staff', 0)
        rostered = rostered_by_hour.get((date, hour), 0)

        if rostered > required:
            overstaffed.append((date, hour, rostered - required))

    return overstaffed


def identify_understaffed_periods(roster: List[Dict], demand: List[Dict]) -> List[Tuple[str, int, int]]:
    """
    Identify time periods with insufficient staff.

    Args:
        roster: List of shifts
        demand: List of demand forecasts with date, hour, required_staff

    Returns:
        List of (date, hour, shortage_count) tuples
    """
    # Build rostered staff by hour
    rostered_by_hour = {}
    for shift in roster:
        date = shift.get('date', '')
        start_h = shift.get('start_hour', 0)
        end_h = shift.get('end_hour', 0)
        for hour in range(start_h, end_h):
            key = (date, hour)
            rostered_by_hour[key] = rostered_by_hour.get(key, 0) + 1

    # Find shortages
    understaffed = []
    for d in demand:
        date = d.get('date', '')
        hour = d.get('hour', 0)
        required = d.get('required_staff', 0)
        rostered = rostered_by_hour.get((date, hour), 0)

        if rostered < required:
            understaffed.append((date, hour, required - rostered))

    return understaffed


def calculate_turnover_risk(employee_reports: List[EmployeeReport]) -> Dict[str, Any]:
    """
    Identify employees at risk of turnover based on hours, fairness, and satisfaction.

    Args:
        employee_reports: List of EmployeeReport instances

    Returns:
        Dict mapping employee_id to risk level and reasons
    """
    if not employee_reports:
        return {}

    # Calculate statistics
    total_hours = [e.total_hours for e in employee_reports]
    avg_hours = sum(total_hours) / len(total_hours) if total_hours else 0

    risk_map = {}

    for report in employee_reports:
        risk_factors = []
        risk_score = 0

        # Low hours might indicate dissatisfaction
        if report.total_hours < avg_hours * 0.5:
            risk_factors.append("Significantly below average hours")
            risk_score += 30

        # High variance in shift types might indicate inflexibility
        evening_ratio = (report.evening_shifts / report.total_shifts * 100) if report.total_shifts > 0 else 0
        if evening_ratio > 70:
            risk_factors.append("Excessive evening shifts")
            risk_score += 15

        # Many swap requests suggest schedule dissatisfaction
        if report.swap_requests > report.swap_approvals + 2:
            risk_factors.append("More swap requests than approvals")
            risk_score += 25

        if risk_score > 0:
            risk_map[report.employee_id] = {
                'risk_level': 'high' if risk_score > 50 else 'medium',
                'risk_score': min(100, risk_score),
                'factors': risk_factors
            }

    return risk_map


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _flatten_dict(d: Dict, flat: Dict, prefix: str = ''):
    """
    Recursively flatten nested dict with dot notation.

    Args:
        d: Nested dictionary
        flat: Flat dictionary to populate
        prefix: Current key prefix
    """
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            _flatten_dict(v, flat, key)
        elif isinstance(v, (list, tuple)):
            flat[key] = json.dumps(v, default=str)
        else:
            flat[key] = v
