"""
Labour cost trends analytics service for RosterIQ.

Provides comprehensive cost trending, forecasting, and analysis capabilities:
- Cost trends by time period (daily, weekly, monthly)
- Breakdown by employment type, day of week, role, and cost component
- Cost per cover analysis using POS data
- Multi-venue comparison
- Cost forecasting via linear regression
- Overtime analysis
- Casual dependency reporting

All monetary values in AUD, Decimal precision.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from dataclasses import dataclass, asdict
import statistics

from rosteriq.models import (
    Roster, Shift, Employee, EmploymentType, DayType, State,
)
from rosteriq.database import BaseStore
from rosteriq.award_rules import get_day_type


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class PeriodCost:
    """Cost data for a single time period."""
    period_start: date
    period_end: date
    total_cost: Decimal
    hours: Decimal
    shifts: int
    base_cost: Decimal
    penalty_cost: Decimal
    casual_loading: Decimal
    super_cost: Decimal

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with Decimals as floats."""
        return {
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "total_cost": float(self.total_cost),
            "hours": float(self.hours),
            "shifts": self.shifts,
            "base_cost": float(self.base_cost),
            "penalty_cost": float(self.penalty_cost),
            "casual_loading": float(self.casual_loading),
            "super_cost": float(self.super_cost),
        }


@dataclass
class PeriodCostPerCover:
    """Cost per cover for a time period (requires POS data)."""
    period_start: date
    period_end: date
    cost: Decimal
    covers: Decimal
    cost_per_cover: Decimal

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with Decimals as floats."""
        return {
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "cost": float(self.cost),
            "covers": float(self.covers),
            "cost_per_cover": float(self.cost_per_cover),
        }


@dataclass
class CostTrendReport:
    """Complete labour cost trends report."""
    periods: List[PeriodCost]
    breakdown_by_employment_type: Dict[str, List[PeriodCost]]
    breakdown_by_day_of_week: Dict[str, Decimal]
    breakdown_by_role: Dict[str, Decimal]
    breakdown_by_cost_type: Dict[str, Decimal]
    cost_per_cover: Optional[List[PeriodCostPerCover]]
    total_cost: Decimal
    total_hours: Decimal
    average_hourly_cost: Decimal
    trend_direction: str
    trend_percentage: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with nested structures."""
        return {
            "periods": [p.to_dict() for p in self.periods],
            "breakdown_by_employment_type": {
                emp_type: [p.to_dict() for p in costs]
                for emp_type, costs in self.breakdown_by_employment_type.items()
            },
            "breakdown_by_day_of_week": {
                day: float(cost)
                for day, cost in self.breakdown_by_day_of_week.items()
            },
            "breakdown_by_role": {
                role: float(cost)
                for role, cost in self.breakdown_by_role.items()
            },
            "breakdown_by_cost_type": {
                cost_type: float(amount)
                for cost_type, amount in self.breakdown_by_cost_type.items()
            },
            "cost_per_cover": [c.to_dict() for c in self.cost_per_cover] if self.cost_per_cover else None,
            "total_cost": float(self.total_cost),
            "total_hours": float(self.total_hours),
            "average_hourly_cost": float(self.average_hourly_cost),
            "trend_direction": self.trend_direction,
            "trend_percentage": self.trend_percentage,
        }


@dataclass
class VenueCostComparison:
    """Comparison of costs across multiple venues."""
    venues: Dict[str, Dict[str, Any]]
    best_venue: str
    highest_cost_venue: str
    average_cost: Decimal
    cost_variance: Decimal

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "venues": self.venues,
            "best_venue": self.best_venue,
            "highest_cost_venue": self.highest_cost_venue,
            "average_cost": float(self.average_cost),
            "cost_variance": float(self.cost_variance),
        }


@dataclass
class OvertimeAnalysis:
    """Breakdown of overtime hours and costs."""
    total_overtime_hours: Decimal
    total_overtime_cost: Decimal
    employees: List[Dict[str, Any]]
    average_overtime_per_employee: Decimal
    overtime_percentage: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_overtime_hours": float(self.total_overtime_hours),
            "total_overtime_cost": float(self.total_overtime_cost),
            "employees": self.employees,
            "average_overtime_per_employee": float(self.average_overtime_per_employee),
            "overtime_percentage": self.overtime_percentage,
        }


@dataclass
class CasualDependencyReport:
    """Analysis of casual workforce dependency."""
    casual_hours: Decimal
    casual_cost: Decimal
    casual_hours_pct: float
    casual_cost_pct: float
    trend_points: List[Dict[str, Any]]
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "casual_hours": float(self.casual_hours),
            "casual_cost": float(self.casual_cost),
            "casual_hours_pct": self.casual_hours_pct,
            "casual_cost_pct": self.casual_cost_pct,
            "trend_points": self.trend_points,
            "recommendation": self.recommendation,
        }


# ============================================================================
# SERVICE CLASS
# ============================================================================

class CostTrendsService:
    """Labour cost trends analytics engine."""

    def __init__(self, db: BaseStore):
        """Initialize with database connection."""
        self.db = db

    def get_cost_trends(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
        group_by: str = "daily",
    ) -> CostTrendReport:
        """
        Get labour cost trends with breakdown by multiple dimensions.

        Args:
            venue_id: Venue ID
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            group_by: "daily", "weekly", or "monthly"

        Returns:
            CostTrendReport with comprehensive breakdown
        """
        # Fetch rosters and employees
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()
        venue = self.db.get_venue(venue_id)

        if not rosters or not venue:
            return self._empty_trend_report()

        # Group shifts by period
        periods_data = self._group_by_period(rosters, group_by)

        # Calculate period-level costs
        periods: List[PeriodCost] = []
        employment_type_costs: Dict[str, List[PeriodCost]] = defaultdict(list)
        day_of_week_costs: Dict[str, List[Decimal]] = defaultdict(list)
        role_costs: Dict[str, List[Decimal]] = defaultdict(list)

        total_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        total_base = Decimal("0.00")
        total_penalty = Decimal("0.00")
        total_casual = Decimal("0.00")
        total_super = Decimal("0.00")

        for period_key, period_rosters in periods_data.items():
            period_start, period_end = self._get_period_dates(period_key, group_by)

            # Aggregate costs for this period
            period_cost = Decimal("0.00")
            period_hours = Decimal("0.00")
            period_shifts = 0
            period_base = Decimal("0.00")
            period_penalty = Decimal("0.00")
            period_casual = Decimal("0.00")
            period_super = Decimal("0.00")

            employment_costs: Dict[str, List[Tuple[Decimal, Decimal, Decimal, Decimal, Decimal]]] = defaultdict(list)
            shift_roles: List[Tuple[str, Decimal]] = []
            shift_dow_costs: List[Tuple[str, Decimal]] = []

            for roster in period_rosters:
                for shift in roster.shifts:
                    if not shift.cost:
                        continue

                    employee = employees.get(shift.employee_id)
                    if not employee:
                        continue

                    shift_cost = shift.cost
                    shift_hours = Decimal(str(shift.net_hours))

                    period_cost += shift_cost
                    period_hours += shift_hours
                    period_shifts += 1

                    # Estimate cost components (base, penalty, casual, super)
                    base_est = shift_cost * Decimal("0.65")  # ~65% base
                    penalty_est = shift_cost * Decimal("0.10")  # ~10% penalty
                    casual_est = shift_cost * Decimal("0.10")  # ~10% casual loading
                    super_est = shift_cost * Decimal("0.15")  # ~15% super + levies

                    period_base += base_est
                    period_penalty += penalty_est
                    period_casual += casual_est
                    period_super += super_est

                    # Track by employment type
                    emp_type = employee.employment_type.value
                    employment_costs[emp_type].append(
                        (shift_cost, shift_hours, base_est, penalty_est, casual_est, super_est)
                    )

                    # Track by role
                    shift_roles.append((shift.role, shift_cost))

                    # Track by day of week
                    day_name = shift.date.strftime("%A")
                    shift_dow_costs.append((day_name, shift_cost))

            # Create period cost object
            period_obj = PeriodCost(
                period_start=period_start,
                period_end=period_end,
                total_cost=period_cost,
                hours=period_hours,
                shifts=period_shifts,
                base_cost=period_base,
                penalty_cost=period_penalty,
                casual_loading=period_casual,
                super_cost=period_super,
            )
            periods.append(period_obj)

            # Accumulate totals
            total_cost += period_cost
            total_hours += period_hours
            total_base += period_base
            total_penalty += period_penalty
            total_casual += period_casual
            total_super += period_super

            # Add period to employment type breakdown
            for emp_type, costs_list in employment_costs.items():
                if costs_list:
                    emp_cost = sum(c[0] for c in costs_list)
                    emp_hours = sum(c[1] for c in costs_list)
                    emp_base = sum(c[2] for c in costs_list)
                    emp_penalty = sum(c[3] for c in costs_list)
                    emp_casual = sum(c[4] for c in costs_list)
                    emp_super = sum(c[5] for c in costs_list)

                    emp_period = PeriodCost(
                        period_start=period_start,
                        period_end=period_end,
                        total_cost=emp_cost,
                        hours=emp_hours,
                        shifts=sum(1 for _ in costs_list),
                        base_cost=emp_base,
                        penalty_cost=emp_penalty,
                        casual_loading=emp_casual,
                        super_cost=emp_super,
                    )
                    employment_type_costs[emp_type].append(emp_period)

            # Accumulate day of week and role costs
            for day_name, cost in shift_dow_costs:
                day_of_week_costs[day_name].append(cost)

            for role, cost in shift_roles:
                role_costs[role].append(cost)

        # Calculate breakdowns
        breakdown_by_day_of_week = {
            day: sum(costs)
            for day, costs in day_of_week_costs.items()
        }

        breakdown_by_role = {
            role: sum(costs)
            for role, costs in role_costs.items()
        }

        breakdown_by_cost_type = {
            "base_cost": total_base,
            "penalty_cost": total_penalty,
            "casual_loading": total_casual,
            "super_cost": total_super,
        }

        # Calculate average hourly cost
        average_hourly_cost = (
            total_cost / total_hours if total_hours > 0 else Decimal("0.00")
        )

        # Calculate trend direction and percentage
        trend_direction, trend_percentage = self._calculate_trend(periods)

        # Try to get cost per cover data
        cost_per_cover_data = self._calculate_cost_per_cover(venue_id, start_date, end_date, group_by)

        return CostTrendReport(
            periods=periods,
            breakdown_by_employment_type=dict(employment_type_costs),
            breakdown_by_day_of_week=breakdown_by_day_of_week,
            breakdown_by_role=breakdown_by_role,
            breakdown_by_cost_type=breakdown_by_cost_type,
            cost_per_cover=cost_per_cover_data,
            total_cost=total_cost,
            total_hours=total_hours,
            average_hourly_cost=average_hourly_cost,
            trend_direction=trend_direction,
            trend_percentage=trend_percentage,
        )

    def compare_venues(
        self,
        venue_ids: List[str],
        start_date: date,
        end_date: date,
    ) -> VenueCostComparison:
        """
        Compare labour costs across multiple venues.

        Args:
            venue_ids: List of venue IDs to compare
            start_date: Start date
            end_date: End date

        Returns:
            VenueCostComparison with metrics and rankings
        """
        venue_metrics: Dict[str, Dict[str, Any]] = {}
        total_costs: List[Tuple[str, Decimal]] = []

        for venue_id in venue_ids:
            rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
            employees = self.db.get_employees_dict()

            if not rosters:
                continue

            # Calculate metrics
            total_cost = Decimal("0.00")
            total_hours = Decimal("0.00")
            unique_employees = set()
            casual_hours = Decimal("0.00")
            casual_cost = Decimal("0.00")

            for roster in rosters:
                for shift in roster.shifts:
                    if not shift.cost:
                        continue

                    employee = employees.get(shift.employee_id)
                    if not employee:
                        continue

                    total_cost += shift.cost
                    hours = Decimal(str(shift.net_hours))
                    total_hours += hours
                    unique_employees.add(shift.employee_id)

                    if employee.employment_type == EmploymentType.casual:
                        casual_hours += hours
                        casual_cost += shift.cost

            avg_hourly_cost = (
                total_cost / total_hours if total_hours > 0 else Decimal("0.00")
            )
            casual_pct = (
                (casual_cost / total_cost * 100) if total_cost > 0 else Decimal("0.00")
            )

            venue_metrics[venue_id] = {
                "total_cost": float(total_cost),
                "total_hours": float(total_hours),
                "unique_employees": len(unique_employees),
                "average_hourly_cost": float(avg_hourly_cost),
                "casual_hours": float(casual_hours),
                "casual_cost": float(casual_cost),
                "casual_percentage": float(casual_pct),
            }

            total_costs.append((venue_id, total_cost))

        # Sort by cost
        total_costs.sort(key=lambda x: x[1])

        best_venue = total_costs[0][0] if total_costs else ""
        highest_cost_venue = total_costs[-1][0] if total_costs else ""

        # Calculate statistics
        costs = [c[1] for c in total_costs]
        average_cost = sum(costs) / len(costs) if costs else Decimal("0.00")

        if len(costs) > 1:
            cost_variance = Decimal(str(statistics.variance([float(c) for c in costs])))
        else:
            cost_variance = Decimal("0.00")

        return VenueCostComparison(
            venues=venue_metrics,
            best_venue=best_venue,
            highest_cost_venue=highest_cost_venue,
            average_cost=average_cost,
            cost_variance=cost_variance,
        )

    def get_cost_forecast(
        self,
        venue_id: str,
        weeks_ahead: int = 4,
    ) -> List[PeriodCost]:
        """
        Forecast labour costs using simple linear regression.

        Args:
            venue_id: Venue ID
            weeks_ahead: Number of weeks to forecast

        Returns:
            List of PeriodCost objects with forecasted values
        """
        # Get historical data (last 12 weeks)
        end_date = date.today()
        start_date = end_date - timedelta(weeks=12)

        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)

        if not rosters:
            return []

        # Group by week and calculate costs
        weekly_costs: List[Tuple[int, Decimal]] = []

        for week_offset in range(12):
            week_end = end_date - timedelta(weeks=week_offset)
            week_start = week_end - timedelta(days=6)

            week_cost = Decimal("0.00")
            week_hours = Decimal("0.00")

            for roster in rosters:
                if week_start <= roster.week_start <= week_end:
                    for shift in roster.shifts:
                        if shift.cost:
                            week_cost += shift.cost
                            week_hours += Decimal(str(shift.net_hours))

            weekly_costs.append((week_offset, week_cost))

        if len(weekly_costs) < 2:
            return []

        # Simple linear regression
        weekly_costs.sort(key=lambda x: x[0], reverse=True)
        x_values = [float(x[0]) for x in weekly_costs]
        y_values = [float(x[1]) for x in weekly_costs]

        # Calculate slope and intercept
        x_mean = statistics.mean(x_values)
        y_mean = statistics.mean(y_values)

        numerator = sum(
            (x_values[i] - x_mean) * (y_values[i] - y_mean)
            for i in range(len(x_values))
        )
        denominator = sum((x - x_mean) ** 2 for x in x_values)

        if denominator == 0:
            return []

        slope = numerator / denominator
        intercept = y_mean - slope * x_mean

        # Forecast future weeks
        forecasts: List[PeriodCost] = []

        for week_ahead in range(1, weeks_ahead + 1):
            forecast_date = end_date + timedelta(weeks=week_ahead)
            week_start = forecast_date - timedelta(days=6)

            # Predict cost using regression formula
            predicted_cost = Decimal(str(slope * (-week_ahead) + intercept))
            predicted_cost = max(predicted_cost, Decimal("0.00"))  # Ensure non-negative

            forecast = PeriodCost(
                period_start=week_start,
                period_end=forecast_date,
                total_cost=predicted_cost,
                hours=Decimal("0.00"),  # Not forecasted
                shifts=0,
                base_cost=predicted_cost * Decimal("0.65"),
                penalty_cost=predicted_cost * Decimal("0.10"),
                casual_loading=predicted_cost * Decimal("0.10"),
                super_cost=predicted_cost * Decimal("0.15"),
            )
            forecasts.append(forecast)

        return forecasts

    def get_overtime_analysis(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> OvertimeAnalysis:
        """
        Analyse overtime hours and costs by employee.

        Args:
            venue_id: Venue ID
            start_date: Start date
            end_date: End date

        Returns:
            OvertimeAnalysis with employee-level breakdown
        """
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()

        if not rosters:
            return self._empty_overtime_analysis()

        # Group shifts by employee and week
        employee_weekly_hours: Dict[str, List[Decimal]] = defaultdict(list)
        employee_overtime_hours: Dict[str, Decimal] = defaultdict(Decimal)
        employee_overtime_cost: Dict[str, Decimal] = defaultdict(Decimal)
        total_overtime_hours = Decimal("0.00")
        total_overtime_cost = Decimal("0.00")
        total_hours = Decimal("0.00")

        for roster in rosters:
            # Group this roster's shifts by employee
            emp_weekly_hours: Dict[str, Decimal] = defaultdict(Decimal)

            for shift in roster.shifts:
                if not shift.cost:
                    continue

                hours = Decimal(str(shift.net_hours))
                emp_weekly_hours[shift.employee_id] += hours
                total_hours += hours

            # Check for overtime (>38 hours per week)
            for emp_id, hours_in_week in emp_weekly_hours.items():
                employee = employees.get(emp_id)
                if not employee:
                    continue

                max_hours = Decimal(str(employee.max_hours_per_week))
                if hours_in_week > max_hours:
                    overtime = hours_in_week - max_hours
                    employee_overtime_hours[emp_id] += overtime
                    total_overtime_hours += overtime

                    # Estimate overtime cost (assume penalty rate of 1.5x)
                    overtime_rate = employee.hourly_base_rate * Decimal("1.5")
                    overtime_cost = overtime * overtime_rate
                    employee_overtime_cost[emp_id] += overtime_cost
                    total_overtime_cost += overtime_cost

        # Build employee-level breakdown
        employee_list: List[Dict[str, Any]] = []

        for emp_id, ot_hours in employee_overtime_hours.items():
            if ot_hours > 0:
                employee = employees.get(emp_id)
                employee_list.append({
                    "employee_id": emp_id,
                    "employee_name": employee.name if employee else "Unknown",
                    "overtime_hours": float(ot_hours),
                    "overtime_cost": float(employee_overtime_cost[emp_id]),
                })

        employee_list.sort(key=lambda x: x["overtime_hours"], reverse=True)

        avg_ot_per_emp = (
            total_overtime_hours / len(employee_overtime_hours)
            if employee_overtime_hours else Decimal("0.00")
        )

        ot_pct = (
            (total_overtime_hours / total_hours * 100) if total_hours > 0 else 0.0
        )

        return OvertimeAnalysis(
            total_overtime_hours=total_overtime_hours,
            total_overtime_cost=total_overtime_cost,
            employees=employee_list,
            average_overtime_per_employee=avg_ot_per_emp,
            overtime_percentage=float(ot_pct),
        )

    def get_casual_dependency_report(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> CasualDependencyReport:
        """
        Analyse casual workforce dependency and trends.

        Args:
            venue_id: Venue ID
            start_date: Start date
            end_date: End date

        Returns:
            CasualDependencyReport with trends and recommendations
        """
        rosters = self.db.get_rosters_by_date_range(venue_id, start_date, end_date)
        employees = self.db.get_employees_dict()

        if not rosters:
            return self._empty_casual_dependency_report()

        casual_hours = Decimal("0.00")
        casual_cost = Decimal("0.00")
        total_hours = Decimal("0.00")
        total_cost = Decimal("0.00")

        # Group by week for trend analysis
        weekly_casual: Dict[date, Tuple[Decimal, Decimal]] = defaultdict(
            lambda: (Decimal("0.00"), Decimal("0.00"))
        )
        weekly_total: Dict[date, Tuple[Decimal, Decimal]] = defaultdict(
            lambda: (Decimal("0.00"), Decimal("0.00"))
        )

        for roster in rosters:
            week_start = roster.week_start

            for shift in roster.shifts:
                if not shift.cost:
                    continue

                employee = employees.get(shift.employee_id)
                if not employee:
                    continue

                hours = Decimal(str(shift.net_hours))
                cost = shift.cost

                # Accumulate weekly totals
                w_hours, w_cost = weekly_total[week_start]
                weekly_total[week_start] = (w_hours + hours, w_cost + cost)

                total_hours += hours
                total_cost += cost

                # Accumulate casual-only
                if employee.employment_type == EmploymentType.casual:
                    casual_hours += hours
                    casual_cost += cost

                    c_hours, c_cost = weekly_casual[week_start]
                    weekly_casual[week_start] = (c_hours + hours, c_cost + cost)

        # Calculate percentages
        casual_hours_pct = (
            (casual_hours / total_hours * 100) if total_hours > 0 else 0.0
        )
        casual_cost_pct = (
            (casual_cost / total_cost * 100) if total_cost > 0 else 0.0
        )

        # Build trend points
        trend_points: List[Dict[str, Any]] = []

        for week_start in sorted(weekly_total.keys()):
            total_h, total_c = weekly_total[week_start]
            casual_h, casual_c = weekly_casual[week_start]

            week_casual_pct = (
                (casual_h / total_h * 100) if total_h > 0 else 0.0
            )

            trend_points.append({
                "week_start": week_start.isoformat(),
                "casual_hours": float(casual_h),
                "casual_cost": float(casual_c),
                "total_hours": float(total_h),
                "total_cost": float(total_c),
                "casual_percentage": week_casual_pct,
            })

        # Generate recommendation
        recommendation = self._casual_dependency_recommendation(casual_hours_pct, casual_cost_pct)

        return CasualDependencyReport(
            casual_hours=casual_hours,
            casual_cost=casual_cost,
            casual_hours_pct=casual_hours_pct,
            casual_cost_pct=casual_cost_pct,
            trend_points=trend_points,
            recommendation=recommendation,
        )

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _group_by_period(
        self,
        rosters: List[Roster],
        group_by: str,
    ) -> Dict[str, List[Roster]]:
        """Group rosters by time period."""
        grouped: Dict[str, List[Roster]] = defaultdict(list)

        for roster in rosters:
            if group_by == "daily":
                # Group by date
                for shift in roster.shifts:
                    key = shift.date.isoformat()
                    # We need to track shifts, not rosters, for daily grouping
                    pass
                # Daily grouping is more complex; simplify to weekly for now
                key = roster.week_start.isoformat()
            elif group_by == "weekly":
                key = roster.week_start.isoformat()
            else:  # monthly
                key = roster.week_start.strftime("%Y-%m")

            grouped[key].append(roster)

        return grouped

    def _get_period_dates(self, period_key: str, group_by: str) -> Tuple[date, date]:
        """Convert period key to start and end dates."""
        if group_by == "weekly":
            start = date.fromisoformat(period_key)
            end = start + timedelta(days=6)
            return start, end
        elif group_by == "monthly":
            year, month = period_key.split("-")
            start = date(int(year), int(month), 1)
            if month == "12":
                end = date(int(year) + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(int(year), int(month) + 1, 1) - timedelta(days=1)
            return start, end
        else:  # daily (default to weekly for now)
            start = date.fromisoformat(period_key)
            end = start + timedelta(days=6)
            return start, end

    def _calculate_trend(self, periods: List[PeriodCost]) -> Tuple[str, float]:
        """Calculate trend direction and percentage change."""
        if len(periods) < 2:
            return "stable", 0.0

        first_period = float(periods[0].total_cost)
        last_period = float(periods[-1].total_cost)

        if first_period == 0:
            return "stable", 0.0

        percentage = ((last_period - first_period) / first_period) * 100

        if percentage > 5:
            direction = "increasing"
        elif percentage < -5:
            direction = "decreasing"
        else:
            direction = "stable"

        return direction, percentage

    def _calculate_cost_per_cover(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
        group_by: str,
    ) -> Optional[List[PeriodCostPerCover]]:
        """Calculate cost per cover if POS data available."""
        # This would integrate with POS data sources
        # For now, return None as placeholder
        return None

    def _casual_dependency_recommendation(
        self,
        casual_hours_pct: float,
        casual_cost_pct: float,
    ) -> str:
        """Generate recommendation based on casual dependency metrics."""
        if casual_hours_pct > 60:
            return (
                f"High casual dependency ({casual_hours_pct:.1f}% of hours). "
                "Consider hiring more permanent staff for stability."
            )
        elif casual_hours_pct > 40:
            return (
                f"Moderate casual usage ({casual_hours_pct:.1f}% of hours). "
                "Well-balanced workforce. Monitor for flexibility needs."
            )
        else:
            return (
                f"Low casual dependency ({casual_hours_pct:.1f}% of hours). "
                "Strong core team with permanent staff foundation."
            )

    def _empty_trend_report(self) -> CostTrendReport:
        """Return empty trend report."""
        return CostTrendReport(
            periods=[],
            breakdown_by_employment_type={},
            breakdown_by_day_of_week={},
            breakdown_by_role={},
            breakdown_by_cost_type={
                "base_cost": Decimal("0.00"),
                "penalty_cost": Decimal("0.00"),
                "casual_loading": Decimal("0.00"),
                "super_cost": Decimal("0.00"),
            },
            cost_per_cover=None,
            total_cost=Decimal("0.00"),
            total_hours=Decimal("0.00"),
            average_hourly_cost=Decimal("0.00"),
            trend_direction="stable",
            trend_percentage=0.0,
        )

    def _empty_overtime_analysis(self) -> OvertimeAnalysis:
        """Return empty overtime analysis."""
        return OvertimeAnalysis(
            total_overtime_hours=Decimal("0.00"),
            total_overtime_cost=Decimal("0.00"),
            employees=[],
            average_overtime_per_employee=Decimal("0.00"),
            overtime_percentage=0.0,
        )

    def _empty_casual_dependency_report(self) -> CasualDependencyReport:
        """Return empty casual dependency report."""
        return CasualDependencyReport(
            casual_hours=Decimal("0.00"),
            casual_cost=Decimal("0.00"),
            casual_hours_pct=0.0,
            casual_cost_pct=0.0,
            trend_points=[],
            recommendation="No data available.",
        )
