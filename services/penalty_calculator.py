"""
Fast penalty rate calculator for real-time UI feedback during roster editing.

Calculates shift costs including penalty rates, loading, casual loading, and
superannuation with instant feedback for UI interactions. No heavy DB queries —
just rate calculation from employee data.

Key features:
- Instant penalty breakdown for single shifts
- Quick estimates without employee lookup
- Time/day comparison for informed scheduling
- Find cheapest time slots for given duration
"""

from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple
import logging

from rosteriq.models import (
    Employee, EmploymentType, AwardLevel, State,
)
from rosteriq.award_rules import (
    get_day_type, get_penalty_multiplier, get_minimum_break_minutes,
    PENALTY_MULTIPLIERS, EVENING_LOADING_THRESHOLDS, DayType,
)
from rosteriq.database import get_db

logger = logging.getLogger(__name__)

# Superannuation rate for FY 2025-26
SUPER_RATE = Decimal("0.115")
CASUAL_LOADING_RATE = Decimal("0.25")


@dataclass
class PenaltyDetail:
    """Details of a single penalty or loading applied to a shift."""
    name: str
    multiplier: float
    hours_applicable: float
    additional_cost: Decimal
    description: str


@dataclass
class CostComparison:
    """Comparison of actual cost vs weekday baseline."""
    weekday_cost: Decimal
    actual_cost: Decimal
    premium: Decimal
    premium_pct: float


@dataclass
class PenaltyBreakdown:
    """Complete penalty and cost breakdown for a shift."""
    employee_name: str
    base_hourly_rate: Decimal
    date: str
    day_type: str
    start_time: str
    end_time: str
    total_hours: float
    break_minutes: int
    net_hours: float
    penalties_applied: List[PenaltyDetail] = field(default_factory=list)
    base_cost: Decimal = Decimal("0.00")
    penalty_cost: Decimal = Decimal("0.00")
    casual_loading: Decimal = Decimal("0.00")
    super_contribution: Decimal = Decimal("0.00")
    total_hourly_cost: Decimal = Decimal("0.00")
    total_shift_cost: Decimal = Decimal("0.00")
    cost_comparison: Optional[CostComparison] = None


class PenaltyCalculator:
    """Fast penalty rate calculator for real-time shift costing."""

    def __init__(self):
        self.db = get_db()

    def calculate(
        self,
        employee_id: str,
        date_str: str,
        start_time_str: str,
        end_time_str: str,
        role: Optional[str] = None,
        break_minutes: int = 0,
    ) -> PenaltyBreakdown:
        """
        Calculate full penalty breakdown for a shift.

        Designed for instant UI feedback — minimal DB lookups.

        Args:
            employee_id: Employee ID
            date_str: Date in YYYY-MM-DD format
            start_time_str: Start time in HH:MM format
            end_time_str: End time in HH:MM format
            role: Optional role name (not used in calculation, for context)
            break_minutes: Unpaid break duration in minutes (default 0)

        Returns:
            PenaltyBreakdown with full cost analysis
        """
        # Fetch employee
        employee = self.db.get_employee(employee_id)
        if not employee:
            raise ValueError(f"Employee {employee_id} not found")

        # Parse times
        shift_date = self._parse_date(date_str)
        start_time = self._parse_time(start_time_str)
        end_time = self._parse_time(end_time_str)

        # Calculate shift duration
        total_hours = self._calculate_shift_duration(start_time, end_time)
        net_hours = max(0, total_hours - break_minutes / 60.0)

        # Determine day type
        day_type = get_day_type(shift_date, employee.state)
        day_type_str = day_type.value

        # Calculate penalties
        penalties = self._calculate_penalties(
            employee, day_type, start_time.hour, net_hours
        )

        # Calculate costs
        base_cost = self._calculate_base_cost(employee.hourly_base_rate, net_hours)
        penalty_cost = self._calculate_penalty_cost(base_cost, penalties, employee)
        casual_loading = self._calculate_casual_loading(employee, base_cost)
        gross_pay = base_cost + penalty_cost + casual_loading
        super_contribution = self._calculate_super(gross_pay)
        total_cost = gross_pay + super_contribution

        # Calculate effective hourly rate
        total_hourly_cost = (
            total_cost / Decimal(str(net_hours))
            if net_hours > 0
            else Decimal("0.00")
        )

        # Calculate cost comparison vs weekday
        cost_comparison = self._calculate_cost_comparison(
            employee, base_cost, day_type, start_time.hour
        )

        return PenaltyBreakdown(
            employee_name=employee.name,
            base_hourly_rate=employee.hourly_base_rate,
            date=date_str,
            day_type=day_type_str,
            start_time=start_time_str,
            end_time=end_time_str,
            total_hours=total_hours,
            break_minutes=break_minutes,
            net_hours=net_hours,
            penalties_applied=penalties,
            base_cost=base_cost,
            penalty_cost=penalty_cost,
            casual_loading=casual_loading,
            super_contribution=super_contribution,
            total_hourly_cost=total_hourly_cost,
            total_shift_cost=total_cost,
            cost_comparison=cost_comparison,
        )

    def quick_estimate(
        self,
        award_level: str,
        employment_type: str,
        state: str,
        date_str: str,
        start_time_str: str,
        end_time_str: str,
        hourly_rate: Decimal,
        break_minutes: int = 0,
    ) -> PenaltyBreakdown:
        """
        Quick cost estimate without employee lookup.

        For "what would it cost?" calculator — no DB access needed.

        Args:
            award_level: Award level (e.g. "level_1")
            employment_type: "full_time", "part_time", or "casual"
            state: State code (e.g. "vic")
            date_str: Date in YYYY-MM-DD format
            start_time_str: Start time in HH:MM format
            end_time_str: End time in HH:MM format
            hourly_rate: Base hourly rate
            break_minutes: Unpaid break duration

        Returns:
            PenaltyBreakdown without employee lookup
        """
        # Create minimal employee object for calculation
        emp_type = EmploymentType(employment_type)
        emp_state = State(state)

        shift_date = self._parse_date(date_str)
        start_time = self._parse_time(start_time_str)
        end_time = self._parse_time(end_time_str)

        total_hours = self._calculate_shift_duration(start_time, end_time)
        net_hours = max(0, total_hours - break_minutes / 60.0)

        day_type = get_day_type(shift_date, emp_state)
        day_type_str = day_type.value

        # Calculate penalties
        penalties = self._calculate_penalties_for_type(
            emp_type, day_type, start_time.hour, net_hours
        )

        # Calculate costs
        base_cost = self._calculate_base_cost(hourly_rate, net_hours)
        penalty_cost = self._calculate_penalty_cost_for_type(
            base_cost, penalties, emp_type
        )
        casual_loading = self._calculate_casual_loading_for_type(emp_type, base_cost)
        gross_pay = base_cost + penalty_cost + casual_loading
        super_contribution = self._calculate_super(gross_pay)
        total_cost = gross_pay + super_contribution

        total_hourly_cost = (
            total_cost / Decimal(str(net_hours))
            if net_hours > 0
            else Decimal("0.00")
        )

        cost_comparison = self._calculate_cost_comparison_for_type(
            hourly_rate, base_cost, day_type, start_time.hour, emp_type
        )

        return PenaltyBreakdown(
            employee_name="(estimate)",
            base_hourly_rate=hourly_rate,
            date=date_str,
            day_type=day_type_str,
            start_time=start_time_str,
            end_time=end_time_str,
            total_hours=total_hours,
            break_minutes=break_minutes,
            net_hours=net_hours,
            penalties_applied=penalties,
            base_cost=base_cost,
            penalty_cost=penalty_cost,
            casual_loading=casual_loading,
            super_contribution=super_contribution,
            total_hourly_cost=total_hourly_cost,
            total_shift_cost=total_cost,
            cost_comparison=cost_comparison,
        )

    def compare_times(
        self,
        employee_id: str,
        date_str: str,
        options: List[Tuple[str, str]],
    ) -> List[PenaltyBreakdown]:
        """
        Compare cost of same shift at different times.

        Returns breakdown for each time option for easy comparison.

        Args:
            employee_id: Employee ID
            date_str: Date in YYYY-MM-DD format
            options: List of (start_time, end_time) tuples in HH:MM format

        Returns:
            List of PenaltyBreakdown, one per time option
        """
        results = []
        for start_time_str, end_time_str in options:
            breakdown = self.calculate(
                employee_id, date_str, start_time_str, end_time_str
            )
            results.append(breakdown)
        return results

    def compare_days(
        self,
        employee_id: str,
        start_time_str: str,
        end_time_str: str,
        dates: List[str],
    ) -> List[PenaltyBreakdown]:
        """
        Compare cost of same shift on different days.

        Useful for identifying cheapest days to schedule.

        Args:
            employee_id: Employee ID
            start_time_str: Start time in HH:MM format
            end_time_str: End time in HH:MM format
            dates: List of dates in YYYY-MM-DD format

        Returns:
            List of PenaltyBreakdown, one per date
        """
        results = []
        for date_str in dates:
            breakdown = self.calculate(
                employee_id, date_str, start_time_str, end_time_str
            )
            results.append(breakdown)
        return results

    def cheapest_time_slot(
        self,
        employee_id: str,
        date_str: str,
        duration_hours: float,
    ) -> Tuple[str, str, Decimal]:
        """
        Find the cheapest time window for a given duration on a given day.

        Returns start_time, end_time, and total cost.

        Args:
            employee_id: Employee ID
            date_str: Date in YYYY-MM-DD format
            duration_hours: Desired shift duration in hours

        Returns:
            Tuple of (start_time_str, end_time_str, total_cost)
        """
        employee = self.db.get_employee(employee_id)
        if not employee:
            raise ValueError(f"Employee {employee_id} not found")

        shift_date = self._parse_date(date_str)

        # Check each hour of the day (0-23)
        cheapest_cost = None
        cheapest_start = None
        cheapest_end = None

        for hour in range(24):
            # Skip if shift would extend past midnight
            end_hour = hour + int(duration_hours)
            if end_hour > 23:
                continue

            start_time = time(hour, 0)
            end_time = time(end_hour + int((duration_hours % 1) * 60), 0)

            start_str = start_time.strftime("%H:%M")
            end_str = end_time.strftime("%H:%M")

            try:
                breakdown = self.calculate(
                    employee_id, date_str, start_str, end_str
                )
                cost = breakdown.total_shift_cost

                if cheapest_cost is None or cost < cheapest_cost:
                    cheapest_cost = cost
                    cheapest_start = start_str
                    cheapest_end = end_str
            except Exception:
                continue

        if cheapest_start is None:
            raise ValueError(f"Could not find valid time slot for {duration_hours}h")

        return (cheapest_start, cheapest_end, cheapest_cost)

    # ========================================================================
    # PRIVATE HELPER METHODS
    # ========================================================================

    def _parse_date(self, date_str: str) -> date:
        """Parse date string YYYY-MM-DD."""
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    def _parse_time(self, time_str: str) -> time:
        """Parse time string HH:MM."""
        return datetime.strptime(time_str, "%H:%M").time()

    def _calculate_shift_duration(self, start_time: time, end_time: time) -> float:
        """Calculate shift duration in hours, handling overnight shifts."""
        start_minutes = start_time.hour * 60 + start_time.minute
        end_minutes = end_time.hour * 60 + end_time.minute

        # Handle overnight shift
        if end_minutes < start_minutes:
            end_minutes += 24 * 60

        return (end_minutes - start_minutes) / 60.0

    def _calculate_penalties(
        self,
        employee: Employee,
        day_type: DayType,
        start_hour: int,
        net_hours: float,
    ) -> List[PenaltyDetail]:
        """Calculate all penalties applicable to shift."""
        penalties = []

        # Get base multiplier
        base_multiplier = PENALTY_MULTIPLIERS[employee.employment_type][day_type]
        base_multiplier_float = float(base_multiplier)

        # Add day type penalty
        if day_type == DayType.weekday:
            penalty_name = "Weekday"
            penalty_mult = 1.0
        elif day_type == DayType.saturday:
            penalty_name = "Saturday loading"
            penalty_mult = 0.25
            penalties.append(
                PenaltyDetail(
                    name=penalty_name,
                    multiplier=1.25,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),  # Will calc below
                    description="25% loading for Saturday work",
                )
            )
        elif day_type == DayType.sunday:
            penalty_name = "Sunday loading"
            penalty_mult = 0.50
            penalties.append(
                PenaltyDetail(
                    name=penalty_name,
                    multiplier=1.50,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="50% loading for Sunday work",
                )
            )
        elif day_type == DayType.public_holiday:
            penalty_name = "Public holiday"
            penalty_mult = 1.50
            penalties.append(
                PenaltyDetail(
                    name=penalty_name,
                    multiplier=2.50,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="150% loading for public holiday work",
                )
            )

        # Add casual loading if applicable
        if employee.employment_type == EmploymentType.casual:
            penalties.append(
                PenaltyDetail(
                    name="Casual loading",
                    multiplier=1.25,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="25% casual loading (included in base rate for casuals)",
                )
            )

        # Add evening/night loading (Monday-Friday only)
        if day_type == DayType.weekday:
            if start_hour >= 19 and start_hour < 24:
                penalties.append(
                    PenaltyDetail(
                        name="Evening loading (7pm-midnight)",
                        multiplier=1.15,
                        hours_applicable=net_hours,
                        additional_cost=Decimal("0.00"),
                        description="15% loading for work after 7pm",
                    )
                )
            elif start_hour >= 0 and start_hour < 7:
                penalties.append(
                    PenaltyDetail(
                        name="Late night loading (midnight-7am)",
                        multiplier=1.175,
                        hours_applicable=net_hours,
                        additional_cost=Decimal("0.00"),
                        description="17.5% loading for work between midnight and 7am",
                    )
                )

        return penalties

    def _calculate_penalties_for_type(
        self,
        employment_type: EmploymentType,
        day_type: DayType,
        start_hour: int,
        net_hours: float,
    ) -> List[PenaltyDetail]:
        """Calculate penalties without Employee object."""
        penalties = []

        if day_type == DayType.saturday:
            penalties.append(
                PenaltyDetail(
                    name="Saturday loading",
                    multiplier=1.25,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="25% loading for Saturday work",
                )
            )
        elif day_type == DayType.sunday:
            penalties.append(
                PenaltyDetail(
                    name="Sunday loading",
                    multiplier=1.50,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="50% loading for Sunday work",
                )
            )
        elif day_type == DayType.public_holiday:
            penalties.append(
                PenaltyDetail(
                    name="Public holiday",
                    multiplier=2.50,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="150% loading for public holiday work",
                )
            )

        if employment_type == EmploymentType.casual:
            penalties.append(
                PenaltyDetail(
                    name="Casual loading",
                    multiplier=1.25,
                    hours_applicable=net_hours,
                    additional_cost=Decimal("0.00"),
                    description="25% casual loading",
                )
            )

        if day_type == DayType.weekday:
            if start_hour >= 19 and start_hour < 24:
                penalties.append(
                    PenaltyDetail(
                        name="Evening loading (7pm-midnight)",
                        multiplier=1.15,
                        hours_applicable=net_hours,
                        additional_cost=Decimal("0.00"),
                        description="15% loading for work after 7pm",
                    )
                )
            elif start_hour >= 0 and start_hour < 7:
                penalties.append(
                    PenaltyDetail(
                        name="Late night loading (midnight-7am)",
                        multiplier=1.175,
                        hours_applicable=net_hours,
                        additional_cost=Decimal("0.00"),
                        description="17.5% loading for work between midnight and 7am",
                    )
                )

        return penalties

    def _calculate_base_cost(
        self, hourly_rate: Decimal, net_hours: float
    ) -> Decimal:
        """Calculate base cost without any multipliers."""
        return (hourly_rate * Decimal(str(net_hours))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    def _calculate_penalty_cost(
        self,
        base_cost: Decimal,
        penalties: List[PenaltyDetail],
        employee: Employee,
    ) -> Decimal:
        """Calculate additional penalty cost beyond base."""
        multiplier = PENALTY_MULTIPLIERS[employee.employment_type][
            self._get_day_type_from_penalties(penalties)
        ]

        if employee.employment_type == EmploymentType.casual:
            # For casuals, penalty is above the 1.25 base (which includes 25% loading)
            if multiplier > Decimal("1.25"):
                return (base_cost * (multiplier - Decimal("1.25"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            return Decimal("0.00")
        else:
            # For permanent staff, penalty is above 1.0
            if multiplier > Decimal("1.0"):
                return (base_cost * (multiplier - Decimal("1.0"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            return Decimal("0.00")

    def _calculate_penalty_cost_for_type(
        self,
        base_cost: Decimal,
        penalties: List[PenaltyDetail],
        employment_type: EmploymentType,
    ) -> Decimal:
        """Calculate penalty cost without Employee object."""
        day_type = self._get_day_type_from_penalties(penalties)
        multiplier = PENALTY_MULTIPLIERS[employment_type][day_type]

        if employment_type == EmploymentType.casual:
            if multiplier > Decimal("1.25"):
                return (base_cost * (multiplier - Decimal("1.25"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            return Decimal("0.00")
        else:
            if multiplier > Decimal("1.0"):
                return (base_cost * (multiplier - Decimal("1.0"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            return Decimal("0.00")

    def _calculate_casual_loading(
        self, employee: Employee, base_cost: Decimal
    ) -> Decimal:
        """Calculate casual loading (25% for casuals, 0 for others)."""
        if employee.employment_type == EmploymentType.casual:
            return (base_cost * CASUAL_LOADING_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        return Decimal("0.00")

    def _calculate_casual_loading_for_type(
        self, employment_type: EmploymentType, base_cost: Decimal
    ) -> Decimal:
        """Calculate casual loading without Employee object."""
        if employment_type == EmploymentType.casual:
            return (base_cost * CASUAL_LOADING_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        return Decimal("0.00")

    def _calculate_super(self, gross_pay: Decimal) -> Decimal:
        """Calculate superannuation at 11.5% of gross pay."""
        return (gross_pay * SUPER_RATE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    def _get_day_type_from_penalties(self, penalties: List[PenaltyDetail]) -> DayType:
        """Infer day type from penalties list."""
        penalty_names = {p.name.lower() for p in penalties}

        if any("public holiday" in n for n in penalty_names):
            return DayType.public_holiday
        elif any("sunday" in n for n in penalty_names):
            return DayType.sunday
        elif any("saturday" in n for n in penalty_names):
            return DayType.saturday
        else:
            return DayType.weekday

    def _calculate_cost_comparison(
        self,
        employee: Employee,
        base_cost: Decimal,
        day_type: DayType,
        start_hour: int,
    ) -> CostComparison:
        """Calculate cost comparison vs weekday baseline."""
        # Calculate what weekday cost would be
        weekday_multiplier = PENALTY_MULTIPLIERS[employee.employment_type][DayType.weekday]
        weekday_cost_base = base_cost * weekday_multiplier

        # Add casual loading and super if needed
        if employee.employment_type == EmploymentType.casual:
            weekday_casual = base_cost * CASUAL_LOADING_RATE
        else:
            weekday_casual = Decimal("0.00")

        weekday_gross = base_cost + (weekday_cost_base - base_cost) + weekday_casual
        weekday_total = weekday_gross + self._calculate_super(weekday_gross)

        # Current cost
        current_multiplier = PENALTY_MULTIPLIERS[employee.employment_type][day_type]
        current_cost_base = base_cost * current_multiplier

        if employee.employment_type == EmploymentType.casual:
            current_casual = base_cost * CASUAL_LOADING_RATE
        else:
            current_casual = Decimal("0.00")

        current_gross = base_cost + (current_cost_base - base_cost) + current_casual
        current_total = current_gross + self._calculate_super(current_gross)

        premium = current_total - weekday_total
        premium_pct = float(
            (premium / weekday_total * Decimal("100"))
            if weekday_total > 0
            else Decimal("0")
        )

        return CostComparison(
            weekday_cost=weekday_total.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            actual_cost=current_total.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            premium=premium.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            premium_pct=premium_pct,
        )

    def _calculate_cost_comparison_for_type(
        self,
        hourly_rate: Decimal,
        base_cost: Decimal,
        day_type: DayType,
        start_hour: int,
        employment_type: EmploymentType,
    ) -> CostComparison:
        """Calculate cost comparison without Employee object."""
        # Weekday calculation
        weekday_multiplier = PENALTY_MULTIPLIERS[employment_type][DayType.weekday]
        weekday_cost_base = base_cost * weekday_multiplier

        if employment_type == EmploymentType.casual:
            weekday_casual = base_cost * CASUAL_LOADING_RATE
        else:
            weekday_casual = Decimal("0.00")

        weekday_gross = base_cost + (weekday_cost_base - base_cost) + weekday_casual
        weekday_total = weekday_gross + self._calculate_super(weekday_gross)

        # Current calculation
        current_multiplier = PENALTY_MULTIPLIERS[employment_type][day_type]
        current_cost_base = base_cost * current_multiplier

        if employment_type == EmploymentType.casual:
            current_casual = base_cost * CASUAL_LOADING_RATE
        else:
            current_casual = Decimal("0.00")

        current_gross = base_cost + (current_cost_base - base_cost) + current_casual
        current_total = current_gross + self._calculate_super(current_gross)

        premium = current_total - weekday_total
        premium_pct = float(
            (premium / weekday_total * Decimal("100"))
            if weekday_total > 0
            else Decimal("0")
        )

        return CostComparison(
            weekday_cost=weekday_total.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            actual_cost=current_total.quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ),
            premium=premium.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            premium_pct=premium_pct,
        )
