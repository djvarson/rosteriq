"""
Comprehensive employee costing service for RosterIQ.

Provides detailed labour cost calculations including:
- Base hourly rates with penalty multipliers
- Casual loading (25% on top of base)
- Superannuation (11.5% of ordinary time earnings)
- WorkCover levies (state-specific)
- Payroll tax (state-specific, over threshold)
- Leave accrual costs (annual, personal/carer's, long service)
- Shift and roster-level cost breakdowns
- Cost comparison and optimization

All monetary values use Decimal for precision.
Complies with Fair Work Australia regulations for MA000009 (Hospitality Industry Award).
"""

from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, time, timedelta
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    EmploymentType,
    State,
)
from rosteriq.award_rules import (
    get_day_type,
    get_penalty_multiplier,
    DayType,
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown,
)


# ============================================================================
# CONSTANTS
# ============================================================================

# Superannuation rate for FY 2025-26
SUPER_RATE = Decimal("0.115")

# Casual loading rate (applies to casual employees on top of base)
CASUAL_LOADING_RATE = Decimal("0.25")

# WorkCover levy rates by state (annual rate %)
WORKCOVER_LEVIES = {
    State.nsw: Decimal("0.014"),   # 1.4%
    State.vic: Decimal("0.0127"),  # 1.27%
    State.qld: Decimal("0.012"),   # 1.2%
    State.sa: Decimal("0.015"),    # 1.5%
    State.wa: Decimal("0.016"),    # 1.6%
    State.tas: Decimal("0.018"),   # 1.8%
    State.nt: Decimal("0.020"),    # 2.0%
    State.act: Decimal("0.0135"),  # 1.35%
}

# Payroll tax: (threshold, rate)
PAYROLL_TAX_CONFIG = {
    State.nsw: (Decimal("1200000"), Decimal("0.0545")),      # NSW: 5.45% over $1.2M
    State.vic: (Decimal("900000"), Decimal("0.0485")),       # VIC: 4.85% over $900K
    State.qld: (Decimal("1300000"), Decimal("0.0475")),      # QLD: 4.75% over $1.3M
    State.sa: (Decimal("1500000"), Decimal("0.0495")),       # SA: 4.95% over $1.5M
    State.wa: (Decimal("1000000"), Decimal("0.055")),        # WA: 5.5% over $1M
    State.tas: (Decimal("1250000"), Decimal("0.04")),        # TAS: 4% over $1.25M
    State.nt: (Decimal("1500000"), Decimal("0.055")),        # NT: 5.5% over $1.5M
    State.act: (Decimal("2000000"), Decimal("0.0685")),      # ACT: 6.85% over $2M
}

# Leave accrual rates (% of ordinary time earnings per year)
LEAVE_ACCRUAL_RATES = {
    # Annual leave: 4 weeks/year = 7.69%
    "annual_leave": Decimal("0.0769"),
    # Personal/carer's leave: 2 weeks/year = 3.85%
    "personal_carers": Decimal("0.0385"),
    # Long service leave (varies by state, average ~1.67%)
    "long_service_leave": {
        State.nsw: Decimal("0.0167"),   # ~1.67%
        State.vic: Decimal("0.0167"),
        State.qld: Decimal("0.0167"),
        State.sa: Decimal("0.0167"),
        State.wa: Decimal("0.0167"),
        State.tas: Decimal("0.0167"),
        State.nt: Decimal("0.0167"),
        State.act: Decimal("0.0167"),
    }
}

# Thresholds for warnings
OVERTIME_WARNING_HOURS = 48.0  # Hours per week


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ShiftCostDetail:
    """Detailed cost breakdown for a single shift."""
    shift_id: str
    date: date
    start_time: time
    end_time: time
    hours: Decimal
    base_pay: Decimal
    penalty_pay: Decimal
    casual_loading: Decimal
    super_contribution: Decimal
    workcover_component: Decimal
    leave_accrual: Decimal
    total_cost: Decimal
    effective_hourly_rate: Decimal
    penalty_multiplier: Decimal


@dataclass
class EmployeeCostProjection:
    """Complete cost projection for an employee over multiple shifts."""
    employee_id: str
    employee_name: str
    employment_type: EmploymentType
    state: State

    total_hours: Decimal
    total_shifts: int

    # Cost components
    base_pay: Decimal
    penalty_pay: Decimal
    casual_loading: Decimal
    super_contribution: Decimal
    workcover_levy: Decimal
    payroll_tax_component: Decimal
    leave_accrual: Decimal

    # Totals
    total_cost: Decimal
    effective_hourly_rate: Decimal

    # Breakdown
    per_shift_breakdown: List[ShiftCostDetail] = field(default_factory=list)

    # Warnings
    warnings: List[str] = field(default_factory=list)


@dataclass
class RosterCostSummary:
    """Summary of total roster labour costs."""
    roster_id: str
    week_start: date
    week_end: date

    total_hours: Decimal
    total_shifts: int
    total_employees: int

    total_base_pay: Decimal
    total_penalty_pay: Decimal
    total_casual_loading: Decimal
    total_super_contribution: Decimal
    total_workcover_levy: Decimal
    total_payroll_tax: Decimal
    total_leave_accrual: Decimal

    total_cost: Decimal
    average_hourly_rate: Decimal

    employee_costs: List[EmployeeCostProjection] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ============================================================================
# SERVICE
# ============================================================================

class EmployeeCostingService:
    """Comprehensive employee costing calculator for RosterIQ."""

    @staticmethod
    def _get_workcover_levy(employee: Employee, gross_cost: Decimal) -> Decimal:
        """Calculate WorkCover levy component."""
        rate = WORKCOVER_LEVIES.get(employee.state, Decimal("0.015"))
        return (gross_cost * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def _get_leave_accrual(
        employee: Employee,
        ordinary_earnings: Decimal,
    ) -> Decimal:
        """Calculate leave accrual cost."""
        # Leave accrual applies to FT/PT (not casual in the same way)
        if employee.employment_type == EmploymentType.casual:
            # Casuals: accrual only on actual shifts worked
            annual_leave_loading = LEAVE_ACCRUAL_RATES["annual_leave"]
            personal_carers_loading = LEAVE_ACCRUAL_RATES["personal_carers"]
            lsl_rate = LEAVE_ACCRUAL_RATES["long_service_leave"].get(
                employee.state, Decimal("0.0167")
            )
        else:
            # FT/PT: full accrual
            annual_leave_loading = LEAVE_ACCRUAL_RATES["annual_leave"]
            personal_carers_loading = LEAVE_ACCRUAL_RATES["personal_carers"]
            lsl_rate = LEAVE_ACCRUAL_RATES["long_service_leave"].get(
                employee.state, Decimal("0.0167")
            )

        total_leave_loading = annual_leave_loading + personal_carers_loading + lsl_rate
        return (ordinary_earnings * total_leave_loading).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    @staticmethod
    def _calculate_shift_costs(
        employee: Employee,
        shift: Shift,
    ) -> Tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
        """
        Calculate detailed cost components for a single shift.

        Returns:
            (base_pay, penalty_pay, casual_loading, super, workcover, leave_accrual, total)
        """
        net_hours = Decimal(str(shift.net_hours))
        base_hourly = employee.hourly_base_rate

        # Base cost: hours × base rate (no multipliers)
        base_pay = (net_hours * base_hourly).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # Get penalty multiplier
        day_type = get_day_type(shift.date, employee.state)
        start_hour = shift.start_time.hour if shift.start_time else 12
        multiplier = get_penalty_multiplier(
            employee.employment_type,
            day_type,
            hour=start_hour,
            overtime_hours=0,
        )

        # Calculate penalty component and casual loading
        casual_loading = Decimal("0.00")
        penalty_pay = Decimal("0.00")

        if employee.employment_type == EmploymentType.casual:
            # Casuals: 25% loading on base
            casual_loading = (base_pay * CASUAL_LOADING_RATE).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            # Penalty is above 1.25 (base + casual loading)
            if multiplier > Decimal("1.25"):
                penalty_pay = (
                    base_pay * (multiplier - Decimal("1.25"))
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            # FT/PT: penalty is above 1.0
            if multiplier > Decimal("1"):
                penalty_pay = (
                    base_pay * (multiplier - Decimal("1"))
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Ordinary time earnings (base + penalty + casual loading)
        ordinary_earnings = base_pay + penalty_pay + casual_loading

        # Superannuation on ordinary time earnings
        super_contribution = (ordinary_earnings * SUPER_RATE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # WorkCover levy on gross (ordinary earnings + super)
        gross_for_workcover = ordinary_earnings + super_contribution
        workcover_levy = EmployeeCostingService._get_workcover_levy(
            employee, gross_for_workcover
        )

        # Leave accrual on ordinary earnings
        leave_accrual = EmployeeCostingService._get_leave_accrual(
            employee, ordinary_earnings
        )

        # Total shift cost
        total_cost = (
            base_pay + penalty_pay + casual_loading +
            super_contribution + workcover_levy + leave_accrual
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        return base_pay, penalty_pay, casual_loading, super_contribution, workcover_levy, leave_accrual, total_cost

    def project_cost(
        self,
        employee: Employee,
        shifts: List[Shift],
        venue_annual_payroll: Optional[Decimal] = None,
    ) -> EmployeeCostProjection:
        """
        Project total employment cost for an employee across multiple shifts.

        Args:
            employee: The employee
            shifts: List of shifts to calculate costs for
            venue_annual_payroll: Venue's estimated annual payroll (for payroll tax calc)

        Returns:
            EmployeeCostProjection with comprehensive cost breakdown
        """
        total_hours = Decimal("0")
        total_base = Decimal("0")
        total_penalty = Decimal("0")
        total_casual = Decimal("0")
        total_super = Decimal("0")
        total_workcover = Decimal("0")
        total_leave = Decimal("0")
        total_cost = Decimal("0")

        shift_details: List[ShiftCostDetail] = []
        warnings: List[str] = []

        for shift in shifts:
            base_pay, penalty_pay, casual_loading, super_contrib, workcover, leave_accrual, shift_total = (
                self._calculate_shift_costs(employee, shift)
            )

            hours = Decimal(str(shift.net_hours))
            total_hours += hours

            total_base += base_pay
            total_penalty += penalty_pay
            total_casual += casual_loading
            total_super += super_contrib
            total_workcover += workcover
            total_leave += leave_accrual
            total_cost += shift_total

            # Calculate effective hourly rate for this shift
            if shift.net_hours > 0:
                effective_rate = (shift_total / hours).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            else:
                effective_rate = Decimal("0.00")

            # Get penalty multiplier for display
            day_type = get_day_type(shift.date, employee.state)
            multiplier = get_penalty_multiplier(
                employee.employment_type, day_type
            )

            shift_details.append(
                ShiftCostDetail(
                    shift_id=shift.id,
                    date=shift.date,
                    start_time=shift.start_time,
                    end_time=shift.end_time,
                    hours=hours,
                    base_pay=base_pay,
                    penalty_pay=penalty_pay,
                    casual_loading=casual_loading,
                    super_contribution=super_contrib,
                    workcover_component=workcover,
                    leave_accrual=leave_accrual,
                    total_cost=shift_total,
                    effective_hourly_rate=effective_rate,
                    penalty_multiplier=multiplier,
                )
            )

        # Calculate payroll tax component (venue-wide, not per-employee)
        payroll_tax = Decimal("0")
        if venue_annual_payroll:
            threshold, rate = PAYROLL_TAX_CONFIG.get(
                employee.state, (Decimal("1200000"), Decimal("0.0545"))
            )
            if venue_annual_payroll > threshold:
                payroll_tax = (
                    (venue_annual_payroll - threshold) * rate
                    / Decimal(str(len(shifts) if shifts else 1))
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Calculate effective hourly rate
        if total_hours > 0:
            effective_hourly = (total_cost / total_hours).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            effective_hourly = Decimal("0.00")

        # Generate warnings
        weekly_hours = sum(Decimal(str(s.net_hours)) for s in shifts
                          if (datetime.now().date() - s.date).days < 7)
        if weekly_hours > OVERTIME_WARNING_HOURS:
            warnings.append(
                f"Employee {employee.name} approaching overtime threshold: "
                f"{float(weekly_hours):.1f}h this week (>{OVERTIME_WARNING_HOURS}h)"
            )

        if employee.employment_type == EmploymentType.casual and total_casual > 0:
            casual_pct = (total_casual / total_base * 100).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            ) if total_base > 0 else Decimal("0")
            if casual_pct > Decimal("30"):
                warnings.append(
                    f"Casual loading is {float(casual_pct):.1f}% of base "
                    "(typically 25% for weekday work)"
                )

        return EmployeeCostProjection(
            employee_id=employee.id,
            employee_name=employee.name,
            employment_type=employee.employment_type,
            state=employee.state,
            total_hours=total_hours,
            total_shifts=len(shifts),
            base_pay=total_base,
            penalty_pay=total_penalty,
            casual_loading=total_casual,
            super_contribution=total_super,
            workcover_levy=total_workcover,
            payroll_tax_component=payroll_tax,
            leave_accrual=total_leave,
            total_cost=total_cost,
            effective_hourly_rate=effective_hourly,
            per_shift_breakdown=shift_details,
            warnings=warnings,
        )

    def compare_employees(
        self,
        employees: List[Employee],
        shifts: List[Shift],
        venue_annual_payroll: Optional[Decimal] = None,
    ) -> List[EmployeeCostProjection]:
        """
        Compare the cost of assigning the same shifts to different employees.

        Args:
            employees: List of employees to compare
            shifts: List of shifts to assign
            venue_annual_payroll: Venue's annual payroll estimate

        Returns:
            List of cost projections, sorted by total cost (cheapest first)
        """
        projections = [
            self.project_cost(emp, shifts, venue_annual_payroll)
            for emp in employees
        ]
        # Sort by total cost (ascending)
        return sorted(projections, key=lambda p: p.total_cost)

    def find_cheapest_option(
        self,
        eligible_employees: List[Employee],
        shift: Shift,
        venue_annual_payroll: Optional[Decimal] = None,
    ) -> List[Tuple[Employee, Decimal]]:
        """
        Find the cheapest employee option for a single shift.

        Args:
            eligible_employees: List of employees eligible for the shift
            shift: The shift to fill
            venue_annual_payroll: Venue's annual payroll estimate

        Returns:
            List of (Employee, total_cost) tuples, sorted by cost (cheapest first)
        """
        costs = []
        for emp in eligible_employees:
            projection = self.project_cost(emp, [shift], venue_annual_payroll)
            costs.append((emp, projection.total_cost))

        # Sort by cost (ascending)
        return sorted(costs, key=lambda x: x[1])

    def annual_cost_estimate(
        self,
        employee: Employee,
        avg_weekly_hours: float,
        weeks_per_year: int = 52,
        venue_annual_payroll: Optional[Decimal] = None,
    ) -> Dict[str, Decimal]:
        """
        Project annual employment cost based on average weekly hours.

        Args:
            employee: The employee
            avg_weekly_hours: Average hours worked per week
            weeks_per_year: Number of weeks worked per year (default 52)
            venue_annual_payroll: Venue's annual payroll estimate

        Returns:
            Dict with annual cost breakdown (base, penalty, super, etc.)
        """
        # Create synthetic shifts to calculate weekly rate
        today = datetime.now().date()
        synthetic_shifts = []

        # Generate one week of shifts at average hours
        hours_remaining = avg_weekly_hours
        for i in range(7):
            if hours_remaining <= 0:
                break
            shift_date = today + timedelta(days=i)
            shift_hours = min(hours_remaining, 8.0)  # Max 8 hours per shift

            shift = Shift(
                id=f"synthetic_{i}",
                employee_id=employee.id,
                date=shift_date,
                start_time=time(9, 0),
                end_time=time(int(9 + shift_hours), 0),
                break_minutes=0,
                status="scheduled",
                role="general",
            )
            synthetic_shifts.append(shift)
            hours_remaining -= shift_hours

        # Project one week
        weekly_projection = self.project_cost(
            employee, synthetic_shifts, venue_annual_payroll
        )

        # Annualize
        annual_factor = Decimal(str(weeks_per_year))
        return {
            "annual_hours": Decimal(str(avg_weekly_hours)) * annual_factor,
            "annual_base_pay": weekly_projection.base_pay * annual_factor,
            "annual_penalty_pay": weekly_projection.penalty_pay * annual_factor,
            "annual_casual_loading": weekly_projection.casual_loading * annual_factor,
            "annual_super_contribution": weekly_projection.super_contribution * annual_factor,
            "annual_workcover_levy": weekly_projection.workcover_levy * annual_factor,
            "annual_leave_accrual": weekly_projection.leave_accrual * annual_factor,
            "annual_total_cost": weekly_projection.total_cost * annual_factor,
            "effective_hourly_rate": weekly_projection.effective_hourly_rate,
        }

    def calculate_roster_labour_cost(
        self,
        roster_shifts: List[Tuple[Employee, Shift]],
        venue_annual_payroll: Optional[Decimal] = None,
    ) -> RosterCostSummary:
        """
        Calculate total labour cost for a roster with detailed employee breakdowns.

        Args:
            roster_shifts: List of (Employee, Shift) tuples
            venue_annual_payroll: Venue's annual payroll estimate

        Returns:
            RosterCostSummary with comprehensive breakdown
        """
        if not roster_shifts:
            # Return empty summary
            return RosterCostSummary(
                roster_id="",
                week_start=datetime.now().date(),
                week_end=datetime.now().date(),
                total_hours=Decimal("0"),
                total_shifts=0,
                total_employees=0,
                total_base_pay=Decimal("0"),
                total_penalty_pay=Decimal("0"),
                total_casual_loading=Decimal("0"),
                total_super_contribution=Decimal("0"),
                total_workcover_levy=Decimal("0"),
                total_payroll_tax=Decimal("0"),
                total_leave_accrual=Decimal("0"),
                total_cost=Decimal("0"),
                average_hourly_rate=Decimal("0"),
            )

        # Group shifts by employee
        employee_shifts: Dict[str, Tuple[Employee, List[Shift]]] = {}
        for emp, shift in roster_shifts:
            if emp.id not in employee_shifts:
                employee_shifts[emp.id] = (emp, [])
            employee_shifts[emp.id][1].append(shift)

        # Project cost for each employee
        employee_costs: List[EmployeeCostProjection] = []
        total_hours = Decimal("0")
        total_shifts = 0
        total_base = Decimal("0")
        total_penalty = Decimal("0")
        total_casual = Decimal("0")
        total_super = Decimal("0")
        total_workcover = Decimal("0")
        total_leave = Decimal("0")
        total_cost = Decimal("0")

        for emp_id, (emp, shifts) in employee_shifts.items():
            projection = self.project_cost(emp, shifts, venue_annual_payroll)
            employee_costs.append(projection)

            total_hours += projection.total_hours
            total_shifts += projection.total_shifts
            total_base += projection.base_pay
            total_penalty += projection.penalty_pay
            total_casual += projection.casual_loading
            total_super += projection.super_contribution
            total_workcover += projection.workcover_levy
            total_leave += projection.leave_accrual
            total_cost += projection.total_cost

        # Calculate payroll tax (venue-wide)
        total_payroll_tax = Decimal("0")
        if venue_annual_payroll:
            threshold, rate = PAYROLL_TAX_CONFIG.get(
                employee_costs[0].state if employee_costs else State.nsw,
                (Decimal("1200000"), Decimal("0.0545"))
            )
            if venue_annual_payroll > threshold:
                total_payroll_tax = (
                    (venue_annual_payroll - threshold) * rate
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Calculate average hourly rate
        if total_hours > 0:
            avg_hourly = (total_cost / total_hours).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            avg_hourly = Decimal("0.00")

        # Determine week range
        if roster_shifts:
            all_dates = [shift.date for _, shift in roster_shifts]
            week_start = min(all_dates)
            week_end = max(all_dates)
        else:
            week_start = datetime.now().date()
            week_end = datetime.now().date()

        # Collect warnings from all employees
        warnings = []
        for projection in employee_costs:
            warnings.extend(projection.warnings)

        return RosterCostSummary(
            roster_id="",
            week_start=week_start,
            week_end=week_end,
            total_hours=total_hours,
            total_shifts=total_shifts,
            total_employees=len(employee_shifts),
            total_base_pay=total_base,
            total_penalty_pay=total_penalty,
            total_casual_loading=total_casual,
            total_super_contribution=total_super,
            total_workcover_levy=total_workcover,
            total_payroll_tax=total_payroll_tax,
            total_leave_accrual=total_leave,
            total_cost=total_cost,
            average_hourly_rate=avg_hourly,
            employee_costs=employee_costs,
            warnings=warnings,
        )
