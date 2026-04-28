"""
Cost calculation engine for RosterIQ.

Provides detailed labour cost breakdowns for shifts and rosters, including
base costs, penalty multipliers, casual loading, and superannuation.

All monetary values use Decimal for precision.
Superannuation rate: 11.5% (FY 2025-26).
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    State,
    EmploymentType,
    CostBreakdown,
    RosterComparison,
)
from rosteriq.award_rules import (
    get_penalty_multiplier,
    get_day_type,
)


# Superannuation rate for FY 2025-26
SUPER_RATE = Decimal("0.115")

# Casual loading rate
CASUAL_LOADING_RATE = Decimal("0.25")


def calculate_shift_cost_breakdown(
    employee: Employee, shift: Shift, state: State
) -> CostBreakdown:
    """
    Calculate detailed cost breakdown for a single shift.

    Args:
        employee: The employee working the shift.
        shift: The shift to calculate costs for.
        state: The state for award rule application.

    Returns:
        CostBreakdown with base_cost, penalty_cost, casual_loading,
        super_contribution, and total_cost.
    """
    net_hours = Decimal(str(shift.net_hours))
    base_hourly = employee.hourly_base_rate

    # Base cost: hours * base rate (no multipliers)
    base_cost = (net_hours * base_hourly).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Get the penalty multiplier for this shift context
    day_type = get_day_type(shift.date, state)
    shift_hour = shift.start_time.hour if shift.start_time else 12
    multiplier = get_penalty_multiplier(
        employee.employment_type, day_type,
        hour=shift_hour,
        overtime_hours=getattr(shift, 'overtime_hours', 0),
    )

    # Calculate casual loading and penalty separately
    casual_loading = Decimal("0.00")
    penalty_cost = Decimal("0.00")

    if employee.employment_type == EmploymentType.casual:
        # For casuals, the multiplier includes 25% loading
        # Base casual weekday multiplier is 1.25 (1.0 base + 0.25 loading)
        # Additional penalty is everything above 1.25
        casual_loading = (base_cost * CASUAL_LOADING_RATE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        # Penalty portion is what's above the casual base (1.25)
        if multiplier > Decimal("1.25"):
            penalty_cost = (
                base_cost * (multiplier - Decimal("1.25"))
            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        # For permanent employees, penalty is the portion above 1.0
        if multiplier > Decimal("1"):
            penalty_cost = (
                base_cost * (multiplier - Decimal("1"))
            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # Superannuation on gross pay (base + penalty + casual loading)
    gross = base_cost + penalty_cost + casual_loading
    super_contribution = (gross * SUPER_RATE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    total_cost = base_cost + penalty_cost + casual_loading + super_contribution

    return CostBreakdown(
        base_cost=base_cost,
        penalty_cost=penalty_cost,
        casual_loading=casual_loading,
        super_contribution=super_contribution,
        total_cost=total_cost,
    )


def calculate_roster_cost(
    roster: Roster, employees: dict[str, Employee], state: State
) -> Decimal:
    """
    Calculate total cost for an entire roster.

    Args:
        roster: The roster containing all shifts.
        employees: Dict mapping employee IDs to Employee objects.
        state: The state for award rule application.

    Returns:
        Total roster cost as Decimal.

    Raises:
        ValueError: If an employee ID in a shift is not found.
    """
    total = Decimal("0")
    for shift in roster.shifts:
        if shift.employee_id not in employees:
            raise ValueError(f"Employee {shift.employee_id} not found")
        breakdown = calculate_shift_cost_breakdown(
            employees[shift.employee_id], shift, state
        )
        total += breakdown.total_cost

    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compare_rosters(
    original: Roster,
    optimised: Roster,
    employees: dict[str, Employee],
    state: State,
) -> RosterComparison:
    """
    Compare two rosters and return savings analysis.

    Args:
        original: The original roster.
        optimised: The optimised roster.
        employees: Dict mapping employee IDs to Employee objects.
        state: The state for award rule application.

    Returns:
        RosterComparison with cost difference and hours analysis.
    """
    original_cost = calculate_roster_cost(original, employees, state)
    optimised_cost = calculate_roster_cost(optimised, employees, state)
    savings = original_cost - optimised_cost
    hours_saved = original.total_hours - optimised.total_hours

    alerts = []
    if savings < 0:
        alerts.append("Optimised roster is more expensive than original")
    if hours_saved < 0:
        alerts.append("Optimised roster uses more hours than original")

    return RosterComparison(
        original=original,
        optimised=optimised,
        cost_savings=savings,
        hours_saved=hours_saved,
        alerts=alerts,
    )


def estimate_daily_labour_cost(
    shifts: list[Shift], employees: dict[str, Employee], state: State
) -> Decimal:
    """
    Quick estimate of a day's labour cost.

    Args:
        shifts: List of shifts for the day.
        employees: Dict mapping employee IDs to Employee objects.
        state: The state for award rule application.

    Returns:
        Total estimated daily labour cost.
    """
    total = Decimal("0")
    for shift in shifts:
        if shift.employee_id not in employees:
            raise ValueError(f"Employee {shift.employee_id} not found")
        breakdown = calculate_shift_cost_breakdown(
            employees[shift.employee_id], shift, state
        )
        total += breakdown.total_cost

    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calculate_labour_percentage(labour_cost: Decimal, revenue: Decimal) -> float:
    """
    Calculate labour cost as percentage of revenue.

    Args:
        labour_cost: Total labour cost.
        revenue: Total revenue (must be > 0).

    Returns:
        Labour cost as percentage (0-100).

    Raises:
        ValueError: If revenue is zero or negative.
    """
    if revenue <= 0:
        raise ValueError("Revenue must be greater than zero")

    pct = (labour_cost / revenue * Decimal("100")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    return float(pct)


def find_cost_savings_opportunities(
    roster: Roster, employees: dict[str, Employee], state: State
) -> list[str]:
    """
    Identify potential cost savings in a roster.

    Checks for:
    - Long shifts that could be split for penalty savings
    - Overlapping shifts suggesting overstaffing
    - High-cost shifts with large penalty multipliers

    Args:
        roster: The roster to analyse.
        employees: Dict mapping employee IDs to Employee objects.
        state: The state for award rule application.

    Returns:
        List of opportunity descriptions.
    """
    opportunities = []

    # Check for long shifts
    for shift in roster.shifts:
        if shift.net_hours > 8:
            opportunities.append(
                f"Shift {shift.id} is {shift.net_hours:.1f}h — consider splitting "
                f"for penalty savings"
            )

    # Check for overlapping shifts by time slot
    from collections import defaultdict
    time_slots: dict[tuple, list[str]] = defaultdict(list)
    for shift in roster.shifts:
        key = (shift.date, shift.start_time)
        time_slots[key].append(shift.id)

    for key, shift_ids in time_slots.items():
        if len(shift_ids) > 3:
            opportunities.append(
                f"Potential overstaffing on {key[0]} at {key[1]}: "
                f"{len(shift_ids)} employees scheduled"
            )

    # Check for high-cost shifts
    for shift in roster.shifts:
        if shift.employee_id in employees:
            emp = employees[shift.employee_id]
            day_type = get_day_type(shift.date, state)
            mult = get_penalty_multiplier(emp.employment_type, day_type)
            if mult > Decimal("1.5"):
                breakdown = calculate_shift_cost_breakdown(emp, shift, state)
                opportunities.append(
                    f"High-cost shift {shift.id}: {mult}x multiplier, "
                    f"${breakdown.total_cost} total"
                )

    return opportunities
