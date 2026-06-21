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
from datetime import time
from rosteriq.award_rules import (
    get_penalty_multiplier,
    get_day_type,
    split_weekday_hours_by_band,
    EVENING_LOADING_THRESHOLDS,
    OVERTIME_MULTIPLIERS,
    DayType,
)

# Per-band ordinary multipliers (permanent-style: 1.0 + loading). Evening/night
# loading applies to weekday hours actually worked in those windows.
_EVENING_MULT = Decimal("1") + EVENING_LOADING_THRESHOLDS["7pm"]      # 1.15
_NIGHT_MULT = Decimal("1") + EVENING_LOADING_THRESHOLDS["midnight"]   # 1.175
_BAND_MULTS = {"ordinary": Decimal("1"), "evening": _EVENING_MULT, "night": _NIGHT_MULT}


# Superannuation rate for FY 2025-26
SUPER_RATE = Decimal("0.115")

# Casual loading rate
CASUAL_LOADING_RATE = Decimal("0.25")

# Ordinary hours per week before overtime applies (full-time standard).
WEEKLY_OVERTIME_THRESHOLD = Decimal("38")
# The first 2 overtime hours of the week are at 1.5x; the rest at 2x.
OT_FIRST_TIER_HOURS = Decimal("2")
_CENT = Decimal("0.01")


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
    is_casual = employee.employment_type == EmploymentType.casual

    # Calculate casual loading and penalty separately
    casual_loading = Decimal("0.00")
    penalty_cost = Decimal("0.00")

    if is_casual:
        # Casual loading (25%) always applies to the whole shift.
        casual_loading = (base_cost * CASUAL_LOADING_RATE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    if day_type == DayType.weekday and shift.start_time and shift.end_time:
        # Weekday: evening/night loading applies to the hours ACTUALLY WORKED in
        # those windows, not to the whole shift gated on its start hour.
        bands = split_weekday_hours_by_band(shift.start_time, shift.end_time, net_hours)
        for band, bhrs in bands.items():
            band_mult = _BAND_MULTS[band]
            if bhrs <= 0 or band_mult <= Decimal("1"):
                continue
            b_base = (bhrs * base_hourly).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            penalty_cost += (b_base * (band_mult - Decimal("1"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
    else:
        # Sat/Sun/PH (or missing times): whole shift at the day-type penalty.
        shift_hour = shift.start_time.hour if shift.start_time else 12
        multiplier = get_penalty_multiplier(
            employee.employment_type, day_type,
            hour=shift_hour,
            overtime_hours=getattr(shift, 'overtime_hours', 0),
        )
        if is_casual:
            if multiplier > Decimal("1.25"):
                penalty_cost = (
                    base_cost * (multiplier - Decimal("1.25"))
                ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
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


def _overtime_cost(
    ot_hours: Decimal,
    ot_already_used: Decimal,
    base_hourly: Decimal,
    day_type: DayType,
) -> Decimal:
    """
    Cost ``ot_hours`` of overtime, given ``ot_already_used`` OT hours already
    costed earlier this week (for the 1.5x -> 2x tiering).

    Sunday and public-holiday overtime use their flat award rates; weekday and
    Saturday overtime is 1.5x for the first 2 OT hours of the week, then 2x.
    """
    if ot_hours <= 0:
        return Decimal("0.00")
    if day_type == DayType.public_holiday:
        return (ot_hours * base_hourly * OVERTIME_MULTIPLIERS["public_holiday"]).quantize(_CENT, ROUND_HALF_UP)
    if day_type == DayType.sunday:
        return (ot_hours * base_hourly * OVERTIME_MULTIPLIERS["sunday"]).quantize(_CENT, ROUND_HALF_UP)
    first_tier_remaining = max(Decimal("0"), OT_FIRST_TIER_HOURS - ot_already_used)
    at_15 = min(ot_hours, first_tier_remaining)
    at_20 = ot_hours - at_15
    cost = (
        at_15 * base_hourly * OVERTIME_MULTIPLIERS["first_2_hours"]
        + at_20 * base_hourly * OVERTIME_MULTIPLIERS["after_2_hours"]
    )
    return cost.quantize(_CENT, ROUND_HALF_UP)


def cost_employee_week(
    employee: Employee, shifts: list[Shift], state: State
) -> dict:
    """
    Cost one employee's week of shifts WITH weekly overtime.

    Hours up to 38/week are ordinary — costed at the day-type penalty rate, with
    superannuation applying. Hours beyond 38 are overtime — costed at the
    overtime rates (1.5x first 2 OT hours of the week, then 2x; Sunday/public
    holiday OT at their flat rates) and EXCLUDED from super (super is on Ordinary
    Time Earnings; overtime is not OTE). Casual employees do not accrue overtime.

    Returns a dict with base/penalty/casual_loading/overtime/super/total/
    ordinary_hours/overtime_hours so callers (and tests) can see each component.
    """
    base_hourly = employee.hourly_base_rate
    is_casual = employee.employment_type == EmploymentType.casual
    # Process chronologically so the LAST hours of the week become overtime.
    chrono = sorted(shifts, key=lambda s: (s.date, s.start_time or time(0, 0)))

    base_cost = Decimal("0.00")
    penalty_cost = Decimal("0.00")
    casual_loading = Decimal("0.00")
    overtime_cost = Decimal("0.00")
    cumulative = Decimal("0")
    ot_used = Decimal("0")
    ordinary_hours = Decimal("0")
    overtime_hours = Decimal("0")

    for shift in chrono:
        hrs = Decimal(str(shift.net_hours))
        if hrs <= 0:
            continue
        day_type = get_day_type(shift.date, state)

        if is_casual:
            ord_hrs, ot_hrs = hrs, Decimal("0")
        else:
            remaining_ordinary = max(Decimal("0"), WEEKLY_OVERTIME_THRESHOLD - cumulative)
            ord_hrs = min(hrs, remaining_ordinary)
            ot_hrs = hrs - ord_hrs
        cumulative += hrs
        ordinary_hours += ord_hrs
        overtime_hours += ot_hrs

        # Ordinary portion. On a weekday, evening/night loading applies to the
        # hours ACTUALLY WORKED in those windows (19:00-24:00, 00:00-07:00), not
        # to the whole shift gated on its start hour — so a 09:00-23:00 shift
        # earns evening loading on its 19:00-23:00 hours. Sat/Sun/PH use the
        # whole day-type penalty (those rates already exceed evening loading and
        # the award does not stack them).
        if ord_hrs > 0 and day_type == DayType.weekday and shift.start_time and shift.end_time:
            bands = split_weekday_hours_by_band(shift.start_time, shift.end_time, hrs)
            factor = (ord_hrs / hrs) if hrs > 0 else Decimal("0")
            for band, bhrs in bands.items():
                bhrs = bhrs * factor
                if bhrs <= 0:
                    continue
                b_base = (bhrs * base_hourly).quantize(_CENT, ROUND_HALF_UP)
                base_cost += b_base
                band_mult = _BAND_MULTS[band]
                if is_casual:
                    casual_loading += (b_base * CASUAL_LOADING_RATE).quantize(_CENT, ROUND_HALF_UP)
                if band_mult > Decimal("1"):
                    penalty_cost += (b_base * (band_mult - Decimal("1"))).quantize(_CENT, ROUND_HALF_UP)
        else:
            shift_hour = shift.start_time.hour if shift.start_time else 12
            mult = get_penalty_multiplier(
                employee.employment_type, day_type, hour=shift_hour, overtime_hours=0
            )
            ord_base = (ord_hrs * base_hourly).quantize(_CENT, ROUND_HALF_UP)
            base_cost += ord_base
            if is_casual:
                casual_loading += (ord_base * CASUAL_LOADING_RATE).quantize(_CENT, ROUND_HALF_UP)
                if mult > Decimal("1.25"):
                    penalty_cost += (ord_base * (mult - Decimal("1.25"))).quantize(_CENT, ROUND_HALF_UP)
            else:
                if mult > Decimal("1"):
                    penalty_cost += (ord_base * (mult - Decimal("1"))).quantize(_CENT, ROUND_HALF_UP)

        # Overtime portion (permanents only).
        if ot_hrs > 0:
            overtime_cost += _overtime_cost(ot_hrs, ot_used, base_hourly, day_type)
            ot_used += ot_hrs

    # Super on Ordinary Time Earnings only — excludes overtime.
    super_base = base_cost + penalty_cost + casual_loading
    super_contribution = (super_base * SUPER_RATE).quantize(_CENT, ROUND_HALF_UP)
    total = base_cost + penalty_cost + casual_loading + overtime_cost + super_contribution

    return {
        "base_cost": base_cost,
        "penalty_cost": penalty_cost,
        "casual_loading": casual_loading,
        "overtime_cost": overtime_cost,
        "super_contribution": super_contribution,
        "total_cost": total,
        "ordinary_hours": ordinary_hours,
        "overtime_hours": overtime_hours,
    }


def calculate_roster_cost(
    roster: Roster, employees: dict[str, Employee], state: State
) -> Decimal:
    """
    Calculate total cost for an entire roster, INCLUDING weekly overtime.

    Shifts are grouped by employee and costed as a week (see cost_employee_week)
    so hours beyond 38/week are paid at overtime rates and excluded from super —
    previously every shift was costed in isolation, so overtime never applied.

    Args:
        roster: The roster containing all shifts.
        employees: Dict mapping employee IDs to Employee objects.
        state: The state for award rule application.

    Returns:
        Total roster cost as Decimal.

    Raises:
        ValueError: If an employee ID in a shift is not found.
    """
    by_employee: dict[str, list[Shift]] = {}
    for shift in roster.shifts:
        if shift.employee_id not in employees:
            raise ValueError(f"Employee {shift.employee_id} not found")
        by_employee.setdefault(shift.employee_id, []).append(shift)

    total = Decimal("0")
    for emp_id, emp_shifts in by_employee.items():
        total += cost_employee_week(employees[emp_id], emp_shifts, state)["total_cost"]

    return total.quantize(_CENT, rounding=ROUND_HALF_UP)


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
