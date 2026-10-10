"""
Cost calculation engine for RosterIQ.

Provides detailed labour cost breakdowns for shifts and rosters, including
base costs, penalty multipliers, casual loading, and superannuation.

All monetary values use Decimal for precision.
Superannuation is the Superannuation Guarantee rate in force on the date
(SUPER_GUARANTEE_RATES).
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
from datetime import date, time, timedelta
from rosteriq.award_rules import (
    get_penalty_multiplier,
    get_day_type,
    split_weekday_hours_by_band,
    EVENING_LOADING_THRESHOLDS,
    OVERTIME_MULTIPLIERS,
    DayType,
)
from rosteriq.services.pay_basis import is_salaried, salary_per_day

# Per-band ordinary multipliers (permanent-style: 1.0 + loading). Evening/night
# loading applies to weekday hours actually worked in those windows.
_EVENING_MULT = Decimal("1") + EVENING_LOADING_THRESHOLDS["7pm"]      # 1.15
_NIGHT_MULT = Decimal("1") + EVENING_LOADING_THRESHOLDS["midnight"]   # 1.175
_BAND_MULTS = {"ordinary": Decimal("1"), "evening": _EVENING_MULT, "night": _NIGHT_MULT}


# Superannuation Guarantee rate from each effective date (Superannuation
# Guarantee (Administration) Act 1992 s19). Statutory, not an award value.
SUPER_GUARANTEE_RATES = (
    (date(2014, 7, 1), Decimal("0.095")),
    (date(2021, 7, 1), Decimal("0.10")),
    (date(2022, 7, 1), Decimal("0.105")),
    (date(2023, 7, 1), Decimal("0.11")),
    (date(2024, 7, 1), Decimal("0.115")),
    (date(2025, 7, 1), Decimal("0.12")),
)

# Casual loading rate
CASUAL_LOADING_RATE = Decimal("0.25")

# Ordinary hours per week before overtime applies (full-time standard).
WEEKLY_OVERTIME_THRESHOLD = Decimal("38")
# The first 2 overtime hours of the week are at 1.5x; the rest at 2x.
OT_FIRST_TIER_HOURS = Decimal("2")
_CENT = Decimal("0.01")


def super_guarantee_rate(on: date) -> Decimal:
    """The Superannuation Guarantee rate in force on ``on``."""
    rate = SUPER_GUARANTEE_RATES[0][1]
    for effective, r in SUPER_GUARANTEE_RATES:
        if on >= effective:
            rate = r
    return rate


def super_rate_label(rate: Decimal) -> str:
    """A super rate as people read it: Decimal("0.12") -> "12%"."""
    return f"{(rate * 100).normalize():f}%"


def split_at_midnight(shift_date: date, start_time, end_time, net_hours) -> list:
    """
    Split a shift's NET worked hours at midnight into the calendar days they
    were worked on: ``[(day, start, end, hours), ...]`` in time order, summing
    to ``net_hours``. Each part is priced on its own day type, so the hours
    after midnight on a Saturday close are Sunday hours and a Sunday close
    running into a public holiday is holiday time after midnight. The unpaid
    break is deducted proportionally across the worked span, as in
    ``split_weekday_hours_by_band``.

    Pricing each calendar day's hours at that day's rate is the award
    interpretation in use; advisor to confirm.
    """
    net = Decimal(str(net_hours))
    if not (start_time and end_time):
        return [(shift_date, start_time, end_time, net)]
    start_min = start_time.hour * 60 + start_time.minute
    end_min = end_time.hour * 60 + end_time.minute
    if net <= 0 or end_min >= start_min or end_min == 0:
        return [(shift_date, start_time, end_time, net)]
    before_min = 24 * 60 - start_min
    first = net * Decimal(before_min) / Decimal(before_min + end_min)
    return [
        (shift_date, start_time, time(0, 0), first),
        (shift_date + timedelta(days=1), time(0, 0), end_time, net - first),
    ]


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

    A salaried employee's shift costs one day of salary (weekly_salary / 7)
    plus super, with no penalties or loading (services/pay_basis.py).
    """
    if is_salaried(employee):
        return _salaried_day_breakdown(employee, shift.date)
    net_hours = Decimal(str(shift.net_hours))
    base_hourly = employee.hourly_base_rate

    # Base cost: hours * base rate (no multipliers)
    base_cost = (net_hours * base_hourly).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    is_casual = employee.employment_type == EmploymentType.casual

    # Calculate casual loading and penalty separately
    casual_loading = Decimal("0.00")
    penalty_cost = Decimal("0.00")

    if is_casual:
        # Casual loading (25%) always applies to the whole shift.
        casual_loading = (base_cost * CASUAL_LOADING_RATE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    for day, part_start, part_end, part_hours in split_at_midnight(
        shift.date, shift.start_time, shift.end_time, net_hours
    ):
        day_type = get_day_type(day, state)
        if day_type == DayType.weekday and part_start and part_end:
            # Weekday: evening/night loading applies to the hours ACTUALLY WORKED
            # in those windows, not to the whole shift gated on its start hour.
            bands = split_weekday_hours_by_band(part_start, part_end, part_hours)
            for band, bhrs in bands.items():
                band_mult = _BAND_MULTS[band]
                if bhrs <= 0 or band_mult <= Decimal("1"):
                    continue
                b_base = (bhrs * base_hourly).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                penalty_cost += (b_base * (band_mult - Decimal("1"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
        else:
            # Sat/Sun/PH (or missing times): the day's hours at the day-type penalty.
            shift_hour = part_start.hour if part_start else 12
            multiplier = get_penalty_multiplier(
                employee.employment_type, day_type,
                hour=shift_hour,
                overtime_hours=getattr(shift, 'overtime_hours', 0),
            )
            part_base = (part_hours * base_hourly).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            if is_casual:
                if multiplier > Decimal("1.25"):
                    penalty_cost += (
                        part_base * (multiplier - Decimal("1.25"))
                    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            else:
                if multiplier > Decimal("1"):
                    penalty_cost += (
                        part_base * (multiplier - Decimal("1"))
                    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # Superannuation on gross pay (base + penalty + casual loading)
    gross = base_cost + penalty_cost + casual_loading
    super_contribution = (gross * super_guarantee_rate(shift.date)).quantize(
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


def _salaried_day_breakdown(employee: Employee, day: date) -> CostBreakdown:
    base = salary_per_day(employee)
    super_contribution = (base * super_guarantee_rate(day)).quantize(_CENT, ROUND_HALF_UP)
    return CostBreakdown(
        base_cost=base,
        penalty_cost=Decimal("0.00"),
        casual_loading=Decimal("0.00"),
        super_contribution=super_contribution,
        total_cost=base + super_contribution,
    )


def _salaried_week(employee: Employee, shifts: list[Shift]) -> dict:
    """One day of salary per rostered calendar day; never overtime."""
    days = sorted({s.date for s in shifts if s.net_hours > 0})
    base = Decimal("0.00")
    super_contribution = Decimal("0.00")
    for day in days:
        bd = _salaried_day_breakdown(employee, day)
        base += bd.base_cost
        super_contribution += bd.super_contribution
    hours = sum((Decimal(str(s.net_hours)) for s in shifts if s.net_hours > 0), Decimal("0"))
    return {
        "base_cost": base,
        "penalty_cost": Decimal("0.00"),
        "casual_loading": Decimal("0.00"),
        "overtime_cost": Decimal("0.00"),
        "super_contribution": super_contribution,
        "total_cost": base + super_contribution,
        "ordinary_hours": hours,
        "overtime_hours": Decimal("0"),
    }


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

    A salaried employee costs one day of salary per rostered day, with no
    penalties or overtime (services/pay_basis.py).
    """
    if is_salaried(employee):
        return _salaried_week(employee, shifts)
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
    super_exact = Decimal("0")

    for shift in chrono:
        hrs = Decimal(str(shift.net_hours))
        if hrs <= 0:
            continue

        if is_casual:
            ord_hrs, ot_hrs = hrs, Decimal("0")
        else:
            remaining_ordinary = max(Decimal("0"), WEEKLY_OVERTIME_THRESHOLD - cumulative)
            ord_hrs = min(hrs, remaining_ordinary)
            ot_hrs = hrs - ord_hrs
        cumulative += hrs
        ordinary_hours += ord_hrs
        overtime_hours += ot_hrs
        ote_before = base_cost + penalty_cost + casual_loading

        # Overtime is the LAST hours worked, so ordinary hours fill the shift
        # from its start and each calendar day's part is priced on its own day.
        ord_left = ord_hrs
        for day, part_start, part_end, part_hrs in split_at_midnight(
            shift.date, shift.start_time, shift.end_time, hrs
        ):
            day_type = get_day_type(day, state)
            part_ord = min(part_hrs, ord_left)
            ord_left -= part_ord
            part_ot = part_hrs - part_ord

            # Ordinary portion. On a weekday, evening/night loading applies to
            # the hours ACTUALLY WORKED in those windows (19:00-24:00,
            # 00:00-07:00), not to the whole shift gated on its start hour — so
            # a 09:00-23:00 shift earns evening loading on its 19:00-23:00
            # hours. Sat/Sun/PH use the whole day-type penalty (those rates
            # already exceed evening loading and the award does not stack them).
            if part_ord > 0 and day_type == DayType.weekday and part_start and part_end:
                bands = split_weekday_hours_by_band(part_start, part_end, part_hrs)
                factor = part_ord / part_hrs
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
            elif part_ord > 0:
                shift_hour = part_start.hour if part_start else 12
                mult = get_penalty_multiplier(
                    employee.employment_type, day_type, hour=shift_hour, overtime_hours=0
                )
                ord_base = (part_ord * base_hourly).quantize(_CENT, ROUND_HALF_UP)
                base_cost += ord_base
                if is_casual:
                    casual_loading += (ord_base * CASUAL_LOADING_RATE).quantize(_CENT, ROUND_HALF_UP)
                    if mult > Decimal("1.25"):
                        penalty_cost += (ord_base * (mult - Decimal("1.25"))).quantize(_CENT, ROUND_HALF_UP)
                else:
                    if mult > Decimal("1"):
                        penalty_cost += (ord_base * (mult - Decimal("1"))).quantize(_CENT, ROUND_HALF_UP)

            # Overtime portion (permanents only).
            if part_ot > 0:
                overtime_cost += _overtime_cost(part_ot, ot_used, base_hourly, day_type)
                ot_used += part_ot

        shift_ote = base_cost + penalty_cost + casual_loading - ote_before
        super_exact += shift_ote * super_guarantee_rate(shift.date)

    # Super on Ordinary Time Earnings only — excludes overtime.
    super_contribution = super_exact.quantize(_CENT, ROUND_HALF_UP)
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
        if not shift.employee_id:
            continue  # an open shift has nobody to pay yet
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
    salaried_days = set()
    for shift in shifts:
        if not shift.employee_id:
            continue  # an open shift has nobody to pay yet
        if shift.employee_id not in employees:
            raise ValueError(f"Employee {shift.employee_id} not found")
        employee = employees[shift.employee_id]
        if is_salaried(employee):
            if (employee.id, shift.date) in salaried_days:
                continue
            salaried_days.add((employee.id, shift.date))
        breakdown = calculate_shift_cost_breakdown(employee, shift, state)
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
