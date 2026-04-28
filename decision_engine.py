"""
Real-time staffing decision engine for RosterIQ.

When the variance engine detects that actual demand diverges significantly from
forecast, this module decides WHO to cut (overstaffed) or call in (understaffed)
based on a priority ranking system.

Cut ranking (highest priority to cut first):
1. Casual employees (cheapest to send home)
2. Higher penalty multiplier shifts (biggest savings)
3. Shortest time remaining on shift (least disruption)
4. Least skilled for current role
5. Most hours worked this week (spread hours fairly)

Call-in ranking (best candidates first):
1. Available for the day/time
2. Full-time under weekly hours (no overtime cost)
3. Part-time under contracted hours
4. Casuals (flexible but cost loading)
5. Most skilled for the needed role
"""

from datetime import date, time, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from rosteriq.models import (
    Employee,
    Shift,
    StaffAction,
    StaffingRecommendation,
    EmploymentType,
    ShiftStatus,
    State,
)
from rosteriq.award_rules import (
    get_penalty_multiplier,
    get_day_type,
    calculate_shift_cost,
)


def get_weekly_hours(employee_id: str, shifts: list[Shift]) -> float:
    """
    Sum up net hours worked this week for an employee.

    Args:
        employee_id: ID of the employee.
        shifts: All shifts in the current week.

    Returns:
        Total net hours worked.
    """
    return sum(
        s.net_hours
        for s in shifts
        if s.employee_id == employee_id
        and s.status not in (ShiftStatus.cancelled, ShiftStatus.no_show)
    )


def estimate_cut_savings(
    employee: Employee,
    shift: Shift,
    remaining_hours: float,
    state: State,
) -> Decimal:
    """
    Estimate how much we save by cutting an employee's remaining shift time.

    Args:
        employee: The employee to potentially cut.
        shift: Their current shift.
        remaining_hours: Hours left in the shift.
        state: State for penalty rate calculation.

    Returns:
        Estimated savings as Decimal.
    """
    if remaining_hours <= 0:
        return Decimal("0.00")

    day_type = get_day_type(shift.date, state)
    multiplier = get_penalty_multiplier(employee.employment_type, day_type)
    savings = employee.hourly_base_rate * multiplier * Decimal(str(remaining_hours))
    return savings.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _calculate_remaining_hours(shift: Shift, current_hour: int = None) -> float:
    """Calculate hours remaining in a shift from the current time."""
    if current_hour is None:
        current_hour = datetime.now().hour

    end_minutes = shift.end_time.hour * 60 + shift.end_time.minute
    current_minutes = current_hour * 60

    # Handle overnight shifts
    if end_minutes < shift.start_time.hour * 60 + shift.start_time.minute:
        end_minutes += 24 * 60

    remaining = (end_minutes - current_minutes) / 60.0
    return max(0.0, remaining)


def _cut_priority_score(
    employee: Employee,
    shift: Shift,
    state: State,
    weekly_hours: float,
    remaining_hours: float,
    needed_skills: list[str] = None,
) -> float:
    """
    Calculate cut priority score (higher = cut first).

    Factors:
    - Employment type: casual=1.0, part_time=0.5, full_time=0.2
    - Penalty multiplier: higher = cut first (more savings)
    - Remaining hours: shorter remaining = cut first (less disruption)
    - Skills: fewer relevant skills = cut first
    - Weekly hours: more hours worked = cut first (fairness)
    """
    score = 0.0

    # Employment type weight (0-1)
    type_scores = {
        EmploymentType.casual: 1.0,
        EmploymentType.part_time: 0.5,
        EmploymentType.full_time: 0.2,
    }
    score += type_scores.get(employee.employment_type, 0.5) * 0.30

    # Penalty multiplier weight — higher multiplier = more savings = cut first
    day_type = get_day_type(shift.date, state)
    multiplier = float(get_penalty_multiplier(employee.employment_type, day_type))
    score += min(multiplier / 2.5, 1.0) * 0.25

    # Remaining hours — shorter remaining = cut first (less disruption)
    if remaining_hours > 0:
        score += max(0.0, 1.0 - remaining_hours / 12.0) * 0.15
    else:
        score += 0.15

    # Skill match — fewer relevant skills = cut first
    if needed_skills:
        matching = len(set(employee.skills) & set(needed_skills))
        total = len(needed_skills)
        score += (1.0 - matching / max(total, 1)) * 0.15
    else:
        score += 0.075  # neutral if no skills specified

    # Weekly hours — more hours = cut first (fairness)
    score += min(weekly_hours / 38.0, 1.0) * 0.15

    return score


def _call_in_priority_score(
    employee: Employee,
    target_date: date,
    target_hour: int,
    weekly_hours: float,
    needed_roles: list[str] = None,
) -> float:
    """
    Calculate call-in priority score (higher = call first).

    Factors:
    - Availability for the target day/time
    - Employment type: FT under hours > PT under hours > casual
    - Hours remaining under weekly cap
    - Skill match for needed roles
    """
    score = 0.0

    # Availability check
    day_name = target_date.strftime("%A").lower()
    if day_name in employee.availability:
        ranges = employee.availability[day_name]
        for r in ranges:
            start_h = int(r.get("start", "0").split(":")[0])
            end_h = int(r.get("end", "23").split(":")[0])
            if start_h <= target_hour <= end_h:
                score += 0.30
                break
    # If no availability data, give partial score (unknown availability)
    if not employee.availability:
        score += 0.15

    # Employment type + hours headroom
    hours_remaining = max(0.0, employee.max_hours_per_week - weekly_hours)
    if employee.employment_type == EmploymentType.full_time:
        if hours_remaining > 0:
            score += 0.30  # FT under hours — best option, no overtime
        else:
            score += 0.05  # FT over hours — overtime cost
    elif employee.employment_type == EmploymentType.part_time:
        if hours_remaining > 0:
            score += 0.25
        else:
            score += 0.05
    else:  # casual
        score += 0.15  # flexible but costs loading

    # Hours headroom (more room = better)
    score += min(hours_remaining / 38.0, 1.0) * 0.20

    # Skill match
    if needed_roles and employee.skills:
        matching = len(set(employee.skills) & set(needed_roles))
        score += min(matching / max(len(needed_roles), 1), 1.0) * 0.20
    elif not needed_roles:
        score += 0.10  # neutral

    return score


def rank_for_cut(
    active_shifts: list[tuple[Employee, Shift]],
    state: State,
    min_staff: int = 1,
    current_hour: int = None,
    weekly_shifts: dict[str, list[Shift]] = None,
) -> list[StaffingRecommendation]:
    """
    Rank currently active employees by cut priority.

    Never recommends cutting below min_staff. Returns ordered recommendations
    with estimated savings for each potential cut.

    Args:
        active_shifts: List of (Employee, Shift) tuples for currently working staff.
        state: State for penalty rate calculation.
        min_staff: Minimum staff that must remain (default 1).
        current_hour: Current hour for remaining-time calculation (default: now).
        weekly_shifts: Dict of employee_id -> shifts this week (for hours tracking).

    Returns:
        Ordered list of StaffingRecommendations (highest priority cut first).
    """
    if len(active_shifts) <= min_staff:
        return []

    weekly_shifts = weekly_shifts or {}
    recommendations = []

    for employee, shift in active_shifts:
        weekly_hours = get_weekly_hours(employee.id, weekly_shifts.get(employee.id, []))
        remaining = _calculate_remaining_hours(shift, current_hour)

        priority = _cut_priority_score(
            employee=employee,
            shift=shift,
            state=state,
            weekly_hours=weekly_hours,
            remaining_hours=remaining,
        )

        savings = estimate_cut_savings(employee, shift, remaining, state)

        recommendations.append(
            StaffingRecommendation(
                action=StaffAction.cut,
                employee_id=employee.id,
                shift_id=shift.id,
                reason=(
                    f"{employee.employment_type.value} with {remaining:.1f}h remaining, "
                    f"saving ${savings}"
                ),
                priority=min(priority, 1.0),
                estimated_savings=savings,
            )
        )

    # Sort by priority descending, limit to how many we can actually cut
    recommendations.sort(key=lambda r: r.priority, reverse=True)
    max_cuts = len(active_shifts) - min_staff
    return recommendations[:max_cuts]


def rank_for_call_in(
    available_employees: list[Employee],
    current_shifts: list[Shift],
    target_date: date,
    target_hour: int,
    state: State,
    needed_roles: list[str] = None,
) -> list[StaffingRecommendation]:
    """
    Rank available employees for calling in.

    Considers weekly hours, employment type, skills, and availability.

    Args:
        available_employees: Employees who could potentially be called in.
        current_shifts: All shifts this week (to calculate hours worked).
        target_date: Date we need staff for.
        target_hour: Hour we need staff for.
        state: State for context.
        needed_roles: Specific roles/skills needed (optional).

    Returns:
        Ordered list of StaffingRecommendations (best candidate first).
    """
    recommendations = []

    for employee in available_employees:
        weekly_hours = get_weekly_hours(employee.id, current_shifts)

        priority = _call_in_priority_score(
            employee=employee,
            target_date=target_date,
            target_hour=target_hour,
            weekly_hours=weekly_hours,
            needed_roles=needed_roles,
        )

        recommendations.append(
            StaffingRecommendation(
                action=StaffAction.call_in,
                employee_id=employee.id,
                shift_id=None,
                reason=(
                    f"{employee.employment_type.value}, "
                    f"{employee.max_hours_per_week - weekly_hours:.1f}h remaining this week"
                ),
                priority=min(priority, 1.0),
                estimated_savings=None,
            )
        )

    recommendations.sort(key=lambda r: r.priority, reverse=True)
    return recommendations


def make_decision(
    variance: float,
    active_shifts: list[tuple[Employee, Shift]],
    available_employees: list[Employee],
    current_shifts_this_week: dict[str, list[Shift]],
    state: State,
    target_date: date,
    target_hour: int,
    threshold: float = 0.15,
    min_staff: int = 1,
) -> list[StaffingRecommendation]:
    """
    Main decision function based on demand variance.

    - If variance < -threshold: recommend staff cuts (overstaffed)
    - If variance > threshold: recommend call-ins (understaffed)
    - Otherwise: no action needed

    Args:
        variance: Current demand variance (-1.0 to 1.0).
        active_shifts: Currently working (Employee, Shift) pairs.
        available_employees: Employees available to call in.
        current_shifts_this_week: Employee ID -> list of shifts this week.
        state: Australian state.
        target_date: Current date.
        target_hour: Current hour.
        threshold: Variance threshold for action (default 0.15).
        min_staff: Minimum staff to maintain.

    Returns:
        List of StaffingRecommendations, or empty list if no action needed.
    """
    if variance < -threshold:
        # Overstaffed — recommend cuts
        return rank_for_cut(
            active_shifts=active_shifts,
            state=state,
            min_staff=min_staff,
            current_hour=target_hour,
            weekly_shifts=current_shifts_this_week,
        )
    elif variance > threshold:
        # Understaffed — recommend call-ins
        all_shifts = []
        for shifts in current_shifts_this_week.values():
            all_shifts.extend(shifts)

        return rank_for_call_in(
            available_employees=available_employees,
            current_shifts=all_shifts,
            target_date=target_date,
            target_hour=target_hour,
            state=state,
        )
    else:
        # Within acceptable range
        return []
