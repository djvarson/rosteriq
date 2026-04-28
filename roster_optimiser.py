"""
Roster optimisation engine for RosterIQ.

Takes demand forecasts, employee pool, venue config, and award rules to generate
cost-optimised rosters that meet staffing requirements while respecting all
Fair Work compliance constraints.

The optimiser uses a greedy constraint-satisfaction approach:
1. Determine required staff per hour from demand forecasts (covers-to-staff ratio)
2. For each day, assign shifts to meet demand while minimising cost
3. Respect all constraints: max hours, consecutive days, break rules, min engagement
4. Prefer full-time/part-time employees before casuals (lower total cost)
5. Spread hours fairly across the team

This is a heuristic solver — not globally optimal, but fast and practical.
A future version could use mixed-integer linear programming (PuLP/OR-Tools)
for true optimal solutions.
"""

from datetime import date, time, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from collections import defaultdict
import uuid

from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
    CostBreakdown,
)
from rosteriq.award_rules import (
    get_penalty_multiplier, get_day_type, get_minimum_break_minutes,
    get_minimum_engagement_hours, validate_shift_compliance,
    check_consecutive_days, calculate_overtime_hours,
    MAX_SHIFT_LENGTH_HOURS,
)
from rosteriq.cost_calculator import calculate_shift_cost_breakdown


# ============================================================================
# Configuration defaults
# ============================================================================

# Covers per staff member per hour — the core ratio
DEFAULT_COVERS_PER_STAFF = 15.0

# Standard shift templates (start_hour, end_hour, break_minutes)
SHIFT_TEMPLATES = {
    "morning":   (6, 14, 30),    # 8h with 30min break
    "mid":       (10, 18, 30),   # 8h with 30min break
    "afternoon": (14, 22, 30),   # 8h with 30min break
    "evening":   (17, 23, 0),    # 6h no break needed
    "short_am":  (8, 13, 0),     # 5h no break needed
    "short_pm":  (17, 22, 0),    # 5h no break needed
    "full_day":  (9, 18, 50),    # 9h with 50min break
    "split_eve": (18, 23, 0),    # 5h evening
}


# ============================================================================
# Demand-to-staffing conversion
# ============================================================================

def calculate_required_staff(
    forecasts: list[DemandForecast],
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
    min_staff_by_role: dict[str, int] = None,
) -> dict[int, int]:
    """
    Convert demand forecasts into required staff count per hour.

    Args:
        forecasts: Hour-by-hour demand forecasts for a single day.
        covers_per_staff: How many covers one staff member can handle.
        min_staff_by_role: Minimum staff per role (summed as floor).

    Returns:
        Dict mapping hour (0-23) to required staff count.
    """
    min_total = 0
    if min_staff_by_role:
        min_total = sum(min_staff_by_role.values())

    required = {}
    for fc in forecasts:
        staff_needed = max(1, int(fc.predicted_covers / covers_per_staff + 0.5))
        staff_needed = max(staff_needed, min_total)
        required[fc.hour] = staff_needed

    return required


def identify_peak_periods(
    required_staff: dict[int, int],
) -> list[dict]:
    """
    Identify contiguous periods of similar staffing need.

    Groups hours into blocks to align shift templates efficiently.

    Returns:
        List of dicts: {start_hour, end_hour, peak_staff, avg_staff}
    """
    if not required_staff:
        return []

    hours = sorted(required_staff.keys())
    periods = []
    current = {
        "start_hour": hours[0],
        "end_hour": hours[0] + 1,
        "staff_values": [required_staff[hours[0]]],
    }

    for h in hours[1:]:
        prev_staff = required_staff.get(h - 1, 0)
        curr_staff = required_staff[h]

        # Group if adjacent and demand is within 30% of each other
        if h == current["end_hour"] and (
            prev_staff == 0 or abs(curr_staff - prev_staff) / max(prev_staff, 1) < 0.3
        ):
            current["end_hour"] = h + 1
            current["staff_values"].append(curr_staff)
        else:
            periods.append({
                "start_hour": current["start_hour"],
                "end_hour": current["end_hour"],
                "peak_staff": max(current["staff_values"]),
                "avg_staff": sum(current["staff_values"]) / len(current["staff_values"]),
            })
            current = {
                "start_hour": h,
                "end_hour": h + 1,
                "staff_values": [curr_staff],
            }

    # Don't forget the last period
    periods.append({
        "start_hour": current["start_hour"],
        "end_hour": current["end_hour"],
        "peak_staff": max(current["staff_values"]),
        "avg_staff": sum(current["staff_values"]) / len(current["staff_values"]),
    })

    return periods


# ============================================================================
# Employee scoring and selection
# ============================================================================

def _employee_cost_score(
    employee: Employee,
    target_date: date,
    state: State,
    weekly_hours_so_far: float,
) -> float:
    """
    Score an employee for assignment — lower is better (cheaper).

    Factors:
    - Employment type cost: FT=1.0, PT=1.1, Casual=1.3 (loading makes casuals pricier)
    - Penalty multiplier for the day
    - Overtime risk (if close to 38h, they'll trigger overtime)
    - Hours fairness (prefer employees who've worked fewer hours)
    """
    day_type = get_day_type(target_date, state)
    multiplier = float(get_penalty_multiplier(employee.employment_type, day_type))

    # Base type cost
    type_cost = {
        EmploymentType.full_time: 1.0,
        EmploymentType.part_time: 1.1,
        EmploymentType.casual: 1.25,
    }.get(employee.employment_type, 1.2)

    # Overtime risk — penalise if close to weekly limit
    hours_headroom = employee.max_hours_per_week - weekly_hours_so_far
    overtime_penalty = 0.0
    if hours_headroom <= 0:
        overtime_penalty = 2.0  # Very expensive — avoid
    elif hours_headroom < 8:
        overtime_penalty = 0.5  # Getting close

    # Fairness — prefer less-worked employees
    fairness = weekly_hours_so_far / max(employee.max_hours_per_week, 1)

    return (type_cost * multiplier) + overtime_penalty + (fairness * 0.3)


def _is_employee_available(
    employee: Employee,
    target_date: date,
    start_hour: int,
    end_hour: int,
    existing_shifts: list[Shift],
    weekly_hours: float,
) -> tuple[bool, str]:
    """
    Check if an employee can work a proposed shift.

    Returns (is_available, reason_if_not).
    """
    # Check weekly hours cap
    proposed_hours = end_hour - start_hour
    if weekly_hours + proposed_hours > employee.max_hours_per_week:
        return False, "Would exceed weekly hours limit"

    # Check consecutive days
    recent_dates = sorted(set(s.date for s in existing_shifts if s.employee_id == employee.id))
    if recent_dates:
        # Add the proposed date and check
        test_dates = recent_dates + [target_date]
        test_dates = sorted(set(test_dates))
        # Count max consecutive
        max_consec = 1
        current_consec = 1
        for i in range(1, len(test_dates)):
            if test_dates[i] == test_dates[i-1] + timedelta(days=1):
                current_consec += 1
                max_consec = max(max_consec, current_consec)
            else:
                current_consec = 1
        if max_consec > employee.consecutive_days_limit:
            return False, "Would exceed consecutive days limit"

    # Check not already working at that time on that day
    day_shifts = [s for s in existing_shifts
                  if s.employee_id == employee.id and s.date == target_date]
    if day_shifts:
        return False, "Already has a shift on this day"

    # Check minimum rest from previous day's shift
    yesterday_shifts = [s for s in existing_shifts
                        if s.employee_id == employee.id
                        and s.date == target_date - timedelta(days=1)]
    if yesterday_shifts:
        last_shift = max(yesterday_shifts, key=lambda s: s.end_time)
        last_end_hour = last_shift.end_time.hour
        # Hours of rest
        rest_hours = (24 - last_end_hour) + start_hour
        if rest_hours < 10:
            return False, f"Only {rest_hours}h rest (need 10h minimum)"

    # Check availability schedule (if provided)
    if employee.availability:
        day_name = target_date.strftime("%A").lower()
        if day_name in employee.availability:
            ranges = employee.availability[day_name]
            available = False
            for r in ranges:
                try:
                    avail_start = int(str(r.get("start", "0")).split(":")[0])
                    avail_end = int(str(r.get("end", "23")).split(":")[0])
                    if avail_start <= start_hour and avail_end >= end_hour:
                        available = True
                        break
                except (ValueError, TypeError):
                    available = True  # Can't parse = assume available
                    break
            if not available:
                return False, "Not available at this time"

    return True, ""


# ============================================================================
# Shift generation
# ============================================================================

def _best_shift_template(
    start_hour: int,
    end_hour: int,
    employment_type: EmploymentType,
) -> tuple[int, int, int]:
    """
    Pick the best shift template for a period.

    Returns (shift_start, shift_end, break_minutes).
    """
    duration = end_hour - start_hour
    min_engagement = get_minimum_engagement_hours(employment_type)

    # Clamp to max shift length
    if duration > MAX_SHIFT_LENGTH_HOURS:
        end_hour = start_hour + int(MAX_SHIFT_LENGTH_HOURS)
        duration = end_hour - start_hour

    # Ensure minimum engagement
    if duration < min_engagement:
        end_hour = start_hour + int(min_engagement + 0.5)
        duration = end_hour - start_hour

    # Calculate required break
    break_minutes = get_minimum_break_minutes(float(duration))

    return start_hour, min(end_hour, 24), break_minutes


def _create_shift(
    employee: Employee,
    target_date: date,
    start_hour: int,
    end_hour: int,
    break_minutes: int,
    role: str = "general",
) -> Shift:
    """Create a Shift object."""
    return Shift(
        id=str(uuid.uuid4())[:8],
        employee_id=employee.id,
        date=target_date,
        start_time=time(start_hour, 0),
        end_time=time(min(end_hour, 23), 0) if end_hour < 24 else time(0, 0),
        break_minutes=break_minutes,
        status=ShiftStatus.scheduled,
        role=role,
    )


# ============================================================================
# Main optimiser
# ============================================================================

def generate_daily_roster(
    target_date: date,
    forecasts: list[DemandForecast],
    employees: list[Employee],
    state: State,
    existing_shifts: list[Shift] = None,
    venue_config: VenueConfig = None,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
) -> list[Shift]:
    """
    Generate an optimised roster for a single day.

    Algorithm:
    1. Convert forecasts to required staff per hour
    2. Identify peak periods and align to shift templates
    3. For each period, assign cheapest available employees
    4. Validate all shifts for compliance

    Args:
        target_date: Date to roster.
        forecasts: Demand forecasts for this day.
        employees: Available employee pool.
        state: Australian state for award rules.
        existing_shifts: Shifts already assigned this week (for hours tracking).
        venue_config: Venue configuration (for min staff, etc.)
        covers_per_staff: Covers-to-staff ratio.

    Returns:
        List of Shift objects for the day.
    """
    existing_shifts = existing_shifts or []
    min_staff_by_role = venue_config.min_staff if venue_config else None

    # Step 1: Calculate required staff per hour
    required = calculate_required_staff(forecasts, covers_per_staff, min_staff_by_role)
    if not required:
        return []

    # Step 2: Identify peak periods
    periods = identify_peak_periods(required)

    # Track weekly hours for each employee
    weekly_hours = defaultdict(float)
    for shift in existing_shifts:
        if shift.status not in (ShiftStatus.cancelled, ShiftStatus.no_show):
            weekly_hours[shift.employee_id] += shift.net_hours

    # Step 3: For each period, assign employees
    day_shifts = []
    assigned_today = set()  # employee IDs already assigned today

    for period in periods:
        staff_needed = int(period["peak_staff"])
        start_h = period["start_hour"]
        end_h = period["end_hour"]

        # Score and sort employees by cost (cheapest first)
        candidates = []
        for emp in employees:
            if emp.id in assigned_today:
                continue

            available, reason = _is_employee_available(
                emp, target_date, start_h, end_h,
                existing_shifts + day_shifts,
                weekly_hours[emp.id],
            )
            if not available:
                continue

            score = _employee_cost_score(emp, target_date, state, weekly_hours[emp.id])
            candidates.append((score, emp))

        # Sort by score (lowest = cheapest)
        candidates.sort(key=lambda x: x[0])

        # Assign the cheapest N employees
        for _, emp in candidates[:staff_needed]:
            shift_start, shift_end, break_mins = _best_shift_template(
                start_h, end_h, emp.employment_type
            )
            shift = _create_shift(emp, target_date, shift_start, shift_end, break_mins)

            # Validate compliance before committing
            emp_existing = [s for s in existing_shifts + day_shifts if s.employee_id == emp.id]
            violations = validate_shift_compliance(emp, shift, emp_existing)
            if violations:
                continue  # Skip this assignment, try next candidate

            day_shifts.append(shift)
            assigned_today.add(emp.id)
            weekly_hours[emp.id] += shift.net_hours

    return day_shifts


def generate_weekly_roster(
    week_start: date,
    weekly_forecasts: list[DemandForecast],
    employees: list[Employee],
    venue_config: VenueConfig,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
) -> Roster:
    """
    Generate a full weekly roster.

    Processes each day sequentially so that earlier days' assignments
    influence later days (weekly hours tracking, consecutive days).

    Args:
        week_start: Monday of the roster week.
        weekly_forecasts: Forecasts for all 7 days.
        employees: Full employee pool.
        venue_config: Venue configuration.
        covers_per_staff: Covers-to-staff ratio.

    Returns:
        Complete Roster object with all shifts and total cost.
    """
    state = venue_config.state
    all_shifts = []
    week_end = week_start + timedelta(days=6)

    for day_offset in range(7):
        target_date = week_start + timedelta(days=day_offset)

        # Filter forecasts for this day
        day_forecasts = [f for f in weekly_forecasts if f.date == target_date]
        if not day_forecasts:
            continue

        # Generate this day's shifts, passing in all shifts so far this week
        day_shifts = generate_daily_roster(
            target_date=target_date,
            forecasts=day_forecasts,
            employees=employees,
            state=state,
            existing_shifts=all_shifts,
            venue_config=venue_config,
            covers_per_staff=covers_per_staff,
        )
        all_shifts.extend(day_shifts)

    # Calculate total cost
    emp_dict = {e.id: e for e in employees}
    total_cost = Decimal("0")
    for shift in all_shifts:
        if shift.employee_id in emp_dict:
            breakdown = calculate_shift_cost_breakdown(
                emp_dict[shift.employee_id], shift, state
            )
            total_cost += breakdown.total_cost
            shift.cost = breakdown.total_cost
            shift.penalty_multiplier = float(
                get_penalty_multiplier(
                    emp_dict[shift.employee_id].employment_type,
                    get_day_type(shift.date, state),
                )
            )

    return Roster(
        id=str(uuid.uuid4())[:8],
        venue_id=venue_config.id,
        week_start=week_start,
        week_end=week_end,
        shifts=all_shifts,
        total_cost=total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        created_at=datetime.now(),
    )


# ============================================================================
# Roster analysis and comparison
# ============================================================================

def analyse_roster(
    roster: Roster,
    employees: dict[str, Employee],
    state: State,
) -> dict:
    """
    Produce a detailed analysis of a roster.

    Returns:
        Dict with: total_cost, total_hours, avg_cost_per_hour,
        shifts_per_day, employee_hours, compliance_issues,
        cost_by_day, staffing_by_hour.
    """
    total_cost = Decimal("0")
    total_hours = 0.0
    shifts_per_day = defaultdict(int)
    employee_hours = defaultdict(float)
    cost_by_day = defaultdict(Decimal)
    staffing_by_hour = defaultdict(lambda: defaultdict(int))
    compliance_issues = []

    for shift in roster.shifts:
        emp = employees.get(shift.employee_id)
        if not emp:
            continue

        breakdown = calculate_shift_cost_breakdown(emp, shift, state)
        total_cost += breakdown.total_cost
        total_hours += shift.net_hours
        shifts_per_day[shift.date] += 1
        employee_hours[shift.employee_id] += shift.net_hours
        cost_by_day[shift.date] += breakdown.total_cost

        # Track hourly staffing
        start_h = shift.start_time.hour
        end_h = shift.end_time.hour
        if end_h <= start_h:
            end_h += 24
        for h in range(start_h, min(end_h, 24)):
            staffing_by_hour[shift.date][h] += 1

        # Check compliance
        emp_shifts = [s for s in roster.shifts if s.employee_id == shift.employee_id]
        violations = validate_shift_compliance(emp, shift, emp_shifts)
        for v in violations:
            compliance_issues.append({
                "employee": emp.name,
                "shift_id": shift.id,
                "date": shift.date,
                "violation": v,
            })

    avg_cost = (total_cost / Decimal(str(total_hours))).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    ) if total_hours > 0 else Decimal("0")

    return {
        "total_cost": total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "total_hours": round(total_hours, 1),
        "total_shifts": len(roster.shifts),
        "avg_cost_per_hour": avg_cost,
        "unique_employees": len(roster.employees_used),
        "shifts_per_day": dict(shifts_per_day),
        "employee_hours": dict(employee_hours),
        "cost_by_day": {k: v.quantize(Decimal("0.01")) for k, v in cost_by_day.items()},
        "compliance_issues": compliance_issues,
        "compliance_clean": len(compliance_issues) == 0,
    }


def suggest_improvements(
    roster: Roster,
    forecasts: list[DemandForecast],
    employees: dict[str, Employee],
    state: State,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
) -> list[str]:
    """
    Suggest improvements to an existing roster.

    Compares actual staffing levels to forecast demand and identifies
    over/understaffed periods.

    Args:
        roster: The roster to analyse.
        forecasts: Demand forecasts for the roster period.
        employees: Employee dict.
        state: Australian state.
        covers_per_staff: Covers-to-staff ratio.

    Returns:
        List of improvement suggestion strings.
    """
    suggestions = []

    # Build hourly staffing from roster
    staffing = defaultdict(lambda: defaultdict(int))
    for shift in roster.shifts:
        start_h = shift.start_time.hour
        end_h = shift.end_time.hour
        if end_h <= start_h:
            end_h += 24
        for h in range(start_h, min(end_h, 24)):
            staffing[shift.date][h] += 1

    # Compare to forecast demand
    for fc in forecasts:
        required = max(1, int(fc.predicted_covers / covers_per_staff + 0.5))
        actual = staffing.get(fc.date, {}).get(fc.hour, 0)

        if actual > required + 1:
            suggestions.append(
                f"{fc.date} at {fc.hour}:00 — overstaffed by {actual - required} "
                f"(have {actual}, need {required} for {fc.predicted_covers:.0f} covers)"
            )
        elif actual < required - 1 and actual > 0:
            suggestions.append(
                f"{fc.date} at {fc.hour}:00 — understaffed by {required - actual} "
                f"(have {actual}, need {required} for {fc.predicted_covers:.0f} covers)"
            )
        elif actual == 0 and required > 0:
            suggestions.append(
                f"{fc.date} at {fc.hour}:00 — no staff rostered "
                f"(need {required} for {fc.predicted_covers:.0f} covers)"
            )

    # Check for high-cost shifts that could be replaced
    for shift in roster.shifts:
        emp = employees.get(shift.employee_id)
        if not emp:
            continue
        day_type = get_day_type(shift.date, state)
        mult = float(get_penalty_multiplier(emp.employment_type, day_type))
        if mult >= 2.0 and shift.net_hours > 6:
            suggestions.append(
                f"Shift {shift.id} on {shift.date}: {emp.name} ({emp.employment_type.value}) "
                f"at {mult}x rate for {shift.net_hours:.1f}h — consider splitting or swapping"
            )

    return suggestions
