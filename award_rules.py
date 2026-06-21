"""
Fair Work Australia hospitality award compliance engine for RosterIQ.

This module implements penalty rate calculations and compliance validation
for the Hospitality Industry General Award 2020 (MA000009). All calculations
are based on the current Fair Work Commission determinations and are suitable
for real-world compliance use.

Key regulations implemented:
- Penalty rates by day type and employment type
- Overtime calculations and loadings
- Break requirements based on shift duration
- Consecutive work day limits
- Minimum engagement hours
- Public holiday entitlements
"""

import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import List, Optional

logger = logging.getLogger(__name__)

from rosteriq.models import (
    EmploymentType,
    DayType,
    AwardLevel,
    State,
    Shift,
    Employee,
)


# ============================================================================
# PENALTY RATE CONSTANTS
# ============================================================================

# Base multipliers by day type and employment type
PENALTY_MULTIPLIERS = {
    EmploymentType.full_time: {
        DayType.weekday: Decimal("1.0"),
        DayType.saturday: Decimal("1.25"),
        DayType.sunday: Decimal("1.5"),
        # MA000009 public holiday penalty for full-time/part-time is 225%
        # (was incorrectly 250%, the casual rate).
        DayType.public_holiday: Decimal("2.25"),
    },
    EmploymentType.part_time: {
        DayType.weekday: Decimal("1.0"),
        DayType.saturday: Decimal("1.25"),
        DayType.sunday: Decimal("1.5"),
        DayType.public_holiday: Decimal("2.25"),
    },
    EmploymentType.casual: {
        DayType.weekday: Decimal("1.25"),  # includes 25% casual loading
        DayType.saturday: Decimal("1.5"),
        DayType.sunday: Decimal("1.75"),
        DayType.public_holiday: Decimal("2.5"),
    },
}

# Overtime multipliers (full-time and part-time only)
OVERTIME_MULTIPLIERS = {
    "first_2_hours": Decimal("1.5"),
    "after_2_hours": Decimal("2.0"),
    "sunday": Decimal("2.0"),
    "public_holiday": Decimal("2.5"),
}

# Evening/night work penalties (Monday-Friday only)
EVENING_LOADING_THRESHOLDS = {
    # After 7pm: +15% loading
    "7pm": Decimal("0.15"),
    # After midnight: +17.5% loading (replaces 7pm loading)
    "midnight": Decimal("0.175"),
}

# Time-of-day band boundaries (minutes from midnight) for weekday loading.
_EVENING_START_MIN = 19 * 60   # 7:00pm
_NIGHT_END_MIN = 7 * 60        # 7:00am (after-midnight band runs 00:00-07:00)


def split_weekday_hours_by_band(start_time, end_time, net_hours) -> dict:
    """
    Split a weekday shift's NET worked hours across time-of-day bands:
      - 'evening' : hours worked 19:00-24:00 (+15% loading)
      - 'night'   : hours worked 00:00-07:00 (+17.5% loading)
      - 'ordinary': all other hours (07:00-19:00)

    This is what makes loading apply to the hours ACTUALLY WORKED in the evening/
    night, rather than classifying the whole shift by its start hour (so a
    09:00-23:00 shift now earns evening loading on its 19:00-23:00 hours instead
    of $0). Handles shifts crossing midnight. The unpaid break is deducted
    proportionally across the worked span. Returns a dict summing to net_hours.

    NOTE: the loading PERCENTAGES are the existing model values; the exact award
    $ amounts / band boundaries are a separate confirmation item.
    """
    net = Decimal(str(net_hours))
    if net <= 0:
        return {"ordinary": Decimal("0.00"), "evening": Decimal("0.00"), "night": Decimal("0.00")}

    start_min = start_time.hour * 60 + start_time.minute
    end_min = end_time.hour * 60 + end_time.minute
    if end_min <= start_min:
        end_min += 24 * 60  # shift crosses midnight

    def _overlap(a, b, lo, hi):
        return max(0, min(b, hi) - max(a, lo))

    # Evening 19:00-24:00 (this day) and the same window on a next-day spillover.
    evening_min = (
        _overlap(start_min, end_min, _EVENING_START_MIN, 24 * 60)
        + _overlap(start_min, end_min, 24 * 60 + _EVENING_START_MIN, 48 * 60)
    )
    # Night 00:00-07:00 (this day, e.g. an overnight start) and next-day 00:00-07:00.
    night_min = (
        _overlap(start_min, end_min, 0, _NIGHT_END_MIN)
        + _overlap(start_min, end_min, 24 * 60, 24 * 60 + _NIGHT_END_MIN)
    )
    gross_min = end_min - start_min
    ordinary_min = max(0, gross_min - evening_min - night_min)
    gross_hours = Decimal(gross_min) / 60
    if gross_hours <= 0:
        return {"ordinary": net, "evening": Decimal("0.00"), "night": Decimal("0.00")}

    # Scale gross band hours down to NET (proportional unpaid-break deduction).
    scale = net / gross_hours
    return {
        "ordinary": (Decimal(ordinary_min) / 60 * scale),
        "evening": (Decimal(evening_min) / 60 * scale),
        "night": (Decimal(night_min) / 60 * scale),
    }

# Compliance constants
MAX_SHIFT_LENGTH_HOURS = 11.5
MINIMUM_HOURS_BETWEEN_SHIFTS = 10
CASUAL_MINIMUM_ENGAGEMENT_HOURS = 2.0
PART_TIME_MINIMUM_ENGAGEMENT_HOURS = 3.0
FULL_TIME_MINIMUM_ENGAGEMENT_HOURS = 0.0  # No minimum for full-time
MAX_CONSECUTIVE_DAYS = 6

# Break requirements (minutes)
BREAK_REQUIREMENTS = {
    5: 30,      # Shifts > 5 hours: 30 min unpaid
    7: 50,      # Shifts > 7 hours: 30 min unpaid + 20 min paid = 50 total
    10: 90,     # Shifts > 10 hours: 30 min unpaid + 40 min paid = 70 total
}


# ============================================================================
# PUBLIC HOLIDAY CALCULATIONS
# ============================================================================


def _easter_date(year: int) -> date:
    """
    Calculate Easter Sunday using the Anonymous Gregorian algorithm.

    Returns the date of Easter Sunday for the given year.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Date of the nth <weekday> (0=Mon..6=Sun) in a month (n>=1)."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _add_with_substitute(holidays: List[date], d: date) -> None:
    """
    Append a fixed-date holiday and, if it falls on a weekend, the next free
    weekday as a substitute. NYD / Australia Day / Christmas / Boxing Day are
    substituted to a weekday in every state when they land on Sat/Sun (so staff
    working the observed day get public-holiday rates). Process in date order so
    Christmas/Boxing-Day substitutes don't collide.
    """
    holidays.append(d)
    if d.weekday() >= 5:  # Saturday(5) or Sunday(6)
        sub = d + timedelta(days=1)
        while sub.weekday() >= 5 or sub in holidays:
            sub += timedelta(days=1)
        holidays.append(sub)


def _first_monday_in_august(year: int) -> date:
    """Return the first Monday in August for a given year."""
    aug_1 = date(year, 8, 1)
    days_until_monday = (7 - aug_1.weekday()) % 7
    if days_until_monday == 0 and aug_1.weekday() != 0:
        days_until_monday = 7
    return aug_1 + timedelta(days=days_until_monday if aug_1.weekday() != 0 else 0)


def _second_monday_in_june(year: int) -> date:
    """Return the second Monday in June for a given year."""
    jun_1 = date(year, 6, 1)
    days_until_monday = (7 - jun_1.weekday()) % 7
    first_monday = jun_1 + timedelta(days=days_until_monday if jun_1.weekday() != 0 else 0)
    if first_monday.day == 1:
        first_monday = jun_1 + timedelta(days=7 if jun_1.weekday() != 0 else 7)
    # Find first Monday
    for day in range(1, 8):
        d = date(year, 6, day)
        if d.weekday() == 0:
            first_monday = d
            break
    return first_monday + timedelta(days=7)


def _first_tuesday_in_november(year: int) -> date:
    """Return the first Tuesday in November for a given year."""
    nov_1 = date(year, 11, 1)
    days_until_tuesday = (1 - nov_1.weekday()) % 7
    return nov_1 + timedelta(days=days_until_tuesday if nov_1.weekday() != 1 else 0)


def _last_monday_in_october(year: int) -> date:
    """Return the last Monday in October for a given year."""
    oct_31 = date(year, 10, 31)
    days_back = (oct_31.weekday() - 0) % 7
    return oct_31 - timedelta(days=days_back)


# Our State enum -> the `holidays` library's Australian subdivision codes.
_AU_SUBDIV = {
    State.nsw: "NSW", State.vic: "VIC", State.qld: "QLD", State.sa: "SA",
    State.wa: "WA", State.tas: "TAS", State.nt: "NT", State.act: "ACT",
}


@lru_cache(maxsize=512)
def _public_holidays_cached(state: State, year: int) -> tuple:
    """Memoized (state, year) -> immutable tuple of public-holiday dates.

    The PH set for a (state, year) is fixed, but get_day_type() calls this for EVERY
    shift cost calc and the optimiser calls it per candidate×role×period×day — so
    rebuilding the holidays.Australia object each time was ~700x wasted work on a
    single roster pass. Caching the result removes it.
    """
    subdiv = _AU_SUBDIV.get(state)
    if subdiv is not None:
        try:
            import holidays as _holidays
            au = _holidays.Australia(subdiv=subdiv, years=year)
            return tuple(sorted(au.keys()))
        except Exception as e:  # pragma: no cover - defensive fallback
            logger.warning(
                "holidays library unavailable (%s); using fallback PH list for %s",
                e, state,
            )
    return tuple(_get_public_holidays_manual(state, year))


def get_public_holidays(state: State, year: int) -> List[date]:
    """
    Public holidays for an Australian state/territory and year — drives the
    public-holiday penalty rate, so correctness here is money-sensitive for EVERY
    venue, in every state.

    Backed by the maintained `holidays` library, which knows each state's gazetted
    public holidays, including ones a fixed rule can't compute: WA's King's Birthday
    (proclaimed annually, late Sep/early Oct), QLD's King's Birthday (October),
    Easter Saturday only where it actually applies (NOT WA/TAS), and state-only days
    (WA Day, Adelaide Cup, Picnic Day, Reconciliation Day, May Day, etc.), plus
    weekend substitutes. Falls back to a hand-computed list only if the library is
    unavailable. Memoized per (state, year) — returns a fresh list so callers may
    mutate it without corrupting the cache.
    """
    return list(_public_holidays_cached(state, year))


def _get_public_holidays_manual(state: State, year: int) -> List[date]:
    """
    Hand-computed fallback (national + the major state holidays). Used only when the
    `holidays` library can't be imported. Incomplete for WA/SA/TAS/NT/ACT — the
    library is the source of truth; this just keeps the engine running.
    """
    holidays = []

    # National holidays (weekend-substituted to a weekday when they fall Sat/Sun)
    _add_with_substitute(holidays, date(year, 1, 1))   # New Year's Day
    _add_with_substitute(holidays, date(year, 1, 26))  # Australia Day

    # Easter dates
    easter_sunday = _easter_date(year)
    good_friday = easter_sunday - timedelta(days=2)
    easter_saturday = easter_sunday - timedelta(days=1)
    easter_monday = easter_sunday + timedelta(days=1)

    holidays.append(good_friday)
    holidays.append(easter_saturday)
    holidays.append(easter_monday)

    # Anzac Day
    holidays.append(date(year, 4, 25))

    # Christmas and Boxing Day (weekend-substituted)
    _add_with_substitute(holidays, date(year, 12, 25))
    _add_with_substitute(holidays, date(year, 12, 26))

    # Labour Day (state-specific date). Missing entirely before — a real public
    # holiday on which staff were underpaid. Standard AU dates:
    #   WA  1st Mon Mar | VIC/TAS 2nd Mon Mar | QLD/NT 1st Mon May
    #   NSW/SA/ACT 1st Mon Oct
    _labour_day = {
        State.wa: (3, 1), State.vic: (3, 2), State.tas: (3, 2),
        State.qld: (5, 1), State.nt: (5, 1),
        State.nsw: (10, 1), State.sa: (10, 1), State.act: (10, 1),
    }
    if state in _labour_day:
        month, nth = _labour_day[state]
        holidays.append(_nth_weekday(year, month, 0, nth))  # 0 = Monday

    # State-specific holidays
    if state == State.nsw:
        # Queen's Birthday (second Monday in June)
        jun_1 = date(year, 6, 1)
        days_to_first_monday = (7 - jun_1.weekday()) % 7
        if jun_1.weekday() == 0:
            first_monday = jun_1
        else:
            first_monday = jun_1 + timedelta(days=days_to_first_monday)
        holidays.append(first_monday + timedelta(days=7))

        # Bank Holiday (first Monday in August)
        aug_1 = date(year, 8, 1)
        days_to_first_monday = (7 - aug_1.weekday()) % 7
        if aug_1.weekday() == 0:
            holidays.append(aug_1)
        else:
            holidays.append(aug_1 + timedelta(days=days_to_first_monday))

    elif state == State.vic:
        # Queen's/King's Birthday (second Monday in June)
        jun_1 = date(year, 6, 1)
        days_to_first_monday = (7 - jun_1.weekday()) % 7
        if jun_1.weekday() == 0:
            first_monday = jun_1
        else:
            first_monday = jun_1 + timedelta(days=days_to_first_monday)
        holidays.append(first_monday + timedelta(days=7))

        # Melbourne Cup Day (first Tuesday in November)
        nov_1 = date(year, 11, 1)
        days_to_first_tuesday = (1 - nov_1.weekday()) % 7
        if nov_1.weekday() == 1:
            holidays.append(nov_1)
        else:
            holidays.append(nov_1 + timedelta(days=days_to_first_tuesday))

    elif state == State.qld:
        # Queen's/King's Birthday (last Monday in October)
        oct_31 = date(year, 10, 31)
        days_back = (oct_31.weekday() - 0) % 7
        holidays.append(oct_31 - timedelta(days=days_back))

        # Royal Queensland Show - Brisbane only (second Wednesday in August)
        # For simplicity, including statewide
        aug_1 = date(year, 8, 1)
        days_to_first_wednesday = (2 - aug_1.weekday()) % 7
        if aug_1.weekday() == 2:
            first_wednesday = aug_1
        else:
            first_wednesday = aug_1 + timedelta(days=days_to_first_wednesday)
        holidays.append(first_wednesday + timedelta(days=7))

    # Remove duplicates and sort
    return sorted(list(set(holidays)))


def get_day_type(date_obj: date, state: State) -> DayType:
    """
    Determine the day type (weekday, Saturday, Sunday, or public holiday).

    Args:
        date_obj: The date to classify
        state: The state for public holiday determination

    Returns:
        DayType enum value
    """
    # Check if it's a public holiday first
    public_holidays = get_public_holidays(state, date_obj.year)
    if date_obj in public_holidays:
        return DayType.public_holiday

    # Check day of week
    weekday = date_obj.weekday()  # 0=Monday, 6=Sunday
    if weekday == 5:  # Saturday
        return DayType.saturday
    elif weekday == 6:  # Sunday
        return DayType.sunday
    else:  # Monday-Friday
        return DayType.weekday


# ============================================================================
# PENALTY MULTIPLIER CALCULATIONS
# ============================================================================


def get_penalty_multiplier(
    employment_type: EmploymentType,
    day_type: DayType,
    hour: int = 12,
    overtime_hours: float = 0,
) -> Decimal:
    """
    Calculate the total penalty multiplier for a shift.

    Combines base day-type multiplier with overtime and evening loadings.
    For casual employees, the 25% loading is already included in the base multiplier.

    Args:
        employment_type: Type of employment (full-time, part-time, casual)
        day_type: Type of day (weekday, Saturday, Sunday, public_holiday)
        hour: Hour of day for evening loading calculations (0-23)
        overtime_hours: Number of overtime hours worked (only applies to full/part-time)

    Returns:
        Decimal multiplier to apply to base hourly rate
    """
    # Get base multiplier for day type and employment
    base_multiplier = PENALTY_MULTIPLIERS[employment_type][day_type]

    # Casual employees don't get overtime rates beyond their daily penalty
    if employment_type == EmploymentType.casual:
        return base_multiplier

    # Apply overtime multiplier if present
    if overtime_hours > 0:
        if day_type == DayType.public_holiday:
            # Public holiday overtime: flat 2.5x
            return OVERTIME_MULTIPLIERS["public_holiday"]
        elif day_type == DayType.sunday:
            # Sunday overtime: flat 2.0x
            return OVERTIME_MULTIPLIERS["sunday"]
        else:
            # Weekday/Saturday overtime: 1.5x for first 2 hours, 2.0x after
            if overtime_hours <= 2:
                return OVERTIME_MULTIPLIERS["first_2_hours"]
            else:
                return OVERTIME_MULTIPLIERS["after_2_hours"]

    # Apply evening/night loading (Monday-Friday only)
    if day_type == DayType.weekday:
        if hour >= 0:  # Midnight or later until 7pm
            if hour < 19:  # Before 7pm
                pass  # No additional loading
            elif hour < 24:  # 7pm to midnight
                base_multiplier += EVENING_LOADING_THRESHOLDS["7pm"]
        # For simplification, treating midnight-7am as after midnight
        if hour >= 0 and hour < 7:
            base_multiplier += EVENING_LOADING_THRESHOLDS["midnight"]

    return base_multiplier


def _get_hour_from_shift_start(shift: Shift) -> int:
    """Extract hour from shift start time."""
    return shift.start_time.hour


# ============================================================================
# COMPLIANCE VALIDATION
# ============================================================================


def get_minimum_break_minutes(shift_duration_hours: float) -> int:
    """
    Return the minimum required unpaid break for a given shift duration.

    Fair Work requires:
    - Shifts > 5 hours: minimum 30 minutes unpaid break
    - Shifts > 7 hours: minimum 30 min unpaid + 20 min paid = 50 total
    - Shifts > 10 hours: minimum 30 min unpaid + 40 min paid = 70 total

    Args:
        shift_duration_hours: Total shift duration in hours (before breaks)

    Returns:
        Minimum required break minutes
    """
    if shift_duration_hours > 10:
        return 70  # 30 min unpaid + 2x20 min paid
    elif shift_duration_hours > 7:
        return 50  # 30 min unpaid + 20 min paid
    elif shift_duration_hours > 5:
        return 30  # 30 min unpaid
    return 0


def get_minimum_engagement_hours(employment_type: EmploymentType) -> float:
    """
    Return the minimum shift length for an employment type.

    Fair Work requires:
    - Casual: minimum 2 hours per engagement
    - Part-time: minimum 3 hours per engagement
    - Full-time: no minimum (by agreement)

    Args:
        employment_type: Type of employment

    Returns:
        Minimum hours per shift
    """
    if employment_type == EmploymentType.casual:
        return CASUAL_MINIMUM_ENGAGEMENT_HOURS
    elif employment_type == EmploymentType.part_time:
        return PART_TIME_MINIMUM_ENGAGEMENT_HOURS
    return FULL_TIME_MINIMUM_ENGAGEMENT_HOURS


def validate_shift_compliance(
    employee: Employee,
    shift: Shift,
    recent_shifts: Optional[List[Shift]] = None,
) -> List[str]:
    """
    Validate a shift against Fair Work compliance requirements.

    Checks:
    - Shift duration doesn't exceed maximum (11.5 hours)
    - Minimum break provided for duration
    - Minimum engagement hours met
    - Consecutive days limit not exceeded
    - Minimum rest between shifts maintained

    Args:
        employee: The employee working the shift
        shift: The shift to validate
        recent_shifts: List of recent shifts for the employee (optional)

    Returns:
        List of compliance violation descriptions. Empty list means compliant.
    """
    violations = []

    # Check maximum shift length
    if shift.duration_hours > MAX_SHIFT_LENGTH_HOURS:
        violations.append(
            f"Shift duration {shift.duration_hours:.1f}h exceeds maximum "
            f"{MAX_SHIFT_LENGTH_HOURS}h"
        )

    # Check minimum engagement hours
    min_engagement = get_minimum_engagement_hours(employee.employment_type)
    if shift.net_hours < min_engagement:
        violations.append(
            f"Shift duration {shift.net_hours:.1f}h is below minimum "
            f"{min_engagement}h for {employee.employment_type.value}"
        )

    # Check minimum break provided
    required_break = get_minimum_break_minutes(shift.duration_hours)
    if shift.break_minutes < required_break:
        violations.append(
            f"Shift break {shift.break_minutes}min is below minimum "
            f"{required_break}min for {shift.duration_hours:.1f}h shift"
        )

    # Check consecutive days limit
    if recent_shifts:
        violations.extend(_check_consecutive_days_violation(shift, recent_shifts))

        # Check minimum rest between shifts
        violations.extend(_check_minimum_rest_violation(shift, recent_shifts))

    return violations


def _check_consecutive_days_violation(shift: Shift, recent_shifts: List[Shift]) -> List[str]:
    """Check if shift violates consecutive days limit."""
    violations = []

    # Count consecutive days ending on shift date
    consecutive_count = 1
    check_date = shift.date - timedelta(days=1)

    for s in sorted(recent_shifts, key=lambda x: x.date, reverse=True):
        if s.date == check_date:
            consecutive_count += 1
            check_date -= timedelta(days=1)
        elif s.date < check_date:
            break

    if consecutive_count > MAX_CONSECUTIVE_DAYS:
        violations.append(
            f"Shift creates {consecutive_count} consecutive days "
            f"(maximum {MAX_CONSECUTIVE_DAYS} allowed)"
        )

    return violations


def _shift_end_datetime(shift: Shift) -> datetime:
    """A shift's actual END as a datetime, accounting for crossing midnight (an
    overnight shift's end_time is on the day AFTER its .date)."""
    end_dt = datetime.combine(shift.date, shift.end_time)
    if shift.end_time <= shift.start_time:
        end_dt += timedelta(days=1)
    return end_dt


def _check_minimum_rest_violation(shift: Shift, recent_shifts: List[Shift]) -> List[str]:
    """Check if shift violates minimum rest between shifts."""
    violations = []

    # Any OTHER shift that actually ENDS at or before this one starts — compared as
    # real datetimes, so it catches both an overnight previous-day shift (Mon
    # 22:00->Tue 06:00 vs Tue 09:00 = 3h) AND a SAME-DAY split shift under the
    # minimum (06:00-14:00 then 15:00-23:00 = 1h). Filtering by `s.date < shift.date`
    # missed the same-day case, diverging from conflict_detector._check_fatigue_risk.
    this_start = datetime.combine(shift.date, shift.start_time)
    previous_shifts = [
        s for s in recent_shifts
        if getattr(s, "id", None) != getattr(shift, "id", None)
        and _shift_end_datetime(s) <= this_start
    ]
    if not previous_shifts:
        return violations

    last_shift = max(previous_shifts, key=_shift_end_datetime)
    rest_hours = (this_start - _shift_end_datetime(last_shift)).total_seconds() / 3600

    if 0 <= rest_hours < MINIMUM_HOURS_BETWEEN_SHIFTS:
        violations.append(
            f"Only {rest_hours:.1f}h rest between shifts "
            f"(minimum {MINIMUM_HOURS_BETWEEN_SHIFTS}h required)"
        )

    return violations


def check_consecutive_days(shifts: List[Shift], max_consecutive: int = 6) -> bool:
    """
    Check if a list of shifts violates consecutive days limits.

    Args:
        shifts: List of shifts sorted by date
        max_consecutive: Maximum consecutive days allowed (default 6)

    Returns:
        True if limit is violated, False if compliant
    """
    if not shifts:
        return False

    # Sort shifts by date
    sorted_shifts = sorted(shifts, key=lambda x: x.date)

    max_consecutive_found = 1
    current_consecutive = 1

    for i in range(1, len(sorted_shifts)):
        if sorted_shifts[i].date == sorted_shifts[i - 1].date + timedelta(days=1):
            current_consecutive += 1
            max_consecutive_found = max(max_consecutive_found, current_consecutive)
        else:
            current_consecutive = 1

    return max_consecutive_found > max_consecutive


def calculate_overtime_hours(
    employee: Employee,
    shifts_this_week: List[Shift],
) -> float:
    """
    Calculate overtime hours for an employee in a week.

    Overtime is hours beyond 38 hours per week (the ordinary hours for full-time).
    Part-time employees' maximum is based on their employment agreement,
    but for compliance purposes we use 38 as the threshold.
    Casual employees don't accrue traditional overtime.

    Args:
        employee: The employee
        shifts_this_week: Shifts worked this week

    Returns:
        Number of hours worked as overtime this week
    """
    if employee.employment_type == EmploymentType.casual:
        return 0.0

    total_hours = sum(shift.net_hours for shift in shifts_this_week)
    ordinary_hours = min(employee.max_hours_per_week, 38.0)

    return max(0.0, total_hours - ordinary_hours)


# ============================================================================
# SHIFT COST CALCULATION
# ============================================================================


def calculate_shift_cost(
    employee: Employee,
    shift: Shift,
    state: State,
) -> Decimal:
    """
    Calculate the full cost of a shift including all penalties and loadings.

    Includes:
    - Base hourly rate with day-type penalty multiplier
    - Overtime rates if applicable
    - Evening/night work loadings
    - 25% casual loading (already in casual base multiplier)
    (Superannuation is NOT included here — this returns the gross shift wage only.)

    Args:
        employee: The employee working the shift
        shift: The shift to cost
        state: The state for public holiday determination

    Returns:
        Total shift cost as Decimal, rounded to 2 decimal places
    """
    day_type = get_day_type(shift.date, state)

    # Get base multiplier
    base_multiplier = PENALTY_MULTIPLIERS[employee.employment_type][day_type]
    rate = employee.hourly_base_rate
    net_hours = Decimal(str(shift.net_hours))

    # On a weekday, evening/night loading applies to the hours ACTUALLY WORKED in
    # those windows (19:00-24:00 +15%, 00:00-07:00 +17.5%), not the whole shift
    # gated on its start hour — otherwise a 17:00-23:00 shift earns no loading and a
    # 06:00-15:00 shift over-charges all 9h. Sat/Sun/PH use the whole day-type rate.
    if day_type == DayType.weekday and shift.start_time and shift.end_time:
        bands = split_weekday_hours_by_band(shift.start_time, shift.end_time, net_hours)
        band_loading = {
            "ordinary": Decimal("0"),
            "evening": EVENING_LOADING_THRESHOLDS["7pm"],
            "night": EVENING_LOADING_THRESHOLDS["midnight"],
        }
        shift_cost = Decimal("0")
        for band, bhrs in bands.items():
            if bhrs <= 0:
                continue
            shift_cost += rate * (base_multiplier + band_loading[band]) * bhrs
    else:
        shift_cost = rate * base_multiplier * net_hours

    return shift_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
