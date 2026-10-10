"""
Roster optimisation engine for RosterIQ.

Takes demand forecasts, employee pool, venue config, and award rules to generate
cost-optimised rosters that meet staffing requirements while respecting all
Fair Work compliance constraints.

How a week is built:
1. Each day trades over the venue's recorded hours (else the hours its own
   forecasts cover, else 10:00 to midnight).
2. Every area gets its weekday minimum for every trading hour; covers above
   what the minimums handle become extra staff, shared across the areas that
   follow demand in proportion to their minimums (supervisory areas and the
   bottle shop hold their minimum unless the venue says otherwise).
3. Each area's hourly need is shaped into whole shifts: opener and closer
   blocks carry the minimum from open to close, then lunch and dinner blocks
   carry the peaks. A gap next to an existing block extends that block rather
   than adding a person.
4. Minimum blocks across the whole week are filled first (busiest day first),
   then the peak blocks. Each block goes to the cheapest eligible person:
   recorded rate times the day's multiplier (casual loading included), priced
   by the cost calculator. Salaried staff are already paid, so they are used
   first up to their contracted hours.
5. A repair pass extends or adds shifts wherever an area is still below its
   minimum.

Hard limits on every shift: the area's skill rule, approved leave,
availability, weekly hours, a recorded visa fortnight cap, rest between
shifts, consecutive days, minimum engagement, maximum shift length, one shift
a day. Inactive staff are never rostered.
"""

from datetime import date, time, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from collections import defaultdict
import logging
import uuid

logger = logging.getLogger(__name__)

from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
    CostBreakdown, holds_skill, normalise_area, normalise_skills,
)
from rosteriq.award_rules import (
    get_penalty_multiplier, get_day_type, get_minimum_break_minutes,
    get_minimum_engagement_hours, validate_shift_compliance,
    check_consecutive_days, calculate_overtime_hours,
    MAX_SHIFT_LENGTH_HOURS, MAX_CONSECUTIVE_DAYS, MINIMUM_HOURS_BETWEEN_SHIFTS,
    OVERTIME_MULTIPLIERS,
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown, calculate_roster_cost, WEEKLY_OVERTIME_THRESHOLD,
)
from rosteriq.services.availability_rules import is_available
from rosteriq.services import venue_areas as _areas


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
    "full_day":  (9, 18, 30),    # 9h with 30min unpaid meal break (paid rest is worked time)
    "split_eve": (18, 23, 0),    # 5h evening
}

# Shift shapes the generator builds, in hours. Rostering preferences, not
# award values: minimum engagement and MAX_SHIFT_LENGTH_HOURS apply on top.
PREFERRED_MIN_SHIFT_HOURS = 4
PREFERRED_MAX_SHIFT_HOURS = 10
# Two peaks this close together are one shift, not two people.
MERGE_GAP_HOURS = 1
# The trading day when neither the venue's hours nor real forecasts say.
DEFAULT_OPEN_HOUR = 10
DEFAULT_TRADING_HOURS = 14
# Ranking nudge ($/h at a full week) so equally priced people share the hours.
FAIRNESS_PER_HOUR = 0.5


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

def _is_salaried(employee) -> bool:
    basis = getattr(employee, "pay_basis", None)
    return str(getattr(basis, "value", basis) or "hourly").strip().lower() == "salaried"


def _hourly_cost(employee: Employee, target_date: date, state: State) -> float:
    """Dollars an hour of this person costs that day: the recorded rate times
    the day's multiplier (which carries the casual loading)."""
    day_type = get_day_type(target_date, state)
    return float(employee.hourly_base_rate) * float(
        get_penalty_multiplier(employee.employment_type, day_type))


def _ranking_surcharge(employee: Employee, weekly_hours_so_far: float) -> float:
    """Per-hour amount added to pay when ranking (never charged): weekly
    overtime for permanents past the ordinary-hours threshold, a heavy
    penalty at the weekly cap, and a small nudge toward whoever has worked
    less so equally priced people share the hours."""
    rate = float(employee.hourly_base_rate)
    extra = 0.0
    if (employee.employment_type != EmploymentType.casual
            and weekly_hours_so_far >= float(WEEKLY_OVERTIME_THRESHOLD)):
        extra += rate * float(OVERTIME_MULTIPLIERS["first_2_hours"] - 1)
    if employee.max_hours_per_week - weekly_hours_so_far <= 0:
        extra += rate
    extra += FAIRNESS_PER_HOUR * weekly_hours_so_far / max(employee.max_hours_per_week, 1)
    return extra


def _employee_cost_score(
    employee: Employee,
    target_date: date,
    state: State,
    weekly_hours_so_far: float,
) -> float:
    """
    Score an employee for assignment, in dollars per hour — lower is cheaper.

    The recorded rate times the day multiplier (casual loading included),
    plus the ranking surcharge. Salaried staff inside their contracted hours
    are already paid for, so they score zero.
    """
    if _is_salaried(employee) and weekly_hours_so_far < employee.max_hours_per_week:
        return 0.0
    return (_hourly_cost(employee, target_date, state)
            + _ranking_surcharge(employee, weekly_hours_so_far))


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

    # A shift already booked the next day (at another venue) needs the rest
    # gap after this one too.
    proposed_end = datetime.combine(target_date, time(0)) + timedelta(hours=end_hour)
    for s in existing_shifts:
        if s.employee_id == employee.id and s.date == target_date + timedelta(days=1):
            gap = (datetime.combine(s.date, s.start_time) - proposed_end).total_seconds() / 3600
            if gap < MINIMUM_HOURS_BETWEEN_SHIFTS:
                return False, f"Only {gap:g}h rest before their next shift"

    # Availability, by the one shared rule (to the minute; "23:59" = close).
    if not is_available(employee.availability, target_date,
                        f"{start_hour:02d}:00", f"{end_hour:02d}:00"):
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
    """Create a Shift object (end_hour past 24 runs into the next morning)."""
    return Shift(
        id=uuid.uuid4().hex,
        employee_id=employee.id,
        date=target_date,
        start_time=time(start_hour, 0),
        end_time=time(end_hour % 24, 0),
        break_minutes=break_minutes,
        status=ShiftStatus.scheduled,
        role=role,
    )


# ============================================================================
# Role-aware allocation
# ============================================================================

# Generic/catch-all roles that any employee can fill regardless of skills.
# Kept in sync with ConflictDetector._GENERIC_ROLES so generated rosters
# pass skill-mismatch detection cleanly.
GENERIC_ROLES = {"general", "any", "staff", "floor_general"}


def _roles_for_venue(min_staff_by_role: Optional[dict[str, int]]) -> list[str]:
    """
    Determine the ordered list of roles to staff.

    Falls back to a single generic "general" role when the venue defines no
    per-role minimums (preserves the original role-agnostic behaviour).
    """
    if not min_staff_by_role:
        return ["general"]
    return list(min_staff_by_role.keys())


def _apportion(total: int, weights: dict) -> dict:
    """Split `total` whole people across weights (largest remainder; ties go
    to the heavier weight, then the earlier key)."""
    out = {k: 0 for k in weights}
    weight_sum = sum(weights.values())
    if total <= 0 or weight_sum <= 0:
        return out
    exact = {k: total * w / weight_sum for k, w in weights.items()}
    for k in out:
        out[k] = int(exact[k])
    order = list(weights)
    left = total - sum(out.values())
    for k in sorted(order, key=lambda k: (-(exact[k] - out[k]), -weights[k], order.index(k)))[:left]:
        out[k] += 1
    return out


def _allocate_headcount_to_roles(
    staff_needed: int,
    roles: list[str],
    min_staff_by_role: Optional[dict[str, int]],
    weights: Optional[dict[str, float]] = None,
) -> dict[str, int]:
    """
    Split a period's required headcount across roles.

    Each role starts at its configured minimum; headcount above the minimums
    is shared by weight (default: in proportion to the minimums, evenly when
    there are none) so the TOTAL equals staff_needed whenever it is at least
    the sum of the minimums.
    """
    if not roles:
        return {}

    min_staff_by_role = min_staff_by_role or {}
    allocation = {role: min_staff_by_role.get(role, 0) for role in roles}
    if weights is None:
        weights = {role: allocation[role] for role in roles}
    weights = {role: weights.get(role, 0) for role in roles}
    if sum(weights.values()) <= 0:
        weights = {role: 1 for role in roles}
    for role, extra in _apportion(staff_needed - sum(allocation.values()), weights).items():
        allocation[role] += extra
    return allocation


def _employee_can_fill_role(employee: Employee, role: str, venue=None) -> bool:
    """
    Skill-matching rule.

    An employee may work a role if:
    - the role is a generic/catch-all role (no specialised skill required), OR
    - the role is one of the employee's listed skills (ignoring case), OR
    - the employee lists no skills at all and the area is a general one: an
      area that requires its skill (duty manager, supervisor) never takes
      someone without it.
    """
    if normalise_area(role) in GENERIC_ROLES:
        return True
    skills = getattr(employee, "skills", None)
    if holds_skill(skills, role):
        return True
    if normalise_skills(skills):
        return False
    return _areas.can_fill(employee, role, venue)


# ============================================================================
# Shaping an area's day into shifts
# ============================================================================

def _positive_runs(values: list) -> list:
    """[start, end) index runs where values are positive."""
    runs, start = [], None
    for i, v in enumerate(values):
        if v > 0 and start is None:
            start = i
        elif v <= 0 and start is not None:
            runs.append((start, i))
            start = None
    if start is not None:
        runs.append((start, len(values)))
    return runs


def _merge_runs(runs: list) -> list:
    out = []
    for s, e in runs:
        if out and s - out[-1][1] <= MERGE_GAP_HOURS and e - out[-1][0] <= PREFERRED_MAX_SHIFT_HOURS:
            out[-1] = (out[-1][0], e)
        else:
            out.append((s, e))
    return out


def _split_run(s: int, e: int) -> list:
    """A run longer than a shift becomes near-equal shifts (a 10:00-24:00
    day is an opener to 17:00 and a closer from 17:00)."""
    length = e - s
    if length <= PREFERRED_MAX_SHIFT_HOURS:
        return [(s, e)]
    count = -(-length // PREFERRED_MAX_SHIFT_HOURS)
    pieces, cur = [], s
    for i in range(count):
        size = (e - cur) // (count - i)
        pieces.append((cur, cur + size))
        cur += size
    return pieces


def _grow(s: int, e: int, need: list) -> tuple:
    """Stretch a short block to the preferred minimum inside the trading day,
    toward the busier neighbour (the earlier one on a tie)."""
    n = len(need)
    target = min(PREFERRED_MIN_SHIFT_HOURS, n)
    while e - s < target:
        before = need[s - 1] if s > 0 else -1
        after = need[e] if e < n else -1
        if after > before:
            e += 1
        else:
            s -= 1
    return s, e


def _adjacent_block(blocks: list, s: int, e: int):
    """The earlier block that can stretch over [s, e) within a shift's length,
    preferring the one that stays shortest."""
    best = None
    for block in blocks:
        if s - MERGE_GAP_HOURS <= block[1] <= s:
            length = e - block[0]
        elif e <= block[0] <= e + MERGE_GAP_HOURS:
            length = block[1] - s
        else:
            continue
        if length <= PREFERRED_MAX_SHIFT_HOURS and (best is None or length < best[0]):
            best = (length, block)
    return best[1] if best else None


def _shape_blocks(need: list, minimum: int) -> list:
    """Cover an area's hourly need with [start, end, is_minimum] slot blocks.

    Layer by layer: the first `minimum` layers span open to close (split into
    opener and closer when the day is longer than a shift); later layers are
    the peaks, each stretched to a realistic length, and a peak that touches
    an earlier block extends it instead of adding a person.
    """
    n = len(need)
    blocks: list = []
    cover = [0] * n
    layer = 0
    while layer < 500:
        runs = _positive_runs([need[i] - cover[i] for i in range(n)])
        if not runs:
            break
        layer += 1
        is_minimum = layer <= minimum
        earlier = list(blocks)
        for s, e in _merge_runs(runs):
            if not is_minimum:
                block = _adjacent_block(earlier, s, e)
                if block is not None:
                    block[0], block[1] = min(block[0], s), max(block[1], e)
                    continue
            for ps, pe in _split_run(s, e):
                ps, pe = _grow(ps, pe, need)
                blocks.append([ps, pe, is_minimum])
        cover = [0] * n
        for bs, be, _ in blocks:
            for i in range(bs, be):
                cover[i] += 1
    return blocks


def _is_cold_start(fc) -> bool:
    return str(getattr(fc, "model_version", "") or "").startswith("cold_start")


def _forecast_window(day: date, forecasts: list):
    """(open, close) spanned by the day's own forecast hours, or None. The
    day starts after its longest quiet stretch, so early-morning hours read
    as the tail of a late night. Cold-start guesses don't count: their hours
    are a default, not the venue's."""
    hours = sorted({fc.hour for fc in forecasts if fc.date == day and not _is_cold_start(fc)})
    if not hours:
        return None
    if len(hours) == 24:
        start, quiet = 0, 0
    else:
        start, quiet = hours[0], -1
        for i, h in enumerate(hours):
            nxt = hours[(i + 1) % len(hours)]
            gap = (nxt - h - 1) % 24
            if gap > quiet:
                start, quiet = nxt, gap
    opens = datetime.combine(day, time(start))
    return opens, opens + timedelta(hours=24 - quiet)


# ============================================================================
# The builder
# ============================================================================

class _DayPlan:
    """One trading day: its clock-hour slots, each area's hourly need, and
    the blocks shaped from it (slot indices, end exclusive)."""

    def __init__(self, day, opens, closes, areas):
        self.day = day
        self.opens = opens
        self.closes = closes
        self.areas = areas
        self.need = {}
        self.blocks = []
        self.load = 0
        self.slots = []
        cur = opens
        while cur < closes and len(self.slots) < 24:
            self.slots.append(cur)
            cur += timedelta(hours=1)

    def slot_start(self, i):
        return self.slots[i]

    def slot_end(self, i):
        """End of the block whose last slot is i - 1."""
        return min(self.slots[i - 1] + timedelta(hours=1), self.closes)


class _RosterBuilder:
    """Shapes and staffs days against everything already booked: this run's
    shifts, the caller's existing shifts (earlier days, other venues) and the
    previous week."""

    def __init__(self, employees, venue, state, covers_per_staff, forecasts,
                 week_start, existing_shifts=None, approved_leave=None,
                 prior_week_shifts=None):
        self.venue = venue
        self.state = state
        self.cps = covers_per_staff if covers_per_staff and covers_per_staff > 0 \
            else DEFAULT_COVERS_PER_STAFF
        self.week_start = week_start
        self.pool = [e for e in employees if getattr(e, "active", True)]
        self.forecasts = list(forecasts or [])
        self.covers = {}
        for fc in self.forecasts:
            key = (fc.date, fc.hour)
            self.covers[key] = max(self.covers.get(key, 0.0), float(fc.predicted_covers))
        self.leave = {str(k): list(v or []) for k, v in (approved_leave or {}).items()}
        self.prior_known = prior_week_shifts is not None
        self.booked = defaultdict(list)      # employee id -> [(start, end, date, net, gross, shift)]
        seen = set()
        for s in list(existing_shifts or []) + list(prior_week_shifts or []):
            if s.status in (ShiftStatus.cancelled, ShiftStatus.no_show) or s.id in seen:
                continue
            seen.add(s.id)
            self._book(s)
        self.generated: list = []
        self._by_id = {str(e.id): e for e in self.pool}
        self._eligible = {}
        self._costs = {}

    # --- bookings -----------------------------------------------------------

    @staticmethod
    def _span(shift):
        start = datetime.combine(shift.date, shift.start_time)
        return start, start + timedelta(hours=shift.duration_hours)

    def _book(self, shift):
        start, end = self._span(shift)
        self.booked[str(shift.employee_id)].append(
            (start, end, shift.date, shift.net_hours, shift.duration_hours, shift))

    def _unbook(self, shift):
        eid = str(shift.employee_id)
        self.booked[eid] = [b for b in self.booked[eid] if b[5] is not shift]

    def _week_hours(self, eid, gross=False, offset_days=0):
        lo = self.week_start + timedelta(days=offset_days)
        hi = lo + timedelta(days=7)
        return sum(b[4] if gross else b[3] for b in self.booked[eid] if lo <= b[2] < hi)

    def _visa_allows(self, emp, gross_hours):
        """The recorded fortnight cap over this week and the one before. With
        no record of the previous week, plan half the cap per week so no
        fortnight can go over."""
        cap = getattr(emp, "visa_work_limit_fortnight", None)
        if not cap:
            return True
        eid = str(emp.id)
        prior = self._week_hours(eid, gross=True, offset_days=-7)
        limit = float(cap) - prior if (self.prior_known or prior > 0) else float(cap) / 2
        return self._week_hours(eid, gross=True) + gross_hours <= limit + 1e-9

    def _run_of_days(self, eid, day):
        days = {b[2] for b in self.booked[eid]}
        run, cur = 1, day - timedelta(days=1)
        while cur in days:
            run, cur = run + 1, cur - timedelta(days=1)
        cur = day + timedelta(days=1)
        while cur in days:
            run, cur = run + 1, cur + timedelta(days=1)
        return run

    def _can_work(self, emp, day, start, end, net, gross):
        eid = str(emp.id)
        rest = timedelta(hours=MINIMUM_HOURS_BETWEEN_SHIFTS)
        for b_start, b_end, b_day, _, _, _ in self.booked[eid]:
            if b_day == day or (b_start < end and start < b_end):
                return False
            if (start - b_end if b_end <= start else b_start - end) < rest:
                return False
        for lo, hi in self.leave.get(eid, ()):
            if lo <= day <= hi:
                return False
        if self._week_hours(eid) + net > emp.max_hours_per_week + 1e-9:
            return False
        if not self._visa_allows(emp, gross):
            return False
        if self._run_of_days(eid, day) > min(emp.consecutive_days_limit, MAX_CONSECUTIVE_DAYS):
            return False
        return is_available(emp.availability, day, start.strftime("%H:%M"), end.strftime("%H:%M"))

    # --- planning -----------------------------------------------------------

    def _window(self, day):
        if self.venue is not None:
            if _areas.is_closed(self.venue, day):
                return None
            window = _areas.trading_window(self.venue, day)
            if window:
                return window
        window = _forecast_window(day, self.forecasts)
        if window:
            return window
        if any(fc.date == day for fc in self.forecasts):
            opens = datetime.combine(day, time(DEFAULT_OPEN_HOUR))
            return opens, opens + timedelta(hours=DEFAULT_TRADING_HOURS)
        return None

    def _areas_for(self, day):
        areas = _areas.venue_areas(self.venue) if self.venue is not None else []
        if not areas:
            return [{"name": "general", "min": 1, "requires_skill": False, "follows_demand": True}]
        key = _areas.day_key(day)
        return [{"name": a["name"], "min": a["min_by_day"].get(key, 0),
                 "requires_skill": a["requires_skill"],
                 "follows_demand": a.get("follows_demand", not a["requires_skill"])}
                for a in areas]

    def plan_day(self, day):
        window = self._window(day)
        if window is None:
            return None
        opens, closes = window
        areas = self._areas_for(day)
        plan = _DayPlan(day, opens, closes, areas)
        minimums = {a["name"]: a["min"] for a in areas}
        flexible = [a for a in areas if a["follows_demand"]] or areas
        weights = {a["name"]: a["min"] for a in flexible}
        if sum(weights.values()) <= 0:
            weights = {name: 1 for name in weights}
        floor = sum(minimums.values())
        need = {name: [m] * len(plan.slots) for name, m in minimums.items()}
        for i, slot in enumerate(plan.slots):
            covers = self.covers.get((slot.date(), slot.hour))
            if covers is None:
                continue
            extra = int(covers / self.cps + 0.5) - floor
            for name, n in _apportion(extra, weights).items():
                need[name][i] += n
        plan.need = need
        plan.load = sum(sum(v) for v in need.values())
        for a in areas:
            for s, e, is_minimum in _shape_blocks(need[a["name"]], a["min"]):
                plan.blocks.append({"area": a["name"], "s": s, "e": e, "minimum": is_minimum,
                                    "peak": max(need[a["name"]][s:e])})
        return plan

    # --- staffing -----------------------------------------------------------

    def _eligible_for(self, area):
        if area not in self._eligible:
            self._eligible[area] = [e for e in self.pool
                                    if _employee_can_fill_role(e, area, self.venue)]
        return self._eligible[area]

    def _area_order(self, plan):
        """Supervisory areas first, then the scarcest people per shift."""
        counts = defaultdict(int)
        for b in plan.blocks:
            counts[b["area"]] += 1
        order = sorted(plan.areas, key=lambda a: (
            not a["requires_skill"],
            len(self._eligible_for(a["name"])) / max(counts[a["name"]], 1),
            a["name"]))
        return {a["name"]: i for i, a in enumerate(order)}

    def _block_cost(self, emp, day, start, end, brk):
        key = (emp.employment_type, emp.hourly_base_rate, day, start, end, brk)
        if key not in self._costs:
            probe = Shift(id="probe", employee_id=str(emp.id), date=day,
                          start_time=start.time(), end_time=end.time(), break_minutes=brk,
                          status=ShiftStatus.scheduled, role="probe")
            self._costs[key] = float(
                calculate_shift_cost_breakdown(emp, probe, self.state).total_cost)
        return self._costs[key]

    def _shape_for(self, emp, plan, start, end):
        """The block as this person would work it: stretched to their minimum
        engagement (inside the trading day where it fits), with the meal break."""
        short = get_minimum_engagement_hours(emp.employment_type) - (end - start).total_seconds() / 3600
        if short > 0:
            short = timedelta(hours=short)
            later = min(short, max(plan.closes - end, timedelta(0)))
            end, short = end + later, short - later
            earlier = min(short, max(start - plan.opens, timedelta(0)))
            start, short = start - earlier, short - earlier
            end += short
        gross = (end - start).total_seconds() / 3600
        brk = get_minimum_break_minutes(gross)
        return start, end, gross, brk, gross - brk / 60

    def _candidates(self, plan, area, start, end):
        """Who could work this block, best first: people who hold the area's
        skill before people with no skills recorded (a fresh hire is not a
        cook); then salaried staff, already paid; then the cheapest."""
        generic = normalise_area(area) in GENERIC_ROLES
        ranked = []
        for emp in self._eligible_for(area):
            st, en, gross, brk, net = self._shape_for(emp, plan, start, end)
            if gross > MAX_SHIFT_LENGTH_HOURS or not self._can_work(emp, plan.day, st, en, net, gross):
                continue
            eid = str(emp.id)
            worked = self._week_hours(eid)
            untrained = not generic and not holds_skill(emp.skills, area)
            if _is_salaried(emp):
                key = (untrained, 0, 0.0, worked, eid)
            else:
                cost = self._block_cost(emp, plan.day, st, en, brk) \
                    + net * _ranking_surcharge(emp, worked)
                key = (untrained, 1, round(cost, 2), worked, eid)
            ranked.append((key, emp, st, en, brk))
        ranked.sort(key=lambda r: r[0])
        return ranked

    def _place(self, plan, area, start, end):
        for _, emp, st, en, brk in self._candidates(plan, area, start, end):
            shift = Shift(id=uuid.uuid4().hex, employee_id=emp.id, date=plan.day,
                          start_time=st.time(), end_time=en.time(), break_minutes=brk,
                          status=ShiftStatus.scheduled, role=area)
            if validate_shift_compliance(emp, shift, [b[5] for b in self.booked[str(emp.id)]]):
                continue
            self._book(shift)
            self.generated.append(shift)
            return shift
        return None

    def _fill(self, plan, area, s, e):
        """Staff slots [s, e); if nobody can take the whole block, its halves."""
        if self._place(plan, area, plan.slot_start(s), plan.slot_end(e)):
            return True
        if e - s >= 6:
            mid = s + (e - s) // 2
            first = self._fill(plan, area, s, mid)
            second = self._fill(plan, area, mid, e)
            return first or second
        return False

    def _area_shifts(self, plan, area):
        return [s for s in self.generated if s.date == plan.day and s.role == area]

    def _stretch(self, plan, area, s, e):
        """Extend an area's shift that ends at, or starts right after, the
        uncovered slots [s, e) — the person already on beats a new one."""
        start, end = plan.slot_start(s), plan.slot_end(e)
        gap = timedelta(hours=MERGE_GAP_HOURS)
        for shift in sorted(self._area_shifts(plan, area), key=lambda x: x.duration_hours):
            cur_start, cur_end = self._span(shift)
            if start - gap <= cur_end <= start:
                new_start, new_end = cur_start, end
            elif end <= cur_start <= end + gap:
                new_start, new_end = start, cur_end
            else:
                continue
            gross = (new_end - new_start).total_seconds() / 3600
            if gross > MAX_SHIFT_LENGTH_HOURS:
                continue
            emp = self._by_id.get(str(shift.employee_id))
            if emp is None:
                continue
            brk = get_minimum_break_minutes(gross)
            self._unbook(shift)
            if self._can_work(emp, plan.day, new_start, new_end, gross - brk / 60, gross):
                shift.start_time, shift.end_time = new_start.time(), new_end.time()
                shift.break_minutes = brk
                self._book(shift)
                return True
            self._book(shift)
        return False

    def _repair(self, plan):
        """Wherever an area is still below its minimum, extend a shift or add
        a person, until it holds or nobody can."""
        for area in plan.areas:
            if area["min"] <= 0:
                continue
            name = area["name"]
            for _ in range(len(plan.slots) + 1):
                cover = [0] * len(plan.slots)
                for shift in self._area_shifts(plan, name):
                    start, end = self._span(shift)
                    for i, slot in enumerate(plan.slots):
                        if start < plan.slot_end(i + 1) and end > slot:
                            cover[i] += 1
                runs = _positive_runs([area["min"] - c for c in cover])
                if not runs:
                    break
                progress = False
                for s, e in runs:
                    if self._stretch(plan, name, s, e):
                        progress = True
                        continue
                    gs, ge = _grow(s, e, plan.need[name])
                    if self._fill(plan, name, gs, ge):
                        progress = True
                if not progress:
                    break

    def staff(self, plans):
        """Minimum blocks for every day first (busiest day first), then the
        peaks, then the repair pass."""
        plans = sorted(plans, key=lambda p: (-p.load, p.day))
        ranks = {p.day: self._area_order(p) for p in plans}
        for plan in plans:
            rank = ranks[plan.day]
            base = [b for b in plan.blocks if b["minimum"]]
            for b in sorted(base, key=lambda b: (rank[b["area"]], -b["s"], -b["e"])):
                self._fill(plan, b["area"], b["s"], b["e"])
        for plan in plans:
            rank = ranks[plan.day]
            peaks = [b for b in plan.blocks if not b["minimum"]]
            for b in sorted(peaks, key=lambda b: (-b["peak"], rank[b["area"]], b["s"], -b["e"])):
                self._fill(plan, b["area"], b["s"], b["e"])
        for plan in plans:
            self._repair(plan)


def _shift_order(shift):
    return (shift.date, shift.start_time, normalise_area(shift.role), str(shift.employee_id))


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
    Generate a roster for a single day (see the module docstring).

    Args:
        target_date: Date to roster.
        forecasts: Demand forecasts for this day.
        employees: Available employee pool.
        state: Australian state for award rules.
        existing_shifts: Shifts the people already have (earlier days this
            week, other venues): they count toward hours, rest and days.
        venue_config: Venue configuration (areas, minimums, trading hours).
        covers_per_staff: Covers-to-staff ratio.

    Returns:
        List of Shift objects for the day.
    """
    week_start = target_date - timedelta(days=target_date.weekday())
    builder = _RosterBuilder(employees, venue_config, state, covers_per_staff, forecasts,
                             week_start, existing_shifts=existing_shifts)
    plan = builder.plan_day(target_date)
    if plan is None:
        return []
    builder.staff([plan])
    return sorted(builder.generated, key=_shift_order)


def _rostered_coverage_by_cell(roster: Roster) -> dict:
    """Map (date, hour) -> number of rostered staff working that clock hour.

    A shift covers every whole clock-hour it touches, handled correctly for:
    overnight shifts (23:00–03:00 covers hour 23 of its date AND 00–02 of the
    NEXT date — so a post-midnight demand hour is attributed to the right day),
    sub-hour shifts (09:15–09:45 covers only hour 9, never 24 hours), and
    zero-length shifts (cover nothing). This replaces the old start<=h<end
    hour test that mis-scored all three cases.
    """
    from collections import defaultdict
    cells: dict = defaultdict(int)
    for s in roster.shifts:
        if not s.employee_id:
            continue  # an open shift is a gap until someone is on it
        start_dt = datetime.combine(s.date, s.start_time)
        end_dt = datetime.combine(s.date, s.end_time)
        if s.end_time < s.start_time:          # crosses midnight
            end_dt += timedelta(days=1)
        elif s.end_time == s.start_time:        # zero-length -> covers nothing
            continue
        cur = start_dt.replace(minute=0, second=0, microsecond=0)
        while cur < end_dt:
            cells[(cur.date(), cur.hour)] += 1
            cur += timedelta(hours=1)
    return cells


def compute_coverage_gaps(
    roster: Roster,
    weekly_forecasts: list[DemandForecast],
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
    min_staff_by_role: dict = None,
    venue_config: Optional[VenueConfig] = None,
    trading_hours: Optional[dict] = None,
) -> dict:
    """Compare demand against the roster at EVERY forecast hour and report the
    worst-covered hour per day — so a manager never publishes a quietly
    understaffed day. Checking only the single busiest hour (the old
    behaviour) missed a roster that was fine at lunch but short at dinner.

    Per day: for each forecast hour, required staff (demand + venue floor) vs
    rostered staff actually working that hour; a day is SHORT if ANY hour is
    short, and the reported hour is the one with the largest gap.

    With venue_config the floor is the area minimums of the trading day the
    hour belongs to (min_staff_for), in place of min_staff_by_role, and only
    hours inside trading hours count: trading_hours
    (services.trading_hours.venue_trading_hours) when given, else the hours
    the venue recorded.
    """
    from rosteriq.services.trading_hours import trading_day_of
    from rosteriq.services.venue_areas import min_staff_for
    if trading_hours is None:
        trading_hours = getattr(venue_config, "trading_hours", None) or {}
    floors: dict = {}

    def _floor(day):
        if venue_config is None:
            return min_staff_by_role
        if day not in floors:
            floors[day] = min_staff_for(venue_config, day)
        return floors[day]

    cells = _rostered_coverage_by_cell(roster)
    gaps = []
    fully_covered = True
    by_day_forecasts: dict = {}
    for f in weekly_forecasts:
        by_day_forecasts.setdefault(f.date, []).append(f)

    for day, day_fcs in sorted(by_day_forecasts.items()):
        required_by_hour = {}
        for f in day_fcs:
            trading_day = trading_day_of(trading_hours, f.date, f.hour)
            if trading_day is not None:
                required_by_hour.update(
                    calculate_required_staff([f], covers_per_staff, _floor(trading_day)))
        if not required_by_hour:
            continue
        # Worst hour = largest shortfall; tie-break to the busiest hour so the
        # reported hour is the most meaningful one.
        worst = None
        for hour in sorted(required_by_hour):
            required = required_by_hour[hour]
            rostered = cells.get((day, hour), 0)
            gap = required - rostered
            key = (gap, required)
            if worst is None or key > worst["_key"]:
                worst = {"_key": key, "hour": hour, "required": required,
                         "rostered": rostered, "gap": gap}
        if worst is None:
            continue
        if worst["gap"] > 0:
            fully_covered = False
        gaps.append({
            "date": day.isoformat(),
            "day": day.strftime("%A"),
            "peak_hour": f"{worst['hour']:02d}:00",
            "required": worst["required"],
            "rostered": worst["rostered"],
            "gap": max(0, worst["gap"]),
            "status": "short" if worst["gap"] > 0 else "covered",
        })

    shortfalls = [g for g in gaps if g["gap"] > 0]
    return {
        "fully_covered": fully_covered,
        "shortfall_count": len(shortfalls),
        "total_missing_staff": sum(g["gap"] for g in shortfalls),
        "days": gaps,
    }


def generate_weekly_roster(
    week_start: date,
    weekly_forecasts: list[DemandForecast],
    employees: list[Employee],
    venue_config: VenueConfig,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
    existing_shifts: Optional[list[Shift]] = None,
    approved_leave: Optional[dict] = None,
    prior_week_shifts: Optional[list[Shift]] = None,
) -> Roster:
    """
    Generate a full weekly roster (see the module docstring).

    Args:
        week_start: Monday of the roster week.
        weekly_forecasts: Forecasts for all 7 days.
        employees: Full employee pool.
        venue_config: Venue configuration.
        covers_per_staff: Covers-to-staff ratio.
        existing_shifts: Shifts the people already have elsewhere (other
            venues), under this venue's employee ids: never double-booked,
            and they count toward hours, rest, days and visa caps.
        approved_leave: {employee_id: [(first_day, last_day)]} of approved
            leave; nobody is rostered inside it.
        prior_week_shifts: The previous week's roster, for rest across the
            week boundary and recorded visa fortnight caps. None = unknown.

    Returns:
        Complete Roster object with all shifts and total cost.
    """
    state = venue_config.state
    week_end = week_start + timedelta(days=6)
    builder = _RosterBuilder(employees, venue_config, state, covers_per_staff,
                             weekly_forecasts, week_start,
                             existing_shifts=existing_shifts,
                             approved_leave=approved_leave,
                             prior_week_shifts=prior_week_shifts)
    plans = [p for p in (builder.plan_day(week_start + timedelta(days=i)) for i in range(7)) if p]
    builder.staff(plans)
    all_shifts = sorted(builder.generated, key=_shift_order)

    # Surface a trading day that produced no shifts (e.g. everyone hit the
    # consecutive-days cap) — otherwise a manager publishes an empty day
    # unaware. The conflict detector also flags the resulting understaffing.
    rostered_days = {s.date for s in all_shifts}
    for plan in plans:
        if plan.day not in rostered_days and plan.load > 0:
            logger.warning(
                "Roster: %s trades but no staff could be assigned "
                "(likely consecutive-days/hours limits with a thin pool).",
                plan.day.isoformat(),
            )

    # Per-shift display costs (the per-shift breakdown is overtime-unaware).
    emp_dict = {e.id: e for e in employees}
    for shift in all_shifts:
        if shift.employee_id in emp_dict:
            breakdown = calculate_shift_cost_breakdown(
                emp_dict[shift.employee_id], shift, state
            )
            shift.cost = breakdown.total_cost
            shift.penalty_multiplier = float(
                get_penalty_multiplier(
                    emp_dict[shift.employee_id].employment_type,
                    get_day_type(shift.date, state),
                )
            )

    roster = Roster(
        id=uuid.uuid4().hex,
        venue_id=venue_config.id,
        week_start=week_start,
        week_end=week_end,
        shifts=all_shifts,
        total_cost=Decimal("0"),
        created_at=datetime.now(),
    )
    # Roster-level total MUST include weekly overtime — summing per-shift breakdowns
    # under-costs any week where an employee exceeds 38h (OT is a weekly concept).
    roster.total_cost = calculate_roster_cost(roster, emp_dict, state)
    return roster


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

    # Headline total includes weekly overtime (cost_by_day stays a per-day,
    # overtime-unaware breakdown — OT is a weekly concept and can't be attributed
    # to a single day, so total_cost >= sum(cost_by_day) when OT is present).
    accurate_total = calculate_roster_cost(roster, employees, state)
    avg_cost = (accurate_total / Decimal(str(total_hours))).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    ) if total_hours > 0 else Decimal("0")

    return {
        "total_cost": accurate_total,
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
        # Flag long premium-rate shifts (Sunday and public-holiday work).
        # Casual Sunday is 1.75x and public holidays are 2.5x; the previous
        # 2.0 threshold only ever caught public holidays, so premium Sunday
        # shifts were silently never surfaced.
        if mult >= 1.75 and shift.net_hours > 6:
            suggestions.append(
                f"Shift {shift.id} on {shift.date}: {emp.name} ({emp.employment_type.value}) "
                f"at {mult}x rate for {shift.net_hours:.1f}h — consider splitting or swapping"
            )

    return suggestions


# ============================================================================
# Budget reconciliation
# ============================================================================

def apply_budget_cap(
    roster: Roster,
    employees: list[Employee],
    state: State,
    weekly_budget: Decimal,
    min_staff_by_role: dict = None,
    venue=None,
) -> dict:
    """Trim a generated roster to a weekly labour budget — Tanda-style
    "auto-adjust to budget", with one integrity rule theirs doesn't state:
    NEVER cut below the venue's minimum coverage. If the budget can't be met
    without breaking coverage, the roster stops at the coverage floor and the
    report says so plainly, instead of silently rostering an unsafe service.

    Cuts remove whole shifts, most expensive first (biggest savings, fewest
    people affected). A shift is removable only if, for every hour it spans
    through to close (past midnight included), the remaining same-role shifts
    still meet the venue's minimum for that role — the weekday minimums of
    the shift's own trading day when `venue` is given, else min_staff_by_role.
    Mutates roster.shifts / roster.total_cost in place.

    Returns a report dict:
        {"cap", "initial_cost", "final_cost", "met", "removed": [...], "note"}
    """
    min_staff_by_role = min_staff_by_role or {}
    emp_dict = {e.id: e for e in employees}
    initial_cost = roster.total_cost if roster.total_cost is not None else \
        calculate_roster_cost(roster, emp_dict, state)
    weekly_budget = Decimal(str(weekly_budget))

    report = {
        "cap": float(weekly_budget),
        "initial_cost": float(initial_cost),
        "final_cost": float(initial_cost),
        "met": initial_cost <= weekly_budget,
        "removed": [],
        "note": "",
    }
    if report["met"]:
        report["note"] = "Already within budget — nothing was cut."
        return report

    floors = {normalise_area(r): int(n or 0) for r, n in min_staff_by_role.items()}

    def _covering(shifts):
        """(area, date, hour) -> count of shifts of that area on in that clock hour."""
        cover = defaultdict(int)
        for s in shifts:
            area = normalise_area(s.role)
            for day, hour in s.clock_hours():
                cover[(area, day, hour)] += 1
        return cover

    def _removable(shift, cover):
        area = normalise_area(shift.role)
        if venue is not None:
            floor = _areas.min_staff_for(venue, shift.date).get(area, 0)
        else:
            floor = floors.get(area, 0)
        if floor <= 0:
            return True
        return all(cover[(area, day, hour)] - 1 >= floor
                   for day, hour in shift.clock_hours())

    current_cost = initial_cost
    while current_cost > weekly_budget:
        cover = _covering(roster.shifts)
        candidates = [s for s in roster.shifts if _removable(s, cover)]
        if not candidates:
            break
        # most expensive first: fastest route under the cap
        victim = max(candidates, key=lambda s: s.cost or Decimal("0"))
        roster.shifts = [s for s in roster.shifts if s.id != victim.id]
        emp = emp_dict.get(victim.employee_id)
        report["removed"].append({
            "shift_id": victim.id,
            "employee": emp.name if emp else victim.employee_id,
            "date": victim.date.isoformat(),
            "role": victim.role,
            "cost": float(victim.cost or 0),
        })
        # weekly OT means cost must be recomputed, not decremented
        current_cost = calculate_roster_cost(roster, emp_dict, state)

    roster.total_cost = current_cost
    report["final_cost"] = float(current_cost)
    report["met"] = current_cost <= weekly_budget
    if report["met"]:
        report["note"] = (f"Cut {len(report['removed'])} shift(s) to come in "
                          f"under ${float(weekly_budget):,.0f}.")
    else:
        report["note"] = (f"Could not reach ${float(weekly_budget):,.0f} without "
                          "dropping below your minimum coverage — stopped at the "
                          f"coverage floor (${float(current_cost):,.0f}). Raise the "
                          "budget or lower min staff in venue settings.")
    return report
