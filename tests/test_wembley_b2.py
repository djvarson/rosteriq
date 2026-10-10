"""
The Wembley pilot: a generated week Ben would call "about right".

Pins (audit findings 7, 8, 93, 101):
* every area holds its weekday minimum for every trading hour, open to close,
  past midnight on Friday and Saturday, and on a brand-new venue with only
  cold-start forecasts — nobody leaves the pub empty at close; cold-start
  forecasts follow the venue's recorded hours (10:00-24:00 when none)
* supervisory areas only take people with the skill; staff with no skills
  recorded work general areas; busy hours add staff to the floor, bars and
  kitchen, never extra duty managers or bottle-shop staff unless the venue
  says so
* shifts are realistic blocks (openers, closers, lunch and dinner), not 2-3
  hour fragments, with the unpaid meal break from the break table
* the cheapest person by recorded rate x day multiplier gets the shift, so
  junior runners fill floor slots first; salaried managers are already paid
  and are rostered to their contracted hours
* inactive staff, approved leave, other-venue shifts, the previous week's
  late close and recorded visa fortnight caps all hold inside the generator
* the budget trim keeps each day's area minimums; the bulk route generates
"""

import statistics
import time as _time
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.award_rules import (
    MAX_SHIFT_LENGTH_HOURS, get_minimum_break_minutes, get_minimum_engagement_hours,
)
from rosteriq.database import get_db
from rosteriq.models import (
    AwardLevel, DemandForecast, Employee, EmploymentType, Roster, Shift,
    ShiftStatus, State, VenueConfig,
)
from rosteriq.roster_optimiser import (
    apply_budget_cap, generate_daily_roster, generate_weekly_roster,
)
from rosteriq.services import venue_areas
from rosteriq.services.conflict_detector import ConflictDetector, ConflictType


WEEK = date(2026, 10, 19)            # Mon; no WA public holiday that week
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
PW = "Passw0rd!234"

# Synthetic venue-recorded rates (not award values): juniors at the rate the
# venue recorded for them.
ADULT = Decimal("26.00")
JUNIOR = Decimal("20.80")

AREAS = [
    {"name": "duty manager", "label": "Duty Manager", "min_by_day": {d: 1 for d in DAYS}},
    {"name": "restaurant", "min_by_day": dict(zip(DAYS, (2, 2, 2, 3, 3, 4, 3)))},
    {"name": "front bar", "min_by_day": dict(zip(DAYS, (2, 2, 2, 2, 3, 3, 2)))},
    {"name": "sports bar", "min_by_day": dict(zip(DAYS, (1, 1, 2, 2, 3, 3, 2)))},
    {"name": "bottle shop", "min_by_day": {d: 1 for d in DAYS}},
    {"name": "kitchen", "min_by_day": dict(zip(DAYS, (2, 2, 2, 3, 3, 3, 3)))},
    {"name": "functions", "min_by_day": dict(zip(DAYS, (0, 0, 0, 0, 0, 2, 0)))},
]
HOURS = {d: {"open": "10:00", "close": "00:00"} for d in DAYS}
HOURS.update({"fri": {"open": "10:00", "close": "01:00"},
              "sat": {"open": "10:00", "close": "01:00"},
              "sun": {"open": "10:00", "close": "22:00"}})


def _venue(vid="wem-b2", areas=AREAS, trading_hours=HOURS, min_staff=None):
    return VenueConfig(id=vid, name="The Wembley Hotel", tanda_org_id="", state=State.wa,
                       timezone="Australia/Perth", min_staff=min_staff or {},
                       areas=areas, trading_hours=trading_hours,
                       max_labour_pct=30, created_at=datetime(2026, 7, 1))


def _set(emp, **fields):
    """Set fields other wave-B branches add to Employee (active, pay_basis,
    weekly_salary) whether or not this model has them yet."""
    for k, v in fields.items():
        if k in type(emp).model_fields:
            setattr(emp, k, v)
        else:
            object.__setattr__(emp, k, v)
    return emp


def _emp(eid, name, etype, rate, skills, venue_id="wem-b2", max_h=38.0, **extra):
    return Employee(id=eid, venue_id=venue_id, name=name, employment_type=etype,
                    award_level=AwardLevel.level_2, state=State.wa,
                    hourly_base_rate=Decimal(str(rate)), skills=skills,
                    max_hours_per_week=max_h, created_at=datetime(2026, 7, 1),
                    updated_at=datetime(2026, 7, 1), **extra)


def wembley_staff(venue_id="wem-b2"):
    """60 people: 9 managers (two salaried), 3 supervisors, 10 kitchen,
    14 adult bar casuals, 20 junior floor runners, 4 fresh hires with no
    skills recorded yet."""
    staff, n = [], [0]
    tags = {}

    def add(name, etype, rate, skills, tag, **kw):
        n[0] += 1
        e = _emp(f"{venue_id}-{n[0]:03d}", name, etype, rate, skills, venue_id=venue_id, **kw)
        staff.append(e)
        tags[e.id] = tag
        return e

    ft, cas, pt = EmploymentType.full_time, EmploymentType.casual, EmploymentType.part_time
    tom = add("Tom Hartley", ft, "38.00", ["duty manager", "restaurant", "front bar"], "salaried", max_h=45.0)
    liam = add("Liam Okafor", ft, "34.00", ["duty manager", "restaurant", "functions"], "salaried", max_h=45.0)
    _set(tom, pay_basis="salaried", weekly_salary=Decimal("1710.00"))
    _set(liam, pay_basis="salaried", weekly_salary=Decimal("1530.00"))
    add("Ava Brennan", ft, "34.00", ["duty manager", "front bar", "sports bar"], "mgr")
    add("Karis Doyle", ft, "31.00", ["functions", "sports bar"], "mgr")
    add("Jake Morrow", ft, "34.00", ["front bar", "sports bar", "bottle shop", "duty manager"], "mgr")
    add("Nina Patel", ft, "33.00", ["duty manager", "restaurant", "front bar"], "mgr")
    add("Sam Whitlock", cas, "33.00", ["duty manager", "restaurant", "front bar"], "mgr", max_h=35.0)
    add("Meca Ruiz", cas, "33.00", ["duty manager", "front bar"], "mgr", max_h=20.0)
    wed_sun = {d: [{"start": "00:00", "end": "23:59"}]
               for d in ("wednesday", "thursday", "friday", "saturday", "sunday")}
    wed_sun.update({"monday": [], "tuesday": []})
    add("Rhys Kemp", ft, "33.00", ["restaurant", "duty manager"], "mgr", availability=wed_sun)
    for nm in ("Chloe Vance", "Marcus Lee", "Priya Shah"):
        add(nm, cas, "28.00", ["front bar", "sports bar", "restaurant"], "sup")
    add("Dan Fischer", ft, "33.00", ["kitchen"], "kitchen")
    add("Elle Moreau", ft, "31.00", ["kitchen"], "kitchen")
    add("Hamish Grant", pt, "31.00", ["kitchen"], "kitchen", max_h=30.0)
    for nm in ("Kai Tanaka", "Lucy Ford", "Omar Haddad", "Zoe Burke"):
        add(nm, cas, "28.00", ["kitchen"], "kitchen")
    for nm in ("Ben Ng", "Tess Ali", "Jordan Pike"):
        add(nm, cas, "25.00", ["kitchen"], "kitchen")
    for i, f in enumerate(("Ella", "Noah", "Mia", "Leo", "Isla", "Max", "Ruby",
                           "Finn", "Grace", "Oscar", "Lily", "Jack", "Ivy", "Hugo")):
        skills = ["front bar", "sports bar"] + (["bottle shop"] if i % 3 == 0 else [])
        add(f"{f} Adult", cas, ADULT, skills, "adult")
    for f in ("Amy", "Ben", "Cody", "Dana", "Evan", "Faye", "Gus", "Hana", "Ian", "Jade",
              "Kobe", "Lara", "Milo", "Nell", "Owen", "Pia", "Quinn", "Remy", "Sian", "Theo"):
        add(f"{f} Junior", cas, JUNIOR, ["restaurant", "functions"], "junior", max_h=30.0)
    for i in range(4):
        add(f"Fresh Hire{i}", cas, JUNIOR, [], "noskill")
    return staff, tags


def day_covers(dow, afl_night=False):
    """Wembley-shaped venue-wide covers by hour, 10:00-23:00."""
    base = (25, 25, 50, 70, 140, 170, 130)[dow]
    out = {}
    for h in range(10, 24):
        m = 1.4 if 12 <= h <= 14 else 1.8 if 17 <= h <= 21 else 0.8 if h >= 22 else 0.7
        v = base * m * (2.2 if afl_night and 17 <= h <= 22 else 1.0)
        out[h] = round(v, 1)
    return out


def wembley_forecasts(week=WEEK, venue_id="wem-b2", model_version="wembley-test"):
    rows = []
    for d in range(7):
        day = week + timedelta(days=d)
        for h, covers in day_covers(d, afl_night=(d == 3)).items():
            rows.append(DemandForecast(id=f"{venue_id}-{day}-{h}", venue_id=venue_id, date=day,
                                       hour=h, predicted_covers=covers, confidence=0.7,
                                       model_version=model_version))
    return rows


def _span(shift):
    start = datetime.combine(shift.date, shift.start_time)
    return start, start + timedelta(hours=shift.duration_hours)


def area_shortfalls(roster, venue, days=None):
    """[(day, hour, area, on, minimum)] for every trading hour below minimum."""
    short = []
    for d in range(7):
        day = roster.week_start + timedelta(days=d)
        if days is not None and day not in days:
            continue
        opens, closes = venue_areas.trading_window(venue, day)
        cur = opens
        while cur < closes:
            for area, minimum in venue_areas.min_staff_for(venue, day).items():
                on = 0
                for s in roster.shifts:
                    if s.role != area:
                        continue
                    start, end = _span(s)
                    if start <= cur and end >= min(cur + timedelta(hours=1), closes):
                        on += 1
                if on < minimum:
                    short.append((day, cur.strftime("%a %H:%M"), area, on, minimum))
            cur += timedelta(hours=1)
    return short


@pytest.fixture(scope="module")
def wembley_week():
    venue = _venue()
    staff, tags = wembley_staff()
    t0 = _time.perf_counter()
    roster = generate_weekly_roster(WEEK, wembley_forecasts(), staff, venue)
    elapsed = _time.perf_counter() - t0
    return venue, staff, tags, roster, elapsed


# ---------------------------------------------------------------------------
# finding 7: every area, every trading hour, open to close
# ---------------------------------------------------------------------------

def test_every_area_holds_its_minimum_every_trading_hour(wembley_week):
    venue, staff, tags, roster, elapsed = wembley_week
    assert roster.shifts
    assert area_shortfalls(roster, venue) == []
    # the late close is staffed by the people closing, not left empty
    fri = WEEK + timedelta(days=4)
    closers = [s for s in roster.shifts if s.date == fri and _span(s)[1] == datetime.combine(
        fri + timedelta(days=1), time(1, 0))]
    assert {s.role for s in closers} >= {"duty manager", "restaurant", "front bar",
                                         "sports bar", "bottle shop", "kitchen"}
    assert elapsed < 5.0, f"generation took {elapsed:.1f}s"


def test_cold_start_venue_is_staffed_until_midnight():
    """A brand-new venue with no recorded hours and only cold-start forecasts
    trades 10:00-24:00 and every area is staffed to midnight, every day."""
    from rosteriq.ensemble import EnsembleForecaster
    minimums = {"restaurant": 2, "duty manager": 1, "front bar": 2, "sports bar": 1,
                "bottle shop": 1, "kitchen": 2, "functions": 0}
    venue = _venue(vid="wem-cold", areas=[], trading_hours={}, min_staff=minimums)
    staff, _ = wembley_staff("wem-cold")
    forecasts = EnsembleForecaster(venue).predict_week(WEEK)
    assert {f.model_version for f in forecasts} == {"cold_start_v1"}
    assert {f.hour for f in forecasts} == set(range(10, 24))
    roster = generate_weekly_roster(WEEK, forecasts, staff, venue)
    for d in range(7):
        day = WEEK + timedelta(days=d)
        ends = {}
        for s in (x for x in roster.shifts if x.date == day):
            start, end = _span(s)
            assert start.time() >= time(10, 0), s
            ends[s.role] = max(ends.get(s.role, start), end)
        for area, minimum in minimums.items():
            if minimum:
                assert ends.get(area) == datetime.combine(day + timedelta(days=1), time(0)), (day, area)
    cells = defaultdict(int)
    for s in roster.shifts:
        for cell in s.clock_hours():
            cells[(s.role, cell)] += 1
    for d in range(7):
        day = WEEK + timedelta(days=d)
        for h in range(10, 24):
            for area, minimum in minimums.items():
                assert cells[(area, (day, h))] >= minimum, (day, h, area)


def test_cold_start_forecasts_follow_recorded_trading_hours():
    from rosteriq.ensemble import EnsembleForecaster
    by_day = defaultdict(list)
    for f in EnsembleForecaster(_venue()).predict_week(WEEK):
        by_day[f.date.weekday()].append(f.hour)
    assert by_day[0] == list(range(10, 24))                 # Monday: Sunday shut at 22:00
    assert by_day[5] == [0] + list(range(10, 24))           # Saturday opens with Friday's 01:00 close
    assert by_day[6] == [0] + list(range(10, 22))           # Sunday: Saturday's close, shut at 22:00


# ---------------------------------------------------------------------------
# finding 8: skills decide who works where; extra demand goes to the floor
# ---------------------------------------------------------------------------

def test_supervisory_areas_take_only_people_with_the_skill(wembley_week):
    venue, staff, tags, roster, _ = wembley_week
    by_id = {e.id: e for e in staff}
    for s in roster.shifts:
        emp = by_id[s.employee_id]
        if s.role == "duty manager":
            assert "duty manager" in emp.skills, (emp.name, s.date)
        assert s.role in emp.skills, (emp.name, s.role, emp.skills)
    assert not [s for s in roster.shifts if tags[s.employee_id] == "noskill"], \
        "fresh hires took shifts trained staff were free for"


def test_staff_with_no_skills_fill_general_areas_only_when_needed():
    venue = _venue(vid="wem-gen", areas=[
        {"name": "duty manager", "min_by_day": {d: 1 for d in DAYS}},
        {"name": "restaurant", "min_by_day": {d: 2 for d in DAYS}},
    ], trading_hours={"mon": {"open": "11:00", "close": "21:00"}})
    dm = _emp("g-dm", "Duty", EmploymentType.full_time, "34.00", ["duty manager"], venue_id="wem-gen")
    runner = _emp("g-run", "Runner", EmploymentType.casual, "26.00", ["restaurant"], venue_id="wem-gen")
    fresh = [_emp(f"g-new{i}", f"New {i}", EmploymentType.casual, "20.80", [], venue_id="wem-gen")
             for i in range(3)]
    shifts = generate_daily_roster(WEEK, [], [dm, runner] + fresh, State.wa, venue_config=venue)
    roles = {s.employee_id: s.role for s in shifts}
    assert roles.get("g-dm") == "duty manager"
    assert roles.get("g-run") == "restaurant"
    assert [r for e, r in roles.items() if e.startswith("g-new")] == ["restaurant"]

    nobody_skilled = generate_daily_roster(WEEK, [], fresh, State.wa, venue_config=venue)
    assert "duty manager" not in {s.role for s in nobody_skilled}

    shift = Shift(id="x", employee_id="g-new0", date=WEEK, start_time=time(11), end_time=time(17),
                  break_minutes=30, status=ShiftStatus.scheduled, role="Duty Manager")
    roster = Roster(id="r", venue_id="wem-gen", week_start=WEEK, week_end=WEEK + timedelta(days=6),
                    shifts=[shift], created_at=datetime(2026, 10, 1))
    conflicts = ConflictDetector().detect_conflicts(roster, venue, fresh)
    assert [c.conflict_type for c in conflicts].count(ConflictType.SKILL_MISMATCH) == 1


def test_busy_hours_add_floor_staff_not_duty_managers_or_bottle_shop(wembley_week):
    venue, staff, tags, roster, _ = wembley_week
    cells = defaultdict(int)
    for s in roster.shifts:
        for cell in s.clock_hours():
            cells[(s.role, cell)] += 1
    for (role, cell), on in cells.items():
        if role in ("duty manager", "bottle shop"):
            assert on == 1, (role, cell, on)
    thu_dinner = (WEEK + timedelta(days=3), 19)      # AFL night
    floor = sum(cells[(a, thu_dinner)] for a in ("restaurant", "front bar", "sports bar", "kitchen"))
    minimum = sum(venue_areas.min_staff_for(venue, thu_dinner[0])[a]
                  for a in ("restaurant", "front bar", "sports bar", "kitchen"))
    assert floor > minimum, (floor, minimum)


def test_a_venue_can_let_the_bottle_shop_follow_demand():
    staff = [_emp(f"f{i}", f"F{i}", EmploymentType.casual, "26.00", ["front bar", "bottle shop"],
                  venue_id="wem-flex") for i in range(10)]
    fcs = [DemandForecast(id=f"f{h}", venue_id="wem-flex", date=WEEK, hour=h, predicted_covers=90.0,
                          confidence=0.7, model_version="t") for h in range(16, 22)]

    def bottle_shop_peak(follows):
        area = {"name": "bottle shop", "min_by_day": {d: 1 for d in DAYS}}
        if follows is not None:
            area["follows_demand"] = follows
        venue = _venue(vid="wem-flex", areas=[
            {"name": "front bar", "min_by_day": {d: 1 for d in DAYS}}, area],
            trading_hours={"mon": {"open": "16:00", "close": "22:00"}})
        shifts = generate_daily_roster(WEEK, fcs, staff, State.wa, venue_config=venue)
        return max(sum(1 for s in shifts if s.role == "bottle shop" and (WEEK, h) in s.clock_hours())
                   for h in range(16, 22))

    assert bottle_shop_peak(None) == 1                 # holds its minimum by default
    assert bottle_shop_peak(True) == 3                 # 6 staff: 1 + 1 minimums, 4 extra shared


# ---------------------------------------------------------------------------
# finding 93: realistic blocks, not fragments
# ---------------------------------------------------------------------------

def test_shifts_are_realistic_blocks(wembley_week):
    venue, staff, tags, roster, _ = wembley_week
    by_id = {e.id: e for e in staff}
    lengths = [s.duration_hours for s in roster.shifts]
    assert statistics.median(lengths) >= 4
    assert min(lengths) >= 4, sorted(lengths)[:5]
    for s in roster.shifts:
        assert s.duration_hours <= MAX_SHIFT_LENGTH_HOURS
        assert s.break_minutes == get_minimum_break_minutes(s.duration_hours)
        assert s.net_hours >= get_minimum_engagement_hours(by_id[s.employee_id].employment_type)
    per_day = defaultdict(list)
    for s in roster.shifts:
        per_day[(s.employee_id, s.date)].append(s)
    assert all(len(v) == 1 for v in per_day.values())
    # a quiet Monday is an opener and a closer per head, not a string of fragments
    mon = [s for s in roster.shifts if s.date == WEEK]
    assert {(s.start_time, s.end_time) for s in mon} == {(time(10), time(17)), (time(17), time(0))}
    ids = [s.id for s in roster.shifts]
    assert len(set(ids)) == len(ids) and min(len(i) for i in ids) >= 16


def test_a_peak_next_to_a_shift_extends_it():
    """A two-hour afternoon bump right before the closer starts brings the
    closer in early instead of calling in a fourth-hour casual."""
    venue = _venue(vid="wem-ext", areas=[{"name": "front bar", "min_by_day": {d: 1 for d in DAYS}}],
                   trading_hours={"mon": {"open": "10:00", "close": "00:00"}})
    covers = {h: 15.0 for h in range(10, 24)}
    covers.update({15: 30.0, 16: 30.0})
    fcs = [DemandForecast(id=f"e{h}", venue_id="wem-ext", date=WEEK, hour=h, predicted_covers=c,
                          confidence=0.7, model_version="t") for h, c in covers.items()]
    staff = [_emp(f"x{i}", f"X{i}", EmploymentType.casual, "26.00", ["front bar"], venue_id="wem-ext")
             for i in range(6)]
    shifts = generate_daily_roster(WEEK, fcs, staff, State.wa, venue_config=venue)
    assert sorted((s.start_time, s.end_time) for s in shifts) == [(time(10), time(17)), (time(15), time(0))]


# ---------------------------------------------------------------------------
# finding 101: cheapest by recorded rate x day multiplier
# ---------------------------------------------------------------------------

def test_the_cheaper_recorded_rate_gets_the_shift():
    venue = _venue(vid="wem-rate", areas=[{"name": "front bar", "min_by_day": {d: 1 for d in DAYS}}],
                   trading_hours={"mon": {"open": "11:00", "close": "19:00"}})
    pricey = _emp("r-vm", "Venue Manager", EmploymentType.full_time, "38.00", ["front bar"], venue_id="wem-rate")
    casual = _emp("r-cas", "Casual", EmploymentType.casual, "26.00", ["front bar"], venue_id="wem-rate")
    shifts = generate_daily_roster(WEEK, [], [pricey, casual], State.wa, venue_config=venue)
    assert [s.employee_id for s in shifts] == ["r-cas"]


def test_juniors_take_floor_slots_before_adults(wembley_week):
    venue, staff, tags, roster, _ = wembley_week
    restaurant = [tags[s.employee_id] for s in roster.shifts if s.role == "restaurant"]
    assert restaurant and set(restaurant) <= {"junior", "salaried"}, restaurant
    assert restaurant.count("junior") >= len(restaurant) - 2

    venue2 = _venue(vid="wem-jr", areas=[{"name": "restaurant", "min_by_day": {d: 3 for d in DAYS}}],
                    trading_hours={"sat": {"open": "11:00", "close": "21:00"}})
    adults = [_emp(f"a{i}", f"Adult {i}", EmploymentType.casual, "26.00", ["restaurant"], venue_id="wem-jr")
              for i in range(3)]
    juniors = [_emp(f"j{i}", f"Junior {i}", EmploymentType.casual, "20.80", ["restaurant"], venue_id="wem-jr")
               for i in range(3)]
    sat = WEEK + timedelta(days=5)
    shifts = generate_daily_roster(sat, [], adults + juniors, State.wa, venue_config=venue2)
    assert sorted(s.employee_id for s in shifts) == ["j0", "j1", "j2"]


def test_salaried_managers_are_rostered_to_their_contract(wembley_week):
    venue, staff, tags, roster, _ = wembley_week
    hours = defaultdict(float)
    for s in roster.shifts:
        hours[s.employee_id] += s.net_hours
    for e in staff:
        if tags[e.id] == "salaried":
            assert e.max_hours_per_week - 10 <= hours[e.id] <= e.max_hours_per_week, (e.name, hours[e.id])
            dm = [s for s in roster.shifts if s.employee_id == e.id and s.role == "duty manager"]
            assert len(dm) >= 4, (e.name, len(dm))


# ---------------------------------------------------------------------------
# people constraints inside the generator
# ---------------------------------------------------------------------------

def _bar_venue(vid):
    return _venue(vid=vid, areas=[{"name": "front bar", "min_by_day": {d: 1 for d in DAYS}}],
                  trading_hours={d: {"open": "10:00", "close": "18:00"} for d in DAYS})


def test_inactive_staff_and_approved_leave_are_never_rostered():
    venue = _bar_venue("wem-off")
    left = _set(_emp("o-left", "Left", EmploymentType.casual, "20.00", ["front bar"], venue_id="wem-off"),
                active=False)
    away = _emp("o-away", "Away", EmploymentType.casual, "21.00", ["front bar"], venue_id="wem-off")
    rest = [_emp(f"o-{i}", f"P{i}", EmploymentType.casual, "26.00", ["front bar"], venue_id="wem-off")
            for i in range(3)]
    leave = {"o-away": [(WEEK, WEEK + timedelta(days=2))]}
    roster = generate_weekly_roster(WEEK, [], [left, away] + rest, venue, approved_leave=leave)
    assert not [s for s in roster.shifts if s.employee_id == "o-left"]
    away_days = {s.date for s in roster.shifts if s.employee_id == "o-away"}
    assert away_days and min(away_days) > WEEK + timedelta(days=2)
    assert area_shortfalls(roster, venue) == []


def test_other_venue_shifts_last_weeks_close_and_visa_caps_hold():
    venue = _bar_venue("wem-ppl")
    cheap = _emp("p-cheap", "Cheap", EmploymentType.casual, "20.00", ["front bar"], venue_id="wem-ppl",
                 visa_work_limit_fortnight=24.0)
    others = [_emp(f"p-{i}", f"P{i}", EmploymentType.casual, "26.00", ["front bar"], venue_id="wem-ppl")
              for i in range(4)]
    sunday_close = Shift(id="prev-sun", employee_id="p-cheap", date=WEEK - timedelta(days=1),
                         start_time=time(17), end_time=time(1), break_minutes=30,
                         status=ShiftStatus.scheduled, role="front bar")
    elsewhere = Shift(id="apothecary-tue", employee_id="p-cheap", date=WEEK + timedelta(days=1),
                      start_time=time(18), end_time=time(23), break_minutes=0,
                      status=ShiftStatus.scheduled, role="bar")
    roster = generate_weekly_roster(WEEK, [], [cheap] + others, venue,
                                    existing_shifts=[elsewhere], prior_week_shifts=[sunday_close])
    mine = [s for s in roster.shifts if s.employee_id == "p-cheap"]
    days = {s.date for s in mine}
    assert WEEK not in days                         # 01:00 close, no 10:00 open next morning
    assert WEEK + timedelta(days=1) not in days     # working at the other venue
    gross = sum(s.duration_hours for s in mine)
    assert 0 < gross <= 24 - sunday_close.duration_hours
    assert area_shortfalls(roster, venue) == []

    unknown = generate_weekly_roster(WEEK, [], [cheap] + others, venue)
    assert sum(s.duration_hours for s in unknown.shifts if s.employee_id == "p-cheap") <= 12


# ---------------------------------------------------------------------------
# the routes: leave inside generation, budget trim by day, bulk generation
# ---------------------------------------------------------------------------

def _owner_client():
    c = TestClient(app)
    email = f"wb2_{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "Ben"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _api_bar_venue(c, h):
    vid = f"wb2-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={
        "id": vid, "name": "The Wembley", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "timezone": "Australia/Perth", "min_staff": {},
        "areas": [{"name": "front bar", "min_by_day": {d: 2 for d in DAYS}}],
        "trading_hours": {d: {"open": "11:00", "close": "21:00"} for d in DAYS},
        "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text
    db = get_db()
    for i in range(6):
        db.save_employee(_emp(f"{vid}-e{i}", f"P{i}", EmploymentType.casual, f"{24 + i}.00",
                              ["front bar"], venue_id=vid))
    db.add_forecasts([f for f in wembley_forecasts(venue_id=vid)])
    return vid


def test_generate_route_rosters_around_approved_leave():
    c, h = _owner_client()
    vid = _api_bar_venue(c, h)
    get_db().save_leave_request({
        "id": f"{vid}-lv", "venue_id": vid, "employee_id": f"{vid}-e0",
        "start_date": WEEK, "end_date": WEEK + timedelta(days=6), "reason": "Trip",
        "status": "approved", "created_at": datetime(2026, 10, 1)})
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": WEEK.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    shifts = r.json()["shifts"]
    assert not [s for s in shifts if s["employee_id"] == f"{vid}-e0"]
    venue = get_db().get_venue(vid)
    roster = Roster(id="chk", venue_id=vid, week_start=WEEK, week_end=WEEK + timedelta(days=6),
                    shifts=[Shift(**s) for s in shifts], created_at=datetime(2026, 10, 1))
    assert area_shortfalls(roster, venue) == [], "leave left holes in the roster"


def test_budget_trim_keeps_each_days_area_minimums():
    venue = _venue()
    staff, _ = wembley_staff()
    roster = generate_weekly_roster(WEEK, wembley_forecasts(), staff, venue)
    report = apply_budget_cap(roster, staff, State.wa, Decimal("1000"),
                              min_staff_by_role=venue.min_staff, venue=venue)
    assert report["met"] is False and report["removed"]
    assert area_shortfalls(roster, venue) == []


def test_bulk_generate_route_generates():
    c, h = _owner_client()
    vid = _api_bar_venue(c, h)
    r = c.post("/api/bulk/generate-rosters", json={"venue_ids": [vid], "week_start": WEEK.isoformat()},
               headers=h)
    assert r.status_code == 200, r.text
    result = r.json()["results"][0]
    assert result["status"] == "success", result
    assert result["shift_count"] > 0
