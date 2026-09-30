"""
The v2 optimiser and the auto-scheduler read Employee.availability through the
one shared rule (services/availability_rules.py):

* {}                       -> available any time
* {"monday": []}           -> unavailable Monday, AVAILABLE Tuesday (unlisted)
* ranges                   -> the whole shift must fit inside one

What used to go wrong here:
* roster_optimiser_v2.is_employee_available returned False for an unlisted
  day, so staff with {} got no MILP variables and were never rostered.
* AutoScheduler._is_employee_eligible only looked for a blocked day and
  ignored the time windows, so an evenings-only person got a lunch shift.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from rosteriq.models import (
    Employee, DemandForecast, VenueConfig, EmploymentType, AwardLevel, State,
)
from rosteriq.services import roster_optimiser_v2 as v2
from rosteriq.services.roster_optimiser_v2 import (
    is_employee_available, HybridOptimiser, MILPRosterOptimiser, SHIFT_TEMPLATES,
)
from rosteriq.services.auto_scheduler import AutoScheduler

MONDAY = date(2026, 4, 27)
TUESDAY = MONDAY + timedelta(days=1)
EVENINGS_MON = {"monday": [{"start": "17:00", "end": "23:00"}]}


def _emp(emp_id, availability, employment_type=EmploymentType.part_time):
    return Employee(
        id=emp_id,
        name=emp_id,
        employment_type=employment_type,
        award_level=AwardLevel.level_2,
        state=State.vic,
        hourly_base_rate=Decimal("25.00"),
        availability=availability,
        max_hours_per_week=38.0,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


def _venue():
    return VenueConfig(
        id="venue_adopt", name="Adopt", tanda_org_id="org", state=State.vic,
        min_staff={}, max_labour_pct=30.0, created_at=datetime.now(),
    )


def _forecasts(day, hours, covers=10.0):
    return [
        DemandForecast(
            id=f"fc_{day}_{h}", venue_id="venue_adopt", date=day, hour=h,
            predicted_covers=covers, confidence=0.9, model_version="t",
        )
        for h in hours
    ]


def test_calendar_sanity():
    assert MONDAY.weekday() == 0 and TUESDAY.weekday() == 1


# ============================================================================
# roster_optimiser_v2.is_employee_available
# ============================================================================

def test_v2_empty_availability_is_available_everywhere():
    emp = _emp("free", {})
    for offset in range(7):
        for start_h, end_h, _ in SHIFT_TEMPLATES.values():
            assert is_employee_available(emp, MONDAY + timedelta(days=offset), start_h, end_h)


def test_v2_listed_empty_day_blocks_only_that_day():
    emp = _emp("nomon", {"monday": []})
    assert not is_employee_available(emp, MONDAY, 9, 17)
    assert not is_employee_available(emp, MONDAY, 17, 23)
    # Tuesday isn't listed -> available all day (used to be False)
    assert is_employee_available(emp, TUESDAY, 9, 17)
    assert is_employee_available(emp, TUESDAY, 6, 14)


def test_v2_range_fits_or_not():
    emp = _emp("eve", EVENINGS_MON)
    assert is_employee_available(emp, MONDAY, 17, 23)       # exactly the window
    assert is_employee_available(emp, MONDAY, 18, 22)       # inside
    assert not is_employee_available(emp, MONDAY, 10, 18)   # starts before
    assert not is_employee_available(emp, MONDAY, 6, 14)    # outside entirely
    assert is_employee_available(emp, TUESDAY, 6, 14)       # Tuesday unlisted
    # Minute precision: a 17:30 start means a 17:00 shift does not fit
    late = _emp("late", {"monday": [{"start": "17:30", "end": "23:00"}]})
    assert not is_employee_available(late, MONDAY, 17, 23)
    assert is_employee_available(late, MONDAY, 18, 23)


# ============================================================================
# roster_optimiser_v2: the solvers actually roster these people
# ============================================================================

def _greedy(employees, forecasts):
    return HybridOptimiser(
        _venue(), employees, forecasts, MONDAY, MONDAY + timedelta(days=6),
    )._greedy_solve()


def test_v2_greedy_rosters_unconstrained_staff():
    roster = _greedy([_emp("free", {})], _forecasts(MONDAY, range(10, 13)))
    assert [s.date for s in roster.shifts] == [MONDAY]


def test_v2_greedy_respects_blocked_day_but_uses_next_day():
    emp = _emp("nomon", {"monday": []})
    fc = _forecasts(MONDAY, range(10, 13)) + _forecasts(TUESDAY, range(10, 13))
    roster = _greedy([emp], fc)
    assert [s.date for s in roster.shifts] == [TUESDAY]


def test_v2_greedy_only_picks_a_template_inside_the_range():
    roster = _greedy([_emp("eve", EVENINGS_MON)], _forecasts(MONDAY, range(18, 21)))
    assert len(roster.shifts) == 1
    s = roster.shifts[0]
    assert s.date == MONDAY
    assert s.start_time >= time(17, 0) and s.end_time <= time(23, 0)


def _milp(monkeypatch, employees, forecasts):
    """Build (and try to solve) the MILP. Returns (variable keys, roster).

    The decision variables are captured as the model is built, so the
    availability gate is checked even where the bundled CBC binary can't run
    (e.g. an x86 CBC on arm64 without Rosetta); roster is None then."""
    if not v2.PULP_AVAILABLE:
        pytest.skip("PuLP not installed")
    captured = {}
    real = MILPRosterOptimiser._add_coverage_constraints

    def spy(self, prob, x):
        captured.update(x)
        return real(self, prob, x)

    monkeypatch.setattr(MILPRosterOptimiser, "_add_coverage_constraints", spy)
    roster = MILPRosterOptimiser(
        _venue(), employees, forecasts, MONDAY, MONDAY + timedelta(days=6),
    ).solve(timeout_seconds=20)
    return set(captured), roster


def test_v2_milp_gives_unconstrained_staff_variables(monkeypatch):
    keys, roster = _milp(monkeypatch, [_emp("free", {})], _forecasts(MONDAY, range(10, 13)))
    # {} -> a variable for every template on every day (used to be none)
    assert keys == {("free", t, d) for t in SHIFT_TEMPLATES for d in range(7)}
    if roster is not None:
        assert roster.shifts and {s.date for s in roster.shifts} == {MONDAY}


def test_v2_milp_blocked_monday_still_rostered_tuesday(monkeypatch):
    emp = _emp("nomon", {"monday": []})
    fc = _forecasts(MONDAY, range(10, 13)) + _forecasts(TUESDAY, range(10, 13))
    keys, roster = _milp(monkeypatch, [emp], fc)
    assert {d for _, _, d in keys} == {1, 2, 3, 4, 5, 6}   # every day but Monday
    assert {t for _, t, d in keys if d == 1} == set(SHIFT_TEMPLATES)
    if roster is not None:
        assert roster.shifts and {s.date for s in roster.shifts} == {TUESDAY}


def test_v2_milp_range_bounds_the_shift(monkeypatch):
    keys, roster = _milp(monkeypatch, [_emp("eve", EVENINGS_MON)], _forecasts(MONDAY, range(18, 21)))
    monday_templates = {t for _, t, d in keys if d == 0}
    assert monday_templates == {"evening", "short_pm", "split_eve"}   # all inside 17-23
    assert {t for _, t, d in keys if d == 1} == set(SHIFT_TEMPLATES)   # Tuesday unlisted
    if roster is not None:
        assert roster.shifts
        for s in roster.shifts:
            assert s.date == MONDAY
            assert s.start_time >= time(17, 0) and s.end_time <= time(23, 0)


# ============================================================================
# auto_scheduler
# ============================================================================

def _scheduler():
    # The eligibility/assignment helpers use no DB state; skip __init__ so the
    # test doesn't depend on get_db() wiring.
    return AutoScheduler.__new__(AutoScheduler)


def _eligible(emp, day, start_h, end_h):
    return _scheduler()._is_employee_eligible(emp, day, start_h, end_h, {emp.id: 0.0}, _venue())


def test_auto_empty_availability_is_eligible():
    emp = _emp("free", {})
    for offset in range(7):
        assert _eligible(emp, MONDAY + timedelta(days=offset), 6, 23)


def test_auto_listed_empty_day_blocks_only_that_day():
    emp = _emp("nomon", {"monday": []})
    assert not _eligible(emp, MONDAY, 10, 16)
    assert _eligible(emp, TUESDAY, 10, 16)


def test_auto_range_fits_or_not():
    emp = _emp("eve", EVENINGS_MON)
    assert _eligible(emp, MONDAY, 18, 22)
    assert _eligible(emp, MONDAY, 17, 23)
    assert not _eligible(emp, MONDAY, 10, 18)   # used to be eligible
    assert not _eligible(emp, MONDAY, 10, 16)
    assert _eligible(emp, TUESDAY, 10, 16)      # Tuesday unlisted
    # A demand window running to midnight arrives as end_hour 24
    assert not _eligible(emp, MONDAY, 17, 24)
    to_close = _emp("close", {"monday": [{"start": "17:00", "end": "23:59"}]})
    assert _eligible(to_close, MONDAY, 17, 24)


def test_auto_max_hours_still_enforced():
    emp = _emp("free", {})
    sched = _scheduler()
    assert not sched._is_employee_eligible(emp, MONDAY, 10, 16, {emp.id: 35.0}, _venue())


def test_auto_assignment_skips_out_of_range_staff():
    # The evenings-only full-timer out-scores the casual on cost, so before the
    # fix they'd have been handed the lunch slot.
    eve = _emp("eve", EVENINGS_MON, EmploymentType.full_time)
    free = _emp("free", {}, EmploymentType.casual)
    nomon = _emp("nomon", {"monday": []}, EmploymentType.full_time)
    slots = [
        {"date": MONDAY, "start_hour": 10, "end_hour": 16, "role": "general", "required_count": 1},
        {"date": MONDAY, "start_hour": 18, "end_hour": 22, "role": "general", "required_count": 1},
        {"date": TUESDAY, "start_hour": 10, "end_hour": 16, "role": "general", "required_count": 1},
    ]
    shifts = _scheduler()._assign_employees_to_shifts(
        slots, [eve, free, nomon], _venue(), MONDAY, "cost_optimized",
    )
    by_slot = {(s.date, s.start_time.hour): s.employee_id for s in shifts}
    assert by_slot[(MONDAY, 10)] == "free"     # eve out of range, nomon blocked
    assert by_slot[(MONDAY, 18)] == "eve"      # inside eve's window; top score
    assert by_slot[(TUESDAY, 10)] == "nomon"   # blocked Monday only; rested, top score
