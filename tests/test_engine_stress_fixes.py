"""
Regression tests for the engine stress-test findings:

  BUG 1 — roster total_cost (optimiser/analysis) omitted weekly overtime.
  BUG 2 — award_rules.calculate_shift_cost gated evening/night loading on start hour.
  BUG 3 — _check_minimum_rest_violation blind to overnight previous shifts.
  BUG 4 — conflict_detector._check_fatigue_risk blind to overnight previous shifts.
"""

from datetime import date, time, datetime
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, Roster, State, EmploymentType, AwardLevel, ShiftStatus,
)
from rosteriq.award_rules import calculate_shift_cost, _check_minimum_rest_violation
from rosteriq.cost_calculator import calculate_roster_cost
from rosteriq.roster_optimiser import analyse_roster
from rosteriq.services.conflict_detector import ConflictDetector


def _emp(et=EmploymentType.full_time, rate="30.00"):
    return Employee(id="e1", name="A", employment_type=et, award_level=AwardLevel.level_1,
                    state=State.vic, hourly_base_rate=Decimal(rate),
                    created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1))


def _sh(d, sid, st, en, brk=0):
    return Shift(id=sid, employee_id="e1", date=d, start_time=st, end_time=en,
                 break_minutes=brk, status=ShiftStatus.scheduled, role="bar")


# --- BUG 2: calculate_shift_cost band-splits weekday loading -----------------

def test_calculate_shift_cost_evening_hours_loaded():
    # 17:00-23:00 weekday FT $30: 2h ordinary + 4h evening(+15%) = 60 + 138 = 198
    assert calculate_shift_cost(_emp(), _sh(date(2026, 6, 22), "s", time(17, 0), time(23, 0)), State.vic) == Decimal("198.00")


def test_calculate_shift_cost_no_longer_overcharges_morning():
    # 06:00-15:00 weekday FT $30: only 06:00-07:00 is night(+17.5%); 8h ordinary
    # -> 240 + 35.25 = 275.25 (was 317.25, charging +17.5% on all 9h)
    assert calculate_shift_cost(_emp(), _sh(date(2026, 6, 22), "s", time(6, 0), time(15, 0)), State.vic) == Decimal("275.25")


def test_calculate_shift_cost_daytime_unloaded():
    assert calculate_shift_cost(_emp(), _sh(date(2026, 6, 22), "s", time(9, 0), time(17, 0)), State.vic) == Decimal("240.00")


# --- BUG 1: roster total_cost includes weekly overtime ----------------------

def test_analyse_roster_total_cost_includes_overtime():
    emp = _emp()  # FT $30
    # 50h across the week (Mon-Sat weekdays) -> 12h OT; per-shift sum would miss it.
    days = [date(2026, 6, 22), date(2026, 6, 23), date(2026, 6, 24),
            date(2026, 6, 25), date(2026, 6, 26), date(2026, 6, 29)]
    # 6 shifts: five x 9h (08:00-17:00) + one 5h = 50h. Use 09:00-19:00 (10h, 1h break=9.5)? keep simple: 9h each via 08:00-17:00 break 0
    shifts = [_sh(d, f"s{i}", time(8, 0), time(17, 0)) for i, d in enumerate(days[:5])]  # 5*9=45
    shifts.append(_sh(days[5], "s5", time(8, 0), time(13, 0)))  # +5h = 50h
    roster = Roster(id="r", venue_id="v", week_start=date(2026, 6, 22), week_end=date(2026, 6, 28),
                    shifts=shifts, total_cost=Decimal("0"), created_at=datetime(2026, 6, 20))
    report = analyse_roster(roster, {"e1": emp}, State.vic)
    expected = calculate_roster_cost(roster, {"e1": emp}, State.vic)
    assert report["total_cost"] == expected
    # And strictly more than costing all 50h at ordinary (proves OT is in there).
    assert report["total_cost"] > Decimal("50") * Decimal("30")


# --- BUG 3 + 4: overnight rest detected -------------------------------------

def test_minimum_rest_detects_overnight_short_turnaround():
    prev = _sh(date(2026, 6, 22), "p", time(22, 0), time(6, 0))   # Mon 22:00 -> Tue 06:00
    nxt = _sh(date(2026, 6, 23), "n", time(9, 0), time(17, 0))    # Tue 09:00 (3h rest)
    violations = _check_minimum_rest_violation(nxt, [prev, nxt])
    assert violations and "3.0h" in violations[0]


def test_minimum_rest_detects_same_day_split_under_threshold():
    """A same-day split shift with <10h turnaround must be flagged by the AWARD
    validator too (not just the conflict detector). Review found these diverged."""
    early = _sh(date(2026, 6, 22), "e", time(6, 0), time(14, 0))
    late = _sh(date(2026, 6, 22), "l", time(15, 0), time(23, 0))   # 1h rest after 'early'
    v = _check_minimum_rest_violation(late, [early, late])
    assert v and "1.0h" in v[0]


def test_minimum_rest_ok_when_enough_gap():
    prev = _sh(date(2026, 6, 22), "p", time(9, 0), time(17, 0))   # Mon to 17:00
    nxt = _sh(date(2026, 6, 23), "n", time(9, 0), time(17, 0))    # Tue 09:00 (16h rest)
    assert _check_minimum_rest_violation(nxt, [prev, nxt]) == []


def test_conflict_detector_fatigue_detects_overnight():
    prev = _sh(date(2026, 6, 22), "p", time(22, 0), time(6, 0))
    nxt = _sh(date(2026, 6, 23), "n", time(9, 0), time(17, 0))
    d = ConflictDetector()
    d.conflicts = []
    d._check_fatigue_risk(nxt, [prev, nxt])
    assert d.conflicts, "overnight 3h turnaround should raise FATIGUE_RISK"
    assert "3.0h" in d.conflicts[0].message


def test_conflict_detector_fatigue_ok_when_rested():
    prev = _sh(date(2026, 6, 22), "p", time(9, 0), time(17, 0))
    nxt = _sh(date(2026, 6, 23), "n", time(9, 0), time(17, 0))
    d = ConflictDetector()
    d.conflicts = []
    d._check_fatigue_risk(nxt, [prev, nxt])
    assert d.conflicts == []
