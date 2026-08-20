"""
Money correctness in the pay run. Every test here failed before the fix and
describes an amount a real person would have been paid wrongly.

1. Casual loading — a casual's ORDINARY (weekday daytime) hours were priced at
   1.0x. The multiplier table said 1.25 all along; ordinary_gross just never
   consulted it. Every casual was underpaid 25% on their base hours.
2. The 38-hour overtime threshold and the 1.5x->2x tier are WEEKLY under
   MA000009, but the counters ran for the whole pay period — so a fortnightly
   run gave one 38-hour allowance across two weeks and invented ~38 hours of
   overtime in week two.
3. Punches are stored as naive UTC while the venue's own day is stored as
   work_date. Payroll priced shifts off the UTC wall clock, so an Australian
   Sunday morning was paid at Saturday rates and could fall outside the pay
   period entirely.
4. Xero earning types were looked up with str(enum), which on Python 3.12 is
   "PenaltyType.saturday" — so every penalty line was pushed as Ordinary Hours.
"""

from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from rosteriq.models import Employee, EmploymentType, AwardLevel, Shift, ShiftStatus, State
from rosteriq.services.payroll_export import PayrollExporter, PenaltyType


def _emp(eid="e1", etype=EmploymentType.casual, rate="30.00"):
    return Employee(
        id=eid, venue_id="v1", name="Casual Kate", employment_type=etype,
        award_level=AwardLevel.level_2, hourly_base_rate=Decimal(rate), state=State.wa,
        email=f"{eid}@x.com", skills=["bar"],
        created_at=datetime(2026, 6, 1), updated_at=datetime(2026, 6, 1),
    )


def _shift(d, start, end, eid="e1", sid=None, break_minutes=0):
    return Shift(
        id=sid or f"s-{d}-{start}", employee_id=eid, date=d,
        start_time=time(start, 0), end_time=time(end, 0),
        break_minutes=break_minutes, status=ShiftStatus.completed, role="bar",
    )


def _run(employee, shifts, start, end):
    exporter = PayrollExporter(db=None)
    return exporter.prepare_timesheet_data(
        venue_id="v1", period_start=start, period_end=end, state=State.wa,
        shifts=shifts, employees={employee.id: employee},
    )


# ---------------------------------------------------------------------------
# 1. casual loading
# ---------------------------------------------------------------------------

def test_casual_ordinary_hours_carry_the_25_percent_loading():
    """A casual on $30 working a Tuesday 9-5 is paid $37.50/h, not $30."""
    emp = _emp(etype=EmploymentType.casual, rate="30.00")
    tue = date(2026, 8, 18)                       # a Tuesday
    batch = _run(emp, [_shift(tue, 9, 17)], tue, tue)
    e = batch.employees[0]

    assert e.ordinary_hours == Decimal("8.00")
    assert e.ordinary_multiplier == Decimal("1.25")
    # 8h * $30 * 1.25 = $300.00 (was $240.00 — a $60 shortfall on one shift)
    assert e.ordinary_gross == Decimal("300.00")


def test_permanent_ordinary_hours_are_unloaded():
    emp = _emp(eid="e2", etype=EmploymentType.full_time, rate="30.00")
    tue = date(2026, 8, 18)
    e = _run(emp, [_shift(tue, 9, 17, eid="e2")], tue, tue).employees[0]
    assert e.ordinary_multiplier == Decimal("1.00")
    assert e.ordinary_gross == Decimal("240.00")


def test_casual_loading_does_not_double_up_on_penalty_hours():
    """Casual penalty multipliers already include the loading — a Saturday must
    be 1.5x base, not 1.25 * 1.5."""
    emp = _emp(rate="30.00")
    sat = date(2026, 8, 22)
    e = _run(emp, [_shift(sat, 9, 17)], sat, sat).employees[0]
    sat_entry = [p for p in e.penalty_entries if p.penalty_type == PenaltyType.saturday][0]
    assert sat_entry.multiplier == Decimal("1.5")
    assert sat_entry.amount == Decimal("360.00")          # 8h * 30 * 1.5
    assert e.ordinary_hours == Decimal("0.00")            # nothing double-counted


# ---------------------------------------------------------------------------
# 2. the 38-hour threshold is weekly
# ---------------------------------------------------------------------------

def test_overtime_threshold_resets_each_week_in_a_fortnightly_run():
    """Two identical 38-hour weeks are 76 ordinary hours and zero overtime."""
    emp = _emp(eid="ft", etype=EmploymentType.full_time, rate="30.00")
    week1 = date(2026, 8, 17)                     # Monday
    shifts = []
    for w in (week1, week1 + timedelta(days=7)):
        for i in range(5):                        # Mon-Fri, 7.6h each = 38h
            d = w + timedelta(days=i)
            shifts.append(Shift(
                id=f"s{d}", employee_id="ft", date=d,
                start_time=time(9, 0), end_time=time(16, 36),
                break_minutes=0, status=ShiftStatus.completed, role="bar",
            ))
    e = _run(emp, shifts, week1, week1 + timedelta(days=13)).employees[0]

    assert e.overtime_hours == Decimal("0.00"), (
        "a second week's hours were treated as overtime because the 38-hour "
        "counter never reset")
    assert e.ordinary_hours == Decimal("76.00")


def test_overtime_still_applies_within_a_single_week():
    emp = _emp(eid="ft2", etype=EmploymentType.full_time, rate="30.00")
    mon = date(2026, 8, 17)
    shifts = [Shift(id=f"x{i}", employee_id="ft2", date=mon + timedelta(days=i),
                    start_time=time(9, 0), end_time=time(19, 0), break_minutes=0,
                    status=ShiftStatus.completed, role="bar") for i in range(5)]  # 50h
    e = _run(emp, shifts, mon, mon + timedelta(days=6)).employees[0]
    assert e.overtime_hours == Decimal("12.00")           # 50 - 38
    # first 2 OT hours at 1.5x, remaining 10 at 2.0x
    assert e.overtime_amount == Decimal("690.00")         # 2*30*1.5 + 10*30*2


def test_the_15x_overtime_tier_also_resets_each_week():
    emp = _emp(eid="ft3", etype=EmploymentType.full_time, rate="30.00")
    w1, w2 = date(2026, 8, 17), date(2026, 8, 24)
    shifts = []
    for w in (w1, w2):
        for i in range(5):
            shifts.append(Shift(id=f"t{w}{i}", employee_id="ft3", date=w + timedelta(days=i),
                                start_time=time(9, 0), end_time=time(17, 0), break_minutes=0,
                                status=ShiftStatus.completed, role="bar"))  # 40h/week
    e = _run(emp, shifts, w1, w2 + timedelta(days=6)).employees[0]
    assert e.overtime_hours == Decimal("4.00")            # 2h each week
    # both weeks' 2 hours sit in the 1.5x tier: 4 * 30 * 1.5
    assert e.overtime_amount == Decimal("180.00")


# ---------------------------------------------------------------------------
# 4. Xero earning types
# ---------------------------------------------------------------------------

def test_xero_penalty_lines_do_not_collapse_into_ordinary_hours():
    from rosteriq.services.xero_payroll import XeroPayrollClient, XeroEarningType

    sync = XeroPayrollClient.__new__(XeroPayrollClient)   # no network setup needed
    for ptype, expected in (
        (PenaltyType.saturday, XeroEarningType.saturday_loading.value),
        (PenaltyType.sunday, XeroEarningType.sunday_loading.value),
        (PenaltyType.public_holiday, XeroEarningType.public_holiday.value),
        (PenaltyType.evening, XeroEarningType.evening_loading.value),
        (PenaltyType.night, XeroEarningType.night_loading.value),
    ):
        assert sync._map_penalty_to_xero_type(ptype) == expected, ptype


# ---------------------------------------------------------------------------
# 5. the roster editor's live penalty calculator charges what it displays
# ---------------------------------------------------------------------------

def test_evening_and_night_loadings_are_actually_charged():
    """The calculator listed "Evening loading (7pm-midnight)" with
    additional_cost=0.00 and nothing ever totalled it, so every weekday
    evening shift was quoted 15-17.5% under what it costs. Loading applies to
    the hours ACTUALLY worked in each band, not to the whole shift."""
    from rosteriq.database import get_db
    from rosteriq.services.penalty_calculator import PenaltyCalculator

    db = get_db()
    db.save_employee(Employee(
        id="pcalc", venue_id="v1", name="Eve", employment_type=EmploymentType.full_time,
        award_level=AwardLevel.level_2, hourly_base_rate=Decimal("30.00"), state=State.wa,
        email="eve@x.com", skills=["bar"],
        created_at=datetime(2026, 6, 1), updated_at=datetime(2026, 6, 1)))
    calc = PenaltyCalculator()

    # Tuesday 17:00-23:00 = 2 ordinary + 4 evening hours
    b = calc.calculate(employee_id="pcalc", date_str="2026-08-18",
                       start_time_str="17:00", end_time_str="23:00",
                       role="bar", break_minutes=0)
    evening = [p for p in b.penalties_applied if "Evening" in p.name]
    assert evening, "evening loading not reported"
    assert evening[0].hours_applicable == 4.0, "loading applied to the whole shift"
    assert evening[0].additional_cost == Decimal("18.00")     # 4 * 30 * 0.15
    assert b.penalty_cost == Decimal("18.00"), "displayed but never charged"

    # A plain daytime shift carries none
    day = calc.calculate(employee_id="pcalc", date_str="2026-08-18",
                         start_time_str="09:00", end_time_str="17:00",
                         role="bar", break_minutes=0)
    assert day.penalty_cost == Decimal("0") and not day.penalties_applied

    # Overnight: 22:00-06:00 = 2 evening + 6 night
    night = calc.calculate(employee_id="pcalc", date_str="2026-08-18",
                           start_time_str="22:00", end_time_str="06:00",
                           role="bar", break_minutes=0)
    names = {p.name.split(" (")[0]: p for p in night.penalties_applied}
    assert names["Evening loading"].additional_cost == Decimal("9.00")    # 2 * 30 * .15
    assert names["Late night loading"].additional_cost == Decimal("31.50")  # 6 * 30 * .175
    assert night.penalty_cost == Decimal("40.50")
