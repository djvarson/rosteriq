"""
Tests for weekly overtime in roster costing.

Overtime previously NEVER fired (per-shift costing had no weekly context), so
hours beyond 38/week were paid at ordinary rates. These tests pin the corrected
behaviour: hours >38/week at OT rates (1.5x first 2 OT hours, then 2x), OT
excluded from super, casuals exempt.
"""

from datetime import date, time, datetime
from decimal import Decimal

import pytest

from rosteriq.models import (
    Employee, Shift, Roster, State, EmploymentType, AwardLevel, ShiftStatus,
)
from rosteriq.cost_calculator import cost_employee_week, calculate_roster_cost


def _emp(employment_type=EmploymentType.full_time, rate="30.00"):
    return Employee(
        id="e1", name="Alice", employment_type=employment_type,
        award_level=AwardLevel.level_1, state=State.vic,
        hourly_base_rate=Decimal(rate),
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )


def _shift(d, sid, start=time(9, 0), end=time(17, 0), brk=30):
    # 9-17 with 30m break = 7.5h net
    return Shift(
        id=sid, employee_id="e1", date=d, start_time=start, end_time=end,
        break_minutes=brk, status=ShiftStatus.scheduled, role="bar",
    )


def test_full_time_45h_week_triggers_overtime():
    """38h ordinary + 7h OT (2h@1.5x, 5h@2x); OT excluded from super."""
    emp = _emp()
    # 6 weekday shifts x 7.5h = 45h (Mon 2026-06-22 .. Sat 2026-06-27)
    shifts = [_shift(date(2026, 6, 22 + i), f"s{i}") for i in range(6)]
    # 2026-06-27 is a Saturday — keep all within Mon-Fri to test weekday OT:
    shifts = [_shift(date(2026, 6, 22) , "s0"),  # Mon
              _shift(date(2026, 6, 23), "s1"),   # Tue
              _shift(date(2026, 6, 24), "s2"),   # Wed
              _shift(date(2026, 6, 25), "s3"),   # Thu
              _shift(date(2026, 6, 26), "s4"),   # Fri
              _shift(date(2026, 6, 29), "s5")]   # next Mon (still weekday, no Sat/Sun penalty)
    r = cost_employee_week(emp, shifts, State.vic)

    assert r["overtime_hours"] == Decimal("7.0")
    assert r["ordinary_hours"] == Decimal("38.0")
    # base = 38h * $30 = $1140
    assert r["base_cost"] == Decimal("1140.00")
    # OT = 2h*30*1.5 + 5h*30*2.0 = 90 + 300 = 390
    assert r["overtime_cost"] == Decimal("390.00")
    # super on ordinary earnings only (1140), NOT on OT
    assert r["super_contribution"] == Decimal("131.10")  # 1140 * 0.115
    assert r["total_cost"] == Decimal("1661.10")  # 1140 + 390 + 131.10


def test_under_38h_has_no_overtime():
    """A 30h week is entirely ordinary — no OT, super on full earnings."""
    emp = _emp()
    shifts = [_shift(date(2026, 6, 22 + i), f"s{i}") for i in range(4)]  # 4 x 7.5 = 30h
    r = cost_employee_week(emp, shifts, State.vic)
    assert r["overtime_hours"] == Decimal("0")
    assert r["overtime_cost"] == Decimal("0.00")
    assert r["base_cost"] == Decimal("900.00")  # 30 * 30
    assert r["super_contribution"] == Decimal("103.50")  # 900 * 0.115


def test_casual_never_accrues_overtime():
    """Casuals get casual loading but no overtime, even at 45h."""
    emp = _emp(EmploymentType.casual)
    shifts = [_shift(date(2026, 6, 22), "s0"),
              _shift(date(2026, 6, 23), "s1"),
              _shift(date(2026, 6, 24), "s2"),
              _shift(date(2026, 6, 25), "s3"),
              _shift(date(2026, 6, 26), "s4"),
              _shift(date(2026, 6, 29), "s5")]  # 45h
    r = cost_employee_week(emp, shifts, State.vic)
    assert r["overtime_hours"] == Decimal("0")
    assert r["overtime_cost"] == Decimal("0.00")
    # base 45*30 = 1350, casual loading 25% = 337.50
    assert r["base_cost"] == Decimal("1350.00")
    assert r["casual_loading"] == Decimal("337.50")


def test_calculate_roster_cost_applies_overtime():
    """The roster total now includes OT (a 45h week costs more than 45h ordinary)."""
    emp = _emp()
    shifts = [_shift(date(2026, 6, 22), "s0"),
              _shift(date(2026, 6, 23), "s1"),
              _shift(date(2026, 6, 24), "s2"),
              _shift(date(2026, 6, 25), "s3"),
              _shift(date(2026, 6, 26), "s4"),
              _shift(date(2026, 6, 29), "s5")]  # 45h, one employee
    roster = Roster(
        id="r1", venue_id="v1", week_start=date(2026, 6, 22), week_end=date(2026, 6, 28),
        shifts=shifts, total_cost=Decimal("0"), created_at=datetime(2026, 6, 20),
    )
    cost = calculate_roster_cost(roster, {"e1": emp}, State.vic)
    assert cost == Decimal("1661.10")
    # Sanity: strictly more than costing all 45h as ordinary ($1140 + super).
    ordinary_only = Decimal("45") * Decimal("30")  # 1350 base if no OT split
    assert cost > ordinary_only


def test_payroll_batch_computes_overtime_and_excludes_it_from_super():
    """PayrollExporter now populates overtime_hours/amount and excludes OT from super."""
    from rosteriq.database import MemoryStore
    from rosteriq.services.payroll_export import PayrollExporter

    emp = _emp()  # full-time, $30/h
    shifts = [_shift(date(2026, 6, 22), "s0"),
              _shift(date(2026, 6, 23), "s1"),
              _shift(date(2026, 6, 24), "s2"),
              _shift(date(2026, 6, 25), "s3"),
              _shift(date(2026, 6, 26), "s4"),
              _shift(date(2026, 6, 29), "s5")]  # 45h, all weekdays

    exporter = PayrollExporter(MemoryStore())
    batch = exporter.prepare_timesheet_data(
        venue_id="v1", period_start=date(2026, 6, 22), period_end=date(2026, 6, 29),
        state=State.vic, shifts=shifts, employees={"e1": emp},
    )
    ep = batch.employees[0]
    assert ep.overtime_hours == Decimal("7.0")
    assert ep.overtime_amount == Decimal("390.00")  # 2h@1.5 + 5h@2 * $30
    assert ep.ordinary_hours == Decimal("38.0")
    # Super on ordinary earnings ($1140) only — NOT on the $390 overtime.
    assert ep.super_amount == Decimal("131.10")  # 1140 * 0.115
