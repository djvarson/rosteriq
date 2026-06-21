"""
Tests for evening/night loading applying to hours ACTUALLY WORKED.

Previously evening/night loading was gated on a shift's START hour, so a
09:00-23:00 shift earned $0 evening loading despite working 19:00-23:00. These
tests pin the corrected behaviour: loading attaches to the hours worked in the
evening (19:00-24:00, +15%) and night (00:00-07:00, +17.5%) windows on weekdays.

The loading PERCENTAGES here are the existing model values; the exact award $
amounts / band boundaries are a separate payroll-advisor confirmation item.
"""

from datetime import date, time, datetime
from decimal import Decimal

from rosteriq.models import (
    Employee, Shift, State, EmploymentType, AwardLevel, ShiftStatus,
)
from rosteriq.award_rules import split_weekday_hours_by_band
from rosteriq.cost_calculator import (
    cost_employee_week, calculate_shift_cost_breakdown,
)
from rosteriq.database import MemoryStore
from rosteriq.services.payroll_export import PayrollExporter, PenaltyType


def _emp(employment_type=EmploymentType.full_time, rate="30.00"):
    return Employee(
        id="e1", name="Alice", employment_type=employment_type,
        award_level=AwardLevel.level_1, state=State.vic,
        hourly_base_rate=Decimal(rate),
        created_at=datetime(2025, 1, 1), updated_at=datetime(2025, 1, 1),
    )


def _shift(d, sid, start, end, brk=30):
    return Shift(
        id=sid, employee_id="e1", date=d, start_time=start, end_time=end,
        break_minutes=brk, status=ShiftStatus.scheduled, role="bar",
    )


# --- the band-split helper -------------------------------------------------

def test_split_daytime_shift_is_all_ordinary():
    b = split_weekday_hours_by_band(time(9, 0), time(17, 0), Decimal("8"))
    assert b["ordinary"] == Decimal("8")
    assert b["evening"] == Decimal("0.00")
    assert b["night"] == Decimal("0.00")


def test_split_evening_hours_counted():
    # 09:00-23:00, 30m break -> net 13.5h; 19:00-23:00 = 4h gross evening.
    b = split_weekday_hours_by_band(time(9, 0), time(23, 0), Decimal("13.5"))
    # Bands scale to net (proportional break deduction) and sum to net.
    assert b["evening"] > Decimal("0")
    assert b["night"] == Decimal("0.00")
    assert abs((b["ordinary"] + b["evening"] + b["night"]) - Decimal("13.5")) < Decimal("0.001")
    # 4h gross of 14h gross span, scaled by 13.5/14 = ~3.857h.
    assert abs(b["evening"] - Decimal("3.857")) < Decimal("0.001")


def test_split_overnight_shift_bands():
    # 22:00-06:00, no break -> 22:00-24:00 evening (2h), 00:00-06:00 night (6h).
    b = split_weekday_hours_by_band(time(22, 0), time(6, 0), Decimal("8"))
    assert b["evening"] == Decimal("2")
    assert b["night"] == Decimal("6")
    assert b["ordinary"] == Decimal("0")


# --- cost path -------------------------------------------------------------

def test_cost_evening_shift_now_loaded():
    """A 09:00-23:00 weekday shift earns evening loading (was $0 before)."""
    emp = _emp()
    r = cost_employee_week(emp, [_shift(date(2026, 6, 22), "s0", time(9, 0), time(23, 0))], State.vic)
    # base = 13.5h * $30 = $405 regardless of bands.
    assert r["base_cost"] == Decimal("405.00")
    # Evening loading on ~3.857h at +15% of $30 ~= $17.36 (was $0).
    assert r["penalty_cost"] > Decimal("0")
    assert r["penalty_cost"] == Decimal("17.36")


def test_cost_daytime_shift_no_loading():
    """A 09:00-17:00 weekday shift still has zero penalty (regression guard)."""
    emp = _emp()
    r = cost_employee_week(emp, [_shift(date(2026, 6, 22), "s0", time(9, 0), time(17, 0))], State.vic)
    assert r["penalty_cost"] == Decimal("0.00")


def test_shift_breakdown_evening_loaded():
    """Per-shift breakdown also loads evening hours, not gated on start hour."""
    emp = _emp()
    bd = calculate_shift_cost_breakdown(
        emp, _shift(date(2026, 6, 22), "s0", time(9, 0), time(23, 0)), State.vic
    )
    assert bd.penalty_cost == Decimal("17.36")


def test_shift_breakdown_saturday_unchanged():
    """Saturday shift still uses the whole-day penalty (no evening double-count)."""
    emp = _emp()
    # 2026-06-20 is a Saturday.
    bd = calculate_shift_cost_breakdown(
        emp, _shift(date(2026, 6, 20), "s0", time(9, 0), time(17, 0)), State.vic
    )
    # Saturday FT = 1.25x -> penalty = 0.25 * (7.5h * $30 = $225) = $56.25.
    assert bd.penalty_cost == Decimal("56.25")


# --- payroll path ----------------------------------------------------------

def test_payroll_evening_hours_get_penalty_entry():
    """PayrollExporter records an evening penalty entry for worked evening hours."""
    emp = _emp()
    exporter = PayrollExporter(MemoryStore())
    batch = exporter.prepare_timesheet_data(
        venue_id="v1", period_start=date(2026, 6, 22), period_end=date(2026, 6, 22),
        state=State.vic, shifts=[_shift(date(2026, 6, 22), "s0", time(9, 0), time(23, 0))],
        employees={"e1": emp},
    )
    ep = batch.employees[0]
    evening = [pe for pe in ep.penalty_entries if pe.penalty_type == PenaltyType.evening]
    assert len(evening) == 1
    assert evening[0].hours == Decimal("3.86")  # ~19:00-23:00 scaled for break
    assert evening[0].multiplier == Decimal("1.15")
    # The remaining ordinary hours are NOT all bucketed as evening.
    assert ep.ordinary_hours == Decimal("9.64")


def test_payroll_daytime_shift_no_evening_entry():
    """A daytime weekday shift has no evening/night penalty entries."""
    emp = _emp()
    exporter = PayrollExporter(MemoryStore())
    batch = exporter.prepare_timesheet_data(
        venue_id="v1", period_start=date(2026, 6, 22), period_end=date(2026, 6, 22),
        state=State.vic, shifts=[_shift(date(2026, 6, 22), "s0", time(9, 0), time(17, 0))],
        employees={"e1": emp},
    )
    ep = batch.employees[0]
    loaded = [pe for pe in ep.penalty_entries
              if pe.penalty_type in (PenaltyType.evening, PenaltyType.night)]
    assert loaded == []
    assert ep.ordinary_hours == Decimal("7.50")
