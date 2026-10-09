"""
Pay-run and labour-costing maths for The Wembley Hotel pilot.

1. No allowance is paid unless the venue configured it, at the amount the
   venue's payroll advisor set. RosterIQ holds no award dollar values, so a
   junior who works four hours is paid exactly their hours — no phantom
   "meal" line on every employee.
2. Superannuation follows the one Superannuation Guarantee table: 11.5% until
   30 June 2025, 12% from 1 July 2025. Every calculator, the Xero labour
   journal and the accountant CSV label read the same table.
3. A shift that crosses midnight is priced on each calendar day it was worked:
   a Saturday close's after-midnight hours are Sunday hours, a Friday close's
   are Saturday hours, and a Sunday close running into WA King's Birthday is
   holiday time after midnight.
4. Overtime hours are rounded to 2 dp like every other hour column, so the
   accountant never sees 5.6666666666666630.
"""

import asyncio
import re
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Shift, ShiftStatus, State,
)
from rosteriq.services.payroll_export import (
    AllowanceRule, PayrollExporter, PenaltyType,
)

KINGS_BIRTHDAY_WA = date(2026, 9, 28)        # Monday, WA public holiday
SAT, SUN = date(2026, 9, 26), date(2026, 9, 27)
FRI = date(2026, 9, 25)


def _emp(eid="e1", etype=EmploymentType.casual, rate="30.00", state=State.wa):
    return Employee(
        id=eid, venue_id="wembley", name=f"Staff {eid}", employment_type=etype,
        award_level=AwardLevel.level_2, hourly_base_rate=Decimal(rate), state=state,
        email=f"{eid}@x.com", skills=["bar"],
        created_at=datetime(2026, 6, 1), updated_at=datetime(2026, 6, 1),
    )


def _shift(d, start, end, eid="e1", break_minutes=0, sid=None):
    return Shift(
        id=sid or f"s-{eid}-{d}-{start}", employee_id=eid, date=d,
        start_time=start, end_time=end, break_minutes=break_minutes,
        status=ShiftStatus.completed, role="bar",
    )


def _run(employee, shifts, start, end, **kw):
    return PayrollExporter(db=None).prepare_timesheet_data(
        venue_id="wembley", period_start=start, period_end=end, state=State.wa,
        shifts=shifts, employees={employee.id: employee}, **kw,
    )


def _penalty_hours(ep):
    return {p.penalty_type: p.hours for p in ep.penalty_entries}


# ---------------------------------------------------------------------------
# 1. allowances only from venue-configured rules
# ---------------------------------------------------------------------------

def test_no_allowance_is_added_that_the_venue_never_configured():
    junior = _emp("jr", rate="18.40")
    tue = date(2026, 9, 22)
    ep = _run(junior, [_shift(tue, time(17, 0), time(21, 0), eid="jr")], tue, tue).employees[0]

    assert ep.allowances == []
    assert ep.allowances_total == Decimal("0")
    assert ep.total_gross == ep.ordinary_gross + ep.penalty_gross


def test_a_venue_configured_allowance_is_paid_at_the_venues_amount_on_qualifying_shifts():
    ft = _emp("ft", etype=EmploymentType.full_time)
    mon = date(2026, 9, 21)
    # 4 x 9.5h = 38h ordinary, then a 3h Friday that is all overtime.
    shifts = [_shift(mon + timedelta(days=i), time(9, 0), time(18, 30), eid="ft")
              for i in range(4)]
    shifts.append(_shift(FRI, time(9, 0), time(12, 0), eid="ft"))
    rule = AllowanceRule(allowance_type="overtime meal", amount=Decimal("17.25"),
                         min_overtime_hours=Decimal("1"))

    ep = _run(ft, shifts, mon, mon + timedelta(days=6), allowance_rules=[rule]).employees[0]

    assert [(a.allowance_type, a.amount) for a in ep.allowances] == [
        ("overtime meal", Decimal("17.25"))]
    assert ep.allowances_total == Decimal("17.25")


def test_a_configured_allowance_is_paid_once_per_qualifying_shift():
    casual = _emp("c1")
    mon = date(2026, 9, 21)
    shifts = [_shift(mon, time(9, 0), time(15, 0), eid="c1"),       # 6h qualifies
              _shift(mon + timedelta(days=1), time(9, 0), time(12, 0), eid="c1"),  # 3h does not
              _shift(mon + timedelta(days=2), time(9, 0), time(14, 0), eid="c1")]  # 5h qualifies
    rule = AllowanceRule(allowance_type="laundry", amount=Decimal("1.50"),
                         min_shift_hours=Decimal("5"))

    ep = _run(casual, shifts, mon, mon + timedelta(days=6), allowance_rules=[rule]).employees[0]

    assert [(a.allowance_type, a.amount) for a in ep.allowances] == [("laundry", Decimal("3.00"))]


def test_payroll_service_holds_no_allowance_dollar_values():
    src = (Path(__file__).resolve().parents[1] / "services" / "payroll_export.py").read_text()
    assert "15.09" not in src
    assert "3.26" not in src


# ---------------------------------------------------------------------------
# 2. one date-effective Superannuation Guarantee table
# ---------------------------------------------------------------------------

def test_sg_rate_is_12_percent_from_1_july_2025():
    from rosteriq.cost_calculator import super_guarantee_rate, super_rate_label

    assert super_guarantee_rate(date(2025, 6, 30)) == Decimal("0.115")
    assert super_guarantee_rate(date(2025, 7, 1)) == Decimal("0.12")
    assert super_guarantee_rate(date(2026, 10, 9)) == Decimal("0.12")
    assert super_rate_label(Decimal("0.12")) == "12%"
    assert super_rate_label(Decimal("0.115")) == "11.5%"


def test_roster_shift_cost_uses_the_sg_rate_for_the_shift_date():
    from rosteriq.cost_calculator import calculate_shift_cost_breakdown

    ft = _emp("ft", etype=EmploymentType.full_time)
    # Tuesday 9-5 with a 30-minute break: 7.5h x $30 = $225.00 ordinary.
    now = calculate_shift_cost_breakdown(
        ft, _shift(date(2026, 9, 22), time(9, 0), time(17, 0), eid="ft", break_minutes=30),
        State.wa)
    assert now.super_contribution == Decimal("27.00")           # 12%

    before = calculate_shift_cost_breakdown(
        ft, _shift(date(2025, 6, 24), time(9, 0), time(17, 0), eid="ft", break_minutes=30),
        State.wa)
    assert before.super_contribution == Decimal("25.88")        # 11.5%


def test_roster_week_cost_uses_the_sg_rate():
    from rosteriq.cost_calculator import cost_employee_week

    ft = _emp("ft", etype=EmploymentType.full_time)
    mon = date(2026, 9, 21)
    shifts = [_shift(mon + timedelta(days=i), time(9, 0), time(17, 0), eid="ft", break_minutes=30)
              for i in range(4)]                                # 30h, $900
    assert cost_employee_week(ft, shifts, State.wa)["super_contribution"] == Decimal("108.00")


def test_pay_run_super_is_12_percent_of_ordinary_time_earnings():
    casual = _emp("c1")
    tue = date(2026, 9, 22)
    batch = _run(casual, [_shift(tue, time(9, 0), time(17, 0), eid="c1")], tue, tue)
    ep = batch.employees[0]
    assert ep.ordinary_gross == Decimal("300.00")               # 8h x $30 x 1.25
    assert ep.super_amount == Decimal("36.00")
    assert batch.to_dict()["super_rate"] == "0.12"


def test_employee_costing_uses_the_sg_rate():
    from rosteriq.services.employee_costing import EmployeeCostingService

    ft = _emp("ft", etype=EmploymentType.full_time)
    shift = _shift(date(2026, 9, 22), time(9, 0), time(17, 0), eid="ft", break_minutes=30)
    _, _, _, super_contrib, _, _, _ = EmployeeCostingService._calculate_shift_costs(ft, shift)
    assert super_contrib == Decimal("27.00")


def test_penalty_calculator_quick_estimate_uses_the_sg_rate():
    from rosteriq.services.penalty_calculator import PenaltyCalculator

    est = PenaltyCalculator().quick_estimate(
        award_level="level_2", employment_type="full_time", state="wa",
        date_str="2026-09-22", start_time_str="09:00", end_time_str="17:00",
        hourly_rate=Decimal("30.00"), break_minutes=30,
    )
    assert est.super_contribution == Decimal("27.00")


def test_xero_labour_journal_names_the_sg_rate_for_its_date():
    from rosteriq.xero_integration import LabourCostJournal, XeroClient

    sent = {}

    async def _fake_request(method, endpoint, data=None, **kw):
        sent.update(data or {})
        return {}

    client = XeroClient.__new__(XeroClient)
    client._make_request = _fake_request
    journal = LabourCostJournal(
        venue_id="wembley", journal_date=date(2026, 9, 22),
        total_wages_expense=Decimal("1000.00"), total_super_payable=Decimal("120.00"),
        breakdown={}, award_levels={},
    )
    asyncio.run(client.push_labour_costs(journal))
    descriptions = [li["Description"] for li in sent["ManualJournals"][0]["LineItems"]]
    assert any("(12%)" in d for d in descriptions), descriptions
    assert not any("11.5" in d for d in descriptions), descriptions


def test_the_sg_rate_lives_in_one_table():
    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in root.rglob("*.py"):
        rel = path.relative_to(root)
        if rel.parts[0] in ("tests", "scripts") or rel.name.startswith("build_"):
            continue
        if re.search(r'Decimal\("0\.115"\)|\b0\.115\b', path.read_text(errors="ignore")):
            offenders.append(str(rel))
    assert offenders == ["cost_calculator.py"], offenders


# ---------------------------------------------------------------------------
# 3. shifts crossing midnight are priced on each day worked
# ---------------------------------------------------------------------------

def test_saturday_close_after_midnight_is_paid_at_the_sunday_rate():
    casual = _emp("c1")
    ep = _run(casual, [_shift(SAT, time(18, 0), time(1, 30), eid="c1")], SAT, SUN).employees[0]

    hours = _penalty_hours(ep)
    assert hours[PenaltyType.saturday] == Decimal("6.00")
    assert hours[PenaltyType.sunday] == Decimal("1.50")
    # 6h x $30 x 1.5 + 1.5h x $30 x 1.75
    assert ep.penalty_gross == Decimal("348.75")


def test_sunday_close_into_kings_birthday_is_holiday_time_after_midnight():
    casual = _emp("c1")
    ep = _run(casual, [_shift(SUN, time(18, 0), time(1, 0), eid="c1")],
              SUN, KINGS_BIRTHDAY_WA).employees[0]

    hours = _penalty_hours(ep)
    assert hours[PenaltyType.sunday] == Decimal("6.00")
    assert hours[PenaltyType.public_holiday] == Decimal("1.00")


def test_friday_close_after_midnight_is_saturday_not_weekday_night():
    casual = _emp("c1")
    ep = _run(casual, [_shift(FRI, time(18, 0), time(1, 0), eid="c1")], FRI, SAT).employees[0]

    hours = _penalty_hours(ep)
    assert ep.ordinary_hours == Decimal("1.00")                 # 18:00-19:00 Friday
    assert hours[PenaltyType.evening] == Decimal("5.00")        # 19:00-24:00 Friday
    assert hours[PenaltyType.saturday] == Decimal("1.00")       # 00:00-01:00 Saturday
    assert PenaltyType.night not in hours


def test_weekday_into_weekday_keeps_evening_and_night_bands():
    casual = _emp("c1")
    thu = date(2026, 9, 24)
    ep = _run(casual, [_shift(thu, time(18, 0), time(2, 0), eid="c1")], thu, FRI).employees[0]

    hours = _penalty_hours(ep)
    assert ep.ordinary_hours == Decimal("1.00")
    assert hours[PenaltyType.evening] == Decimal("5.00")
    assert hours[PenaltyType.night] == Decimal("2.00")


def test_unpaid_break_is_spread_across_both_days():
    casual = _emp("c1")
    # 18:00-02:00 is 8h worked span on Sat (6h) and Sun (2h); a 60-minute
    # break leaves 7h net, deducted pro rata: 5.25h Saturday, 1.75h Sunday.
    ep = _run(casual, [_shift(SAT, time(18, 0), time(2, 0), eid="c1", break_minutes=60)],
              SAT, SUN).employees[0]
    hours = _penalty_hours(ep)
    assert hours[PenaltyType.saturday] == Decimal("5.25")
    assert hours[PenaltyType.sunday] == Decimal("1.75")


def test_overtime_after_midnight_is_priced_on_the_holiday():
    ft = _emp("ft", etype=EmploymentType.full_time)
    mon = date(2026, 9, 21)
    shifts = [_shift(mon + timedelta(days=i), time(9, 0), time(18, 30), eid="ft")
              for i in range(4)]                                # 38h ordinary
    shifts.append(_shift(SUN, time(20, 0), time(2, 0), eid="ft"))   # 6h, all overtime
    ep = _run(ft, shifts, mon, KINGS_BIRTHDAY_WA).employees[0]

    assert ep.overtime_hours == Decimal("6.00")
    # Sunday 4h x $30 x 2.0 + public holiday 2h x $30 x 2.5
    assert ep.overtime_amount == Decimal("390.00")


def test_roster_shift_cost_prices_each_side_of_midnight():
    from rosteriq.cost_calculator import calculate_shift_cost_breakdown

    casual = _emp("c1")
    b = calculate_shift_cost_breakdown(casual, _shift(SAT, time(18, 0), time(1, 30), eid="c1"),
                                       State.wa)
    assert b.base_cost == Decimal("225.00")                     # 7.5h x $30
    assert b.casual_loading == Decimal("56.25")
    # Saturday 6h: $180 x (1.5 - 1.25); Sunday 1.5h: $45 x (1.75 - 1.25)
    assert b.penalty_cost == Decimal("67.50")


def test_roster_week_cost_prices_each_side_of_midnight():
    from rosteriq.cost_calculator import cost_employee_week

    casual = _emp("c1")
    r = cost_employee_week(casual, [_shift(SUN, time(18, 0), time(1, 0), eid="c1")], State.wa)
    # Sunday 6h: $180 x 0.5; holiday Monday 1h: $30 x (2.5 - 1.25)
    assert r["penalty_cost"] == Decimal("127.50")


# ---------------------------------------------------------------------------
# 4. overtime hours rounded to 2 dp
# ---------------------------------------------------------------------------

def _ot_week():
    ft = _emp("ft", etype=EmploymentType.full_time)
    mon = date(2026, 9, 21)
    # Five 08:40 shifts = 43h20m: 5h20m overtime, a repeating decimal.
    shifts = [_shift(mon + timedelta(days=i), time(9, 0), time(17, 40), eid="ft")
              for i in range(5)]
    return ft, shifts, mon


def test_overtime_hours_are_rounded_to_two_places():
    ft, shifts, mon = _ot_week()
    batch = _run(ft, shifts, mon, mon + timedelta(days=6))
    ep = batch.employees[0]
    assert str(ep.overtime_hours) == "5.33"
    assert str(batch.total_overtime_hours) == "5.33"


def _owner(c):
    email = f"a5{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def test_accountant_csv_labels_super_at_12_percent_and_rounds_overtime():
    from rosteriq.api import app
    from rosteriq.database import get_db

    c = TestClient(app)
    h = _owner(c)
    vid = "a5-wembley-csv"
    c.post("/venues", json={
        "id": vid, "name": "Wembley CSV", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    ft, shifts, mon = _ot_week()
    batch = PayrollExporter(db=None).prepare_timesheet_data(
        venue_id=vid, period_start=mon, period_end=mon + timedelta(days=6),
        state=State.wa, shifts=shifts, employees={ft.id: ft},
    )
    get_db().save_payroll_batch(batch.to_dict())

    r = c.get(f"/api/payroll/batch/{batch.batch_id}/csv?venue_id={vid}", headers=h)
    assert r.status_code == 200, r.text
    lines = r.text.splitlines()
    assert "Super (12%)" in lines[2]
    assert "11.5" not in lines[2]
    row = next(l for l in lines if l.startswith("Staff ft")).split(",")
    assert row[6] == "5.33"
    totals = next(l for l in lines if l.startswith("TOTALS")).split(",")
    assert totals[6] == "5.33"
    assert "meal" not in r.text
