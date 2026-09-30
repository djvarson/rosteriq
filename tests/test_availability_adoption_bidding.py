"""
Bidding group adopts the one availability rule (services/availability_rules.py).

The three cases that used to go wrong:
  * {}                      -> available (no constraints)
  * {"monday": []}          -> unavailable Monday, but AVAILABLE Tuesday
  * a range                 -> available only if the whole shift fits (to the minute)

Covered for shift bidding's preference score, the conflict detector's
availability check, and the MYOB employee mapping (which used to raise a
ValidationError for every employee, so a MYOB sync imported nobody).
"""

import asyncio
from datetime import date, datetime, time
from decimal import Decimal

import pytest

from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Shift, ShiftStatus, State,
)
from rosteriq.services.availability_rules import is_available
from rosteriq.services.conflict_detector import (
    ConflictDetector, ConflictSeverity, ConflictType,
)
from rosteriq.services.shift_bidding import OpenShift, ShiftBiddingService

MONDAY = date(2026, 10, 5)
TUESDAY = date(2026, 10, 6)

BASE = 0.5
AVAILABLE_BONUS = 0.25

NINE_TO_FIVE = {"monday": [{"start": "09:00", "end": "17:00"}]}


def test_fixture_dates_are_the_right_weekdays():
    assert MONDAY.weekday() == 0
    assert TUESDAY.weekday() == 1


def _employee(availability, emp_id="e1") -> Employee:
    now = datetime.utcnow()
    return Employee(
        id=emp_id,
        name="Sam Staff",
        employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_2,
        state=State.wa,
        hourly_base_rate=Decimal("28.00"),
        skills=["bar"],
        availability=availability,
        created_at=now,
        updated_at=now,
    )


# ============================================================================
# Shift bidding: the availability bonus follows is_available for the shift
# ============================================================================


def _open_shift(day: date, start: time, end: time) -> OpenShift:
    # role_required never matches the employee (Employee has no `role`), so
    # the score isolates the availability bonus.
    return OpenShift(
        id="os1", venue_id="v1", date=day,
        start_time=start, end_time=end, role_required="bar",
    )


def _score(availability, day, start=time(10, 0), end=time(16, 0)) -> float:
    svc = ShiftBiddingService.__new__(ShiftBiddingService)
    return svc._calculate_preference_score(
        _employee(availability), _open_shift(day, start, end)
    )


def test_bidding_empty_availability_gets_the_bonus():
    # {} used to score as "no preference" — it means available any time.
    assert _score({}, MONDAY) == pytest.approx(BASE + AVAILABLE_BONUS)


def test_bidding_day_marked_off_gets_no_bonus_but_other_days_do():
    # Used to be inverted: the listed-but-empty Monday earned the bonus and
    # the unlisted (available) Tuesday didn't.
    avail = {"monday": []}
    assert _score(avail, MONDAY) == pytest.approx(BASE)
    assert _score(avail, TUESDAY) == pytest.approx(BASE + AVAILABLE_BONUS)


def test_bidding_range_must_hold_the_whole_shift():
    assert _score(NINE_TO_FIVE, MONDAY, time(10, 0), time(16, 0)) == pytest.approx(BASE + AVAILABLE_BONUS)
    assert _score(NINE_TO_FIVE, MONDAY, time(9, 0), time(17, 0)) == pytest.approx(BASE + AVAILABLE_BONUS)
    assert _score(NINE_TO_FIVE, MONDAY, time(8, 0), time(16, 0)) == pytest.approx(BASE)
    # minute precision on both edges
    assert _score(NINE_TO_FIVE, MONDAY, time(8, 59), time(16, 0)) == pytest.approx(BASE)
    assert _score(NINE_TO_FIVE, MONDAY, time(9, 30), time(17, 1)) == pytest.approx(BASE)
    # a range on Monday says nothing about Tuesday
    assert _score(NINE_TO_FIVE, TUESDAY, time(6, 0), time(23, 0)) == pytest.approx(BASE + AVAILABLE_BONUS)


def test_bidding_score_tolerates_an_employee_without_availability_attr():
    class Bare:
        role = "floor"

    svc = ShiftBiddingService.__new__(ShiftBiddingService)
    assert svc._calculate_preference_score(
        Bare(), _open_shift(MONDAY, time(10, 0), time(16, 0))
    ) == pytest.approx(BASE + AVAILABLE_BONUS)


# ============================================================================
# Conflict detector: CRITICAL for a day off, WARNING for outside the window
# ============================================================================


def _shift(day: date, start: time, end: time) -> Shift:
    return Shift(
        id="s1", employee_id="e1", date=day, start_time=start, end_time=end,
        break_minutes=0, status=ShiftStatus.scheduled, role="general",
    )


def _availability_conflicts(availability, day, start=time(10, 0), end=time(16, 0)):
    det = ConflictDetector()
    det._check_availability_violation(_shift(day, start, end), _employee(availability))
    return [c for c in det.conflicts if c.conflict_type == ConflictType.AVAILABILITY_VIOLATION]


def test_conflicts_empty_availability_is_no_conflict():
    assert _availability_conflicts({}, MONDAY) == []


def test_conflicts_day_marked_off_is_critical_other_days_clear():
    avail = {"monday": []}
    found = _availability_conflicts(avail, MONDAY)
    assert len(found) == 1
    assert found[0].severity == ConflictSeverity.CRITICAL
    assert "not available on monday" in found[0].message
    assert found[0].shift_ids == ["s1"] and found[0].employee_ids == ["e1"]
    assert found[0].date == MONDAY

    assert _availability_conflicts(avail, TUESDAY) == []


def test_conflicts_range_fit_clear_and_misfit_is_warning():
    assert _availability_conflicts(NINE_TO_FIVE, MONDAY, time(9, 0), time(17, 0)) == []
    assert _availability_conflicts(NINE_TO_FIVE, MONDAY, time(9, 30), time(16, 45)) == []

    for start, end in [(time(7, 0), time(12, 0)), (time(9, 0), time(17, 30)), (time(8, 59), time(12, 0))]:
        found = _availability_conflicts(NINE_TO_FIVE, MONDAY, start, end)
        assert len(found) == 1, (start, end)
        assert found[0].severity == ConflictSeverity.WARNING
        assert "falls outside availability on monday" in found[0].message

    # unlisted day -> available all day
    assert _availability_conflicts(NINE_TO_FIVE, TUESDAY, time(6, 0), time(23, 0)) == []


def test_conflicts_any_of_several_windows_may_hold_the_shift():
    split = {"monday": [{"start": "07:00", "end": "11:00"}, {"start": "15:00", "end": "23:00"}]}
    assert _availability_conflicts(split, MONDAY, time(16, 0), time(22, 0)) == []
    found = _availability_conflicts(split, MONDAY, time(10, 0), time(16, 0))
    assert len(found) == 1 and found[0].severity == ConflictSeverity.WARNING


# ============================================================================
# MYOB: the mapped Employee builds, claims no availability, and is stamped
# ============================================================================

_MYOB_RECORD = {
    "UID": "abc-123",
    "FirstName": "Jo",
    "LastName": "Myob",
    "EmploymentBasis": "Casual",
    "Addresses": [{"Phone1": "0400000000", "Email": "jo@example.test", "State": "NSW"}],
    "PayrollDetails": {"Wage": {"HourlyRate": 31.5}},
}


def _myob_adapter():
    from rosteriq.myob_adapter import MYOBAdapter

    adapter = MYOBAdapter.__new__(MYOBAdapter)
    adapter.state = State.wa
    return adapter


def test_myob_map_employee_builds_with_no_constraints():
    before = datetime.utcnow()
    emp = _myob_adapter()._map_employee(_MYOB_RECORD)

    assert isinstance(emp, Employee)
    assert emp.id == "myob_abc-123"
    assert emp.availability == {}
    assert emp.created_at is not None and emp.updated_at is not None
    assert emp.created_at >= before and emp.updated_at >= before
    assert emp.state == State.nsw
    assert emp.hourly_base_rate == Decimal("31.5")

    # {} means available any day, any time under the shared rule
    assert is_available(emp.availability, MONDAY, "00:00", "23:59")
    assert is_available(emp.availability, TUESDAY)


def test_myob_get_employees_no_longer_drops_everyone():
    from types import SimpleNamespace

    adapter = _myob_adapter()
    adapter.credentials = SimpleNamespace(base_url="https://myob.invalid/cf")

    async def fake_get(url, **kwargs):
        return {"Items": [_MYOB_RECORD, dict(_MYOB_RECORD, UID="def-456", FirstName="Al")]}

    adapter._get = fake_get
    employees = asyncio.run(adapter.get_employees())

    assert [e.id for e in employees] == ["myob_abc-123", "myob_def-456"]
    assert all(e.availability == {} for e in employees)
