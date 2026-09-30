"""
Surge on-call finder and decision-engine call-in scoring read availability
through the one shared rule (services/availability_rules.py):

* {} = no constraints -> available
* {"monday": []} = unavailable Monday, but AVAILABLE Tuesday (unlisted day)
* ranges = available only when the window fits inside one (minute precision)

The on-call finder used to read only the current day's list, so {} (the
default) and any unlisted day counted as unavailable. Call-in scoring used to
give {} a half "unknown" bonus and an unlisted day no bonus at all.
"""

import asyncio
from datetime import date, datetime
from decimal import Decimal

from rosteriq.models import AwardLevel, Employee, EmploymentType, State
from rosteriq.decision_engine import _call_in_priority_score, rank_for_call_in
from rosteriq.services import surge_detector as sd

MONDAY = date(2026, 10, 5)
TUESDAY = date(2026, 10, 6)

BONUS = 0.30   # the availability share of the call-in score


def _emp(emp_id="e1", availability=None, **kw) -> Employee:
    return Employee(
        id=emp_id, name=f"Staff {emp_id}", employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_1, state=State.wa,
        hourly_base_rate=Decimal("31.50"),
        availability=availability if availability is not None else {},
        created_at=datetime(2026, 1, 1), updated_at=datetime(2026, 1, 1), **kw,
    )


# ---------------------------------------------------------------------------
# Surge on-call finder
# ---------------------------------------------------------------------------

class _FakeDB:
    def __init__(self, employees, active_ids=()):
        self._employees = employees
        self._active = [type("S", (), {"employee_id": i})() for i in active_ids]

    def list_employees(self, venue_id):
        return self._employees

    def get_active_shifts(self, venue_id, day, hour):
        return self._active


def _oncall_ids(monkeypatch, employees, now, active_ids=()):
    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(sd, "datetime", _Clock)
    det = sd.SurgeDetector(_FakeDB(employees, active_ids), pos_realtime_feed=None)
    found = asyncio.run(det.find_available_oncall("v1", {"general": 10}))
    return {e.employee_id for e in found}


def test_surge_empty_availability_is_on_call(monkeypatch):
    for now in (datetime(2026, 10, 5, 9, 0), datetime(2026, 10, 6, 19, 10)):
        assert _oncall_ids(monkeypatch, [_emp("any", {})], now) == {"any"}


def test_surge_day_marked_off_only_blocks_that_day(monkeypatch):
    emp = _emp("monoff", {"monday": []})
    monday = datetime(2026, 10, 5, 19, 10)
    tuesday = datetime(2026, 10, 6, 19, 10)
    assert _oncall_ids(monkeypatch, [emp], monday) == set()
    assert _oncall_ids(monkeypatch, [emp], tuesday) == {"monoff"}


def test_surge_range_must_contain_now_to_the_minute(monkeypatch):
    emp = _emp("eve", {"tuesday": [{"start": "17:30", "end": "22:00"}]})
    assert _oncall_ids(monkeypatch, [emp], datetime(2026, 10, 6, 19, 10)) == {"eve"}
    assert _oncall_ids(monkeypatch, [emp], datetime(2026, 10, 6, 17, 29)) == set()
    assert _oncall_ids(monkeypatch, [emp], datetime(2026, 10, 6, 17, 30)) == {"eve"}
    assert _oncall_ids(monkeypatch, [emp], datetime(2026, 10, 6, 21, 59)) == {"eve"}
    assert _oncall_ids(monkeypatch, [emp], datetime(2026, 10, 6, 22, 0)) == set()


def test_surge_last_minute_of_day_and_already_working(monkeypatch):
    late = _emp("late", {"tuesday": [{"start": "20:00", "end": "23:59"}]})
    working = _emp("busy", {})
    now = datetime(2026, 10, 6, 23, 59)
    assert _oncall_ids(monkeypatch, [late, working], now, active_ids=["busy"]) == {"late"}


# ---------------------------------------------------------------------------
# Decision engine call-in scoring
# ---------------------------------------------------------------------------

def _score(availability, day, hour):
    return _call_in_priority_score(_emp("x", availability), day, hour, weekly_hours=0.0)


def test_call_in_empty_availability_gets_full_bonus():
    unconstrained = _score({}, TUESDAY, 14)
    marked_off = _score({"tuesday": []}, TUESDAY, 14)
    assert round(unconstrained - marked_off, 6) == BONUS
    # {} scores exactly like an explicit range that covers the hour
    assert _score({"tuesday": [{"start": "08:00", "end": "18:00"}]}, TUESDAY, 14) == unconstrained


def test_call_in_monday_off_is_unavailable_monday_available_tuesday():
    avail = {"monday": []}
    assert round(_score(avail, TUESDAY, 14) - _score(avail, MONDAY, 14), 6) == BONUS
    assert _score(avail, TUESDAY, 14) == _score({}, TUESDAY, 14)


def test_call_in_range_must_fit_the_whole_hour():
    fits = {"tuesday": [{"start": "13:30", "end": "15:00"}]}
    ends_on_the_hour = {"tuesday": [{"start": "09:00", "end": "14:00"}]}
    starts_mid_hour = {"tuesday": [{"start": "14:30", "end": "22:00"}]}
    base = _score({"tuesday": []}, TUESDAY, 14)
    assert round(_score(fits, TUESDAY, 14) - base, 6) == BONUS
    # used to pass with an inclusive hour-level end / start compare
    assert _score(ends_on_the_hour, TUESDAY, 14) == base
    assert _score(starts_mid_hour, TUESDAY, 14) == base
    # last hour of the day runs to midnight
    late = {"tuesday": [{"start": "22:00", "end": "23:59"}]}
    assert round(_score(late, TUESDAY, 23) - _score({"tuesday": []}, TUESDAY, 23), 6) == BONUS


def test_rank_for_call_in_prefers_available_over_marked_off():
    off_today = _emp("off", {"tuesday": []})
    off_monday = _emp("mon", {"monday": []})
    recs = rank_for_call_in([off_today, off_monday], [], TUESDAY, 14, State.wa)
    assert [r.employee_id for r in recs] == ["mon", "off"]
