"""
Deputy → RosterIQ model mapping — a rehearsal of the pilot venue's first sync.

Both mappers used to construct models with fields that don't exist
(first_name/hourly_rate/is_active on Employee; datetime-typed start/end and no
date/role on Shift). Pydantic rejected every record, the per-record error
handler swallowed each failure, and a first sync would import ZERO staff and
ZERO shifts while reporting "success". These tests drive realistic Deputy API
payloads through the mappers and the adapter to pin the fixed behaviour.
"""

import asyncio
from datetime import datetime, date, time
from decimal import Decimal

import httpx
import respx

from rosteriq.deputy_adapter import DeputyAdapter, DeputyCredentials
from rosteriq.models import Employee, Shift, State, EmploymentType, ShiftStatus


def _adapter(state=State.wa):
    creds = DeputyCredentials(
        subdomain="testco",
        client_id="cid",
        client_secret="sec",
        access_token="tok",
    )
    return DeputyAdapter(credentials=creds, state=state)


# Realistic Deputy Employee resource payloads
_DEPUTY_EMP_FULL = {
    "Id": 42, "FirstName": "Emma", "LastName": "Thompson",
    "Email": "emma@venue.com", "Phone": "0400111222",
    "EmpType": 1, "Active": True, "Company": 3,
    "HourlyRate": 34.50, "Role": {"ReportingName": "Bartender"},
}
_DEPUTY_EMP_NO_RATE = {
    "Id": 43, "FirstName": "James", "LastName": "Wilson",
    "Email": "james@venue.com", "EmpType": 3, "Active": True, "Company": 3,
}
_DEPUTY_EMP_NO_NAME = {
    "Id": 44, "DisplayName": "Casual Worker 44", "EmpType": 3, "Active": True,
}


def test_map_employee_full_record():
    a = _adapter()
    emp = a._map_employee(_DEPUTY_EMP_FULL)
    assert isinstance(emp, Employee)
    assert emp.id == "deputy-42"            # prefixed: no collision, idempotent re-sync
    assert emp.name == "Emma Thompson"
    assert emp.hourly_base_rate == Decimal("34.50")
    assert emp.employment_type == EmploymentType.full_time
    assert emp.state == State.wa            # venue's state carried through
    assert "Bartender" in emp.skills
    assert a._rate_review_ids == []         # real rate -> nothing to review


def test_map_employee_missing_rate_gets_flagged_placeholder():
    a = _adapter()
    emp = a._map_employee(_DEPUTY_EMP_NO_RATE)
    assert emp.hourly_base_rate == DeputyAdapter.DEFAULT_SYNC_RATE
    assert "43" in a._rate_review_ids       # flagged for operator review


def test_map_employee_display_name_fallback():
    a = _adapter()
    emp = a._map_employee(_DEPUTY_EMP_NO_NAME)
    assert emp.name == "Casual Worker 44"


# Realistic Deputy Roster resource payloads (epoch AND ISO timestamps)
_DEPUTY_SHIFT_EPOCH = {
    "Id": 900, "Employee": 42, "Company": 3,
    "StartTime": 1782892800,  # epoch seconds
    "EndTime": 1782921600,
    "ConfirmStatus": 1, "OperationalUnitName": "Bar", "Cost": 276.0,
}
_DEPUTY_SHIFT_ISO = {
    "Id": 901, "Employee": 43, "Company": 3,
    "StartTime": "2026-07-10T18:00:00", "EndTime": "2026-07-10T23:30:00",
    "ConfirmStatus": 0, "OperationalUnitName": None, "Mealbreak": 30,
}


def test_map_shift_epoch_timestamps():
    a = _adapter()
    s = a._map_shift(_DEPUTY_SHIFT_EPOCH)
    assert isinstance(s, Shift)
    assert s.id == "deputy-900"
    assert s.employee_id == "deputy-42"
    assert isinstance(s.date, date) and isinstance(s.start_time, time)
    assert s.role == "Bar"
    assert s.cost == Decimal("276.0")


def test_map_shift_iso_timestamps_and_defaults():
    a = _adapter()
    s = a._map_shift(_DEPUTY_SHIFT_ISO)
    assert s.date == date(2026, 7, 10)
    assert s.start_time == time(18, 0) and s.end_time == time(23, 30)
    assert s.role == "general"              # missing OperationalUnitName -> default
    assert s.break_minutes == 30
    assert s.status == ShiftStatus.scheduled
    assert s.cost is None                   # no cost -> honest None, not 0


def test_map_shift_missing_start_raises():
    a = _adapter()
    try:
        a._map_shift({"Id": 902, "Employee": 1, "EndTime": "2026-07-10T23:00:00"})
        assert False, "expected ValueError"
    except ValueError:
        pass


@respx.mock
def test_get_employees_end_to_end_returns_valid_models():
    """Adapter-level: a mocked Deputy QUERY response maps every record and
    flags the rate-less one — the exact behaviour of the venue's first sync."""
    respx.post("https://testco.au.deputy.com/api/v1/resource/Employee/QUERY").mock(
        return_value=httpx.Response(200, json=[_DEPUTY_EMP_FULL, _DEPUTY_EMP_NO_RATE])
    )

    async def run():
        async with _adapter() as a:
            emps = await a.get_employees()
            return emps, list(a._rate_review_ids)

    emps, review = asyncio.run(run())
    assert len(emps) == 2                   # NOTHING silently dropped
    assert {e.name for e in emps} == {"Emma Thompson", "James Wilson"}
    assert review == ["43"]
