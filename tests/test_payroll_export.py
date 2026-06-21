"""
Tests for real payroll export wiring.

Critically verifies the export no longer FAKES success: a failed provider
export returns success=False and leaves the batch 'approved' (no silent payroll
data loss), while a successful export marks it 'exported'. Also covers the
PayrollBatch <-> dict round-trip used to reconstruct a stored batch for export.
"""

from datetime import date
from decimal import Decimal

import pytest

from rosteriq.database import MemoryStore
from rosteriq.services.payroll_export import (
    PayrollBatch,
    EmployeePayroll,
    PenaltyEntry,
    PenaltyType,
    PayrollStatus,
    payroll_batch_from_dict,
)
import rosteriq.services.xero_payroll as xero_payroll_mod
import rosteriq.services.keypay_export as keypay_mod
from routes.payroll import export_to_xero, export_to_keypay, ExportXeroRequest, ExportKeyPayRequest


def _make_batch(status=PayrollStatus.approved) -> PayrollBatch:
    emp = EmployeePayroll(
        employee_id="e1", name="Alice", email="a@x.com", tax_file_number_masked="123",
        ordinary_hours=Decimal("38"), ordinary_rate=Decimal("25.00"), ordinary_gross=Decimal("950.00"),
        penalty_entries=[PenaltyEntry(
            penalty_type=PenaltyType.saturday, hours=Decimal("4"),
            multiplier=Decimal("1.25"), amount=Decimal("125.00"),
        )],
        penalty_gross=Decimal("125.00"), super_amount=Decimal("123.62"),
    )
    batch = PayrollBatch(
        batch_id="b1", venue_id="v1",
        period_start=date(2026, 6, 1), period_end=date(2026, 6, 14),
        status=status, employees=[emp],
    )
    batch.calculate_totals()
    return batch


def _make_two_employee_batch(status=PayrollStatus.approved) -> PayrollBatch:
    """A batch with two employees, used to exercise per-employee idempotency."""
    def _emp(eid, name, email):
        return EmployeePayroll(
            employee_id=eid, name=name, email=email, tax_file_number_masked="123",
            ordinary_hours=Decimal("38"), ordinary_rate=Decimal("25.00"),
            ordinary_gross=Decimal("950.00"), super_amount=Decimal("109.25"),
        )

    batch = PayrollBatch(
        batch_id="b2", venue_id="v1",
        period_start=date(2026, 6, 1), period_end=date(2026, 6, 14),
        status=status,
        employees=[
            _emp("e1", "Alice", "a@x.com"),
            _emp("e2", "Bob", "b@x.com"),
        ],
    )
    batch.calculate_totals()
    return batch


def test_payroll_batch_round_trips_through_dict():
    """to_dict -> payroll_batch_from_dict reconstructs the batch faithfully."""
    batch = _make_batch()
    rebuilt = payroll_batch_from_dict(batch.to_dict())

    assert rebuilt.batch_id == "b1"
    assert rebuilt.venue_id == "v1"
    assert rebuilt.period_start == date(2026, 6, 1)
    assert rebuilt.status == PayrollStatus.approved
    assert len(rebuilt.employees) == 1
    e = rebuilt.employees[0]
    assert e.employee_id == "e1"
    assert e.ordinary_hours == Decimal("38")
    assert e.penalty_entries[0].penalty_type == PenaltyType.saturday
    assert e.penalty_entries[0].amount == Decimal("125.00")
    assert e.super_amount == Decimal("123.62")
    # Totals recomputed and match.
    assert rebuilt.total_gross == batch.total_gross


class _FakeResult(dict):
    pass


def _seed_approved_batch(db):
    db.save_payroll_batch(_make_batch().to_dict())


async def test_xero_export_failure_does_not_mark_exported(monkeypatch):
    """A failed Xero push returns success=False and leaves the batch approved."""
    db = MemoryStore()
    _seed_approved_batch(db)

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def push_timesheets(self, batch, **kwargs):
            return _FakeResult(success=False, error_message="Xero token rejected", employee_count=0)

    monkeypatch.setattr(xero_payroll_mod, "XeroPayrollClient", FakeClient)

    resp = await export_to_xero(
        ExportXeroRequest(batch_id="b1", xero_tenant_id="t1", xero_access_token="tok"), db=db
    )
    assert resp.success is False
    assert "Xero" in (resp.error_message or "")
    # The batch must NOT have been marked exported — no silent data loss.
    assert db.get_payroll_batch("b1")["status"] == "approved"


async def test_xero_export_success_marks_exported(monkeypatch):
    """A successful Xero push returns success=True and marks the batch exported."""
    db = MemoryStore()
    _seed_approved_batch(db)

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def push_timesheets(self, batch, **kwargs):
            return _FakeResult(success=True, employee_count=1, xero_reference="PR-B1")

    monkeypatch.setattr(xero_payroll_mod, "XeroPayrollClient", FakeClient)

    resp = await export_to_xero(
        ExportXeroRequest(batch_id="b1", xero_tenant_id="t1", xero_access_token="tok"), db=db
    )
    assert resp.success is True
    assert resp.employee_count == 1
    assert db.get_payroll_batch("b1")["status"] == "exported"


async def test_keypay_export_failure_does_not_mark_exported(monkeypatch):
    """A failed KeyPay push returns success=False and leaves the batch approved."""
    db = MemoryStore()
    _seed_approved_batch(db)

    class FakeClient:
        def __init__(self, *a, **k): pass
        async def push_timesheets(self, batch, **kwargs):
            return _FakeResult(success=False, error_message="KeyPay 401", employee_count=0)

    monkeypatch.setattr(keypay_mod, "KeyPayClient", FakeClient)

    resp = await export_to_keypay(
        ExportKeyPayRequest(batch_id="b1", keypay_api_key="k", keypay_business_id="b"), db=db
    )
    assert resp.success is False
    assert db.get_payroll_batch("b1")["status"] == "approved"


class _RecordingFakeClient:
    """A fake payroll client that mimics the real push_timesheets idempotency
    contract: it skips employees already in pushed_employees, invokes the
    checkpoint callback after each successful per-employee push, and records
    every employee it was *actually asked to push* across all calls.

    ``fail_employee_ids`` is the set of employee_ids that fail on the FIRST
    attempt; once an employee has been attempted once it is removed from the
    set so a retry succeeds (simulating a transient failure being resolved).
    """

    # Shared across instances so the route constructing a fresh client per
    # export call still accumulates the push history.
    push_calls: list = []
    fail_employee_ids: set = set()

    def __init__(self, *a, **k):
        pass

    async def push_timesheets(self, batch, pushed_employees=None, on_employee_pushed=None):
        already = set(pushed_employees or [])
        result = dict(success=False, employee_count=len(already), xero_reference="PR-B2", keypay_reference="KP-B2")
        for emp in batch.employees:
            if emp.employee_id in already:
                # Idempotency: must NOT re-push an already-pushed employee.
                continue
            # Record that the provider was asked to push this employee.
            type(self).push_calls.append(emp.employee_id)
            if emp.employee_id in type(self).fail_employee_ids:
                # Transient failure on the first attempt only.
                type(self).fail_employee_ids.discard(emp.employee_id)
                continue
            already.add(emp.employee_id)
            if on_employee_pushed:
                on_employee_pushed(emp.employee_id)
        result["employee_count"] = len(already)
        result["success"] = all(e.employee_id in already for e in batch.employees)
        if not result["success"]:
            result["error_message"] = "transient push failure"
        return result


async def test_xero_export_idempotent_retry_does_not_repush(monkeypatch):
    """Employee 2 fails on the first attempt; a SECOND export call pushes ONLY
    employee 2. Employee 1 must be pushed exactly once across both calls."""
    db = MemoryStore()
    db.save_payroll_batch(_make_two_employee_batch().to_dict())

    _RecordingFakeClient.push_calls = []
    _RecordingFakeClient.fail_employee_ids = {"e2"}
    monkeypatch.setattr(xero_payroll_mod, "XeroPayrollClient", _RecordingFakeClient)

    # First attempt: e1 succeeds, e2 fails -> batch stays approved.
    resp1 = await export_to_xero(
        ExportXeroRequest(batch_id="b2", xero_tenant_id="t1", xero_access_token="tok"), db=db
    )
    assert resp1.success is False
    assert db.get_payroll_batch("b2")["status"] == "approved"
    # e1 was checkpointed as pushed.
    assert db.get_payroll_batch("b2")["pushed_employees"] == ["e1"]

    # Second attempt (re-post the same batch): only e2 should be pushed.
    resp2 = await export_to_xero(
        ExportXeroRequest(batch_id="b2", xero_tenant_id="t1", xero_access_token="tok"), db=db
    )
    assert resp2.success is True
    assert db.get_payroll_batch("b2")["status"] == "exported"

    # The crux: e1 was asked to push EXACTLY ONCE across both calls (no double
    # pay); e2 was attempted twice (failed then succeeded).
    assert _RecordingFakeClient.push_calls.count("e1") == 1
    assert _RecordingFakeClient.push_calls.count("e2") == 2
    assert sorted(set(db.get_payroll_batch("b2")["pushed_employees"])) == ["e1", "e2"]


async def test_keypay_export_idempotent_retry_does_not_repush(monkeypatch):
    """Same idempotency guarantee for the KeyPay export path."""
    db = MemoryStore()
    db.save_payroll_batch(_make_two_employee_batch().to_dict())

    _RecordingFakeClient.push_calls = []
    _RecordingFakeClient.fail_employee_ids = {"e2"}
    monkeypatch.setattr(keypay_mod, "KeyPayClient", _RecordingFakeClient)

    resp1 = await export_to_keypay(
        ExportKeyPayRequest(batch_id="b2", keypay_api_key="k", keypay_business_id="b"), db=db
    )
    assert resp1.success is False
    assert db.get_payroll_batch("b2")["pushed_employees"] == ["e1"]

    resp2 = await export_to_keypay(
        ExportKeyPayRequest(batch_id="b2", keypay_api_key="k", keypay_business_id="b"), db=db
    )
    assert resp2.success is True
    assert db.get_payroll_batch("b2")["status"] == "exported"

    assert _RecordingFakeClient.push_calls.count("e1") == 1
    assert _RecordingFakeClient.push_calls.count("e2") == 2
