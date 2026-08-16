"""
Payroll export API routes for RosterIQ.

Endpoints:
- POST /api/payroll/prepare — prepare timesheet batch for a period
- GET /api/payroll/batch/{batch_id} — get batch details
- POST /api/payroll/export/xero — push to Xero Payroll
- POST /api/payroll/export/keypay — push to KeyPay
- GET /api/payroll/reconcile/{venue_id} — reconciliation report
- PUT /api/payroll/batch/{batch_id}/approve — approve batch before export
- GET /api/payroll/history — export history
"""

import csv
import io
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.models import Shift, ShiftStatus, State
from rosteriq.services.payroll_export import (
    PayrollExporter, PayrollBatch, PayrollStatus
)
from rosteriq.services.xero_payroll import XeroPayrollClient
from rosteriq.services.keypay_export import KeyPayClient
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/payroll", tags=["payroll"])


def _audit_batch_created(batch, errors, basis: str) -> None:
    """Event-log a prepared payroll batch: the period, how many people are in
    it, the money, and what it was built from (rostered shifts vs approved
    timesheets). Never raises — the events spine swallows failures."""
    audit("payroll.batch_create", batch.venue_id, "payroll_batch", batch.batch_id,
          period_start=str(batch.period_start), period_end=str(batch.period_end),
          basis=basis, employee_count=len(batch.employees),
          total_gross=str(batch.total_gross), total_super=str(batch.total_super),
          status=batch.status.value, validation_errors=len(errors or []))


# ============================================================================
# Request/Response Models
# ============================================================================

class PreparePayrollRequest(BaseModel):
    """Request to prepare payroll batch."""
    venue_id: str
    period_start: date
    period_end: date
    state: State


class ApprovePayrollRequest(BaseModel):
    """Request to approve a payroll batch."""
    approved_by: str  # user ID
    notes: Optional[str] = None


class ExportXeroRequest(BaseModel):
    """Request to export to Xero Payroll."""
    batch_id: str
    xero_tenant_id: str
    xero_access_token: str


class ExportKeyPayRequest(BaseModel):
    """Request to export to KeyPay."""
    batch_id: str
    keypay_api_key: str
    keypay_business_id: str


class ReconcileRequest(BaseModel):
    """Request to reconcile exported payroll."""
    venue_id: str
    period_start: date
    period_end: date


class PayrollBatchResponse(BaseModel):
    """Response with payroll batch details."""
    batch_id: str
    venue_id: str
    period_start: date
    period_end: date
    status: str
    employee_count: int
    total_gross: str
    total_super: str
    validation_errors: List[str]
    created_at: str


class ExportResultResponse(BaseModel):
    """Response with export result."""
    success: bool
    batch_id: str
    exported_at: Optional[str]
    employee_count: int
    total_gross: str
    error_message: Optional[str]


class ReconcileResultResponse(BaseModel):
    """Response with reconciliation result."""
    venue_id: str
    period_start: date
    period_end: date
    reconciled: bool
    expected_gross: str
    actual_gross: str
    variance: str
    variance_percentage: float
    discrepancies: List[str]


# ============================================================================
# Routes
# ============================================================================

@router.post("/prepare", response_model=PayrollBatchResponse)
async def prepare_payroll_batch(
    request: PreparePayrollRequest,
    db=Depends(get_db),
):
    """
    Prepare timesheet batch for a pay period.

    Aggregates shifts and leave to calculate:
    - Ordinary and penalty hours
    - Superannuation (11.5%)
    - Allowances per MA000009
    - Validation status

    Returns payroll batch ready for approval and export.
    """
    try:
        enforce_venue_access(request.venue_id)
        # Fetch shifts and employees for the period
        venue = db.get_venue(request.venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {request.venue_id} not found")

        # Get all employees for the venue
        employees = {e.id: e for e in db.list_employees() if hasattr(e, 'id')}
        if not employees:
            raise HTTPException(
                status_code=404,
                detail=f"No employees found for venue {request.venue_id}",
            )

        # Get rosters/shifts for the period
        rosters = db.get_rosters_by_date_range(
            request.venue_id,
            request.period_start,
            request.period_end,
        )

        # Flatten shifts from rosters
        shifts = []
        for roster in rosters:
            shifts.extend(roster.shifts)

        if not shifts:
            raise HTTPException(
                status_code=400,
                detail=f"No shifts found for period {request.period_start} to {request.period_end}",
            )

        # Prepare payroll batch
        exporter = PayrollExporter(db)
        batch = exporter.prepare_timesheet_data(
            venue_id=request.venue_id,
            period_start=request.period_start,
            period_end=request.period_end,
            state=request.state,
            shifts=shifts,
            employees=employees,
        )

        # Validate batch
        errors = exporter.validate_batch(batch)

        # Save batch to DB
        db.save_payroll_batch(batch.to_dict())
        _audit_batch_created(batch, errors, basis="rostered_shifts")

        return PayrollBatchResponse(
            batch_id=batch.batch_id,
            venue_id=batch.venue_id,
            period_start=batch.period_start,
            period_end=batch.period_end,
            status=batch.status.value,
            employee_count=len(batch.employees),
            total_gross=str(batch.total_gross),
            total_super=str(batch.total_super),
            validation_errors=errors,
            created_at=batch.created_at.isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error preparing payroll batch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def _parse_dt(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)


@router.post("/prepare-actuals", response_model=PayrollBatchResponse)
async def prepare_payroll_from_timesheets(
    request: PreparePayrollRequest,
    db=Depends(get_db),
):
    """
    Prepare a payroll batch from APPROVED timesheets — actual punched time,
    not the rostered plan.

    Fail-loud by design: if any timesheet in the period is still open
    (someone's clocked in) or closed-but-unapproved, the batch is refused
    with each one named. Approve everything on the Timesheets page first —
    payroll built on unreviewed punches is how venues get burned.
    """
    enforce_venue_access(request.venue_id)
    venue = db.get_venue(request.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {request.venue_id} not found")

    sheets = db.get_timesheets(request.venue_id, request.period_start,
                               request.period_end) or []
    if not sheets:
        raise HTTPException(
            status_code=422,
            detail="No timesheets in this period — staff clock in at /timeclock, "
                   "and approved punches become payroll.")

    employees = {e.id: e for e in (db.get_employees(request.venue_id) or [])}
    blockers = []
    for t in sheets:
        status = t.get("status")
        if status in ("approved",):
            continue
        emp_name = getattr(employees.get(t.get("employee_id")), "name", t.get("employee_id"))
        label = "still clocked in" if status == "open" else "awaiting approval"
        blockers.append(f"{emp_name} on {t.get('work_date')} ({label})")
    if blockers:
        raise HTTPException(
            status_code=409,
            detail="Payroll uses approved timesheets only — sort these first: "
                   + "; ".join(sorted(blockers)))

    shifts = []
    for t in sheets:
        ci, co = _parse_dt(t["clock_in"]), _parse_dt(t["clock_out"])
        shifts.append(Shift(
            id=str(t.get("id")),
            employee_id=str(t.get("employee_id")),
            date=ci.date(),
            start_time=ci.time(),
            end_time=co.time(),
            break_minutes=int(t.get("break_minutes") or 0),
            status=ShiftStatus.completed,
            role="",
        ))

    exporter = PayrollExporter(db)
    batch = exporter.prepare_timesheet_data(
        venue_id=request.venue_id,
        period_start=request.period_start,
        period_end=request.period_end,
        state=request.state,
        shifts=shifts,
        employees=employees,
    )
    errors = exporter.validate_batch(batch)
    db.save_payroll_batch(batch.to_dict())
    _audit_batch_created(batch, errors, basis="approved_timesheets")
    logger.info(f"Payroll batch {batch.batch_id} prepared from {len(shifts)} "
                f"approved timesheets at {request.venue_id}")

    return PayrollBatchResponse(
        batch_id=batch.batch_id,
        venue_id=batch.venue_id,
        period_start=batch.period_start,
        period_end=batch.period_end,
        status=batch.status.value,
        employee_count=len(batch.employees),
        total_gross=str(batch.total_gross),
        total_super=str(batch.total_super),
        validation_errors=errors,
        created_at=batch.created_at.isoformat(),
    )


@router.get("/batch/{batch_id}/csv")
async def payroll_batch_csv(batch_id: str, venue_id: str = Query(...)):
    """The accountant's file: one row per employee with hours and gross
    broken down, plus a totals row. Opens straight into Excel or imports
    into Xero as a payroll worksheet."""
    enforce_venue_access(venue_id)
    db = get_db()
    batch = db.get_payroll_batch(batch_id)
    if not batch or batch.get("venue_id") != venue_id:
        raise HTTPException(status_code=404, detail="Payroll batch not found")

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([f"RosterIQ payroll batch {batch['batch_id']}",
                f"period {batch['period_start']} to {batch['period_end']}",
                "basis: approved timesheets / rostered shifts as prepared",
                "NOT a payment file — review with your payroll advisor"])
    w.writerow([])
    w.writerow(["Employee", "Ordinary hours", "Ordinary rate", "Ordinary gross",
                "Penalty hours detail", "Penalty gross", "Overtime hours",
                "Overtime gross", "Allowance detail", "Allowances", "Total gross",
                "Super (11.5%)", "Total incl super"])
    for e in batch.get("employees", []):
        penalty_detail = "; ".join(
            f"{p['penalty_type']} {p['hours']}h x{p['multiplier']}"
            for p in e.get("penalty_entries", []))
        # Every allowance dollar is NAMED — an accountant should never have to
        # guess what a $15.09 line is.
        allowance_detail = "; ".join(
            f"{a['allowance_type']} ${a['amount']}"
            for a in e.get("allowances", []))
        w.writerow([
            e.get("name"), e.get("ordinary_hours"), e.get("ordinary_rate"),
            e.get("ordinary_gross"), penalty_detail, e.get("penalty_gross"),
            e.get("overtime_hours"), e.get("overtime_amount"),
            allowance_detail, e.get("allowances_total"), e.get("total_gross"),
            e.get("super_amount"), e.get("total_with_super"),
        ])
    w.writerow([])
    w.writerow(["TOTALS", batch.get("total_ordinary_hours"), "",
                batch.get("total_ordinary_gross"), "", batch.get("total_penalty_gross"),
                batch.get("total_overtime_hours"), batch.get("total_overtime_gross"),
                "", batch.get("total_allowances"), batch.get("total_gross"),
                batch.get("total_super"), ""])

    return PlainTextResponse(
        buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition":
                 f"attachment; filename=payroll-{batch['period_start']}-to-{batch['period_end']}.csv"},
    )


@router.get("/batch/{batch_id}", response_model=PayrollBatchResponse)
async def get_payroll_batch(
    batch_id: str,
    db=Depends(get_db),
):
    """Get details of a payroll batch."""
    try:
        batch_dict = db.get_payroll_batch(batch_id)
        if not batch_dict:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        enforce_venue_access(batch_dict.get("venue_id"))

        return PayrollBatchResponse(
            batch_id=batch_dict["batch_id"],
            venue_id=batch_dict["venue_id"],
            period_start=date.fromisoformat(batch_dict["period_start"]),
            period_end=date.fromisoformat(batch_dict["period_end"]),
            status=batch_dict["status"],
            employee_count=len(batch_dict.get("employees", [])),
            total_gross=batch_dict["total_gross"],
            total_super=batch_dict["total_super"],
            validation_errors=batch_dict.get("validation_errors", []),
            created_at=batch_dict["created_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching payroll batch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/batch/{batch_id}/approve", response_model=PayrollBatchResponse)
async def approve_payroll_batch(
    batch_id: str,
    request: ApprovePayrollRequest,
    db=Depends(get_db),
):
    """Approve a payroll batch for export."""
    try:
        batch_dict = db.get_payroll_batch(batch_id)
        if not batch_dict:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        enforce_venue_access(batch_dict.get("venue_id"))

        if batch_dict["status"] != "draft":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot approve batch with status {batch_dict['status']}",
            )

        # Update batch status
        batch_dict["status"] = "approved"
        batch_dict["approved_at"] = date.today().isoformat()
        db.save_payroll_batch(batch_dict)
        audit("payroll.batch_approve", batch_dict.get("venue_id"), "payroll_batch", batch_id,
              period_start=str(batch_dict.get("period_start")),
              period_end=str(batch_dict.get("period_end")),
              employee_count=len(batch_dict.get("employees", [])),
              total_gross=str(batch_dict.get("total_gross")),
              approved_by=request.approved_by, notes=request.notes)

        return PayrollBatchResponse(
            batch_id=batch_dict["batch_id"],
            venue_id=batch_dict["venue_id"],
            period_start=date.fromisoformat(batch_dict["period_start"]),
            period_end=date.fromisoformat(batch_dict["period_end"]),
            status=batch_dict["status"],
            employee_count=len(batch_dict.get("employees", [])),
            total_gross=batch_dict["total_gross"],
            total_super=batch_dict["total_super"],
            validation_errors=batch_dict.get("validation_errors", []),
            created_at=batch_dict["created_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving payroll batch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export/xero", response_model=ExportResultResponse)
async def export_to_xero(
    request: ExportXeroRequest,
    db=Depends(get_db),
):
    """Push payroll batch to Xero Payroll."""
    try:
        batch_dict = db.get_payroll_batch(request.batch_id)
        if not batch_dict:
            raise HTTPException(status_code=404, detail=f"Batch {request.batch_id} not found")
        enforce_venue_access(batch_dict.get("venue_id"))

        if batch_dict["status"] != "approved":
            raise HTTPException(
                status_code=400,
                detail=f"Only approved batches can be exported (current: {batch_dict['status']})",
            )

        # Reconstruct the stored batch into a PayrollBatch and push it to Xero.
        from datetime import datetime, timedelta
        from rosteriq.xero_integration import XeroCredentials
        from rosteriq.services.xero_payroll import XeroPayrollClient
        from rosteriq.services.payroll_export import payroll_batch_from_dict

        venue_id = batch_dict["venue_id"]
        batch = payroll_batch_from_dict(batch_dict)

        # Prefer the venue's stored Xero credentials (so token refresh can work);
        # fall back to the request-supplied access token. token_expires is set in
        # the future so the provided token is used as-is.
        stored = db.get_xero_credentials(venue_id) if hasattr(db, "get_xero_credentials") else None
        stored = stored or {}
        credentials = XeroCredentials(
            venue_id=venue_id,
            client_id=stored.get("client_id", ""),
            client_secret=stored.get("client_secret", ""),
            tenant_id=request.xero_tenant_id,
            access_token=request.xero_access_token,
            refresh_token=stored.get("refresh_token", ""),
            token_expires=datetime.utcnow() + timedelta(hours=1),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )

        def _persist_xero(creds):
            if hasattr(db, "save_xero_credentials"):
                try:
                    db.save_xero_credentials(venue_id, creds.model_dump(mode="json"))
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"Failed to persist refreshed Xero creds: {e}")

        # Idempotency: the set of employee_ids already pushed for this batch in
        # a prior (partially-failed) attempt. Skipping these on a retry is what
        # prevents double-paying staff. The checkpoint callback persists each
        # newly-pushed employee immediately so progress survives a mid-batch
        # failure AND a process restart.
        pushed_employees = list(batch_dict.get("pushed_employees", []))

        def _checkpoint_pushed(employee_id: str):
            current = db.get_payroll_batch(request.batch_id) or batch_dict
            pushed = current.get("pushed_employees", [])
            if employee_id not in pushed:
                pushed.append(employee_id)
            current["pushed_employees"] = pushed
            db.save_payroll_batch(current)

        # Optional tenant-specific NAME -> EarningsRateID GUID map (Xero rejects
        # timesheet lines whose EarningTypeID is not a real GUID). When stored on
        # the venue's Xero credentials it pins the IDs; otherwise the client
        # resolves them live from the tenant's PayItems.
        earnings_rate_ids = stored.get("earnings_rate_ids") or None
        client = XeroPayrollClient(
            credentials,
            on_token_refresh=_persist_xero,
            earnings_rate_ids=earnings_rate_ids,
        )
        result = await client.push_timesheets(
            batch,
            pushed_employees=pushed_employees,
            on_employee_pushed=_checkpoint_pushed,
        )

        if not result.get("success"):
            # Honest failure — do NOT mark the batch exported.
            logger.warning(
                f"Xero export for batch {request.batch_id} failed: "
                f"{result.get('error_message')}"
            )
            return ExportResultResponse(
                success=False,
                batch_id=request.batch_id,
                exported_at=None,
                employee_count=result.get("employee_count", 0),
                total_gross=batch_dict.get("total_gross", "0.00"),
                error_message=result.get("error_message") or "Xero payroll export failed",
            )

        # Success — mark exported and record.
        batch_dict["status"] = "exported"
        batch_dict["exported_at"] = date.today().isoformat()
        db.save_payroll_batch(batch_dict)
        db.save_payroll_export({
            "batch_id": request.batch_id,
            "service": "xero",
            "status": "success",
            "total_gross": batch_dict["total_gross"],
            "employee_count": result.get("employee_count", len(batch_dict.get("employees", []))),
            "reference": result.get("xero_reference"),
            "exported_at": date.today().isoformat(),
        })
        return ExportResultResponse(
            success=True,
            batch_id=request.batch_id,
            exported_at=date.today().isoformat(),
            employee_count=result.get("employee_count", len(batch_dict.get("employees", []))),
            total_gross=batch_dict["total_gross"],
            error_message=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting to Xero: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export/keypay", response_model=ExportResultResponse)
async def export_to_keypay(
    request: ExportKeyPayRequest,
    db=Depends(get_db),
):
    """Push payroll batch to KeyPay."""
    try:
        batch_dict = db.get_payroll_batch(request.batch_id)
        if not batch_dict:
            raise HTTPException(status_code=404, detail=f"Batch {request.batch_id} not found")
        enforce_venue_access(batch_dict.get("venue_id"))

        if batch_dict["status"] != "approved":
            raise HTTPException(
                status_code=400,
                detail=f"Only approved batches can be exported (current: {batch_dict['status']})",
            )

        # Reconstruct the stored batch and push it to KeyPay.
        from rosteriq.services.keypay_export import KeyPayClient
        from rosteriq.services.payroll_export import payroll_batch_from_dict

        batch = payroll_batch_from_dict(batch_dict)
        # Idempotency: skip employees already pushed in a prior attempt so a
        # retry of a partially-failed batch never double-pays staff. The
        # checkpoint callback persists each newly-pushed employee immediately.
        pushed_employees = list(batch_dict.get("pushed_employees", []))

        def _checkpoint_pushed(employee_id: str):
            current = db.get_payroll_batch(request.batch_id) or batch_dict
            pushed = current.get("pushed_employees", [])
            if employee_id not in pushed:
                pushed.append(employee_id)
            current["pushed_employees"] = pushed
            db.save_payroll_batch(current)

        client = KeyPayClient(
            api_key=request.keypay_api_key,
            business_id=request.keypay_business_id,
        )
        result = await client.push_timesheets(
            batch,
            pushed_employees=pushed_employees,
            on_employee_pushed=_checkpoint_pushed,
        )

        if not result.get("success"):
            # Honest failure — do NOT mark the batch exported.
            logger.warning(
                f"KeyPay export for batch {request.batch_id} failed: "
                f"{result.get('error_message')}"
            )
            return ExportResultResponse(
                success=False,
                batch_id=request.batch_id,
                exported_at=None,
                employee_count=result.get("employee_count", 0),
                total_gross=batch_dict.get("total_gross", "0.00"),
                error_message=result.get("error_message") or "KeyPay payroll export failed",
            )

        # Success — mark exported and record.
        batch_dict["status"] = "exported"
        batch_dict["exported_at"] = date.today().isoformat()
        db.save_payroll_batch(batch_dict)
        db.save_payroll_export({
            "batch_id": request.batch_id,
            "service": "keypay",
            "status": "success",
            "total_gross": batch_dict["total_gross"],
            "employee_count": result.get("employee_count", len(batch_dict.get("employees", []))),
            "exported_at": date.today().isoformat(),
        })
        return ExportResultResponse(
            success=True,
            batch_id=request.batch_id,
            exported_at=date.today().isoformat(),
            employee_count=result.get("employee_count", len(batch_dict.get("employees", []))),
            total_gross=batch_dict["total_gross"],
            error_message=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting to KeyPay: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reconcile/{venue_id}", response_model=ReconcileResultResponse)
async def reconcile_payroll(
    venue_id: str,
    period_start: date = Query(...),
    period_end: date = Query(...),
    db=Depends(get_db),
):
    """Get reconciliation report for a pay period."""
    try:
        enforce_venue_access(venue_id)
        # Get payroll batches for the period
        batches = db.list_payroll_batches(venue_id)
        period_batches = [
            b for b in batches
            if (date.fromisoformat(b["period_start"]) >= period_start and
                date.fromisoformat(b["period_end"]) <= period_end)
        ]

        if not period_batches:
            raise HTTPException(
                status_code=404,
                detail=f"No payroll batches found for venue {venue_id} in period",
            )

        # Calculate expected gross
        expected_gross = Decimal("0.00")
        for batch_dict in period_batches:
            expected_gross += Decimal(batch_dict["total_gross"])

        # For now, mock reconciliation
        return ReconcileResultResponse(
            venue_id=venue_id,
            period_start=period_start,
            period_end=period_end,
            reconciled=True,
            expected_gross=str(expected_gross),
            actual_gross=str(expected_gross),
            variance="0.00",
            variance_percentage=0.0,
            discrepancies=[],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconciling payroll: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=List[ExportResultResponse])
async def get_payroll_export_history(
    venue_id: str = Query(..., description="Venue to list export history for"),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db),
):
    """Get export history for venue. Venue-scoped like every other payroll
    route — payroll data is the last place a cross-tenant read is acceptable."""
    enforce_venue_access(venue_id)
    try:
        # Mock implementation
        return []

    except Exception as e:
        logger.error(f"Error fetching export history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
