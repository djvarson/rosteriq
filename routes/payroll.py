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
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.services.clock import venue_timezone
from rosteriq.services.current_roster import live_shifts
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Shift, ShiftStatus, State
from rosteriq.cost_calculator import super_rate_label
from rosteriq.services.payroll_export import (
    PayrollExporter, PayrollBatch, PayrollStatus
)
from rosteriq.services.xero_payroll import XeroPayrollClient
from rosteriq.services.keypay_export import KeyPayClient
from rosteriq.services.events import audit
from rosteriq.routes.timeclock import MAX_SPAN_MINUTES, _overlapping_ids, _span_minutes

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
    # "Meca Rossi: 56h across both venues in the week of 12 Oct (...) - check
    # overtime with your advisor". Flags only: whether two venues are one
    # employer for overtime is the owner's and their advisor's call.
    cross_venue_hours: List[str] = []


def _cross_venue_hours(db, venue_id: str, period_start: date, period_end: date) -> List[str]:
    """People here whose worked hours across the venues the caller fully
    manages pass the weekly overtime threshold the pay run uses."""
    from rosteriq.cost_calculator import WEEKLY_OVERTIME_THRESHOLD
    from rosteriq.middleware.tenant import get_tenant_context_optional
    from rosteriq.services.person_identity import combined_hours_flags
    from rosteriq.services.venue_access import fully_managed_group
    tenant = get_tenant_context_optional()
    if tenant is None:
        return []
    try:
        group = fully_managed_group(db, tenant, venue_id)
        return [f["message"] for f in combined_hours_flags(
            db, venue_id, period_start, period_end, group, float(WEEKLY_OVERTIME_THRESHOLD))]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Cross-venue hours check failed for {venue_id}: {e}")
        return []


def _salaried_elsewhere(db, venue_id: str, batch) -> List[str]:
    """Salaried lines here whose person (same email) is also salaried at
    another venue the caller fully manages: two fixed lines for one person.
    Flags only — whether both are owed is the advisor's call."""
    from rosteriq.middleware.tenant import get_tenant_context_optional
    from rosteriq.services.pay_basis import is_salaried
    from rosteriq.services.person_identity import person_key, records_by_person
    from rosteriq.services.venue_access import fully_managed_group
    tenant = get_tenant_context_optional()
    lines = [e for e in batch.employees if e.pay_basis == "salaried"]
    if tenant is None or not lines:
        return []
    try:
        others = [v for v in fully_managed_group(db, tenant, venue_id) if v != venue_id]
        if not others:
            return []
        index = records_by_person(db, others)
        here = {e.id: e for e in (db.get_employees(venue_id) or [])}
        out = []
        for line in lines:
            key = person_key(here.get(line.employee_id))
            venues = sorted({vid for other, vid in index.get(key, []) if is_salaried(other)}) if key else []
            if venues:
                names = ", ".join(getattr(db.get_venue(v), "name", None) or v for v in venues)
                out.append(f"{line.name} is also salaried at {names} — one person with a salary "
                           "line at each venue; confirm with your advisor before paying both")
        return out
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Salaried-elsewhere check failed for {venue_id}: {e}")
        return []


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
    - Superannuation at the Superannuation Guarantee rate
    - Allowances only from venue-configured rules (none otherwise)
    - Validation status

    Returns payroll batch ready for approval and export.
    """
    enforce_venue_manager(request.venue_id)
    try:
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

        # The plan staff were given: each week's published roster, else its
        # imported one. Never a draft, a superseded roster, or two at once.
        shifts = live_shifts(db, request.venue_id, request.period_start,
                             request.period_end, staff=True)

        if not shifts:
            raise HTTPException(
                status_code=400,
                detail=(f"No published or imported roster shifts for period "
                        f"{request.period_start} to {request.period_end}"),
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
            leave_requests=db.list_leave_requests(request.venue_id),
            rostered=True,
        )
        batch.notes.extend(_salaried_elsewhere(db, request.venue_id, batch))

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


def _overlap_notes(sheets, employees, tz) -> List[str]:
    """One note per person whose approved punches overlap: the same time is in
    the batch twice (a double tap, or a correction onto another shift)."""
    def _hhmm(v):
        return _parse_dt(v).replace(tzinfo=timezone.utc).astimezone(tz).strftime("%H:%M")

    overlapping = _overlapping_ids(sheets)
    by_emp: dict = {}
    for t in sheets:
        if t.get("id") in overlapping:
            by_emp.setdefault(t.get("employee_id"), []).append(t)
    out = []
    for emp_id, rows in by_emp.items():
        name = getattr(employees.get(emp_id), "name", emp_id)
        spans = ", ".join(f"{t.get('work_date')} {_hhmm(t['clock_in'])}-{_hhmm(t['clock_out'])}"
                          for t in sorted(rows, key=lambda r: str(r.get("clock_in"))))
        out.append(f"{name}: approved punches overlap ({spans}) — the same time is priced more "
                   "than once in this batch; check them before using these hours")
    return out


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
    enforce_venue_manager(request.venue_id)
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

    # Punches are stored as naive UTC (timeclock uses datetime.utcnow()), while
    # the true local day is stored alongside as work_date. Pricing a shift needs
    # VENUE-LOCAL wall clock: a Perth Sunday 07:00-15:00 is Saturday 23:00 UTC,
    # so using the UTC stamps paid it at Saturday rates — and its UTC date could
    # fall outside the pay period and drop the shift from the run entirely.
    tz = ZoneInfo(venue_timezone(request.venue_id, db))

    def _to_local(dt: datetime) -> datetime:
        return dt.replace(tzinfo=timezone.utc).astimezone(tz)

    too_long = []
    for t in sheets:
        span = _span_minutes(t)
        if span is not None and span >= MAX_SPAN_MINUTES:
            emp_name = getattr(employees.get(t.get("employee_id")), "name", t.get("employee_id"))
            too_long.append(f"{emp_name} on {t.get('work_date')} ({span // 60}h {span % 60:02d}m)")
    if too_long:
        raise HTTPException(
            status_code=409,
            detail="These timesheets run 24 hours or more, so they can't be priced as one "
                   "shift — check their dates: " + "; ".join(sorted(too_long)))

    # The Timesheets page's arithmetic: whole minutes of the punch's span from
    # the clock-in's minute, so payroll hours equal what the manager approved.
    shifts = []
    for t in sheets:
        local_in = _to_local(_parse_dt(t["clock_in"])).replace(second=0, microsecond=0)
        local_out = local_in + timedelta(minutes=_span_minutes(t))
        work_date = t.get("work_date")
        if isinstance(work_date, str):
            work_date = date.fromisoformat(work_date[:10])
        shifts.append(Shift(
            id=str(t.get("id")),
            employee_id=str(t.get("employee_id")),
            # work_date is what the venue calls that shift's day (set from the
            # venue clock at clock-in); fall back to the localised punch.
            date=work_date or local_in.date(),
            start_time=local_in.time(),
            end_time=local_out.time(),
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
        leave_requests=db.list_leave_requests(request.venue_id),
    )
    batch.notes.extend(_overlap_notes(sheets, employees, tz))
    batch.notes.extend(_salaried_elsewhere(db, request.venue_id, batch))
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
        cross_venue_hours=_cross_venue_hours(db, request.venue_id, request.period_start,
                                             request.period_end),
    )


@router.get("/batch/{batch_id}/csv")
async def payroll_batch_csv(batch_id: str, venue_id: str = Query(...)):
    """The accountant's file: one row per employee with hours and gross
    broken down, plus a totals row. Opens straight into Excel or imports
    into Xero as a payroll worksheet."""
    enforce_venue_manager(venue_id)
    db = get_db()
    batch = db.get_payroll_batch(batch_id)
    if not batch or batch.get("venue_id") != venue_id:
        raise HTTPException(status_code=404, detail="Payroll batch not found")

    # A batch saved before the rate was stored doesn't say what it used.
    super_header = (f"Super ({super_rate_label(Decimal(batch['super_rate']))})"
                    if batch.get("super_rate") else "Super")

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
                super_header, "Total incl super",
                "Pay basis", "Salary (fixed line)", "Hours worked (salaried, not priced)",
                "Paid leave hours (advisor to price)"])
    for e in batch.get("employees", []):
        penalty_detail = "; ".join(
            f"{p['penalty_type']} {p['hours']}h x{p['multiplier']}"
            for p in e.get("penalty_entries", []))
        # Every allowance dollar is NAMED — an accountant should never have to
        # guess what an allowance line is.
        allowance_detail = "; ".join(
            f"{a['allowance_type']} ${a['amount']}"
            for a in e.get("allowances", []))
        salaried = e.get("pay_basis") == "salaried"
        leave_detail = "; ".join(
            f"{le['leave_type']} {le['hours']}h" + (f" ({le['note']})" if le.get("note") else "")
            for le in e.get("leave_entries", []))
        w.writerow([
            e.get("name"), e.get("ordinary_hours"), e.get("ordinary_rate"),
            e.get("ordinary_gross"), penalty_detail, e.get("penalty_gross"),
            e.get("overtime_hours"), e.get("overtime_amount"),
            allowance_detail, e.get("allowances_total"), e.get("total_gross"),
            e.get("super_amount"), e.get("total_with_super"),
            e.get("pay_basis") or "hourly",
            e.get("salary_amount") if salaried else "",
            e.get("worked_hours") if salaried else "",
            leave_detail,
        ])
    w.writerow([])
    w.writerow(["TOTALS", batch.get("total_ordinary_hours"), "",
                batch.get("total_ordinary_gross"), "", batch.get("total_penalty_gross"),
                batch.get("total_overtime_hours"), batch.get("total_overtime_gross"),
                "", batch.get("total_allowances"), batch.get("total_gross"),
                batch.get("total_super"), "", "", batch.get("total_salary", ""), "", ""])
    # What the prep found for the advisor: a leaver to prorate, unpaid leave,
    # a day on paid leave left unpriced, overlapping punches.
    notes = batch.get("validation_errors") or []
    if notes:
        w.writerow([])
        for note in notes:
            w.writerow(["Note", note])

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
        enforce_venue_manager(batch_dict.get("venue_id"))

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
        enforce_venue_manager(batch_dict.get("venue_id"))  # dual-control gate

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
        enforce_venue_manager(batch_dict.get("venue_id"))  # real pay run -> manager

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
        enforce_venue_manager(batch_dict.get("venue_id"))  # real pay run -> manager

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
        enforce_venue_manager(venue_id)
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
    enforce_venue_manager(venue_id)
    try:
        # Mock implementation
        return []

    except Exception as e:
        logger.error(f"Error fetching export history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
