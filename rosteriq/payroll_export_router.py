"""FastAPI router for payroll export endpoints.

Provides REST API for managing and exporting payroll data:
- POST /generate/{venue_id} (L2+) — generate payroll for period
- GET /{venue_id} (L1+) — list payroll records
- GET /employee/{venue_id}/{employee_id} (L1+) — employee payroll history
- POST /approve/{record_id} (OWNER) — approve payroll record
- GET /export/xero/{venue_id} (L2+) — export Xero CSV
- GET /export/myob/{venue_id} (L2+) — export MYOB CSV
- GET /export/keypay/{venue_id} (L2+) — export KeyPay JSON
- GET /summary/{venue_id} (L1+) — payroll summary
- POST /{record_id}/allowance (L2+) — add allowance
- POST /{record_id}/deduction (L2+) — add deduction
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

# Lazy imports for optional deps
try:
    from fastapi import APIRouter, HTTPException, Request, Query
    from pydantic import BaseModel, Field
    FASTAPI_AVAILABLE = True
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    Query = None
    BaseModel = object
    Field = None
    FASTAPI_AVAILABLE = False

from rosteriq.payroll_export import (
    get_payroll_export_store,
    PayrollRecord,
    PeriodType,
    PayrollStatus,
)

logger = logging.getLogger("rosteriq.payroll_export_router")

# Auth gating - fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic Models (only if FastAPI is available)
# ---------------------------------------------------------------------------

if FASTAPI_AVAILABLE:
    class ShiftData(BaseModel):
        """Shift data for payroll generation."""
        employee_id: str = Field(..., description="Employee ID")
        employee_name: str = Field(..., description="Employee name")
        date: str = Field(..., description="ISO date YYYY-MM-DD")
        start_time: str = Field(..., description="HH:MM format")
        end_time: str = Field(..., description="HH:MM format")
        base_rate: float = Field(..., description="Hourly rate in AUD")
        is_public_holiday: bool = Field(False, description="Is this a public holiday?")

    class GeneratePayrollRequest(BaseModel):
        """Request to generate payroll."""
        period_start: str = Field(..., description="ISO date YYYY-MM-DD")
        period_end: str = Field(..., description="ISO date YYYY-MM-DD")
        period_type: str = Field(..., description="weekly, fortnightly, or monthly")
        shifts_data: List[ShiftData] = Field(..., description="Shift data for all employees")

    class PayrollRecordResponse(BaseModel):
        """Response containing a payroll record."""
        id: str
        venue_id: str
        employee_id: str
        employee_name: str
        period_start: str
        period_end: str
        period_type: str
        ordinary_hours: float
        saturday_hours: float
        sunday_hours: float
        public_holiday_hours: float
        evening_hours: float
        overtime_hours: float
        base_rate: float
        gross_pay: float
        super_amount: float
        status: str
        notes: Optional[str]

    class AllowanceRequest(BaseModel):
        """Request to add an allowance."""
        name: str = Field(..., description="Allowance name")
        amount: float = Field(..., description="Amount in AUD")

    class DeductionRequest(BaseModel):
        """Request to add a deduction."""
        name: str = Field(..., description="Deduction name")
        amount: float = Field(..., description="Amount in AUD")

    class PayrollSummaryResponse(BaseModel):
        """Payroll summary for a period."""
        venue_id: str
        period_start: str
        period_end: str
        employee_count: int
        total_hours: float
        total_gross_pay: float
        total_superannuation: float
        total_allowances: float
        total_deductions: float
        net_pay: float


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

if FASTAPI_AVAILABLE:
    router = APIRouter(prefix="/api/v1/payroll", tags=["payroll"])

    @router.post("/generate/{venue_id}", response_model=List[PayrollRecordResponse])
    async def generate_payroll(
        venue_id: str,
        request: Request,
        body: GeneratePayrollRequest,
    ):
        """Generate payroll records for a venue and period (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()

            # Convert ShiftData to dicts for store
            shifts_data = [
                {
                    "employee_id": s.employee_id,
                    "employee_name": s.employee_name,
                    "date": s.date,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "base_rate": s.base_rate,
                    "is_public_holiday": s.is_public_holiday,
                }
                for s in body.shifts_data
            ]

            records = store.generate_payroll(
                venue_id=venue_id,
                period_start=body.period_start,
                period_end=body.period_end,
                period_type=body.period_type,
                shifts_data=shifts_data,
            )

            return [PayrollRecordResponse(**r.to_dict()) for r in records]

        except Exception as e:
            logger.error("Failed to generate payroll: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/{venue_id}", response_model=List[PayrollRecordResponse])
    async def list_payroll(
        venue_id: str,
        request: Request,
        period_start: Optional[str] = Query(None),
        period_end: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
    ):
        """List payroll records for a venue (L1+)."""
        await _gate(request, "L1")

        try:
            store = get_payroll_export_store()
            records = store.get_payroll_records(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
                status=status,
            )
            return [PayrollRecordResponse(**r.to_dict()) for r in records]

        except Exception as e:
            logger.error("Failed to list payroll records: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/employee/{venue_id}/{employee_id}", response_model=List[PayrollRecordResponse])
    async def get_employee_payroll(
        venue_id: str,
        employee_id: str,
        request: Request,
        period_start: Optional[str] = Query(None),
    ):
        """Get payroll history for an employee (L1+)."""
        await _gate(request, "L1")

        try:
            store = get_payroll_export_store()
            records = store.get_employee_payroll(
                venue_id=venue_id,
                employee_id=employee_id,
                period_start=period_start,
            )
            return [PayrollRecordResponse(**r.to_dict()) for r in records]

        except Exception as e:
            logger.error("Failed to get employee payroll: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/approve/{record_id}", response_model=PayrollRecordResponse)
    async def approve_payroll(
        record_id: str,
        request: Request,
    ):
        """Approve a payroll record (OWNER)."""
        await _gate(request, "OWNER")

        try:
            store = get_payroll_export_store()
            record = store.approve_payroll(record_id)
            return PayrollRecordResponse(**record.to_dict())

        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error("Failed to approve payroll: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/export/xero/{venue_id}")
    async def export_xero(
        venue_id: str,
        request: Request,
        period_start: str = Query(...),
        period_end: str = Query(...),
    ):
        """Export payroll as Xero CSV (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()
            csv_data = store.export_xero_csv(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
            )
            return {
                "format": "csv",
                "data": csv_data,
                "filename": f"payroll_xero_{venue_id}_{period_start}_to_{period_end}.csv",
            }

        except Exception as e:
            logger.error("Failed to export Xero CSV: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/export/myob/{venue_id}")
    async def export_myob(
        venue_id: str,
        request: Request,
        period_start: str = Query(...),
        period_end: str = Query(...),
    ):
        """Export payroll as MYOB CSV (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()
            csv_data = store.export_myob_csv(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
            )
            return {
                "format": "csv",
                "data": csv_data,
                "filename": f"payroll_myob_{venue_id}_{period_start}_to_{period_end}.csv",
            }

        except Exception as e:
            logger.error("Failed to export MYOB CSV: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/export/keypay/{venue_id}")
    async def export_keypay(
        venue_id: str,
        request: Request,
        period_start: str = Query(...),
        period_end: str = Query(...),
    ):
        """Export payroll as KeyPay JSON (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()
            json_data = store.export_keypay_json(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
            )
            return json_data

        except Exception as e:
            logger.error("Failed to export KeyPay JSON: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/summary/{venue_id}", response_model=PayrollSummaryResponse)
    async def get_summary(
        venue_id: str,
        request: Request,
        period_start: str = Query(...),
        period_end: str = Query(...),
    ):
        """Get payroll summary for a venue and period (L1+)."""
        await _gate(request, "L1")

        try:
            store = get_payroll_export_store()
            summary = store.get_payroll_summary(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
            )
            return PayrollSummaryResponse(**summary)

        except Exception as e:
            logger.error("Failed to get payroll summary: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/{record_id}/allowance", response_model=PayrollRecordResponse)
    async def add_allowance(
        record_id: str,
        request: Request,
        body: AllowanceRequest,
    ):
        """Add an allowance to a payroll record (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()
            record = store.add_allowance(
                record_id=record_id,
                name=body.name,
                amount=body.amount,
            )
            return PayrollRecordResponse(**record.to_dict())

        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error("Failed to add allowance: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/{record_id}/deduction", response_model=PayrollRecordResponse)
    async def add_deduction(
        record_id: str,
        request: Request,
        body: DeductionRequest,
    ):
        """Add a deduction to a payroll record (L2+)."""
        await _gate(request, "L2")

        try:
            store = get_payroll_export_store()
            record = store.add_deduction(
                record_id=record_id,
                name=body.name,
                amount=body.amount,
            )
            return PayrollRecordResponse(**record.to_dict())

        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error("Failed to add deduction: %s", e)
            raise HTTPException(status_code=500, detail=str(e))
else:
    # Dummy router when FastAPI not available
    router = None
