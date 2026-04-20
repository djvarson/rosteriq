"""
Close-of-Day REST API endpoints for RosterIQ.

Provides REST interface for:
- Submitting close-of-day records
- Retrieving CoD history and details
- Manager sign-off workflow
- Period summaries and anomaly detection

All endpoints require auth gating (L1_SUPERVISOR minimum for read,
L2_ROSTER_MAKER for write/sign-off).
"""

from datetime import date, datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.close_of_day import (
    CloseOfDay, CoDSummary, PaymentMethod, RevenueBreakdown, TillCount,
    TillStatus, SignOffStatus, create_close_of_day, sign_off, query_cod,
    build_cod_summary, get_discrepancy_trend, flag_anomalies,
    get_store,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(prefix="/api/v1/cod", tags=["close-of-day"])


# ============================================================================
# Request/Response Models
# ============================================================================

class TillCountInput(BaseModel):
    """Till count input for CoD submission."""
    till_id: str = Field(..., description="Till identifier")
    counted_amount: float = Field(..., description="Physical cash count in AUD")
    expected_amount: float = Field(..., description="Expected amount from POS")
    counted_by: str = Field(..., description="Employee ID of counter")
    counted_at: str = Field(..., description="ISO 8601 datetime of count")
    notes: str = Field("", description="Optional notes about count")


class RevenueBreakdownInput(BaseModel):
    """Revenue breakdown input."""
    payment_method: str = Field(..., description="Payment method (cash, card, eftpos, online, voucher, other)")
    amount: float = Field(..., description="Amount in AUD")
    transaction_count: int = Field(0, description="Number of transactions")


class CloseOfDayInput(BaseModel):
    """Request body for POST /api/v1/cod/"""
    venue_id: str = Field(..., description="Venue identifier")
    trading_date: str = Field(..., description="Trading date (ISO 8601)")
    closed_by: str = Field(..., description="Employee ID closing the till")
    closed_by_name: str = Field(..., description="Full name of closer")
    pos_total: float = Field(..., description="Total POS revenue")
    till_counts: List[TillCountInput] = Field(..., description="List of till counts")
    revenue_breakdown: List[RevenueBreakdownInput] = Field(..., description="Revenue breakdown by method")
    labour_cost: float = Field(0.0, description="Labour cost for the day")
    covers: int = Field(0, description="Number of customer covers")
    notes: str = Field("", description="Additional notes")


class TillCountResponse(BaseModel):
    """Till count in response."""
    till_id: str
    counted_amount: float
    expected_amount: float
    variance: float
    status: str
    counted_by: str
    counted_at: str
    notes: str


class RevenueBreakdownResponse(BaseModel):
    """Revenue breakdown in response."""
    payment_method: str
    amount: float
    transaction_count: int


class CloseOfDayResponse(BaseModel):
    """Response for a single CoD record."""
    cod_id: str
    venue_id: str
    trading_date: str
    closed_by: str
    closed_by_name: str
    closed_at: str
    pos_total: float
    till_counts: List[TillCountResponse]
    revenue_breakdown: List[RevenueBreakdownResponse]
    total_revenue: float
    total_variance: float
    labour_cost: float
    labour_pct: float
    covers: int
    average_spend: float
    sign_off_status: str
    signed_off_by: Optional[str]
    signed_off_at: Optional[str]
    notes: str


class CloseOfDayListResponse(BaseModel):
    """Response for listing CoD records."""
    records: List[CloseOfDayResponse]
    total: int


class CoDSummaryResponse(BaseModel):
    """Response for period summary."""
    venue_id: str
    period_start: str
    period_end: str
    trading_days: int
    total_revenue: float
    avg_daily_revenue: float
    total_variance: float
    variance_pct: float
    avg_labour_pct: float
    total_covers: int
    avg_spend: float
    days_with_discrepancies: int
    best_day: Optional[Dict[str, Any]]
    worst_day: Optional[Dict[str, Any]]


class AnomalyResponse(BaseModel):
    """Response for a flagged anomaly."""
    date: str
    variance: float
    variance_pct: float
    reason: str
    cod_id: str


class AnomaliesListResponse(BaseModel):
    """Response for listing anomalies."""
    anomalies: List[AnomalyResponse]
    total: int


class SignOffRequest(BaseModel):
    """Request body for POST /api/v1/cod/{cod_id}/sign-off"""
    signed_off_by: str = Field(..., description="Employee ID of manager signing off")


class QueryRequest(BaseModel):
    """Request body for POST /api/v1/cod/{cod_id}/query"""
    queried_by: str = Field(..., description="Employee ID of manager querying")


# ============================================================================
# Helper Functions
# ============================================================================

def _cod_to_response(cod: CloseOfDay) -> CloseOfDayResponse:
    """Convert CloseOfDay to response model."""
    return CloseOfDayResponse(
        cod_id=cod.cod_id,
        venue_id=cod.venue_id,
        trading_date=cod.trading_date.isoformat(),
        closed_by=cod.closed_by,
        closed_by_name=cod.closed_by_name,
        closed_at=cod.closed_at.isoformat(),
        pos_total=cod.pos_total,
        till_counts=[
            TillCountResponse(
                till_id=t.till_id,
                counted_amount=t.counted_amount,
                expected_amount=t.expected_amount,
                variance=t.variance,
                status=t.status.value,
                counted_by=t.counted_by,
                counted_at=t.counted_at.isoformat(),
                notes=t.notes,
            )
            for t in cod.till_counts
        ],
        revenue_breakdown=[
            RevenueBreakdownResponse(
                payment_method=r.payment_method.value,
                amount=r.amount,
                transaction_count=r.transaction_count,
            )
            for r in cod.revenue_breakdown
        ],
        total_revenue=cod.total_revenue,
        total_variance=cod.total_variance,
        labour_cost=cod.labour_cost,
        labour_pct=cod.labour_pct,
        covers=cod.covers,
        average_spend=cod.average_spend,
        sign_off_status=cod.sign_off_status.value,
        signed_off_by=cod.signed_off_by,
        signed_off_at=cod.signed_off_at.isoformat() if cod.signed_off_at else None,
        notes=cod.notes,
    )


def _summary_to_response(summary: CoDSummary) -> CoDSummaryResponse:
    """Convert CoDSummary to response model."""
    return CoDSummaryResponse(
        venue_id=summary.venue_id,
        period_start=summary.period_start.isoformat(),
        period_end=summary.period_end.isoformat(),
        trading_days=summary.trading_days,
        total_revenue=summary.total_revenue,
        avg_daily_revenue=summary.avg_daily_revenue,
        total_variance=summary.total_variance,
        variance_pct=summary.variance_pct,
        avg_labour_pct=summary.avg_labour_pct,
        total_covers=summary.total_covers,
        avg_spend=summary.avg_spend,
        days_with_discrepancies=summary.days_with_discrepancies,
        best_day=summary.best_day,
        worst_day=summary.worst_day,
    )


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/", response_model=CloseOfDayResponse)
async def submit_close_of_day(body: CloseOfDayInput, request: Request):
    """
    Submit a new close-of-day record (L1+).

    Validates revenue breakdown, calculates variances, and persists to
    SQLite. Returns the created record with till status classifications.
    """
    await _gate(request, "L1_SUPERVISOR")

    try:
        trading_date = date.fromisoformat(body.trading_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid trading_date format")

    # Parse till counts
    till_counts = []
    for tc in body.till_counts:
        try:
            counted_at = datetime.fromisoformat(tc.counted_at)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid counted_at format in till {tc.till_id}")

        till_counts.append(
            TillCount(
                till_id=tc.till_id,
                counted_amount=tc.counted_amount,
                expected_amount=tc.expected_amount,
                counted_by=tc.counted_by,
                counted_at=counted_at,
                notes=tc.notes,
            )
        )

    # Parse revenue breakdown
    revenue_breakdown = []
    for rb in body.revenue_breakdown:
        try:
            pm = PaymentMethod(rb.payment_method)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid payment_method: {rb.payment_method}"
            )
        revenue_breakdown.append(
            RevenueBreakdown(
                payment_method=pm,
                amount=rb.amount,
                transaction_count=rb.transaction_count,
            )
        )

    # Create the CoD record
    cod = create_close_of_day(
        venue_id=body.venue_id,
        trading_date=trading_date,
        closed_by=body.closed_by,
        closed_by_name=body.closed_by_name,
        pos_total=body.pos_total,
        till_counts=till_counts,
        revenue_breakdown=revenue_breakdown,
        labour_cost=body.labour_cost,
        covers=body.covers,
        notes=body.notes,
    )

    return _cod_to_response(cod)


@router.get("/{venue_id}", response_model=CloseOfDayListResponse)
async def list_close_of_day(
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    request: Request = None,
):
    """
    List close-of-day records for a venue (L1+).

    Supports filtering by date range and sign-off status.
    """
    await _gate(request, "L1_SUPERVISOR")

    # Parse dates
    if date_from:
        try:
            df = date.fromisoformat(date_from)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date_from format")
    else:
        df = date(2000, 1, 1)

    if date_to:
        try:
            dt = date.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date_to format")
    else:
        dt = date.today()

    # Validate status if provided
    if status:
        try:
            SignOffStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    records = get_store().get_by_venue_and_date_range(venue_id, df, dt, status)

    return CloseOfDayListResponse(
        records=[_cod_to_response(r) for r in records],
        total=len(records),
    )


@router.get("/{venue_id}/{cod_id}", response_model=CloseOfDayResponse)
async def get_close_of_day(venue_id: str, cod_id: str, request: Request = None):
    """
    Retrieve a single close-of-day record (L1+).
    """
    await _gate(request, "L1_SUPERVISOR")

    cod = get_store().get_by_id(cod_id)
    if not cod or cod.venue_id != venue_id:
        raise HTTPException(status_code=404, detail="Close-of-day record not found")

    return _cod_to_response(cod)


@router.post("/{cod_id}/sign-off", response_model=CloseOfDayResponse)
async def sign_off_close_of_day(cod_id: str, body: SignOffRequest, request: Request = None):
    """
    Manager sign-off on a close-of-day record (L2+).

    Marks the record as SIGNED_OFF by the manager.
    """
    await _gate(request, "L2_ROSTER_MAKER")

    cod = get_store().get_by_id(cod_id)
    if not cod:
        raise HTTPException(status_code=404, detail="Close-of-day record not found")

    updated = sign_off(cod_id, body.signed_off_by)
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to sign off record")

    return _cod_to_response(updated)


@router.post("/{cod_id}/query", response_model=CloseOfDayResponse)
async def query_close_of_day(cod_id: str, body: QueryRequest, request: Request = None):
    """
    Manager queries/reopens a close-of-day record (L2+).

    Marks the record as QUERIED for investigation.
    """
    await _gate(request, "L2_ROSTER_MAKER")

    cod = get_store().get_by_id(cod_id)
    if not cod:
        raise HTTPException(status_code=404, detail="Close-of-day record not found")

    updated = query_cod(cod_id, body.queried_by)
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to query record")

    return _cod_to_response(updated)


@router.get("/{venue_id}/summary", response_model=CoDSummaryResponse)
async def get_cod_summary(
    venue_id: str,
    period_start: str,
    period_end: str,
    request: Request = None,
):
    """
    Get period summary for a venue (L2+).

    Returns aggregated metrics: revenue, variance, labour %, covers,
    best/worst days, days with discrepancies.
    """
    await _gate(request, "L2_ROSTER_MAKER")

    try:
        ps = date.fromisoformat(period_start)
        pe = date.fromisoformat(period_end)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    records = get_store().get_by_venue_and_date_range(venue_id, ps, pe)
    summary = build_cod_summary(venue_id, records, ps, pe)

    return _summary_to_response(summary)


@router.get("/{venue_id}/anomalies", response_model=AnomaliesListResponse)
async def get_anomalies(
    venue_id: str,
    period_start: str,
    period_end: str,
    threshold_pct: float = 2.0,
    request: Request = None,
):
    """
    Get flagged anomalies for a venue (L2+).

    Returns days where variance exceeds threshold_pct (default 2%).
    """
    await _gate(request, "L2_ROSTER_MAKER")

    try:
        ps = date.fromisoformat(period_start)
        pe = date.fromisoformat(period_end)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    records = get_store().get_by_venue_and_date_range(venue_id, ps, pe)
    anomalies = flag_anomalies(records, threshold_pct)

    return AnomaliesListResponse(
        anomalies=[AnomalyResponse(**a) for a in anomalies],
        total=len(anomalies),
    )
