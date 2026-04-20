"""REST API for Timesheet Reconciliation Engine (Round 30).

Endpoints:
- POST /api/v1/recon/reconcile — Run reconciliation on a day/period
- GET /api/v1/recon/{venue_id}/summary — Summary stats for a period
- GET /api/v1/recon/{venue_id}/shifts — Individual shift reconciliations
- GET /api/v1/recon/{venue_id}/patterns — Recurring issues and patterns
- GET /api/v1/recon/{venue_id}/no-shows — No-show report

Access control: L1+ for reads, L2+ for writes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Query, Request
    from pydantic import BaseModel, Field
except ImportError:
    # Sandbox mode
    APIRouter = None
    HTTPException = None
    Query = None
    BaseModel = None
    Field = None
    Request = None

from rosteriq.timesheet_recon import (
    ShiftRecon,
    ReconSummary,
    ReconStatus,
    reconcile_day,
    build_recon_summary,
    detect_patterns,
    store as recon_store,
)

logger = logging.getLogger("rosteriq.timesheet_recon_router")


def _gate(request: Request, level_name: str) -> None:
    """Verify access level on request. Raises HTTPException if unauthorized."""
    try:
        from rosteriq.auth import require_access
        require_access(request, level_name)
    except ImportError:
        # Auth disabled in sandbox mode
        pass


# ============================================================================
# Router setup (only if FastAPI available)
# ============================================================================

if APIRouter is not None:

    # ────────────────────────────────────────────────────────────────────────
    # Request/Response Models
    # ────────────────────────────────────────────────────────────────────────

    class ShiftInput(BaseModel):
        """Input for a single rostered or actual shift."""
        employee_id: str
        employee_name: Optional[str] = None
        venue_id: Optional[str] = None
        shift_date: str  # YYYY-MM-DD
        start: Optional[str] = None  # HH:MM
        end: Optional[str] = None  # HH:MM
        hours: float
        hourly_rate: Optional[float] = 0.0

    class ReconcileRequest(BaseModel):
        """Request to reconcile a day or period."""
        venue_id: str
        shift_date: str  # YYYY-MM-DD
        rostered_shifts: List[ShiftInput]
        actual_shifts: List[ShiftInput]

    class ShiftReconResponse(BaseModel):
        """Response for a single shift reconciliation."""
        recon_id: str
        employee_id: str
        employee_name: str
        venue_id: str
        shift_date: str
        rostered_start: Optional[str]
        rostered_end: Optional[str]
        rostered_hours: float
        actual_start: Optional[str]
        actual_end: Optional[str]
        actual_hours: float
        variance_hours: float
        variance_pct: Optional[float]
        rostered_cost: Optional[float]
        actual_cost: Optional[float]
        cost_variance: Optional[float]
        status: str
        notes: Optional[str]
        created_at: str

    class ReconSummaryResponse(BaseModel):
        """Response for reconciliation summary."""
        summary_id: str
        venue_id: str
        period_start: str
        period_end: str
        total_rostered_hours: float
        total_actual_hours: float
        total_variance_hours: float
        total_rostered_cost: Optional[float]
        total_actual_cost: Optional[float]
        total_cost_variance: Optional[float]
        match_rate_pct: float
        no_show_count: int
        late_start_count: int
        early_finish_count: int
        over_roster_count: int
        under_roster_count: int
        unrostered_count: int
        shifts_reconciled: int
        created_at: str

    class ReconcileResponse(BaseModel):
        """Response from reconciliation operation."""
        venue_id: str
        shift_date: str
        shifts_reconciled: int
        statuses: Dict[str, int]
        summary: ReconSummaryResponse

    class PatternsResponse(BaseModel):
        """Response for pattern detection."""
        frequent_no_shows: Dict[str, Dict[str, Any]]
        frequent_late_starts: Dict[str, Dict[str, Any]]
        frequent_early_finishes: Dict[str, Dict[str, Any]]
        over_rostered_trend: Dict[str, Dict[str, Any]]
        under_rostered_trend: Dict[str, Dict[str, Any]]
        high_cost_variance: List[Dict[str, Any]]

    # ────────────────────────────────────────────────────────────────────────
    # Router definition
    # ────────────────────────────────────────────────────────────────────────

    router = APIRouter(prefix="/api/v1/recon", tags=["reconciliation"])

    @router.post("/reconcile", response_model=ReconcileResponse)
    def reconcile(request: Request, payload: ReconcileRequest) -> ReconcileResponse:
        """Reconcile a single day of shifts.

        Accepts rostered and actual shifts, matches by employee, and produces
        reconciliation records with variance analysis.

        Requires: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")

        store = recon_store()

        # Build shift dicts for reconciliation
        rostered_dicts = []
        for shift in payload.rostered_shifts:
            d = shift.dict()
            d["venue_id"] = payload.venue_id
            rostered_dicts.append(d)

        actual_dicts = []
        for shift in payload.actual_shifts:
            d = shift.dict()
            d["venue_id"] = payload.venue_id
            actual_dicts.append(d)

        # Reconcile the day
        recons = reconcile_day(
            payload.venue_id,
            payload.shift_date,
            rostered_dicts,
            actual_dicts,
        )

        # Persist each recon
        for recon in recons:
            store.persist_shift_recon(recon)

        # Build summary
        summary = build_recon_summary(
            recons,
            payload.venue_id,
            payload.shift_date,
            payload.shift_date,
        )
        store.persist_summary(summary)

        # Count statuses
        status_counts = {}
        for status in ReconStatus:
            status_counts[status.value] = sum(1 for r in recons if r.status == status)

        return ReconcileResponse(
            venue_id=payload.venue_id,
            shift_date=payload.shift_date,
            shifts_reconciled=len(recons),
            statuses=status_counts,
            summary=ReconSummaryResponse(**summary.to_dict()),
        )

    @router.get("/{venue_id}/summary", response_model=ReconSummaryResponse)
    def get_summary(
        request: Request,
        venue_id: str,
        date_from: str = Query(..., description="Period start (YYYY-MM-DD)"),
        date_to: str = Query(..., description="Period end (YYYY-MM-DD)"),
    ) -> ReconSummaryResponse:
        """Get reconciliation summary for a venue over a period.

        Aggregates all shift reconciliations into summary stats (hours, cost,
        variance, match rate, issue counts).

        Requires: L1_SUPERVISOR or higher
        """
        _gate(request, "L1_SUPERVISOR")

        store = recon_store()

        # Query recons for the period
        recons = store.query_recons(venue_id=venue_id)
        filtered = [
            r for r in recons
            if date_from <= r.shift_date <= date_to
        ]

        if not filtered:
            # Return empty summary if no data
            summary = build_recon_summary([], venue_id, date_from, date_to)
        else:
            summary = build_recon_summary(filtered, venue_id, date_from, date_to)

        store.persist_summary(summary)
        return ReconSummaryResponse(**summary.to_dict())

    @router.get("/{venue_id}/shifts", response_model=List[ShiftReconResponse])
    def get_shifts(
        request: Request,
        venue_id: str,
        status: Optional[str] = Query(None, description="Filter by status"),
        employee_id: Optional[str] = Query(None, description="Filter by employee"),
        shift_date: Optional[str] = Query(None, description="Filter by date"),
    ) -> List[ShiftReconResponse]:
        """Get individual shift reconciliations with optional filters.

        Requires: L1_SUPERVISOR or higher
        """
        _gate(request, "L1_SUPERVISOR")

        store = recon_store()

        # Convert status string to enum if provided
        status_enum = None
        if status:
            try:
                status_enum = ReconStatus(status)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status: {status}. Must be one of: {', '.join(s.value for s in ReconStatus)}",
                )

        recons = store.query_recons(
            venue_id=venue_id,
            employee_id=employee_id,
            shift_date=shift_date,
            status=status_enum,
        )

        return [ShiftReconResponse(**r.to_dict()) for r in recons]

    @router.get("/{venue_id}/patterns", response_model=PatternsResponse)
    def get_patterns(
        request: Request,
        venue_id: str,
        days: int = Query(30, description="Lookback period in days"),
    ) -> PatternsResponse:
        """Detect recurring issues and patterns in reconciliations.

        Identifies frequent no-shows, late arrivals, early finishes, and
        cost variance outliers.

        Requires: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")

        store = recon_store()

        # Get recons for the venue
        recons = store.query_recons(venue_id=venue_id)

        # Filter by days if needed (rough date filter)
        if days > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
            recons = [r for r in recons if r.shift_date >= cutoff]

        patterns = detect_patterns(recons)
        return PatternsResponse(**patterns)

    @router.get("/{venue_id}/no-shows", response_model=List[ShiftReconResponse])
    def get_no_shows(
        request: Request,
        venue_id: str,
        date_from: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
        date_to: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    ) -> List[ShiftReconResponse]:
        """Get no-show report for a venue.

        Lists all shifts where employee was rostered but did not clock in.

        Requires: L1_SUPERVISOR or higher
        """
        _gate(request, "L1_SUPERVISOR")

        store = recon_store()

        recons = store.query_recons(
            venue_id=venue_id,
            status=ReconStatus.NO_SHOW,
        )

        # Filter by date range if provided
        if date_from or date_to:
            if date_from:
                recons = [r for r in recons if r.shift_date >= date_from]
            if date_to:
                recons = [r for r in recons if r.shift_date <= date_to]

        return [ShiftReconResponse(**r.to_dict()) for r in recons]

else:
    # Fallback for sandbox mode (no FastAPI)
    router = None
