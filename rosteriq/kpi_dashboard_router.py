"""FastAPI router for KPI Dashboard endpoints (Round 53).

Provides REST API for KPI metrics and dashboard data:
- POST /snapshot/{venue_id} — record daily KPIs (L2+)
- GET /current/{venue_id} — latest + trends (L1+)
- GET /history/{venue_id} — historical (L1+)
- GET /weekly/{venue_id} — weekly agg (L1+)
- GET /monthly/{venue_id} — monthly agg (L1+)
- GET /compare/{venue_id} — period comparison (L1+)
- GET /alerts/{venue_id} — alerts (L1+)
- GET /ranking — venue ranking (OWNER)
- POST /targets/{venue_id} — set targets (L2+)
- GET /progress/{venue_id} — target progress (L1+)
"""

from __future__ import annotations

import logging
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    APIRouter = None
    HTTPException = None
    Request = None
    BaseModel = object
    def Field(*args, **kwargs):
        return None

from rosteriq.kpi_dashboard import (
    get_kpi_dashboard_store,
    KPIPeriod,
    KPISnapshot,
)

logger = logging.getLogger("rosteriq.kpi_dashboard_router")

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
# Pydantic Request/Response Models
# ---------------------------------------------------------------------------

if _FASTAPI_AVAILABLE:
    class RecordDailyKPIsRequest(BaseModel):
        """Request to record daily KPI snapshot."""
        date: str = Field(..., description="ISO date YYYY-MM-DD")
        revenue: float = Field(..., description="Daily revenue")
        labour_cost: float = Field(..., description="Daily labour cost")
        hours_worked: float = Field(..., description="Total labour hours")
        covers: int = Field(..., description="Number of covers served")
        shifts_scheduled: int = Field(..., description="Shifts scheduled")
        shifts_filled: int = Field(..., description="Shifts filled")
        no_shows: int = Field(..., description="No-show count")
        break_violations: int = Field(..., description="Break violation count")
        total_breaks: int = Field(..., description="Total breaks scheduled")

    class SetTargetsRequest(BaseModel):
        """Request to set KPI targets."""
        targets: Dict[str, float] = Field(..., description="Metric name -> target value")

    class ComparisonParams(BaseModel):
        """Parameters for period comparison."""
        date1: str = Field(..., description="First ISO date")
        date2: str = Field(..., description="Second ISO date")
        period: str = Field("DAILY", description="KPI period (DAILY/WEEKLY/MONTHLY)")

    class RankingParams(BaseModel):
        """Parameters for venue ranking."""
        venue_ids: List[str] = Field(..., description="Venue IDs to rank")
        date: str = Field(..., description="ISO date")
        metric_name: str = Field(..., description="Metric to rank by")

    class KPISnapshotResponse(BaseModel):
        """Response with KPI snapshot data."""
        id: str
        venue_id: str
        date: str
        period: str
        metrics: Dict[str, Any]
        created_at: str

    class AlertResponse(BaseModel):
        """Response with alert data."""
        alert_id: str
        venue_id: str
        date: str
        metric_name: str
        actual_value: float
        threshold_min: Optional[float]
        threshold_max: Optional[float]
        severity: str
        created_at: str
else:
    RecordDailyKPIsRequest = None
    SetTargetsRequest = None
    ComparisonParams = None
    RankingParams = None
    KPISnapshotResponse = None
    AlertResponse = None


# Only create router if FastAPI is available
if _FASTAPI_AVAILABLE:
    router = APIRouter(prefix="/api/v1/kpi", tags=["kpi-dashboard"])

    @router.post("/snapshot/{venue_id}", response_model=KPISnapshotResponse)
    async def record_daily_kpis(
        venue_id: str,
        request: Request,
        body: RecordDailyKPIsRequest,
    ) -> KPISnapshotResponse:
        """Record daily KPI snapshot for a venue.

        Requires L2+ access.
        """
        await _gate(request, "L2")

        store = get_kpi_dashboard_store()
        snapshot = store.calculate_daily_kpis(
            venue_id=venue_id,
            date_str=body.date,
            revenue=body.revenue,
            labour_cost=body.labour_cost,
            hours_worked=body.hours_worked,
            covers=body.covers,
            shifts_scheduled=body.shifts_scheduled,
            shifts_filled=body.shifts_filled,
            no_shows=body.no_shows,
            break_violations=body.break_violations,
            total_breaks=body.total_breaks,
        )

        return KPISnapshotResponse(**snapshot.to_dict())

    @router.get("/current/{venue_id}")
    async def get_current_kpis(
        venue_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get latest daily KPIs and 7-day trends.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        data = store.get_current_kpis(venue_id)

        if not data:
            raise HTTPException(status_code=404, detail="No KPI data for this venue")

        return data

    @router.get("/history/{venue_id}")
    async def get_history(
        venue_id: str,
        date_from: str,
        date_to: str,
        period: str = "DAILY",
        request: Request = None,
    ) -> Dict[str, Any]:
        """Get KPI history within date range.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        snapshots = store.get_snapshots(venue_id, date_from, date_to, period)

        return {
            "venue_id": venue_id,
            "date_from": date_from,
            "date_to": date_to,
            "period": period,
            "count": len(snapshots),
            "snapshots": [s.to_dict() for s in snapshots],
        }

    @router.get("/weekly/{venue_id}")
    async def get_weekly_kpis(
        venue_id: str,
        week_start: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get weekly aggregated KPIs.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        snapshot = store.calculate_weekly_kpis(venue_id, week_start)

        if not snapshot:
            raise HTTPException(status_code=404, detail="No data for this week")

        return snapshot.to_dict()

    @router.get("/monthly/{venue_id}")
    async def get_monthly_kpis(
        venue_id: str,
        year: int,
        month: int,
        request: Request,
    ) -> Dict[str, Any]:
        """Get monthly aggregated KPIs.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        snapshot = store.calculate_monthly_kpis(venue_id, year, month)

        if not snapshot:
            raise HTTPException(status_code=404, detail="No data for this month")

        return snapshot.to_dict()

    @router.get("/compare/{venue_id}")
    async def compare_periods(
        venue_id: str,
        date1: str,
        date2: str,
        period: str = "DAILY",
        request: Request = None,
    ) -> Dict[str, Any]:
        """Compare KPI metrics between two periods.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        comparison = store.compare_periods(venue_id, date1, date2, period)

        if not comparison:
            raise HTTPException(status_code=404, detail="No data for comparison")

        return comparison

    @router.get("/alerts/{venue_id}")
    async def get_alerts(
        venue_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get active KPI alerts for a venue.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        alerts = store.get_alerts(venue_id)

        return {
            "venue_id": venue_id,
            "count": len(alerts),
            "alerts": alerts,
        }

    @router.get("/ranking")
    async def get_ranking(
        venue_ids: List[str],
        date: str,
        metric_name: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Rank venues by metric on a date.

        Requires OWNER access.
        """
        await _gate(request, "OWNER")

        store = get_kpi_dashboard_store()
        ranking = store.get_venue_ranking(venue_ids, date, metric_name)

        return {
            "metric": metric_name,
            "date": date,
            "count": len(ranking),
            "ranking": ranking,
        }

    @router.post("/targets/{venue_id}")
    async def set_targets(
        venue_id: str,
        request: Request,
        body: SetTargetsRequest,
    ) -> Dict[str, Any]:
        """Set KPI targets for a venue.

        Requires L2+ access.
        """
        await _gate(request, "L2")

        store = get_kpi_dashboard_store()
        targets = store.set_targets(venue_id, body.targets)

        return {
            "venue_id": venue_id,
            "targets": targets,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    @router.get("/progress/{venue_id}")
    async def get_target_progress(
        venue_id: str,
        date: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get actual vs target metrics.

        Requires L1+ access.
        """
        await _gate(request, "L1")

        store = get_kpi_dashboard_store()
        progress = store.get_target_progress(venue_id, date)

        if not progress:
            raise HTTPException(status_code=404, detail="No target or snapshot data")

        return progress

else:
    router = None
