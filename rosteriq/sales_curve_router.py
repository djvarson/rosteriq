"""FastAPI router for POS Sales Curve Forecaster."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.sales_curve_router")

try:
    from fastapi import APIRouter, HTTPException, Request, Query
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    Query = None
    BaseModel = object
    Field = lambda *a, **kw: None  # noqa: E731

router = None

if APIRouter is not None:
    router = APIRouter()

    def _gate(request, level_name):
        try:
            from rosteriq.auth import require_access
            require_access(request, level_name)
        except Exception:
            pass

    # Request models

    class HourlyDataPoint(BaseModel):
        hour: int
        revenue: float = 0
        transaction_count: int = 0
        covers: int = 0

    class IngestDailyRequest(BaseModel):
        date: str
        hourly_data: List[HourlyDataPoint]
        source: str = "pos"

    class BulkIngestRequest(BaseModel):
        records: List[Dict[str, Any]]

    class StaffingTargetsRequest(BaseModel):
        revenue_per_staff_hour: float = 300.0
        min_staff: int = 2
        max_staff: int = 15
        covers_per_staff_hour: float = 8.0

    class StaffingPlanRequest(BaseModel):
        date: str
        open_hour: int = 6
        close_hour: int = 23

    # Endpoints

    @router.post("/ingest/{venue_id}")
    async def ingest_daily(request: Request, venue_id: str,
                           body: IngestDailyRequest):
        """Ingest a full day of POS hourly data (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        hourly = [{"hour": h.hour, "revenue": h.revenue,
                    "transaction_count": h.transaction_count,
                    "covers": h.covers} for h in body.hourly_data]
        count = store.ingest_daily_pos(venue_id, body.date, hourly, body.source)
        return {"venue_id": venue_id, "date": body.date,
                "records_ingested": count}

    @router.post("/ingest-bulk/{venue_id}")
    async def bulk_ingest(request: Request, venue_id: str,
                          body: BulkIngestRequest):
        """Bulk ingest hourly records (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        for rec in body.records:
            rec["venue_id"] = venue_id
        count = store.bulk_ingest(body.records)
        return {"venue_id": venue_id, "records_ingested": count}

    @router.get("/curve/{venue_id}/{day_of_week}")
    async def get_day_curve(request: Request, venue_id: str,
                            day_of_week: int, weeks_back: int = 8):
        """Get sales curve for a day of week (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        if day_of_week < 0 or day_of_week > 6:
            raise HTTPException(400, "day_of_week must be 0-6")
        curve = store.build_day_of_week_curve(venue_id, day_of_week, weeks_back)
        return curve.to_dict()

    @router.get("/curves/{venue_id}")
    async def get_weekly_curves(request: Request, venue_id: str,
                                weeks_back: int = 8):
        """Get sales curves for all 7 days (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        curves = store.build_weekly_curves(venue_id, weeks_back)
        return {"venue_id": venue_id,
                "curves": [c.to_dict() for c in curves]}

    @router.get("/curve-range/{venue_id}")
    async def get_custom_curve(request: Request, venue_id: str,
                               date_from: str = "", date_to: str = ""):
        """Get sales curve for a custom date range (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        curve = store.build_custom_curve(venue_id, date_from, date_to)
        return curve.to_dict()

    @router.post("/targets/{venue_id}")
    async def set_targets(request: Request, venue_id: str,
                          body: StaffingTargetsRequest):
        """Set venue staffing targets (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        targets = store.set_targets(venue_id, body.dict())
        return {"venue_id": venue_id, "targets": targets}

    @router.get("/targets/{venue_id}")
    async def get_targets(request: Request, venue_id: str):
        """Get venue staffing targets (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        return {"venue_id": venue_id,
                "targets": store.get_targets(venue_id)}

    @router.get("/recommend/{venue_id}/{target_date}")
    async def recommend_staffing(request: Request, venue_id: str,
                                 target_date: str):
        """Get hourly staffing recommendations for a date (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        recs = store.recommend_staffing(venue_id, target_date)
        return {"venue_id": venue_id, "date": target_date,
                "recommendations": [r.to_dict() for r in recs]}

    @router.post("/plan/{venue_id}")
    async def get_staffing_plan(request: Request, venue_id: str,
                                body: StaffingPlanRequest):
        """Get full day staffing plan (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        plan = store.get_daily_staffing_plan(
            venue_id, body.date, (body.open_hour, body.close_hour))
        return plan

    @router.get("/trend/{venue_id}")
    async def get_revenue_trend(request: Request, venue_id: str,
                                weeks: int = 8):
        """Get weekly revenue trend (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        return {"venue_id": venue_id,
                "trend": store.get_weekly_revenue_trend(venue_id, weeks)}

    @router.get("/daily/{venue_id}/{target_date}")
    async def get_daily_total(request: Request, venue_id: str,
                              target_date: str):
        """Get daily revenue total (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.sales_curve import get_sales_curve_store
        store = get_sales_curve_store()
        return store.get_daily_total(venue_id, target_date)
