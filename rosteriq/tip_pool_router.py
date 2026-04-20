"""REST endpoints for tip pooling — mounted at /api/v1/tips."""
from __future__ import annotations

try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
    from typing import List, Optional
    router = APIRouter()
except ImportError:
    router = None

from datetime import date, datetime


def _gate(request, level):
    pass


if router:
    @router.post("/entry")
    async def add_entry(request: Request, body: dict):
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.tip_pool import add_tip_entry
        entry = add_tip_entry(
            venue_id=body["venue_id"],
            shift_date=date.fromisoformat(body["shift_date"]),
            amount=body["amount"],
            source=body.get("source", "cash"),
            entered_by=body.get("entered_by", "unknown"),
            notes=body.get("notes", ""),
        )
        return {"status": "ok", "entry": entry.to_dict()}

    @router.post("/pool")
    async def create_pool_endpoint(request: Request, body: dict):
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.tip_pool import create_pool, get_tip_pool_store, DistributionMethod
        store = get_tip_pool_store()
        entries = store.list_entries(
            body["venue_id"],
            date.fromisoformat(body["pool_date"]),
            date.fromisoformat(body["pool_date"]),
        )
        method = DistributionMethod(body.get("method", "hours_based"))
        pool = create_pool(body["venue_id"], date.fromisoformat(body["pool_date"]),
                          entries, method)
        return {"status": "ok", "pool": pool.to_dict()}

    @router.post("/pool/{pool_id}/distribute")
    async def distribute_endpoint(request: Request, pool_id: str, body: dict):
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.tip_pool import distribute_pool, DistributionMethod
        method = DistributionMethod(body["method"]) if "method" in body else None
        allocations = distribute_pool(pool_id, body["staff"], method,
                                      body.get("point_weights"))
        return {"status": "ok", "allocations": [a.to_dict() for a in allocations]}

    @router.post("/pool/{pool_id}/undo")
    async def undo_endpoint(request: Request, pool_id: str):
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.tip_pool import undo_distribution
        pool = undo_distribution(pool_id)
        return {"status": "ok", "pool": pool.to_dict()}

    @router.get("/{venue_id}/pools")
    async def list_pools(request: Request, venue_id: str,
                         date_from: Optional[str] = None, date_to: Optional[str] = None):
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.tip_pool import get_tip_pool_store
        store = get_tip_pool_store()
        df = date.fromisoformat(date_from) if date_from else None
        dt = date.fromisoformat(date_to) if date_to else None
        pools = store.list_pools(venue_id, df, dt)
        return {"pools": [p.to_dict() for p in pools]}

    @router.get("/{venue_id}/employee/{employee_id}")
    async def employee_tips(request: Request, venue_id: str, employee_id: str,
                            date_from: Optional[str] = None, date_to: Optional[str] = None):
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.tip_pool import get_employee_tips
        df = date.fromisoformat(date_from) if date_from else None
        dt = date.fromisoformat(date_to) if date_to else None
        allocs = get_employee_tips(employee_id, venue_id, df, dt)
        return {"allocations": [a.to_dict() for a in allocs]}

    @router.get("/{venue_id}/summary")
    async def tip_summary(request: Request, venue_id: str,
                          date_from: str = "2026-01-01", date_to: str = "2026-12-31"):
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.tip_pool import build_tip_summary
        summary = build_tip_summary(venue_id, date.fromisoformat(date_from),
                                    date.fromisoformat(date_to))
        return summary.to_dict()
