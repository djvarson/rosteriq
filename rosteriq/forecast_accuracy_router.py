"""REST endpoint for forecast accuracy reporting (Round 13).

Endpoint:
  GET /api/v1/reports/forecast-accuracy?venue_id=&days=28
    L1+ access. Returns MAPE, bias, worst/best days, rolling trend,
    and per-day rows for the last `days` days.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

from rosteriq.forecast_accuracy import build_accuracy_report

# Auth gating — fall back to no-op in demo/sandbox when auth unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger("rosteriq.forecast_accuracy_router")
router = APIRouter(tags=["reports"])


@router.get("/api/v1/reports/forecast-accuracy")
async def get_forecast_accuracy(
    request: Request,
    venue_id: str = Query(...),
    days: int = Query(28, ge=1, le=365),
) -> dict:
    """Return a forecast-vs-actual accuracy digest for a venue."""
    await _gate(request, "L1_SUPERVISOR")
    try:
        return build_accuracy_report(venue_id=venue_id, days=days)
    except Exception as e:
        logger.error("accuracy report failed: %s", e)
        raise HTTPException(status_code=500, detail="accuracy report failed")
