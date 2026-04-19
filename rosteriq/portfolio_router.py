"""Portfolio reporting router for RosterIQ.

Provides API endpoints for multi-venue portfolio reporting:
- GET /api/v1/portfolio?venue_ids=v1,v2,v3&days=7 (OWNER level) — consolidated report
- GET /api/v1/portfolio/{venue_id}/summary?days=7 (L1+) — single venue summary

Uses auth gating via the _gate pattern.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore

logger = logging.getLogger("rosteriq.portfolio_router")

router = APIRouter(prefix="/api/v1/portfolio", tags=["portfolio"])


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


@router.get("/")
async def get_portfolio(
    request: Request,
    venue_ids: Optional[str] = None,
    days: int = 7,
):
    """Get consolidated portfolio report across multiple venues.

    OWNER level access required.

    Query parameters:
    - venue_ids: comma-separated list of venue IDs (required)
    - days: number of days to report on (default: 7)

    Returns:
        {
            "period_days": int,
            "period_start": str,
            "period_end": str,
            "venue_count": int,
            "venues": [...],
            "totals": {...},
            "anomalies": [...],
            "rankings": {...}
        }
    """
    await _gate(request, "OWNER")

    if not venue_ids:
        raise HTTPException(status_code=400, detail="venue_ids parameter required")

    # Parse venue_ids
    parsed_ids = [vid.strip() for vid in venue_ids.split(",") if vid.strip()]
    if not parsed_ids:
        raise HTTPException(status_code=400, detail="venue_ids parameter required")

    try:
        from rosteriq.portfolio import build_portfolio_report

        report = build_portfolio_report(parsed_ids, days=days)
        return report
    except Exception as e:
        logger.error("portfolio report failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Portfolio report error: {str(e)}")


@router.get("/{venue_id}/summary")
async def get_venue_summary(
    request: Request,
    venue_id: str,
    days: int = 7,
):
    """Get performance summary for a single venue.

    L1 (Manager) level access required.

    Path parameters:
    - venue_id: venue identifier

    Query parameters:
    - days: number of days to report on (default: 7)

    Returns:
        {
            "venue_id": str,
            "period_days": int,
            "total_revenue": float,
            "total_labour_cost": float,
            "avg_labour_pct": float | None,
            "total_variance_hours": float,
            "days_over_forecast": int,
            "days_under_forecast": int,
            "busiest_day": str | None,
            "quietest_day": str | None,
        }
    """
    await _gate(request, "L1")

    try:
        from rosteriq.portfolio import build_venue_summary

        summary = build_venue_summary(venue_id, days=days)
        return summary
    except Exception as e:
        logger.error("venue summary failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Venue summary error: {str(e)}")
