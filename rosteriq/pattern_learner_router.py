"""REST endpoints for pattern detection and learning loop (Round 13).

Endpoints:
  POST /api/v1/patterns/{venue_id}/detect (L2+)
    Triggers pattern detection run, returns summary of new/updated patterns.

  GET /api/v1/patterns/{venue_id}?active_only=true (L1+)
    Returns learned patterns for a venue.

  GET /api/v1/patterns/{venue_id}/day/{day_of_week} (L1+)
    Returns patterns relevant to a specific day (0-6, Monday-Sunday).

  POST /api/v1/patterns/{venue_id}/{pattern_id}/deactivate (L2+)
    Manager can dismiss/deactivate a pattern.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from rosteriq.pattern_learner import get_pattern_store, run_detection

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger("rosteriq.pattern_learner_router")

router = APIRouter(tags=["patterns"])


@router.post("/api/v1/patterns/{venue_id}/detect")
async def detect_patterns(request: Request, venue_id: str) -> dict:
    """Trigger pattern detection run for a venue.

    L2+ (manager+). Analyzes accumulated shift notes, headcount, and
    history data to surface recurring patterns.
    """
    await _gate(request, "L2_MANAGER")
    if not venue_id:
        raise HTTPException(status_code=400, detail="venue_id is required")

    try:
        result = run_detection(venue_id, days=56)
        return result
    except Exception as e:
        logger.error("pattern detection failed for venue %s: %s", venue_id, e)
        raise HTTPException(status_code=500, detail="detection run failed")


@router.get("/api/v1/patterns/{venue_id}")
async def list_patterns(
    request: Request,
    venue_id: str,
    active_only: bool = Query(True),
) -> dict:
    """List learned patterns for a venue.

    L1+ (staff+). Returns active patterns by default; set active_only=false
    to include deactivated patterns.
    """
    await _gate(request, "L1_SUPERVISOR")
    if not venue_id:
        raise HTTPException(status_code=400, detail="venue_id is required")

    try:
        store = get_pattern_store()
        patterns = store.list_for_venue(venue_id, active_only=active_only)
        return {
            "venue_id": venue_id,
            "active_only": active_only,
            "count": len(patterns),
            "patterns": [p.to_dict() for p in patterns],
        }
    except Exception as e:
        logger.error("list patterns failed for venue %s: %s", venue_id, e)
        raise HTTPException(status_code=500, detail="list failed")


@router.get("/api/v1/patterns/{venue_id}/day/{day_of_week}")
async def get_patterns_for_day(
    request: Request,
    venue_id: str,
    day_of_week: int,
) -> dict:
    """Get patterns relevant to a specific day.

    L1+ (staff+). day_of_week is 0-6 (Monday-Sunday).
    Returns patterns specific to that day plus day-independent patterns.
    """
    await _gate(request, "L1_SUPERVISOR")
    if not venue_id:
        raise HTTPException(status_code=400, detail="venue_id is required")
    if not (0 <= day_of_week <= 6):
        raise HTTPException(status_code=400, detail="day_of_week must be 0-6")

    try:
        store = get_pattern_store()
        patterns = store.get_for_day(venue_id, day_of_week)
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return {
            "venue_id": venue_id,
            "day_of_week": day_of_week,
            "day_name": day_names[day_of_week],
            "count": len(patterns),
            "patterns": [p.to_dict() for p in patterns],
        }
    except Exception as e:
        logger.error("get patterns for day failed: %s", e)
        raise HTTPException(status_code=500, detail="query failed")


@router.post("/api/v1/patterns/{venue_id}/{pattern_id}/deactivate")
async def deactivate_pattern(
    request: Request,
    venue_id: str,
    pattern_id: str,
) -> dict:
    """Deactivate a pattern (dismiss it).

    L2+ (manager+). Once deactivated, the pattern won't be returned by
    active_only=true queries. It can be reactivated by running detection again.
    """
    await _gate(request, "L2_MANAGER")
    if not venue_id or not pattern_id:
        raise HTTPException(status_code=400, detail="venue_id and pattern_id required")

    try:
        store = get_pattern_store()
        pattern = store.get(pattern_id)
        if not pattern:
            raise HTTPException(status_code=404, detail="pattern not found")
        if pattern.venue_id != venue_id:
            raise HTTPException(status_code=403, detail="pattern does not belong to this venue")

        found = store.deactivate(pattern_id)
        if not found:
            raise HTTPException(status_code=404, detail="pattern not found")

        return {
            "status": "deactivated",
            "venue_id": venue_id,
            "pattern_id": pattern_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("deactivate pattern failed: %s", e)
        raise HTTPException(status_code=500, detail="deactivation failed")
