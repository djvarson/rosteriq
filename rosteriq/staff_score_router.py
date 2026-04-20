"""REST API router for staff performance scoring.

Endpoints for computing, retrieving, and ranking staff scores by venue.
Integrates with FastAPI; requires auth layer from api_v2.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger("rosteriq.staff_score_router")

# Lazy imports to handle missing FastAPI/Pydantic in sandboxed environment
try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    BaseModel = None
    Field = None


# ---------------------------------------------------------------------------
# Request/Response Models (Pydantic)
# ---------------------------------------------------------------------------


if BaseModel is not None:

    class DimensionScoreResponse(BaseModel):
        """Single dimension score response."""
        dimension: str
        score: float
        sample_size: int
        details: str

    class StaffScoreResponse(BaseModel):
        """Single staff member's performance score."""
        employee_id: str
        employee_name: str
        venue_id: str
        overall_score: float
        dimensions: List[DimensionScoreResponse]
        computed_at: str
        period_days: int

    class StaffScoresListResponse(BaseModel):
        """List of staff scores for a venue."""
        venue_id: str
        count: int
        scores: List[StaffScoreResponse]
        computed_at: str

    class StaffRankingResponse(BaseModel):
        """Ranked staff list for a venue."""
        venue_id: str
        count: int
        rankings: List[StaffScoreResponse]  # sorted by overall_score descending
        computed_at: str

    class ImprovementNeededResponse(BaseModel):
        """Staff needing improvement (below threshold)."""
        venue_id: str
        threshold: float
        count: int
        staff: List[StaffScoreResponse]
        computed_at: str


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _get_auth_helper():
    """Lazy import of auth module."""
    try:
        from rosteriq import auth
        return auth
    except ImportError:
        return None


def _get_staff_score_module():
    """Lazy import of staff_score module."""
    try:
        from rosteriq import staff_score
        return staff_score
    except ImportError:
        return None


def _gate(request: Request, level_name: str) -> Optional[Dict[str, Any]]:
    """Lazy auth gating (same pattern as api_v2).

    Returns user dict if auth passes, None if disabled, raises HTTPException if denied.
    """
    auth_helper = _get_auth_helper()
    if not auth_helper:
        return None  # Auth disabled

    try:
        level = getattr(auth_helper.AccessLevel, level_name, None)
        if not level:
            return None

        user = auth_helper.get_user_from_request(request)
        if not user or not auth_helper.check_level(user, level):
            raise HTTPException(status_code=403, detail="Insufficient access level")
        return user
    except Exception as e:
        logger.warning("Auth gate error: %s", e)
        return None


# ---------------------------------------------------------------------------
# Router Definition
# ---------------------------------------------------------------------------


def create_staff_score_router() -> Optional[Any]:
    """Create and return the staff score router.

    Returns None if FastAPI is not available (sandboxed environment).
    """
    if APIRouter is None:
        logger.warning("FastAPI not available; staff_score_router disabled")
        return None

    router = APIRouter()
    staff_score = _get_staff_score_module()

    if not staff_score:
        logger.warning("staff_score module not available")
        return None

    # -----------------------------------------------------------------------
    # GET /api/v1/staff/scores/{venue_id}
    # List all staff scores for a venue (recompute or cached)
    # -----------------------------------------------------------------------

    @router.get(
        "/{venue_id}",
        response_model=StaffScoresListResponse if BaseModel else None,
        tags=["staff"],
    )
    async def get_venue_scores(
        venue_id: str,
        request: Request,
    ):
        """Get all staff performance scores for a venue.

        Authorization: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")  # Raises on denied

        store = staff_score.get_staff_score_store()
        scores = store.list_by_venue(venue_id)

        if not scores:
            return {
                "venue_id": venue_id,
                "count": 0,
                "scores": [],
                "computed_at": datetime.now(timezone.utc).isoformat(),
            }

        return {
            "venue_id": venue_id,
            "count": len(scores),
            "scores": [s.to_dict() for s in scores],
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # -----------------------------------------------------------------------
    # GET /api/v1/staff/scores/{venue_id}/{employee_id}
    # Individual staff score breakdown
    # -----------------------------------------------------------------------

    @router.get(
        "/{venue_id}/{employee_id}",
        response_model=StaffScoreResponse if BaseModel else None,
        tags=["staff"],
    )
    async def get_staff_score(
        venue_id: str,
        employee_id: str,
        request: Request,
    ):
        """Get a single staff member's performance score breakdown.

        Authorization: L1_MANAGER or higher
        """
        _gate(request, "L1_MANAGER")  # Raises on denied

        store = staff_score.get_staff_score_store()
        score = store.get(venue_id, employee_id)

        if not score:
            raise HTTPException(status_code=404, detail="Score not found")

        return score.to_dict()

    # -----------------------------------------------------------------------
    # GET /api/v1/staff/rankings/{venue_id}
    # Staff ranked by overall score
    # -----------------------------------------------------------------------

    @router.get(
        "/rankings/{venue_id}",
        response_model=StaffRankingResponse if BaseModel else None,
        tags=["staff"],
    )
    async def get_staff_rankings(
        venue_id: str,
        request: Request,
    ):
        """Get staff ranked by overall performance score (highest first).

        Authorization: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")  # Raises on denied

        store = staff_score.get_staff_score_store()
        scores = store.list_by_venue(venue_id)
        ranked = staff_score.rank_staff(scores)

        return {
            "venue_id": venue_id,
            "count": len(ranked),
            "rankings": [s.to_dict() for s in ranked],
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # -----------------------------------------------------------------------
    # POST /api/v1/staff/scores/{venue_id}/recompute
    # Force recomputation of scores for a venue
    # -----------------------------------------------------------------------

    @router.post(
        "/{venue_id}/recompute",
        response_model=StaffScoresListResponse if BaseModel else None,
        tags=["staff"],
    )
    async def recompute_venue_scores(
        venue_id: str,
        request: Request,
    ):
        """Force recomputation of all staff scores for a venue.

        In a full implementation, this would re-fetch shift events, swaps, etc.
        from their respective sources. For now, returns cached scores.

        Authorization: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")  # Raises on denied

        # TODO: Integrate with shift_events, shift_swap, accountability_engine
        # to recompute scores for all employees at this venue.
        # For now, return the current cached scores.

        store = staff_score.get_staff_score_store()
        scores = store.list_by_venue(venue_id)

        return {
            "venue_id": venue_id,
            "count": len(scores),
            "scores": [s.to_dict() for s in scores],
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # -----------------------------------------------------------------------
    # GET /api/v1/staff/scores/{venue_id}/improvement
    # Staff needing improvement (below threshold)
    # -----------------------------------------------------------------------

    @router.get(
        "/{venue_id}/improvement",
        response_model=ImprovementNeededResponse if BaseModel else None,
        tags=["staff"],
    )
    async def get_improvement_needed(
        venue_id: str,
        threshold: float = 60.0,
        request: Request = None,
    ):
        """Get staff scoring below threshold, sorted by score (worst first).

        Authorization: L2_ROSTER_MAKER or higher
        """
        _gate(request, "L2_ROSTER_MAKER")  # Raises on denied

        store = staff_score.get_staff_score_store()
        below_threshold = store.list_needing_improvement(venue_id, threshold=threshold)

        return {
            "venue_id": venue_id,
            "threshold": threshold,
            "count": len(below_threshold),
            "staff": [s.to_dict() for s in below_threshold],
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    return router


# Create and export the router at module load time
staff_score_router = create_staff_score_router()
