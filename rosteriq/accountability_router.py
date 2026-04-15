"""Accountability Router — endpoints for decision logging and variance analysis.

Provides:
- POST /api/v1/accountability/decisions — log a decision (L2 gated)
- GET /api/v1/accountability/decisions — list decisions (L2 gated)
- GET /api/v1/accountability/variance — variance for a shift (L2 gated)
- GET /api/v1/accountability/scorecard/{manager_id} — manager score (OWNER gated)
- GET /api/v1/accountability/leaderboard — portfolio leaderboard (OWNER gated)

In demo mode (AUTH_ENABLED=False), require_access short-circuits to OWNER.
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field

from rosteriq.auth import (
    AccessLevel,
    User,
    require_access,
    AUTH_ENABLED,
)
from rosteriq import accountability_engine as acct_eng

# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(prefix="/api/v1/accountability", tags=["accountability"])

# Seed demo data on first load if store is empty
acct_eng._seed_demo_data()


# ============================================================================
# Pydantic Models
# ============================================================================


class DecisionTypeEnum(str):
    """Decision type values."""

    KEPT_STAFF_ON = "kept_staff_on"
    CUT_STAFF = "cut_staff"
    CALLED_IN_STAFF = "called_in_staff"
    IGNORED_ALERT = "ignored_alert"
    PUBLISHED_ROSTER = "published_roster"
    MODIFIED_ROSTER = "modified_roster"


class DecisionLogRequest(BaseModel):
    """Request to log a decision."""

    venue_id: str = Field(..., description="Venue ID")
    shift_id: str = Field(..., description="Shift ID (e.g. shift_2026-04-15_0900)")
    manager_id: str = Field(..., description="Manager ID making decision")
    manager_name: str = Field(..., description="Manager name")
    decision_type: str = Field(
        ...,
        description="Decision type: kept_staff_on, cut_staff, called_in_staff, ignored_alert, published_roster, modified_roster",
    )
    signals_available: Dict[str, Any] = Field(
        ..., description="Snapshot of forecasts/alerts at decision time"
    )
    notes: Optional[str] = Field(None, description="Optional notes")


class DecisionLogResponse(BaseModel):
    """Response from decision log."""

    decision_id: str
    venue_id: str
    shift_id: str
    manager_id: str
    manager_name: str
    decision_type: str
    taken_at: str
    signals_available: Dict[str, Any]
    outcome_variance: Dict[str, Any]
    notes: Optional[str]


class VarianceRecordResponse(BaseModel):
    """Variance record for a shift."""

    venue_id: str
    shift_id: str
    shift_date: str
    forecast_revenue: Optional[float]
    actual_revenue: Optional[float]
    forecast_headcount_peak: Optional[int]
    actual_headcount_peak: Optional[int]
    forecast_staff_hours: Optional[float]
    actual_staff_hours: Optional[float]
    variance_revenue_pct: Optional[float]
    variance_staff_hours_pct: Optional[float]
    computed_at: str


class ManagerScoreResponse(BaseModel):
    """Manager accountability score."""

    manager_id: str
    manager_name: str
    venue_id: str
    decisions_total: int
    alerts_actioned_pct: float
    avg_variance_revenue: float
    avg_variance_staff_hours: float
    decisions_against_signals: int


class LeaderboardEntryResponse(BaseModel):
    """Leaderboard entry."""

    manager_id: str
    manager_name: str
    venue_id: str
    decisions_total: int
    alerts_actioned_pct: float
    avg_variance_revenue: float
    avg_variance_staff_hours: float
    decisions_against_signals: int


class LeaderboardResponse(BaseModel):
    """Leaderboard response."""

    leaderboard: List[LeaderboardEntryResponse]
    generated_at: str


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/decisions", response_model=DecisionLogResponse)
async def post_decision(
    request: DecisionLogRequest,
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)),
) -> DecisionLogResponse:
    """Log a manager decision (L2 gated).

    Called by the dashboard when a manager cuts/keeps staff during a shift.
    Stores the decision and the signals available at the time.

    Args:
        request: Decision details
        user: Authenticated user (L2+)

    Returns:
        DecisionLogResponse with decision_id and metadata

    """
    # Convert decision_type string to enum
    try:
        decision_type = acct_eng.DecisionType(request.decision_type)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid decision_type '{request.decision_type}'",
        )

    # Record the decision
    decision = acct_eng.record_decision(
        venue_id=request.venue_id,
        shift_id=request.shift_id,
        manager_id=request.manager_id,
        manager_name=request.manager_name,
        decision_type=decision_type,
        signals_available=request.signals_available,
        notes=request.notes,
    )

    return DecisionLogResponse(
        decision_id=decision.decision_id,
        venue_id=decision.venue_id,
        shift_id=decision.shift_id,
        manager_id=decision.manager_id,
        manager_name=decision.manager_name,
        decision_type=decision.decision_type.value,
        taken_at=decision.taken_at,
        signals_available=decision.signals_available,
        outcome_variance=decision.outcome_variance,
        notes=decision.notes,
    )


@router.get("/decisions", response_model=List[DecisionLogResponse])
async def list_decisions(
    venue_id: str = Query(..., description="Venue ID"),
    manager_id: Optional[str] = Query(None, description="Optional filter by manager"),
    since: Optional[str] = Query(None, description="ISO 8601 datetime filter"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)),
) -> List[DecisionLogResponse]:
    """List decisions for a venue (L2 gated).

    Args:
        venue_id: Venue ID
        manager_id: Optional manager ID filter
        since: Optional ISO 8601 datetime to filter
        limit: Max results
        user: Authenticated user (L2+)

    Returns:
        List of DecisionLogResponse

    """
    # Parse since if provided
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid since format; use ISO 8601",
            )

    # List decisions
    decisions = acct_eng.list_decisions(
        venue_id=venue_id,
        manager_id=manager_id,
        since=since_dt,
        limit=limit,
    )

    return [
        DecisionLogResponse(
            decision_id=d.decision_id,
            venue_id=d.venue_id,
            shift_id=d.shift_id,
            manager_id=d.manager_id,
            manager_name=d.manager_name,
            decision_type=d.decision_type.value,
            taken_at=d.taken_at,
            signals_available=d.signals_available,
            outcome_variance=d.outcome_variance,
            notes=d.notes,
        )
        for d in decisions
    ]


@router.get("/variance", response_model=VarianceRecordResponse)
async def get_variance(
    venue_id: str = Query(..., description="Venue ID"),
    shift_id: str = Query(..., description="Shift ID"),
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)),
) -> VarianceRecordResponse:
    """Get variance for a single shift (L2 gated).

    Args:
        venue_id: Venue ID
        shift_id: Shift ID
        user: Authenticated user (L2+)

    Returns:
        VarianceRecordResponse, or 404 if not found

    """
    variances = acct_eng.list_variance(
        venue_id=venue_id,
        shift_id=shift_id,
        limit=1,
    )

    if not variances:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No variance record for shift {shift_id}",
        )

    v = variances[0]
    return VarianceRecordResponse(
        venue_id=v.venue_id,
        shift_id=v.shift_id,
        shift_date=v.shift_date.isoformat(),
        forecast_revenue=v.forecast_revenue,
        actual_revenue=v.actual_revenue,
        forecast_headcount_peak=v.forecast_headcount_peak,
        actual_headcount_peak=v.actual_headcount_peak,
        forecast_staff_hours=v.forecast_staff_hours,
        actual_staff_hours=v.actual_staff_hours,
        variance_revenue_pct=v.variance_revenue_pct,
        variance_staff_hours_pct=v.variance_staff_hours_pct,
        computed_at=v.computed_at,
    )


@router.get("/scorecard/{manager_id}", response_model=ManagerScoreResponse)
async def get_scorecard(
    manager_id: str,
    venue_id: str = Query(..., description="Venue ID"),
    since: Optional[str] = Query(None, description="ISO 8601 datetime filter"),
    user: User = Depends(require_access(AccessLevel.OWNER)),
) -> ManagerScoreResponse:
    """Get accountability scorecard for a manager (OWNER gated).

    Args:
        manager_id: Manager ID
        venue_id: Venue ID
        since: Optional ISO 8601 datetime to filter
        user: Authenticated user (OWNER)

    Returns:
        ManagerScoreResponse

    """
    # Parse since if provided
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid since format; use ISO 8601",
            )

    score = acct_eng.score_manager(
        venue_id=venue_id,
        manager_id=manager_id,
        since=since_dt,
    )

    return ManagerScoreResponse(
        manager_id=score.manager_id,
        manager_name=score.manager_name,
        venue_id=score.venue_id,
        decisions_total=score.decisions_total,
        alerts_actioned_pct=score.alerts_actioned_pct,
        avg_variance_revenue=score.avg_variance_revenue,
        avg_variance_staff_hours=score.avg_variance_staff_hours,
        decisions_against_signals=score.decisions_against_signals,
    )


@router.get("/leaderboard", response_model=LeaderboardResponse)
async def get_leaderboard(
    venue_ids: str = Query(..., description="Comma-separated venue IDs"),
    since: Optional[str] = Query(None, description="ISO 8601 datetime filter"),
    limit: int = Query(100, ge=1, le=500, description="Max managers"),
    user: User = Depends(require_access(AccessLevel.OWNER)),
) -> LeaderboardResponse:
    """Get manager leaderboard across venues (OWNER gated).

    Sorted by alerts_actioned_pct descending (best actors first).

    Args:
        venue_ids: Comma-separated venue IDs
        since: Optional ISO 8601 datetime to filter
        limit: Max managers
        user: Authenticated user (OWNER)

    Returns:
        LeaderboardResponse with ranked managers

    """
    # Parse venue_ids
    venues = [v.strip() for v in venue_ids.split(",") if v.strip()]
    if not venues:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="venue_ids cannot be empty",
        )

    # Parse since if provided
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid since format; use ISO 8601",
            )

    scores = acct_eng.venue_leaderboard(
        venue_ids=venues,
        since=since_dt,
        limit=limit,
    )

    return LeaderboardResponse(
        leaderboard=[
            LeaderboardEntryResponse(
                manager_id=s.manager_id,
                manager_name=s.manager_name,
                venue_id=s.venue_id,
                decisions_total=s.decisions_total,
                alerts_actioned_pct=s.alerts_actioned_pct,
                avg_variance_revenue=s.avg_variance_revenue,
                avg_variance_staff_hours=s.avg_variance_staff_hours,
                decisions_against_signals=s.decisions_against_signals,
            )
            for s in scores
        ],
        generated_at=datetime.now().isoformat(),
    )
