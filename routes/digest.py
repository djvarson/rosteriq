"""
Weekly Digest API routes for RosterIQ.

Endpoints:
- GET /api/v1/venues/{venue_id}/digest/preview — preview this week's digest
- POST /api/v1/venues/{venue_id}/digest/send — send digest now
- GET /api/v1/venues/{venue_id}/digest/history — past digest summaries
- GET /api/v1/portfolio/digest — multi-venue digest for owners
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.weekly_digest import (
    WeeklyDigestGenerator, WeeklyDigest, MultiVenueDigest
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["digest"])


# ============================================================================
# Request/Response Models
# ============================================================================

class SendDigestRequest(BaseModel):
    """Request to send digest immediately."""
    recipients: List[str]  # email addresses


class DigestPreviewResponse(BaseModel):
    """Response with digest preview data."""
    venue_id: str
    venue_name: str
    week_start: date
    week_end: date
    score: float
    labour_cost_total: float
    forecast_mape: float
    staffing_headcount: int
    compliance_violations: int
    insights: List[str]


class DigestHistoryItem(BaseModel):
    """Historical digest summary."""
    week_start: date
    week_end: date
    generated_at: datetime
    score: float
    labour_cost_total: float


class DigestHistoryResponse(BaseModel):
    """Response with digest history."""
    venue_id: str
    venue_name: str
    digests: List[DigestHistoryItem]


class PortfolioVenueDigestSummary(BaseModel):
    """Summary of a venue's digest in portfolio view."""
    venue_id: str
    venue_name: str
    score: float
    labour_cost: float
    forecast_mape: float
    staffing_headcount: int


class PortfolioDigestResponse(BaseModel):
    """Response with multi-venue portfolio digest."""
    week_start: date
    week_end: date
    venue_count: int
    total_cost: float
    avg_health_score: float
    venues: List[PortfolioVenueDigestSummary]
    insights: List[str]


# ============================================================================
# Route Handlers
# ============================================================================

@router.get("/venues/{venue_id}/digest/preview", response_model=DigestPreviewResponse)
async def preview_digest(
    venue_id: str,
    week_start: Optional[date] = Query(None, description="Start of week (Monday)"),
) -> DigestPreviewResponse:
    """
    Preview this week's digest for a venue.

    Args:
        venue_id: Venue identifier
        week_start: Week to digest (defaults to current week)

    Returns:
        Digest summary with key metrics and insights
    """
    try:
        generator = WeeklyDigestGenerator()
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Default to this week if not specified
        if not week_start:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

        digest = generator.generate_digest(venue_id, week_start)

        return DigestPreviewResponse(
            venue_id=digest.venue_id,
            venue_name=digest.venue_name,
            week_start=digest.week_start,
            week_end=digest.week_end,
            score=digest.score,
            labour_cost_total=float(digest.labour_cost.total_cost),
            forecast_mape=float(digest.forecast_accuracy.mape),
            staffing_headcount=digest.staffing.unique_headcount,
            compliance_violations=digest.compliance.violations_found,
            insights=digest.insights,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to preview digest: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate digest")


@router.post("/venues/{venue_id}/digest/send")
async def send_digest(
    venue_id: str,
    request: SendDigestRequest,
    week_start: Optional[date] = Query(None, description="Week to digest"),
) -> dict:
    """
    Generate and send digest immediately to specified recipients.

    Args:
        venue_id: Venue identifier
        request: Recipients list
        week_start: Week to digest (defaults to current week)

    Returns:
        Status and confirmation
    """
    enforce_venue_manager(venue_id)
    try:
        if not request.recipients:
            raise HTTPException(status_code=400, detail="No recipients specified")

        generator = WeeklyDigestGenerator()
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Default to this week if not specified
        if not week_start:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

        # Send asynchronously
        success = await generator.send_digest(
            venue_id=venue_id,
            week_start=week_start,
            recipients=request.recipients,
        )

        if not success:
            raise HTTPException(status_code=500, detail="Failed to send digest emails")

        return {
            "status": "sent",
            "venue_id": venue_id,
            "recipients": len(request.recipients),
            "week_start": week_start.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send digest: {e}")
        raise HTTPException(status_code=500, detail="Failed to send digest")


@router.get("/venues/{venue_id}/digest/history", response_model=DigestHistoryResponse)
async def get_digest_history(
    venue_id: str,
    weeks: int = Query(12, ge=1, le=52, description="Number of weeks of history"),
) -> DigestHistoryResponse:
    """
    Get historical digest summaries for a venue.

    Args:
        venue_id: Venue identifier
        weeks: Number of past weeks to retrieve (default 12, max 52)

    Returns:
        List of historical digests with key metrics
    """
    try:
        generator = WeeklyDigestGenerator()
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        # Generate digests for past weeks
        today = date.today()
        week_end = today - timedelta(days=today.weekday() + 1)  # Last Sunday
        digests_list = []

        for i in range(weeks):
            week_start = week_end - timedelta(days=6)

            try:
                digest = generator.generate_digest(venue_id, week_start)
                digests_list.append(DigestHistoryItem(
                    week_start=digest.week_start,
                    week_end=digest.week_end,
                    generated_at=digest.generated_at,
                    score=digest.score,
                    labour_cost_total=float(digest.labour_cost.total_cost),
                ))
            except Exception as e:
                logger.warning(f"Failed to generate digest for week {week_start}: {e}")

            week_end = week_start - timedelta(days=1)

        return DigestHistoryResponse(
            venue_id=venue_id,
            venue_name=venue.name,
            digests=digests_list,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve digest history: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve history")


@router.get("/portfolio/digest", response_model=PortfolioDigestResponse)
async def get_portfolio_digest(
    venue_ids: str = Query(..., description="Comma-separated venue IDs"),
    week_start: Optional[date] = Query(None, description="Week to digest"),
) -> PortfolioDigestResponse:
    """
    Generate portfolio digest for owners with multiple venues.

    Args:
        venue_ids: Comma-separated list of venue IDs
        week_start: Week to digest (defaults to current week)

    Returns:
        Multi-venue digest with comparison insights
    """
    try:
        venue_list = [v.strip() for v in venue_ids.split(",")]
        if not venue_list or len(venue_list) < 2:
            raise HTTPException(
                status_code=400,
                detail="Portfolio digest requires at least 2 venues"
            )

        generator = WeeklyDigestGenerator()
        db = get_db()

        # Validate all venues exist
        for vid in venue_list:
            venue = db.get_venue(vid)
            if not venue:
                raise HTTPException(
                    status_code=404,
                    detail=f"Venue {vid} not found"
                )

        # Default to this week
        if not week_start:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

        multi_digest = generator.generate_multi_venue_digest(venue_list, week_start)

        # Build response
        venue_summaries = [
            PortfolioVenueDigestSummary(
                venue_id=d.venue_id,
                venue_name=d.venue_name,
                score=d.score,
                labour_cost=float(d.labour_cost.total_cost),
                forecast_mape=float(d.forecast_accuracy.mape),
                staffing_headcount=d.staffing.unique_headcount,
            )
            for d in multi_digest.venue_digests
        ]

        return PortfolioDigestResponse(
            week_start=multi_digest.week_start,
            week_end=multi_digest.week_end,
            venue_count=len(multi_digest.venue_digests),
            total_cost=float(multi_digest.portfolio_totals.get("total_cost", 0)),
            avg_health_score=multi_digest.portfolio_totals.get("avg_health_score", 0.0),
            venues=venue_summaries,
            insights=multi_digest.comparison_insights,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to generate portfolio digest: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate portfolio digest")
