"""
Routes for real-time labour tracking with colour-coded alerts.

Endpoints:
- GET /api/v1/venues/{id}/labour-live — current live status
- GET /api/v1/venues/{id}/labour-hourly — hourly trend today
- GET /api/v1/venues/{id}/labour-prediction — end-of-day prediction
- GET /api/v1/venues/{id}/labour-weekly — weekly summary
- GET /api/v1/venues/{id}/labour-vs-forecast — actual vs forecast
- PUT /api/v1/venues/{id}/labour-thresholds — update alert thresholds
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict

from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field

from rosteriq.services.labour_tracker import LabourTracker
from rosteriq.middleware.tenant import enforce_venue_manager

logger = logging.getLogger(__name__)

# Global labour tracker instance
_labour_tracker: Optional[LabourTracker] = None


# ============================================================================
# Pydantic Models for Responses
# ============================================================================


class HourlySnapshotResponse(BaseModel):
    """Hourly labour efficiency snapshot."""

    hour: int
    revenue: str
    labour_cost: str
    staff_count: int
    labour_pct: float


class LabourStatusResponse(BaseModel):
    """Current live labour status."""

    venue_id: str
    timestamp: str
    revenue_today: str
    labour_cost_today: str
    labour_percentage: float
    status: str  # "green" | "amber" | "red" | "critical"
    staff_on_shift: int
    average_cost_per_hour: str
    revenue_per_staff_hour: str
    hourly_breakdown: list[HourlySnapshotResponse]
    projected_eod_labour_pct: float
    projected_eod_revenue: str
    projected_eod_cost: str
    alert_message: Optional[str] = None


class HourlyTrendResponse(BaseModel):
    """Hourly labour trend for today."""

    venue_id: str
    date: str  # Today's date in YYYY-MM-DD
    hourly_data: list[HourlySnapshotResponse]
    current_hour: int


class EODPredictionResponse(BaseModel):
    """End-of-day labour cost projection."""

    venue_id: str
    timestamp: str
    predicted_revenue: str
    predicted_cost: str
    predicted_labour_pct: float
    confidence: float  # 0.0-1.0
    method: str  # "linear" | "weighted" | "trend"
    hours_extrapolated: int
    last_update: str


class ForecastComparisonResponse(BaseModel):
    """Actual vs forecasted labour performance."""

    venue_id: str
    forecast_revenue: str
    actual_revenue: str
    revenue_variance_pct: float
    forecast_labour_pct: float
    actual_labour_pct: float
    labour_variance_pct: float
    hours_completed: int
    hours_remaining: int


class DailyLabourSummaryResponse(BaseModel):
    """Daily labour summary."""

    date: str
    revenue: str
    labour_cost: str
    labour_pct: float
    status: str
    staff_hours: str


class WeeklyLabourSummaryResponse(BaseModel):
    """Week-to-date labour summary."""

    week_start: str
    week_end: str
    daily_summaries: list[DailyLabourSummaryResponse]
    week_avg_labour_pct: float
    week_total_revenue: str
    week_total_cost: str
    best_day: Optional[str] = None
    worst_day: Optional[str] = None
    trend: str  # "improving" | "stable" | "declining"


class ThresholdConfigRequest(BaseModel):
    """Request to update alert thresholds."""

    green_max: Optional[float] = Field(
        None, description="Max labour % for green status (e.g., 28.0)"
    )
    amber_max: Optional[float] = Field(
        None, description="Max labour % for amber status (e.g., 33.0)"
    )
    red_max: Optional[float] = Field(
        None, description="Max labour % for red status (e.g., 38.0)"
    )


class ThresholdConfigResponse(BaseModel):
    """Current alert threshold configuration."""

    venue_id: str
    green_max: float
    amber_max: float
    red_max: float


# ============================================================================
# Router Factory
# ============================================================================


def create_labour_router(labour_tracker: LabourTracker) -> APIRouter:
    """
    Factory to create labour routes with injected labour_tracker.

    Args:
        labour_tracker: Initialized LabourTracker instance
    """
    global _labour_tracker
    _labour_tracker = labour_tracker

    router = APIRouter(prefix="/api/v1/venues", tags=["Labour Tracking"])

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-live
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-live",
        response_model=LabourStatusResponse,
        summary="Get current live labour status",
        description="Returns real-time labour percentage, staff on shift, revenue, and cost breakdown with colour-coded status.",
    )
    async def get_live_labour_status(
        venue_id: str = Path(..., description="Venue ID"),
    ):
        """
        Get current live labour status.

        Returns:
        - Current revenue today (so far)
        - Current labour cost (staff on shift)
        - Real-time labour percentage with colour status
        - Staff on shift count
        - Hourly breakdown
        - End-of-day projection
        - Alert message (if status warrants)
        """
        try:
            status = _labour_tracker.get_live_status(venue_id)
            return status.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error fetching live labour status: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch labour status")

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-hourly
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-hourly",
        response_model=HourlyTrendResponse,
        summary="Get hourly labour trend",
        description="Hour-by-hour labour efficiency breakdown for today.",
    )
    async def get_hourly_labour_trend(
        venue_id: str = Path(..., description="Venue ID"),
    ):
        """
        Get today's hour-by-hour labour efficiency.

        Shows revenue, labour cost, staff count, and labour % for each hour.
        Useful for identifying peak labour cost hours.
        """
        try:
            from datetime import date
            hourly_data = _labour_tracker.get_hourly_trend(venue_id)
            return {
                "venue_id": venue_id,
                "date": date.today().isoformat(),
                "hourly_data": [
                    {
                        "hour": h.hour,
                        "revenue": str(h.revenue),
                        "labour_cost": str(h.labour_cost),
                        "staff_count": h.staff_count,
                        "labour_pct": h.labour_pct,
                    }
                    for h in hourly_data
                ],
                "current_hour": datetime.now().hour,
            }
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error fetching hourly trend: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch hourly trend")

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-prediction
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-prediction",
        response_model=EODPredictionResponse,
        summary="Get end-of-day labour projection",
        description="Predicts final labour cost and percentage based on current trends.",
    )
    async def get_eod_prediction(
        venue_id: str = Path(..., description="Venue ID"),
    ):
        """
        Get end-of-day labour cost prediction.

        Uses linear extrapolation from current trends to project:
        - Final revenue for the day
        - Final labour cost
        - Final labour percentage
        - Confidence level (0.0-1.0)
        """
        try:
            prediction = _labour_tracker.predict_end_of_day(venue_id)
            result = prediction.to_dict()
            result["venue_id"] = venue_id
            result["timestamp"] = datetime.now().isoformat()
            return result
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error predicting EOD: {e}")
            raise HTTPException(status_code=500, detail="Failed to predict EOD")

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-weekly
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-weekly",
        response_model=WeeklyLabourSummaryResponse,
        summary="Get weekly labour summary",
        description="Week-to-date labour performance with daily breakdown and trend analysis.",
    )
    async def get_weekly_labour_summary(
        venue_id: str = Path(..., description="Venue ID"),
    ):
        """
        Get week-to-date labour performance summary.

        Returns:
        - Daily labour % breakdown for each day this week
        - Weekly average labour %
        - Best and worst performing days
        - Trend (improving/stable/declining)
        - Weekly totals (revenue, cost)
        """
        try:
            summary = _labour_tracker.get_weekly_summary(venue_id)
            return summary.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error fetching weekly summary: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch weekly summary")

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-vs-forecast
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-vs-forecast",
        response_model=ForecastComparisonResponse,
        summary="Compare actual vs forecasted labour",
        description="Shows variance between predicted and actual labour performance.",
    )
    async def get_labour_vs_forecast(
        venue_id: str = Path(..., description="Venue ID"),
        forecast_revenue: float = Query(
            ..., description="Forecasted revenue for today (in dollars)"
        ),
        forecast_labour_pct: float = Query(
            ..., description="Forecasted labour percentage for today"
        ),
    ):
        """
        Compare actual vs forecasted labour performance.

        Shows:
        - Revenue variance (actual vs forecast)
        - Labour % variance (actual vs forecast)
        - Hours completed / remaining
        """
        try:
            comparison = _labour_tracker.compare_to_forecast(
                venue_id,
                Decimal(str(forecast_revenue)),
                forecast_labour_pct,
            )
            result = comparison.to_dict()
            result["venue_id"] = venue_id
            return result
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error comparing to forecast: {e}")
            raise HTTPException(status_code=500, detail="Failed to compare forecast")

    # ========================================================================
    # GET /api/v1/venues/{id}/labour-thresholds
    # ========================================================================

    @router.get(
        "/{venue_id}/labour-thresholds",
        response_model=ThresholdConfigResponse,
        summary="Get alert threshold configuration",
        description="Returns current alert thresholds (green, amber, red, critical).",
    )
    async def get_labour_thresholds(
        venue_id: str = Path(..., description="Venue ID"),
    ):
        """
        Get current alert threshold configuration for the venue.

        Returns thresholds that determine colour-coded status:
        - green_max: Maximum labour % for green (default: 28%)
        - amber_max: Maximum labour % for amber (default: 33%)
        - red_max: Maximum labour % for red (default: 38%)
        - Critical is anything above red_max
        """
        try:
            thresholds = _labour_tracker.get_threshold_config(venue_id)
            return {
                "venue_id": venue_id,
                "green_max": thresholds["green_max"],
                "amber_max": thresholds["amber_max"],
                "red_max": thresholds["red_max"],
            }
        except Exception as e:
            logger.error(f"Error fetching thresholds: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch thresholds")

    # ========================================================================
    # PUT /api/v1/venues/{id}/labour-thresholds
    # ========================================================================

    @router.put(
        "/{venue_id}/labour-thresholds",
        response_model=ThresholdConfigResponse,
        summary="Update alert thresholds",
        description="Update the labour % thresholds that trigger colour-coded alerts.",
    )
    async def update_labour_thresholds(
        venue_id: str = Path(..., description="Venue ID"),
        request: ThresholdConfigRequest = None,
    ):
        """
        Update alert threshold configuration.

        Thresholds must be in order: green < amber < red.

        Example:
            PUT /api/v1/venues/venue-123/labour-thresholds
            {
              "green_max": 25.0,
              "amber_max": 30.0,
              "red_max": 35.0
            }
        """
        enforce_venue_manager(venue_id)
        try:
            # Build update dict from non-None values
            update_dict = {}
            if request and request.green_max is not None:
                update_dict["green_max"] = request.green_max
            if request and request.amber_max is not None:
                update_dict["amber_max"] = request.amber_max
            if request and request.red_max is not None:
                update_dict["red_max"] = request.red_max

            if not update_dict:
                raise ValueError("No thresholds provided")

            updated = _labour_tracker.update_thresholds(venue_id, update_dict)
            return {
                "venue_id": venue_id,
                "green_max": updated["green_max"],
                "amber_max": updated["amber_max"],
                "red_max": updated["red_max"],
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error updating thresholds: {e}")
            raise HTTPException(status_code=500, detail="Failed to update thresholds")

    # Return the configured router so app.include_router() receives a real
    # APIRouter (previously returned None -> "'NoneType' has no attribute 'routes'").
    return router


# ============================================================================
# Module initialization
# ============================================================================

# Default router (requires labour_tracker injection at startup)
router = APIRouter()


@router.on_event("startup")
async def init_labour_tracker():
    """Initialize labour tracker at API startup."""
    global _labour_tracker
    if _labour_tracker is None:
        _labour_tracker = LabourTracker()
        logger.info("Labour tracker initialized")
