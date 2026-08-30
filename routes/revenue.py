"""
Revenue forecasting API routes for RosterIQ.

Endpoints:
- POST /api/revenue/train/{venue_id} — train model on historical POS data
- GET /api/revenue/predict/{venue_id}/{date} — predict revenue for a date
- GET /api/revenue/week/{venue_id} — predict revenue for a week
- POST /api/revenue/budget-check — check roster vs predicted revenue
- POST /api/revenue/record-actual — record actual revenue for tracking
- GET /api/revenue/accuracy/{venue_id} — get accuracy metrics
"""

import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.revenue_forecast import (
    RevenueForecaster, RevenueEstimate, BudgetCheckResult
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/revenue", tags=["revenue"])


# ============================================================================
# Request/Response Models
# ============================================================================


class TrainRevenueRequest(BaseModel):
    """Request to train revenue model."""
    venue_id: str
    lookback_days: int = 90


class HourlyRevenueBreakdown(BaseModel):
    """Hourly revenue breakdown."""
    hour: int  # 0-23
    revenue: Decimal


class RevenueEstimateResponse(BaseModel):
    """Response with revenue estimate."""
    venue_id: str
    target_date: date
    daily_total: str  # Decimal as string
    hourly_breakdown: Dict[int, str]  # hour -> revenue as string
    confidence: float
    factors: Dict[str, float]


class BudgetCheckRequest(BaseModel):
    """Request to check roster against revenue budget."""
    venue_id: str
    roster_id: str
    target_labour_pct: float = 0.30  # 30% default


class BudgetCheckResponse(BaseModel):
    """Response with budget check results."""
    venue_id: str
    target_date: str
    predicted_revenue: str
    roster_labour_cost: str
    labour_pct: float
    target_labour_pct: float
    within_budget: bool
    savings_opportunity: Optional[str] = None
    risk_flag: Optional[str] = None


class RecordActualRequest(BaseModel):
    """Request to record actual revenue."""
    venue_id: str
    date: str  # ISO format
    daily_total: Decimal
    hourly_breakdown: Optional[Dict[int, Decimal]] = None


class RecordActualResponse(BaseModel):
    """Response after recording actual."""
    venue_id: str
    date: str
    actual_revenue: str
    recorded: bool


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/train/{venue_id}")
async def train_revenue_model(
    venue_id: str,
    lookback_days: int = Query(90),
) -> Dict:
    """
    Train revenue model on historical POS data.

    Learns day-of-week patterns, hourly distribution, trend, and base revenue
    from historical POS records in the database.

    Args:
        venue_id: Venue to train for
        lookback_days: How many days of history to use (default 90)

    Returns:
        Model dict with learned parameters and confidence score
    """
    enforce_venue_manager(venue_id)
    try:
        forecaster = RevenueForecaster()
        model = forecaster.train(venue_id, lookback_days=lookback_days)

        return {
            "success": True,
            "venue_id": venue_id,
            "model": {
                "base_revenue": model.get("base_revenue"),
                "confidence": model.get("confidence"),
                "sample_size": model.get("sample_size"),
                "trained_at": model.get("trained_at"),
            },
        }
    except Exception as e:
        logger.error(f"Error training revenue model for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to train model: {str(e)}",
        )


@router.get("/predict/{venue_id}/{target_date}")
async def predict_revenue(
    venue_id: str,
    target_date: str,  # ISO format
    weather_modifier: float = Query(1.0),
    event_modifier: float = Query(1.0),
) -> RevenueEstimateResponse:
    """
    Predict revenue for a specific date.

    Uses trained model with adjustments for weather, events, day-of-week patterns,
    and trend.

    Args:
        venue_id: Venue to predict for
        target_date: Target date in ISO format (YYYY-MM-DD)
        weather_modifier: 0.5-1.5 (0.8 = 20% reduction due to weather)
        event_modifier: 0.5-2.0 (1.2 = 20% increase due to event)

    Returns:
        RevenueEstimate with daily_total and hourly breakdown
    """
    try:
        target_date_obj = datetime.fromisoformat(target_date).date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format, use ISO (YYYY-MM-DD)",
        )

    try:
        forecaster = RevenueForecaster()
        estimate = forecaster.predict_revenue(
            venue_id,
            target_date_obj,
            weather_modifier=weather_modifier,
            event_modifier=event_modifier,
        )

        return RevenueEstimateResponse(
            venue_id=estimate.venue_id,
            target_date=estimate.target_date,
            daily_total=str(estimate.daily_total),
            hourly_breakdown={
                hour: str(revenue)
                for hour, revenue in estimate.hourly_breakdown.items()
            },
            confidence=estimate.confidence,
            factors=estimate.factors,
        )
    except Exception as e:
        logger.error(f"Error predicting revenue for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to predict revenue: {str(e)}",
        )


@router.get("/week/{venue_id}")
async def predict_week(
    venue_id: str,
    start_date: str = Query(...),  # ISO format, should be Monday
) -> Dict:
    """
    Predict revenue for a full week.

    Args:
        venue_id: Venue to predict for
        start_date: Start of week in ISO format (preferably Monday)

    Returns:
        List of 7 daily revenue estimates
    """
    try:
        start_date_obj = datetime.fromisoformat(start_date).date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format, use ISO (YYYY-MM-DD)",
        )

    try:
        forecaster = RevenueForecaster()
        estimates = forecaster.predict_week(venue_id, start_date_obj)

        return {
            "success": True,
            "venue_id": venue_id,
            "week_start": start_date_obj.isoformat(),
            "estimates": [
                {
                    "date": est.target_date.isoformat(),
                    "daily_total": str(est.daily_total),
                    "confidence": est.confidence,
                }
                for est in estimates
            ],
            "week_total": str(sum(est.daily_total for est in estimates)),
        }
    except Exception as e:
        logger.error(f"Error predicting week for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to predict week: {str(e)}",
        )


@router.post("/budget-check")
async def check_budget(request: BudgetCheckRequest) -> BudgetCheckResponse:
    """
    Check roster labour cost against predicted revenue.

    Compares actual roster labour cost to revenue forecast and flags
    over/understaffing situations with savings opportunities.

    Args:
        request: BudgetCheckRequest with venue_id, roster_id, target_labour_pct

    Returns:
        BudgetCheckResult with budget status and recommendations
    """
    db = get_db()
    roster = db.get_roster(request.roster_id)

    if not roster:
        raise HTTPException(status_code=404, detail=f"Roster {request.roster_id} not found")

    try:
        forecaster = RevenueForecaster()
        result = forecaster.labour_budget_check(
            request.venue_id,
            roster,
            target_labour_pct=request.target_labour_pct,
        )

        return BudgetCheckResponse(
            venue_id=result.venue_id,
            target_date=result.target_date.isoformat(),
            predicted_revenue=str(result.predicted_revenue),
            roster_labour_cost=str(result.roster_labour_cost),
            labour_pct=result.labour_pct,
            target_labour_pct=result.target_labour_pct,
            within_budget=result.within_budget,
            savings_opportunity=(
                str(result.savings_opportunity)
                if result.savings_opportunity
                else None
            ),
            risk_flag=result.risk_flag,
        )
    except Exception as e:
        logger.error(f"Error checking budget for {request.venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check budget: {str(e)}",
        )


@router.post("/record-actual")
async def record_actual(request: RecordActualRequest) -> RecordActualResponse:
    """
    Record actual revenue for a date.

    Used for accuracy tracking and model refinement over time.

    Args:
        request: RecordActualRequest with actual revenue and breakdown

    Returns:
        Confirmation and any accuracy metrics
    """
    enforce_venue_manager(request.venue_id)
    try:
        target_date = datetime.fromisoformat(request.date).date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format, use ISO (YYYY-MM-DD)",
        )

    try:
        forecaster = RevenueForecaster()

        actual_data = {
            "daily_total": str(request.daily_total),
            "hourly_breakdown": (
                {int(h): str(r) for h, r in request.hourly_breakdown.items()}
                if request.hourly_breakdown
                else {}
            ),
        }

        result = forecaster.track_accuracy(
            request.venue_id,
            target_date,
            actual_data,
        )

        return RecordActualResponse(
            venue_id=result["venue_id"],
            date=result["date"],
            actual_revenue=result["actual_revenue"],
            recorded=result["recorded"],
        )
    except Exception as e:
        logger.error(f"Error recording actual revenue for {request.venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to record actual: {str(e)}",
        )


@router.get("/accuracy/{venue_id}")
async def get_accuracy_metrics(
    venue_id: str,
    days: int = Query(30),
) -> Dict:
    """
    Get accuracy metrics for revenue forecasts.

    Compares predictions to actual revenue over past N days.

    Args:
        venue_id: Venue to get accuracy for
        days: Number of days to analyse (default 30)

    Returns:
        Accuracy metrics and forecast performance
    """
    try:
        db = get_db()

        # Get recent actuals
        start_date = (date.today() - timedelta(days=days)).isoformat()
        end_date = date.today().isoformat()

        actuals = db.list_revenue_actuals(venue_id, start=start_date, end=end_date)

        if not actuals:
            return {
                "venue_id": venue_id,
                "period_days": days,
                "sample_size": 0,
                "message": "No actual revenue data found",
                "accuracy": None,
            }

        # Parse actuals and calculate basic metrics
        total_actual = Decimal("0")
        for actual in actuals:
            total_actual += Decimal(str(actual.get("daily_total", 0)))

        return {
            "venue_id": venue_id,
            "period_days": days,
            "sample_size": len(actuals),
            "total_revenue": str(total_actual),
            "avg_daily": str(total_actual / len(actuals)) if actuals else "0",
            "message": "Accuracy metrics (detailed comparison requires stored predictions)",
        }
    except Exception as e:
        logger.error(f"Error getting accuracy metrics for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get accuracy metrics: {str(e)}",
        )
