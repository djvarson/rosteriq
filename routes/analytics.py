"""
Analytics API routes for labour cost intelligence.

Provides REST endpoints for labour trending, forecast accuracy, venue benchmarking,
peak hour analysis, and cost optimisation insights.

All dates in ISO 8601 format (YYYY-MM-DD).
All monetary values in AUD, returned as floats.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from rosteriq.database import get_db, BaseStore
from rosteriq.services.analytics import AnalyticsService


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


# ============================================================================
# Pydantic Response Models
# ============================================================================


class LabourTrendPoint(BaseModel):
    """Labour cost trend data point."""
    date: str
    total_labour_cost: float
    total_revenue: float
    labour_percentage: float
    headcount: int
    hours_worked: float
    labour_pct_ma: Optional[float] = None  # Moving average


class LabourTrendResponse(BaseModel):
    """Labour trend response."""
    venue_id: str
    period: str
    days: int
    trend_points: List[LabourTrendPoint]


class LabourBreakdownStats(BaseModel):
    """Labour breakdown stats."""
    total_cost: float
    total_hours: float
    unique_staff: int
    shift_count: int
    avg_cost_per_hour: float


class LabourBreakdownResponse(BaseModel):
    """Labour breakdown by category."""
    venue_id: str
    start_date: str
    end_date: str
    by_day_type: dict
    by_shift_type: dict
    by_employment: dict
    total: dict


class ForecastAccuracyMetrics(BaseModel):
    """Forecast accuracy metrics."""
    mape: Optional[float]  # Mean absolute percentage error
    mae: Optional[float]   # Mean absolute error
    rmse: Optional[float]  # Root mean squared error
    bias: Optional[float]  # Over/under-prediction bias
    samples: int
    per_day_of_week: dict


class ForecastAccuracyResponse(BaseModel):
    """Forecast accuracy response."""
    venue_id: str
    start_date: str
    end_date: str
    metrics: ForecastAccuracyMetrics


class AccuracyHistoryPoint(BaseModel):
    """Weekly accuracy history point."""
    week_ending: str
    mape: Optional[float]
    mae: Optional[float]
    rmse: Optional[float]
    bias: Optional[float]
    samples: int


class AccuracyHistoryResponse(BaseModel):
    """Accuracy history response."""
    venue_id: str
    weeks: int
    history: List[AccuracyHistoryPoint]


class VenueMetrics(BaseModel):
    """Metrics for a single venue."""
    labour_pct: float
    avg_cost_per_cover: float
    avg_staff_util: float
    casual_pct: float
    headcount: int
    total_cost: float
    total_revenue: float


class BenchmarkResponse(BaseModel):
    """Venue benchmarking response."""
    venue_count: int
    venues: dict
    rankings: dict
    outliers: dict


class PeakHourData(BaseModel):
    """Peak hour heatmap data point."""
    day_of_week: str
    hour: int
    avg_headcount: float
    avg_labour_cost: float
    avg_revenue_per_hour: float
    revenue_per_labour_hour: float


class PeakAnalysisResponse(BaseModel):
    """Peak hour analysis response."""
    venue_id: str
    weeks: int
    heatmap: dict
    peak_windows: List[dict]
    dead_zones: List[dict]


class OptimisationInsight(BaseModel):
    """Cost optimisation insight."""
    category: str
    severity: str  # "low", "medium", "high"
    estimated_savings_weekly: float
    recommendation: str
    affected_dates: Optional[int] = None
    affected_shifts: Optional[int] = None


class OptimisationResponse(BaseModel):
    """Cost optimisation response."""
    venue_id: str
    insights: List[OptimisationInsight]


class AnalyticsSummary(BaseModel):
    """All-in-one dashboard summary."""
    venue_id: str
    latest_labour_trend: Optional[dict]
    labour_breakdown: Optional[dict]
    forecast_accuracy: Optional[dict]
    peak_analysis: Optional[dict]
    optimisation_insights: List[OptimisationInsight]


# ============================================================================
# Routes
# ============================================================================


@router.get("/labour-trend/{venue_id}", response_model=LabourTrendResponse)
async def get_labour_trend(
    venue_id: str,
    period: str = Query("daily", regex="^(daily|weekly|monthly)$"),
    days: int = Query(90, ge=7, le=365),
    db: BaseStore = Depends(get_db),
):
    """
    Get labour cost trending over time.

    Args:
        venue_id: Target venue
        period: "daily" (7-day MA), "weekly" (4-week MA), or "monthly"
        days: Lookback period (7-365 days)

    Returns:
        Trend points with labour %, costs, headcount, hours
    """
    try:
        service = AnalyticsService(db)
        trend_points = service.get_labour_trend(venue_id, period, days)

        return LabourTrendResponse(
            venue_id=venue_id,
            period=period,
            days=days,
            trend_points=[LabourTrendPoint(**pt) for pt in trend_points],
        )
    except Exception as e:
        logger.error(f"Error getting labour trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/labour-breakdown/{venue_id}", response_model=LabourBreakdownResponse)
async def get_labour_breakdown(
    venue_id: str,
    start: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    end: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    db: BaseStore = Depends(get_db),
):
    """
    Break down labour costs by day type, shift type, and employment type.

    Args:
        venue_id: Target venue
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)

    Returns:
        Costs and hours grouped by category
    """
    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)

        if start_date >= end_date:
            raise HTTPException(status_code=400, detail="start must be before end")

        service = AnalyticsService(db)
        breakdown = service.get_labour_breakdown(venue_id, start_date, end_date)

        return LabourBreakdownResponse(
            venue_id=venue_id,
            start_date=start,
            end_date=end,
            **breakdown,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error getting labour breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast-accuracy/{venue_id}", response_model=ForecastAccuracyResponse)
async def get_forecast_accuracy(
    venue_id: str,
    days: int = Query(90, ge=7, le=365),
    db: BaseStore = Depends(get_db),
):
    """
    Score forecast accuracy over a date range.

    Compares DemandForecast predictions vs actual roster data.

    Args:
        venue_id: Target venue
        days: Lookback period (default 90)

    Returns:
        MAPE, MAE, RMSE, bias, and per-day breakdown
    """
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        service = AnalyticsService(db)
        metrics = service.score_forecast_accuracy(venue_id, start_date, end_date)

        return ForecastAccuracyResponse(
            venue_id=venue_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            metrics=ForecastAccuracyMetrics(**metrics),
        )
    except Exception as e:
        logger.error(f"Error scoring forecast accuracy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/accuracy-history/{venue_id}", response_model=AccuracyHistoryResponse)
async def get_accuracy_history(
    venue_id: str,
    weeks: int = Query(12, ge=1, le=52),
    db: BaseStore = Depends(get_db),
):
    """
    Get weekly forecast accuracy trend.

    Shows if accuracy is improving or degrading over time.

    Args:
        venue_id: Target venue
        weeks: Number of weeks to look back (default 12)

    Returns:
        List of weekly accuracy scores
    """
    try:
        service = AnalyticsService(db)
        history = service.get_accuracy_history(venue_id, weeks)

        return AccuracyHistoryResponse(
            venue_id=venue_id,
            weeks=weeks,
            history=[AccuracyHistoryPoint(**h) for h in history],
        )
    except Exception as e:
        logger.error(f"Error getting accuracy history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/benchmarks", response_model=BenchmarkResponse)
async def benchmark_venues(
    venue_ids: str = Query(..., description="Comma-separated venue IDs"),
    start: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$|^$"),
    end: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$|^$"),
    db: BaseStore = Depends(get_db),
):
    """
    Compare labour metrics across multiple venues.

    Args:
        venue_ids: Comma-separated list of venue IDs (e.g., "v1,v2,v3")
        start: Optional start date (defaults to 90 days ago)
        end: Optional end date (defaults to today)

    Returns:
        Metrics for each venue, rankings, and outlier detection
    """
    try:
        venues = [v.strip() for v in venue_ids.split(",")]

        start_date = None
        end_date = None

        if start:
            start_date = date.fromisoformat(start)
        if end:
            end_date = date.fromisoformat(end)

        service = AnalyticsService(db)
        result = service.benchmark_venues(venues, start_date, end_date)

        return BenchmarkResponse(
            venue_count=len(venues),
            venues=result["venues"],
            rankings=result["rankings"],
            outliers=result["outliers"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error benchmarking venues: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/peak-hours/{venue_id}", response_model=PeakAnalysisResponse)
async def get_peak_analysis(
    venue_id: str,
    weeks: int = Query(4, ge=1, le=52),
    db: BaseStore = Depends(get_db),
):
    """
    Analyse hourly patterns (7 days × 24 hours heatmap).

    Returns peak windows, dead zones, and optimal staffing recommendations.

    Args:
        venue_id: Target venue
        weeks: Lookback period in weeks (default 4)

    Returns:
        Heatmap data, peak windows, dead zones
    """
    try:
        service = AnalyticsService(db)
        analysis = service.get_peak_analysis(venue_id, weeks)

        return PeakAnalysisResponse(
            venue_id=venue_id,
            weeks=weeks,
            **analysis,
        )
    except Exception as e:
        logger.error(f"Error analysing peak hours: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimisation/{venue_id}", response_model=OptimisationResponse)
async def get_optimisation_opportunities(
    venue_id: str,
    db: BaseStore = Depends(get_db),
):
    """
    Get actionable cost optimisation insights.

    Detects:
    - Overstaffed periods (labour % > 35%)
    - Understaffed peaks
    - Excessive overtime
    - Too many casuals on weekdays
    - Shifts that could be shortened

    Args:
        venue_id: Target venue

    Returns:
        List of insights with severity and estimated savings
    """
    try:
        service = AnalyticsService(db)
        insights = service.get_optimisation_opportunities(venue_id)

        return OptimisationResponse(
            venue_id=venue_id,
            insights=[OptimisationInsight(**i) for i in insights],
        )
    except Exception as e:
        logger.error(f"Error getting optimisation opportunities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary/{venue_id}", response_model=AnalyticsSummary)
async def get_analytics_summary(
    venue_id: str,
    db: BaseStore = Depends(get_db),
):
    """
    All-in-one dashboard summary.

    Combines latest labour trend, breakdown, forecast accuracy,
    peak analysis, and optimisation insights.

    Args:
        venue_id: Target venue

    Returns:
        Complete analytics snapshot
    """
    try:
        service = AnalyticsService(db)

        # Get all components
        trend = service.get_labour_trend(venue_id, "weekly", 90)
        latest_trend = trend[-1] if trend else None

        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        breakdown = service.get_labour_breakdown(venue_id, start_date, end_date)

        accuracy = service.score_forecast_accuracy(venue_id, start_date, end_date)

        peak = service.get_peak_analysis(venue_id, 4)

        insights = service.get_optimisation_opportunities(venue_id)

        return AnalyticsSummary(
            venue_id=venue_id,
            latest_labour_trend=latest_trend,
            labour_breakdown=breakdown,
            forecast_accuracy=accuracy,
            peak_analysis=peak,
            optimisation_insights=[OptimisationInsight(**i) for i in insights],
        )
    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))
