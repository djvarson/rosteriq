"""
Forecast accuracy tracking and analysis routes.

Provides REST endpoints for calculating, comparing, and analyzing
forecast accuracy with detailed breakdowns and recommendations.

All dates in ISO 8601 format (YYYY-MM-DD).
"""

import logging
from datetime import date, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from rosteriq.database import get_db, BaseStore
from rosteriq.services.forecast_accuracy import (
    ForecastAccuracyService, AccuracyReport, WeeklyAccuracy,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics/forecast-accuracy", tags=["forecast-accuracy"])


# ============================================================================
# Pydantic Response Models
# ============================================================================


class DayAccuracyResponse(BaseModel):
    """Accuracy metrics for a day of week."""
    day_of_week: str
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int


class HourAccuracyResponse(BaseModel):
    """Accuracy metrics for an hour of day."""
    hour: int
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int
    avg_predicted: float
    avg_actual: float


class WeeklyAccuracyResponse(BaseModel):
    """Weekly accuracy snapshot."""
    week_ending: str
    mape: Optional[float]
    mae: float
    rmse: float
    samples: int
    directional_accuracy: float


class PredictionDetailResponse(BaseModel):
    """Details of a single prediction."""
    date: str
    hour: int
    predicted_covers: float
    actual_covers: float
    error: float
    absolute_error: float
    percentage_error: Optional[float]
    confidence: float
    signals_used: List[str]


class AccuracyReportResponse(BaseModel):
    """Complete forecast accuracy report."""
    venue_id: str
    period_start: str
    period_end: str

    # Overall metrics
    overall_mape: Optional[float]
    overall_rmse: float
    overall_mae: float
    overall_bias: float
    overall_r_squared: float
    directional_accuracy: float

    # Breakdowns
    by_day_of_week: Dict[str, DayAccuracyResponse]
    by_hour: Dict[int, HourAccuracyResponse]
    by_signal: Dict[str, float]
    weekly_trend: List[WeeklyAccuracyResponse]

    # Best and worst
    worst_predictions: List[PredictionDetailResponse]
    best_predictions: List[PredictionDetailResponse]

    # Recommendations
    recommendations: List[str]

    # Fraction of forecast hours graded against observed actuals (0.0 = none;
    # the metrics above are unmeasurable when this is 0.0).
    data_coverage: float = 0.0
    measured_samples: int = 0


class AccuracyTrendResponse(BaseModel):
    """Accuracy trend over time."""
    venue_id: str
    weeks: int
    trend: List[WeeklyAccuracyResponse]
    trend_direction: str  # "improving", "degrading", "stable"


class ModelComparisonResponse(BaseModel):
    """Comparison of multiple model versions."""
    venue_id: str
    period_start: str
    period_end: str
    models: Dict[str, Dict[str, Any]]  # model_version -> metrics


class PatternsResponse(BaseModel):
    """Identified prediction patterns."""
    venue_id: str
    patterns: List[str]


class ImprovementsResponse(BaseModel):
    """Improvement suggestions."""
    venue_id: str
    suggestions: List[str]


# ============================================================================
# Routes
# ============================================================================


@router.get("", response_model=AccuracyReportResponse)
async def get_forecast_accuracy(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
):
    """
    Calculate comprehensive forecast accuracy metrics.

    Compares DemandForecast predictions against INDEPENDENTLY OBSERVED actuals
    (POS covers/transactions) — never roster-derived numbers, which would grade
    the forecast against itself. Returns MAPE, RMSE, MAE, bias, R², directional
    accuracy, detailed breakdowns by day/hour/signal, and ``data_coverage`` (the
    fraction of forecast hours that had real actuals to grade against).

    Args:
        venue_id: Target venue ID
        start_date: Period start (YYYY-MM-DD)
        end_date: Period end (YYYY-MM-DD)

    Returns:
        Complete accuracy report with metrics and recommendations
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start >= end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        service = ForecastAccuracyService(db)
        report = service.calculate_accuracy(venue_id, start, end)

        return AccuracyReportResponse(
            venue_id=report.venue_id,
            period_start=report.period_start.isoformat(),
            period_end=report.period_end.isoformat(),
            overall_mape=report.overall_mape,
            overall_rmse=report.overall_rmse,
            overall_mae=report.overall_mae,
            overall_bias=report.overall_bias,
            overall_r_squared=report.overall_r_squared,
            directional_accuracy=report.directional_accuracy,
            by_day_of_week={
                dow: DayAccuracyResponse(
                    day_of_week=acc.day_of_week,
                    mape=acc.mape,
                    mae=acc.mae,
                    rmse=acc.rmse,
                    samples=acc.samples,
                )
                for dow, acc in report.by_day_of_week.items()
            },
            by_hour={
                hour: HourAccuracyResponse(
                    hour=acc.hour,
                    mape=acc.mape,
                    mae=acc.mae,
                    rmse=acc.rmse,
                    samples=acc.samples,
                    avg_predicted=acc.avg_predicted,
                    avg_actual=acc.avg_actual,
                )
                for hour, acc in report.by_hour.items()
            },
            by_signal=report.by_signal,
            weekly_trend=[
                WeeklyAccuracyResponse(
                    week_ending=w.week_ending.isoformat(),
                    mape=w.mape,
                    mae=w.mae,
                    rmse=w.rmse,
                    samples=w.samples,
                    directional_accuracy=w.directional_accuracy,
                )
                for w in report.weekly_trend
            ],
            worst_predictions=[
                PredictionDetailResponse(
                    date=p.date.isoformat(),
                    hour=p.hour,
                    predicted_covers=p.predicted_covers,
                    actual_covers=p.actual_covers,
                    error=p.error,
                    absolute_error=p.absolute_error,
                    percentage_error=p.percentage_error,
                    confidence=p.confidence,
                    signals_used=p.signals_used,
                )
                for p in report.worst_predictions
            ],
            best_predictions=[
                PredictionDetailResponse(
                    date=p.date.isoformat(),
                    hour=p.hour,
                    predicted_covers=p.predicted_covers,
                    actual_covers=p.actual_covers,
                    error=p.error,
                    absolute_error=p.absolute_error,
                    percentage_error=p.percentage_error,
                    confidence=p.confidence,
                    signals_used=p.signals_used,
                )
                for p in report.best_predictions
            ],
            recommendations=report.recommendations,
            data_coverage=report.data_coverage,
            measured_samples=report.measured_samples,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error calculating forecast accuracy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trend", response_model=AccuracyTrendResponse)
async def get_accuracy_trend(
    venue_id: str = Query(..., description="Venue ID"),
    weeks: int = Query(12, ge=1, le=52, description="Number of weeks to look back"),
    db: BaseStore = Depends(get_db),
):
    """
    Get weekly accuracy trend showing improvement or degradation.

    Returns a time-series of weekly accuracy snapshots to visualize
    whether forecasting performance is improving or getting worse.

    Args:
        venue_id: Target venue ID
        weeks: Lookback period in weeks (1-52, default 12)

    Returns:
        Weekly accuracy trend with direction indicator
    """
    try:
        service = ForecastAccuracyService(db)
        trend = service.get_accuracy_trend(venue_id, weeks)

        # Determine trend direction
        if len(trend) < 2:
            direction = "stable"
        else:
            first_mae = trend[0].mae
            last_mae = trend[-1].mae
            pct_change = (first_mae - last_mae) / first_mae if first_mae > 0 else 0

            if pct_change > 0.05:
                direction = "improving"
            elif pct_change < -0.05:
                direction = "degrading"
            else:
                direction = "stable"

        return AccuracyTrendResponse(
            venue_id=venue_id,
            weeks=weeks,
            trend=[
                WeeklyAccuracyResponse(
                    week_ending=w.week_ending.isoformat(),
                    mape=w.mape,
                    mae=w.mae,
                    rmse=w.rmse,
                    samples=w.samples,
                    directional_accuracy=w.directional_accuracy,
                )
                for w in trend
            ],
            trend_direction=direction,
        )
    except Exception as e:
        logger.error(f"Error getting accuracy trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare-models", response_model=ModelComparisonResponse)
async def compare_models(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
):
    """
    Compare forecast accuracy across different model versions.

    Groups forecasts by model_version and calculates metrics for each
    to identify which models perform best.

    Args:
        venue_id: Target venue ID
        start_date: Period start (YYYY-MM-DD)
        end_date: Period end (YYYY-MM-DD)

    Returns:
        Accuracy metrics for each model version
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start >= end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        service = ForecastAccuracyService(db)
        comparison = service.compare_models(venue_id, start, end)

        # Convert report objects to dicts for JSON serialization
        models_dict = {}
        for model_version, report in comparison.items():
            models_dict[model_version] = {
                "mae": report.overall_mae,
                "rmse": report.overall_rmse,
                "mape": report.overall_mape,
                "r_squared": report.overall_r_squared,
                "bias": report.overall_bias,
            }

        return ModelComparisonResponse(
            venue_id=venue_id,
            period_start=start_date,
            period_end=end_date,
            models=models_dict,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error comparing models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns", response_model=PatternsResponse)
async def identify_patterns(
    venue_id: str = Query(..., description="Venue ID"),
    db: BaseStore = Depends(get_db),
):
    """
    Identify systematic prediction patterns in recent data.

    Analyzes the last 90 days to detect recurring issues like:
    - Day-of-week bias
    - Time-of-day bias
    - Signal effectiveness issues
    - Systematic over/under-prediction

    Args:
        venue_id: Target venue ID

    Returns:
        List of identified patterns with explanations
    """
    try:
        service = ForecastAccuracyService(db)
        patterns = service.identify_patterns(venue_id)

        return PatternsResponse(
            venue_id=venue_id,
            patterns=patterns,
        )
    except Exception as e:
        logger.error(f"Error identifying patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/improvements", response_model=ImprovementsResponse)
async def suggest_improvements(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
):
    """
    Get ML-informed suggestions to improve forecast accuracy.

    Based on accuracy metrics, provides actionable recommendations
    for improving the forecasting model.

    Args:
        venue_id: Target venue ID
        start_date: Period start (YYYY-MM-DD)
        end_date: Period end (YYYY-MM-DD)

    Returns:
        List of improvement suggestions
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start >= end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        service = ForecastAccuracyService(db)
        report = service.calculate_accuracy(venue_id, start, end)
        suggestions = service.suggest_improvements(report)

        return ImprovementsResponse(
            venue_id=venue_id,
            suggestions=suggestions,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error suggesting improvements: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/worst-predictions", response_model=List[PredictionDetailResponse])
async def get_worst_predictions(
    venue_id: str = Query(..., description="Venue ID"),
    limit: int = Query(10, ge=1, le=50, description="Number of predictions to return"),
    start_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$", description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
):
    """
    Get the worst (most inaccurate) predictions in a date range.

    Returns the predictions with the largest absolute errors,
    useful for debugging what went wrong.

    Args:
        venue_id: Target venue ID
        limit: Number of worst predictions to return (1-50, default 10)
        start_date: Period start (YYYY-MM-DD)
        end_date: Period end (YYYY-MM-DD)

    Returns:
        List of worst predictions with error details
    """
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start >= end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        service = ForecastAccuracyService(db)
        report = service.calculate_accuracy(venue_id, start, end)

        worst = report.worst_predictions[:limit]

        return [
            PredictionDetailResponse(
                date=p.date.isoformat(),
                hour=p.hour,
                predicted_covers=p.predicted_covers,
                actual_covers=p.actual_covers,
                error=p.error,
                absolute_error=p.absolute_error,
                percentage_error=p.percentage_error,
                confidence=p.confidence,
                signals_used=p.signals_used,
            )
            for p in worst
        ]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error getting worst predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))
