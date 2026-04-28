"""
FastAPI routes for enhanced demand forecasting (v2).

Endpoints:
- POST /api/forecast/v2/train/{venue_id}
- GET /api/forecast/v2/predict
- GET /api/forecast/v2/week/{venue_id}
- GET /api/forecast/v2/staffing/{venue_id}/{date}
- GET /api/forecast/v2/accuracy/{venue_id}
- GET /api/forecast/v2/components/{venue_id}
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from rosteriq.services.forecast_v2 import (
    EnhancedForecaster,
    SeasonalComponents,
    ForecastResult,
    AccuracyReport,
    WeatherForecast,
)
from rosteriq.database import get_db, BaseStore
from rosteriq.models import VenueConfig

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/forecast/v2", tags=["forecast_v2"])

# In-memory store of forecaster instances per venue
_forecasters: dict[str, EnhancedForecaster] = {}


# ============================================================================
# Request/Response Models
# ============================================================================


class PredictRequest(BaseModel):
    """Single prediction request."""
    venue_id: str
    date: date
    hour: int
    weather: Optional[dict] = Field(
        None,
        description="Optional: {temp_celsius, wind_kmh, rain_mm, cloud_cover_pct}",
    )


class PredictResponse(BaseModel):
    """Enhanced prediction response."""
    venue_id: str
    date: str
    hour: int
    point_estimate: float
    confidence_interval_80: list[float]  # [low, high]
    confidence_interval_95: list[float]  # [low, high]
    components: dict[str, float]
    confidence_score: float
    model_version: str


class WeekForecastResponse(BaseModel):
    """Full week forecast."""
    venue_id: str
    week_start: str
    forecasts: list[PredictResponse]


class StaffingRecommendation(BaseModel):
    """Staffing recommendation for an hour."""
    date: str
    hour: int
    recommended_staff: float
    confidence: float
    low: float
    high: float


class StaffingWeekResponse(BaseModel):
    """Full week staffing recommendations."""
    venue_id: str
    date: str
    recommendations: list[StaffingRecommendation]


class ComponentsResponse(BaseModel):
    """Seasonal decomposition components."""
    venue_id: str
    day_of_week: dict[int, float]
    month_of_year: dict[int, float]
    hour_of_day: dict[int, float]
    trend: float
    training_days: int
    last_retrain: str


class AccuracyResponse(BaseModel):
    """Model accuracy report."""
    venue_id: str
    mape: float
    bias: float
    worst_days: list[list]  # [[date, error_pct], ...]
    evaluation_period_days: int
    sample_count: int


class TrainingResponse(BaseModel):
    """Training completion response."""
    venue_id: str
    status: str
    message: str
    training_days: int


# ============================================================================
# Helper Functions
# ============================================================================


def _get_forecaster(venue_id: str, db: BaseStore) -> EnhancedForecaster:
    """Get or create forecaster for a venue."""
    if venue_id in _forecasters:
        return _forecasters[venue_id]

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    # Create new forecaster
    # TODO: integrate with base_forecaster when available
    forecaster = EnhancedForecaster(venue, base_forecaster=None)

    # Load historical data (if available in DB)
    # This is a placeholder — adapt to your actual DB schema
    # forecasts = db.get_forecasts(venue_id=venue_id, start_date=..., end_date=...)

    _forecasters[venue_id] = forecaster
    return forecaster


def _weather_from_dict(weather_dict: Optional[dict]) -> Optional[WeatherForecast]:
    """Convert dict to WeatherForecast."""
    if not weather_dict:
        return None

    return WeatherForecast(
        date=weather_dict.get("date", date.today()),
        hour=weather_dict.get("hour", 12),
        temp_celsius=float(weather_dict.get("temp_celsius", 20.0)),
        wind_kmh=float(weather_dict.get("wind_kmh", 0.0)),
        rain_mm=float(weather_dict.get("rain_mm", 0.0)),
        cloud_cover_pct=int(weather_dict.get("cloud_cover_pct", 50)),
    )


def _forecast_to_response(result: ForecastResult) -> PredictResponse:
    """Convert ForecastResult to response model."""
    return PredictResponse(
        venue_id=result.venue_id,
        date=str(result.date),
        hour=result.hour,
        point_estimate=result.point_estimate,
        confidence_interval_80=[
            round(result.confidence_interval_80[0], 1),
            round(result.confidence_interval_80[1], 1),
        ],
        confidence_interval_95=[
            round(result.confidence_interval_95[0], 1),
            round(result.confidence_interval_95[1], 1),
        ],
        components={k: round(v, 3) for k, v in result.components.items()},
        confidence_score=round(result.confidence_score, 2),
        model_version=result.model_version,
    )


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/train/{venue_id}", response_model=TrainingResponse)
async def train_forecaster(
    venue_id: str,
    db: BaseStore = Depends(get_db),
) -> TrainingResponse:
    """
    Trigger training of seasonal components for a venue.

    Extracts seasonal patterns from historical data.
    """
    try:
        forecaster = _get_forecaster(venue_id, db)

        # Load historical demand data
        # This is a placeholder — adapt to your DB schema
        historical = db.get_forecasts(venue_id=venue_id, start_date=date.today() - timedelta(days=180))
        if historical:
            forecaster.add_historical_data([
                {
                    "date": f.date,
                    "hour": f.hour,
                    "covers": f.predicted_covers,
                }
                for f in historical
            ])

        components = forecaster.retrain()

        return TrainingResponse(
            venue_id=venue_id,
            status="success",
            message=f"Trained on {components.training_days} data points",
            training_days=components.training_days,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Training failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/predict", response_model=PredictResponse)
async def predict_single(
    venue_id: str = Query(...),
    date_str: str = Query(..., alias="date"),
    hour: int = Query(...),
    temp_celsius: Optional[float] = None,
    wind_kmh: Optional[float] = None,
    rain_mm: Optional[float] = None,
    cloud_cover_pct: Optional[int] = None,
    db: BaseStore = Depends(get_db),
) -> PredictResponse:
    """
    Generate prediction for a single venue/date/hour.

    Query parameters:
    - venue_id: Venue ID
    - date: YYYY-MM-DD format
    - hour: 0-23
    - Optional weather: temp_celsius, wind_kmh, rain_mm, cloud_cover_pct
    """
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")

    if not (0 <= hour <= 23):
        raise HTTPException(status_code=400, detail="Hour must be 0-23")

    try:
        forecaster = _get_forecaster(venue_id, db)

        # Build weather forecast if provided
        weather = None
        if any([temp_celsius is not None, wind_kmh is not None, rain_mm is not None]):
            weather = WeatherForecast(
                date=target_date,
                hour=hour,
                temp_celsius=float(temp_celsius or 20.0),
                wind_kmh=float(wind_kmh or 0.0),
                rain_mm=float(rain_mm or 0.0),
                cloud_cover_pct=int(cloud_cover_pct or 50),
            )

        result = forecaster.predict(target_date, hour, weather=weather)
        return _forecast_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/week/{venue_id}", response_model=WeekForecastResponse)
async def forecast_week(
    venue_id: str,
    week_start_str: str = Query(..., alias="week_start"),
    db: BaseStore = Depends(get_db),
) -> WeekForecastResponse:
    """
    Generate full-week forecast (168 hourly predictions).

    Query parameters:
    - venue_id: Venue ID
    - week_start: YYYY-MM-DD format (Monday)
    """
    try:
        week_start = date.fromisoformat(week_start_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")

    # Ensure it's a Monday
    if week_start.weekday() != 0:
        week_start = week_start - timedelta(days=week_start.weekday())
        logger.info(f"Adjusted week_start to Monday: {week_start}")

    try:
        forecaster = _get_forecaster(venue_id, db)
        results = forecaster.forecast_week(week_start)

        forecasts = [_forecast_to_response(r) for r in results]

        return WeekForecastResponse(
            venue_id=venue_id,
            week_start=str(week_start),
            forecasts=forecasts,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Week forecast failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/staffing/{venue_id}/{date_str}", response_model=StaffingWeekResponse)
async def forecast_staffing(
    venue_id: str,
    date_str: str,
    covers_per_staff: float = Query(20.0),
    db: BaseStore = Depends(get_db),
) -> StaffingWeekResponse:
    """
    Convert demand forecast to staffing recommendations.

    Query parameters:
    - venue_id: Venue ID
    - date_str: YYYY-MM-DD format
    - covers_per_staff: Covers per FTE (default 20)
    """
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")

    try:
        forecaster = _get_forecaster(venue_id, db)
        recommendations = forecaster.forecast_staffing_needs(
            target_date,
            covers_per_staff=covers_per_staff,
        )

        return StaffingWeekResponse(
            venue_id=venue_id,
            date=str(target_date),
            recommendations=[
                StaffingRecommendation(**rec) for rec in recommendations
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Staffing forecast failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/accuracy/{venue_id}", response_model=AccuracyResponse)
async def get_accuracy(
    venue_id: str,
    period_days: int = Query(30),
    db: BaseStore = Depends(get_db),
) -> AccuracyResponse:
    """
    Get model accuracy metrics.

    Query parameters:
    - venue_id: Venue ID
    - period_days: Evaluation period (default 30)
    """
    try:
        forecaster = _get_forecaster(venue_id, db)
        report = forecaster.evaluate_accuracy(period_days=period_days)

        return AccuracyResponse(
            venue_id=venue_id,
            mape=round(report.mape, 3),
            bias=round(report.bias, 3),
            worst_days=[
                [str(dt), round(err, 3)] for dt, err in report.worst_days
            ],
            evaluation_period_days=report.evaluation_period_days,
            sample_count=report.sample_count,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Accuracy report failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/components/{venue_id}", response_model=ComponentsResponse)
async def get_components(
    venue_id: str,
    db: BaseStore = Depends(get_db),
) -> ComponentsResponse:
    """
    Get seasonal decomposition components.

    Shows learned day-of-week, month, and hour patterns.
    """
    try:
        forecaster = _get_forecaster(venue_id, db)

        if not forecaster.seasonal_components:
            forecaster.retrain()

        components = forecaster.seasonal_components

        return ComponentsResponse(
            venue_id=venue_id,
            day_of_week={k: round(v, 3) for k, v in components.day_of_week.items()},
            month_of_year={k: round(v, 3) for k, v in components.month_of_year.items()},
            hour_of_day={k: round(v, 3) for k, v in components.hour_of_day.items()},
            trend=round(components.trend, 4),
            training_days=components.training_days,
            last_retrain=components.last_retrain.isoformat(),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Components retrieval failed for {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
