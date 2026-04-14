"""
Weather API Router for RosterIQ
================================

Exposes weather data endpoints:
  - GET /api/v1/weather/current/{venue_id}
  - GET /api/v1/weather/forecast/{venue_id}?days=N

Operates in demo or live mode based on ROSTERIQ_DATA_MODE environment variable.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Query

from rosteriq.data_feeds.bom import (
    DemoWeatherAdapter,
    BOMAdapter,
    BOMFetchError,
    WeatherObservation,
    WeatherForecastDay,
)

logger = logging.getLogger("rosteriq.weather_router")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

DATA_MODE = os.getenv("ROSTERIQ_DATA_MODE", "demo")
BOM_VENUE_CONFIG = {
    # Example config (users would populate this per deployment)
    # "venue_123": {
    #     "product_id_obs": "IDQBRCM0001",
    #     "product_id_forecast": "IDQBRCM0",
    # }
}

# Global adapter instance
_adapter = None


def get_adapter():
    """Lazy-load adapter based on mode."""
    global _adapter
    if _adapter is None:
        if DATA_MODE == "live":
            _adapter = BOMAdapter(BOM_VENUE_CONFIG)
            logger.info("Using live BOM adapter")
        else:
            _adapter = DemoWeatherAdapter()
            logger.info("Using demo weather adapter")
    return _adapter


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


class CurrentWeatherResponse:
    """Current weather observation response."""

    def __init__(self, observation: WeatherObservation):
        self.observation = observation
        self.source = observation.source

    def dict(self) -> Dict[str, Any]:
        return {
            "observation": {
                "venue_id": self.observation.venue_id,
                "timestamp": self.observation.timestamp.isoformat(),
                "temperature_c": self.observation.temperature_c,
                "apparent_temperature_c": self.observation.apparent_temperature_c,
                "rain_mm_last_hour": self.observation.rain_mm_last_hour,
                "humidity_pct": self.observation.humidity_pct,
                "wind_kmh": self.observation.wind_kmh,
                "conditions": self.observation.conditions,
            },
            "source": self.source,
        }


class ForecastResponse:
    """Forecast response."""

    def __init__(self, forecast: list[WeatherForecastDay]):
        self.forecast = forecast

    def dict(self) -> Dict[str, Any]:
        return {
            "forecast": [
                {
                    "venue_id": day.venue_id,
                    "date": day.date.isoformat(),
                    "min_c": day.min_c,
                    "max_c": day.max_c,
                    "rain_probability_pct": day.rain_probability_pct,
                    "rain_mm_expected": day.rain_mm_expected,
                    "conditions": day.conditions,
                }
                for day in self.forecast
            ],
            "source": self.forecast[0].source if self.forecast else "unknown",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/v1/weather", tags=["weather"])


@router.get("/current/{venue_id}")
async def get_current_weather(venue_id: str) -> Dict[str, Any]:
    """
    Get current weather observation for a venue.

    Returns:
        {
            "observation": {
                "venue_id": str,
                "timestamp": ISO datetime,
                "temperature_c": float,
                "apparent_temperature_c": float | null,
                "rain_mm_last_hour": float | null,
                "humidity_pct": int | null,
                "wind_kmh": float | null,
                "conditions": str
            },
            "source": "demo" | "bom"
        }

    Errors:
        - 404: Venue has no BOM configuration (live mode only)
        - 502: Adapter failed to fetch data
    """
    adapter = get_adapter()

    try:
        observation = await adapter.get_current(venue_id)
        response = CurrentWeatherResponse(observation)
        return response.dict()

    except BOMFetchError as e:
        if "No BOM config" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        else:
            logger.error(f"BOM fetch error: {e}")
            raise HTTPException(status_code=502, detail="Weather service unavailable")

    except Exception as e:
        logger.error(f"Unexpected error fetching weather: {e}")
        raise HTTPException(status_code=502, detail="Weather service error")


@router.get("/forecast/{venue_id}")
async def get_forecast(
    venue_id: str, days: int = Query(7, ge=1, le=14)
) -> Dict[str, Any]:
    """
    Get weather forecast for a venue.

    Query Parameters:
        days: Number of days to forecast (1-14, default 7)

    Returns:
        {
            "forecast": [
                {
                    "venue_id": str,
                    "date": ISO date,
                    "min_c": float,
                    "max_c": float,
                    "rain_probability_pct": float (0-100),
                    "rain_mm_expected": float,
                    "conditions": str
                },
                ...
            ],
            "source": "demo" | "bom"
        }

    Errors:
        - 400: Invalid days parameter
        - 404: Venue has no BOM configuration (live mode only)
        - 502: Adapter failed to fetch data
    """
    adapter = get_adapter()

    try:
        forecast = await adapter.get_forecast(venue_id, days=days)
        response = ForecastResponse(forecast)
        return response.dict()

    except BOMFetchError as e:
        if "No BOM config" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        else:
            logger.error(f"BOM fetch error: {e}")
            raise HTTPException(status_code=502, detail="Weather service unavailable")

    except Exception as e:
        logger.error(f"Unexpected error fetching forecast: {e}")
        raise HTTPException(status_code=502, detail="Weather service error")
