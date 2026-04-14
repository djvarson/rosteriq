"""
Tests for BOM Weather Adapter
=============================

Tests for:
- categorise_conditions helper
- DemoWeatherAdapter (realistic AU hospitality weather)
- BOMAdapter with mocked httpx responses
"""

import asyncio
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.data_feeds.bom import (
    categorise_conditions,
    DemoWeatherAdapter,
    BOMAdapter,
    BOMFetchError,
    WeatherObservation,
    WeatherForecastDay,
)


def _run(coro):
    """Helper to run async tests without asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ─────────────────────────────────────────────────────────────────────────────
# categorise_conditions Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_categorise_conditions_clear():
    """Clear skies: low rain prob, no rain expected."""
    result = categorise_conditions(rain_mm_expected=0.0, rain_prob=10.0, max_c=22.0)
    assert result == "clear"


def test_categorise_conditions_cloudy():
    """Cloudy: moderate rain prob, no expected rain."""
    result = categorise_conditions(rain_mm_expected=0.0, rain_prob=30.0, max_c=22.0)
    assert result == "cloudy"


def test_categorise_conditions_light_rain():
    """Light rain: some rain expected but < 10mm."""
    result = categorise_conditions(rain_mm_expected=3.5, rain_prob=40.0, max_c=20.0)
    assert result == "light_rain"


def test_categorise_conditions_heavy_rain_by_mm():
    """Heavy rain: >= 10mm expected."""
    result = categorise_conditions(rain_mm_expected=12.0, rain_prob=50.0, max_c=20.0)
    assert result == "heavy_rain"


def test_categorise_conditions_heavy_rain_by_probability():
    """Heavy rain: >= 80% probability."""
    result = categorise_conditions(rain_mm_expected=2.0, rain_prob=85.0, max_c=20.0)
    assert result == "heavy_rain"


def test_categorise_conditions_storm():
    """Storm: moderate rain with moderate-high probability."""
    result = categorise_conditions(rain_mm_expected=6.0, rain_prob=50.0, max_c=22.0)
    assert result == "storm"


def test_categorise_conditions_hot():
    """Hot: >= 32°C."""
    result = categorise_conditions(rain_mm_expected=0.0, rain_prob=10.0, max_c=35.0)
    assert result == "hot"


def test_categorise_conditions_cold():
    """Cold: <= 12°C."""
    result = categorise_conditions(rain_mm_expected=0.0, rain_prob=10.0, max_c=8.0)
    assert result == "cold"


# ─────────────────────────────────────────────────────────────────────────────
# DemoWeatherAdapter Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_demo_get_current_returns_observation():
    """DemoWeatherAdapter.get_current returns a valid WeatherObservation."""
    adapter = DemoWeatherAdapter()
    obs = _run(adapter.get_current("venue_test_01"))

    assert isinstance(obs, WeatherObservation)
    assert obs.venue_id == "venue_test_01"
    assert isinstance(obs.timestamp, datetime)
    assert isinstance(obs.temperature_c, float)
    assert obs.source == "demo"
    assert obs.conditions in [
        "clear",
        "cloudy",
        "light_rain",
        "heavy_rain",
        "storm",
        "hot",
        "cold",
    ]


def test_demo_get_forecast_returns_days():
    """DemoWeatherAdapter.get_forecast(days=7) returns 7 entries."""
    adapter = DemoWeatherAdapter()
    forecast = _run(adapter.get_forecast("venue_test_02", days=7))

    assert len(forecast) == 7
    for day in forecast:
        assert isinstance(day, WeatherForecastDay)
        assert day.venue_id == "venue_test_02"
        assert isinstance(day.date, date)
        assert 0 <= day.rain_probability_pct <= 100
        assert day.rain_mm_expected >= 0


def test_demo_forecast_dates_strictly_increasing():
    """Forecast dates must be strictly increasing."""
    adapter = DemoWeatherAdapter()
    forecast = _run(adapter.get_forecast("venue_test_03", days=7))

    for i in range(len(forecast) - 1):
        assert forecast[i].date < forecast[i + 1].date


def test_demo_stability_within_session():
    """Two calls with same venue_id return identical forecast within session."""
    adapter = DemoWeatherAdapter()

    forecast1 = _run(adapter.get_forecast("venue_stable", days=5))
    forecast2 = _run(adapter.get_forecast("venue_stable", days=5))

    assert len(forecast1) == len(forecast2)
    for d1, d2 in zip(forecast1, forecast2):
        assert d1.date == d2.date
        assert d1.min_c == d2.min_c
        assert d1.max_c == d2.max_c
        assert d1.rain_probability_pct == d2.rain_probability_pct
        assert d1.rain_mm_expected == d2.rain_mm_expected


# ─────────────────────────────────────────────────────────────────────────────
# BOMAdapter Tests (with mocked httpx)
# ─────────────────────────────────────────────────────────────────────────────


@patch("rosteriq.data_feeds.bom.httpx")
async def test_bom_get_current_parses_observation(mock_httpx_module):
    """BOMAdapter parses BOM observations correctly."""
    # Mock httpx.AsyncClient
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    # Mock BOM API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "observations": {
            "data": [
                {
                    "air_temp": "23.5",
                    "apparent_t": "21.0",
                    "rain_trace": "0.5",
                    "rel_hum": "65",
                    "wind_spd_kmh": "12.3",
                    "local_date_time_full": "2026-04-15T14:30:00+10:00",
                }
            ]
        }
    }
    mock_client.get = AsyncMock(return_value=mock_response)

    # Create adapter with minimal config
    config = {
        "venue_123": {
            "product_id_obs": "IDTEST001",
            "product_id_forecast": "IDTEST002",
        }
    }
    adapter = BOMAdapter(config)
    adapter._client = mock_client

    obs = await adapter.get_current("venue_123")

    assert isinstance(obs, WeatherObservation)
    assert obs.venue_id == "venue_123"
    assert obs.temperature_c == 23.5
    assert obs.apparent_temperature_c == 21.0
    assert obs.rain_mm_last_hour == 0.5
    assert obs.humidity_pct == 65
    assert obs.wind_kmh == 12.3
    assert obs.source == "bom"


@patch("rosteriq.data_feeds.bom.httpx")
async def test_bom_raises_on_non_200(mock_httpx_module):
    """BOMAdapter raises BOMFetchError on non-200 response."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_client.get = AsyncMock(return_value=mock_response)

    config = {
        "venue_456": {
            "product_id_obs": "IDTEST003",
            "product_id_forecast": "IDTEST004",
        }
    }
    adapter = BOMAdapter(config)
    adapter._client = mock_client

    try:
        await adapter.get_current("venue_456")
        assert False, "Should have raised BOMFetchError"
    except BOMFetchError as e:
        assert "404" in str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Run async tests using _run helper
# ─────────────────────────────────────────────────────────────────────────────


def test_bom_get_current_parses_observation():
    """Sync wrapper for async test."""
    _run(test_bom_get_current_parses_observation(MagicMock()))


def test_bom_raises_on_non_200():
    """Sync wrapper for async test."""
    _run(test_bom_raises_on_non_200(MagicMock()))
