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
    BOMClient,
    resolve_station,
    haversine_km,
    STATION_MAPPING,
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


# ─────────────────────────────────────────────────────────────────────────────
# BOMClient Tests (new caching + retry logic)
# ─────────────────────────────────────────────────────────────────────────────


@patch("rosteriq.data_feeds.bom.httpx")
async def test_bom_client_constructs_correct_url(mock_httpx_module):
    """BOMClient constructs correct URL for product_id."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"observations": {"data": []}}
    mock_client.get = AsyncMock(return_value=mock_response)

    client = BOMClient()
    await client.get_observations("IDW14400")

    # Verify URL construction
    mock_client.get.assert_called_once()
    call_args = mock_client.get.call_args
    assert "http://reg.bom.gov.au/fwo/IDW14400.json" in str(call_args)


def test_bom_client_constructs_correct_url():
    """Sync wrapper."""
    _run(test_bom_client_constructs_correct_url(MagicMock()))


@patch("rosteriq.data_feeds.bom.httpx")
async def test_bom_client_caches_results(mock_httpx_module):
    """BOMClient caches results for 10 minutes."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"observations": {"data": [{"air_temp": "20"}]}}
    mock_client.get = AsyncMock(return_value=mock_response)

    client = BOMClient()

    # First call
    result1 = await client.get_observations("IDW14400")
    call_count_1 = mock_client.get.call_count

    # Second call (should be cached)
    result2 = await client.get_observations("IDW14400")
    call_count_2 = mock_client.get.call_count

    # Should only have called once due to caching
    assert call_count_1 == 1
    assert call_count_2 == 1
    assert result1 == result2


def test_bom_client_caches_results():
    """Sync wrapper."""
    _run(test_bom_client_caches_results(MagicMock()))


@patch("rosteriq.data_feeds.bom.httpx")
async def test_bom_client_parses_sample_json(mock_httpx_module):
    """BOMClient parses sample BOM observation JSON."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    # Sample BOM observations response
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

    client = BOMClient()
    obs = await client.get_observations("IDW14400")

    assert "observations" in obs
    assert "data" in obs["observations"]
    assert len(obs["observations"]["data"]) > 0


def test_bom_client_parses_sample_json():
    """Sync wrapper."""
    _run(test_bom_client_parses_sample_json(MagicMock()))


# ─────────────────────────────────────────────────────────────────────────────
# Station Resolution Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_resolve_station_perth():
    """Perth lat/lon resolves to IDW14400."""
    # Perth CBD coordinates
    perth_lat, perth_lon = -31.9454, 115.8381
    obs_id, fcst_id = resolve_station(perth_lat, perth_lon)
    assert obs_id == "IDW14400"
    assert fcst_id == "IDW14400"


def test_resolve_station_brisbane():
    """Brisbane lat/lon resolves to IDQ11295."""
    # Brisbane CBD coordinates
    brisbane_lat, brisbane_lon = -27.4698, 153.0251
    obs_id, fcst_id = resolve_station(brisbane_lat, brisbane_lon)
    assert obs_id == "IDQ11295"
    assert fcst_id == "IDQ11295"


def test_resolve_station_gold_coast():
    """Gold Coast lat/lon resolves to Brisbane station (nearest)."""
    # Gold Coast coordinates
    gc_lat, gc_lon = -28.0028, 153.4318
    obs_id, fcst_id = resolve_station(gc_lat, gc_lon)
    # Should resolve to Brisbane (IDQ11295) as nearest station
    assert obs_id == "IDQ11295"


def test_resolve_station_none_coordinates():
    """None coordinates default to Brisbane."""
    obs_id, fcst_id = resolve_station(None, None)
    assert obs_id == "IDQ11295"  # Brisbane default


def test_haversine_perth_to_brisbane():
    """Haversine distance Perth to Brisbane ~3600km."""
    perth_lat, perth_lon = -31.9454, 115.8381
    brisbane_lat, brisbane_lon = -27.4698, 153.0251

    distance = haversine_km(perth_lat, perth_lon, brisbane_lat, brisbane_lon)

    # Perth to Brisbane is roughly 3600km (across Australia)
    assert 3500 <= distance <= 3700, f"Expected ~3600km, got {distance}km"
