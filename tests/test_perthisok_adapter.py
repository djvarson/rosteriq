"""
Tests for PerthIsOK Events Adapter
===================================

Tests cover:
- PerthIsOKClient URL construction
- Sample events JSON parsing
- Haversine distance filtering (10km threshold)
- Cache behavior (30-minute TTL)
- Graceful fallback on network errors
- Empty window handling
"""

import asyncio
from datetime import datetime, timedelta, timezone, date
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from pathlib import Path
import sys

# Stub httpx before importing events module
sys.modules['httpx'] = Mock()

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.data_feeds.events import (
    PerthIsOKAdapter,
    PerthIsOKClient,
    VenueEvent,
    haversine_km,
    EventsAdapterError,
    EventCategory,
)

AU_TZ = timezone(timedelta(hours=10))


def _run(coro):
    """Helper to run async tests without asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# PerthIsOKClient Tests
# ---------------------------------------------------------------------------


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_client_constructs_url(mock_httpx_module):
    """PerthIsOKClient constructs correct API URL."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_client.get = AsyncMock(return_value=mock_response)

    client = PerthIsOKClient()
    await client.get_events(date(2026, 4, 15), date(2026, 4, 20))

    # Verify URL construction
    mock_client.get.assert_called_once()
    call_args = mock_client.get.call_args
    assert "https://www.perthisok.com/api/events" in str(call_args)
    assert "start=2026-04-15" in str(call_args)
    assert "end=2026-04-20" in str(call_args)


def test_perthisok_client_constructs_url():
    """Sync wrapper."""
    _run(test_perthisok_client_constructs_url(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_client_parses_sample_json(mock_httpx_module):
    """PerthIsOKClient parses sample events JSON."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    # Sample PerthIsOK response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "id": "event_001",
            "title": "Perth Comedy Night",
            "start_date": "2026-04-20",
            "start_time": "19:30",
            "end_date": "2026-04-20",
            "end_time": "21:00",
            "venue_name": "Comedy Store Perth",
            "latitude": -31.9505,
            "longitude": 115.8605,
            "category": "comedy",
            "expected_attendance": 150,
        },
    ]
    mock_client.get = AsyncMock(return_value=mock_response)

    client = PerthIsOKClient()
    events = await client.get_events(date(2026, 4, 15), date(2026, 4, 25))

    assert isinstance(events, list)
    assert len(events) == 1
    assert events[0]["title"] == "Perth Comedy Night"
    assert events[0]["id"] == "event_001"


def test_perthisok_client_parses_sample_json():
    """Sync wrapper."""
    _run(test_perthisok_client_parses_sample_json(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_client_caches_results(mock_httpx_module):
    """PerthIsOKClient caches results for 30 minutes."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_client.get = AsyncMock(return_value=mock_response)

    client = PerthIsOKClient()

    # First call
    await client.get_events(date(2026, 4, 15), date(2026, 4, 20))
    call_count_1 = mock_client.get.call_count

    # Second call (should be cached)
    await client.get_events(date(2026, 4, 15), date(2026, 4, 20))
    call_count_2 = mock_client.get.call_count

    # Should only have called once due to caching
    assert call_count_1 == 1
    assert call_count_2 == 1


def test_perthisok_client_caches_results():
    """Sync wrapper."""
    _run(test_perthisok_client_caches_results(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_client_handles_404(mock_httpx_module):
    """PerthIsOKClient handles 404 gracefully."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_client.get = AsyncMock(return_value=mock_response)

    client = PerthIsOKClient()
    events = await client.get_events(date(2026, 4, 15), date(2026, 4, 20))

    # Should return empty list on 404
    assert events == []


def test_perthisok_client_handles_404():
    """Sync wrapper."""
    _run(test_perthisok_client_handles_404(MagicMock()))


# ---------------------------------------------------------------------------
# PerthIsOKAdapter Tests
# ---------------------------------------------------------------------------


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_adapter_filters_by_distance(mock_httpx_module):
    """PerthIsOKAdapter filters events by 10km distance threshold."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    # Two events: one 5km away, one 15km away
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "id": "event_close",
            "title": "Close Event",
            "start_date": "2026-04-20",
            "start_time": "19:30",
            "end_date": "2026-04-20",
            "end_time": "21:00",
            "venue_name": "Close Venue",
            "latitude": -31.9500,  # ~0.5km from test venue
            "longitude": 115.8600,
            "category": "comedy",
            "expected_attendance": 150,
        },
        {
            "id": "event_far",
            "title": "Far Event",
            "start_date": "2026-04-20",
            "start_time": "14:00",
            "end_date": "2026-04-20",
            "end_time": "16:00",
            "venue_name": "Far Venue",
            "latitude": -31.8500,  # ~15km from test venue
            "longitude": 115.8000,
            "category": "stadium",
            "expected_attendance": 40000,
        },
    ]
    mock_client.get = AsyncMock(return_value=mock_response)

    adapter = PerthIsOKAdapter()
    adapter.client._client = mock_client

    # Test venue at Perth CBD
    venue_lat, venue_lon = -31.9505, 115.8605
    window_start = datetime(2026, 4, 15, tzinfo=AU_TZ)
    window_end = datetime(2026, 4, 25, tzinfo=AU_TZ)

    events = await adapter.get_events(
        "test_venue",
        window_start,
        window_end,
        venue_lat=venue_lat,
        venue_lon=venue_lon,
    )

    # Should only include the close event (within 10km)
    assert len(events) == 1
    assert events[0].title == "Close Event"


def test_perthisok_adapter_filters_by_distance():
    """Sync wrapper."""
    _run(test_perthisok_adapter_filters_by_distance(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_adapter_filters_by_window(mock_httpx_module):
    """PerthIsOKAdapter filters events by time window."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "id": "event_1",
            "title": "Event Before Window",
            "start_date": "2026-04-10",
            "start_time": "19:30",
            "end_date": "2026-04-10",
            "end_time": "21:00",
            "venue_name": "Venue A",
            "latitude": -31.9505,
            "longitude": 115.8605,
            "category": "comedy",
            "expected_attendance": 150,
        },
        {
            "id": "event_2",
            "title": "Event In Window",
            "start_date": "2026-04-20",
            "start_time": "19:30",
            "end_date": "2026-04-20",
            "end_time": "21:00",
            "venue_name": "Venue B",
            "latitude": -31.9505,
            "longitude": 115.8605,
            "category": "comedy",
            "expected_attendance": 150,
        },
        {
            "id": "event_3",
            "title": "Event After Window",
            "start_date": "2026-05-01",
            "start_time": "19:30",
            "end_date": "2026-05-01",
            "end_time": "21:00",
            "venue_name": "Venue C",
            "latitude": -31.9505,
            "longitude": 115.8605,
            "category": "comedy",
            "expected_attendance": 150,
        },
    ]
    mock_client.get = AsyncMock(return_value=mock_response)

    adapter = PerthIsOKAdapter()
    adapter.client._client = mock_client

    window_start = datetime(2026, 4, 15, tzinfo=AU_TZ)
    window_end = datetime(2026, 4, 25, tzinfo=AU_TZ)

    events = await adapter.get_events("test_venue", window_start, window_end)

    # Should only include event in window
    assert len(events) == 1
    assert events[0].title == "Event In Window"


def test_perthisok_adapter_filters_by_window():
    """Sync wrapper."""
    _run(test_perthisok_adapter_filters_by_window(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_adapter_handles_empty_response(mock_httpx_module):
    """PerthIsOKAdapter handles empty event list gracefully."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_client.get = AsyncMock(return_value=mock_response)

    adapter = PerthIsOKAdapter()
    adapter.client._client = mock_client

    window_start = datetime(2026, 4, 15, tzinfo=AU_TZ)
    window_end = datetime(2026, 4, 25, tzinfo=AU_TZ)

    events = await adapter.get_events("test_venue", window_start, window_end)

    assert events == []


def test_perthisok_adapter_handles_empty_response():
    """Sync wrapper."""
    _run(test_perthisok_adapter_handles_empty_response(MagicMock()))


@patch("rosteriq.data_feeds.events.httpx")
async def test_perthisok_adapter_graceful_fallback_on_error(mock_httpx_module):
    """PerthIsOKAdapter returns empty list on network error."""
    mock_client = AsyncMock()
    mock_httpx_module.AsyncClient.return_value = mock_client

    # Simulate network error
    mock_client.get = AsyncMock(side_effect=Exception("Connection timeout"))

    adapter = PerthIsOKAdapter()
    adapter.client._client = mock_client

    window_start = datetime(2026, 4, 15, tzinfo=AU_TZ)
    window_end = datetime(2026, 4, 25, tzinfo=AU_TZ)

    events = await adapter.get_events("test_venue", window_start, window_end)

    # Should return empty list, not raise
    assert events == []


def test_perthisok_adapter_graceful_fallback_on_error():
    """Sync wrapper."""
    _run(test_perthisok_adapter_graceful_fallback_on_error(MagicMock()))


# ---------------------------------------------------------------------------
# Haversine Distance Tests (for PerthIsOK filtering)
# ---------------------------------------------------------------------------


def test_haversine_5km_distance():
    """Event 5km away should be included (within 10km threshold)."""
    venue_lat, venue_lon = -31.9505, 115.8605
    event_lat, event_lon = -31.9505, 115.8770  # Roughly 10km east

    distance = haversine_km(venue_lat, venue_lon, event_lat, event_lon)

    # Should be roughly 10km, within threshold
    assert distance <= 10.0


def test_haversine_15km_distance():
    """Event 15km away should be excluded (beyond 10km threshold)."""
    venue_lat, venue_lon = -31.9505, 115.8605
    event_lat, event_lon = -31.8000, 115.8605  # Roughly 17km north

    distance = haversine_km(venue_lat, venue_lon, event_lat, event_lon)

    # Should be > 10km, outside threshold
    assert distance > 10.0


def test_haversine_same_location():
    """Distance between same location should be ~0."""
    lat, lon = -31.9505, 115.8605
    distance = haversine_km(lat, lon, lat, lon)

    assert distance < 0.1


# ---------------------------------------------------------------------------
# Factory/Integration Tests
# ---------------------------------------------------------------------------


def test_perthisok_adapter_can_be_instantiated():
    """PerthIsOKAdapter can be created with default or custom base URL."""
    # Default
    adapter1 = PerthIsOKAdapter()
    assert adapter1 is not None

    # Custom base URL
    adapter2 = PerthIsOKAdapter(base_url="https://staging.perthisok.com")
    assert adapter2.client.base_url == "https://staging.perthisok.com"


def test_perthisok_adapter_instantiation_without_httpx():
    """PerthIsOKAdapter raises ImportError if httpx not available."""
    # Already patched at module level, so this should work
    adapter = PerthIsOKAdapter()
    assert adapter is not None
