"""
Tests for Events Adapter
========================

Tests cover:
- haversine_km distance calculation
- DemoEventsAdapter determinism and window filtering
- CompositeEventsAdapter deduplication
- PerthIsOKAdapter JSON parsing
- Empty window handling
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from pathlib import Path
import sys

# Stub httpx before importing events module (httpx may not be installed)
sys.modules['httpx'] = Mock()

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.data_feeds.events import (
    DemoEventsAdapter,
    PerthIsOKAdapter,
    CompositeEventsAdapter,
    VenueEvent,
    haversine_km,
    EventCategory,
)

AU_TZ = timezone(timedelta(hours=10))


def _run(coro):
    """Small helper to run async tests without an asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Haversine Tests
# ---------------------------------------------------------------------------

def test_haversine_brisbane_to_gold_coast():
    """Test haversine on a known distance (Brisbane CBD to Gold Coast ≈ 70km)."""
    brisbane_lat, brisbane_lon = -27.4698, 153.0251
    gold_coast_lat, gold_coast_lon = -28.0028, 153.4318

    distance = haversine_km(brisbane_lat, brisbane_lon, gold_coast_lat, gold_coast_lon)

    # Gold Coast is roughly 70km south of Brisbane
    # Allow ±5km tolerance
    assert 65 <= distance <= 75, f"Expected ~70km, got {distance}km"


def test_haversine_same_point():
    """Distance from a point to itself should be ~0."""
    distance = haversine_km(-27.4698, 153.0251, -27.4698, 153.0251)
    assert distance < 0.1


# ---------------------------------------------------------------------------
# Demo Adapter Tests
# ---------------------------------------------------------------------------

def test_demo_adapter_returns_events_in_window():
    """Demo adapter should return non-empty list for a 7-day window."""
    adapter = DemoEventsAdapter()
    now = datetime.now(AU_TZ)
    window_start = now
    window_end = now + timedelta(days=7)

    events = _run(adapter.get_events("demo_venue", window_start, window_end))

    assert len(events) > 0, "Demo adapter should return events for 7-day window"


def test_demo_adapter_filters_to_window():
    """Events should all be within [window_start, window_end]."""
    adapter = DemoEventsAdapter()
    now = datetime.now(AU_TZ)
    window_start = now
    window_end = now + timedelta(days=7)

    events = _run(adapter.get_events("demo_venue", window_start, window_end))

    for event in events:
        assert window_start <= event.start_time <= window_end, \
            f"Event {event.title} at {event.start_time} outside window"


def test_demo_adapter_valid_categories():
    """All event categories should be valid enum values."""
    adapter = DemoEventsAdapter()
    now = datetime.now(AU_TZ)
    window_start = now
    window_end = now + timedelta(days=7)

    events = _run(adapter.get_events("demo_venue", window_start, window_end))
    valid_categories = {c.value for c in EventCategory}

    for event in events:
        assert event.category in valid_categories, \
            f"Event {event.title} has invalid category: {event.category}"


def test_demo_adapter_deterministic():
    """Same venue_id should produce same events (deterministic)."""
    adapter = DemoEventsAdapter()
    now = datetime.now(AU_TZ)
    window_start = now
    window_end = now + timedelta(days=7)

    events1 = _run(adapter.get_events("venue_123", window_start, window_end))
    events2 = _run(adapter.get_events("venue_123", window_start, window_end))

    assert len(events1) == len(events2), "Event count should be deterministic"
    for e1, e2 in zip(events1, events2):
        assert e1.title == e2.title, f"Title mismatch: {e1.title} vs {e2.title}"
        assert e1.start_time == e2.start_time, f"Time mismatch: {e1.start_time} vs {e2.start_time}"


def test_demo_adapter_tz_aware():
    """All events should be tz-aware."""
    adapter = DemoEventsAdapter()
    now = datetime.now(AU_TZ)
    window_start = now
    window_end = now + timedelta(days=7)

    events = _run(adapter.get_events("demo_venue", window_start, window_end))

    for event in events:
        assert event.start_time.tzinfo is not None, f"Event {event.title} missing tzinfo"
        if event.end_time:
            assert event.end_time.tzinfo is not None, f"Event {event.title} end_time missing tzinfo"


# ---------------------------------------------------------------------------
# Composite Adapter Tests
# ---------------------------------------------------------------------------

def test_composite_adapter_dedupes_by_title_and_time():
    """CompositeEventsAdapter should dedupe by (title, start_time)."""

    async def get_dup_events():
        # Create two mock adapters returning duplicate events
        event1 = VenueEvent(
            event_id="evt_1",
            title="Same Event",
            start_time=datetime(2026, 4, 20, 19, 0, tzinfo=AU_TZ),
            category=EventCategory.STADIUM.value,
            source="adapter1",
        )
        event2 = VenueEvent(
            event_id="evt_2",
            title="Same Event",
            start_time=datetime(2026, 4, 20, 19, 0, tzinfo=AU_TZ),
            category=EventCategory.CONCERT.value,
            source="adapter2",
        )

        adapter1 = AsyncMock()
        adapter1.get_events = AsyncMock(return_value=[event1])

        adapter2 = AsyncMock()
        adapter2.get_events = AsyncMock(return_value=[event2])

        composite = CompositeEventsAdapter([adapter1, adapter2])
        now = datetime.now(AU_TZ)
        events = await composite.get_events("venue", now, now + timedelta(days=7))

        return events

    events = _run(get_dup_events())

    # Should have only 1 event (deduped)
    assert len(events) == 1, f"Expected 1 event after dedup, got {len(events)}"


# ---------------------------------------------------------------------------
# PerthIsOK Adapter Tests
# ---------------------------------------------------------------------------

def test_perthisok_adapter_parses_json():
    """PerthIsOKAdapter should parse mocked JSON response."""

    async def parse_mock_response():
        # Mock the httpx client
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "event_123",
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
        mock_response.raise_for_status = MagicMock()

        # Mock httpx.AsyncClient directly on the module
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False
        mock_client.aclose = AsyncMock()

        import rosteriq.data_feeds.events as events_module
        events_module.httpx.AsyncClient = MagicMock(return_value=mock_client)

        adapter = PerthIsOKAdapter()
        window_start = datetime(2026, 4, 20, 0, 0, tzinfo=AU_TZ)
        window_end = datetime(2026, 4, 21, 23, 59, tzinfo=AU_TZ)

        events = await adapter.get_events("venue_123", window_start, window_end)
        await adapter.close()

        return events

    events = _run(parse_mock_response())

    assert len(events) == 1, f"Expected 1 event, got {len(events)}"
    assert events[0].title == "Perth Comedy Night"
    assert events[0].category == EventCategory.COMEDY.value
    assert events[0].source == "perthisok"


# ---------------------------------------------------------------------------
# Empty Window Tests
# ---------------------------------------------------------------------------

def test_empty_window_returns_empty_list():
    """Events outside the window should return empty list."""

    async def get_empty():
        adapter = DemoEventsAdapter()
        # Create a very short window (1 hour) on a Monday at midnight
        # Demo adapter generates events on Sat/Sun, so this should be empty
        window_start = datetime(2026, 4, 13, 0, 0, tzinfo=AU_TZ)  # Monday
        window_end = datetime(2026, 4, 13, 1, 0, tzinfo=AU_TZ)    # 1 hour later, still Monday

        events = await adapter.get_events("venue", window_start, window_end)
        return events

    events = _run(get_empty())
    assert len(events) == 0, f"Single-hour Monday window should return no events, got {len(events)}"
