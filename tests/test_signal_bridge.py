"""
Tests for Signal Bridge and EnrichedSignalAggregator
=====================================================

Tests for:
- weather_to_signals: DemoWeatherAdapter → list[Signal]
- events_to_signals: DemoEventsAdapter → list[Signal]
- EnrichedSignalAggregator: combines legacy + new signals with dedup
- get_enriched_forecast_engine: factory function
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.signal_bridge import weather_to_signals, events_to_signals
from rosteriq.signal_feeds import Signal, SignalSourceType, SignalImpactType
from rosteriq.signal_feeds_v2 import EnrichedSignalAggregator
from rosteriq.data_feeds.bom import DemoWeatherAdapter, WeatherForecastDay
from rosteriq.data_feeds.events import DemoEventsAdapter, VenueEvent, EventCategory
from rosteriq.forecast_engine import get_enriched_forecast_engine


def _run(coro):
    """Helper to run async tests without asyncio plugin."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ─────────────────────────────────────────────────────────────────────────────
# weather_to_signals Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_weather_to_signals_7day_range():
    """weather_to_signals returns non-empty list for 7-day range from DemoWeatherAdapter."""
    adapter = DemoWeatherAdapter()
    today = date.today()
    date_range = (today, today + timedelta(days=6))

    signals = _run(weather_to_signals(adapter, "venue_test", date_range))

    assert isinstance(signals, list)
    assert len(signals) > 0
    assert all(isinstance(s, Signal) for s in signals)
    assert all(s.source == SignalSourceType.WEATHER for s in signals)


def test_weather_to_signals_confidence_decay():
    """Confidence is 0.85 for <=3 days, 0.65 beyond."""
    adapter = DemoWeatherAdapter()
    today = date.today()
    date_range = (today, today + timedelta(days=6))

    signals = _run(weather_to_signals(adapter, "venue_test", date_range))

    # Separate near and far signals
    near_signals = [s for s in signals if (s.raw_data.get("date") or date.today()) <= today + timedelta(days=3)]
    far_signals = [s for s in signals if (s.raw_data.get("date") or date.today()) > today + timedelta(days=3)]

    # Near signals should have higher confidence
    if near_signals:
        assert all(s.confidence >= 0.85 for s in near_signals)
    if far_signals:
        assert all(s.confidence <= 0.65 for s in far_signals)


def test_weather_to_signals_heavy_rain():
    """Heavy rain (>=10mm) produces NEGATIVE signal with 0.6 impact."""
    # Create a mock adapter that returns a heavy rain day
    class MockWeatherAdapter:
        async def get_forecast(self, venue_id, days):
            return [
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=date.today(),
                    min_c=18,
                    max_c=22,
                    rain_probability_pct=50,
                    rain_mm_expected=15.0,
                    conditions="heavy_rain",
                    source="mock",
                )
            ]

    adapter = MockWeatherAdapter()
    signals = _run(weather_to_signals(adapter, "venue_test", (date.today(), date.today())))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalImpactType.NEGATIVE
    assert signals[0].impact_score == 0.6
    assert "15.0mm" in signals[0].description


def test_weather_to_signals_rain_probability():
    """Rain probability >=60% produces NEGATIVE signal with 0.3 impact."""
    class MockWeatherAdapter:
        async def get_forecast(self, venue_id, days):
            return [
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=date.today(),
                    min_c=18,
                    max_c=22,
                    rain_probability_pct=70,
                    rain_mm_expected=3.0,
                    conditions="light_rain",
                    source="mock",
                )
            ]

    adapter = MockWeatherAdapter()
    signals = _run(weather_to_signals(adapter, "venue_test", (date.today(), date.today())))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalImpactType.NEGATIVE
    assert signals[0].impact_score == 0.3


def test_weather_to_signals_hot():
    """Hot (>=32°C) produces POSITIVE signal with 0.2 impact."""
    class MockWeatherAdapter:
        async def get_forecast(self, venue_id, days):
            return [
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=date.today(),
                    min_c=25,
                    max_c=35,
                    rain_probability_pct=10,
                    rain_mm_expected=0,
                    conditions="hot",
                    source="mock",
                )
            ]

    adapter = MockWeatherAdapter()
    signals = _run(weather_to_signals(adapter, "venue_test", (date.today(), date.today())))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalImpactType.POSITIVE
    assert signals[0].impact_score == 0.2


def test_weather_to_signals_cold():
    """Cold (<=12°C) produces NEGATIVE signal with 0.2 impact."""
    class MockWeatherAdapter:
        async def get_forecast(self, venue_id, days):
            return [
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=date.today(),
                    min_c=5,
                    max_c=10,
                    rain_probability_pct=20,
                    rain_mm_expected=0,
                    conditions="cold",
                    source="mock",
                )
            ]

    adapter = MockWeatherAdapter()
    signals = _run(weather_to_signals(adapter, "venue_test", (date.today(), date.today())))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalImpactType.NEGATIVE
    assert signals[0].impact_score == 0.2


# ─────────────────────────────────────────────────────────────────────────────
# events_to_signals Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_events_to_signals_demo_adapter():
    """events_to_signals returns POSITIVE signals with valid impact_scores."""
    adapter = DemoEventsAdapter()
    today = date.today()
    date_range = (today, today + timedelta(days=6))

    signals = _run(events_to_signals(adapter, "venue_test", date_range))

    assert isinstance(signals, list)
    assert all(isinstance(s, Signal) for s in signals)
    assert all(s.source == SignalSourceType.EVENTS for s in signals)
    assert all(s.signal_type == SignalImpactType.POSITIVE for s in signals)
    assert all(0 <= s.impact_score <= 1 for s in signals)


def test_events_to_signals_distance_filtering():
    """Events >10km away get skipped (impact_score becomes 0)."""
    class MockEventsAdapter:
        async def get_events(self, venue_id, window_start, window_end):
            return [
                VenueEvent(
                    event_id="distant_event",
                    title="Distant Concert",
                    start_time=datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc),
                    expected_attendance=50000,
                    distance_km_from_venue=15.0,  # >10km, should be skipped
                    category=EventCategory.CONCERT.value,
                    source="mock",
                )
            ]

    adapter = MockEventsAdapter()
    signals = _run(events_to_signals(adapter, "venue_test", (date.today(), date.today())))

    # Should be empty because distance is >10km and impact < 0.05
    assert len(signals) == 0


def test_events_to_signals_close_event():
    """Nearby event (<=10km) with high attendance produces high impact_score."""
    class MockEventsAdapter:
        async def get_events(self, venue_id, window_start, window_end):
            return [
                VenueEvent(
                    event_id="close_stadium",
                    title="Stadium Game",
                    start_time=datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc),
                    expected_attendance=40000,
                    distance_km_from_venue=2.0,  # Close by
                    category=EventCategory.STADIUM.value,
                    source="mock",
                )
            ]

    adapter = MockEventsAdapter()
    signals = _run(events_to_signals(adapter, "venue_test", (date.today(), date.today())))

    assert len(signals) == 1
    signal = signals[0]
    assert signal.signal_type == SignalImpactType.POSITIVE
    # impact = min(40000/20000, 1.0) * max(0, 1 - 2.0/10.0) = 1.0 * 0.8 = 0.8
    assert signal.impact_score > 0.75  # Should be around 0.8


def test_events_to_signals_confidence_by_category():
    """Confidence varies by category: stadium 0.8, concert/festival 0.65, other 0.55."""
    class MockEventsAdapter:
        async def get_events(self, venue_id, window_start, window_end):
            today_dt = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
            return [
                VenueEvent(
                    event_id="stadium",
                    title="Stadium Game",
                    start_time=today_dt,
                    expected_attendance=30000,
                    distance_km_from_venue=1.0,
                    category=EventCategory.STADIUM.value,
                    source="mock",
                ),
                VenueEvent(
                    event_id="concert",
                    title="Concert",
                    start_time=today_dt + timedelta(hours=1),
                    expected_attendance=5000,
                    distance_km_from_venue=1.0,
                    category=EventCategory.CONCERT.value,
                    source="mock",
                ),
                VenueEvent(
                    event_id="comedy",
                    title="Comedy Show",
                    start_time=today_dt + timedelta(hours=2),
                    expected_attendance=1500,  # >0.05 threshold after distance
                    distance_km_from_venue=1.0,
                    category=EventCategory.COMEDY.value,
                    source="mock",
                ),
            ]

    adapter = MockEventsAdapter()
    signals = _run(events_to_signals(adapter, "venue_test", (date.today(), date.today())))

    stadium_sig = [s for s in signals if "Stadium" in s.description][0]
    concert_sig = [s for s in signals if "Concert" in s.description][0]
    comedy_sig = [s for s in signals if "Comedy" in s.description][0]

    assert stadium_sig.confidence == 0.8
    assert concert_sig.confidence == 0.65
    assert comedy_sig.confidence == 0.55


# ─────────────────────────────────────────────────────────────────────────────
# EnrichedSignalAggregator Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_enriched_aggregator_collects_mixed_signals():
    """EnrichedSignalAggregator.collect_all_signals mixes legacy + bridge signals."""
    aggregator = EnrichedSignalAggregator(
        weather_adapter=DemoWeatherAdapter(),
        events_adapter=DemoEventsAdapter(),
    )

    today = date.today()
    signals = _run(aggregator.collect_all_signals("venue_test", today))

    # Should have some signals (weather and events)
    assert isinstance(signals, list)
    assert len(signals) > 0
    assert all(isinstance(s, Signal) for s in signals)


def test_enriched_aggregator_weather_dedup():
    """Weather signals from new adapter are marked with bridge_version v2."""
    class MockWeatherAdapter:
        async def get_forecast(self, venue_id, days):
            return [
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=date.today(),
                    min_c=20,
                    max_c=32,  # Hot
                    rain_probability_pct=10,
                    rain_mm_expected=0,
                    conditions="hot",
                    source="mock",
                )
            ]

    aggregator = EnrichedSignalAggregator(
        weather_adapter=MockWeatherAdapter(),
        events_adapter=None,
    )

    signals = _run(aggregator.collect_all_signals("venue_test", date.today()))

    # Check that new weather signals are marked with bridge_version = "v2"
    weather_signals = [s for s in signals if s.source == SignalSourceType.WEATHER]
    assert len(weather_signals) > 0
    # At least some should be marked as v2
    v2_signals = [s for s in weather_signals if s.raw_data.get("bridge_version") == "v2"]
    assert len(v2_signals) > 0, "No v2 signals found"


def test_enriched_aggregator_init_with_demo_mode():
    """EnrichedSignalAggregator initializes with demo adapters in demo mode."""
    import os

    # Ensure demo mode
    os.environ["ROSTERIQ_DATA_MODE"] = "demo"

    aggregator = EnrichedSignalAggregator()

    assert aggregator.weather_adapter is not None
    assert aggregator.events_adapter is not None


# ─────────────────────────────────────────────────────────────────────────────
# get_enriched_forecast_engine Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_get_enriched_forecast_engine_returns_instance():
    """get_enriched_forecast_engine returns a ForecastEngine with EnrichedSignalAggregator."""
    engine = _run(get_enriched_forecast_engine(demo_mode=True))

    from rosteriq.forecast_engine import ForecastEngine

    assert isinstance(engine, ForecastEngine)
    assert isinstance(engine.signal_aggregator, EnrichedSignalAggregator)


def test_get_enriched_forecast_engine_custom_adapters():
    """get_enriched_forecast_engine accepts custom weather and events adapters."""
    weather_adapter = DemoWeatherAdapter()
    events_adapter = DemoEventsAdapter()

    engine = _run(
        get_enriched_forecast_engine(
            weather_adapter=weather_adapter,
            events_adapter=events_adapter,
            demo_mode=True,
        )
    )

    from rosteriq.forecast_engine import ForecastEngine

    assert isinstance(engine, ForecastEngine)
    assert engine.signal_aggregator.weather_adapter is weather_adapter
    assert engine.signal_aggregator.events_adapter is events_adapter


if __name__ == "__main__":
    # Run tests
    test_weather_to_signals_7day_range()
    print("✓ test_weather_to_signals_7day_range")

    test_weather_to_signals_confidence_decay()
    print("✓ test_weather_to_signals_confidence_decay")

    test_weather_to_signals_heavy_rain()
    print("✓ test_weather_to_signals_heavy_rain")

    test_weather_to_signals_rain_probability()
    print("✓ test_weather_to_signals_rain_probability")

    test_weather_to_signals_hot()
    print("✓ test_weather_to_signals_hot")

    test_weather_to_signals_cold()
    print("✓ test_weather_to_signals_cold")

    test_events_to_signals_demo_adapter()
    print("✓ test_events_to_signals_demo_adapter")

    test_events_to_signals_distance_filtering()
    print("✓ test_events_to_signals_distance_filtering")

    test_events_to_signals_close_event()
    print("✓ test_events_to_signals_close_event")

    test_events_to_signals_confidence_by_category()
    print("✓ test_events_to_signals_confidence_by_category")

    test_enriched_aggregator_collects_mixed_signals()
    print("✓ test_enriched_aggregator_collects_mixed_signals")

    test_enriched_aggregator_weather_dedup()
    print("✓ test_enriched_aggregator_weather_dedup")

    test_enriched_aggregator_init_with_demo_mode()
    print("✓ test_enriched_aggregator_init_with_demo_mode")

    test_get_enriched_forecast_engine_returns_instance()
    print("✓ test_get_enriched_forecast_engine_returns_instance")

    test_get_enriched_forecast_engine_custom_adapters()
    print("✓ test_get_enriched_forecast_engine_custom_adapters")

    print("\nAll tests passed!")
