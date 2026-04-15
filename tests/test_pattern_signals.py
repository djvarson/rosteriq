"""Tests for pattern_signals.py — conversion of patterns to demand signals."""

import asyncio
from datetime import date, datetime, timedelta, timezone
import pytest

from rosteriq.shift_events import ShiftEventStore, ShiftEvent, EventCategory, PatternLearner
from rosteriq.pattern_signals import patterns_to_signals
from rosteriq.signal_feeds import SignalSourceType, SignalImpactType
from rosteriq.signal_feeds_v2 import EnrichedSignalAggregator


@pytest.fixture
def event_store():
    """Fresh ShiftEventStore for each test."""
    return ShiftEventStore()


def test_patterns_to_signals_empty_store():
    """patterns_to_signals with no events returns empty list."""
    store = ShiftEventStore()
    venue_id = "v001"
    target_date = date(2026, 4, 17)  # Friday

    signals = asyncio.run(patterns_to_signals(store, venue_id, target_date))

    assert signals == []


def test_patterns_to_signals_none_store():
    """patterns_to_signals with None store returns empty list."""
    signals = asyncio.run(patterns_to_signals(None, "v001", date(2026, 4, 17)))
    assert signals == []


def test_patterns_to_signals_four_pub_groups():
    """
    With 4 PUB_GROUP events on distinct Fridays at 18:00,
    patterns_to_signals returns 1 POSITIVE Signal with PATTERN source.
    """
    store = ShiftEventStore()
    venue_id = "v001"

    # Create 4 PUB_GROUP events on different Fridays at 18:00
    # Friday = weekday 4
    fridays = [
        date(2026, 4, 3),    # Friday, week 14
        date(2026, 4, 10),   # Friday, week 15
        date(2026, 4, 17),   # Friday, week 16
        date(2026, 4, 24),   # Friday, week 17
    ]

    for fri in fridays:
        event = ShiftEvent(
            event_id=f"evt_{fri.isoformat()}",
            venue_id=venue_id,
            category=EventCategory.PUB_GROUP,
            description="Pub group came in",
            timestamp=datetime.combine(fri, datetime.min.time(), tzinfo=timezone.utc),
            headcount_at_time=25,
            logged_by="manager1",
            shift_date=fri,
            day_of_week=fri.weekday(),
            hour_of_day=18,
            weather_condition=None,
            active_event_ids=[],
            tags=["peak"],
        )
        store.record(event)

    # Query for any Friday
    target_date = date(2026, 4, 17)  # A Friday
    signals = asyncio.run(patterns_to_signals(store, venue_id, target_date))

    # Should get exactly 1 signal (1 pattern)
    assert len(signals) == 1

    sig = signals[0]
    assert sig.source == SignalSourceType.PATTERN
    assert sig.signal_type == SignalImpactType.POSITIVE
    assert sig.confidence == pytest.approx(1.0, abs=0.01)  # 4 occurrences / 4 weeks
    assert sig.impact_score <= 0.8  # Capped
    assert "Pub group" in sig.description
    assert "observed 4x" in sig.description


def test_patterns_to_signals_hour_filtering():
    """
    With events at different hours, patterns_to_signals filters by hour
    when hour is provided.
    """
    store = ShiftEventStore()
    venue_id = "v001"

    # Create 3 WALK_IN_SURGE events on Fridays at hour 12 (lunch)
    fridays_noon = [
        date(2026, 4, 3),
        date(2026, 4, 10),
        date(2026, 4, 17),
    ]

    for fri in fridays_noon:
        event = ShiftEvent(
            event_id=f"evt_noon_{fri.isoformat()}",
            venue_id=venue_id,
            category=EventCategory.WALK_IN_SURGE,
            description="Lunch surge",
            timestamp=datetime.combine(fri, datetime.min.time(), tzinfo=timezone.utc),
            headcount_at_time=40,
            logged_by="manager1",
            shift_date=fri,
            day_of_week=fri.weekday(),
            hour_of_day=12,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )
        store.record(event)

    # Also add an event at hour 20 (dinner) — should not match hour 12
    event_dinner = ShiftEvent(
        event_id="evt_dinner_2026-04-03",
        venue_id=venue_id,
        category=EventCategory.WALK_IN_SURGE,
        description="Dinner surge",
        timestamp=datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc),
        headcount_at_time=50,
        logged_by="manager1",
        shift_date=date(2026, 4, 3),
        day_of_week=4,
        hour_of_day=20,
        weather_condition=None,
        active_event_ids=[],
        tags=[],
    )
    store.record(event_dinner)

    # Query for Friday at hour 12 (12:00–14:00 window)
    target_date = date(2026, 4, 17)  # Friday
    signals = asyncio.run(patterns_to_signals(store, venue_id, target_date, hour=12))

    # Should get exactly 1 signal (lunch pattern)
    assert len(signals) == 1
    assert "Walk-in surge" in signals[0].description


def test_patterns_to_signals_negative_impact():
    """
    WEATHER_SHIFT events produce NEGATIVE impact signals.
    """
    store = ShiftEventStore()
    venue_id = "v001"

    # Create 3 WEATHER_SHIFT events on Mondays at 19:00
    mondays = [
        date(2026, 4, 6),
        date(2026, 4, 13),
        date(2026, 4, 20),
    ]

    for mon in mondays:
        event = ShiftEvent(
            event_id=f"evt_weather_{mon.isoformat()}",
            venue_id=venue_id,
            category=EventCategory.WEATHER_SHIFT,
            description="Rain reduced foot traffic",
            timestamp=datetime.combine(mon, datetime.min.time(), tzinfo=timezone.utc),
            headcount_at_time=20,
            logged_by="manager2",
            shift_date=mon,
            day_of_week=mon.weekday(),
            hour_of_day=19,
            weather_condition="rainy",
            active_event_ids=[],
            tags=[],
        )
        store.record(event)

    # Query for a Monday
    target_date = date(2026, 4, 6)  # Monday
    signals = asyncio.run(patterns_to_signals(store, venue_id, target_date))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalImpactType.NEGATIVE


def test_enriched_aggregator_with_pattern_store():
    """
    EnrichedSignalAggregator with a pattern_store injects pattern
    signals into collect_all_signals.
    """
    # Create store and add 3 PUB_GROUP events on Fridays
    store = ShiftEventStore()
    venue_id = "v001"

    fridays = [
        date(2026, 4, 3),
        date(2026, 4, 10),
        date(2026, 4, 17),
    ]

    for fri in fridays:
        event = ShiftEvent(
            event_id=f"evt_{fri.isoformat()}",
            venue_id=venue_id,
            category=EventCategory.PUB_GROUP,
            description="Pub group",
            timestamp=datetime.combine(fri, datetime.min.time(), tzinfo=timezone.utc),
            headcount_at_time=25,
            logged_by="mgr",
            shift_date=fri,
            day_of_week=fri.weekday(),
            hour_of_day=18,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )
        store.record(event)

    # Create aggregator with pattern_store
    agg = EnrichedSignalAggregator(
        weather_adapter=None,
        events_adapter=None,
        pattern_store=store,
    )

    # Collect signals for a Friday
    target_date = date(2026, 4, 17)
    signals = asyncio.run(agg.collect_all_signals(venue_id, target_date))

    # Should include pattern signals
    pattern_signals = [s for s in signals if s.source == SignalSourceType.PATTERN]
    assert len(pattern_signals) >= 1
    assert any(
        "Pub group" in s.description and s.signal_type == SignalImpactType.POSITIVE
        for s in pattern_signals
    )


def test_pattern_confidence_capping():
    """
    A pattern with confidence 1.0 produces impact_score <= 0.8
    (confidence * 0.6 capped at 0.8).
    """
    store = ShiftEventStore()
    venue_id = "v001"

    # Create 5 STADIUM_SPILLBACK events on Saturdays
    # (5 events across any number of weeks will have confidence >= 0.8)
    saturdays = [
        date(2026, 4, 5),   # Week 14
        date(2026, 4, 12),  # Week 15
        date(2026, 4, 19),  # Week 16
        date(2026, 4, 26),  # Week 17
        date(2026, 5, 3),   # Week 18
    ]

    for sat in saturdays:
        event = ShiftEvent(
            event_id=f"evt_{sat.isoformat()}",
            venue_id=venue_id,
            category=EventCategory.STADIUM_SPILLBACK,
            description="Stadium event spillover",
            timestamp=datetime.combine(sat, datetime.min.time(), tzinfo=timezone.utc),
            headcount_at_time=60,
            logged_by="mgr",
            shift_date=sat,
            day_of_week=sat.weekday(),
            hour_of_day=17,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )
        store.record(event)

    # Query for a Saturday
    target_date = date(2026, 4, 19)  # Saturday
    signals = asyncio.run(patterns_to_signals(store, venue_id, target_date))

    assert len(signals) >= 1
    for sig in signals:
        if sig.signal_type == SignalImpactType.POSITIVE:
            # impact_score = min(confidence * 0.6, 0.8)
            # confidence should be around 1.0, so impact_score should be 0.6
            assert sig.impact_score <= 0.8
            assert sig.impact_score >= 0.5  # At least 0.6 for high-confidence patterns
