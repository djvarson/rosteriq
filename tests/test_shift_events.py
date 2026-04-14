"""Tests for shift_events module: event recording, filtering, and pattern learning."""

import unittest
from datetime import datetime, date, timedelta, timezone

from rosteriq.shift_events import (
    ShiftEvent,
    EventCategory,
    ShiftEventStore,
    PatternLearner,
)


class TestShiftEventStore(unittest.TestCase):
    """Tests for ShiftEventStore basic operations."""

    def setUp(self):
        """Fresh store for each test."""
        self.store = ShiftEventStore()
        self.sample_event = ShiftEvent(
            event_id="evt_abc123",
            venue_id="v_001",
            category=EventCategory.WALK_IN_SURGE,
            description="Unexpected crowd at door",
            timestamp=datetime.now(timezone.utc),
            headcount_at_time=25,
            logged_by="alice",
            shift_date=date.today(),
            day_of_week=4,
            hour_of_day=18,
            weather_condition="clear",
            active_event_ids=["evt_001"],
            tags=["rush_hour"],
        )

    def test_record_and_retrieve(self):
        """Record an event and retrieve it."""
        recorded = self.store.record(self.sample_event)
        self.assertEqual(recorded.event_id, self.sample_event.event_id)

        events = self.store.for_venue("v_001")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_id, self.sample_event.event_id)

    def test_for_shift_filters_by_date(self):
        """for_shift returns only events for a specific date."""
        today = date.today()
        tomorrow = today + timedelta(days=1)

        event_today = ShiftEvent(
            event_id="evt_1",
            venue_id="v_001",
            category=EventCategory.WALK_IN_SURGE,
            description="Today surge",
            timestamp=datetime.now(timezone.utc),
            headcount_at_time=20,
            logged_by="alice",
            shift_date=today,
            day_of_week=today.weekday(),
            hour_of_day=18,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )

        event_tomorrow = ShiftEvent(
            event_id="evt_2",
            venue_id="v_001",
            category=EventCategory.PUB_GROUP,
            description="Tomorrow group",
            timestamp=datetime.now(timezone.utc) + timedelta(days=1),
            headcount_at_time=15,
            logged_by="bob",
            shift_date=tomorrow,
            day_of_week=tomorrow.weekday(),
            hour_of_day=19,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )

        self.store.record(event_today)
        self.store.record(event_tomorrow)

        today_events = self.store.for_shift("v_001", today)
        self.assertEqual(len(today_events), 1)
        self.assertEqual(today_events[0].event_id, "evt_1")

    def test_recent_excludes_old_events(self):
        """recent(hours=6) excludes events older than 6 hours."""
        now = datetime.now(timezone.utc)
        old_time = now - timedelta(hours=12)

        old_event = ShiftEvent(
            event_id="evt_old",
            venue_id="v_001",
            category=EventCategory.WEATHER_SHIFT,
            description="Old event",
            timestamp=old_time,
            headcount_at_time=10,
            logged_by="alice",
            shift_date=date.today(),
            day_of_week=0,
            hour_of_day=12,
            weather_condition="rain",
            active_event_ids=[],
            tags=[],
        )

        recent_event = ShiftEvent(
            event_id="evt_recent",
            venue_id="v_001",
            category=EventCategory.WALK_IN_SURGE,
            description="Recent event",
            timestamp=now - timedelta(hours=2),
            headcount_at_time=25,
            logged_by="bob",
            shift_date=date.today(),
            day_of_week=date.today().weekday(),
            hour_of_day=now.hour,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )

        self.store.record(old_event)
        self.store.record(recent_event)

        recent_6h = self.store.recent("v_001", hours=6)
        self.assertEqual(len(recent_6h), 1)
        self.assertEqual(recent_6h[0].event_id, "evt_recent")

    def test_empty_store_returns_empty_list(self):
        """Empty store returns empty lists from all methods."""
        self.assertEqual(self.store.for_venue("v_unknown"), [])
        self.assertEqual(self.store.for_shift("v_unknown", date.today()), [])
        self.assertEqual(self.store.recent("v_unknown", hours=24), [])
        self.assertEqual(self.store.all(), [])

    def test_clear_venue(self):
        """clear_venue removes all events for a venue."""
        self.store.record(self.sample_event)
        self.assertEqual(len(self.store.for_venue("v_001")), 1)

        self.store.clear_venue("v_001")
        self.assertEqual(len(self.store.for_venue("v_001")), 0)


class TestPatternLearner(unittest.TestCase):
    """Tests for PatternLearner pattern detection."""

    def test_single_occurrence_no_pattern(self):
        """Single event does NOT produce a pattern."""
        event = ShiftEvent(
            event_id="evt_1",
            venue_id="v_001",
            category=EventCategory.PUB_GROUP,
            description="One pub group",
            timestamp=datetime.now(timezone.utc),
            headcount_at_time=15,
            logged_by="alice",
            shift_date=date.today(),
            day_of_week=4,
            hour_of_day=18,
            weather_condition=None,
            active_event_ids=[],
            tags=[],
        )

        patterns = PatternLearner.analyse([event])
        self.assertEqual(len(patterns), 0)

    def test_four_distinct_weeks_pub_group_friday_evening(self):
        """Four pub group events on Fridays at 18:00 across distinct weeks emits high-confidence pattern."""
        base_date = date(2026, 1, 2)
        events = []

        for week_offset in range(4):
            event_date = base_date + timedelta(weeks=week_offset)
            event = ShiftEvent(
                event_id=f"evt_fri_{week_offset}",
                venue_id="v_001",
                category=EventCategory.PUB_GROUP,
                description="Friday evening pub group",
                timestamp=datetime.combine(event_date, datetime.min.time()).replace(tzinfo=timezone.utc),
                headcount_at_time=20 + week_offset,
                logged_by="alice",
                shift_date=event_date,
                day_of_week=event_date.weekday(),
                hour_of_day=18,
                weather_condition=None,
                active_event_ids=[],
                tags=[],
            )
            events.append(event)

        patterns = PatternLearner.analyse(events)
        self.assertGreaterEqual(len(patterns), 1)

        pub_pattern = None
        for p in patterns:
            if p.category == EventCategory.PUB_GROUP and p.weekday == 4:
                pub_pattern = p
                break

        self.assertIsNotNone(pub_pattern)
        self.assertGreaterEqual(pub_pattern.occurrences, 3)
        self.assertGreater(pub_pattern.confidence, 0.5)

    def test_predict_for_returns_relevant_patterns(self):
        """predict_for returns only patterns matching the target date and hour."""
        base_date = date(2026, 1, 2)
        events = []

        for week_offset in range(4):
            event_date = base_date + timedelta(weeks=week_offset)
            event = ShiftEvent(
                event_id=f"evt_fri_{week_offset}",
                venue_id="v_001",
                category=EventCategory.PUB_GROUP,
                description="Friday pub group",
                timestamp=datetime.combine(event_date, datetime.min.time()).replace(tzinfo=timezone.utc),
                headcount_at_time=20,
                logged_by="alice",
                shift_date=event_date,
                day_of_week=4,
                hour_of_day=18,
                weather_condition=None,
                active_event_ids=[],
                tags=[],
            )
            events.append(event)

        target_friday = date(2026, 2, 6)
        applicable = PatternLearner.predict_for("v_001", target_friday, 18, events)

        self.assertGreaterEqual(len(applicable), 1)
        self.assertEqual(applicable[0].category, EventCategory.PUB_GROUP)
        self.assertEqual(applicable[0].weekday, 4)

    def test_predict_for_excludes_irrelevant_patterns(self):
        """predict_for does not return patterns outside the time window."""
        base_date = date(2026, 1, 2)
        events = []

        for week_offset in range(4):
            event_date = base_date + timedelta(weeks=week_offset)
            event = ShiftEvent(
                event_id=f"evt_fri_{week_offset}",
                venue_id="v_001",
                category=EventCategory.PUB_GROUP,
                description="Friday pub group",
                timestamp=datetime.combine(event_date, datetime.min.time()).replace(tzinfo=timezone.utc),
                headcount_at_time=20,
                logged_by="alice",
                shift_date=event_date,
                day_of_week=4,
                hour_of_day=18,
                weather_condition=None,
                active_event_ids=[],
                tags=[],
            )
            events.append(event)

        target_friday = date(2026, 2, 6)
        applicable = PatternLearner.predict_for("v_001", target_friday, 12, events)

        pub_patterns = [p for p in applicable if p.category == EventCategory.PUB_GROUP]
        self.assertEqual(len(pub_patterns), 0)

    def test_predict_for_different_day_excludes_pattern(self):
        """predict_for does not return patterns from different days of week."""
        base_date = date(2026, 1, 2)
        events = []

        for week_offset in range(4):
            event_date = base_date + timedelta(weeks=week_offset)
            event = ShiftEvent(
                event_id=f"evt_fri_{week_offset}",
                venue_id="v_001",
                category=EventCategory.PUB_GROUP,
                description="Friday pub group",
                timestamp=datetime.combine(event_date, datetime.min.time()).replace(tzinfo=timezone.utc),
                headcount_at_time=20,
                logged_by="alice",
                shift_date=event_date,
                day_of_week=4,
                hour_of_day=18,
                weather_condition=None,
                active_event_ids=[],
                tags=[],
            )
            events.append(event)

        target_thursday = date(2026, 2, 5)
        applicable = PatternLearner.predict_for("v_001", target_thursday, 18, events)

        self.assertEqual(len(applicable), 0)


if __name__ == '__main__':
    unittest.main()
