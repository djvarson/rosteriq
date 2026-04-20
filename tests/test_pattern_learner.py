"""Tests for rosteriq.pattern_learner — pure stdlib, no pytest.

Pattern detection and learning loop module. Tests cover:
- Pattern dataclass and serialization
- Store add/list/deactivate operations
- All detector functions
- Full run_detection pipeline
- Persistence roundtrip
"""

from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import pattern_learner as pl
from rosteriq import persistence as _p
from rosteriq.headcount import HeadcountEntry, ShiftNote, HeadcountStore, ShiftNoteStore
from rosteriq.tanda_history import DailyActuals

AU_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def reset_all_stores() -> None:
    """Reset pattern store and persistence for test isolation."""
    global _pattern_store
    pl._store = None
    _p.reset_for_tests()
    _p.reset_rehydrate_for_tests()


# ---------------------------------------------------------------------------
# Test: LearnedPattern
# ---------------------------------------------------------------------------


class TestLearnedPattern(unittest.TestCase):
    """Test LearnedPattern dataclass."""

    def test_pattern_defaults(self):
        """Pattern created with defaults has valid structure."""
        p = pl.LearnedPattern()
        self.assertEqual(len(p.pattern_id), 12)  # uuid4().hex[:12]
        self.assertEqual(p.confidence, 0.0)
        self.assertEqual(p.evidence_count, 0)
        self.assertTrue(p.active)
        self.assertEqual(p.tags, [])

    def test_pattern_to_dict(self):
        """Pattern.to_dict() returns serializable dict."""
        p = pl.LearnedPattern(
            venue_id="v1",
            pattern_type="day_of_week",
            description="Friday peak",
            confidence=0.85,
            evidence_count=12,
            day_of_week=4,
            hour_range=(17, 21),
            impact_pct=40.0,
            tags=["dinner"],
        )
        d = p.to_dict()
        self.assertEqual(d["venue_id"], "v1")
        self.assertEqual(d["pattern_type"], "day_of_week")
        self.assertEqual(d["confidence"], 0.85)
        self.assertEqual(d["hour_range"], (17, 21))
        self.assertEqual(d["tags"], ["dinner"])
        self.assertTrue("pattern_id" in d)
        self.assertTrue("first_seen" in d)


# ---------------------------------------------------------------------------
# Test: PatternStore
# ---------------------------------------------------------------------------


class TestPatternStore(unittest.TestCase):
    """Test PatternStore thread-safety and operations."""

    def setUp(self):
        reset_all_stores()

    def test_add_and_list(self):
        """Can add and list patterns."""
        _p.force_enable_for_tests(False)
        store = pl.get_pattern_store()

        p1 = pl.LearnedPattern(
            venue_id="v1",
            pattern_type="day_of_week",
            day_of_week=4,
            description="Friday peak",
        )
        stored = store.add(p1)
        self.assertEqual(stored.pattern_id, p1.pattern_id)

        patterns = store.list_for_venue("v1")
        self.assertEqual(len(patterns), 1)
        self.assertEqual(patterns[0].description, "Friday peak")

    def test_add_updates_existing_pattern(self):
        """Adding pattern with same type/day_of_week updates existing."""
        _p.force_enable_for_tests(False)
        store = pl.get_pattern_store()

        p1 = pl.LearnedPattern(
            venue_id="v1",
            pattern_type="day_of_week",
            day_of_week=4,
            description="Friday peak v1",
            confidence=0.7,
        )
        stored1 = store.add(p1)

        # Add another with same type/day_of_week but different description
        p2 = pl.LearnedPattern(
            pattern_id=stored1.pattern_id,  # Same ID
            venue_id="v1",
            pattern_type="day_of_week",
            day_of_week=4,
            description="Friday peak v2",
            confidence=0.9,
        )
        stored2 = store.add(p2)

        patterns = store.list_for_venue("v1")
        self.assertEqual(len(patterns), 1)  # Still only one
        self.assertEqual(patterns[0].description, "Friday peak v2")
        self.assertEqual(patterns[0].confidence, 0.9)

    def test_deactivate_pattern(self):
        """Can deactivate patterns; they don't show in active_only=True."""
        _p.force_enable_for_tests(False)
        store = pl.get_pattern_store()

        p = pl.LearnedPattern(venue_id="v1", pattern_type="day_of_week", description="test")
        stored = store.add(p)

        patterns_before = store.list_for_venue("v1", active_only=True)
        self.assertEqual(len(patterns_before), 1)

        store.deactivate(stored.pattern_id)

        patterns_after = store.list_for_venue("v1", active_only=True)
        self.assertEqual(len(patterns_after), 0)

        patterns_all = store.list_for_venue("v1", active_only=False)
        self.assertEqual(len(patterns_all), 1)
        self.assertFalse(patterns_all[0].active)

    def test_get_for_day(self):
        """get_for_day returns day-specific patterns plus generic ones."""
        _p.force_enable_for_tests(False)
        store = pl.get_pattern_store()

        # Add Friday-specific pattern
        p1 = pl.LearnedPattern(
            venue_id="v1", pattern_type="day_of_week", day_of_week=4, description="Friday"
        )
        store.add(p1)

        # Add generic pattern (no day_of_week)
        p2 = pl.LearnedPattern(
            venue_id="v1", pattern_type="weather_impact", day_of_week=None, description="Rain"
        )
        store.add(p2)

        # Query for Friday (4)
        patterns = store.get_for_day("v1", 4)
        self.assertEqual(len(patterns), 2)  # Both Friday-specific and generic

        # Query for Monday (0)
        patterns = store.get_for_day("v1", 0)
        self.assertEqual(len(patterns), 1)  # Only generic

    def test_get_pattern_by_id(self):
        """Can retrieve pattern by ID."""
        _p.force_enable_for_tests(False)
        store = pl.get_pattern_store()

        p = pl.LearnedPattern(venue_id="v1", description="test")
        stored = store.add(p)

        retrieved = store.get(stored.pattern_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.description, "test")

        # Non-existent ID returns None
        missing = store.get("nonexistent")
        self.assertIsNone(missing)


# ---------------------------------------------------------------------------
# Test: Detectors
# ---------------------------------------------------------------------------


class TestDetectDayOfWeekPatterns(unittest.TestCase):
    """Test day-of-week pattern detection."""

    def setUp(self):
        reset_all_stores()

    def test_detect_day_of_week_no_data(self):
        """Empty history returns no patterns."""
        patterns = pl.detect_day_of_week_patterns("v1", days=28, history_store=None)
        self.assertEqual(patterns, [])

    def test_detect_day_of_week_with_data(self):
        """Detects Saturday revenue spike."""
        _p.force_enable_for_tests(True)
        _p.init_db()

        # Mock history store
        class MockHistoryStore:
            def daily_range(self, venue_id, start, end):
                # Weekdays: $5k, Saturdays: $10k
                results = []
                current = start
                while current <= end:
                    revenue = 10000 if current.weekday() == 5 else 5000  # 5 = Saturday
                    actuals = DailyActuals(
                        venue_id=venue_id,
                        day=current,
                        actual_revenue=revenue,
                    )
                    results.append(actuals)
                    current += timedelta(days=1)
                return results

        patterns = pl.detect_day_of_week_patterns("v1", days=28, history_store=MockHistoryStore())

        # Should detect Saturday as positive outlier
        saturday_patterns = [p for p in patterns if p.day_of_week == 5]
        self.assertGreater(len(saturday_patterns), 0)
        self.assertGreater(saturday_patterns[0].impact_pct, 20)


class TestDetectHeadcountTrends(unittest.TestCase):
    """Test headcount trend detection."""

    def setUp(self):
        reset_all_stores()

    def test_detect_headcount_no_data(self):
        """Empty headcount returns no patterns."""
        patterns = pl.detect_headcount_trends("v1", days=28, headcount_store=None)
        self.assertEqual(patterns, [])

    def test_detect_headcount_with_data(self):
        """Detects consistent Friday dinner peak."""
        # Mock headcount store with Friday dinner peak
        class MockHeadcountStore:
            def get_venue_entries(self, venue_id, limit):
                entries = []
                # Create multiple Fridays with consistent dinner (18-20) peak at 150 patrons
                # April 2026: Fridays are 3rd, 10th, 17th, 24th
                friday_dates = [3, 10, 17, 24]
                for day in friday_dates:
                    for hour in range(18, 20):  # 18-19 hour
                        recorded_at = datetime(
                            2026, 4, day, hour, 0, tzinfo=AU_TZ
                        )  # Fridays
                        entry = HeadcountEntry(
                            venue_id=venue_id,
                            shift_id=f"shift_{day}_{hour}",
                            count=150,
                            recorded_at=recorded_at,
                            recorded_by="manager",
                        )
                        entries.append(entry)
                # Add some Monday entries (lower count)
                monday_dates = [1, 8, 15, 22]
                for day in monday_dates:
                    recorded_at = datetime(2026, 4, day, 19, 0, tzinfo=AU_TZ)
                    entry = HeadcountEntry(
                        venue_id=venue_id, shift_id=f"mon_{day}", count=80, recorded_at=recorded_at
                    )
                    entries.append(entry)
                return entries

        patterns = pl.detect_headcount_trends("v1", days=28, headcount_store=MockHeadcountStore())

        # Should detect Friday dinner trend
        friday_patterns = [p for p in patterns if p.day_of_week == 4]
        self.assertGreater(len(friday_patterns), 0, f"Expected Friday patterns, got {len(patterns)} total: {[p.description for p in patterns]}")


class TestDetectNoteTagPatterns(unittest.TestCase):
    """Test shift note tag pattern detection."""

    def setUp(self):
        reset_all_stores()

    def test_detect_note_tag_no_data(self):
        """Empty notes returns no patterns."""
        patterns = pl.detect_note_tag_patterns("v1", days=28, note_store=None)
        self.assertEqual(patterns, [])

    def test_detect_note_tag_patterns(self):
        """Detects 'event' tag on Fridays."""
        class MockNoteStore:
            def get_venue_notes(self, venue_id, limit):
                notes = []
                # Fridays in March 2026: 6, 13, 20, 27 (and April 3, 10...)
                friday_dates = [6, 13, 20, 27]  # Four Fridays in March
                for day in friday_dates:
                    created_at = datetime(2026, 3, day, 22, 0, tzinfo=AU_TZ)
                    # 3 out of 4 Fridays with 'event' tag (75%)
                    tags = ["event"] if day in [6, 13, 20] else []
                    note = ShiftNote(
                        venue_id=venue_id,
                        shift_id=f"shift_{day}",
                        author_id="mgr",
                        author_name="Manager",
                        content="Shift notes",
                        tags=tags,
                        created_at=created_at,
                    )
                    notes.append(note)
                # Add Monday notes without 'event' (March Mondays: 1, 8, 15, 22, 29)
                monday_dates = [1, 8, 15, 22, 29]
                for day in monday_dates:
                    created_at = datetime(2026, 3, day, 22, 0, tzinfo=AU_TZ)
                    note = ShiftNote(
                        venue_id=venue_id,
                        shift_id=f"mon_{day}",
                        author_id="mgr",
                        author_name="Manager",
                        content="Shift notes",
                        tags=[],
                        created_at=created_at,
                    )
                    notes.append(note)
                return notes

        patterns = pl.detect_note_tag_patterns("v1", days=56, note_store=MockNoteStore())

        event_patterns = [p for p in patterns if "event" in p.tags and p.day_of_week == 4]
        self.assertGreater(len(event_patterns), 0, f"Expected Friday event patterns, got {len(patterns)} total: {[p.description for p in patterns]}")


class TestDetectWeatherImpact(unittest.TestCase):
    """Test weather impact pattern detection."""

    def setUp(self):
        reset_all_stores()

    def test_detect_weather_impact_no_data(self):
        """No weather notes returns no patterns."""
        patterns = pl.detect_weather_impact("v1", days=56, history_store=None)
        self.assertEqual(patterns, [])


# ---------------------------------------------------------------------------
# Test: Run Detection
# ---------------------------------------------------------------------------


class TestRunDetection(unittest.TestCase):
    """Test full detection pipeline."""

    def setUp(self):
        reset_all_stores()

    def test_run_detection_returns_summary(self):
        """run_detection returns proper summary dict."""
        _p.force_enable_for_tests(False)

        result = pl.run_detection("v1", days=28)

        self.assertIn("venue_id", result)
        self.assertEqual(result["venue_id"], "v1")
        self.assertIn("patterns_found", result)
        self.assertIn("new", result)
        self.assertIn("updated", result)
        self.assertIn("patterns", result)
        self.assertIsInstance(result["patterns"], list)


# ---------------------------------------------------------------------------
# Test: Persistence
# ---------------------------------------------------------------------------


class TestPatternPersistence(unittest.TestCase):
    """Test pattern persistence roundtrip."""

    def setUp(self):
        reset_all_stores()

    def test_persistence_roundtrip(self):
        """Add pattern, verify it's persisted to SQLite."""
        _p.force_enable_for_tests(True)
        _p.init_db()

        # Add pattern
        store1 = pl.get_pattern_store()
        p = pl.LearnedPattern(
            venue_id="v1",
            pattern_type="day_of_week",
            day_of_week=4,
            description="Friday peak",
            confidence=0.85,
            evidence_count=12,
            impact_pct=40.0,
        )
        stored = store1.add(p)

        # Verify it was persisted by querying directly from SQLite
        rows = _p.fetchall("SELECT * FROM learned_patterns WHERE venue_id = ?", ["v1"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["description"], "Friday peak")

    def test_pattern_deactivation_persists(self):
        """Deactivated patterns persisted with active=0."""
        _p.force_enable_for_tests(True)
        _p.init_db()

        # Add and deactivate
        store1 = pl.get_pattern_store()
        p = pl.LearnedPattern(venue_id="v1", description="test")
        stored = store1.add(p)
        store1.deactivate(stored.pattern_id)

        # Verify persisted with active=0
        rows = _p.fetchall("SELECT * FROM learned_patterns WHERE venue_id = ?", ["v1"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["active"], 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
