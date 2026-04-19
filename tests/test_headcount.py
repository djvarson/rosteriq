"""Tests for headcount clicker and shift notes (on-shift features).

Covers:
- HeadcountEntry creation and serialization
- HeadcountStore (record, list, delta calculation, persistence)
- ShiftNote creation and serialization
- ShiftNoteStore (add, list, search by tag, persistence)
- Persistence round-trip: write, drop singleton, rehydrate, verify
"""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone

from rosteriq import persistence as _p


def _reset_persistence_with_path(path: str) -> None:
    """Point persistence at a fresh DB and reset all singletons."""
    os.environ["ROSTERIQ_DB_PATH"] = path
    _p.reset_for_tests()
    _p.reset_rehydrate_for_tests()
    # Reset headcount singletons
    import rosteriq.headcount as _h
    _h._headcount_store_instance = None
    _h._shift_note_store_instance = None


class HeadcountEntryTests(unittest.TestCase):
    """Test HeadcountEntry dataclass."""

    def test_entry_creation(self):
        from rosteriq.headcount import HeadcountEntry

        entry = HeadcountEntry(
            venue_id="venue-1",
            shift_id="shift-1",
            count=50,
            delta=0,
            recorded_by="mgr-1",
        )
        self.assertEqual(entry.venue_id, "venue-1")
        self.assertEqual(entry.shift_id, "shift-1")
        self.assertEqual(entry.count, 50)
        self.assertEqual(entry.delta, 0)
        self.assertTrue(len(entry.entry_id) == 12)  # uuid4 hex[:12]

    def test_entry_to_dict(self):
        from rosteriq.headcount import HeadcountEntry

        entry = HeadcountEntry(
            venue_id="venue-1",
            shift_id="shift-1",
            count=75,
            delta=5,
            recorded_by="mgr-1",
            note="busy",
        )
        d = entry.to_dict()
        self.assertEqual(d["venue_id"], "venue-1")
        self.assertEqual(d["count"], 75)
        self.assertEqual(d["delta"], 5)
        self.assertEqual(d["note"], "busy")
        self.assertIn("recorded_at", d)


class HeadcountStoreTests(unittest.TestCase):
    """Test HeadcountStore."""

    def setUp(self):
        from rosteriq.headcount import get_headcount_store
        # Reset in-memory store
        import rosteriq.headcount as _h
        _h._headcount_store_instance = None
        self.store = get_headcount_store()

    def test_record_headcount_first_entry(self):
        """First entry should have delta=0."""
        entry = self.store.record(
            venue_id="v1",
            shift_id="s1",
            count=42,
            recorded_by="mgr-1",
        )
        self.assertEqual(entry.count, 42)
        self.assertEqual(entry.delta, 0)  # first entry

    def test_record_headcount_delta(self):
        """Subsequent entry should calculate delta."""
        self.store.record("v1", "s1", 42, "mgr-1")
        entry2 = self.store.record("v1", "s1", 50, "mgr-1")
        self.assertEqual(entry2.count, 50)
        self.assertEqual(entry2.delta, 8)  # 50 - 42

    def test_record_multiple_deltas(self):
        """Test sequence of delta calculations."""
        self.store.record("v1", "s1", 10, "mgr-1")
        e2 = self.store.record("v1", "s1", 20, "mgr-1")
        e3 = self.store.record("v1", "s1", 15, "mgr-1")
        self.assertEqual(e2.delta, 10)
        self.assertEqual(e3.delta, -5)

    def test_get_shift_entries_chronological(self):
        """Entries should be in chronological order."""
        e1 = self.store.record("v1", "s1", 10, "mgr-1")
        e2 = self.store.record("v1", "s1", 20, "mgr-1")
        e3 = self.store.record("v1", "s1", 30, "mgr-1")

        entries = self.store.get_shift_entries("s1")
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0].entry_id, e1.entry_id)
        self.assertEqual(entries[1].entry_id, e2.entry_id)
        self.assertEqual(entries[2].entry_id, e3.entry_id)

    def test_get_venue_entries_newest_first(self):
        """get_venue_entries should return newest first."""
        # Add entries from different shifts
        e1 = self.store.record("v1", "s1", 10, "mgr-1")
        e2 = self.store.record("v1", "s2", 20, "mgr-1")
        e3 = self.store.record("v1", "s1", 30, "mgr-1")

        entries = self.store.get_venue_entries("v1")
        self.assertEqual(len(entries), 3)
        # Newest first — e3 has the latest recorded_at
        self.assertEqual(entries[0].entry_id, e3.entry_id)

    def test_get_latest(self):
        """get_latest should return the most recent entry."""
        self.store.record("v1", "s1", 10, "mgr-1")
        e2 = self.store.record("v1", "s1", 20, "mgr-1")
        e3 = self.store.record("v1", "s2", 30, "mgr-1")

        latest = self.store.get_latest("v1")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.entry_id, e3.entry_id)

    def test_get_latest_no_entries(self):
        """get_latest should return None when no entries."""
        latest = self.store.get_latest("v-nonexistent")
        self.assertIsNone(latest)

    def test_clear_shift(self):
        """clear_shift should remove all entries for a shift."""
        self.store.record("v1", "s1", 10, "mgr-1")
        self.store.record("v1", "s1", 20, "mgr-1")
        self.store.record("v1", "s2", 30, "mgr-1")

        self.store.clear_shift("s1")
        entries_s1 = self.store.get_shift_entries("s1")
        entries_s2 = self.store.get_shift_entries("s2")
        self.assertEqual(len(entries_s1), 0)
        self.assertEqual(len(entries_s2), 1)


class ShiftNoteTests(unittest.TestCase):
    """Test ShiftNote dataclass."""

    def test_note_creation(self):
        from rosteriq.headcount import ShiftNote

        note = ShiftNote(
            venue_id="venue-1",
            shift_id="shift-1",
            author_id="auth-1",
            author_name="Alice",
            content="Busy night",
            tags=["weather", "event"],
        )
        self.assertEqual(note.venue_id, "venue-1")
        self.assertEqual(note.content, "Busy night")
        self.assertEqual(note.tags, ["weather", "event"])
        self.assertTrue(len(note.note_id) == 12)

    def test_note_to_dict(self):
        from rosteriq.headcount import ShiftNote

        note = ShiftNote(
            venue_id="venue-1",
            shift_id="shift-1",
            author_id="auth-1",
            author_name="Bob",
            content="Incident occurred",
            tags=["incident"],
        )
        d = note.to_dict()
        self.assertEqual(d["author_name"], "Bob")
        self.assertEqual(d["content"], "Incident occurred")
        self.assertEqual(d["tags"], ["incident"])


class ShiftNoteStoreTests(unittest.TestCase):
    """Test ShiftNoteStore."""

    def setUp(self):
        from rosteriq.headcount import get_shift_note_store
        # Reset in-memory store
        import rosteriq.headcount as _h
        _h._shift_note_store_instance = None
        self.store = get_shift_note_store()

    def test_shift_note_creation(self):
        """Add a note and verify fields."""
        note = self.store.add(
            venue_id="v1",
            shift_id="s1",
            author_id="a1",
            author_name="Alice",
            content="Quiet night",
            tags=["weather"],
        )
        self.assertEqual(note.content, "Quiet night")
        self.assertEqual(note.tags, ["weather"])

    def test_get_shift_notes_chronological(self):
        """Notes should be in chronological order."""
        n1 = self.store.add("v1", "s1", "a1", "Alice", "Note 1", ["tag1"])
        n2 = self.store.add("v1", "s1", "a1", "Alice", "Note 2", ["tag2"])
        n3 = self.store.add("v1", "s1", "a1", "Alice", "Note 3", ["tag3"])

        notes = self.store.get_shift_notes("s1")
        self.assertEqual(len(notes), 3)
        self.assertEqual(notes[0].note_id, n1.note_id)
        self.assertEqual(notes[1].note_id, n2.note_id)
        self.assertEqual(notes[2].note_id, n3.note_id)

    def test_get_venue_notes_newest_first(self):
        """get_venue_notes should return newest first."""
        n1 = self.store.add("v1", "s1", "a1", "Alice", "Note 1", [])
        n2 = self.store.add("v1", "s2", "a1", "Alice", "Note 2", [])
        n3 = self.store.add("v1", "s1", "a1", "Alice", "Note 3", [])

        notes = self.store.get_venue_notes("v1")
        self.assertEqual(len(notes), 3)
        # Newest first
        self.assertEqual(notes[0].note_id, n3.note_id)

    def test_search_by_tag(self):
        """search_by_tag should only return matching notes."""
        self.store.add("v1", "s1", "a1", "Alice", "Weather note", ["weather"])
        self.store.add("v1", "s1", "a1", "Alice", "Event note", ["event"])
        n3 = self.store.add("v1", "s2", "a1", "Alice", "Another weather", ["weather"])

        weather_notes = self.store.search_by_tag("v1", "weather")
        self.assertEqual(len(weather_notes), 2)
        # Verify correct notes
        note_ids = {n.note_id for n in weather_notes}
        self.assertIn(n3.note_id, note_ids)

    def test_search_by_tag_no_matches(self):
        """search_by_tag should return empty when no matches."""
        self.store.add("v1", "s1", "a1", "Alice", "Note", ["tag1"])
        notes = self.store.search_by_tag("v1", "nonexistent")
        self.assertEqual(len(notes), 0)

    def test_add_note_default_tags(self):
        """add() should handle default empty tags."""
        note = self.store.add(
            venue_id="v1",
            shift_id="s1",
            author_id="a1",
            author_name="Alice",
            content="Content",
        )
        self.assertEqual(note.tags, [])


class PersistenceTests(unittest.TestCase):
    """Test persistence round-trip for headcount and shift notes."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "headcount_test.db")
        _reset_persistence_with_path(self.db_path)
        # Force persistence on in-memory DB for testing
        _p.force_enable_for_tests(True)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        _p.force_enable_for_tests(False)
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.tmpdir.cleanup()

    def test_headcount_persistence_roundtrip(self):
        """Write headcount, drop singleton, rehydrate, verify."""
        from rosteriq.headcount import get_headcount_store

        # Write some entries
        store1 = get_headcount_store()
        e1 = store1.record("v1", "s1", 10, "mgr-1", note="first")
        e2 = store1.record("v1", "s1", 20, "mgr-1", note="second")
        e3 = store1.record("v1", "s2", 30, "mgr-1")

        # Drop singleton and rehydrate
        import rosteriq.headcount as _h
        _h._headcount_store_instance = None
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        # Verify data restored
        store2 = get_headcount_store()
        entries = store2.get_shift_entries("s1")
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].entry_id, e1.entry_id)
        self.assertEqual(entries[0].count, 10)
        self.assertEqual(entries[0].note, "first")
        self.assertEqual(entries[1].entry_id, e2.entry_id)
        self.assertEqual(entries[1].delta, 10)

        # Verify venue entries
        venue_entries = store2.get_venue_entries("v1")
        self.assertEqual(len(venue_entries), 3)

    def test_shift_note_persistence_roundtrip(self):
        """Write shift notes, drop singleton, rehydrate, verify."""
        from rosteriq.headcount import get_shift_note_store

        # Write some notes
        store1 = get_shift_note_store()
        n1 = store1.add("v1", "s1", "a1", "Alice", "Weather note", ["weather"])
        n2 = store1.add("v1", "s1", "a1", "Alice", "Event note", ["event"])
        n3 = store1.add("v1", "s2", "a2", "Bob", "Incident", ["incident", "staffing"])

        # Drop singleton and rehydrate
        import rosteriq.headcount as _h
        _h._shift_note_store_instance = None
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        # Verify data restored
        store2 = get_shift_note_store()
        notes = store2.get_shift_notes("s1")
        self.assertEqual(len(notes), 2)
        self.assertEqual(notes[0].note_id, n1.note_id)
        self.assertEqual(notes[0].author_name, "Alice")
        self.assertEqual(notes[0].tags, ["weather"])

        # Verify search works
        incident_notes = store2.search_by_tag("v1", "incident")
        self.assertEqual(len(incident_notes), 1)
        self.assertEqual(incident_notes[0].note_id, n3.note_id)

    def test_combined_persistence(self):
        """Persist both headcount and shift notes together."""
        from rosteriq.headcount import get_headcount_store, get_shift_note_store

        # Write entries to both stores
        hc_store = get_headcount_store()
        hc_store.record("v1", "s1", 50, "mgr-1")
        hc_store.record("v1", "s1", 60, "mgr-1")

        notes_store = get_shift_note_store()
        notes_store.add("v1", "s1", "a1", "Alice", "Busy", ["event"])

        # Drop both singletons
        import rosteriq.headcount as _h
        _h._headcount_store_instance = None
        _h._shift_note_store_instance = None
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        # Verify both restored
        hc_store2 = get_headcount_store()
        notes_store2 = get_shift_note_store()

        hc_entries = hc_store2.get_shift_entries("s1")
        self.assertEqual(len(hc_entries), 2)

        notes = notes_store2.get_shift_notes("s1")
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0].content, "Busy")


class SingletonTests(unittest.TestCase):
    """Test singleton getter functions."""

    def setUp(self):
        import rosteriq.headcount as _h
        _h._headcount_store_instance = None
        _h._shift_note_store_instance = None

    def test_get_headcount_store_singleton(self):
        from rosteriq.headcount import get_headcount_store

        store1 = get_headcount_store()
        store2 = get_headcount_store()
        self.assertIs(store1, store2)

    def test_get_shift_note_store_singleton(self):
        from rosteriq.headcount import get_shift_note_store

        store1 = get_shift_note_store()
        store2 = get_shift_note_store()
        self.assertIs(store1, store2)


if __name__ == "__main__":
    unittest.main()
