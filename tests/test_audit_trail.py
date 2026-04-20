"""Test suite for audit_trail.py module.

Comprehensive test coverage (25+ tests) for:
- Event logging (log_event)
- Query with various filters and pagination
- Entity history (get_entity_history)
- Actor activity (get_actor_activity)
- Summary aggregation (build_audit_summary)
- Diff computation (diff_changes)
- Format entry (format_audit_entry)
- Pagination (offset/limit)
- Immutability (no update/delete)
- Store persistence and isolation
- Multiple venue isolation
"""

import sys
import os
import unittest
from datetime import datetime, timezone, timedelta
import json

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.audit_trail import (
    AuditAction,
    AuditEntityType,
    AuditEntry,
    AuditQuery,
    AuditSummary,
    build_audit_summary,
    diff_changes,
    format_audit_entry,
    get_actor_activity,
    get_audit_store,
    get_entity_history,
    log_event,
    query_audit,
    _reset_for_tests,
)
from rosteriq import persistence as _p


class TestAuditEventLogging(unittest.TestCase):
    """Test basic event logging functionality."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_log_simple_event(self):
        """Test logging a simple event."""
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale Ingvarson",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created new shift",
        )

        self.assertIsNotNone(entry.entry_id)
        self.assertEqual(entry.venue_id, "venue-001")
        self.assertEqual(entry.actor_id, "user-123")
        self.assertEqual(entry.actor_name, "Dale Ingvarson")
        self.assertEqual(entry.action, AuditAction.CREATE)
        self.assertEqual(entry.entity_type, AuditEntityType.SHIFT)
        self.assertEqual(entry.entity_id, "shift-001")
        self.assertEqual(entry.description, "Created new shift")
        self.assertIsNone(entry.changes)
        self.assertIsNone(entry.ip_address)
        self.assertIsNone(entry.metadata)

    def test_log_event_with_changes(self):
        """Test logging an event with field changes."""
        changes = {
            "pay_rate": {"old": 25.50, "new": 26.00},
            "shift_length": {"old": 8, "new": 9},
        }
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.UPDATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Updated shift details",
            changes=changes,
        )

        self.assertEqual(entry.changes, changes)

    def test_log_event_with_metadata(self):
        """Test logging an event with metadata."""
        metadata = {"reason": "temporary cover", "approved_by": "manager-001"}
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
            metadata=metadata,
        )

        self.assertEqual(entry.metadata, metadata)

    def test_log_event_with_ip_address(self):
        """Test logging an event with IP address."""
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.LOGIN,
            entity_type=AuditEntityType.EMPLOYEE,
            entity_id="user-123",
            description="User logged in",
            ip_address="192.168.1.100",
        )

        self.assertEqual(entry.ip_address, "192.168.1.100")

    def test_timestamp_always_utc(self):
        """Test that timestamps are always in UTC."""
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        self.assertIsNotNone(entry.timestamp.tzinfo)
        self.assertEqual(entry.timestamp.tzinfo, timezone.utc)


class TestAuditQuery(unittest.TestCase):
    """Test querying audit entries."""

    def setUp(self):
        """Reset store and create test data."""
        _reset_for_tests()
        # Log multiple events for different venues and actors
        self.now = datetime.now(timezone.utc)

        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-456",
            actor_name="Alice",
            action=AuditAction.APPROVE,
            entity_type=AuditEntityType.LEAVE_REQUEST,
            entity_id="LR-001",
            description="Approved leave",
        )

        log_event(
            venue_id="venue-002",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.UPDATE,
            entity_type=AuditEntityType.ROSTER,
            entity_id="roster-001",
            description="Updated roster",
        )

    def test_query_by_venue(self):
        """Test querying entries by venue_id."""
        query = AuditQuery(venue_id="venue-001")
        entries = query_audit(query)
        self.assertEqual(len(entries), 2)
        self.assertTrue(all(e.venue_id == "venue-001" for e in entries))

    def test_query_by_actor(self):
        """Test querying entries by actor_id."""
        query = AuditQuery(venue_id="venue-001", actor_id="user-123")
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].actor_id, "user-123")

    def test_query_by_entity_type(self):
        """Test querying entries by entity_type."""
        query = AuditQuery(venue_id="venue-001", entity_type=AuditEntityType.SHIFT)
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].entity_type, AuditEntityType.SHIFT)

    def test_query_by_entity_id(self):
        """Test querying entries by entity_id."""
        query = AuditQuery(venue_id="venue-001", entity_id="LR-001")
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].entity_id, "LR-001")

    def test_query_by_action(self):
        """Test querying entries by action."""
        query = AuditQuery(venue_id="venue-001", action=AuditAction.APPROVE)
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].action, AuditAction.APPROVE)

    def test_query_with_limit_and_offset(self):
        """Test pagination with limit and offset."""
        query = AuditQuery(venue_id="venue-001", limit=1, offset=0)
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)

        query = AuditQuery(venue_id="venue-001", limit=1, offset=1)
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)

    def test_query_multiple_filters(self):
        """Test querying with multiple filters."""
        query = AuditQuery(
            venue_id="venue-001",
            actor_id="user-123",
            entity_type=AuditEntityType.SHIFT,
        )
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].entity_id, "shift-001")

    def test_query_date_range(self):
        """Test querying with date range."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        query = AuditQuery(venue_id="venue-001", date_from=past, date_to=future)
        entries = query_audit(query)
        self.assertEqual(len(entries), 2)

        # Query with date range that excludes entries
        far_past = self.now - timedelta(days=1)
        query = AuditQuery(venue_id="venue-001", date_from=far_past, date_to=far_past)
        entries = query_audit(query)
        self.assertEqual(len(entries), 0)

    def test_query_isolation_by_venue(self):
        """Test that queries are isolated by venue."""
        query = AuditQuery(venue_id="venue-002")
        entries = query_audit(query)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].venue_id, "venue-002")


class TestEntityHistory(unittest.TestCase):
    """Test entity history retrieval."""

    def setUp(self):
        """Reset store and create test data."""
        _reset_for_tests()

        # Log multiple events for the same entity
        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.UPDATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Updated shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-456",
            actor_name="Alice",
            action=AuditAction.APPROVE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Approved shift",
        )

    def test_get_entity_history(self):
        """Test retrieving history for a specific entity."""
        history = get_entity_history(AuditEntityType.SHIFT, "shift-001")
        self.assertEqual(len(history), 3)
        self.assertTrue(all(e.entity_id == "shift-001" for e in history))

    def test_entity_history_chronological_order(self):
        """Test that entity history is in chronological order."""
        history = get_entity_history(AuditEntityType.SHIFT, "shift-001")
        self.assertEqual(history[0].action, AuditAction.CREATE)
        self.assertEqual(history[1].action, AuditAction.UPDATE)
        self.assertEqual(history[2].action, AuditAction.APPROVE)

    def test_entity_history_for_nonexistent_entity(self):
        """Test that nonexistent entity returns empty list."""
        history = get_entity_history(AuditEntityType.SHIFT, "shift-999")
        self.assertEqual(len(history), 0)


class TestActorActivity(unittest.TestCase):
    """Test actor activity retrieval."""

    def setUp(self):
        """Reset store and create test data."""
        _reset_for_tests()
        self.now = datetime.now(timezone.utc)

        # Log multiple events by different actors
        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.UPDATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Updated shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-456",
            actor_name="Alice",
            action=AuditAction.APPROVE,
            entity_type=AuditEntityType.LEAVE_REQUEST,
            entity_id="LR-001",
            description="Approved leave",
        )

    def test_get_actor_activity(self):
        """Test retrieving activity for a specific actor."""
        activity = get_actor_activity("user-123")
        self.assertEqual(len(activity), 2)
        self.assertTrue(all(e.actor_id == "user-123" for e in activity))

    def test_get_actor_activity_with_date_filter(self):
        """Test filtering actor activity by date range."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        activity = get_actor_activity("user-123", date_from=past, date_to=future)
        self.assertEqual(len(activity), 2)

    def test_get_actor_activity_for_inactive_actor(self):
        """Test that inactive actor returns empty list."""
        activity = get_actor_activity("user-999")
        self.assertEqual(len(activity), 0)


class TestAuditSummary(unittest.TestCase):
    """Test audit summary aggregation."""

    def setUp(self):
        """Reset store and create test data."""
        _reset_for_tests()
        self.now = datetime.now(timezone.utc)

        # Create diverse audit entries
        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.UPDATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Updated shift",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-456",
            actor_name="Alice",
            action=AuditAction.APPROVE,
            entity_type=AuditEntityType.LEAVE_REQUEST,
            entity_id="LR-001",
            description="Approved leave",
        )

        log_event(
            venue_id="venue-001",
            actor_id="user-456",
            actor_name="Alice",
            action=AuditAction.REJECT,
            entity_type=AuditEntityType.LEAVE_REQUEST,
            entity_id="LR-002",
            description="Rejected leave",
        )

    def test_build_summary(self):
        """Test building audit summary."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.venue_id, "venue-001")
        self.assertEqual(summary.total_entries, 4)

    def test_summary_by_action(self):
        """Test summary counts by action."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.by_action["create"], 1)
        self.assertEqual(summary.by_action["update"], 1)
        self.assertEqual(summary.by_action["approve"], 1)
        self.assertEqual(summary.by_action["reject"], 1)

    def test_summary_by_entity_type(self):
        """Test summary counts by entity type."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.by_entity_type["shift"], 2)
        self.assertEqual(summary.by_entity_type["leave_request"], 2)

    def test_summary_by_actor(self):
        """Test summary counts by actor."""
        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.by_actor["Dale"], 2)
        self.assertEqual(summary.by_actor["Alice"], 2)

    def test_summary_most_active_actor(self):
        """Test identifying most active actor."""
        # Log more events from Dale
        for i in range(3):
            log_event(
                venue_id="venue-001",
                actor_id="user-123",
                actor_name="Dale",
                action=AuditAction.UPDATE,
                entity_type=AuditEntityType.SHIFT,
                entity_id=f"shift-{i}",
                description="Updated shift",
            )

        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.most_active_actor, "Dale")

    def test_summary_most_changed_entity_type(self):
        """Test identifying most changed entity type."""
        # Log more shift events
        for i in range(3):
            log_event(
                venue_id="venue-001",
                actor_id="user-123",
                actor_name="Dale",
                action=AuditAction.CREATE,
                entity_type=AuditEntityType.SHIFT,
                entity_id=f"shift-extra-{i}",
                description="Created shift",
            )

        past = self.now - timedelta(hours=1)
        future = self.now + timedelta(hours=1)

        summary = build_audit_summary("venue-001", past, future)

        self.assertEqual(summary.most_changed_entity_type, "shift")


class TestDiffChanges(unittest.TestCase):
    """Test diff computation for changed fields."""

    def test_diff_simple_update(self):
        """Test computing diff for simple field change."""
        old = {"name": "Old Name", "age": 30}
        new = {"name": "New Name", "age": 30}

        changes = diff_changes(old, new)

        self.assertIn("name", changes)
        self.assertEqual(changes["name"]["old"], "Old Name")
        self.assertEqual(changes["name"]["new"], "New Name")
        self.assertNotIn("age", changes)

    def test_diff_multiple_changes(self):
        """Test computing diff for multiple field changes."""
        old = {"a": 1, "b": 2, "c": 3}
        new = {"a": 1, "b": 99, "c": 30}

        changes = diff_changes(old, new)

        self.assertEqual(len(changes), 2)
        self.assertIn("b", changes)
        self.assertIn("c", changes)

    def test_diff_added_field(self):
        """Test diff when new field is added."""
        old = {"name": "Test"}
        new = {"name": "Test", "age": 25}

        changes = diff_changes(old, new)

        self.assertIn("age", changes)
        self.assertIsNone(changes["age"]["old"])
        self.assertEqual(changes["age"]["new"], 25)

    def test_diff_removed_field(self):
        """Test diff when field is removed."""
        old = {"name": "Test", "age": 25}
        new = {"name": "Test"}

        changes = diff_changes(old, new)

        self.assertIn("age", changes)
        self.assertEqual(changes["age"]["old"], 25)
        self.assertIsNone(changes["age"]["new"])

    def test_diff_no_changes(self):
        """Test diff when nothing changes."""
        old = {"a": 1, "b": 2}
        new = {"a": 1, "b": 2}

        changes = diff_changes(old, new)

        self.assertEqual(len(changes), 0)


class TestFormatAuditEntry(unittest.TestCase):
    """Test human-readable formatting of audit entries."""

    def test_format_simple_entry(self):
        """Test formatting a simple entry."""
        entry = AuditEntry(
            entry_id="id-001",
            venue_id="venue-001",
            timestamp=datetime(2026, 4, 20, 14, 30, 0, tzinfo=timezone.utc),
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        formatted = format_audit_entry(entry)

        self.assertIn("Dale", formatted)
        self.assertIn("create", formatted)
        self.assertIn("shift", formatted)
        self.assertIn("shift-001", formatted)
        self.assertIn("2026-04-20", formatted)

    def test_format_entry_with_spaces(self):
        """Test formatting entry with action that has underscores."""
        entry = AuditEntry(
            entry_id="id-001",
            venue_id="venue-001",
            timestamp=datetime(2026, 4, 20, 14, 30, 0, tzinfo=timezone.utc),
            actor_id="user-123",
            actor_name="Alice Manager",
            action=AuditAction.SIGN_OFF,
            entity_type=AuditEntityType.CLOSE_OF_DAY,
            entity_id="cod-001",
            description="Signed off on close of day",
        )

        formatted = format_audit_entry(entry)

        self.assertIn("Alice Manager", formatted)
        self.assertIn("close", formatted.lower())


class TestImmutability(unittest.TestCase):
    """Test that audit log is append-only."""

    def setUp(self):
        """Reset store."""
        _reset_for_tests()

    def test_no_update_method(self):
        """Test that AuditStore has no update method."""
        store = get_audit_store()
        self.assertFalse(hasattr(store, "update"))

    def test_no_delete_method(self):
        """Test that AuditStore has no delete method."""
        store = get_audit_store()
        self.assertFalse(hasattr(store, "delete"))

    def test_append_only_semantics(self):
        """Test that entries are only appended, not modified."""
        entry1 = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        # Attempt to modify the returned entry (should not affect store)
        original_desc = entry1.description
        entry1.description = "MODIFIED"

        # Query should return original
        history = get_entity_history(AuditEntityType.SHIFT, "shift-001")
        self.assertEqual(history[0].description, original_desc)


class TestPersistence(unittest.TestCase):
    """Test persistence to SQLite."""

    def setUp(self):
        """Reset store and enable persistence for tests."""
        _reset_for_tests()
        _p.force_enable_for_tests(True)

    def tearDown(self):
        """Disable persistence force."""
        _p.force_enable_for_tests(False)

    def test_entry_roundtrip(self):
        """Test that entries are persisted and loaded."""
        entry = log_event(
            venue_id="venue-001",
            actor_id="user-123",
            actor_name="Dale",
            action=AuditAction.CREATE,
            entity_type=AuditEntityType.SHIFT,
            entity_id="shift-001",
            description="Created shift",
        )

        # Create new store (should load from persistence)
        store = get_audit_store()
        history = store.get_entity_history(AuditEntityType.SHIFT, "shift-001")

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].entry_id, entry.entry_id)


if __name__ == "__main__":
    unittest.main()
