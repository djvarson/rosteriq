"""Test suite for comms_hub.py module.

Tests the Staff Communication Hub with 25+ test cases covering:
- Template rendering with variable substitution
- Quiet hours logic
- Single message send
- Bulk send with preference filtering
- Delivery statistics
- Preference CRUD
- Store persistence
- Edge cases (missing variables, empty recipients, etc.)

Uses unittest.TestCase. Pure stdlib, no pytest.
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.comms_hub import (
    Channel,
    MessagePriority,
    MessageStatus,
    MessageTemplate,
    StaffMessage,
    NotificationPreference,
    BulkSendResult,
    render_template,
    check_quiet_hours,
    send_message,
    send_bulk,
    get_delivery_stats,
    get_comms_store,
    _reset_for_tests,
    BUILTIN_TEMPLATES,
)
from rosteriq import persistence as _p


class TestTemplateRendering(unittest.TestCase):
    """Test template rendering with variable substitution."""

    def test_render_roster_published(self):
        """Test rendering roster_published template."""
        template = BUILTIN_TEMPLATES["roster_published"]
        subject, body = render_template(template, {
            "week_start": "2026-04-20",
            "shift_count": "5",
            "total_hours": "40",
        })
        self.assertIn("2026-04-20", subject)
        self.assertIn("5 shifts", body)
        self.assertIn("40h", body)

    def test_render_shift_change(self):
        """Test rendering shift_change template."""
        template = BUILTIN_TEMPLATES["shift_change"]
        subject, body = render_template(template, {
            "date": "2026-04-21",
            "old_time": "09:00-17:00",
            "new_time": "10:00-18:00",
        })
        self.assertIn("2026-04-21", subject)
        self.assertIn("09:00-17:00", body)
        self.assertIn("10:00-18:00", body)

    def test_render_shift_offer(self):
        """Test rendering shift_offer template."""
        template = BUILTIN_TEMPLATES["shift_offer"]
        subject, body = render_template(template, {
            "date": "2026-04-25",
            "start": "18:00",
            "end": "22:00",
            "role": "bartender",
        })
        self.assertIn("2026-04-25", subject)
        self.assertIn("18:00-22:00", body)
        self.assertIn("bartender", body)

    def test_render_announcement(self):
        """Test rendering announcement template."""
        template = BUILTIN_TEMPLATES["announcement"]
        subject, body = render_template(template, {
            "venue_name": "The Mojo Bar",
            "message": "New cocktail menu available",
        })
        self.assertIn("The Mojo Bar", subject)
        self.assertIn("New cocktail menu available", body)

    def test_render_reminder(self):
        """Test rendering reminder template."""
        template = BUILTIN_TEMPLATES["reminder"]
        subject, body = render_template(template, {
            "date": "2026-04-22",
            "start_time": "14:00",
            "end_time": "22:00",
            "role": "floor",
        })
        self.assertIn("reminder", subject.lower())
        self.assertIn("2026-04-22", body)
        self.assertIn("floor", body)

    def test_render_leave_approved(self):
        """Test rendering leave_approved template."""
        template = BUILTIN_TEMPLATES["leave_approved"]
        subject, body = render_template(template, {
            "leave_type": "annual",
            "start_date": "2026-05-01",
            "end_date": "2026-05-05",
        })
        self.assertIn("approved", subject.lower())
        self.assertIn("annual", body)
        self.assertIn("2026-05-01", body)

    def test_render_missing_variable(self):
        """Test that missing variables raise KeyError."""
        template = BUILTIN_TEMPLATES["roster_published"]
        with self.assertRaises(KeyError):
            render_template(template, {
                "week_start": "2026-04-20",
                # Missing shift_count and total_hours
            })

    def test_render_extra_variables_ignored(self):
        """Test that extra variables don't cause errors."""
        template = BUILTIN_TEMPLATES["roster_published"]
        subject, body = render_template(template, {
            "week_start": "2026-04-20",
            "shift_count": "5",
            "total_hours": "40",
            "extra_field": "ignored",
        })
        self.assertIn("2026-04-20", subject)


class TestQuietHours(unittest.TestCase):
    """Test quiet hours logic."""

    def test_quiet_hours_standard_range(self):
        """Test quiet hours with standard start < end (e.g., 9-5)."""
        pref = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.SMS,
            quiet_hours_start=22,  # 10 PM
            quiet_hours_end=8,     # 8 AM
        )

        # Test hours within quiet hours
        self.assertTrue(check_quiet_hours(pref, 23))  # 11 PM
        self.assertTrue(check_quiet_hours(pref, 0))   # Midnight
        self.assertTrue(check_quiet_hours(pref, 7))   # 7 AM

        # Test hours outside quiet hours
        self.assertFalse(check_quiet_hours(pref, 8))   # 8 AM (boundary)
        self.assertFalse(check_quiet_hours(pref, 12))  # Noon
        self.assertFalse(check_quiet_hours(pref, 21))  # 9 PM (boundary)

    def test_quiet_hours_edge_case_start(self):
        """Test quiet hours at start boundary."""
        pref = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.SMS,
            quiet_hours_start=22,
            quiet_hours_end=8,
        )
        self.assertTrue(check_quiet_hours(pref, 22))   # At start
        self.assertFalse(check_quiet_hours(pref, 21))  # Just before

    def test_quiet_hours_edge_case_end(self):
        """Test quiet hours at end boundary."""
        pref = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.SMS,
            quiet_hours_start=22,
            quiet_hours_end=8,
        )
        self.assertFalse(check_quiet_hours(pref, 8))   # At end
        self.assertTrue(check_quiet_hours(pref, 7))    # Just before


class TestSingleMessageSend(unittest.TestCase):
    """Test single message send functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_send_message_basic(self):
        """Test sending a basic message."""
        message = StaffMessage(
            message_id="msg_001",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="+61412345678",
            channel=Channel.SMS,
            priority=MessagePriority.NORMAL,
            subject="Test",
            body="Test message",
            status=MessageStatus.QUEUED,
        )

        sent = send_message(message)

        self.assertEqual(sent.status, MessageStatus.SENT)
        self.assertIsNotNone(sent.sent_at)

    def test_message_persists(self):
        """Test that sent message persists in store."""
        store = get_comms_store()

        message = StaffMessage(
            message_id="msg_001",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="alice@example.com",
            channel=Channel.EMAIL,
            priority=MessagePriority.HIGH,
            subject="Test",
            body="Test message",
            status=MessageStatus.QUEUED,
        )

        sent = send_message(message)
        messages = store.get_messages("venue_001")

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message_id, "msg_001")


class TestBulkSend(unittest.TestCase):
    """Test bulk send with preference filtering."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_bulk_send_basic(self):
        """Test bulk send to multiple recipients."""
        result = send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001", "emp_002", "emp_003"],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                "shift_count": "5",
                "total_hours": "40",
            },
        )

        self.assertEqual(result.total, 3)
        self.assertEqual(result.sent, 3)
        self.assertEqual(result.failed, 0)
        self.assertEqual(len(result.messages), 3)

    def test_bulk_send_empty_recipients(self):
        """Test bulk send with empty recipient list."""
        result = send_bulk(
            venue_id="venue_001",
            recipient_ids=[],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                "shift_count": "5",
                "total_hours": "40",
            },
        )

        self.assertEqual(result.total, 0)
        self.assertEqual(result.sent, 0)
        self.assertEqual(result.failed, 0)

    def test_bulk_send_invalid_template(self):
        """Test bulk send with invalid template ID."""
        result = send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001"],
            template_id="nonexistent_template",
            variables={"foo": "bar"},
        )

        self.assertEqual(result.total, 1)
        self.assertEqual(result.sent, 0)
        self.assertEqual(result.failed, 1)

    def test_bulk_send_missing_variables(self):
        """Test bulk send with missing template variables."""
        result = send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001"],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                # Missing shift_count and total_hours
            },
        )

        self.assertEqual(result.total, 1)
        self.assertEqual(result.sent, 0)
        self.assertEqual(result.failed, 1)

    def test_bulk_send_channel_override(self):
        """Test bulk send with channel override."""
        result = send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001"],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                "shift_count": "5",
                "total_hours": "40",
            },
            channel=Channel.EMAIL,  # Override default SMS
        )

        self.assertEqual(result.sent, 1)
        self.assertEqual(result.messages[0].channel, Channel.EMAIL)


class TestDeliveryStats(unittest.TestCase):
    """Test delivery statistics."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_delivery_stats_basic(self):
        """Test basic delivery statistics."""
        # Send some messages
        send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001", "emp_002"],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                "shift_count": "5",
                "total_hours": "40",
            },
        )

        stats = get_delivery_stats("venue_001")

        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["sent"], 2)
        self.assertEqual(stats["failed"], 0)

    def test_delivery_stats_empty_venue(self):
        """Test statistics for venue with no messages."""
        stats = get_delivery_stats("nonexistent_venue")

        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["sent"], 0)
        self.assertEqual(stats["failed"], 0)


class TestPreferenceCRUD(unittest.TestCase):
    """Test preference CRUD operations."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_save_and_retrieve_preference(self):
        """Test saving and retrieving preferences."""
        store = get_comms_store()

        pref = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.EMAIL,
            roster_changes=True,
            shift_offers=False,
            announcements=True,
            reminders=False,
            quiet_hours_start=22,
            quiet_hours_end=9,
        )

        saved = store.save_preference(pref)
        retrieved = store.get_preference("emp_001", "venue_001")

        self.assertEqual(retrieved.employee_id, "emp_001")
        self.assertEqual(retrieved.preferred_channel, Channel.EMAIL)
        self.assertFalse(retrieved.shift_offers)
        self.assertEqual(retrieved.quiet_hours_start, 22)

    def test_get_default_preference(self):
        """Test getting default preference when not set."""
        store = get_comms_store()

        pref = store.get_preference("emp_999", "venue_999")

        self.assertEqual(pref.employee_id, "emp_999")
        self.assertEqual(pref.venue_id, "venue_999")
        self.assertEqual(pref.preferred_channel, Channel.SMS)
        self.assertTrue(pref.roster_changes)
        self.assertEqual(pref.quiet_hours_start, 22)

    def test_update_preference(self):
        """Test updating an existing preference."""
        store = get_comms_store()

        # Save initial
        pref1 = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.SMS,
            shift_offers=True,
        )
        store.save_preference(pref1)

        # Update
        pref2 = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.EMAIL,
            shift_offers=False,
        )
        store.save_preference(pref2)

        # Verify update
        retrieved = store.get_preference("emp_001", "venue_001")
        self.assertEqual(retrieved.preferred_channel, Channel.EMAIL)
        self.assertFalse(retrieved.shift_offers)


class TestStorePersistence(unittest.TestCase):
    """Test store persistence across resets."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_messages_persist_across_store_reset(self):
        """Test that messages persist even after store singleton reset."""
        store1 = get_comms_store()

        # Send a message
        send_bulk(
            venue_id="venue_001",
            recipient_ids=["emp_001"],
            template_id="roster_published",
            variables={
                "week_start": "2026-04-20",
                "shift_count": "5",
                "total_hours": "40",
            },
        )

        # Verify in store1
        msgs1 = store1.get_messages("venue_001")
        self.assertEqual(len(msgs1), 1)

        # Reset singleton
        _reset_for_tests()

        # Get new store instance — should rehydrate from DB
        store2 = get_comms_store()
        msgs2 = store2.get_messages("venue_001")

        # Should still have the message
        self.assertEqual(len(msgs2), 1)

    def test_preferences_persist_across_store_reset(self):
        """Test that preferences persist even after store singleton reset."""
        store1 = get_comms_store()

        pref = NotificationPreference(
            employee_id="emp_001",
            venue_id="venue_001",
            preferred_channel=Channel.EMAIL,
            shift_offers=False,
        )
        store1.save_preference(pref)

        # Verify in store1
        pref1 = store1.get_preference("emp_001", "venue_001")
        self.assertEqual(pref1.preferred_channel, Channel.EMAIL)

        # Reset singleton
        _reset_for_tests()

        # Get new store instance
        store2 = get_comms_store()
        pref2 = store2.get_preference("emp_001", "venue_001")

        # Should still have the preference
        self.assertEqual(pref2.preferred_channel, Channel.EMAIL)
        self.assertFalse(pref2.shift_offers)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_get_unread_count(self):
        """Test getting unread message count."""
        store = get_comms_store()

        message = StaffMessage(
            message_id="msg_001",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="+61412345678",
            channel=Channel.SMS,
            priority=MessagePriority.NORMAL,
            subject="Test",
            body="Test",
            status=MessageStatus.QUEUED,
        )
        store.add_message(message)

        count = store.get_unread_count("emp_001")
        self.assertEqual(count, 1)

    def test_filter_messages_by_channel(self):
        """Test filtering messages by channel."""
        store = get_comms_store()

        # Add SMS message
        msg1 = StaffMessage(
            message_id="msg_001",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="+61412345678",
            channel=Channel.SMS,
            priority=MessagePriority.NORMAL,
            subject="SMS",
            body="SMS message",
            status=MessageStatus.SENT,
        )
        store.add_message(msg1)

        # Add EMAIL message
        msg2 = StaffMessage(
            message_id="msg_002",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="alice@example.com",
            channel=Channel.EMAIL,
            priority=MessagePriority.NORMAL,
            subject="Email",
            body="Email message",
            status=MessageStatus.SENT,
        )
        store.add_message(msg2)

        # Filter by SMS
        sms_msgs = store.get_messages("venue_001", channel=Channel.SMS)
        self.assertEqual(len(sms_msgs), 1)
        self.assertEqual(sms_msgs[0].channel, Channel.SMS)

        # Filter by EMAIL
        email_msgs = store.get_messages("venue_001", channel=Channel.EMAIL)
        self.assertEqual(len(email_msgs), 1)
        self.assertEqual(email_msgs[0].channel, Channel.EMAIL)

    def test_filter_messages_by_status(self):
        """Test filtering messages by status."""
        store = get_comms_store()

        # Add multiple messages with different statuses
        for i in range(3):
            msg = StaffMessage(
                message_id=f"msg_{i:03d}",
                venue_id="venue_001",
                recipient_id="emp_001",
                recipient_name="Alice",
                recipient_contact="+61412345678",
                channel=Channel.SMS,
                priority=MessagePriority.NORMAL,
                subject="Test",
                body="Test",
                status=MessageStatus.SENT,
            )
            store.add_message(msg)

        sent_msgs = store.get_messages("venue_001", status=MessageStatus.SENT)
        self.assertEqual(len(sent_msgs), 3)

    def test_dataclass_to_dict_conversion(self):
        """Test that dataclasses convert to dict correctly."""
        message = StaffMessage(
            message_id="msg_001",
            venue_id="venue_001",
            recipient_id="emp_001",
            recipient_name="Alice",
            recipient_contact="+61412345678",
            channel=Channel.SMS,
            priority=MessagePriority.HIGH,
            subject="Test",
            body="Test message",
            status=MessageStatus.SENT,
            template_id="roster_published",
            sent_at=datetime.now(timezone.utc),
        )

        d = message.to_dict()
        self.assertEqual(d["message_id"], "msg_001")
        self.assertEqual(d["channel"], "sms")
        self.assertEqual(d["priority"], "high")
        self.assertEqual(d["status"], "sent")
        self.assertIsNotNone(d["sent_at"])


class TestBuiltinTemplates(unittest.TestCase):
    """Test built-in templates."""

    def test_all_builtin_templates_defined(self):
        """Test that all expected templates are available."""
        expected_templates = [
            "roster_published",
            "shift_change",
            "shift_offer",
            "announcement",
            "reminder",
            "leave_approved",
        ]

        for template_id in expected_templates:
            self.assertIn(template_id, BUILTIN_TEMPLATES)
            template = BUILTIN_TEMPLATES[template_id]
            self.assertEqual(template.template_id, template_id)
            self.assertGreater(len(template.variables), 0)

    def test_template_channels(self):
        """Test that templates have correct channels."""
        # SMS templates
        sms_templates = ["roster_published", "shift_change", "shift_offer", "reminder"]
        for tid in sms_templates:
            self.assertEqual(BUILTIN_TEMPLATES[tid].channel, Channel.SMS)

        # EMAIL templates
        email_templates = ["announcement", "leave_approved"]
        for tid in email_templates:
            self.assertEqual(BUILTIN_TEMPLATES[tid].channel, Channel.EMAIL)

    def test_template_list_via_store(self):
        """Test listing templates from store."""
        store = get_comms_store()
        templates = store.list_templates()

        self.assertEqual(len(templates), 6)
        template_ids = {t.template_id for t in templates}
        expected_ids = {
            "roster_published",
            "shift_change",
            "shift_offer",
            "announcement",
            "reminder",
            "leave_approved",
        }
        self.assertEqual(template_ids, expected_ids)


if __name__ == "__main__":
    unittest.main()
