"""
Tests for rosteriq.ws_hub — WebSocket notification hub and store.

Runs with: PYTHONPATH=. python3 -m unittest tests.test_ws_hub -v

Stdlib unittest, no pytest, no asyncio.run — uses asyncio.new_event_loop().run_until_complete().
"""

from __future__ import annotations

import sys
import unittest
import asyncio
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import ws_hub


class TestNotificationCreation(unittest.TestCase):
    """Test Notification dataclass."""

    def test_notification_creation(self):
        """Verify Notification fields and defaults."""
        notif = ws_hub.Notification(
            venue_id="venue1",
            kind="cut_recommendation",
            title="Cut 2 staff",
            body="Consider cutting Alice and Bob",
            severity="warning",
        )
        assert notif.venue_id == "venue1"
        assert notif.kind == "cut_recommendation"
        assert notif.title == "Cut 2 staff"
        assert notif.body == "Consider cutting Alice and Bob"
        assert notif.severity == "warning"
        assert notif.acknowledged is False
        assert notif.id  # auto-generated
        assert len(notif.id) == 12  # uuid4 hex[:12]
        assert notif.created_at  # auto-generated

    def test_notification_to_dict(self):
        """Verify Notification.to_dict() serialization."""
        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
            data={"key": "value"},
            severity="critical",
            acknowledged=True,
        )
        d = notif.to_dict()
        assert d["venue_id"] == "v1"
        assert d["kind"] == "alert"
        assert d["title"] == "Test"
        assert d["body"] == "Body"
        assert d["data"] == {"key": "value"}
        assert d["severity"] == "critical"
        assert d["acknowledged"] is True
        assert d["id"]
        assert d["created_at"]  # ISO format string


class TestNotificationStore(unittest.TestCase):
    """Test NotificationStore."""

    def setUp(self):
        """Reset singletons before each test."""
        ws_hub.reset_singletons()

    def test_add_and_list(self):
        """Test adding notifications and listing them."""
        store = ws_hub.NotificationStore(max_size=10)
        notifs = []
        for i in range(5):
            n = ws_hub.Notification(
                venue_id="v1",
                kind="alert",
                title=f"Alert {i}",
                body=f"Body {i}",
            )
            store.add(n)
            notifs.append(n)

        # List should return newest first
        listed = store.list_for_venue("v1", limit=50)
        assert len(listed) == 5
        assert listed[0].id == notifs[4].id  # newest
        assert listed[4].id == notifs[0].id  # oldest

    def test_max_size_enforcement(self):
        """Test that store trims old notifications beyond max_size."""
        store = ws_hub.NotificationStore(max_size=200)

        # Add 250 notifications
        for i in range(250):
            n = ws_hub.Notification(
                venue_id="v1",
                kind="alert",
                title=f"Alert {i}",
                body=f"Body {i}",
            )
            store.add(n)

        # Should only keep last 200
        listed = store.list_for_venue("v1", limit=500)
        assert len(listed) == 200

    def test_acknowledge(self):
        """Test acknowledging a notification."""
        store = ws_hub.NotificationStore()
        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )
        store.add(notif)
        assert notif.acknowledged is False

        success = store.acknowledge(notif.id)
        assert success is True
        assert notif.acknowledged is True

    def test_acknowledge_nonexistent(self):
        """Test acknowledging a notification that doesn't exist."""
        store = ws_hub.NotificationStore()
        success = store.acknowledge("nonexistent_id")
        assert success is False

    def test_clear_venue(self):
        """Test clearing all notifications for a venue."""
        store = ws_hub.NotificationStore()
        for i in range(5):
            store.add(
                ws_hub.Notification(
                    venue_id="v1",
                    kind="alert",
                    title=f"Alert {i}",
                    body=f"Body {i}",
                )
            )
        for i in range(3):
            store.add(
                ws_hub.Notification(
                    venue_id="v2",
                    kind="alert",
                    title=f"Alert {i}",
                    body=f"Body {i}",
                )
            )

        store.clear_venue("v1")
        assert len(store.list_for_venue("v1", limit=50)) == 0
        assert len(store.list_for_venue("v2", limit=50)) == 3


class TestWSHub(unittest.TestCase):
    """Test WSHub pub-sub functionality."""

    def setUp(self):
        """Reset singletons before each test."""
        ws_hub.reset_singletons()

    def test_subscribe_and_unsubscribe(self):
        """Test subscribe and unsubscribe."""
        hub = ws_hub.WSHub()

        async def dummy_send(data):
            pass

        hub.subscribe("v1", dummy_send)
        assert hub.subscriber_count("v1") == 1

        hub.unsubscribe("v1", dummy_send)
        assert hub.subscriber_count("v1") == 0

    def test_broadcast_to_subscribers(self):
        """Test broadcasting to subscribers."""
        hub = ws_hub.WSHub()

        received = []

        async def mock_send(data):
            received.append(data)

        hub.subscribe("v1", mock_send)
        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )

        # Run broadcast in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(hub.broadcast("v1", notif))
        finally:
            loop.close()

        assert len(received) == 1
        assert received[0]["id"] == notif.id
        assert received[0]["title"] == "Test"

    def test_broadcast_multiple_subscribers(self):
        """Test broadcasting to multiple subscribers."""
        hub = ws_hub.WSHub()

        received1 = []
        received2 = []

        async def send1(data):
            received1.append(data)

        async def send2(data):
            received2.append(data)

        hub.subscribe("v1", send1)
        hub.subscribe("v1", send2)

        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(hub.broadcast("v1", notif))
        finally:
            loop.close()

        assert len(received1) == 1
        assert len(received2) == 1

    def test_broadcast_isolated_by_venue(self):
        """Test that broadcasts are isolated by venue."""
        hub = ws_hub.WSHub()

        received_v1 = []
        received_v2 = []

        async def send_v1(data):
            received_v1.append(data)

        async def send_v2(data):
            received_v2.append(data)

        hub.subscribe("v1", send_v1)
        hub.subscribe("v2", send_v2)

        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(hub.broadcast("v1", notif))
        finally:
            loop.close()

        assert len(received_v1) == 1
        assert len(received_v2) == 0

    def test_broadcast_error_handling(self):
        """Test that broadcast catches per-subscriber errors."""
        hub = ws_hub.WSHub()

        received_ok = []

        async def send_error(data):
            raise RuntimeError("Test error")

        async def send_ok(data):
            received_ok.append(data)

        hub.subscribe("v1", send_error)
        hub.subscribe("v1", send_ok)

        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Should not raise despite send_error failing
            loop.run_until_complete(hub.broadcast("v1", notif))
        finally:
            loop.close()

        assert len(received_ok) == 1

    def test_publish_stores_and_broadcasts(self):
        """Test publish: stores notification and broadcasts."""
        hub = ws_hub.WSHub()
        store = ws_hub.NotificationStore()

        # Replace singleton
        ws_hub._store_instance = store

        received = []

        async def mock_send(data):
            received.append(data)

        hub.subscribe("v1", mock_send)

        notif = ws_hub.Notification(
            venue_id="v1",
            kind="alert",
            title="Test",
            body="Body",
        )

        hub.publish("v1", notif)

        # Give time for async broadcast
        import time
        time.sleep(0.1)

        # Check it was stored
        stored = store.list_for_venue("v1", limit=50)
        assert len(stored) >= 1
        assert stored[0].id == notif.id

        # Check it was broadcast
        assert len(received) >= 1


class TestBuildHelpers(unittest.TestCase):
    """Test notification builder functions."""

    def test_build_cut_recommendation(self):
        """Test building cut recommendation."""
        notif = ws_hub.build_cut_recommendation(
            venue_id="v1",
            confidence_pct=85.5,
            staff_names=["Alice", "Bob"],
            reason="low footfall",
        )

        assert notif.venue_id == "v1"
        assert notif.kind == "cut_recommendation"
        assert notif.severity == "warning"
        assert "86%" in notif.title or "85%" in notif.title  # confidence (0-based rounding)
        assert "Cut" in notif.title
        assert "low footfall" in notif.body
        assert notif.data["confidence_pct"] == 85.5
        assert notif.data["staff_names"] == ["Alice", "Bob"]
        assert notif.data["reason"] == "low footfall"

    def test_build_cut_recommendation_empty_staff(self):
        """Test cut recommendation with empty staff list."""
        notif = ws_hub.build_cut_recommendation(
            venue_id="v1",
            confidence_pct=50,
            staff_names=[],
            reason="testing",
        )

        assert notif.kind == "cut_recommendation"
        assert "staff" in notif.body

    def test_build_call_recommendation(self):
        """Test building call recommendation."""
        notif = ws_hub.build_call_recommendation(
            venue_id="v1",
            confidence_pct=75.0,
            role="line cook",
            reason="high orders",
        )

        assert notif.venue_id == "v1"
        assert notif.kind == "call_recommendation"
        assert notif.severity == "warning"
        assert "75" in notif.title  # confidence
        assert "line cook" in notif.title
        assert "high orders" in notif.body
        assert notif.data["confidence_pct"] == 75.0
        assert notif.data["role"] == "line cook"
        assert notif.data["reason"] == "high orders"

    def test_build_signal_update(self):
        """Test building signal update."""
        notif = ws_hub.build_signal_update(
            venue_id="v1",
            signal_type="footfall",
            old_value=100,
            new_value=150,
        )

        assert notif.venue_id == "v1"
        assert notif.kind == "signal_update"
        assert notif.severity == "info"
        assert "footfall" in notif.title.lower()
        assert "100" in notif.body
        assert "150" in notif.body
        assert notif.data["signal_type"] == "footfall"
        assert notif.data["old_value"] == 100
        assert notif.data["new_value"] == 150


class TestSingletons(unittest.TestCase):
    """Test singleton management."""

    def setUp(self):
        """Reset singletons before each test."""
        ws_hub.reset_singletons()

    def test_get_hub_singleton(self):
        """Test get_hub returns same instance."""
        hub1 = ws_hub.get_hub()
        hub2 = ws_hub.get_hub()
        assert hub1 is hub2

    def test_get_notification_store_singleton(self):
        """Test get_notification_store returns same instance."""
        store1 = ws_hub.get_notification_store()
        store2 = ws_hub.get_notification_store()
        assert store1 is store2

    def test_reset_singletons(self):
        """Test resetting singletons."""
        hub1 = ws_hub.get_hub()
        store1 = ws_hub.get_notification_store()

        ws_hub.reset_singletons()

        hub2 = ws_hub.get_hub()
        store2 = ws_hub.get_notification_store()

        assert hub1 is not hub2
        assert store1 is not store2


if __name__ == "__main__":
    unittest.main()
