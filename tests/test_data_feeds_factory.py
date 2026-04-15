"""
Tests for Data Feeds Factory
============================

Tests factory functions for creating POS and bookings adapters
with fallback to demo on configuration errors.

Uses stdlib unittest, no pytest required.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.data_feeds_factory import (
    get_pos_adapter,
    get_bookings_adapter,
    get_bookings_store,
    BookingsStore,
    _get_demo_pos_adapter,
    _get_demo_bookings_adapter,
)


class TestBookingsStore(unittest.TestCase):
    """Test in-memory bookings store."""

    def test_store_creation(self):
        store = BookingsStore()
        self.assertEqual(store.count(), 0)

    def test_add_bookings(self):
        store = BookingsStore()
        bookings = [
            {"date": "2026-04-15", "time": "18:00", "covers": 4, "name": "Test"},
            {"date": "2026-04-16", "time": "19:00", "covers": 2, "name": "Test2"},
        ]
        store.add_bookings(bookings)
        self.assertEqual(store.count(), 2)

    def test_get_bookings_all(self):
        store = BookingsStore()
        bookings = [
            {"date": "2026-04-15", "covers": 4},
            {"date": "2026-04-16", "covers": 2},
        ]
        store.add_bookings(bookings)
        result = store.get_bookings()
        self.assertEqual(len(result), 2)

    def test_get_bookings_filtered(self):
        store = BookingsStore()
        bookings = [
            {"date": "2026-04-14", "covers": 1},
            {"date": "2026-04-15", "covers": 4},
            {"date": "2026-04-16", "covers": 2},
            {"date": "2026-04-17", "covers": 3},
        ]
        store.add_bookings(bookings)
        result = store.get_bookings("2026-04-15", "2026-04-16")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["date"], "2026-04-15")
        self.assertEqual(result[1]["date"], "2026-04-16")

    def test_clear(self):
        store = BookingsStore()
        store.add_bookings([{"date": "2026-04-15", "covers": 4}])
        self.assertEqual(store.count(), 1)
        store.clear()
        self.assertEqual(store.count(), 0)


class TestGlobalBookingsStore(unittest.TestCase):
    """Test global bookings store singleton."""

    def setUp(self):
        # Clear global store before each test
        store = get_bookings_store()
        store.clear()

    def test_singleton_pattern(self):
        store1 = get_bookings_store()
        store2 = get_bookings_store()
        self.assertIs(store1, store2)

    def test_persistence_across_calls(self):
        store1 = get_bookings_store()
        store1.add_bookings([{"date": "2026-04-15", "covers": 4}])

        store2 = get_bookings_store()
        self.assertEqual(store2.count(), 1)


class TestPOSAdapterFactory(unittest.TestCase):
    """Test POS adapter factory."""

    def test_demo_adapter_default(self):
        """Test that demo adapter is used by default."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove POS env vars to force demo
            for key in list(os.environ.keys()):
                if key.startswith("SWIFTPOS") or key.startswith("ROSTERIQ_POS"):
                    del os.environ[key]

            adapter = get_pos_adapter()
            self.assertIsNotNone(adapter)
            self.assertEqual(adapter.location_id, "DEMO001")

    def test_demo_adapter_on_missing_credentials(self):
        """Test fallback to demo on incomplete credentials."""
        with patch.dict(os.environ, {"ROSTERIQ_POS_BACKEND": "swiftpos"}):
            # Missing required env vars
            adapter = get_pos_adapter()
            self.assertEqual(adapter.location_id, "DEMO001")

    def test_demo_pos_adapter_function(self):
        """Test _get_demo_pos_adapter directly."""
        adapter = _get_demo_pos_adapter()
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.location_id, "DEMO001")
        self.assertEqual(adapter.location_name, "Demo Venue")


class TestBookingsAdapterFactory(unittest.TestCase):
    """Test bookings adapter factory."""

    def test_demo_adapter_default(self):
        """Test that demo adapter is used by default."""
        with patch.dict(os.environ, {}, clear=False):
            # Remove bookings env vars to force demo
            for key in list(os.environ.keys()):
                if key.startswith("NOWBOOKIT") or key.startswith("ROSTERIQ_BOOKINGS"):
                    del os.environ[key]

            adapter = get_bookings_adapter()
            self.assertIsNotNone(adapter)
            self.assertEqual(adapter.venue_id, "demo_venue")

    def test_demo_adapter_on_missing_credentials(self):
        """Test fallback to demo on incomplete credentials."""
        with patch.dict(os.environ, {"ROSTERIQ_BOOKINGS_BACKEND": "nowbookit"}):
            # Missing required env vars
            adapter = get_bookings_adapter()
            self.assertEqual(adapter.venue_id, "demo_venue")

    def test_demo_bookings_adapter_function(self):
        """Test _get_demo_bookings_adapter directly."""
        adapter = _get_demo_bookings_adapter()
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.venue_id, "demo_venue")


class TestEnvironmentVariables(unittest.TestCase):
    """Test environment variable handling."""

    def test_pos_backend_env_var(self):
        """Test ROSTERIQ_POS_BACKEND env var."""
        with patch.dict(os.environ, {"ROSTERIQ_POS_BACKEND": "demo"}):
            adapter = get_pos_adapter()
            self.assertIsNotNone(adapter)

    def test_bookings_backend_env_var(self):
        """Test ROSTERIQ_BOOKINGS_BACKEND env var."""
        with patch.dict(os.environ, {"ROSTERIQ_BOOKINGS_BACKEND": "demo"}):
            adapter = get_bookings_adapter()
            self.assertIsNotNone(adapter)

    def test_nowbookit_api_variant(self):
        """Test nowbookit_api backend name."""
        with patch.dict(os.environ, {"ROSTERIQ_BOOKINGS_BACKEND": "nowbookit_api"}):
            # Missing credentials, should fall back to demo
            adapter = get_bookings_adapter()
            self.assertEqual(adapter.venue_id, "demo_venue")


if __name__ == "__main__":
    # Run all tests
    passed = failed = 0
    for name, obj in list(globals().items()):
        if isinstance(obj, type) and name.startswith("Test"):
            inst = obj()
            if hasattr(inst, "setUp"):
                inst.setUp()
            for mname in sorted(dir(inst)):
                if mname.startswith("test_"):
                    try:
                        getattr(inst, mname)()
                        passed += 1
                        print(f"  PASS {name}.{mname}")
                    except AssertionError as e:
                        failed += 1
                        print(f"  FAIL {name}.{mname}: {e}")
                    except Exception as e:
                        failed += 1
                        print(f"  ERROR {name}.{mname}: {type(e).__name__}: {e}")

    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
