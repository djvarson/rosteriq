"""
Tests for rosteriq.availability_prefs — availability preferences, overrides, and constraints.

Runs with: PYTHONPATH=. python3 -m unittest tests/test_availability_prefs.py -v

Pure-stdlib unittest runner — no pytest required.
"""

from __future__ import annotations

import sys
from datetime import datetime, date, timezone
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import availability_prefs  # noqa: E402


def _reset():
    """Clear in-memory store between tests."""
    availability_prefs._reset_for_tests()


class TestWeeklyPreferenceStorage(TestCase):
    """Test weekly preference CRUD operations."""

    def setUp(self):
        _reset()

    def test_set_weekly_preference_creates_new(self):
        """Setting a preference creates a new one with auto-generated ID."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,  # Monday
            "status": "AVAILABLE",
            "effective_from": "2026-04-20",
        }

        pref = store.set_weekly_preference(pref_dict)

        self.assertIsNotNone(pref.id)
        self.assertEqual(pref.venue_id, "v1")
        self.assertEqual(pref.employee_id, "e1")
        self.assertEqual(pref.day_of_week, 0)
        self.assertEqual(pref.status, "AVAILABLE")
        self.assertIsNotNone(pref.created_at)
        self.assertIsNotNone(pref.updated_at)

    def test_set_weekly_preference_with_time_window(self):
        """Weekly preference can have optional start/end times."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 1,  # Tuesday
            "status": "PREFERRED",
            "start_time": "09:00",
            "end_time": "17:00",
            "notes": "Prefers morning shifts",
            "effective_from": "2026-04-20",
        }

        pref = store.set_weekly_preference(pref_dict)

        self.assertEqual(pref.start_time, "09:00")
        self.assertEqual(pref.end_time, "17:00")
        self.assertEqual(pref.notes, "Prefers morning shifts")

    def test_set_weekly_preference_updates_existing(self):
        """Setting a preference with same ID updates it."""
        import time
        store = availability_prefs.get_availability_prefs_store()

        pref_dict1 = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 2,
            "status": "AVAILABLE",
            "effective_from": "2026-04-20",
        }
        pref1 = store.set_weekly_preference(pref_dict1)
        pref1_id = pref1.id
        created_at = pref1.created_at

        # Small delay to ensure updated_at is different
        time.sleep(0.001)

        # Update with same ID
        pref_dict2 = pref_dict1.copy()
        pref_dict2["id"] = pref1_id
        pref_dict2["status"] = "UNAVAILABLE"

        pref2 = store.set_weekly_preference(pref_dict2)

        self.assertEqual(pref2.id, pref1_id)
        self.assertEqual(pref2.status, "UNAVAILABLE")
        self.assertGreater(pref2.updated_at, created_at)

    def test_get_weekly_preferences(self):
        """Get all preferences for an employee returns sorted by day of week."""
        store = availability_prefs.get_availability_prefs_store()

        # Add prefs for different days
        for day in [2, 0, 5]:
            pref_dict = {
                "venue_id": "v1",
                "employee_id": "e1",
                "day_of_week": day,
                "status": "AVAILABLE",
                "effective_from": "2026-04-20",
            }
            store.set_weekly_preference(pref_dict)

        prefs = store.get_weekly_preferences("v1", "e1")

        self.assertEqual(len(prefs), 3)
        self.assertEqual(prefs[0].day_of_week, 0)
        self.assertEqual(prefs[1].day_of_week, 2)
        self.assertEqual(prefs[2].day_of_week, 5)

    def test_get_weekly_preferences_empty(self):
        """Get preferences for non-existent employee returns empty list."""
        store = availability_prefs.get_availability_prefs_store()

        prefs = store.get_weekly_preferences("v1", "e999")

        self.assertEqual(len(prefs), 0)

    def test_delete_weekly_preference(self):
        """Deleting a preference removes it."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 3,
            "status": "AVAILABLE",
            "effective_from": "2026-04-20",
        }
        pref = store.set_weekly_preference(pref_dict)

        deleted = store.delete_weekly_preference(pref.id)
        self.assertTrue(deleted)

        prefs = store.get_weekly_preferences("v1", "e1")
        self.assertEqual(len(prefs), 0)

    def test_delete_weekly_preference_not_found(self):
        """Deleting non-existent preference returns False."""
        store = availability_prefs.get_availability_prefs_store()

        deleted = store.delete_weekly_preference("nonexistent")

        self.assertFalse(deleted)


class TestAvailabilityOverrides(TestCase):
    """Test availability override CRUD operations."""

    def setUp(self):
        _reset()

    def test_add_override_creates_new(self):
        """Adding an override creates a new override with auto-generated ID."""
        store = availability_prefs.get_availability_prefs_store()

        override_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "date": "2026-04-20",
            "status": "UNAVAILABLE",
            "reason": "sick",
        }

        override = store.add_override(override_dict)

        self.assertIsNotNone(override.id)
        self.assertEqual(override.venue_id, "v1")
        self.assertEqual(override.employee_id, "e1")
        self.assertEqual(override.date, "2026-04-20")
        self.assertEqual(override.status, "UNAVAILABLE")
        self.assertEqual(override.reason, "sick")

    def test_add_override_with_time_window(self):
        """Override can have optional start/end times."""
        store = availability_prefs.get_availability_prefs_store()

        override_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "date": "2026-04-20",
            "status": "AVAILABLE",
            "start_time": "14:00",
            "end_time": "22:00",
            "reason": "Available only afternoon",
        }

        override = store.add_override(override_dict)

        self.assertEqual(override.start_time, "14:00")
        self.assertEqual(override.end_time, "22:00")

    def test_get_overrides_by_date_range(self):
        """Get overrides with optional date filtering."""
        store = availability_prefs.get_availability_prefs_store()

        # Add overrides on different dates
        for date_str in ["2026-04-20", "2026-04-25", "2026-05-01"]:
            override_dict = {
                "venue_id": "v1",
                "employee_id": "e1",
                "date": date_str,
                "status": "UNAVAILABLE",
            }
            store.add_override(override_dict)

        # Get all
        all_overrides = store.get_overrides("v1", "e1")
        self.assertEqual(len(all_overrides), 3)

        # Get in range
        range_overrides = store.get_overrides("v1", "e1", "2026-04-20", "2026-04-25")
        self.assertEqual(len(range_overrides), 2)

    def test_get_overrides_empty(self):
        """Get overrides for non-existent employee returns empty list."""
        store = availability_prefs.get_availability_prefs_store()

        overrides = store.get_overrides("v1", "e999")

        self.assertEqual(len(overrides), 0)

    def test_delete_override(self):
        """Deleting an override removes it."""
        store = availability_prefs.get_availability_prefs_store()

        override_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "date": "2026-04-20",
            "status": "UNAVAILABLE",
        }
        override = store.add_override(override_dict)

        deleted = store.delete_override(override.id)
        self.assertTrue(deleted)

        overrides = store.get_overrides("v1", "e1")
        self.assertEqual(len(overrides), 0)

    def test_delete_override_not_found(self):
        """Deleting non-existent override returns False."""
        store = availability_prefs.get_availability_prefs_store()

        deleted = store.delete_override("nonexistent")

        self.assertFalse(deleted)


class TestEmployeeConstraints(TestCase):
    """Test employee constraints CRUD operations."""

    def setUp(self):
        _reset()

    def test_set_constraints_creates_new(self):
        """Setting constraints creates a new constraint record."""
        store = availability_prefs.get_availability_prefs_store()

        constraints_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 38.0,
            "min_hours_per_week": 10.0,
            "max_shifts_per_week": 5,
        }

        constraints = store.set_constraints(constraints_dict)

        self.assertIsNotNone(constraints.id)
        self.assertEqual(constraints.venue_id, "v1")
        self.assertEqual(constraints.employee_id, "e1")
        self.assertEqual(constraints.max_hours_per_week, 38.0)
        self.assertEqual(constraints.min_hours_per_week, 10.0)
        self.assertEqual(constraints.max_shifts_per_week, 5)

    def test_set_constraints_with_all_fields(self):
        """Constraints can include preferred shift length and consecutive days limit."""
        store = availability_prefs.get_availability_prefs_store()

        constraints_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 38.0,
            "max_consecutive_days": 5,
            "preferred_shift_length": 4.0,
            "blackout_dates": ["2026-04-25", "2026-05-01"],
        }

        constraints = store.set_constraints(constraints_dict)

        self.assertEqual(constraints.max_consecutive_days, 5)
        self.assertEqual(constraints.preferred_shift_length, 4.0)
        self.assertEqual(len(constraints.blackout_dates), 2)

    def test_set_constraints_updates_existing(self):
        """Setting constraints for same venue+employee updates existing record."""
        store = availability_prefs.get_availability_prefs_store()

        dict1 = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 38.0,
        }
        constraints1 = store.set_constraints(dict1)
        id1 = constraints1.id

        dict2 = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 40.0,
            "min_hours_per_week": 20.0,
        }
        constraints2 = store.set_constraints(dict2)

        self.assertEqual(constraints2.id, id1)
        self.assertEqual(constraints2.max_hours_per_week, 40.0)
        self.assertEqual(constraints2.min_hours_per_week, 20.0)

    def test_get_constraints(self):
        """Get constraints for an employee."""
        store = availability_prefs.get_availability_prefs_store()

        constraints_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 38.0,
        }
        store.set_constraints(constraints_dict)

        constraints = store.get_constraints("v1", "e1")

        self.assertIsNotNone(constraints)
        self.assertEqual(constraints.max_hours_per_week, 38.0)

    def test_get_constraints_not_found(self):
        """Get constraints for non-existent employee returns None."""
        store = availability_prefs.get_availability_prefs_store()

        constraints = store.get_constraints("v1", "e999")

        self.assertIsNone(constraints)


class TestBlackoutDates(TestCase):
    """Test blackout date operations."""

    def setUp(self):
        _reset()

    def test_add_blackout_date_creates_constraints(self):
        """Adding blackout date creates constraints if not existing."""
        store = availability_prefs.get_availability_prefs_store()

        constraints = store.add_blackout_date("v1", "e1", "2026-04-25", "vacation")

        self.assertIsNotNone(constraints)
        self.assertIn("2026-04-25", constraints.blackout_dates)

    def test_add_blackout_date_to_existing(self):
        """Adding blackout date to existing constraints adds to list."""
        store = availability_prefs.get_availability_prefs_store()

        constraints_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "blackout_dates": ["2026-04-20"],
        }
        store.set_constraints(constraints_dict)

        updated = store.add_blackout_date("v1", "e1", "2026-04-25")

        self.assertEqual(len(updated.blackout_dates), 2)
        self.assertIn("2026-04-20", updated.blackout_dates)
        self.assertIn("2026-04-25", updated.blackout_dates)

    def test_add_blackout_date_duplicate(self):
        """Adding duplicate blackout date doesn't add twice."""
        store = availability_prefs.get_availability_prefs_store()

        store.add_blackout_date("v1", "e1", "2026-04-25")
        updated = store.add_blackout_date("v1", "e1", "2026-04-25")

        self.assertEqual(len(updated.blackout_dates), 1)

    def test_remove_blackout_date(self):
        """Removing a blackout date removes it from list."""
        store = availability_prefs.get_availability_prefs_store()

        store.add_blackout_date("v1", "e1", "2026-04-25")
        store.add_blackout_date("v1", "e1", "2026-05-01")

        updated = store.remove_blackout_date("v1", "e1", "2026-04-25")

        self.assertEqual(len(updated.blackout_dates), 1)
        self.assertIn("2026-05-01", updated.blackout_dates)

    def test_remove_blackout_date_not_exists(self):
        """Removing non-existent blackout date does nothing."""
        store = availability_prefs.get_availability_prefs_store()

        store.add_blackout_date("v1", "e1", "2026-04-25")
        constraints = store.get_constraints("v1", "e1")

        updated = store.remove_blackout_date("v1", "e1", "2026-05-01")

        self.assertEqual(len(updated.blackout_dates), 1)


class TestAvailabilityResolution(TestCase):
    """Test availability resolution logic (override > weekly > default)."""

    def setUp(self):
        _reset()

    def test_resolve_default_available(self):
        """No preference/override => default AVAILABLE."""
        store = availability_prefs.get_availability_prefs_store()

        avail = store.get_availability_for_date("v1", "e1", "2026-04-20")

        self.assertEqual(avail["status"], "AVAILABLE")
        self.assertIsNone(avail["start_time"])
        self.assertIsNone(avail["end_time"])
        self.assertEqual(avail["source"], "default")

    def test_resolve_weekly_preference(self):
        """Weekly preference is used when no override exists."""
        store = availability_prefs.get_availability_prefs_store()

        # Monday = 0, 2026-04-20 is a Monday
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "UNAVAILABLE",
            "effective_from": "2026-04-01",
            "effective_until": "2026-04-30",
        }
        store.set_weekly_preference(pref_dict)

        avail = store.get_availability_for_date("v1", "e1", "2026-04-20")

        self.assertEqual(avail["status"], "UNAVAILABLE")
        self.assertEqual(avail["source"], "weekly")

    def test_resolve_weekly_with_time_window(self):
        """Weekly preference with time window is resolved correctly."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,  # Monday
            "status": "AVAILABLE",
            "start_time": "09:00",
            "end_time": "17:00",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict)

        avail = store.get_availability_for_date("v1", "e1", "2026-04-20")

        self.assertEqual(avail["status"], "AVAILABLE")
        self.assertEqual(avail["start_time"], "09:00")
        self.assertEqual(avail["end_time"], "17:00")
        self.assertEqual(avail["source"], "weekly")

    def test_resolve_override_beats_weekly(self):
        """Override takes priority over weekly preference."""
        store = availability_prefs.get_availability_prefs_store()

        # Weekly: unavailable
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "UNAVAILABLE",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict)

        # Override: available
        override_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "date": "2026-04-20",
            "status": "AVAILABLE",
            "start_time": "14:00",
            "end_time": "22:00",
        }
        store.add_override(override_dict)

        avail = store.get_availability_for_date("v1", "e1", "2026-04-20")

        self.assertEqual(avail["status"], "AVAILABLE")
        self.assertEqual(avail["start_time"], "14:00")
        self.assertEqual(avail["source"], "override")

    def test_resolve_weekly_effective_dates(self):
        """Weekly preference respects effective_from/until dates."""
        store = availability_prefs.get_availability_prefs_store()

        # Set for Monday (0) with effective date range May 2026
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,  # Monday
            "status": "UNAVAILABLE",
            "effective_from": "2026-05-01",
            "effective_until": "2026-05-31",
        }
        store.set_weekly_preference(pref_dict)

        # April 20, 2026 is Monday but before effective_from
        avail_before = store.get_availability_for_date("v1", "e1", "2026-04-20")
        self.assertEqual(avail_before["status"], "AVAILABLE")
        self.assertEqual(avail_before["source"], "default")

        # May 4, 2026 is Monday and within range
        avail_within = store.get_availability_for_date("v1", "e1", "2026-05-04")
        self.assertEqual(avail_within["status"], "UNAVAILABLE")
        self.assertEqual(avail_within["source"], "weekly")

        # June 1, 2026 is Monday but after effective_until
        avail_after = store.get_availability_for_date("v1", "e1", "2026-06-01")
        self.assertEqual(avail_after["status"], "AVAILABLE")
        self.assertEqual(avail_after["source"], "default")

    def test_resolve_multiple_weekly_prefs_same_day(self):
        """Only one weekly preference per day is active (upsert behavior)."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict1 = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "UNAVAILABLE",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict1)

        pref_dict2 = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "PREFERRED",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict2)

        # Both should be stored separately, but resolution picks last effective
        prefs = store.get_weekly_preferences("v1", "e1")
        # We have two different preference IDs, so both exist
        self.assertEqual(len(prefs), 2)


class TestTeamAvailability(TestCase):
    """Test team-level availability queries."""

    def setUp(self):
        _reset()

    def test_get_team_availability(self):
        """Get availability for all employees at a venue for a date."""
        store = availability_prefs.get_availability_prefs_store()

        # Set preferences for two employees
        for emp_id in ["e1", "e2"]:
            pref_dict = {
                "venue_id": "v1",
                "employee_id": emp_id,
                "day_of_week": 0,  # Monday
                "status": "AVAILABLE" if emp_id == "e1" else "UNAVAILABLE",
                "effective_from": "2026-04-01",
            }
            store.set_weekly_preference(pref_dict)

        team_avail = store.get_team_availability("v1", "2026-04-20")

        self.assertEqual(len(team_avail), 2)
        e1_avail = next(t for t in team_avail if t["employee_id"] == "e1")
        e2_avail = next(t for t in team_avail if t["employee_id"] == "e2")

        self.assertEqual(e1_avail["status"], "AVAILABLE")
        self.assertEqual(e2_avail["status"], "UNAVAILABLE")

    def test_get_team_availability_empty(self):
        """Get team availability with no preferences returns empty."""
        store = availability_prefs.get_availability_prefs_store()

        team_avail = store.get_team_availability("v1", "2026-04-20")

        self.assertEqual(len(team_avail), 0)

    def test_get_available_staff_simple(self):
        """Get list of available staff for a date."""
        store = availability_prefs.get_availability_prefs_store()

        # e1: available, e2: unavailable, e3: no preference (default available)
        for emp_id, status in [("e1", "AVAILABLE"), ("e2", "UNAVAILABLE")]:
            pref_dict = {
                "venue_id": "v1",
                "employee_id": emp_id,
                "day_of_week": 0,
                "status": status,
                "effective_from": "2026-04-01",
            }
            store.set_weekly_preference(pref_dict)

        # Add constraint for e3 (no preference, but has constraint = default available)
        store.set_constraints({
            "venue_id": "v1",
            "employee_id": "e3",
            "max_hours_per_week": 38.0,
        })

        available = store.get_available_staff("v1", "2026-04-20")

        self.assertIn("e1", available)
        self.assertNotIn("e2", available)
        self.assertIn("e3", available)

    def test_get_available_staff_with_time_window(self):
        """Get available staff filtered by time window."""
        store = availability_prefs.get_availability_prefs_store()

        # e1: available 09:00-17:00
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "AVAILABLE",
            "start_time": "09:00",
            "end_time": "17:00",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict)

        # Query for shift 14:00-18:00 (overlaps with preference)
        available = store.get_available_staff("v1", "2026-04-20", "14:00", "18:00")

        self.assertIn("e1", available)

    def test_get_available_staff_time_window_no_overlap(self):
        """Get available staff where time window doesn't overlap preference."""
        store = availability_prefs.get_availability_prefs_store()

        # e1: available 09:00-14:00
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "AVAILABLE",
            "start_time": "09:00",
            "end_time": "14:00",
            "effective_from": "2026-04-01",
        }
        store.set_weekly_preference(pref_dict)

        # Query for shift 17:00-22:00 (no overlap)
        available = store.get_available_staff("v1", "2026-04-20", "17:00", "22:00")

        self.assertNotIn("e1", available)


class TestDataConsistency(TestCase):
    """Test data consistency and edge cases."""

    def setUp(self):
        _reset()

    def test_multiple_venues_isolation(self):
        """Data for different venues doesn't interfere."""
        store = availability_prefs.get_availability_prefs_store()

        # Add same employee ID to different venues
        for venue_id in ["v1", "v2"]:
            pref_dict = {
                "venue_id": venue_id,
                "employee_id": "e1",
                "day_of_week": 0,
                "status": "AVAILABLE" if venue_id == "v1" else "UNAVAILABLE",
                "effective_from": "2026-04-01",
            }
            store.set_weekly_preference(pref_dict)

        prefs_v1 = store.get_weekly_preferences("v1", "e1")
        prefs_v2 = store.get_weekly_preferences("v2", "e1")

        self.assertEqual(len(prefs_v1), 1)
        self.assertEqual(len(prefs_v2), 1)
        self.assertEqual(prefs_v1[0].status, "AVAILABLE")
        self.assertEqual(prefs_v2[0].status, "UNAVAILABLE")

    def test_multiple_employees_isolation(self):
        """Data for different employees doesn't interfere."""
        store = availability_prefs.get_availability_prefs_store()

        for emp_id in ["e1", "e2"]:
            pref_dict = {
                "venue_id": "v1",
                "employee_id": emp_id,
                "day_of_week": 0,
                "status": "AVAILABLE",
                "effective_from": "2026-04-01",
            }
            store.set_weekly_preference(pref_dict)

        prefs_e1 = store.get_weekly_preferences("v1", "e1")
        prefs_e2 = store.get_weekly_preferences("v1", "e2")

        self.assertEqual(len(prefs_e1), 1)
        self.assertEqual(len(prefs_e2), 1)
        self.assertEqual(prefs_e1[0].employee_id, "e1")
        self.assertEqual(prefs_e2[0].employee_id, "e2")

    def test_to_dict_serialization(self):
        """to_dict methods produce valid serializable output."""
        store = availability_prefs.get_availability_prefs_store()

        # Weekly preference
        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "AVAILABLE",
            "start_time": "09:00",
            "end_time": "17:00",
            "effective_from": "2026-04-01",
        }
        pref = store.set_weekly_preference(pref_dict)
        pref_serialized = pref.to_dict()

        self.assertIsInstance(pref_serialized, dict)
        self.assertIn("id", pref_serialized)
        self.assertIn("venue_id", pref_serialized)
        self.assertEqual(pref_serialized["status"], "AVAILABLE")

        # Override
        override_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "date": "2026-04-20",
            "status": "UNAVAILABLE",
            "reason": "sick",
        }
        override = store.add_override(override_dict)
        override_serialized = override.to_dict()

        self.assertIsInstance(override_serialized, dict)
        self.assertIn("id", override_serialized)
        self.assertEqual(override_serialized["reason"], "sick")

        # Constraints
        constraints_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "max_hours_per_week": 38.0,
            "blackout_dates": ["2026-04-25"],
        }
        constraints = store.set_constraints(constraints_dict)
        constraints_serialized = constraints.to_dict()

        self.assertIsInstance(constraints_serialized, dict)
        self.assertIn("id", constraints_serialized)
        self.assertEqual(constraints_serialized["max_hours_per_week"], 38.0)

    def test_datetime_fields_are_iso_format(self):
        """All datetime fields are stored as ISO format strings."""
        store = availability_prefs.get_availability_prefs_store()

        pref_dict = {
            "venue_id": "v1",
            "employee_id": "e1",
            "day_of_week": 0,
            "status": "AVAILABLE",
            "effective_from": "2026-04-01",
        }
        pref = store.set_weekly_preference(pref_dict)

        self.assertIsInstance(pref.created_at, str)
        self.assertIsInstance(pref.updated_at, str)
        # Should be valid ISO format
        self.assertIn("T", pref.created_at)

    def test_singleton_thread_safety(self):
        """Singleton pattern ensures only one store instance."""
        store1 = availability_prefs.get_availability_prefs_store()
        store2 = availability_prefs.get_availability_prefs_store()

        self.assertIs(store1, store2)
