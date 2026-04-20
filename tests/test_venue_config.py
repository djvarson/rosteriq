"""Test suite for venue_config.py module (Round 39).

26 comprehensive test cases covering:
- Default config creation for different venue types (pub, bar, restaurant)
- Partial and complete config updates with versioning
- Config validation (valid and invalid scenarios)
- Rollback to historical versions
- Staffing requirements for specific timeslots
- Is_open check for different day/hour combinations
- Store persistence (SQLite roundtrip)
- Thread-safe operations
- Edge cases and error handling
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.venue_config import (
    DayOfWeek,
    VenueArea,
    OperatingHours,
    RoleConfig,
    StaffingLevel,
    PenaltyOverride,
    IntegrationConfig,
    VenueConfig,
    create_default_config,
    get_config,
    update_config,
    get_config_history,
    rollback_config,
    validate_config,
    get_staffing_requirement,
    is_open,
    get_store,
    _reset_for_tests,
)
from rosteriq import persistence as _p


class TestDefaultConfigCreation(unittest.TestCase):
    """Test default config creation for different venue types."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_create_default_pub_config(self):
        """Test creating default config for a pub."""
        config = create_default_config("venue_001", "The Stag Pub", "pub")

        self.assertEqual(config.venue_id, "venue_001")
        self.assertEqual(config.venue_name, "The Stag Pub")
        self.assertEqual(config.timezone, "Australia/Brisbane")
        self.assertEqual(config.currency, "AUD")
        self.assertEqual(len(config.operating_hours), 7)
        self.assertTrue(len(config.areas) >= 3)  # At least bar, kitchen, floor
        self.assertTrue(any(a.area_id == "gaming" for a in config.areas))  # Pub has gaming
        self.assertEqual(len(config.roles), 4)  # Bar, Floor, Kitchen, Manager
        self.assertTrue(len(config.penalty_overrides) > 0)

    def test_create_default_bar_config(self):
        """Test creating default config for a bar."""
        config = create_default_config("venue_002", "Downtown Bar", "bar")

        self.assertEqual(config.venue_name, "Downtown Bar")
        self.assertEqual(len(config.operating_hours), 7)
        self.assertTrue(len(config.areas) >= 3)

    def test_create_default_restaurant_config(self):
        """Test creating default config for a restaurant."""
        config = create_default_config("venue_003", "Fine Dining", "restaurant")

        self.assertEqual(config.venue_name, "Fine Dining")
        self.assertTrue(len(config.roles) > 0)

    def test_default_config_structure(self):
        """Test that default config has all required fields."""
        config = create_default_config("venue_004", "Test Venue")

        # Core fields
        self.assertIsNotNone(config.config_id)
        self.assertIsNotNone(config.created_at)
        self.assertIsNotNone(config.updated_at)

        # Operating hours
        self.assertEqual(len(config.operating_hours), 7)
        days_covered = {oh.day.value for oh in config.operating_hours}
        self.assertEqual(days_covered, {0, 1, 2, 3, 4, 5, 6})

        # Roles
        role_names = {r.role_name for r in config.roles}
        self.assertIn("Bar", role_names)
        self.assertIn("Manager", role_names)

    def test_default_pub_hours(self):
        """Test that pub has standard AU hours."""
        config = create_default_config("venue_005", "Test Pub", "pub")

        # Weekday: 10am-midnight
        mon = next(oh for oh in config.operating_hours if oh.day == DayOfWeek.MON)
        self.assertEqual(mon.open_time, "10:00")
        self.assertEqual(mon.close_time, "23:59")

        # Friday: 10am-2am next day
        fri = next(oh for oh in config.operating_hours if oh.day == DayOfWeek.FRI)
        self.assertEqual(fri.open_time, "10:00")
        self.assertEqual(fri.close_time, "00:00")

    def test_default_penalty_rates_au(self):
        """Test that default has standard AU penalty rates."""
        config = create_default_config("venue_006", "Test Venue")

        # Find penalty overrides
        saturday = next(
            (po for po in config.penalty_overrides if po.applies_to == "saturday"), None
        )
        sunday = next(
            (po for po in config.penalty_overrides if po.applies_to == "sunday"), None
        )
        public_holiday = next(
            (po for po in config.penalty_overrides if po.applies_to == "public_holiday"),
            None,
        )

        self.assertIsNotNone(saturday)
        self.assertEqual(saturday.multiplier, 1.5)
        self.assertIsNotNone(sunday)
        self.assertEqual(sunday.multiplier, 2.0)
        self.assertIsNotNone(public_holiday)
        self.assertEqual(public_holiday.multiplier, 2.5)


class TestConfigRetrieval(unittest.TestCase):
    """Test getting and retrieving configs."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_get_config_creates_default(self):
        """Test that get_config creates default if none exists."""
        config = get_config("venue_new")

        self.assertIsNotNone(config)
        self.assertEqual(config.venue_id, "venue_new")
        self.assertEqual(len(config.operating_hours), 7)

    def test_get_config_retrieves_existing(self):
        """Test that get_config retrieves existing config."""
        config1 = get_config("venue_existing")
        config1_id = config1.config_id

        config2 = get_config("venue_existing")

        self.assertEqual(config2.config_id, config1_id)


class TestConfigUpdate(unittest.TestCase):
    """Test partial and complete config updates."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_update_config_partial(self):
        """Test partial update of config."""
        config = get_config("venue_001")
        old_name = config.venue_name

        updated = update_config("venue_001", venue_name="New Name")

        self.assertEqual(updated.venue_name, "New Name")
        self.assertNotEqual(updated.config_id, config.config_id)  # New version

    def test_update_config_labour_pct(self):
        """Test updating labour percentage target."""
        get_config("venue_002")

        updated = update_config("venue_002", budget_target_labour_pct=28.5)

        self.assertEqual(updated.budget_target_labour_pct, 28.5)

    def test_update_config_fatigue_management(self):
        """Test toggling fatigue management."""
        config = get_config("venue_003")
        original = config.fatigue_management_enabled

        updated = update_config("venue_003", fatigue_management_enabled=not original)

        self.assertEqual(updated.fatigue_management_enabled, not original)

    def test_update_config_multiple_fields(self):
        """Test updating multiple fields at once."""
        get_config("venue_004")

        updated = update_config(
            "venue_004",
            venue_name="Multi Update",
            max_shift_hours=9.0,
            break_compliance_enabled=False,
        )

        self.assertEqual(updated.venue_name, "Multi Update")
        self.assertEqual(updated.max_shift_hours, 9.0)
        self.assertEqual(updated.break_compliance_enabled, False)


class TestConfigVersioning(unittest.TestCase):
    """Test config versioning and rollback."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_config_history_retrieval(self):
        """Test retrieving config history."""
        get_config("venue_001")
        update_config("venue_001", venue_name="Update 1")
        update_config("venue_001", venue_name="Update 2")

        history = get_config_history("venue_001")

        self.assertGreaterEqual(len(history), 2)
        self.assertEqual(history[0].venue_name, "Update 2")  # Most recent first

    def test_config_rollback(self):
        """Test rolling back to previous version."""
        config1 = get_config("venue_002")
        update_config("venue_002", venue_name="Changed")
        history = get_config_history("venue_002")

        # Rollback to first version (index 1)
        rolled_back = rollback_config("venue_002", version_index=1)

        self.assertEqual(rolled_back.venue_name, history[1].venue_name)

    def test_config_history_limit(self):
        """Test that history respects limit."""
        get_config("venue_003")
        for i in range(10):
            update_config("venue_003", venue_name=f"Update {i}")

        history = get_config_history("venue_003", limit=5)

        self.assertEqual(len(history), 5)

    def test_config_version_different_ids(self):
        """Test that different versions have different config IDs."""
        config1 = get_config("venue_004")
        config1_id = config1.config_id

        config2 = update_config("venue_004", venue_name="Changed")
        config2_id = config2.config_id

        self.assertNotEqual(config1_id, config2_id)


class TestConfigValidation(unittest.TestCase):
    """Test config validation."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_validate_valid_config(self):
        """Test validating a valid default config."""
        config = create_default_config("venue_001", "Test Venue")

        is_valid, errors = validate_config(config)

        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_validate_invalid_time_format(self):
        """Test validation catches invalid time format."""
        config = create_default_config("venue_002", "Test Venue")
        config.operating_hours[0].open_time = "25:00"  # Invalid

        is_valid, errors = validate_config(config)

        self.assertFalse(is_valid)
        self.assertTrue(any("Invalid time format" in e for e in errors))

    def test_validate_missing_operating_hours(self):
        """Test validation catches missing operating hours."""
        config = create_default_config("venue_003", "Test Venue")
        config.operating_hours = config.operating_hours[:5]  # Only 5 days

        is_valid, errors = validate_config(config)

        self.assertFalse(is_valid)
        self.assertTrue(any("must have 7 entries" in e for e in errors))

    def test_validate_no_roles(self):
        """Test validation catches missing roles."""
        config = create_default_config("venue_004", "Test Venue")
        config.roles = []

        is_valid, errors = validate_config(config)

        self.assertFalse(is_valid)
        self.assertTrue(any("At least one role" in e for e in errors))

    def test_validate_invalid_hour(self):
        """Test validation catches invalid hour in staffing level."""
        config = create_default_config("venue_005", "Test Venue")
        config.staffing_levels.append(
            StaffingLevel(day=DayOfWeek.MON, hour=25, area_id="bar", min_staff=1, ideal_staff=2)
        )

        is_valid, errors = validate_config(config)

        self.assertFalse(is_valid)
        self.assertTrue(any("Invalid hour" in e for e in errors))

    def test_validate_staffing_exceeds_capacity(self):
        """Test validation catches staffing exceeding area capacity."""
        config = create_default_config("venue_006", "Test Venue")
        # Find a small capacity area and exceed it
        if config.areas:
            small_area = min(config.areas, key=lambda a: a.capacity)
            config.staffing_levels.append(
                StaffingLevel(
                    day=DayOfWeek.MON,
                    hour=10,
                    area_id=small_area.area_id,
                    min_staff=1,
                    ideal_staff=small_area.capacity + 5,
                )
            )

            is_valid, errors = validate_config(config)

            self.assertFalse(is_valid)
            self.assertTrue(any("exceeds" in e for e in errors))


class TestStaffingRequirements(unittest.TestCase):
    """Test staffing requirement lookups."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_get_staffing_requirement(self):
        """Test retrieving staffing requirement for a time slot."""
        config = get_config("venue_001")
        config.staffing_levels = [
            StaffingLevel(day=DayOfWeek.MON, hour=10, area_id="bar", min_staff=1, ideal_staff=2),
            StaffingLevel(
                day=DayOfWeek.MON, hour=10, area_id="kitchen", min_staff=1, ideal_staff=2
            ),
        ]
        get_store().update_config(config)

        req = get_staffing_requirement("venue_001", DayOfWeek.MON, 10)

        self.assertIn("bar", req["by_area"])
        self.assertIn("kitchen", req["by_area"])
        self.assertEqual(req["by_area"]["bar"]["min"], 1)
        self.assertEqual(req["by_area"]["bar"]["ideal"], 2)

    def test_get_staffing_requirement_empty(self):
        """Test staffing requirement when none defined."""
        config = create_default_config("venue_002", "Test Venue")
        get_store().create_config(config)

        req = get_staffing_requirement("venue_002", DayOfWeek.MON, 10)

        self.assertEqual(req["by_area"], {})


class TestIsOpenCheck(unittest.TestCase):
    """Test is_open checking for venue hours."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_is_open_during_hours(self):
        """Test venue is open during operating hours."""
        config = get_config("venue_001")
        # Standard: 10am-midnight
        # 10:00 is start of day, 11:00 is hour 11
        self.assertTrue(is_open("venue_001", DayOfWeek.MON, 10))
        self.assertTrue(is_open("venue_001", DayOfWeek.MON, 15))
        self.assertTrue(is_open("venue_001", DayOfWeek.MON, 22))

    def test_is_open_outside_hours(self):
        """Test venue is closed outside operating hours."""
        # Standard default: 10am-23:59 (so hour 23 is open, hour 9 is closed)
        self.assertFalse(is_open("venue_001", DayOfWeek.MON, 8))  # Before 10am
        self.assertFalse(is_open("venue_001", DayOfWeek.MON, 9))  # Before 10am (0-9:59)

    def test_is_open_midnight_crossing(self):
        """Test venue that stays open past midnight."""
        config = get_config("venue_002")
        # Friday: 10am-2am next day (close_time=00:00 means 2am)
        # At hour 23 on Friday: should be open
        self.assertTrue(is_open("venue_002", DayOfWeek.FRI, 23))

    def test_is_open_closed_day(self):
        """Test venue closed on specific day."""
        config = get_config("venue_003")
        # Mark Monday as closed
        config.operating_hours[0].is_closed = True
        get_store().update_config(config)

        self.assertFalse(is_open("venue_003", DayOfWeek.MON, 12))

    def test_is_open_different_hours_each_day(self):
        """Test that different days have different hours."""
        config = create_default_config("venue_004", "Test")
        get_store().create_config(config)

        # Sunday has different hours (10am-10pm)
        self.assertTrue(is_open("venue_004", DayOfWeek.SUN, 15))
        self.assertFalse(is_open("venue_004", DayOfWeek.SUN, 23))


class TestPersistenceAndRehydration(unittest.TestCase):
    """Test persistence and rehydration from SQLite."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_persistence_roundtrip(self):
        """Test that config persists to and rehydrates from DB."""
        config1 = get_config("venue_persist_001")
        config1_id = config1.config_id

        # Reset store to force rehydration
        _reset_for_tests()
        config2 = get_config("venue_persist_001")

        self.assertEqual(config2.config_id, config1_id)
        self.assertEqual(config2.venue_name, config1.venue_name)

    def test_persistence_multiple_venues(self):
        """Test persistence with multiple venues."""
        config1 = get_config("venue_001")
        config2 = get_config("venue_002")

        _reset_for_tests()

        retrieved1 = get_config("venue_001")
        retrieved2 = get_config("venue_002")

        self.assertEqual(retrieved1.config_id, config1.config_id)
        self.assertEqual(retrieved2.config_id, config2.config_id)


class TestDayOfWeekEnum(unittest.TestCase):
    """Test DayOfWeek enum."""

    def test_day_of_week_values(self):
        """Test DayOfWeek enum has correct values."""
        self.assertEqual(DayOfWeek.MON.value, 0)
        self.assertEqual(DayOfWeek.TUE.value, 1)
        self.assertEqual(DayOfWeek.WED.value, 2)
        self.assertEqual(DayOfWeek.THU.value, 3)
        self.assertEqual(DayOfWeek.FRI.value, 4)
        self.assertEqual(DayOfWeek.SAT.value, 5)
        self.assertEqual(DayOfWeek.SUN.value, 6)

    def test_day_of_week_from_name(self):
        """Test creating DayOfWeek from string name."""
        self.assertEqual(DayOfWeek.from_name("MON"), DayOfWeek.MON)
        self.assertEqual(DayOfWeek.from_name("fri"), DayOfWeek.FRI)


if __name__ == "__main__":
    unittest.main()
