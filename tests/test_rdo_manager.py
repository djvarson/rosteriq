"""Test suite for rdo_manager.py module.

Tests the RDOManagerStore and related functions with 40+ test cases covering:
- Policy CRUD operations
- Employee enrolment
- Accrual calculations
- Balance tracking and updates
- RDO scheduling with balance checks
- Taking, cancelling, and swapping RDOs
- Eligibility checks
- Team calendar generation
- Accrual forecasting
- Persistence and rehydration
"""

import sys
import os
import unittest
from datetime import date, datetime, timedelta, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.rdo_manager import (
    get_rdo_manager_store,
    RDOManagerStore,
    RDOPolicy,
    RDOBalance,
    RDOSchedule,
    RDOStatus,
    _reset_for_tests,
)

try:
    from rosteriq import persistence as _p
    has_persistence = True
except ImportError:
    has_persistence = False


class TestPolicyCRUD(unittest.TestCase):
    """Test RDO policy CRUD operations."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_create_policy(self):
        """Create a basic RDO policy."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard 28-day",
            cycle_days=28,
            accrual_hours_per_day=0.4,
            rdo_length_hours=7.6,
        )
        self.assertIsNotNone(policy.id)
        self.assertEqual(policy.venue_id, "venue_123")
        self.assertEqual(policy.cycle_days, 28)
        self.assertEqual(policy.accrual_hours_per_day, 0.4)

    def test_create_policy_with_employment_types(self):
        """Create policy with custom employment types."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Custom",
            cycle_days=14,
            accrual_hours_per_day=0.5,
            eligible_employment_types=["FULL_TIME", "PART_TIME"],
        )
        self.assertEqual(policy.eligible_employment_types, ["FULL_TIME", "PART_TIME"])

    def test_get_policy(self):
        """Retrieve a policy by ID."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Test",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        retrieved = store.get_policy(policy.id)
        self.assertEqual(retrieved.id, policy.id)
        self.assertEqual(retrieved.name, "Test")

    def test_get_nonexistent_policy(self):
        """Get nonexistent policy returns None."""
        store = get_rdo_manager_store()
        result = store.get_policy("fake_id")
        self.assertIsNone(result)

    def test_list_policies_for_venue(self):
        """List all policies for a venue."""
        store = get_rdo_manager_store()
        store.create_policy(
            venue_id="venue_123",
            name="Policy 1",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.create_policy(
            venue_id="venue_123",
            name="Policy 2",
            cycle_days=14,
            accrual_hours_per_day=0.5,
        )
        store.create_policy(
            venue_id="venue_456",
            name="Policy 3",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        policies_123 = store.list_policies("venue_123")
        policies_456 = store.list_policies("venue_456")

        self.assertEqual(len(policies_123), 2)
        self.assertEqual(len(policies_456), 1)

    def test_update_policy(self):
        """Update a policy."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Original",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        updated = store.update_policy(
            policy.id,
            name="Updated",
            accrual_hours_per_day=0.5,
        )
        self.assertEqual(updated.name, "Updated")
        self.assertEqual(updated.accrual_hours_per_day, 0.5)

    def test_update_nonexistent_policy_raises(self):
        """Updating nonexistent policy raises ValueError."""
        store = get_rdo_manager_store()
        with self.assertRaises(ValueError):
            store.update_policy("fake_id", name="Test")


class TestEnrolment(unittest.TestCase):
    """Test employee enrolment in RDO policies."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_enrol_employee(self):
        """Enrol an employee in a policy."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        start_date = date(2026, 1, 1)
        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=start_date,
            employment_type="FULL_TIME",
        )

        self.assertEqual(balance.employee_id, "emp_001")
        self.assertEqual(balance.policy_id, policy.id)
        self.assertEqual(balance.accrued_hours, 0.0)
        self.assertEqual(balance.taken_hours, 0.0)

    def test_get_balance(self):
        """Retrieve employee balance."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        retrieved = store.get_balance("venue_123", "emp_001")
        self.assertEqual(retrieved.id, balance.id)

    def test_get_nonexistent_balance(self):
        """Get nonexistent balance returns None."""
        store = get_rdo_manager_store()
        result = store.get_balance("venue_123", "emp_unknown")
        self.assertIsNone(result)


class TestAccrual(unittest.TestCase):
    """Test RDO hour accrual."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_accrue_hours_single(self):
        """Accrue hours for a single employee."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        # Work 19 days (typical 4-week month)
        balance = store.accrue_hours(
            venue_id="venue_123",
            employee_id="emp_001",
            hours_worked=19,
            work_date=date(2026, 1, 31),
        )

        # 19 * 0.4 = 7.6 hours (one RDO)
        self.assertAlmostEqual(balance.accrued_hours, 7.6, places=1)
        self.assertEqual(balance.taken_hours, 0.0)

    def test_accrue_hours_multiple_calls(self):
        """Accrue hours across multiple calls."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        # First accrual
        store.accrue_hours("venue_123", "emp_001", 10, date(2026, 1, 15))
        balance1 = store.get_balance("venue_123", "emp_001")
        self.assertAlmostEqual(balance1.accrued_hours, 4.0, places=1)

        # Second accrual
        store.accrue_hours("venue_123", "emp_001", 9, date(2026, 1, 31))
        balance2 = store.get_balance("venue_123", "emp_001")
        self.assertAlmostEqual(balance2.accrued_hours, 7.6, places=1)

    def test_bulk_accrue(self):
        """Bulk accrue for multiple employees."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        # Enrol two employees
        store.enrol_employee("venue_123", "emp_001", policy.id, date(2026, 1, 1))
        store.enrol_employee("venue_123", "emp_002", policy.id, date(2026, 1, 1))

        # Bulk accrue
        results = store.bulk_accrue(
            venue_id="venue_123",
            work_date=date(2026, 1, 31),
            employee_hours={"emp_001": 19, "emp_002": 15},
        )

        self.assertEqual(len(results), 2)
        self.assertAlmostEqual(results[0].accrued_hours, 7.6, places=1)
        self.assertAlmostEqual(results[1].accrued_hours, 6.0, places=1)

    def test_accrue_to_nonexistent_employee(self):
        """Accruing to nonexistent employee returns None."""
        store = get_rdo_manager_store()
        result = store.accrue_hours("venue_123", "emp_unknown", 10, date(2026, 1, 31))
        self.assertIsNone(result)


class TestBalanceTracking(unittest.TestCase):
    """Test RDO balance calculations."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_balance_hours_computed(self):
        """Balance hours is accrued - taken."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        # Manually update for testing
        balance.accrued_hours = 15.2
        balance.taken_hours = 7.6

        self.assertAlmostEqual(balance.balance_hours, 7.6, places=1)

    def test_balance_zero_initially(self):
        """New employee has zero balance."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        self.assertEqual(balance.balance_hours, 0.0)


class TestScheduling(unittest.TestCase):
    """Test RDO scheduling."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_schedule_rdo_with_sufficient_balance(self):
        """Schedule RDO when balance is sufficient."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        # Accrue hours
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))

        # Schedule RDO
        schedule = store.schedule_rdo(
            venue_id="venue_123",
            employee_id="emp_001",
            date_=date(2026, 2, 5),
        )

        self.assertEqual(schedule.status, RDOStatus.SCHEDULED)
        self.assertEqual(schedule.hours, 7.6)
        self.assertEqual(schedule.employee_id, "emp_001")

    def test_schedule_rdo_insufficient_balance_raises(self):
        """Schedule RDO with insufficient balance raises ValueError."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        # Try to schedule with zero balance
        with self.assertRaises(ValueError) as ctx:
            store.schedule_rdo(
                venue_id="venue_123",
                employee_id="emp_001",
                date_=date(2026, 2, 5),
            )
        self.assertIn("Insufficient balance", str(ctx.exception))

    def test_schedule_rdo_no_balance_record(self):
        """Schedule for employee with no balance raises."""
        store = get_rdo_manager_store()
        with self.assertRaises(ValueError) as ctx:
            store.schedule_rdo(
                venue_id="venue_123",
                employee_id="emp_unknown",
                date_=date(2026, 2, 5),
            )
        self.assertIn("No balance found", str(ctx.exception))


class TestScheduleStatus(unittest.TestCase):
    """Test RDO schedule status transitions."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def _setup_scheduled_rdo(self):
        """Helper: set up a scheduled RDO."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))
        schedule = store.schedule_rdo(
            venue_id="venue_123",
            employee_id="emp_001",
            date_=date(2026, 2, 5),
        )
        return store, schedule

    def test_take_rdo(self):
        """Take a scheduled RDO and deduct balance."""
        store, schedule = self._setup_scheduled_rdo()

        # Before taking
        balance_before = store.get_balance("venue_123", "emp_001")
        self.assertAlmostEqual(balance_before.taken_hours, 0.0, places=1)

        # Take it
        updated = store.take_rdo(schedule.id)
        self.assertEqual(updated.status, RDOStatus.TAKEN)

        # Check balance updated
        balance_after = store.get_balance("venue_123", "emp_001")
        self.assertAlmostEqual(balance_after.taken_hours, 7.6, places=1)

    def test_cancel_rdo(self):
        """Cancel a scheduled RDO."""
        store, schedule = self._setup_scheduled_rdo()

        updated = store.cancel_rdo(schedule.id)
        self.assertEqual(updated.status, RDOStatus.CANCELLED)

        # Balance should not be affected
        balance = store.get_balance("venue_123", "emp_001")
        self.assertEqual(balance.taken_hours, 0.0)

    def test_swap_rdo(self):
        """Swap RDO to new date."""
        store, schedule = self._setup_scheduled_rdo()

        new_date = date(2026, 2, 12)
        updated = store.swap_rdo(schedule.id, new_date)

        self.assertEqual(updated.status, RDOStatus.SWAPPED)
        self.assertEqual(updated.swap_date, new_date)

    def test_take_nonexistent_schedule_raises(self):
        """Take nonexistent schedule raises."""
        store = get_rdo_manager_store()
        with self.assertRaises(ValueError):
            store.take_rdo("fake_id")


class TestQueries(unittest.TestCase):
    """Test RDO query methods."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def _setup_test_data(self):
        """Helper: set up test data with multiple schedules."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )

        # Enrol and accrue
        for i in range(1, 4):
            store.enrol_employee(
                venue_id="venue_123",
                employee_id=f"emp_00{i}",
                policy_id=policy.id,
                employment_start_date=date(2026, 1, 1),
            )
            store.accrue_hours(
                venue_id="venue_123",
                employee_id=f"emp_00{i}",
                hours_worked=19,
                work_date=date(2026, 1, 31),
            )

        # Schedule multiple RDOs
        today = date.today()
        base = today + timedelta(days=1)

        store.schedule_rdo("venue_123", "emp_001", base)
        store.schedule_rdo("venue_123", "emp_001", base + timedelta(days=7))
        store.schedule_rdo("venue_123", "emp_002", base + timedelta(days=3))
        store.schedule_rdo("venue_123", "emp_003", base + timedelta(days=30))

        return store, base

    def test_get_schedule_by_venue(self):
        """Get all schedules for a venue."""
        store, _ = self._setup_test_data()
        schedules = store.get_schedule("venue_123")
        self.assertEqual(len(schedules), 4)

    def test_get_schedule_by_employee(self):
        """Get schedules for specific employee."""
        store, _ = self._setup_test_data()
        schedules = store.get_schedule("venue_123", employee_id="emp_001")
        self.assertEqual(len(schedules), 2)

    def test_get_schedule_by_date_range(self):
        """Get schedules within date range."""
        store, base = self._setup_test_data()
        schedules = store.get_schedule(
            "venue_123",
            date_from=base,
            date_to=base + timedelta(days=10),
        )
        self.assertGreater(len(schedules), 0)

    def test_get_schedule_by_status(self):
        """Get schedules by status."""
        store, _ = self._setup_test_data()
        # Mark one as taken
        schedules = store.get_schedule("venue_123")
        if schedules:
            store.take_rdo(schedules[0].id)

        taken = store.get_schedule("venue_123", status=RDOStatus.TAKEN)
        self.assertEqual(len(taken), 1)

    def test_get_upcoming_rdos(self):
        """Get upcoming RDOs."""
        store, _ = self._setup_test_data()
        upcoming = store.get_upcoming_rdos("venue_123", days_ahead=28)
        self.assertGreater(len(upcoming), 0)

    def test_get_team_rdo_calendar(self):
        """Get team calendar for month."""
        store, _ = self._setup_test_data()
        today = date.today()
        calendar = store.get_team_rdo_calendar(
            "venue_123",
            today.month,
            today.year,
        )
        self.assertEqual(calendar["month"], today.month)
        self.assertEqual(calendar["year"], today.year)


class TestEligibility(unittest.TestCase):
    """Test eligibility checks."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_eligible_with_balance(self):
        """Employee with sufficient balance is eligible."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))

        result = store.check_eligibility("venue_123", "emp_001")
        self.assertTrue(result["eligible"])
        self.assertAlmostEqual(result["balance_hours"], 7.6, places=1)

    def test_not_eligible_insufficient_balance(self):
        """Employee with insufficient balance is not eligible."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )

        result = store.check_eligibility("venue_123", "emp_001")
        self.assertFalse(result["eligible"])
        self.assertIn("Insufficient balance", result["reason"])

    def test_not_eligible_no_enrollment(self):
        """Employee not enrolled is not eligible."""
        store = get_rdo_manager_store()
        result = store.check_eligibility("venue_123", "emp_unknown")
        self.assertFalse(result["eligible"])

    def test_eligibility_employment_type_check(self):
        """Eligibility checks employment type."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="FT Only",
            cycle_days=28,
            accrual_hours_per_day=0.4,
            eligible_employment_types=["FULL_TIME"],
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
            employment_type="PART_TIME",
        )
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))

        result = store.check_eligibility("venue_123", "emp_001")
        self.assertFalse(result["eligible"])
        self.assertIn("not eligible", result["reason"])


class TestForecast(unittest.TestCase):
    """Test accrual forecasting."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_forecast_basic(self):
        """Get accrual forecast."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))

        forecast = store.get_accrual_forecast(
            "venue_123",
            "emp_001",
            days_ahead=28,
        )

        self.assertAlmostEqual(forecast["current_balance"], 7.6, places=1)
        self.assertGreater(forecast["expected_accrual"], 0)

    def test_forecast_nonexistent_employee(self):
        """Forecast for nonexistent employee returns empty."""
        store = get_rdo_manager_store()
        forecast = store.get_accrual_forecast(
            "venue_123",
            "emp_unknown",
            days_ahead=28,
        )
        self.assertEqual(forecast, {})


class TestSingletonAndReset(unittest.TestCase):
    """Test singleton behavior and reset."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_singleton_same_instance(self):
        """get_rdo_manager_store always returns same instance."""
        store1 = get_rdo_manager_store()
        store2 = get_rdo_manager_store()
        self.assertIs(store1, store2)

    def test_reset_for_tests_clears_data(self):
        """_reset_for_tests clears all data."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Test",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        self.assertEqual(len(store.list_policies("venue_123")), 1)

        _reset_for_tests()
        store_after = get_rdo_manager_store()
        self.assertEqual(len(store_after.list_policies("venue_123")), 0)


class TestPersistenceIntegration(unittest.TestCase):
    """Test persistence operations (if available)."""

    def setUp(self):
        _reset_for_tests()
        if has_persistence:
            _p.reset_for_tests()

    def test_policy_to_dict(self):
        """Policy can be converted to dict."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Test",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        d = policy.to_dict()
        self.assertEqual(d["name"], "Test")
        self.assertEqual(d["cycle_days"], 28)

    def test_balance_to_dict(self):
        """Balance can be converted to dict."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        balance = store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )
        d = balance.to_dict()
        self.assertEqual(d["employee_id"], "emp_001")
        self.assertEqual(d["balance_hours"], 0.0)

    def test_schedule_to_dict(self):
        """Schedule can be converted to dict."""
        store = get_rdo_manager_store()
        policy = store.create_policy(
            venue_id="venue_123",
            name="Standard",
            cycle_days=28,
            accrual_hours_per_day=0.4,
        )
        store.enrol_employee(
            venue_id="venue_123",
            employee_id="emp_001",
            policy_id=policy.id,
            employment_start_date=date(2026, 1, 1),
        )
        store.accrue_hours("venue_123", "emp_001", 19, date(2026, 1, 31))
        schedule = store.schedule_rdo(
            venue_id="venue_123",
            employee_id="emp_001",
            date_=date(2026, 2, 5),
        )
        d = schedule.to_dict()
        self.assertEqual(d["employee_id"], "emp_001")
        self.assertEqual(d["status"], "scheduled")


if __name__ == "__main__":
    unittest.main()
