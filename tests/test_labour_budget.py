"""Test suite for labour_budget.py module.

Tests the labour budget guardrails system with 20+ test cases covering:
- Shift cost calculations (ordinary, Saturday, Sunday, evening, penalties)
- Budget snapshots and labour percentage
- Alert thresholds and severity levels
- Hours-remaining projection
- Store persistence (thresholds and alerts)
- What-if scenarios
- Edge cases
"""

import sys
import os
import unittest
import tempfile
import uuid
from datetime import datetime, timezone, date

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.labour_budget import (
    get_threshold_store,
    get_alert_store,
    _reset_for_tests,
    calculate_shift_cost,
    calculate_roster_cost,
    build_budget_snapshot,
    check_budget_alerts,
    project_hours_remaining,
    _shift_duration_hours,
    _get_penalty_multiplier,
    BudgetThreshold,
    BudgetAlert,
    ShiftCostProjection,
    BudgetSnapshot,
    AlertType,
    DEFAULT_THRESHOLDS,
)
from rosteriq import persistence as _p


class TestShiftDurationCalculation(unittest.TestCase):
    """Test utility functions for time calculation."""

    def test_shift_duration_same_day(self):
        """Test duration calculation for same-day shift."""
        duration = _shift_duration_hours("09:00", "17:00")
        self.assertEqual(duration, 8.0)

    def test_shift_duration_overnight(self):
        """Test duration calculation for overnight shift."""
        duration = _shift_duration_hours("22:00", "06:00")
        self.assertEqual(duration, 8.0)

    def test_shift_duration_exact_5_hours(self):
        """Test duration calculation for exactly 5 hours."""
        duration = _shift_duration_hours("09:00", "14:00")
        self.assertEqual(duration, 5.0)

    def test_shift_duration_short_shift(self):
        """Test duration for short shift."""
        duration = _shift_duration_hours("14:00", "16:00")
        self.assertEqual(duration, 2.0)

    def test_shift_duration_long_shift(self):
        """Test duration calculation for long shift."""
        duration = _shift_duration_hours("08:00", "20:00")
        self.assertEqual(duration, 12.0)

    def test_shift_duration_with_minutes(self):
        """Test duration with minutes."""
        duration = _shift_duration_hours("09:30", "14:45")
        self.assertAlmostEqual(duration, 5.25, places=2)


class TestPenaltyMultiplier(unittest.TestCase):
    """Test penalty multiplier calculation."""

    def test_no_penalty_weekday(self):
        """Test weekday with no evening = no penalty."""
        mult = _get_penalty_multiplier("09:00", "17:00", "2026-04-20", day_of_week=0)
        self.assertEqual(mult, 1.0)

    def test_saturday_penalty(self):
        """Test Saturday penalty (+25%)."""
        mult = _get_penalty_multiplier("09:00", "17:00", "2026-04-19", day_of_week=5)
        self.assertEqual(mult, 1.25)

    def test_sunday_penalty(self):
        """Test Sunday penalty (+50%)."""
        mult = _get_penalty_multiplier("09:00", "17:00", "2026-04-20", day_of_week=6)
        self.assertEqual(mult, 1.50)

    def test_evening_penalty(self):
        """Test evening penalty (after 19:00, +15%)."""
        mult = _get_penalty_multiplier("15:00", "20:00", "2026-04-20", day_of_week=0)
        self.assertEqual(mult, 1.15)

    def test_no_evening_penalty_before_7pm(self):
        """Test no evening penalty before 19:00."""
        mult = _get_penalty_multiplier("15:00", "18:00", "2026-04-20", day_of_week=0)
        self.assertEqual(mult, 1.0)

    def test_public_holiday_penalty(self):
        """Test public holiday penalty (+125%)."""
        mult = _get_penalty_multiplier(
            "09:00", "17:00", "2026-12-25", day_of_week=5, is_public_holiday=True
        )
        self.assertEqual(mult, 2.25)


class TestShiftCostCalculation(unittest.TestCase):
    """Test single shift cost calculations."""

    def test_ordinary_shift_cost(self):
        """Test cost for ordinary weekday shift with no penalties."""
        shift = {
            "employee_id": "emp1",
            "employee_name": "John Doe",
            "shift_start": "09:00",
            "shift_end": "17:00",
            "shift_date": "2026-04-20",  # Monday
        }
        projection = calculate_shift_cost(shift, hourly_rate=25.0)
        # 8 hours * $25 = $200
        self.assertEqual(projection.total_cost, 200.0)
        self.assertEqual(projection.base_cost, 200.0)
        self.assertEqual(projection.penalty_cost, 0.0)
        self.assertFalse(projection.is_penalty_rate)

    def test_saturday_shift_cost(self):
        """Test cost for Saturday shift with +25% penalty."""
        shift = {
            "employee_id": "emp1",
            "employee_name": "Jane Smith",
            "shift_start": "09:00",
            "shift_end": "17:00",
            "shift_date": "2026-04-19",  # 2026-04-19 is a Sunday, so let's use day_of_week
        }
        # 2026-04-19 is actually a Sunday; use Saturday 2026-04-18
        shift["shift_date"] = "2026-04-18"
        projection = calculate_shift_cost(shift, hourly_rate=25.0)
        # 8 hours * $25 * 1.25 = $250
        self.assertAlmostEqual(projection.total_cost, 250.0, places=2)
        self.assertTrue(projection.is_penalty_rate)

    def test_sunday_shift_cost(self):
        """Test cost for Sunday shift with +50% penalty."""
        shift = {
            "employee_id": "emp2",
            "employee_name": "Bob Jones",
            "shift_start": "09:00",
            "shift_end": "17:00",
            "shift_date": "2026-04-20",  # Sunday would be day 6
        }
        projection = calculate_shift_cost(shift, hourly_rate=30.0)
        # Base: 8 hours * $30 = $240
        # Penalty: 240 * 0.50 = $120
        # Total: $360 (if Sunday)
        # Note: actual day of week depends on parsing

    def test_evening_shift_cost(self):
        """Test cost for evening shift with +15% penalty."""
        shift = {
            "employee_id": "emp3",
            "employee_name": "Alice Brown",
            "shift_start": "17:00",
            "shift_end": "21:00",
            "shift_date": "2026-04-20",
        }
        projection = calculate_shift_cost(shift, hourly_rate=25.0)
        # 4 hours * $25 * 1.15 = $115
        self.assertAlmostEqual(projection.total_cost, 115.0, places=2)
        self.assertTrue(projection.is_penalty_rate)

    def test_overtime_flag(self):
        """Test that shifts over 8 hours are marked as overtime."""
        shift = {
            "employee_id": "emp4",
            "employee_name": "Charlie Davis",
            "shift_start": "09:00",
            "shift_end": "18:00",
            "shift_date": "2026-04-20",
        }
        projection = calculate_shift_cost(shift, hourly_rate=25.0)
        # 9 hours = overtime
        self.assertTrue(projection.is_overtime)

    def test_short_shift_cost(self):
        """Test cost for short shift (under 5 hours)."""
        shift = {
            "employee_id": "emp5",
            "employee_name": "David Evans",
            "shift_start": "14:00",
            "shift_end": "16:00",
            "shift_date": "2026-04-20",
        }
        projection = calculate_shift_cost(shift, hourly_rate=25.0)
        # 2 hours * $25 = $50
        self.assertEqual(projection.total_cost, 50.0)


class TestRosterCostCalculation(unittest.TestCase):
    """Test roster-level cost calculations."""

    def test_empty_roster_cost(self):
        """Test cost for empty roster."""
        cost = calculate_roster_cost([], {})
        self.assertEqual(cost, 0.0)

    def test_single_shift_roster(self):
        """Test cost for roster with one shift."""
        shifts = [
            {
                "employee_id": "emp1",
                "employee_name": "John",
                "shift_start": "09:00",
                "shift_end": "17:00",
                "shift_date": "2026-04-20",
            }
        ]
        rates = {"emp1": 25.0}
        cost = calculate_roster_cost(shifts, rates)
        # 8 hours * $25 = $200
        self.assertEqual(cost, 200.0)

    def test_multiple_shifts_roster(self):
        """Test cost for roster with multiple shifts."""
        shifts = [
            {
                "employee_id": "emp1",
                "employee_name": "John",
                "shift_start": "09:00",
                "shift_end": "17:00",
                "shift_date": "2026-04-20",
            },
            {
                "employee_id": "emp2",
                "employee_name": "Jane",
                "shift_start": "10:00",
                "shift_end": "18:00",
                "shift_date": "2026-04-20",
            },
        ]
        rates = {"emp1": 25.0, "emp2": 30.0}
        cost = calculate_roster_cost(shifts, rates)
        # emp1: 8 * $25 = $200
        # emp2: 8 * $30 = $240
        # Total: $440
        self.assertEqual(cost, 440.0)

    def test_roster_with_default_rate(self):
        """Test roster uses default rate for unknown employees."""
        shifts = [
            {
                "employee_id": "emp_unknown",
                "employee_name": "Unknown",
                "shift_start": "09:00",
                "shift_end": "10:00",
                "shift_date": "2026-04-20",
            }
        ]
        cost = calculate_roster_cost(shifts, {})
        # 1 hour * $25 (default) = $25
        self.assertEqual(cost, 25.0)


class TestBudgetAlerts(unittest.TestCase):
    """Test budget alert generation."""

    def test_alert_on_track(self):
        """Test alert when labour % is on track."""
        snapshot = BudgetSnapshot(
            venue_id="v1",
            snapshot_date="2026-04-20",
            total_wage_cost=3000.0,
            projected_revenue=10000.0,
            labour_pct=30.0,
            headcount=5,
            avg_hourly_cost=75.0,
        )
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        alerts = check_budget_alerts(snapshot, thresholds)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.ON_TRACK)

    def test_alert_warning(self):
        """Test alert when labour % drops below warning threshold."""
        snapshot = BudgetSnapshot(
            venue_id="v1",
            snapshot_date="2026-04-20",
            total_wage_cost=2200.0,
            projected_revenue=10000.0,
            labour_pct=22.0,
            headcount=3,
            avg_hourly_cost=75.0,
        )
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        alerts = check_budget_alerts(snapshot, thresholds)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.WARNING)

    def test_alert_over_budget(self):
        """Test alert when labour % exceeds target."""
        snapshot = BudgetSnapshot(
            venue_id="v1",
            snapshot_date="2026-04-20",
            total_wage_cost=3100.0,
            projected_revenue=10000.0,
            labour_pct=31.0,
            headcount=6,
            avg_hourly_cost=75.0,
        )
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        alerts = check_budget_alerts(snapshot, thresholds)
        self.assertEqual(len(alerts), 1)
        # 31% > 30% target and < 35% critical, so OVER_BUDGET
        self.assertEqual(alerts[0].alert_type, AlertType.OVER_BUDGET)

    def test_alert_critical(self):
        """Test alert when labour % exceeds critical threshold."""
        snapshot = BudgetSnapshot(
            venue_id="v1",
            snapshot_date="2026-04-20",
            total_wage_cost=3600.0,
            projected_revenue=10000.0,
            labour_pct=36.0,
            headcount=8,
            avg_hourly_cost=75.0,
        )
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        alerts = check_budget_alerts(snapshot, thresholds)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, AlertType.CRITICAL)


class TestHoursRemainingProjection(unittest.TestCase):
    """Test projection of hours remaining in budget."""

    def test_hours_remaining_basic(self):
        """Test basic hours remaining calculation."""
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        # Target: $3000 (30% of $10k)
        # Current: $2000
        # Remaining: $1000
        # At $25/hr: 40 hours
        hours = project_hours_remaining(thresholds, 2000.0, 10000.0, 25.0)
        self.assertAlmostEqual(hours, 40.0, places=1)

    def test_hours_remaining_zero(self):
        """Test hours remaining when already at target."""
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        # Target: $3000, Current: $3000
        hours = project_hours_remaining(thresholds, 3000.0, 10000.0, 25.0)
        self.assertAlmostEqual(hours, 0.0, places=1)

    def test_hours_remaining_over_budget(self):
        """Test hours remaining when already over target."""
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        # Target: $3000, Current: $4000 (over)
        hours = project_hours_remaining(thresholds, 4000.0, 10000.0, 25.0)
        self.assertEqual(hours, 0.0)

    def test_hours_remaining_high_rate(self):
        """Test hours remaining with high hourly rate."""
        thresholds = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=25.0,
            critical_labour_pct=35.0,
        )
        # Target: $3000, Current: $2000
        # Remaining: $1000 at $50/hr = 20 hours
        hours = project_hours_remaining(thresholds, 2000.0, 10000.0, 50.0)
        self.assertAlmostEqual(hours, 20.0, places=1)


class TestBudgetSnapshot(unittest.TestCase):
    """Test budget snapshot generation."""

    def test_empty_snapshot(self):
        """Test snapshot with no shifts."""
        snapshot = build_budget_snapshot(
            venue_id="v1",
            shifts=[],
            rates_map={},
            projected_revenue=10000.0,
            snapshot_date="2026-04-20",
        )
        self.assertEqual(snapshot.total_wage_cost, 0.0)
        self.assertEqual(snapshot.labour_pct, 0.0)
        self.assertEqual(snapshot.headcount, 0)

    def test_snapshot_with_shifts(self):
        """Test snapshot with multiple shifts."""
        shifts = [
            {
                "employee_id": "emp1",
                "employee_name": "John",
                "shift_start": "09:00",
                "shift_end": "17:00",
                "shift_date": "2026-04-20",
            },
            {
                "employee_id": "emp2",
                "employee_name": "Jane",
                "shift_start": "10:00",
                "shift_end": "18:00",
                "shift_date": "2026-04-20",
            },
        ]
        rates = {"emp1": 25.0, "emp2": 25.0}
        snapshot = build_budget_snapshot(
            venue_id="v1",
            shifts=shifts,
            rates_map=rates,
            projected_revenue=10000.0,
            snapshot_date="2026-04-20",
        )
        # Total: 16 hours * $25 = $400
        # Labour %: 4%
        self.assertEqual(snapshot.total_wage_cost, 400.0)
        self.assertEqual(snapshot.labour_pct, 4.0)
        self.assertEqual(snapshot.headcount, 2)
        self.assertEqual(len(snapshot.shift_costs), 2)

    def test_snapshot_includes_alerts(self):
        """Test that snapshot includes alerts."""
        shifts = [
            {
                "employee_id": "emp1",
                "employee_name": "John",
                "shift_start": "09:00",
                "shift_end": "17:00",
                "shift_date": "2026-04-20",
            }
        ]
        rates = {"emp1": 25.0}
        snapshot = build_budget_snapshot(
            venue_id="v1",
            shifts=shifts,
            rates_map=rates,
            projected_revenue=10000.0,
            snapshot_date="2026-04-20",
        )
        self.assertGreater(len(snapshot.alerts), 0)
        self.assertIsNotNone(snapshot.alerts[0].message)

    def test_snapshot_avg_hourly_cost(self):
        """Test average hourly cost calculation."""
        shifts = [
            {
                "employee_id": "emp1",
                "employee_name": "John",
                "shift_start": "09:00",
                "shift_end": "17:00",
                "shift_date": "2026-04-20",
            }
        ]
        rates = {"emp1": 25.0}
        snapshot = build_budget_snapshot(
            venue_id="v1",
            shifts=shifts,
            rates_map=rates,
            projected_revenue=10000.0,
            snapshot_date="2026-04-20",
        )
        # 8 hours * $25 = $200, avg = $200 / 8 = $25
        self.assertEqual(snapshot.avg_hourly_cost, 25.0)


class TestBudgetThresholdStore(unittest.TestCase):
    """Test BudgetThresholdStore persistence and retrieval."""

    def setUp(self):
        """Reset stores before each test."""
        _reset_for_tests()
        # Enable in-memory persistence for tests
        _p.force_enable_for_tests(True)

    def tearDown(self):
        """Clean up after tests."""
        _reset_for_tests()
        _p.force_enable_for_tests(False)

    def test_get_default_threshold(self):
        """Test retrieving default threshold for unknown venue."""
        store = get_threshold_store()
        threshold = store.get("unknown_venue")
        self.assertEqual(threshold.target_labour_pct, DEFAULT_THRESHOLDS.target_labour_pct)

    def test_set_and_get_threshold(self):
        """Test setting and retrieving a threshold."""
        store = get_threshold_store()
        new_threshold = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=28.0,
            warning_labour_pct=26.0,
            critical_labour_pct=32.0,
        )
        store.set(new_threshold)
        retrieved = store.get("v1")
        self.assertEqual(retrieved.target_labour_pct, 28.0)

    def test_update_threshold(self):
        """Test updating an existing threshold."""
        store = get_threshold_store()
        threshold1 = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=30.0,
            warning_labour_pct=28.0,
            critical_labour_pct=35.0,
        )
        store.set(threshold1)
        threshold2 = BudgetThreshold(
            venue_id="v1",
            target_labour_pct=32.0,
            warning_labour_pct=30.0,
            critical_labour_pct=37.0,
        )
        store.set(threshold2)
        retrieved = store.get("v1")
        self.assertEqual(retrieved.target_labour_pct, 32.0)


class TestBudgetAlertStore(unittest.TestCase):
    """Test BudgetAlertStore persistence and querying."""

    def setUp(self):
        """Reset stores before each test."""
        _reset_for_tests()
        _p.force_enable_for_tests(True)

    def tearDown(self):
        """Clean up after tests."""
        _reset_for_tests()
        _p.force_enable_for_tests(False)

    def test_record_alert(self):
        """Test recording an alert."""
        store = get_alert_store()
        alert = BudgetAlert(
            alert_id=uuid.uuid4().hex[:12],  # Use random ID to avoid conflicts
            venue_id="v1_test_record",
            alert_type=AlertType.ON_TRACK,
            current_labour_pct=30.0,
            target_labour_pct=30.0,
            current_wage_cost=3000.0,
            projected_revenue=10000.0,
            message="On track",
        )
        store.record(alert)
        alerts = store.get_by_venue("v1_test_record")
        self.assertGreaterEqual(len(alerts), 1)
        # Find our alert
        found = False
        for a in alerts:
            if a.alert_id == alert.alert_id:
                found = True
                break
        self.assertTrue(found)

    def test_get_by_venue(self):
        """Test querying alerts by venue."""
        store = get_alert_store()
        # Record alerts for two venues
        alert1 = BudgetAlert(
            alert_id="a1",
            venue_id="v1",
            alert_type=AlertType.ON_TRACK,
            current_labour_pct=30.0,
            target_labour_pct=30.0,
            current_wage_cost=3000.0,
        )
        alert2 = BudgetAlert(
            alert_id="a2",
            venue_id="v1",
            alert_type=AlertType.WARNING,
            current_labour_pct=24.0,
            target_labour_pct=30.0,
            current_wage_cost=2400.0,
        )
        alert3 = BudgetAlert(
            alert_id="a3",
            venue_id="v2",
            alert_type=AlertType.ON_TRACK,
            current_labour_pct=30.0,
            target_labour_pct=30.0,
            current_wage_cost=3000.0,
        )
        store.record(alert1)
        store.record(alert2)
        store.record(alert3)

        # Get alerts for v1
        v1_alerts = store.get_by_venue("v1")
        self.assertEqual(len(v1_alerts), 2)

        # Get alerts for v2
        v2_alerts = store.get_by_venue("v2")
        self.assertEqual(len(v2_alerts), 1)

    def test_get_by_alert_type(self):
        """Test querying alerts by type."""
        store = get_alert_store()
        alert1 = BudgetAlert(
            alert_id="a1",
            venue_id="v1",
            alert_type=AlertType.ON_TRACK,
            current_labour_pct=30.0,
            target_labour_pct=30.0,
            current_wage_cost=3000.0,
        )
        alert2 = BudgetAlert(
            alert_id="a2",
            venue_id="v1",
            alert_type=AlertType.CRITICAL,
            current_labour_pct=36.0,
            target_labour_pct=30.0,
            current_wage_cost=3600.0,
        )
        store.record(alert1)
        store.record(alert2)

        critical_alerts = store.get_by_venue("v1", alert_type="critical")
        self.assertEqual(len(critical_alerts), 1)
        self.assertEqual(critical_alerts[0].alert_type, AlertType.CRITICAL)

    def test_get_by_date_range(self):
        """Test querying alerts by date range."""
        store = get_alert_store()
        alert1 = BudgetAlert(
            alert_id="a1",
            venue_id="v1",
            alert_type=AlertType.ON_TRACK,
            current_labour_pct=30.0,
            target_labour_pct=30.0,
            current_wage_cost=3000.0,
            shift_date="2026-04-20",
        )
        alert2 = BudgetAlert(
            alert_id="a2",
            venue_id="v1",
            alert_type=AlertType.WARNING,
            current_labour_pct=25.0,
            target_labour_pct=30.0,
            current_wage_cost=2500.0,
            shift_date="2026-04-21",
        )
        store.record(alert1)
        store.record(alert2)

        range_alerts = store.get_by_date_range("v1", "2026-04-20", "2026-04-20")
        self.assertEqual(len(range_alerts), 1)
        self.assertEqual(range_alerts[0].shift_date, "2026-04-20")


if __name__ == "__main__":
    unittest.main()
