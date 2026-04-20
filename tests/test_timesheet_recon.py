"""Tests for rosteriq.timesheet_recon — pure-stdlib, no pytest.

Runs with `PYTHONPATH=. python3 -m unittest tests.test_timesheet_recon -v`
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from datetime import datetime, timezone, date

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import timesheet_recon as tr
from rosteriq import persistence as _p


class TestReconcileShift(unittest.TestCase):
    """Tests for reconcile_shift() — core matching logic."""

    def test_matched_shift_no_variance(self):
        """Identical rostered and actual shifts result in MATCHED status."""
        rostered = {
            "employee_id": "emp_1",
            "employee_name": "Alice",
            "venue_id": "v_1",
            "shift_date": "2026-04-20",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_1",
            "shift_date": "2026-04-20",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.MATCHED
        assert recon.variance_hours == 0.0
        assert recon.variance_pct == 0.0
        assert recon.employee_id == "emp_1"

    def test_no_show_when_actual_hours_zero(self):
        """NO_SHOW status when actual hours = 0."""
        rostered = {
            "employee_id": "emp_2",
            "employee_name": "Bob",
            "venue_id": "v_1",
            "shift_date": "2026-04-21",
            "start": "10:00",
            "end": "18:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_2",
            "shift_date": "2026-04-21",
            "start": None,
            "end": None,
            "hours": 0.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.NO_SHOW
        assert recon.actual_hours == 0.0

    def test_unrostered_clock_in(self):
        """UNROSTERED_CLOCK_IN when actual > 0 but rostered = 0."""
        rostered = {
            "employee_id": "emp_3",
            "employee_name": "Charlie",
            "venue_id": "v_1",
            "shift_date": "2026-04-22",
            "start": None,
            "end": None,
            "hours": 0.0,
            "hourly_rate": 0.0,
        }
        actual = {
            "employee_id": "emp_3",
            "employee_name": "Charlie",
            "shift_date": "2026-04-22",
            "start": "12:00",
            "end": "16:00",
            "hours": 4.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.UNROSTERED_CLOCK_IN
        assert recon.actual_hours == 4.0

    def test_late_start_15min_threshold(self):
        """LATE_START when actual_start > rostered_start + 15min."""
        rostered = {
            "employee_id": "emp_4",
            "employee_name": "Diana",
            "venue_id": "v_1",
            "shift_date": "2026-04-23",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_4",
            "shift_date": "2026-04-23",
            "start": "09:20",  # 20 min late
            "end": "17:20",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.LATE_START
        assert "20 min late" in recon.notes

    def test_late_start_within_threshold(self):
        """Shift with <15min late start is MATCHED (not LATE_START)."""
        rostered = {
            "employee_id": "emp_5",
            "employee_name": "Eve",
            "venue_id": "v_1",
            "shift_date": "2026-04-24",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_5",
            "shift_date": "2026-04-24",
            "start": "09:10",  # 10 min late, within threshold
            "end": "17:10",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        # Should still be MATCHED because variance is within 10%
        assert recon.status == tr.ReconStatus.MATCHED

    def test_early_finish_15min_threshold(self):
        """EARLY_FINISH when actual_end < rostered_end - 15min."""
        rostered = {
            "employee_id": "emp_6",
            "employee_name": "Frank",
            "venue_id": "v_1",
            "shift_date": "2026-04-25",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_6",
            "shift_date": "2026-04-25",
            "start": "09:00",
            "end": "16:40",  # 20 min early
            "hours": 7.67,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.EARLY_FINISH
        assert "20 min early" in recon.notes

    def test_over_rostered_plus_10pct(self):
        """OVER_ROSTERED when variance > +10%."""
        rostered = {
            "employee_id": "emp_7",
            "employee_name": "Grace",
            "venue_id": "v_1",
            "shift_date": "2026-04-26",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_7",
            "shift_date": "2026-04-26",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.9,  # +11.25%
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.OVER_ROSTERED
        assert recon.variance_pct > 10

    def test_under_rostered_minus_10pct(self):
        """UNDER_ROSTERED when variance < -10%."""
        rostered = {
            "employee_id": "emp_8",
            "employee_name": "Henry",
            "venue_id": "v_1",
            "shift_date": "2026-04-27",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_8",
            "shift_date": "2026-04-27",
            "start": "09:00",
            "end": "17:00",
            "hours": 7.0,  # -12.5%
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.UNDER_ROSTERED
        assert recon.variance_pct < -10

    def test_variance_within_10pct_is_matched(self):
        """Variance between -10% and +10% results in MATCHED."""
        rostered = {
            "employee_id": "emp_9",
            "employee_name": "Ivy",
            "venue_id": "v_1",
            "shift_date": "2026-04-28",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_9",
            "shift_date": "2026-04-28",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.4,  # +5%
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.MATCHED
        assert -10 <= recon.variance_pct <= 10

    def test_cost_calculation_weekday(self):
        """Cost for weekday shift uses base rate."""
        rostered = {
            "employee_id": "emp_10",
            "employee_name": "Jack",
            "venue_id": "v_1",
            "shift_date": "2026-04-21",  # Monday
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_10",
            "shift_date": "2026-04-21",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        # Monday is day_of_week=0, no penalty
        assert recon.rostered_cost == 8.0 * 25.0  # 200.0

    def test_cost_calculation_saturday(self):
        """Cost for Saturday includes +25% penalty."""
        rostered = {
            "employee_id": "emp_11",
            "employee_name": "Kate",
            "venue_id": "v_1",
            "shift_date": "2026-04-25",  # Saturday
            "start": "10:00",
            "end": "18:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_11",
            "shift_date": "2026-04-25",
            "start": "10:00",
            "end": "18:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        # Saturday (day 5): 8.0 * 25.0 * 1.25 = 250.0
        assert recon.rostered_cost == 250.0

    def test_cost_calculation_sunday(self):
        """Cost for Sunday includes +50% penalty."""
        rostered = {
            "employee_id": "emp_12",
            "employee_name": "Leo",
            "venue_id": "v_1",
            "shift_date": "2026-04-26",  # Sunday
            "start": "10:00",
            "end": "18:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_12",
            "shift_date": "2026-04-26",
            "start": "10:00",
            "end": "18:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        # Sunday (day 6): 8.0 * 25.0 * 1.50 = 300.0
        assert recon.rostered_cost == 300.0

    def test_recon_has_created_at(self):
        """ShiftRecon always has created_at timestamp."""
        rostered = {
            "employee_id": "emp_13",
            "employee_name": "Mia",
            "venue_id": "v_1",
            "shift_date": "2026-04-29",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_13",
            "shift_date": "2026-04-29",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.created_at is not None
        assert isinstance(recon.created_at, datetime)


class TestReconcileDay(unittest.TestCase):
    """Tests for reconcile_day() — matching and aggregation."""

    def test_reconcile_day_single_shift(self):
        """Reconcile a day with a single matched shift."""
        rostered = [
            {
                "employee_id": "emp_1",
                "employee_name": "Alice",
                "venue_id": "v_1",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            }
        ]
        actual = [
            {
                "employee_id": "emp_1",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            }
        ]

        recons = tr.reconcile_day("v_1", "2026-04-20", rostered, actual)

        assert len(recons) == 1
        assert recons[0].status == tr.ReconStatus.MATCHED

    def test_reconcile_day_multiple_shifts(self):
        """Reconcile a day with multiple employees."""
        rostered = [
            {
                "employee_id": "emp_1",
                "employee_name": "Alice",
                "venue_id": "v_1",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            },
            {
                "employee_id": "emp_2",
                "employee_name": "Bob",
                "venue_id": "v_1",
                "shift_date": "2026-04-20",
                "start": "12:00",
                "end": "20:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            },
        ]
        actual = [
            {
                "employee_id": "emp_1",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            },
            {
                "employee_id": "emp_2",
                "shift_date": "2026-04-20",
                "start": "12:00",
                "end": "20:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            },
        ]

        recons = tr.reconcile_day("v_1", "2026-04-20", rostered, actual)

        assert len(recons) == 2
        assert all(r.status == tr.ReconStatus.MATCHED for r in recons)

    def test_reconcile_day_with_no_show(self):
        """Reconcile includes no-show record when employee absent."""
        rostered = [
            {
                "employee_id": "emp_1",
                "employee_name": "Alice",
                "venue_id": "v_1",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            }
        ]
        actual = []

        recons = tr.reconcile_day("v_1", "2026-04-20", rostered, actual)

        assert len(recons) == 1
        assert recons[0].status == tr.ReconStatus.NO_SHOW

    def test_reconcile_day_unrostered_clock_in(self):
        """Reconcile detects unrostered employees who clocked in."""
        rostered = []
        actual = [
            {
                "employee_id": "emp_1",
                "employee_name": "Alice",
                "shift_date": "2026-04-20",
                "start": "09:00",
                "end": "17:00",
                "hours": 8.0,
                "hourly_rate": 25.0,
            }
        ]

        recons = tr.reconcile_day("v_1", "2026-04-20", rostered, actual)

        assert len(recons) == 1
        assert recons[0].status == tr.ReconStatus.UNROSTERED_CLOCK_IN


class TestBuildReconSummary(unittest.TestCase):
    """Tests for build_recon_summary() — aggregation."""

    def test_summary_empty_recons(self):
        """Summary with no recons has zero totals and 100% match rate."""
        summary = tr.build_recon_summary([], "v_1", "2026-04-20", "2026-04-26")

        assert summary.total_rostered_hours == 0.0
        assert summary.total_actual_hours == 0.0
        assert summary.shifts_reconciled == 0
        assert summary.match_rate_pct == 0.0

    def test_summary_aggregates_hours(self):
        """Summary totals rostered and actual hours."""
        recons = [
            tr.reconcile_shift(
                {
                    "employee_id": "emp_1",
                    "employee_name": "Alice",
                    "venue_id": "v_1",
                    "shift_date": "2026-04-20",
                    "start": "09:00",
                    "end": "17:00",
                    "hours": 8.0,
                    "hourly_rate": 25.0,
                },
                {
                    "employee_id": "emp_1",
                    "shift_date": "2026-04-20",
                    "start": "09:00",
                    "end": "17:00",
                    "hours": 8.0,
                    "hourly_rate": 25.0,
                },
            ),
            tr.reconcile_shift(
                {
                    "employee_id": "emp_2",
                    "employee_name": "Bob",
                    "venue_id": "v_1",
                    "shift_date": "2026-04-20",
                    "start": "12:00",
                    "end": "18:00",
                    "hours": 6.0,
                    "hourly_rate": 25.0,
                },
                {
                    "employee_id": "emp_2",
                    "shift_date": "2026-04-20",
                    "start": "12:00",
                    "end": "18:00",
                    "hours": 6.0,
                    "hourly_rate": 25.0,
                },
            ),
        ]

        summary = tr.build_recon_summary(recons, "v_1", "2026-04-20", "2026-04-20")

        assert summary.total_rostered_hours == 14.0
        assert summary.total_actual_hours == 14.0
        assert summary.total_variance_hours == 0.0

    def test_summary_counts_statuses(self):
        """Summary counts issues by status."""
        # Create recons with different statuses
        no_show_recon = tr.ShiftRecon(
            recon_id="r1",
            employee_id="e1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start=None,
            actual_end=None,
            actual_hours=0.0,
            variance_hours=-8.0,
            variance_pct=-100.0,
            rostered_cost=200.0,
            actual_cost=0.0,
            cost_variance=-200.0,
            status=tr.ReconStatus.NO_SHOW,
        )
        late_recon = tr.ShiftRecon(
            recon_id="r2",
            employee_id="e2",
            employee_name="Bob",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:20",
            actual_end="17:20",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.LATE_START,
        )

        summary = tr.build_recon_summary(
            [no_show_recon, late_recon], "v1", "2026-04-20", "2026-04-20"
        )

        assert summary.no_show_count == 1
        assert summary.late_start_count == 1
        assert summary.shifts_reconciled == 2


class TestDetectPatterns(unittest.TestCase):
    """Tests for detect_patterns() — pattern detection."""

    def test_patterns_empty_recons(self):
        """Pattern detection with no recons returns empty patterns."""
        patterns = tr.detect_patterns([])

        assert patterns["frequent_no_shows"] == {}
        assert patterns["frequent_late_starts"] == {}
        assert patterns["high_cost_variance"] == []

    def test_patterns_frequent_no_shows(self):
        """Pattern detection identifies employees with 2+ no-shows."""
        recons = []
        for i in range(3):
            recon = tr.ShiftRecon(
                recon_id=f"r{i}",
                employee_id="emp_1",
                employee_name="Alice",
                venue_id="v1",
                shift_date=f"2026-04-{20+i}",
                rostered_start="09:00",
                rostered_end="17:00",
                rostered_hours=8.0,
                actual_start=None,
                actual_end=None,
                actual_hours=0.0,
                variance_hours=-8.0,
                variance_pct=-100.0,
                rostered_cost=200.0,
                actual_cost=0.0,
                cost_variance=-200.0,
                status=tr.ReconStatus.NO_SHOW,
            )
            recons.append(recon)

        patterns = tr.detect_patterns(recons)

        assert "emp_1" in patterns["frequent_no_shows"]
        assert patterns["frequent_no_shows"]["emp_1"]["count"] == 3

    def test_patterns_high_cost_variance(self):
        """Pattern detection identifies shifts with cost variance > $50."""
        recon = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=10.0,  # Over rostered
            variance_hours=2.0,
            variance_pct=25.0,
            rostered_cost=200.0,
            actual_cost=300.0,
            cost_variance=100.0,  # > $50
            status=tr.ReconStatus.OVER_ROSTERED,
        )

        patterns = tr.detect_patterns([recon])

        assert len(patterns["high_cost_variance"]) == 1
        assert patterns["high_cost_variance"][0]["variance_aud"] == 100.0


class TestReconStore(unittest.TestCase):
    """Tests for ReconStore — persistence and querying."""

    def setUp(self):
        """Reset store before each test."""
        _p.reset_for_tests()
        _p.force_enable_for_tests(True)
        # Create a fresh store for each test
        self.store = tr.ReconStore()

    def tearDown(self):
        """Clean up after test."""
        _p.reset_for_tests()
        _p.force_enable_for_tests(False)

    def test_store_persist_shift_recon(self):
        """Store persists and retrieves shift reconciliation."""
        recon = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )

        self.store.persist_shift_recon(recon)
        retrieved = self.store.get_recon("r1")

        assert retrieved is not None
        assert retrieved.employee_id == "emp_1"
        assert retrieved.status == tr.ReconStatus.MATCHED

    def test_store_query_by_venue(self):
        """Store queries shift recons by venue_id."""
        recon1 = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )
        recon2 = tr.ShiftRecon(
            recon_id="r2",
            employee_id="emp_2",
            employee_name="Bob",
            venue_id="v2",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )

        self.store.persist_shift_recon(recon1)
        self.store.persist_shift_recon(recon2)

        results = self.store.query_recons(venue_id="v1")

        assert len(results) == 1
        assert results[0].venue_id == "v1"

    def test_store_query_by_status(self):
        """Store queries shift recons by status."""
        no_show = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start=None,
            actual_end=None,
            actual_hours=0.0,
            variance_hours=-8.0,
            variance_pct=-100.0,
            rostered_cost=200.0,
            actual_cost=0.0,
            cost_variance=-200.0,
            status=tr.ReconStatus.NO_SHOW,
        )
        matched = tr.ShiftRecon(
            recon_id="r2",
            employee_id="emp_2",
            employee_name="Bob",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )

        self.store.persist_shift_recon(no_show)
        self.store.persist_shift_recon(matched)

        results = self.store.query_recons(status=tr.ReconStatus.NO_SHOW)

        assert len(results) == 1
        assert results[0].status == tr.ReconStatus.NO_SHOW

    def test_store_persist_summary(self):
        """Store persists and retrieves reconciliation summary."""
        summary = tr.ReconSummary(
            summary_id="s1",
            venue_id="v1",
            period_start="2026-04-20",
            period_end="2026-04-26",
            total_rostered_hours=56.0,
            total_actual_hours=56.0,
            total_variance_hours=0.0,
            total_rostered_cost=1400.0,
            total_actual_cost=1400.0,
            total_cost_variance=0.0,
            match_rate_pct=100.0,
            no_show_count=0,
            late_start_count=0,
            early_finish_count=0,
            over_roster_count=0,
            under_roster_count=0,
            unrostered_count=0,
            shifts_reconciled=7,
        )

        self.store.persist_summary(summary)
        retrieved = self.store.query_summaries(venue_id="v1")

        assert len(retrieved) > 0
        assert retrieved[0].match_rate_pct == 100.0

    def test_store_clear(self):
        """Store clear() removes all records."""
        recon = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )

        self.store.persist_shift_recon(recon)
        assert len(self.store.query_recons()) > 0

        self.store.clear()
        assert len(self.store.query_recons()) == 0


class TestEdgeCases(unittest.TestCase):
    """Edge case and integration tests."""

    def test_midnight_boundary_shift(self):
        """Shifts crossing midnight are handled."""
        rostered = {
            "employee_id": "emp_1",
            "employee_name": "Alice",
            "venue_id": "v_1",
            "shift_date": "2026-04-20",
            "start": "22:00",
            "end": "06:00",  # Next day
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_1",
            "shift_date": "2026-04-20",
            "start": "22:00",
            "end": "06:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.MATCHED
        assert recon.variance_hours == 0.0

    def test_zero_hours_shift(self):
        """Shift with zero hours is handled."""
        rostered = {
            "employee_id": "emp_1",
            "employee_name": "Alice",
            "venue_id": "v_1",
            "shift_date": "2026-04-20",
            "start": None,
            "end": None,
            "hours": 0.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_1",
            "shift_date": "2026-04-20",
            "start": None,
            "end": None,
            "hours": 0.0,
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.variance_hours == 0.0
        assert recon.variance_pct is None

    def test_malformed_time_string(self):
        """Malformed time strings are handled gracefully."""
        rostered = {
            "employee_id": "emp_1",
            "employee_name": "Alice",
            "venue_id": "v_1",
            "shift_date": "2026-04-20",
            "start": "invalid_time",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_1",
            "shift_date": "2026-04-20",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }

        # Should not raise exception
        recon = tr.reconcile_shift(rostered, actual)
        assert recon is not None

    def test_very_large_variance(self):
        """Very large positive/negative variance is detected."""
        rostered = {
            "employee_id": "emp_1",
            "employee_name": "Alice",
            "venue_id": "v_1",
            "shift_date": "2026-04-20",
            "start": "09:00",
            "end": "17:00",
            "hours": 8.0,
            "hourly_rate": 25.0,
        }
        actual = {
            "employee_id": "emp_1",
            "shift_date": "2026-04-20",
            "start": "09:00",
            "end": "17:00",
            "hours": 20.0,  # 150% variance
            "hourly_rate": 25.0,
        }

        recon = tr.reconcile_shift(rostered, actual)

        assert recon.status == tr.ReconStatus.OVER_ROSTERED
        assert recon.variance_pct == 150.0

    def test_multiple_recons_same_employee_different_dates(self):
        """Multiple shifts for same employee on different dates are separate."""
        store = tr.ReconStore()

        recon1 = tr.ShiftRecon(
            recon_id="r1",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-20",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start="09:00",
            actual_end="17:00",
            actual_hours=8.0,
            variance_hours=0.0,
            variance_pct=0.0,
            rostered_cost=200.0,
            actual_cost=200.0,
            cost_variance=0.0,
            status=tr.ReconStatus.MATCHED,
        )
        recon2 = tr.ShiftRecon(
            recon_id="r2",
            employee_id="emp_1",
            employee_name="Alice",
            venue_id="v1",
            shift_date="2026-04-21",
            rostered_start="09:00",
            rostered_end="17:00",
            rostered_hours=8.0,
            actual_start=None,
            actual_end=None,
            actual_hours=0.0,
            variance_hours=-8.0,
            variance_pct=-100.0,
            rostered_cost=200.0,
            actual_cost=0.0,
            cost_variance=-200.0,
            status=tr.ReconStatus.NO_SHOW,
        )

        store.persist_shift_recon(recon1)
        store.persist_shift_recon(recon2)

        results = store.query_recons(employee_id="emp_1")

        assert len(results) == 2
        assert results[0].shift_date != results[1].shift_date


if __name__ == "__main__":
    unittest.main()
