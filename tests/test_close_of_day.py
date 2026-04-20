"""
Tests for rosteriq.close_of_day — Close-of-Day reconciliation module.

Runs with: PYTHONPATH=. python3 -m unittest tests.test_close_of_day -v

Coverage:
- Till variance calculations and classifications
- CoD record creation and persistence
- Sign-off workflow (sign_off, query_cod)
- Revenue breakdown aggregation
- Period summary and anomaly detection
- SQLite store CRUD operations
- Edge cases (zero revenue, negative variance, etc.)
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.close_of_day import (
    PaymentMethod, TillStatus, SignOffStatus,
    TillCount, RevenueBreakdown, CloseOfDay, CoDSummary,
    create_close_of_day, calculate_till_variance, classify_till,
    sign_off, query_cod, build_cod_summary, get_discrepancy_trend,
    flag_anomalies, get_store, CloseOfDayStore,
)
from rosteriq import persistence as persistence_module

# Enable in-memory SQLite for tests
persistence_module.force_enable_for_tests(True)


def _reset_store():
    """Reset persistence for each test."""
    persistence_module.reset_for_tests()
    persistence_module.force_enable_for_tests(True)


# ============================================================================
# TillCount Tests
# ============================================================================

class TestTillCount(unittest.TestCase):
    """Test TillCount dataclass and variance calculation."""

    def setUp(self):
        self.now = datetime.now(timezone.utc)

    def test_till_count_creation(self):
        """Create a till count."""
        tc = TillCount(
            till_id="till-1",
            counted_amount=500.0,
            expected_amount=495.0,
            counted_by="emp-1",
            counted_at=self.now,
            notes="Regular count",
        )
        self.assertEqual(tc.till_id, "till-1")
        self.assertEqual(tc.counted_amount, 500.0)
        self.assertEqual(tc.variance, 5.0)

    def test_till_variance_positive(self):
        """Test positive variance (overage)."""
        tc = TillCount(
            till_id="till-1",
            counted_amount=510.0,
            expected_amount=500.0,
            counted_by="emp-1",
            counted_at=self.now,
        )
        self.assertEqual(tc.variance, 10.0)

    def test_till_variance_negative(self):
        """Test negative variance (shortage)."""
        tc = TillCount(
            till_id="till-1",
            counted_amount=485.0,
            expected_amount=500.0,
            counted_by="emp-1",
            counted_at=self.now,
        )
        self.assertEqual(tc.variance, -15.0)

    def test_till_count_serialization(self):
        """Serialize and deserialize till count."""
        tc = TillCount(
            till_id="till-1",
            counted_amount=500.0,
            expected_amount=495.0,
            counted_by="emp-1",
            counted_at=self.now,
            notes="Test",
        )
        d = tc.to_dict()
        tc2 = TillCount.from_dict(d)
        self.assertEqual(tc2.till_id, "till-1")
        self.assertEqual(tc2.variance, 5.0)
        self.assertEqual(tc2.counted_by, "emp-1")


# ============================================================================
# RevenueBreakdown Tests
# ============================================================================

class TestRevenueBreakdown(unittest.TestCase):
    """Test RevenueBreakdown dataclass."""

    def test_revenue_breakdown_creation(self):
        """Create revenue breakdown."""
        rb = RevenueBreakdown(
            payment_method=PaymentMethod.CASH,
            amount=1000.0,
            transaction_count=50,
        )
        self.assertEqual(rb.payment_method, PaymentMethod.CASH)
        self.assertEqual(rb.amount, 1000.0)
        self.assertEqual(rb.transaction_count, 50)

    def test_revenue_breakdown_serialization(self):
        """Serialize and deserialize revenue breakdown."""
        rb = RevenueBreakdown(
            payment_method=PaymentMethod.CARD,
            amount=2500.0,
            transaction_count=120,
        )
        d = rb.to_dict()
        rb2 = RevenueBreakdown.from_dict(d)
        self.assertEqual(rb2.payment_method, PaymentMethod.CARD)
        self.assertEqual(rb2.amount, 2500.0)


# ============================================================================
# Till Classification Tests
# ============================================================================

class TestTillClassification(unittest.TestCase):
    """Test till status classification."""

    def test_classify_till_balanced(self):
        """Till within tolerance is BALANCED."""
        status = classify_till(expected_amount=500.0, counted_amount=502.0, tolerance=5.0)
        self.assertEqual(status, TillStatus.BALANCED)

    def test_classify_till_balanced_exact(self):
        """Till with zero variance is BALANCED."""
        status = classify_till(expected_amount=500.0, counted_amount=500.0, tolerance=5.0)
        self.assertEqual(status, TillStatus.BALANCED)

    def test_classify_till_over(self):
        """Till with positive variance is OVER."""
        status = classify_till(expected_amount=500.0, counted_amount=510.0, tolerance=5.0)
        self.assertEqual(status, TillStatus.OVER)

    def test_classify_till_short(self):
        """Till with negative variance is SHORT."""
        status = classify_till(expected_amount=500.0, counted_amount=490.0, tolerance=5.0)
        self.assertEqual(status, TillStatus.SHORT)

    def test_classify_till_custom_tolerance(self):
        """Test custom tolerance value."""
        status = classify_till(expected_amount=500.0, counted_amount=505.5, tolerance=10.0)
        self.assertEqual(status, TillStatus.BALANCED)

        status = classify_till(expected_amount=500.0, counted_amount=511.0, tolerance=10.0)
        self.assertEqual(status, TillStatus.OVER)


# ============================================================================
# Variance Calculation Tests
# ============================================================================

class TestVarianceCalculation(unittest.TestCase):
    """Test total variance calculations."""

    def setUp(self):
        self.now = datetime.now(timezone.utc)

    def test_calculate_single_till_variance(self):
        """Calculate variance from single till."""
        tc = TillCount(
            till_id="till-1",
            counted_amount=550.0,
            expected_amount=500.0,
            counted_by="emp-1",
            counted_at=self.now,
        )
        variance = calculate_till_variance([tc])
        self.assertEqual(variance, 50.0)

    def test_calculate_multiple_tills_variance(self):
        """Calculate total variance from multiple tills."""
        tills = [
            TillCount("till-1", 550.0, 500.0, "emp-1", self.now),
            TillCount("till-2", 480.0, 500.0, "emp-1", self.now),
            TillCount("till-3", 520.0, 500.0, "emp-1", self.now),
        ]
        variance = calculate_till_variance(tills)
        # 50 + (-20) + 20 = 50
        self.assertEqual(variance, 50.0)

    def test_calculate_zero_variance(self):
        """Calculate zero variance."""
        tills = [
            TillCount("till-1", 500.0, 500.0, "emp-1", self.now),
            TillCount("till-2", 500.0, 500.0, "emp-1", self.now),
        ]
        variance = calculate_till_variance(tills)
        self.assertEqual(variance, 0.0)

    def test_calculate_empty_tills(self):
        """Calculate variance from empty list."""
        variance = calculate_till_variance([])
        self.assertEqual(variance, 0.0)


# ============================================================================
# CloseOfDay Creation Tests
# ============================================================================

class TestCloseOfDayCreation(unittest.TestCase):
    """Test close-of-day record creation."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_create_close_of_day_basic(self):
        """Create a basic close-of-day record."""
        tills = [
            TillCount("till-1", 500.0, 500.0, "emp-1", self.now),
        ]
        revenue = [
            RevenueBreakdown(PaymentMethod.CASH, 500.0, 50),
        ]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        self.assertIsNotNone(cod.cod_id)
        self.assertEqual(cod.venue_id, "v-1")
        self.assertEqual(cod.total_revenue, 500.0)
        self.assertEqual(cod.total_variance, 0.0)
        self.assertEqual(cod.sign_off_status, SignOffStatus.PENDING)

    def test_create_close_of_day_with_variance(self):
        """Create CoD with till variances."""
        tills = [
            TillCount("till-1", 550.0, 500.0, "emp-1", self.now),
            TillCount("till-2", 480.0, 500.0, "emp-1", self.now),
        ]
        revenue = [
            RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100),
        ]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        # 50 + (-20) = 30
        self.assertEqual(cod.total_variance, 30.0)

    def test_create_close_of_day_calculates_labour_pct(self):
        """Test labour percentage calculation."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            labour_cost=200.0,
        )

        # 200 / 1000 * 100 = 20.0
        self.assertEqual(cod.labour_pct, 20.0)

    def test_create_close_of_day_calculates_average_spend(self):
        """Test average spend calculation."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            covers=50,
        )

        # 1000 / 50 = 20.0
        self.assertEqual(cod.average_spend, 20.0)

    def test_create_close_of_day_classifies_tills(self):
        """Tills are classified during creation."""
        tills = [
            TillCount("till-1", 505.0, 500.0, "emp-1", self.now),
            TillCount("till-2", 490.0, 500.0, "emp-1", self.now),
        ]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        self.assertEqual(cod.till_counts[0].status, TillStatus.BALANCED)
        self.assertEqual(cod.till_counts[1].status, TillStatus.SHORT)


# ============================================================================
# SignOff Workflow Tests
# ============================================================================

class TestSignOffWorkflow(unittest.TestCase):
    """Test sign-off and query workflow."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def _create_test_cod(self) -> str:
        """Create a test CoD record and return its ID."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 500.0, 50)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )
        return cod.cod_id

    def test_sign_off_pending_to_signed(self):
        """Sign off changes status from PENDING to SIGNED_OFF."""
        cod_id = self._create_test_cod()

        signed = sign_off(cod_id, "mgr-1")
        self.assertIsNotNone(signed)
        self.assertEqual(signed.sign_off_status, SignOffStatus.SIGNED_OFF)
        self.assertEqual(signed.signed_off_by, "mgr-1")
        self.assertIsNotNone(signed.signed_off_at)

    def test_query_cod_changes_status(self):
        """Query CoD changes status to QUERIED."""
        cod_id = self._create_test_cod()

        queried = query_cod(cod_id, "mgr-1")
        self.assertIsNotNone(queried)
        self.assertEqual(queried.sign_off_status, SignOffStatus.QUERIED)
        self.assertEqual(queried.signed_off_by, "mgr-1")

    def test_sign_off_nonexistent_record(self):
        """Sign off on non-existent record returns None."""
        result = sign_off("nonexistent", "mgr-1")
        # When persistence is disabled, None is returned
        # When enabled, may return None if not found
        if result is not None:
            self.fail("Should return None for non-existent record")


# ============================================================================
# Period Summary Tests
# ============================================================================

class TestPeriodSummary(unittest.TestCase):
    """Test CoD summary aggregation."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_summary_empty_records(self):
        """Summary with no records."""
        summary = build_cod_summary("v-1", [], self.today, self.today)
        self.assertEqual(summary.trading_days, 0)
        self.assertEqual(summary.total_revenue, 0.0)

    def test_summary_single_record(self):
        """Summary from single CoD record."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            labour_cost=200.0,
            covers=50,
        )

        summary = build_cod_summary("v-1", [cod], self.today, self.today)
        self.assertEqual(summary.trading_days, 1)
        self.assertEqual(summary.total_revenue, 1000.0)
        self.assertEqual(summary.avg_daily_revenue, 1000.0)
        self.assertEqual(summary.avg_labour_pct, 20.0)
        self.assertEqual(summary.avg_spend, 20.0)
        self.assertEqual(summary.total_covers, 50)

    def test_summary_multiple_records(self):
        """Summary from multiple CoD records."""
        cods = []
        for i in range(3):
            tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
            revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]
            trading_date = self.today - timedelta(days=2-i)

            cod = create_close_of_day(
                venue_id="v-1",
                trading_date=trading_date,
                closed_by="emp-1",
                closed_by_name="John Doe",
                pos_total=1000.0,
                till_counts=tills,
                revenue_breakdown=revenue,
                covers=50,
            )
            cods.append(cod)

        ps = self.today - timedelta(days=2)
        pe = self.today
        summary = build_cod_summary("v-1", cods, ps, pe)
        self.assertEqual(summary.trading_days, 3)
        self.assertEqual(summary.total_revenue, 3000.0)
        self.assertEqual(summary.avg_daily_revenue, 1000.0)

    def test_summary_best_worst_days(self):
        """Summary identifies best and worst revenue days."""
        cods = []
        revenues = [500.0, 1500.0, 1000.0]

        for i, rev in enumerate(revenues):
            tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
            revenue = [RevenueBreakdown(PaymentMethod.CASH, rev, 50)]
            trading_date = self.today - timedelta(days=2-i)

            cod = create_close_of_day(
                venue_id="v-1",
                trading_date=trading_date,
                closed_by="emp-1",
                closed_by_name="John Doe",
                pos_total=rev,
                till_counts=tills,
                revenue_breakdown=revenue,
            )
            cods.append(cod)

        ps = self.today - timedelta(days=2)
        pe = self.today
        summary = build_cod_summary("v-1", cods, ps, pe)

        self.assertIsNotNone(summary.best_day)
        self.assertIsNotNone(summary.worst_day)
        self.assertEqual(summary.best_day["revenue"], 1500.0)
        self.assertEqual(summary.worst_day["revenue"], 500.0)


# ============================================================================
# Anomaly Detection Tests
# ============================================================================

class TestAnomalyDetection(unittest.TestCase):
    """Test anomaly flagging."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_flag_anomalies_no_anomalies(self):
        """No anomalies when variance is low."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 10000.0, 500)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=10000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        anomalies = flag_anomalies([cod], threshold_pct=2.0)
        self.assertEqual(len(anomalies), 0)

    def test_flag_anomalies_exceeds_threshold(self):
        """Anomaly flagged when variance exceeds threshold."""
        # Variance of 300 on 10000 revenue = 3%
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 10000.0, 500)]

        # Manually create CoD with high variance
        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=10000.0,
            till_counts=[TillCount("till-1", 800.0, 500.0, "emp-1", self.now)],
            revenue_breakdown=revenue,
        )

        anomalies = flag_anomalies([cod], threshold_pct=2.0)
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["reason"], "variance_exceeds_threshold")

    def test_flag_anomalies_custom_threshold(self):
        """Anomaly detection respects custom threshold."""
        tills = [TillCount("till-1", 700.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 10000.0, 500)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=10000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        # 200 / 10000 = 2%, should not flag at 2.5% threshold
        anomalies = flag_anomalies([cod], threshold_pct=2.5)
        self.assertEqual(len(anomalies), 0)

        # Should flag at 1.5% threshold
        anomalies = flag_anomalies([cod], threshold_pct=1.5)
        self.assertEqual(len(anomalies), 1)


# ============================================================================
# Discrepancy Trend Tests
# ============================================================================

class TestDiscrepancyTrend(unittest.TestCase):
    """Test variance trend data."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_discrepancy_trend_empty(self):
        """Empty trend for no records."""
        trend = get_discrepancy_trend([])
        self.assertEqual(len(trend), 0)

    def test_discrepancy_trend_ordered(self):
        """Trend is ordered by date."""
        cods = []
        for i in range(3):
            tills = [TillCount("till-1", 500.0 + (i * 50), 500.0, "emp-1", self.now)]
            revenue = [RevenueBreakdown(PaymentMethod.CASH, 10000.0, 500)]
            trading_date = self.today - timedelta(days=2-i)

            cod = create_close_of_day(
                venue_id="v-1",
                trading_date=trading_date,
                closed_by="emp-1",
                closed_by_name="John Doe",
                pos_total=10000.0,
                till_counts=tills,
                revenue_breakdown=revenue,
            )
            cods.append(cod)

        trend = get_discrepancy_trend(cods)
        self.assertEqual(len(trend), 3)

        # Check ordering (oldest first)
        self.assertEqual(trend[0]["variance"], 0.0)
        self.assertEqual(trend[1]["variance"], 50.0)
        self.assertEqual(trend[2]["variance"], 100.0)


# ============================================================================
# Store Persistence Tests
# ============================================================================

class TestCloseOfDayStore(unittest.TestCase):
    """Test SQLite store operations."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()
        self.store = get_store()

    def test_store_create_and_retrieve(self):
        """Create a CoD and retrieve it."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 500.0, 50)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        retrieved = self.store.get_by_id(cod.cod_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.cod_id, cod.cod_id)
        self.assertEqual(retrieved.venue_id, "v-1")

    def test_store_retrieve_by_venue_and_date_range(self):
        """Retrieve CoDs for a venue in date range."""
        for i in range(3):
            tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
            revenue = [RevenueBreakdown(PaymentMethod.CASH, 500.0, 50)]
            trading_date = self.today - timedelta(days=i)

            create_close_of_day(
                venue_id="v-1",
                trading_date=trading_date,
                closed_by="emp-1",
                closed_by_name="John Doe",
                pos_total=500.0,
                till_counts=tills,
                revenue_breakdown=revenue,
            )

        ps = self.today - timedelta(days=2)
        pe = self.today
        records = self.store.get_by_venue_and_date_range("v-1", ps, pe)
        self.assertEqual(len(records), 3)

    def test_store_filter_by_status(self):
        """Filter CoDs by sign-off status."""
        # Create first record
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 500.0, 50)]

        cod1 = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        # Create second record and sign it off
        cod2 = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today - timedelta(days=1),
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )
        sign_off(cod2.cod_id, "mgr-1")

        # Filter by PENDING
        pending = self.store.get_by_venue_and_date_range(
            "v-1", self.today - timedelta(days=2), self.today,
            status_filter="pending"
        )
        self.assertEqual(len(pending), 1)

        # Filter by SIGNED_OFF
        signed = self.store.get_by_venue_and_date_range(
            "v-1", self.today - timedelta(days=2), self.today,
            status_filter="signed_off"
        )
        self.assertEqual(len(signed), 1)

    def test_store_update_sign_off(self):
        """Update sign-off status."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 500.0, 50)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=500.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        updated = self.store.update_sign_off(
            cod.cod_id, "mgr-1", SignOffStatus.SIGNED_OFF
        )

        self.assertIsNotNone(updated)
        self.assertEqual(updated.sign_off_status, SignOffStatus.SIGNED_OFF)
        self.assertEqual(updated.signed_off_by, "mgr-1")


# ============================================================================
# Edge Cases
# ============================================================================

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_zero_revenue_handles_division(self):
        """Handle zero revenue gracefully in percentages."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 0.0, 0)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=0.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            labour_cost=50.0,
        )

        # Should not raise division error
        self.assertEqual(cod.labour_pct, 0.0)
        self.assertEqual(cod.average_spend, 0.0)

    def test_zero_covers_handles_division(self):
        """Handle zero covers gracefully."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            covers=0,
        )

        self.assertEqual(cod.average_spend, 0.0)

    def test_multiple_payment_methods(self):
        """Handle multiple payment methods in revenue breakdown."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [
            RevenueBreakdown(PaymentMethod.CASH, 300.0, 30),
            RevenueBreakdown(PaymentMethod.CARD, 500.0, 50),
            RevenueBreakdown(PaymentMethod.EFTPOS, 200.0, 20),
        ]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        self.assertEqual(cod.total_revenue, 1000.0)
        self.assertEqual(len(cod.revenue_breakdown), 3)

    def test_large_variance_amounts(self):
        """Handle large variance amounts."""
        tills = [TillCount("till-1", 1000.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 50000.0, 5000)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=50000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
        )

        self.assertEqual(cod.total_variance, 500.0)
        variance_pct = (abs(cod.total_variance) / cod.total_revenue * 100)
        self.assertAlmostEqual(variance_pct, 1.0, places=2)


# ============================================================================
# Serialization Tests
# ============================================================================

class TestSerialization(unittest.TestCase):
    """Test serialization and deserialization."""

    def setUp(self):
        _reset_store()
        self.now = datetime.now(timezone.utc)
        self.today = date.today()

    def test_close_of_day_serialization(self):
        """Serialize and deserialize CloseOfDay."""
        tills = [TillCount("till-1", 500.0, 500.0, "emp-1", self.now)]
        revenue = [RevenueBreakdown(PaymentMethod.CASH, 1000.0, 100)]

        cod = create_close_of_day(
            venue_id="v-1",
            trading_date=self.today,
            closed_by="emp-1",
            closed_by_name="John Doe",
            pos_total=1000.0,
            till_counts=tills,
            revenue_breakdown=revenue,
            labour_cost=200.0,
        )

        d = cod.to_dict()
        cod2 = CloseOfDay.from_dict(d)

        self.assertEqual(cod2.cod_id, cod.cod_id)
        self.assertEqual(cod2.venue_id, cod.venue_id)
        self.assertEqual(cod2.total_revenue, cod.total_revenue)
        self.assertEqual(cod2.labour_pct, cod.labour_pct)


if __name__ == "__main__":
    unittest.main()
