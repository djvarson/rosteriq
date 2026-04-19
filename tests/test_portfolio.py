"""Test suite for portfolio.py — multi-venue portfolio reporting.

Tests the build_venue_summary, build_portfolio_report, and detect_anomalies
functions with realistic multi-venue scenarios.
"""

import unittest
from datetime import date, timedelta

from rosteriq.tanda_history import DailyActuals, get_history_store
from rosteriq.portfolio import (
    build_venue_summary,
    build_portfolio_report,
    detect_anomalies,
)


class TestEmptyVenueSummary(unittest.TestCase):
    """Test venue summary with no data returns zeros."""

    def setUp(self):
        """Reset history store."""
        store = get_history_store()
        store.clear()

    def test_empty_venue_summary(self):
        """Venue with no data returns zeros."""
        summary = build_venue_summary("v_empty", days=7)

        self.assertEqual(summary["venue_id"], "v_empty")
        self.assertEqual(summary["period_days"], 7)
        self.assertEqual(summary["total_revenue"], 0.0)
        self.assertEqual(summary["total_labour_cost"], 0.0)
        self.assertIsNone(summary["avg_labour_pct"])
        self.assertEqual(summary["total_variance_hours"], 0.0)
        self.assertEqual(summary["days_over_forecast"], 0)
        self.assertEqual(summary["days_under_forecast"], 0)
        self.assertIsNone(summary["busiest_day"])
        self.assertIsNone(summary["quietest_day"])


class TestVenueSummaryWithData(unittest.TestCase):
    """Test venue summary with populated data."""

    def setUp(self):
        """Reset history store and populate with test data."""
        store = get_history_store()
        store.clear()
        self.store = store

        # Seed 7 days of daily actuals
        today = date.today()
        for i in range(7):
            day = today - timedelta(days=6 - i)
            revenue = 2000.0 + (i * 100)  # Increasing revenue
            worked_cost = revenue * 0.30  # 30% labour
            actuals = DailyActuals(
                venue_id="v1",
                day=day,
                rostered_hours=40.0,
                worked_hours=40.0 + (i * 0.5),  # Slight over-rostering
                rostered_cost=worked_cost - (i * 10),
                worked_cost=worked_cost,
                forecast_revenue=revenue * 0.95,
                actual_revenue=revenue,
                shift_count=5,
                employee_count=4,
            )
            store.upsert_daily(actuals)

    def test_venue_summary_with_data(self):
        """Summary with 7 days of data verifies totals."""
        summary = build_venue_summary("v1", days=7)

        self.assertEqual(summary["venue_id"], "v1")
        self.assertEqual(summary["period_days"], 7)

        # Should have 7 days of revenue (2000 + 100 + 200 + ... + 600)
        expected_revenue = sum(2000.0 + (i * 100) for i in range(7))
        self.assertAlmostEqual(summary["total_revenue"], expected_revenue, places=1)

        # Labour cost should be ~30% of revenue
        expected_labour = expected_revenue * 0.30
        self.assertAlmostEqual(summary["total_labour_cost"], expected_labour, delta=100)

        # Average labour pct should be around 30%
        self.assertIsNotNone(summary["avg_labour_pct"])
        self.assertAlmostEqual(summary["avg_labour_pct"], 30.0, delta=1.0)

        # Should have variance hours (over-rostering)
        self.assertGreater(summary["total_variance_hours"], 0.0)

        # Most days should be over forecast (positive variance)
        self.assertGreater(summary["days_over_forecast"], 0)

        # Should have busiest and quietest days
        self.assertIsNotNone(summary["busiest_day"])
        self.assertIsNotNone(summary["quietest_day"])


class TestPortfolioReportMultipleVenues(unittest.TestCase):
    """Test portfolio report aggregates multiple venues correctly."""

    def setUp(self):
        """Reset history store and populate with 2-venue test data."""
        store = get_history_store()
        store.clear()
        self.store = store

        today = date.today()

        # Venue 1: high revenue, 30% labour
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v1",
                day=day,
                rostered_hours=40.0,
                worked_hours=40.0,
                rostered_cost=600.0,
                worked_cost=600.0,
                forecast_revenue=2000.0,
                actual_revenue=2000.0,
                shift_count=5,
                employee_count=4,
            )
            store.upsert_daily(actuals)

        # Venue 2: lower revenue, 25% labour
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v2",
                day=day,
                rostered_hours=30.0,
                worked_hours=30.0,
                rostered_cost=375.0,
                worked_cost=375.0,
                forecast_revenue=1500.0,
                actual_revenue=1500.0,
                shift_count=4,
                employee_count=3,
            )
            store.upsert_daily(actuals)

    def test_portfolio_report_multiple_venues(self):
        """Portfolio report correctly aggregates across venues."""
        report = build_portfolio_report(["v1", "v2"], days=7)

        self.assertEqual(report["period_days"], 7)
        self.assertEqual(report["venue_count"], 2)
        self.assertEqual(len(report["venues"]), 2)

        # Venues should be sorted by revenue (v1 is higher)
        self.assertEqual(report["venues"][0]["venue_id"], "v1")
        self.assertEqual(report["venues"][1]["venue_id"], "v2")

        # Total revenue should be sum of both
        expected_revenue = (2000.0 * 7) + (1500.0 * 7)
        self.assertAlmostEqual(report["totals"]["total_revenue"], expected_revenue, places=1)

        # Total labour cost should be sum of both
        expected_labour = (600.0 * 7) + (375.0 * 7)
        self.assertAlmostEqual(report["totals"]["total_labour_cost"], expected_labour, places=1)

        # Average labour pct should be weighted average
        self.assertIsNotNone(report["totals"]["avg_labour_pct"])

        # Rankings should identify v1 as highest revenue
        self.assertEqual(report["rankings"]["highest_revenue"], "v1")


class TestAnomalyHighLabour(unittest.TestCase):
    """Test anomaly detection for high labour costs."""

    def setUp(self):
        """Reset history store."""
        self.store = get_history_store()
        self.store.clear()

    def test_anomaly_high_labour(self):
        """Detect venues with labour > 35%."""
        today = date.today()

        # Venue with 40% labour cost
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v_high_labour",
                day=day,
                rostered_hours=40.0,
                worked_hours=40.0,
                rostered_cost=800.0,
                worked_cost=800.0,
                forecast_revenue=2000.0,
                actual_revenue=2000.0,
                shift_count=5,
                employee_count=4,
            )
            self.store.upsert_daily(actuals)

        summary = build_venue_summary("v_high_labour", days=7)
        anomalies = detect_anomalies([summary])

        # Should flag high labour
        high_labour_flags = [a for a in anomalies if a["type"] == "high_labour"]
        self.assertEqual(len(high_labour_flags), 1)
        self.assertIn("40.0%", high_labour_flags[0]["message"])


class TestAnomalyLowLabour(unittest.TestCase):
    """Test anomaly detection for suspiciously low labour costs."""

    def setUp(self):
        """Reset history store."""
        self.store = get_history_store()
        self.store.clear()

    def test_anomaly_low_labour(self):
        """Detect venues with labour < 20%."""
        today = date.today()

        # Venue with 15% labour cost
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v_low_labour",
                day=day,
                rostered_hours=20.0,
                worked_hours=20.0,
                rostered_cost=300.0,
                worked_cost=300.0,
                forecast_revenue=2000.0,
                actual_revenue=2000.0,
                shift_count=3,
                employee_count=2,
            )
            self.store.upsert_daily(actuals)

        summary = build_venue_summary("v_low_labour", days=7)
        anomalies = detect_anomalies([summary])

        # Should flag low labour
        low_labour_flags = [a for a in anomalies if a["type"] == "low_labour"]
        self.assertEqual(len(low_labour_flags), 1)
        self.assertIn("15.0%", low_labour_flags[0]["message"])


class TestAnomalyOverRostered(unittest.TestCase):
    """Test anomaly detection for over-rostering."""

    def setUp(self):
        """Reset history store."""
        self.store = get_history_store()
        self.store.clear()

    def test_anomaly_over_rostered(self):
        """Detect venues with significant over-rostering."""
        today = date.today()

        # Venue with 10+ hours over-rostering
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v_over",
                day=day,
                rostered_hours=40.0,
                worked_hours=50.0,  # 10 hours over
                rostered_cost=600.0,
                worked_cost=750.0,
                forecast_revenue=2000.0,
                actual_revenue=2000.0,
                shift_count=6,
                employee_count=5,
            )
            self.store.upsert_daily(actuals)

        summary = build_venue_summary("v_over", days=7)
        anomalies = detect_anomalies([summary])

        # Should flag over-rostering
        over_flags = [a for a in anomalies if a["type"] == "over_rostered"]
        self.assertEqual(len(over_flags), 1)


class TestAnomalyDataGap(unittest.TestCase):
    """Test anomaly detection for data gaps."""

    def setUp(self):
        """Reset history store."""
        self.store = get_history_store()
        self.store.clear()

    def test_anomaly_data_gap(self):
        """Detect venues with no revenue (data gap)."""
        today = date.today()

        # Venue with 0 revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v_gap",
                day=day,
                rostered_hours=40.0,
                worked_hours=40.0,
                rostered_cost=600.0,
                worked_cost=600.0,
                forecast_revenue=0.0,
                actual_revenue=0.0,
                shift_count=0,
                employee_count=0,
            )
            self.store.upsert_daily(actuals)

        summary = build_venue_summary("v_gap", days=7)
        anomalies = detect_anomalies([summary])

        # Should flag data gap
        gap_flags = [a for a in anomalies if a["type"] == "data_gap"]
        self.assertEqual(len(gap_flags), 1)


class TestRankings(unittest.TestCase):
    """Test portfolio rankings with multiple venues."""

    def setUp(self):
        """Reset history store and populate with 3 venues."""
        store = get_history_store()
        store.clear()
        self.store = store

        today = date.today()

        # Venue 1: 30% labour, high revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v1",
                day=day,
                rostered_hours=40.0,
                worked_hours=41.0,
                rostered_cost=600.0,
                worked_cost=600.0,
                forecast_revenue=2000.0,
                actual_revenue=2000.0,
                shift_count=5,
                employee_count=4,
            )
            store.upsert_daily(actuals)

        # Venue 2: 25% labour, medium revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v2",
                day=day,
                rostered_hours=30.0,
                worked_hours=30.0,
                rostered_cost=375.0,
                worked_cost=375.0,
                forecast_revenue=1500.0,
                actual_revenue=1500.0,
                shift_count=4,
                employee_count=3,
            )
            store.upsert_daily(actuals)

        # Venue 3: 35% labour, low revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="v3",
                day=day,
                rostered_hours=30.0,
                worked_hours=30.0,
                rostered_cost=525.0,
                worked_cost=525.0,
                forecast_revenue=1500.0,
                actual_revenue=1500.0,
                shift_count=4,
                employee_count=3,
            )
            store.upsert_daily(actuals)

    def test_rankings(self):
        """Verify rankings identify correct venues."""
        report = build_portfolio_report(["v1", "v2", "v3"], days=7)

        # Best labour should be v2 (25%)
        self.assertEqual(report["rankings"]["best_labour_pct"], "v2")

        # Worst labour should be v3 (35%)
        self.assertEqual(report["rankings"]["worst_labour_pct"], "v3")

        # Highest revenue should be v1
        self.assertEqual(report["rankings"]["highest_revenue"], "v1")

        # Most over-rostered should be v1 (7 hours)
        self.assertEqual(report["rankings"]["most_over_rostered"], "v1")


class TestPortfolioSortedByRevenue(unittest.TestCase):
    """Test portfolio venues are sorted by revenue descending."""

    def setUp(self):
        """Reset history store and populate with 3 venues in non-sorted order."""
        store = get_history_store()
        store.clear()
        self.store = store

        today = date.today()

        # Venue A: low revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="vA",
                day=day,
                rostered_hours=20.0,
                worked_hours=20.0,
                rostered_cost=300.0,
                worked_cost=300.0,
                forecast_revenue=1000.0,
                actual_revenue=1000.0,
                shift_count=2,
                employee_count=2,
            )
            store.upsert_daily(actuals)

        # Venue B: high revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="vB",
                day=day,
                rostered_hours=50.0,
                worked_hours=50.0,
                rostered_cost=750.0,
                worked_cost=750.0,
                forecast_revenue=2500.0,
                actual_revenue=2500.0,
                shift_count=6,
                employee_count=5,
            )
            store.upsert_daily(actuals)

        # Venue C: medium revenue
        for i in range(7):
            day = today - timedelta(days=6 - i)
            actuals = DailyActuals(
                venue_id="vC",
                day=day,
                rostered_hours=35.0,
                worked_hours=35.0,
                rostered_cost=525.0,
                worked_cost=525.0,
                forecast_revenue=1750.0,
                actual_revenue=1750.0,
                shift_count=4,
                employee_count=3,
            )
            store.upsert_daily(actuals)

    def test_portfolio_sorted_by_revenue(self):
        """Venues list should be sorted by revenue descending."""
        # Query in non-sorted order
        report = build_portfolio_report(["vA", "vC", "vB"], days=7)

        venues = report["venues"]
        self.assertEqual(len(venues), 3)

        # Should be sorted: vB > vC > vA
        self.assertEqual(venues[0]["venue_id"], "vB")
        self.assertEqual(venues[1]["venue_id"], "vC")
        self.assertEqual(venues[2]["venue_id"], "vA")

        # Verify revenue order
        self.assertGreater(venues[0]["total_revenue"], venues[1]["total_revenue"])
        self.assertGreater(venues[1]["total_revenue"], venues[2]["total_revenue"])


if __name__ == "__main__":
    unittest.main()
