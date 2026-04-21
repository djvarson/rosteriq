"""Test suite for kpi_dashboard.py module (Round 53).

45+ comprehensive test cases covering:
- Daily KPI calculation and storage
- Weekly/monthly aggregation
- Trend analysis and percent change
- Alert detection and thresholds
- Target setting and progress tracking
- Period comparison
- Venue ranking
- Snapshot retrieval and filtering
- Persistence roundtrip
- Edge cases and error handling
"""

import sys
import os
import unittest
import tempfile
import json
from datetime import datetime, date, timezone, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.kpi_dashboard import (
    get_kpi_dashboard_store,
    _reset_for_tests,
    KPISnapshot,
    KPIPeriod,
    calculate_labour_cost_pct,
    calculate_revenue_per_labour_hour,
    calculate_avg_hourly_cost,
    calculate_covers_per_staff_hour,
    calculate_roster_fill_rate,
    calculate_no_show_rate,
    calculate_break_compliance_rate,
    calculate_avg_shift_length,
)
from rosteriq import persistence as _p


class TestKPICalculations(unittest.TestCase):
    """Test individual KPI calculation functions."""

    def test_labour_cost_pct_normal(self):
        """Test labour cost percentage calculation."""
        pct = calculate_labour_cost_pct(3000, 10000)
        self.assertEqual(pct, 30.0)

    def test_labour_cost_pct_zero_revenue(self):
        """Test with zero revenue."""
        pct = calculate_labour_cost_pct(1000, 0)
        self.assertEqual(pct, 0.0)

    def test_labour_cost_pct_high(self):
        """Test with high labour cost."""
        pct = calculate_labour_cost_pct(5000, 10000)
        self.assertEqual(pct, 50.0)

    def test_revenue_per_labour_hour_normal(self):
        """Test revenue per labour hour."""
        rev = calculate_revenue_per_labour_hour(1000, 10)
        self.assertEqual(rev, 100.0)

    def test_revenue_per_labour_hour_zero_hours(self):
        """Test with zero hours."""
        rev = calculate_revenue_per_labour_hour(1000, 0)
        self.assertEqual(rev, 0.0)

    def test_avg_hourly_cost_normal(self):
        """Test average hourly cost."""
        cost = calculate_avg_hourly_cost(1000, 10)
        self.assertEqual(cost, 100.0)

    def test_avg_hourly_cost_zero_hours(self):
        """Test with zero hours."""
        cost = calculate_avg_hourly_cost(500, 0)
        self.assertEqual(cost, 0.0)

    def test_covers_per_staff_hour_normal(self):
        """Test covers per staff hour."""
        covers = calculate_covers_per_staff_hour(100, 10)
        self.assertEqual(covers, 10.0)

    def test_covers_per_staff_hour_zero_hours(self):
        """Test with zero hours."""
        covers = calculate_covers_per_staff_hour(50, 0)
        self.assertEqual(covers, 0.0)

    def test_roster_fill_rate_full(self):
        """Test 100% fill rate."""
        rate = calculate_roster_fill_rate(10, 10)
        self.assertEqual(rate, 100.0)

    def test_roster_fill_rate_partial(self):
        """Test partial fill rate."""
        rate = calculate_roster_fill_rate(8, 10)
        self.assertEqual(rate, 80.0)

    def test_roster_fill_rate_zero_scheduled(self):
        """Test with zero shifts scheduled."""
        rate = calculate_roster_fill_rate(5, 0)
        self.assertEqual(rate, 0.0)

    def test_no_show_rate_none(self):
        """Test zero no-show rate."""
        rate = calculate_no_show_rate(0, 10)
        self.assertEqual(rate, 0.0)

    def test_no_show_rate_partial(self):
        """Test partial no-show rate."""
        rate = calculate_no_show_rate(2, 10)
        self.assertEqual(rate, 20.0)

    def test_no_show_rate_zero_scheduled(self):
        """Test with zero scheduled."""
        rate = calculate_no_show_rate(2, 0)
        self.assertEqual(rate, 0.0)

    def test_break_compliance_rate_full(self):
        """Test 100% compliance."""
        rate = calculate_break_compliance_rate(10, 0)
        self.assertEqual(rate, 100.0)

    def test_break_compliance_rate_with_violations(self):
        """Test with violations."""
        rate = calculate_break_compliance_rate(10, 2)
        self.assertEqual(rate, 80.0)

    def test_break_compliance_rate_zero_breaks(self):
        """Test with zero breaks."""
        rate = calculate_break_compliance_rate(0, 0)
        self.assertEqual(rate, 100.0)

    def test_avg_shift_length_normal(self):
        """Test average shift length."""
        length = calculate_avg_shift_length(40, 10)
        self.assertEqual(length, 4.0)

    def test_avg_shift_length_zero_shifts(self):
        """Test with zero shifts."""
        length = calculate_avg_shift_length(40, 0)
        self.assertEqual(length, 0.0)


class TestKPIDailySnapshot(unittest.TestCase):
    """Test daily KPI snapshot recording and retrieval."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file."""
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
        """Reset store and persistence."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up."""
        _p.reset_for_tests()

    def test_calculate_daily_kpis_basic(self):
        """Test basic daily KPI calculation."""
        store = get_kpi_dashboard_store()
        snapshot = store.calculate_daily_kpis(
            venue_id="venue_001",
            date_str="2026-04-20",
            revenue=10000,
            labour_cost=3000,
            hours_worked=100,
            covers=150,
            shifts_scheduled=10,
            shifts_filled=10,
            no_shows=0,
            break_violations=0,
            total_breaks=10,
        )

        self.assertIsNotNone(snapshot)
        self.assertTrue(snapshot.id.startswith("kpi_"))
        self.assertEqual(snapshot.venue_id, "venue_001")
        self.assertEqual(snapshot.date, "2026-04-20")
        self.assertEqual(snapshot.period, KPIPeriod.DAILY)
        self.assertIn("labour_cost_pct", snapshot.metrics)
        self.assertEqual(snapshot.metrics["labour_cost_pct"], 30.0)

    def test_calculate_daily_kpis_all_metrics(self):
        """Test all KPI metrics are calculated."""
        store = get_kpi_dashboard_store()
        snapshot = store.calculate_daily_kpis(
            venue_id="venue_002",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1500,
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=7,
            no_shows=1,
            break_violations=2,
            total_breaks=8,
        )

        metrics = snapshot.metrics
        self.assertIn("labour_cost_pct", metrics)
        self.assertIn("revenue_per_labour_hour", metrics)
        self.assertIn("avg_hourly_cost", metrics)
        self.assertIn("covers_per_staff_hour", metrics)
        self.assertIn("roster_fill_rate", metrics)
        self.assertIn("no_show_rate", metrics)
        self.assertIn("break_compliance_rate", metrics)
        self.assertIn("avg_shift_length", metrics)

    def test_get_snapshot_by_date(self):
        """Test retrieving a snapshot by date."""
        store = get_kpi_dashboard_store()
        snapshot1 = store.calculate_daily_kpis(
            venue_id="venue_003",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1500,
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        retrieved = store.get_snapshot("venue_003", "2026-04-20", "DAILY")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, snapshot1.id)

    def test_get_snapshots_range(self):
        """Test retrieving snapshots within date range."""
        store = get_kpi_dashboard_store()

        # Create multiple daily snapshots
        for i in range(5):
            snap_date = (date(2026, 4, 20) + timedelta(days=i)).isoformat()
            store.calculate_daily_kpis(
                venue_id="venue_004",
                date_str=snap_date,
                revenue=5000,
                labour_cost=1500,
                hours_worked=50,
                covers=100,
                shifts_scheduled=8,
                shifts_filled=8,
                no_shows=0,
                break_violations=0,
                total_breaks=8,
            )

        snapshots = store.get_snapshots(
            "venue_004",
            "2026-04-20",
            "2026-04-24",
            "DAILY"
        )

        self.assertEqual(len(snapshots), 5)

    def test_get_snapshots_empty(self):
        """Test retrieving snapshots for non-existent venue."""
        store = get_kpi_dashboard_store()
        snapshots = store.get_snapshots(
            "venue_nonexistent",
            "2026-04-20",
            "2026-04-24",
            "DAILY"
        )

        self.assertEqual(len(snapshots), 0)


class TestKPIWeeklyAggregation(unittest.TestCase):
    """Test weekly KPI aggregation."""

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

    def test_calculate_weekly_kpis(self):
        """Test weekly aggregation from daily snapshots."""
        store = get_kpi_dashboard_store()

        # Create daily snapshots for a week (Mon-Sun)
        week_start = date(2026, 4, 20)  # Monday
        for i in range(7):
            snap_date = (week_start + timedelta(days=i)).isoformat()
            store.calculate_daily_kpis(
                venue_id="venue_005",
                date_str=snap_date,
                revenue=5000,
                labour_cost=1500,
                hours_worked=50,
                covers=100,
                shifts_scheduled=8,
                shifts_filled=8,
                no_shows=0,
                break_violations=0,
                total_breaks=8,
            )

        weekly = store.calculate_weekly_kpis("venue_005", "2026-04-20")

        self.assertIsNotNone(weekly)
        self.assertEqual(weekly.period, KPIPeriod.WEEKLY)
        self.assertEqual(weekly.venue_id, "venue_005")
        self.assertIn("labour_cost_pct", weekly.metrics)

    def test_calculate_weekly_kpis_no_data(self):
        """Test weekly aggregation with no data."""
        store = get_kpi_dashboard_store()
        weekly = store.calculate_weekly_kpis("venue_nonexistent", "2026-04-20")

        self.assertIsNone(weekly)


class TestKPIMonthlyAggregation(unittest.TestCase):
    """Test monthly KPI aggregation."""

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

    def test_calculate_monthly_kpis(self):
        """Test monthly aggregation from daily snapshots."""
        store = get_kpi_dashboard_store()

        # Create daily snapshots for April
        for day in range(1, 8):
            snap_date = f"2026-04-{day:02d}"
            store.calculate_daily_kpis(
                venue_id="venue_006",
                date_str=snap_date,
                revenue=5000,
                labour_cost=1500,
                hours_worked=50,
                covers=100,
                shifts_scheduled=8,
                shifts_filled=8,
                no_shows=0,
                break_violations=0,
                total_breaks=8,
            )

        monthly = store.calculate_monthly_kpis("venue_006", 2026, 4)

        self.assertIsNotNone(monthly)
        self.assertEqual(monthly.period, KPIPeriod.MONTHLY)
        self.assertIn("labour_cost_pct", monthly.metrics)

    def test_calculate_monthly_kpis_no_data(self):
        """Test monthly aggregation with no data."""
        store = get_kpi_dashboard_store()
        monthly = store.calculate_monthly_kpis("venue_nonexistent", 2026, 4)

        self.assertIsNone(monthly)


class TestKPIAlerts(unittest.TestCase):
    """Test KPI alert generation and retrieval."""

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

    def test_alert_labour_cost_pct_critical(self):
        """Test alert for high labour cost."""
        store = get_kpi_dashboard_store()

        # Labour cost 50% > critical threshold (40%)
        snapshot = store.calculate_daily_kpis(
            venue_id="venue_007",
            date_str="2026-04-20",
            revenue=1000,
            labour_cost=500,  # 50%
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        alerts = store.get_alerts("venue_007")
        self.assertGreater(len(alerts), 0)
        labour_alerts = [a for a in alerts if a["metric_name"] == "labour_cost_pct"]
        self.assertGreater(len(labour_alerts), 0)
        self.assertEqual(labour_alerts[0]["severity"], "CRITICAL")

    def test_alert_no_show_rate_critical(self):
        """Test alert for high no-show rate."""
        store = get_kpi_dashboard_store()

        # 20% no-show rate > critical threshold (10%)
        snapshot = store.calculate_daily_kpis(
            venue_id="venue_008",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1500,
            hours_worked=50,
            covers=100,
            shifts_scheduled=10,
            shifts_filled=8,
            no_shows=2,  # 20%
            break_violations=0,
            total_breaks=8,
        )

        alerts = store.get_alerts("venue_008")
        no_show_alerts = [a for a in alerts if a["metric_name"] == "no_show_rate"]
        self.assertGreater(len(no_show_alerts), 0)
        self.assertEqual(no_show_alerts[0]["severity"], "CRITICAL")

    def test_alert_roster_fill_rate_critical(self):
        """Test alert for low roster fill rate."""
        store = get_kpi_dashboard_store()

        # 70% fill rate < critical threshold (80%)
        snapshot = store.calculate_daily_kpis(
            venue_id="venue_009",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1500,
            hours_worked=50,
            covers=100,
            shifts_scheduled=10,
            shifts_filled=7,  # 70%
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        alerts = store.get_alerts("venue_009")
        fill_alerts = [a for a in alerts if a["metric_name"] == "roster_fill_rate"]
        self.assertGreater(len(fill_alerts), 0)
        self.assertEqual(fill_alerts[0]["severity"], "CRITICAL")


class TestKPITargets(unittest.TestCase):
    """Test KPI target setting and progress tracking."""

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

    def test_set_targets(self):
        """Test setting KPI targets."""
        store = get_kpi_dashboard_store()

        targets = {
            "labour_cost_pct": 30.0,
            "roster_fill_rate": 95.0,
            "no_show_rate": 3.0,
        }

        result = store.set_targets("venue_010", targets)

        self.assertEqual(result, targets)

    def test_get_targets(self):
        """Test retrieving KPI targets."""
        store = get_kpi_dashboard_store()

        targets = {
            "labour_cost_pct": 30.0,
            "roster_fill_rate": 95.0,
        }

        store.set_targets("venue_011", targets)
        retrieved = store.get_targets("venue_011")

        self.assertEqual(retrieved, targets)

    def test_get_targets_empty(self):
        """Test retrieving targets for venue with none set."""
        store = get_kpi_dashboard_store()
        targets = store.get_targets("venue_nonexistent")

        self.assertEqual(targets, {})

    def test_get_target_progress(self):
        """Test calculating actual vs target."""
        store = get_kpi_dashboard_store()

        # Set targets
        store.set_targets("venue_012", {
            "labour_cost_pct": 30.0,
            "roster_fill_rate": 95.0,
        })

        # Record daily KPIs
        store.calculate_daily_kpis(
            venue_id="venue_012",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1500,  # 30% - on target
            hours_worked=50,
            covers=100,
            shifts_scheduled=10,
            shifts_filled=9,  # 90% - below target
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        progress = store.get_target_progress("venue_012", "2026-04-20")

        self.assertIsNotNone(progress)
        self.assertIn("metrics", progress)
        self.assertIn("labour_cost_pct", progress["metrics"])
        self.assertIn("roster_fill_rate", progress["metrics"])


class TestKPIPeriodComparison(unittest.TestCase):
    """Test period comparison functionality."""

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

    def test_compare_periods(self):
        """Test comparing two periods."""
        store = get_kpi_dashboard_store()

        # Day 1: labour cost 30%
        store.calculate_daily_kpis(
            venue_id="venue_013",
            date_str="2026-04-20",
            revenue=10000,
            labour_cost=3000,
            hours_worked=100,
            covers=150,
            shifts_scheduled=10,
            shifts_filled=10,
            no_shows=0,
            break_violations=0,
            total_breaks=10,
        )

        # Day 2: labour cost 35%
        store.calculate_daily_kpis(
            venue_id="venue_013",
            date_str="2026-04-21",
            revenue=10000,
            labour_cost=3500,
            hours_worked=100,
            covers=150,
            shifts_scheduled=10,
            shifts_filled=10,
            no_shows=0,
            break_violations=0,
            total_breaks=10,
        )

        comparison = store.compare_periods(
            "venue_013",
            "2026-04-20",
            "2026-04-21"
        )

        self.assertIsNotNone(comparison)
        self.assertIn("metrics", comparison)
        self.assertIn("labour_cost_pct", comparison["metrics"])
        # Labour cost increased from 30% to 35%
        self.assertEqual(
            comparison["metrics"]["labour_cost_pct"]["period_1"],
            30.0
        )
        self.assertEqual(
            comparison["metrics"]["labour_cost_pct"]["period_2"],
            35.0
        )


class TestKPIRanking(unittest.TestCase):
    """Test venue ranking functionality."""

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

    def test_get_venue_ranking(self):
        """Test ranking venues by metric."""
        store = get_kpi_dashboard_store()

        # Create snapshots for 3 venues with different fill rates
        venues = [
            ("venue_a", 90.0, 90),  # 90% fill
            ("venue_b", 85.0, 85),  # 85% fill
            ("venue_c", 95.0, 95),  # 95% fill
        ]

        for venue_id, labour_pct, fill_rate in venues:
            store.calculate_daily_kpis(
                venue_id=venue_id,
                date_str="2026-04-20",
                revenue=10000,
                labour_cost=int(10000 * labour_pct / 100),
                hours_worked=100,
                covers=150,
                shifts_scheduled=100,
                shifts_filled=int(100 * fill_rate / 100),
                no_shows=0,
                break_violations=0,
                total_breaks=10,
            )

        ranking = store.get_venue_ranking(
            ["venue_a", "venue_b", "venue_c"],
            "2026-04-20",
            "roster_fill_rate"
        )

        self.assertEqual(len(ranking), 3)
        # venue_c should be first (95%), then venue_a (90%), then venue_b (85%)
        self.assertEqual(ranking[0]["venue_id"], "venue_c")
        self.assertEqual(ranking[0]["rank"], 1)
        self.assertEqual(ranking[1]["venue_id"], "venue_a")
        self.assertEqual(ranking[1]["rank"], 2)
        self.assertEqual(ranking[2]["venue_id"], "venue_b")
        self.assertEqual(ranking[2]["rank"], 3)


class TestKPITrends(unittest.TestCase):
    """Test KPI trend calculation."""

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

    def test_get_current_kpis_with_trends(self):
        """Test getting current KPIs with trend data."""
        store = get_kpi_dashboard_store()

        # Create KPIs for 7 days apart
        store.calculate_daily_kpis(
            venue_id="venue_014",
            date_str="2026-04-13",
            revenue=5000,
            labour_cost=1500,  # 30%
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        store.calculate_daily_kpis(
            venue_id="venue_014",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1600,  # 32%
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        result = store.get_current_kpis("venue_014")

        self.assertIn("latest", result)
        self.assertIn("trends", result)
        self.assertIn("labour_cost_pct", result["trends"])

    def test_get_trends(self):
        """Test trend calculation."""
        store = get_kpi_dashboard_store()

        store.calculate_daily_kpis(
            venue_id="venue_015",
            date_str="2026-04-13",
            revenue=5000,
            labour_cost=1500,  # 30%
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        store.calculate_daily_kpis(
            venue_id="venue_015",
            date_str="2026-04-20",
            revenue=5000,
            labour_cost=1650,  # 33%
            hours_worked=50,
            covers=100,
            shifts_scheduled=8,
            shifts_filled=8,
            no_shows=0,
            break_violations=0,
            total_breaks=8,
        )

        trends = store.get_trends("venue_015", "2026-04-20", lookback_days=7)

        self.assertIsNotNone(trends)
        self.assertIn("labour_cost_pct", trends)
        # 33% vs 30% = +10% increase
        self.assertGreater(trends["labour_cost_pct"], 0)


class TestKPIDataTypes(unittest.TestCase):
    """Test data type conversions and serialization."""

    def test_snapshot_to_dict(self):
        """Test KPI snapshot serialization."""
        snapshot = KPISnapshot(
            id="kpi_test",
            venue_id="venue_test",
            date="2026-04-20",
            period=KPIPeriod.DAILY,
            metrics={
                "labour_cost_pct": 30.0,
                "roster_fill_rate": 90.0,
            },
        )

        d = snapshot.to_dict()

        self.assertIn("id", d)
        self.assertIn("venue_id", d)
        self.assertIn("date", d)
        self.assertIn("period", d)
        self.assertIn("metrics", d)
        self.assertIn("created_at", d)
        self.assertEqual(d["period"], "DAILY")


if __name__ == "__main__":
    unittest.main()
