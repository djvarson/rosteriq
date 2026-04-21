"""Tests for POS Sales Curve Forecaster module."""

import unittest
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.sales_curve import (
    SalesCurveStore, get_sales_curve_store, _reset_for_tests,
    DAYS_OF_WEEK,
)


def _seed_tuesday_data(store, venue_id="v1", weeks=4):
    """Seed hourly data for N Tuesdays (weekday=1)."""
    today = date.today()
    # Find the most recent Tuesday
    days_since_tue = (today.weekday() - 1) % 7
    last_tue = today - timedelta(days=days_since_tue)

    for w in range(weeks):
        d = last_tue - timedelta(weeks=w)
        for hour in range(10, 23):  # 10am-10pm
            base_rev = 200 + (hour - 10) * 100  # ramps up through day
            if hour >= 18:  # dinner peak
                base_rev = 1200 + (hour - 18) * 50
            if hour >= 21:  # drops off
                base_rev = 800 - (hour - 21) * 200

            # Add some variance per week
            variance = (w * 30) - 60
            revenue = max(0, base_rev + variance)

            store.add_hourly_record({
                "venue_id": venue_id,
                "date": d.isoformat(),
                "hour": hour,
                "revenue": revenue,
                "transaction_count": int(revenue / 25),
                "covers": int(revenue / 30),
                "source": "pos_test",
            })


class TestHourlyRecordIngest(unittest.TestCase):
    """Tests for POS data ingestion."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_sales_curve_store()

    def test_add_hourly_record(self):
        rec = self.store.add_hourly_record({
            "venue_id": "v1",
            "date": "2026-04-20",
            "hour": 18,
            "revenue": 1500.00,
            "transaction_count": 60,
            "covers": 45,
            "source": "pos_swiftpos",
        })
        self.assertEqual(rec.venue_id, "v1")
        self.assertEqual(rec.hour, 18)
        self.assertEqual(rec.revenue, 1500.00)
        self.assertEqual(rec.avg_transaction, 25.00)

    def test_add_hourly_record_zero_transactions(self):
        rec = self.store.add_hourly_record({
            "venue_id": "v1",
            "date": "2026-04-20",
            "hour": 6,
            "revenue": 0,
            "transaction_count": 0,
            "covers": 0,
        })
        self.assertEqual(rec.avg_transaction, 0)

    def test_ingest_daily_pos(self):
        hourly = [
            {"hour": 10, "revenue": 200, "transaction_count": 8, "covers": 6},
            {"hour": 11, "revenue": 400, "transaction_count": 16, "covers": 12},
            {"hour": 12, "revenue": 800, "transaction_count": 32, "covers": 24},
        ]
        count = self.store.ingest_daily_pos("v1", "2026-04-20", hourly)
        self.assertEqual(count, 3)
        records = self.store.get_records("v1")
        self.assertEqual(len(records), 3)

    def test_bulk_ingest(self):
        records = [
            {"venue_id": "v1", "date": "2026-04-20", "hour": 10, "revenue": 100},
            {"venue_id": "v1", "date": "2026-04-20", "hour": 11, "revenue": 200},
        ]
        count = self.store.bulk_ingest(records)
        self.assertEqual(count, 2)

    def test_get_records_filters(self):
        self.store.add_hourly_record({"venue_id": "v1", "date": "2026-04-18",
                                      "hour": 10, "revenue": 100})
        self.store.add_hourly_record({"venue_id": "v1", "date": "2026-04-19",
                                      "hour": 10, "revenue": 200})
        self.store.add_hourly_record({"venue_id": "v1", "date": "2026-04-19",
                                      "hour": 18, "revenue": 1000})
        self.store.add_hourly_record({"venue_id": "v2", "date": "2026-04-19",
                                      "hour": 10, "revenue": 500})

        # Venue filter
        v1 = self.store.get_records("v1")
        self.assertEqual(len(v1), 3)

        # Date filter
        apr19 = self.store.get_records("v1", date_from="2026-04-19")
        self.assertEqual(len(apr19), 2)

        # Hour filter
        h10 = self.store.get_records("v1", hour=10)
        self.assertEqual(len(h10), 2)

    def test_get_daily_total(self):
        self.store.add_hourly_record({"venue_id": "v1", "date": "2026-04-20",
                                      "hour": 10, "revenue": 500,
                                      "covers": 15, "transaction_count": 20})
        self.store.add_hourly_record({"venue_id": "v1", "date": "2026-04-20",
                                      "hour": 11, "revenue": 800,
                                      "covers": 25, "transaction_count": 32})
        daily = self.store.get_daily_total("v1", "2026-04-20")
        self.assertEqual(daily["total_revenue"], 1300.0)
        self.assertEqual(daily["total_covers"], 40)
        self.assertEqual(daily["hours_with_data"], 2)


class TestSalesCurveGeneration(unittest.TestCase):
    """Tests for sales curve building."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_sales_curve_store()
        _seed_tuesday_data(self.store, weeks=4)

    def test_build_day_of_week_curve(self):
        curve = self.store.build_day_of_week_curve("v1", 1)  # Tuesday
        self.assertEqual(curve.venue_id, "v1")
        self.assertEqual(curve.day_of_week, 1)
        self.assertEqual(curve.label, "Tuesday")
        self.assertEqual(curve.sample_count, 4)
        self.assertGreater(curve.total_avg_revenue, 0)
        self.assertGreater(curve.peak_revenue, 0)
        self.assertIn(curve.peak_hour, range(24))

    def test_curve_has_24_hours(self):
        curve = self.store.build_day_of_week_curve("v1", 1)
        self.assertEqual(len(curve.hourly_profile), 24)

    def test_peak_hour_detection(self):
        curve = self.store.build_day_of_week_curve("v1", 1)
        # Based on seed data, peak should be around 18-20 (dinner)
        self.assertGreaterEqual(curve.peak_hour, 17)
        self.assertLessEqual(curve.peak_hour, 21)

    def test_quiet_hours_detection(self):
        curve = self.store.build_day_of_week_curve("v1", 1)
        # Hours before 10am should be quiet (no data)
        for h in range(0, 10):
            self.assertIn(h, curve.quiet_hours)

    def test_confidence_increases_with_samples(self):
        _reset_for_tests()
        store = get_sales_curve_store()
        _seed_tuesday_data(store, weeks=2)
        curve_2w = store.build_day_of_week_curve("v1", 1)

        _reset_for_tests()
        store = get_sales_curve_store()
        _seed_tuesday_data(store, weeks=8)
        curve_8w = store.build_day_of_week_curve("v1", 1)

        self.assertGreater(curve_8w.confidence, curve_2w.confidence)

    def test_empty_day_curve(self):
        """Curve for a day with no data should return zeros."""
        curve = self.store.build_day_of_week_curve("v1", 0)  # Monday (no data)
        self.assertEqual(curve.sample_count, 0)
        self.assertEqual(curve.total_avg_revenue, 0)

    def test_build_weekly_curves(self):
        curves = self.store.build_weekly_curves("v1")
        self.assertEqual(len(curves), 7)
        # Only Tuesday should have data
        tuesday_curve = curves[1]
        self.assertGreater(tuesday_curve.sample_count, 0)

    def test_build_custom_curve(self):
        today = date.today()
        four_weeks_ago = today - timedelta(weeks=4)
        curve = self.store.build_custom_curve(
            "v1", four_weeks_ago.isoformat(), today.isoformat())
        self.assertGreater(curve.sample_count, 0)
        self.assertIsNone(curve.day_of_week)

    def test_curve_to_dict(self):
        curve = self.store.build_day_of_week_curve("v1", 1)
        d = curve.to_dict()
        self.assertIn("hourly_profile", d)
        self.assertIn("peak_hour", d)
        self.assertIn("confidence", d)
        self.assertIn("total_avg_revenue", d)

    def test_venue_isolation(self):
        _seed_tuesday_data(self.store, venue_id="v2", weeks=2)
        curve_v1 = self.store.build_day_of_week_curve("v1", 1)
        curve_v2 = self.store.build_day_of_week_curve("v2", 1)
        self.assertEqual(curve_v1.sample_count, 4)
        self.assertEqual(curve_v2.sample_count, 2)


class TestStaffingRecommendations(unittest.TestCase):
    """Tests for dollar-backed staffing recommendations."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_sales_curve_store()
        _seed_tuesday_data(self.store, weeks=4)

    def _find_next_tuesday(self):
        today = date.today()
        days_ahead = (1 - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return (today + timedelta(days=days_ahead)).isoformat()

    def test_recommend_staffing(self):
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue)
        self.assertEqual(len(recs), 24)  # all hours

        # Peak hours should have more staff
        dinner_rec = [r for r in recs if r.hour == 19][0]
        morning_rec = [r for r in recs if r.hour == 6][0]
        self.assertGreaterEqual(dinner_rec.recommended_staff,
                                morning_rec.recommended_staff)

    def test_recommend_specific_hours(self):
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue, hours=[18, 19, 20])
        self.assertEqual(len(recs), 3)

    def test_custom_targets(self):
        self.store.set_targets("v1", {
            "revenue_per_staff_hour": 150.0,  # lower target = more staff
            "min_staff": 3,
            "max_staff": 20,
        })
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue, hours=[19])
        self.assertGreaterEqual(recs[0].recommended_staff, 3)

    def test_min_staff_floor(self):
        """Hours with no revenue should still get minimum staff."""
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue, hours=[4])  # 4am
        self.assertEqual(recs[0].recommended_staff, 2)  # default min

    def test_max_staff_cap(self):
        self.store.set_targets("v1", {
            "revenue_per_staff_hour": 50.0,  # very low = lots of staff
            "max_staff": 10,
        })
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue)
        for r in recs:
            self.assertLessEqual(r.recommended_staff, 10)

    def test_get_daily_staffing_plan(self):
        next_tue = self._find_next_tuesday()
        plan = self.store.get_daily_staffing_plan(
            "v1", next_tue, operating_hours=(10, 22))
        self.assertIn("hourly_plan", plan)
        self.assertEqual(len(plan["hourly_plan"]), 13)  # 10-22 inclusive
        self.assertIn("summary", plan)
        self.assertGreater(plan["summary"]["total_staff_hours"], 0)

    def test_recommendation_has_reasoning(self):
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue, hours=[19])
        self.assertIn("Predicted", recs[0].reasoning)

    def test_recommendation_to_dict(self):
        next_tue = self._find_next_tuesday()
        recs = self.store.recommend_staffing("v1", next_tue, hours=[18])
        d = recs[0].to_dict()
        self.assertIn("predicted_revenue", d)
        self.assertIn("recommended_staff", d)
        self.assertIn("reasoning", d)


class TestTargets(unittest.TestCase):
    """Tests for staffing targets."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_sales_curve_store()

    def test_default_targets(self):
        targets = self.store.get_targets("v1")
        self.assertEqual(targets["revenue_per_staff_hour"], 300.0)
        self.assertEqual(targets["min_staff"], 2)
        self.assertEqual(targets["max_staff"], 15)

    def test_set_targets(self):
        self.store.set_targets("v1", {
            "revenue_per_staff_hour": 250.0,
            "min_staff": 3,
        })
        targets = self.store.get_targets("v1")
        self.assertEqual(targets["revenue_per_staff_hour"], 250.0)
        self.assertEqual(targets["min_staff"], 3)
        # Defaults for unset
        self.assertEqual(targets["max_staff"], 15)

    def test_venue_isolation(self):
        self.store.set_targets("v1", {"min_staff": 5})
        self.store.set_targets("v2", {"min_staff": 3})
        self.assertEqual(self.store.get_targets("v1")["min_staff"], 5)
        self.assertEqual(self.store.get_targets("v2")["min_staff"], 3)


class TestTrendAnalysis(unittest.TestCase):
    """Tests for revenue trend analysis."""

    def setUp(self):
        _reset_for_tests()
        self.store = get_sales_curve_store()
        _seed_tuesday_data(self.store, weeks=4)

    def test_weekly_revenue_trend(self):
        trend = self.store.get_weekly_revenue_trend("v1", weeks=4)
        self.assertGreater(len(trend), 0)
        for week in trend:
            self.assertIn("week_start", week)
            self.assertIn("total_revenue", week)
            self.assertIn("avg_daily_revenue", week)

    def test_hour_comparison(self):
        result = self.store.get_hour_comparison("v1", hour=18, weeks=4)
        self.assertEqual(result["hour"], 18)
        self.assertIn("avg_revenue", result)

    def test_empty_trend(self):
        trend = self.store.get_weekly_revenue_trend("v_empty", weeks=4)
        self.assertIsInstance(trend, list)
        for week in trend:
            self.assertEqual(week["total_revenue"], 0)


class TestStoreReset(unittest.TestCase):
    """Tests for store reset and singleton."""

    def test_reset_clears_data(self):
        _reset_for_tests()
        store = get_sales_curve_store()
        store.add_hourly_record({"venue_id": "v1", "date": "2026-04-20",
                                 "hour": 10, "revenue": 100})
        self.assertEqual(len(store.get_records("v1")), 1)

        _reset_for_tests()
        store = get_sales_curve_store()
        self.assertEqual(len(store.get_records("v1")), 0)

    def test_singleton(self):
        _reset_for_tests()
        s1 = get_sales_curve_store()
        s2 = get_sales_curve_store()
        self.assertIs(s1, s2)


if __name__ == "__main__":
    unittest.main()
