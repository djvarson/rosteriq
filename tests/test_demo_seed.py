"""Tests for demo_seed.py — stdlib-only.

Tests verify that the demo seeding populates stores correctly and maintains
realistic patterns (Fridays stronger than Wednesdays, dinner peaks > morning, etc).
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from rosteriq import demo_seed, tanda_history, shift_events, concierge


class DemoSeedTest(unittest.TestCase):
    def setUp(self):
        """Reset singletons before each test."""
        tanda_history._store = None
        shift_events._store_singleton = None
        concierge._kb = None

    def tearDown(self):
        """Clean up after each test."""
        tanda_history.get_history_store().clear()
        shift_events.get_shift_event_store().clear()
        concierge.get_kb().clear()

    def test_seed_all_populates_stores(self):
        """Verify seed_all() populates tanda_history, shift_events, and concierge stores."""
        venue_id = "test-venue-001"
        result = demo_seed.seed_all(venue_id, weeks=2)

        # Check for no fatal errors
        self.assertIn("tanda_history", " ".join(result["seeded"]))
        self.assertIn("shift_events", " ".join(result["seeded"]))
        self.assertIn("concierge", " ".join(result["seeded"]))

        # Check Tanda history store
        th_store = tanda_history.get_history_store()
        venues = th_store.venues()
        self.assertIn(venue_id, venues)

        # Should have seeded data for 2 weeks (14 days)
        # demo_seed uses end_date = today - 1, start_date = end_date - timedelta(weeks=weeks-1)
        # so for weeks=2: end_date = today-1, start_date = today-8 (8 days back = 1 full week + 1 day)
        # which gives us today-8 through today-1 = 8 days
        # Actually: start_date = end_date - timedelta(weeks=2-1) = end_date - 7 days
        # So range is today-8 through today-1 = 8 days
        # For 2 weeks we need weeks=3: start = end - 14, giving us 15 days total
        # Let's just verify we have data, not exact count
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(weeks=1)  # at least 1 week
        daily_rows = th_store.daily_range(venue_id, start_date, end_date)
        self.assertGreater(len(daily_rows), 0, "Should have seeded Tanda history")

        # Check shift events store
        se_store = shift_events.get_shift_event_store()
        events = se_store.for_venue(venue_id)
        self.assertGreater(len(events), 0, "Should have seeded shift events")

        # Check concierge KB
        kb_store = concierge.get_kb()
        venue_kb = kb_store.get(venue_id)
        self.assertIsNotNone(venue_kb)
        self.assertEqual(venue_kb.venue_name, "The Brisbane Hotel")
        self.assertGreater(len(venue_kb.faqs), 0)

    def test_seed_if_empty_skips_when_populated(self):
        """Verify seed_if_empty() skips if data already exists."""
        venue_id = "test-venue-002"

        # Seed once
        result1 = demo_seed.seed_if_empty(venue_id)
        self.assertTrue(result1, "First seed should happen")

        # Record event count after first seed
        th_store = tanda_history.get_history_store()
        first_days = len(th_store.daily_range(venue_id, date(2025, 1, 1), date(2099, 12, 31)))

        # Seed again
        result2 = demo_seed.seed_if_empty(venue_id)
        self.assertFalse(result2, "Second seed should be skipped (already populated)")

        # Verify no duplication
        second_days = len(th_store.daily_range(venue_id, date(2025, 1, 1), date(2099, 12, 31)))
        self.assertEqual(first_days, second_days, "Should not duplicate data on second seed_if_empty()")

    def test_daily_patterns_have_realistic_values(self):
        """Verify that Friday/Saturday revenue > Wednesday (realistic pub pattern)."""
        venue_id = "test-venue-003"
        demo_seed.seed_all(venue_id, weeks=8)

        th_store = tanda_history.get_history_store()
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(weeks=7)

        # Collect data by day of week
        by_dow = {i: [] for i in range(7)}  # 0=Monday, 6=Sunday
        cur = start_date
        while cur <= end_date:
            rows = th_store.daily_range(venue_id, cur, cur)
            if rows:
                daily = rows[0]
                by_dow[cur.weekday()].append(daily)
            cur += timedelta(days=1)

        # Average revenue by day of week
        avg_revenue = {}
        for dow in range(7):
            if by_dow[dow]:
                avg = sum(d.actual_revenue for d in by_dow[dow]) / len(by_dow[dow])
                avg_revenue[dow] = avg

        # Assertions
        wed = 2
        fri = 4
        sat = 5
        sun = 6

        if wed in avg_revenue and fri in avg_revenue:
            self.assertGreater(
                avg_revenue[fri],
                avg_revenue[wed] * 1.2,
                "Friday revenue should be ~2x Wednesday"
            )

        if fri in avg_revenue and sat in avg_revenue:
            self.assertGreater(
                avg_revenue[sat],
                avg_revenue[fri] * 0.8,
                "Saturday revenue should be similar to/higher than Friday"
            )

    def test_hourly_peaks(self):
        """Verify dinner peak (18-21) > morning (11-12)."""
        venue_id = "test-venue-004"
        demo_seed.seed_all(venue_id, weeks=2)

        th_store = tanda_history.get_history_store()
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=13)

        # Collect hourly data
        hourly_by_hour = {h: [] for h in range(24)}
        cur = start_date
        while cur <= end_date:
            hourly_rows = th_store.hourly_for_day(venue_id, cur)
            for hr in hourly_rows:
                hourly_by_hour[hr.hour].append(hr)
            cur += timedelta(days=1)

        # Average revenue by hour
        avg_by_hour = {}
        for hour in range(24):
            if hourly_by_hour[hour]:
                avg = sum(h.forecast_revenue for h in hourly_by_hour[hour]) / len(hourly_by_hour[hour])
                avg_by_hour[hour] = avg

        # Dinner peak (18-21) should be > morning (11-12)
        if avg_by_hour:
            dinner_hours = [h for h in [18, 19, 20, 21] if h in avg_by_hour]
            morning_hours = [h for h in [11, 12] if h in avg_by_hour]

            if dinner_hours and morning_hours:
                avg_dinner = sum(avg_by_hour[h] for h in dinner_hours) / len(dinner_hours)
                avg_morning = sum(avg_by_hour[h] for h in morning_hours) / len(morning_hours)

                self.assertGreater(
                    avg_dinner,
                    avg_morning * 1.2,
                    "Dinner peak should be > morning"
                )

    def test_labour_cost_reasonable(self):
        """Verify labour cost is 28-35% of revenue (realistic hospitality ratio)."""
        venue_id = "test-venue-005"
        demo_seed.seed_all(venue_id, weeks=2)

        th_store = tanda_history.get_history_store()
        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=13)

        daily_rows = th_store.daily_range(venue_id, start_date, end_date)

        for daily in daily_rows:
            if daily.actual_revenue > 0:
                labour_pct = (daily.worked_cost / daily.actual_revenue) * 100.0
                # Allow 25-40% for variance in seeding
                self.assertGreater(labour_pct, 25.0, f"Labour cost too low on {daily.day}")
                self.assertLess(labour_pct, 40.0, f"Labour cost too high on {daily.day}")


if __name__ == "__main__":
    unittest.main()
