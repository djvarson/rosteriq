"""Tests for tanda_history.py — stdlib-only."""

from __future__ import annotations

import asyncio
import unittest
from datetime import date, datetime, time, timedelta, timezone

from rosteriq.tanda_history import (
    DailyActuals,
    HourlyActuals,
    TandaHistoryIngestor,
    TandaHistoryStore,
    _shift_hour_buckets,
    _shift_hours,
    variance_summary,
)


class _Shift:
    def __init__(self, employee_id, d, start, end, break_minutes=0):
        self.id = "s" + employee_id + d.isoformat()
        self.employee_id = employee_id
        self.date = d
        self.start_time = start
        self.end_time = end
        self.break_minutes = break_minutes


class _Timesheet:
    def __init__(self, employee_id, d, hours, shifts=None):
        self.id = "t" + employee_id + d.isoformat()
        self.employee_id = employee_id
        self.date = d
        self.hours = hours
        self.shifts = shifts or []


class _Forecast:
    def __init__(self, dt, forecast, actual=0.0):
        self.datetime = dt
        self.forecast = forecast
        self.actual = actual


class _Employee:
    def __init__(self, eid, rate):
        self.id = eid
        self.hourly_rate = rate


class _StubAdapter:
    def __init__(self, shifts, timesheets, forecasts, employees):
        self.shifts = shifts
        self.timesheets = timesheets
        self.forecasts = forecasts
        self.employees = employees

    async def get_employees(self, org_id):
        return self.employees

    async def get_shifts(self, org_id, date_range):
        s, e = date_range
        return [sh for sh in self.shifts if s <= sh.date <= e]

    async def get_timesheets(self, org_id, date_range):
        s, e = date_range
        return [t for t in self.timesheets if s <= t.date <= e]

    async def get_forecast_revenue(self, org_id, date_range):
        s, e = date_range
        return [f for f in self.forecasts if s <= f.datetime.date() <= e]


class ShiftMathTest(unittest.TestCase):
    def test_basic_hours(self):
        self.assertEqual(_shift_hours(time(9, 0), time(17, 0)), 8.0)

    def test_break_subtracted(self):
        self.assertEqual(_shift_hours(time(9, 0), time(17, 0), 30), 7.5)

    def test_overnight(self):
        # 22:00 -> 06:00 = 8h
        self.assertEqual(_shift_hours(time(22, 0), time(6, 0)), 8.0)

    def test_buckets_simple(self):
        self.assertEqual(_shift_hour_buckets(time(9, 0), time(13, 0)), [9, 10, 11, 12, 13])

    def test_buckets_partial_end(self):
        self.assertEqual(_shift_hour_buckets(time(9, 30), time(12, 30)), [9, 10, 11, 12, 13])


class StoreTest(unittest.TestCase):
    def test_upsert_and_range(self):
        store = TandaHistoryStore()
        d = date(2026, 4, 10)
        store.upsert_daily(DailyActuals(venue_id="v1", day=d, rostered_hours=8.0))
        rows = store.daily_range("v1", d, d)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rostered_hours, 8.0)

    def test_hourly_for_day(self):
        store = TandaHistoryStore()
        d = date(2026, 4, 10)
        store.upsert_hourly(HourlyActuals(venue_id="v1", day=d, hour=9, rostered_heads=2))
        store.upsert_hourly(HourlyActuals(venue_id="v1", day=d, hour=10, rostered_heads=3))
        store.upsert_hourly(HourlyActuals(venue_id="v1", day=date(2026, 4, 11), hour=9))
        rows = store.hourly_for_day("v1", d)
        self.assertEqual(len(rows), 2)

    def test_labour_pct(self):
        agg = DailyActuals(
            venue_id="v1", day=date.today(),
            worked_cost=600.0, actual_revenue=2000.0,
        )
        self.assertAlmostEqual(agg.labour_pct, 30.0)

    def test_labour_pct_no_revenue(self):
        agg = DailyActuals(venue_id="v1", day=date.today(), worked_cost=600.0)
        self.assertIsNone(agg.labour_pct)


class IngestTest(unittest.TestCase):
    def setUp(self):
        self.day = date(2026, 4, 10)
        self.store = TandaHistoryStore()

    def _run(self, coro):
        return asyncio.new_event_loop().run_until_complete(coro)

    def test_ingest_rolls_up_correctly(self):
        shifts = [
            _Shift("e1", self.day, time(9, 0), time(17, 0)),
            _Shift("e2", self.day, time(12, 0), time(20, 0)),
        ]
        timesheets = [
            _Timesheet("e1", self.day, 8.0),
            _Timesheet("e2", self.day, 9.0),  # worked 1h over
        ]
        forecasts = [
            _Forecast(datetime(2026, 4, 10, 12, 0), 500.0, actual=620.0),
        ]
        employees = [_Employee("e1", 30.0), _Employee("e2", 40.0)]
        adapter = _StubAdapter(shifts, timesheets, forecasts, employees)
        ingestor = TandaHistoryIngestor(adapter=adapter, store=self.store)

        summary = self._run(ingestor.ingest_range("v1", "org1", self.day, self.day))

        self.assertEqual(summary["shift_count"], 2)
        self.assertEqual(summary["rostered_hours"], 16.0)
        self.assertEqual(summary["timesheet_hours"], 17.0)
        self.assertEqual(summary["actual_revenue"], 620.0)

        rows = self.store.daily_range("v1", self.day, self.day)
        self.assertEqual(len(rows), 1)
        agg = rows[0]
        self.assertEqual(agg.rostered_cost, 8 * 30 + 8 * 40)  # 560
        self.assertEqual(agg.worked_cost, 8 * 30 + 9 * 40)    # 600
        self.assertEqual(agg.employee_count, 2)
        self.assertEqual(agg.variance_hours, 1.0)

        # Hourly buckets should exist
        hourly = self.store.hourly_for_day("v1", self.day)
        self.assertGreater(len(hourly), 0)

    def test_ingest_handles_adapter_failures(self):
        class _BrokenAdapter:
            async def get_employees(self, *a, **k):
                raise RuntimeError("boom")
            async def get_shifts(self, *a, **k):
                raise RuntimeError("boom")
            async def get_timesheets(self, *a, **k):
                raise RuntimeError("boom")
            async def get_forecast_revenue(self, *a, **k):
                raise RuntimeError("boom")

        ingestor = TandaHistoryIngestor(adapter=_BrokenAdapter(), store=self.store)
        summary = self._run(ingestor.ingest_range("v1", "org1", self.day, self.day))
        # Empty but not crashed
        self.assertEqual(summary["shift_count"], 0)
        self.assertEqual(summary["timesheet_hours"], 0.0)

    def test_ingest_invalid_range(self):
        adapter = _StubAdapter([], [], [], [])
        ingestor = TandaHistoryIngestor(adapter=adapter, store=self.store)
        with self.assertRaises(ValueError):
            self._run(ingestor.ingest_range("v1", "org1", self.day, self.day - timedelta(days=1)))


class VarianceSummaryTest(unittest.TestCase):
    def test_no_data(self):
        store = TandaHistoryStore()
        out = variance_summary("v_unknown", days=7, store=store)
        self.assertEqual(out["rows"], 0)

    def test_with_data(self):
        store = TandaHistoryStore()
        today = date.today()
        store.upsert_daily(DailyActuals(
            venue_id="v1", day=today,
            rostered_hours=40.0, worked_hours=42.0,
            worked_cost=1200.0, actual_revenue=4000.0,
        ))
        out = variance_summary("v1", days=7, store=store)
        self.assertEqual(out["rows"], 1)
        self.assertEqual(out["variance_hours"], 2.0)
        self.assertEqual(out["labour_pct"], 30.0)


if __name__ == "__main__":
    unittest.main()
