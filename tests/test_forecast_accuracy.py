"""Tests for forecast accuracy reporting (Round 13)."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from rosteriq.forecast_accuracy import (
    _bias,
    _direction,
    _mape,
    _pct_err,
    _rolling_mape,
    _to_rows,
    build_accuracy_report,
)
from rosteriq.tanda_history import DailyActuals, TandaHistoryStore


def _make_daily(day: date, forecast: float, actual: float,
                rostered: float = 40.0, worked: float = 40.0) -> DailyActuals:
    return DailyActuals(
        venue_id="v-accuracy-test",
        day=day,
        rostered_hours=rostered,
        worked_hours=worked,
        forecast_revenue=forecast,
        actual_revenue=actual,
    )


class PctErrTests(unittest.TestCase):
    def test_positive_when_overforecast(self):
        self.assertAlmostEqual(_pct_err(120, 100), 20.0)

    def test_negative_when_underforecast(self):
        self.assertAlmostEqual(_pct_err(80, 100), -20.0)

    def test_none_when_actual_zero(self):
        self.assertIsNone(_pct_err(100, 0))

    def test_none_when_actual_negative(self):
        self.assertIsNone(_pct_err(100, -5))


class MetricsTests(unittest.TestCase):
    def setUp(self):
        today = date.today()
        self.rows = _to_rows([
            _make_daily(today - timedelta(days=3), 120, 100),  # +20%
            _make_daily(today - timedelta(days=2), 90, 100),   # -10%
            _make_daily(today - timedelta(days=1), 110, 100),  # +10%
            _make_daily(today, 100, 0),                        # unscoreable
        ])

    def test_mape_average_of_abs_errs(self):
        self.assertAlmostEqual(_mape(self.rows), round((20 + 10 + 10) / 3, 2))

    def test_bias_signed_mean(self):
        self.assertAlmostEqual(_bias(self.rows), round((20 - 10 + 10) / 3, 2))

    def test_direction_over_forecast(self):
        # bias > 5 -> over_forecasting
        self.assertEqual(_direction(20.0), "over_forecasting")

    def test_direction_under_forecast(self):
        self.assertEqual(_direction(-15.0), "under_forecasting")

    def test_direction_on_target(self):
        self.assertEqual(_direction(2.0), "on_target")

    def test_direction_no_data(self):
        self.assertEqual(_direction(None), "no_data")

    def test_rolling_mape_window(self):
        trend = _rolling_mape(self.rows, window=2)
        self.assertEqual(len(trend), 4)
        # First row: just one scoreable (abs 20) → 20
        self.assertEqual(trend[0]["rolling_mape"], 20.0)


class EndToEndReportTests(unittest.TestCase):
    def setUp(self):
        self.store = TandaHistoryStore()
        today = date.today()
        for i, (fc, act) in enumerate(
            [(120, 100), (90, 100), (110, 100), (105, 100)]
        ):
            self.store.upsert_daily(
                _make_daily(today - timedelta(days=3 - i), fc, act)
            )

    def test_full_report_shape(self):
        report = build_accuracy_report(
            venue_id="v-accuracy-test",
            days=7,
            store=self.store,
        )
        self.assertEqual(report["venue_id"], "v-accuracy-test")
        self.assertEqual(report["rows_total"], 4)
        self.assertEqual(report["rows_scoreable"], 4)
        self.assertIsNotNone(report["mape"])
        self.assertIsNotNone(report["bias"])
        self.assertIn(report["direction"], {"over_forecasting", "on_target", "under_forecasting"})
        self.assertEqual(len(report["rolling"]), 4)
        self.assertIn("worst", report["extremes"])
        self.assertIn("best", report["extremes"])

    def test_report_empty_when_no_data(self):
        report = build_accuracy_report(
            venue_id="no-such-venue",
            days=7,
            store=self.store,
        )
        self.assertEqual(report["rows_total"], 0)
        self.assertIsNone(report["mape"])
        self.assertEqual(report["direction"], "no_data")


if __name__ == "__main__":
    unittest.main()
