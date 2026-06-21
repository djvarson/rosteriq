"""
Tests for forecast accuracy measured against REAL observed actuals.

The accuracy service used to reconstruct "actuals" from rosters (covers =
shift.net_hours / 0.04). Rosters are built FROM the forecast, so that graded the
forecast against itself — a circular, flattering, meaningless number. These tests
pin the corrected behaviour:

  - actuals come from observed POS/revenue data (revenue_actuals hourly_breakdown),
  - when no observed actuals exist, accuracy is reported as UNMEASURABLE
    (overall_mape is None, data_coverage 0.0) rather than fabricated,
  - rosters alone never produce an accuracy number.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

from rosteriq.database import MemoryStore
from rosteriq.models import (
    DemandForecast, SignalType, Roster, Shift, ShiftStatus,
)
from rosteriq.services.forecast_accuracy import ForecastAccuracyService


VENUE = "v1"
D = date(2026, 6, 22)  # Monday


def _forecast(hour, covers, fid=None):
    return DemandForecast(
        id=fid or f"f-{hour}", venue_id=VENUE, date=D, hour=hour,
        predicted_covers=float(covers), confidence=0.8,
        signals_used=[SignalType.historical], model_version="v1",
    )


def _seed_forecasts(db, pairs):
    db.add_forecasts([_forecast(h, c) for h, c in pairs])


def _seed_actuals(db, hourly):
    """hourly: list of {hour, covers} -> persisted as observed revenue actuals."""
    db.save_revenue_actual(
        VENUE, D.isoformat(),
        {"daily_total": 0.0, "hourly_breakdown": hourly},
    )


def _seed_roster(db, hours):
    """A roster covering the same hours — must NOT be used as an actuals source."""
    shifts = [
        Shift(
            id=f"s-{h}", employee_id="e1", date=D,
            start_time=datetime(2026, 6, 22, h, 0).time(),
            end_time=datetime(2026, 6, 22, min(h + 4, 23), 0).time(),
            break_minutes=0, status=ShiftStatus.scheduled, role="bar",
        )
        for h in hours
    ]
    db.save_roster(Roster(
        id="r1", venue_id=VENUE, week_start=D, week_end=date(2026, 6, 28),
        shifts=shifts, total_cost=Decimal("0"), created_at=datetime(2026, 6, 20),
    ))


# ---------------------------------------------------------------------------

def test_no_actuals_is_unmeasurable_not_fabricated():
    """Forecasts but no observed actuals -> mape None, zero coverage, honest note."""
    db = MemoryStore()
    _seed_forecasts(db, [(18, 40), (19, 60), (20, 50)])
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.overall_mape is None
    assert report.data_coverage == 0.0
    assert report.measured_samples == 0
    assert any("cannot be measured" in r for r in report.recommendations)


def test_rosters_alone_do_not_create_an_accuracy_number():
    """A full roster with NO observed actuals must still be unmeasurable.

    This is the regression guard for the circularity: previously the roster would
    have been turned into 'actuals' and produced a (meaningless) MAPE.
    """
    db = MemoryStore()
    _seed_forecasts(db, [(18, 40), (19, 60), (20, 50)])
    _seed_roster(db, [18, 19, 20])  # roster present, but no POS actuals
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.overall_mape is None
    assert report.data_coverage == 0.0


def test_perfect_forecast_against_observed_actuals():
    """Forecast == observed actuals -> ~0 error, full coverage."""
    db = MemoryStore()
    _seed_forecasts(db, [(18, 40), (19, 60), (20, 50)])
    _seed_actuals(db, [
        {"hour": 18, "covers": 40},
        {"hour": 19, "covers": 60},
        {"hour": 20, "covers": 50},
    ])
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.overall_mape == pytest.approx(0.0)
    assert report.overall_mae == pytest.approx(0.0)
    assert report.data_coverage == pytest.approx(1.0)
    assert report.measured_samples == 3


def test_imperfect_forecast_has_real_error():
    """A forecast off from observed actuals yields a non-zero, correct MAPE."""
    db = MemoryStore()
    # Predicted 50 vs actual 40 -> 25% error at hour 19.
    _seed_forecasts(db, [(19, 50)])
    _seed_actuals(db, [{"hour": 19, "covers": 40}])
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.overall_mape == pytest.approx(25.0)  # |40-50|/40 * 100 (MAPE normalises by actual)
    assert report.measured_samples == 1
    assert report.data_coverage == pytest.approx(1.0)


def test_transactions_used_when_covers_absent():
    """When hourly_breakdown lacks 'covers', the real 'transactions' count is used."""
    db = MemoryStore()
    _seed_forecasts(db, [(19, 30)])
    _seed_actuals(db, [{"hour": 19, "transactions": 30, "revenue": 900.0}])
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.measured_samples == 1
    assert report.overall_mape == pytest.approx(0.0)


def test_partial_coverage_flagged():
    """Only some forecast hours observed -> coverage < 1 and a low-coverage note."""
    db = MemoryStore()
    _seed_forecasts(db, [(18, 40), (19, 60), (20, 50), (21, 45)])
    _seed_actuals(db, [{"hour": 19, "covers": 60}])  # only 1 of 4 observed
    svc = ForecastAccuracyService(db)
    report = svc.calculate_accuracy(VENUE, date(2026, 6, 21), date(2026, 6, 23))

    assert report.measured_samples == 1
    assert report.data_coverage == pytest.approx(0.25)
    assert any("forecast hours had observed" in r for r in report.recommendations)
