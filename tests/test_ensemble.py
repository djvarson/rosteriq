"""Tests for the ensemble forecasting module."""

import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal

from rosteriq.models import VenueConfig, State, DemandForecast, SignalType
from rosteriq.ensemble import (
    EnsembleForecaster, XGBOOST_WEIGHT, PROPHET_WEIGHT,
    MIN_TRAINING_WEEKS, COLD_START_DAY_DEFAULTS,
    COLD_START_HOUR_MULTIPLIERS, _get_hour_multiplier,
    DEFAULT_TRADING_HOURS,
)


# ============================================================================
# Helpers
# ============================================================================

def make_venue_config() -> VenueConfig:
    return VenueConfig(
        id="venue-1", name="The Local Pub", tanda_org_id="org-123",
        state=State.vic, max_labour_pct=35.0,
        created_at=datetime(2025, 1, 1),
    )


def make_historical_data(weeks: int = 2) -> list[dict]:
    """Generate N weeks of synthetic historical data."""
    data = []
    start = date(2026, 1, 5)  # a Monday
    for week in range(weeks):
        for day in range(7):
            d = start + timedelta(weeks=week, days=day)
            weekday = d.weekday()
            base = COLD_START_DAY_DEFAULTS.get(weekday, 50)
            for hour in range(6, 24):
                mult = _get_hour_multiplier(hour)
                covers = base * mult * (0.9 + 0.2 * (hour % 3) / 3)
                data.append({"date": d, "hour": hour, "covers": covers})
    return data


# ============================================================================
# Constants tests
# ============================================================================

class TestConstants:
    def test_weights_sum_to_one(self):
        assert XGBOOST_WEIGHT + PROPHET_WEIGHT == pytest.approx(1.0)

    def test_xgboost_heavier(self):
        assert XGBOOST_WEIGHT > PROPHET_WEIGHT

    def test_min_training_weeks(self):
        assert MIN_TRAINING_WEEKS == 4

    def test_cold_start_defaults_all_days(self):
        for day in range(7):
            assert day in COLD_START_DAY_DEFAULTS

    def test_friday_saturday_busiest(self):
        assert COLD_START_DAY_DEFAULTS[4] > COLD_START_DAY_DEFAULTS[0]  # Fri > Mon
        assert COLD_START_DAY_DEFAULTS[5] > COLD_START_DAY_DEFAULTS[4]  # Sat > Fri

    def test_trading_hours(self):
        assert DEFAULT_TRADING_HOURS == list(range(6, 24))


# ============================================================================
# Hour multiplier tests
# ============================================================================

class TestHourMultiplier:
    def test_dinner_peak(self):
        for hour in [18, 19, 20, 21]:
            assert _get_hour_multiplier(hour) == 1.5

    def test_lunch_peak(self):
        for hour in [12, 13]:
            assert _get_hour_multiplier(hour) == 1.3

    def test_morning(self):
        assert _get_hour_multiplier(8) == 0.5

    def test_late_night(self):
        assert _get_hour_multiplier(22) == 0.4

    def test_afternoon(self):
        assert _get_hour_multiplier(16) == 0.7


# ============================================================================
# Forecaster initialisation tests
# ============================================================================

class TestForecasterInit:
    def test_initial_state(self):
        fc = EnsembleForecaster(make_venue_config())
        assert fc.is_trained is False
        assert fc.training_weeks == 0
        assert fc.historical_data == []

    def test_model_status_initial(self):
        fc = EnsembleForecaster(make_venue_config())
        status = fc.get_model_status()
        assert status["is_trained"] is False
        assert status["data_points"] == 0
        assert status["model_version"] == "cold_start_v1"


# ============================================================================
# Historical data tests
# ============================================================================

class TestHistoricalData:
    def test_add_valid_data(self):
        fc = EnsembleForecaster(make_venue_config())
        data = [{"date": date(2026, 1, 5), "hour": 12, "covers": 80}]
        fc.add_historical_data(data)
        assert len(fc.historical_data) == 1

    def test_add_multiple_batches(self):
        fc = EnsembleForecaster(make_venue_config())
        fc.add_historical_data([{"date": date(2026, 1, 5), "hour": 12, "covers": 80}])
        fc.add_historical_data([{"date": date(2026, 1, 6), "hour": 13, "covers": 90}])
        assert len(fc.historical_data) == 2

    def test_invalid_data_raises(self):
        fc = EnsembleForecaster(make_venue_config())
        with pytest.raises(ValueError):
            fc.add_historical_data([{"date": date(2026, 1, 5)}])  # missing hour, covers


# ============================================================================
# Training tests
# ============================================================================

class TestTraining:
    def test_no_data_returns_false(self):
        fc = EnsembleForecaster(make_venue_config())
        assert fc.train() is False
        assert fc.is_trained is False

    def test_insufficient_data_returns_false(self):
        fc = EnsembleForecaster(make_venue_config())
        fc.add_historical_data(make_historical_data(weeks=2))
        result = fc.train()
        assert result is False  # 2 weeks < MIN_TRAINING_WEEKS

    def test_training_weeks_calculated(self):
        fc = EnsembleForecaster(make_venue_config())
        fc.add_historical_data(make_historical_data(weeks=2))
        fc.train()
        assert fc.training_weeks >= 1


# ============================================================================
# Cold-start prediction tests
# ============================================================================

class TestColdStartPrediction:
    def test_predict_returns_forecasts(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7))  # Tuesday
        assert len(forecasts) == len(DEFAULT_TRADING_HOURS)
        assert all(isinstance(f, DemandForecast) for f in forecasts)

    def test_predict_specific_hours(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7), hours=[12, 13, 18])
        assert len(forecasts) == 3

    def test_cold_start_confidence_low(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7))
        for f in forecasts:
            assert f.confidence <= 0.6  # Cold start has low confidence

    def test_cold_start_model_version(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7))
        assert all(f.model_version == "cold_start_v1" for f in forecasts)

    def test_cold_start_covers_non_negative(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7))
        for f in forecasts:
            assert f.predicted_covers >= 0

    def test_saturday_busier_than_monday(self):
        fc = EnsembleForecaster(make_venue_config())
        mon_forecasts = fc.predict(date(2026, 4, 6), hours=[19])  # Monday
        sat_forecasts = fc.predict(date(2026, 4, 11), hours=[19])  # Saturday
        assert sat_forecasts[0].predicted_covers > mon_forecasts[0].predicted_covers

    def test_dinner_peak_busier_than_morning(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict(date(2026, 4, 7), hours=[8, 19])
        morning = forecasts[0]
        dinner = forecasts[1]
        assert dinner.predicted_covers > morning.predicted_covers

    def test_cold_start_with_some_data(self):
        fc = EnsembleForecaster(make_venue_config())
        fc.add_historical_data([
            {"date": date(2026, 4, 7), "hour": 12, "covers": 120},
            {"date": date(2026, 4, 7), "hour": 19, "covers": 200},
        ])
        forecasts = fc.predict(date(2026, 4, 7), hours=[12])
        # Should use actual data for known hour
        assert forecasts[0].predicted_covers == pytest.approx(120.0, abs=1)


# ============================================================================
# Week prediction tests
# ============================================================================

class TestPredictWeek:
    def test_predict_week_length(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict_week(date(2026, 4, 6))
        expected = 7 * len(DEFAULT_TRADING_HOURS)
        assert len(forecasts) == expected

    def test_predict_week_covers_all_days(self):
        fc = EnsembleForecaster(make_venue_config())
        forecasts = fc.predict_week(date(2026, 4, 6))
        dates_covered = set(f.date for f in forecasts)
        assert len(dates_covered) == 7

    def test_predict_week_venue_id(self):
        config = make_venue_config()
        fc = EnsembleForecaster(config)
        forecasts = fc.predict_week(date(2026, 4, 6))
        assert all(f.venue_id == config.id for f in forecasts)


# ============================================================================
# Model status tests
# ============================================================================

class TestModelStatus:
    def test_untrained_status(self):
        fc = EnsembleForecaster(make_venue_config())
        status = fc.get_model_status()
        assert status["is_trained"] is False
        assert status["has_xgboost"] is False
        assert status["has_prophet"] is False

    def test_status_after_data(self):
        fc = EnsembleForecaster(make_venue_config())
        fc.add_historical_data(make_historical_data(weeks=2))
        fc.train()
        status = fc.get_model_status()
        assert status["data_points"] > 0
        assert status["training_weeks"] >= 1
