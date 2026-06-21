"""
Tests for enhanced forecasting engine (forecast_v2).

Tests seasonal decomposition, weather modeling, event overlays,
confidence intervals, and accuracy evaluation.
"""

import pytest
from datetime import date, datetime, timedelta

from rosteriq.services.forecast_v2 import (
    EnhancedForecaster,
    SeasonalDecomposer,
    WeatherModeler,
    EventOverlay,
    WeatherForecast,
    SeasonalComponents,
    ForecastResult,
)
from rosteriq.models import VenueConfig, State


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_venue() -> VenueConfig:
    """Create a sample venue."""
    return VenueConfig(
        id="venue_001",
        name="Test Cafe",
        tanda_org_id="tanda_001",
        state=State.vic,
        timezone="Australia/Melbourne",
        max_labour_pct=30.0,
        pos_system="cafe",
        created_at=datetime.now(),
    )


@pytest.fixture
def historical_data() -> list[dict]:
    """Create sample historical demand data."""
    data = []
    base_date = date.today() - timedelta(days=90)

    for day_offset in range(90):
        current_date = base_date + timedelta(days=day_offset)
        weekday = current_date.weekday()

        # Higher demand on weekends
        base_covers = 40 if weekday < 5 else 80

        for hour in range(6, 22):
            # Peak hours: 7-9am, 12-2pm, 6-8pm
            if 7 <= hour <= 9 or 12 <= hour <= 14 or 18 <= hour <= 20:
                covers = base_covers * 1.5
            else:
                covers = base_covers * 0.7

            data.append({
                "date": current_date,
                "hour": hour,
                "covers": covers + (weekday * 5),  # Add variation by day
            })

    return data


@pytest.fixture
def sample_weather() -> WeatherForecast:
    """Create sample weather forecast."""
    return WeatherForecast(
        date=date.today(),
        hour=12,
        temp_celsius=22.0,
        wind_kmh=15.0,
        rain_mm=0.0,
        cloud_cover_pct=30,
    )


# ============================================================================
# Seasonal Decomposer Tests
# ============================================================================


class TestSeasonalDecomposer:
    """Test seasonal component extraction."""

    def test_decompose_basic(self, historical_data: list[dict]):
        """Test basic decomposition."""
        decomposer = SeasonalDecomposer()
        components = decomposer.decompose(historical_data)

        assert components is not None
        assert len(components.day_of_week) == 7
        assert len(components.month_of_year) == 12
        assert len(components.hour_of_day) == 24
        assert components.training_days > 0

    def test_day_of_week_seasonality(self, historical_data: list[dict]):
        """Test that weekends have higher multiplier than weekdays."""
        decomposer = SeasonalDecomposer()
        components = decomposer.decompose(historical_data)

        # Saturday (5) and Sunday (6) should have higher multipliers
        assert components.day_of_week[5] > components.day_of_week[0]
        assert components.day_of_week[6] > components.day_of_week[1]

    def test_hour_of_day_seasonality(self, historical_data: list[dict]):
        """Test that peak hours have higher multipliers."""
        decomposer = SeasonalDecomposer()
        components = decomposer.decompose(historical_data)

        # Peak hours (8, 13, 19) should exceed off-peak (3)
        peak_hours = [components.hour_of_day[h] for h in [8, 13, 19]]
        off_peak = components.hour_of_day[3]

        assert all(p > off_peak for p in peak_hours)

    def test_default_components_when_empty(self):
        """Test fallback defaults when no data."""
        decomposer = SeasonalDecomposer()
        components = decomposer.decompose([])

        assert components is not None
        assert len(components.day_of_week) == 7
        assert components.trend == 0.0


# ============================================================================
# Weather Modeler Tests
# ============================================================================


class TestWeatherModeler:
    """Test weather impact modeling."""

    def test_rain_penalty_outdoor(self):
        """Test that rain reduces outdoor venue demand."""
        modeler = WeatherModeler()

        clear_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=22.0, wind_kmh=10.0,
            rain_mm=0.0, cloud_cover_pct=20,
        )
        rainy_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=22.0, wind_kmh=10.0,
            rain_mm=5.0, cloud_cover_pct=90,
        )

        clear_mod = modeler.calculate_modifier("beer_garden", clear_weather)
        rain_mod = modeler.calculate_modifier("beer_garden", rainy_weather)

        assert rain_mod < clear_mod

    def test_temperature_extreme_cold(self):
        """Test that extreme cold reduces demand."""
        modeler = WeatherModeler()

        mild_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=20.0, wind_kmh=10.0,
            rain_mm=0.0, cloud_cover_pct=50,
        )
        cold_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=2.0, wind_kmh=10.0,
            rain_mm=0.0, cloud_cover_pct=50,
        )

        mild_mod = modeler.calculate_modifier("restaurant", mild_weather)
        cold_mod = modeler.calculate_modifier("restaurant", cold_weather)

        assert cold_mod < mild_mod

    def test_temperature_boost_outdoor(self):
        """Test that warm weather boosts outdoor venues."""
        modeler = WeatherModeler()

        mild_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=20.0, wind_kmh=10.0,
            rain_mm=0.0, cloud_cover_pct=50,
        )
        warm_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=28.0, wind_kmh=10.0,
            rain_mm=0.0, cloud_cover_pct=20,
        )

        mild_mod = modeler.calculate_modifier("beer_garden", mild_weather)
        warm_mod = modeler.calculate_modifier("beer_garden", warm_weather)

        assert warm_mod > mild_mod

    def test_wind_penalty(self):
        """Test that high wind reduces outdoor demand."""
        modeler = WeatherModeler()

        calm_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=20.0, wind_kmh=15.0,
            rain_mm=0.0, cloud_cover_pct=50,
        )
        windy_weather = WeatherForecast(
            date=date.today(), hour=12,
            temp_celsius=20.0, wind_kmh=50.0,
            rain_mm=0.0, cloud_cover_pct=50,
        )

        calm_mod = modeler.calculate_modifier("beer_garden", calm_weather)
        wind_mod = modeler.calculate_modifier("beer_garden", windy_weather)

        assert wind_mod < calm_mod


# ============================================================================
# Event Overlay Tests
# ============================================================================


class TestEventOverlay:
    """Test event and date-based modifiers."""

    def test_australia_day(self):
        """Test Australia Day modifier."""
        overlay = EventOverlay()

        australia_day = date(2026, 1, 26)
        modifier = overlay.calculate_modifier("venue_001", australia_day)

        # Australia Day should boost demand
        assert modifier > 1.0

    def test_christmas_day(self):
        """Test Christmas Day modifier."""
        overlay = EventOverlay()

        christmas = date(2026, 12, 25)
        modifier = overlay.calculate_modifier("venue_001", christmas)

        # Most venues closed or quiet on Christmas
        assert modifier < 1.0

    def test_school_holidays(self):
        """Test school holiday impact for family venues."""
        overlay = EventOverlay()

        # Mid-April (school holidays)
        school_holiday = date(2026, 4, 10)

        cafe_mod = overlay.calculate_modifier(
            "venue_001", school_holiday, "cafe"
        )
        bar_mod = overlay.calculate_modifier(
            "venue_001", school_holiday, "bar"
        )

        # Cafes benefit more from families during school holidays
        assert cafe_mod > bar_mod


# ============================================================================
# Enhanced Forecaster Tests
# ============================================================================


class TestEnhancedForecaster:
    """Test the main forecaster."""

    def test_initialization(self, sample_venue: VenueConfig):
        """Test forecaster initialization."""
        forecaster = EnhancedForecaster(sample_venue)

        assert forecaster.venue_config == sample_venue
        assert forecaster.seasonal_components is None
        assert len(forecaster.historical_data) == 0

    def test_retrain(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test training seasonal components."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        components = forecaster.retrain()

        assert components is not None
        assert components.training_days > 0
        assert forecaster.seasonal_components is not None

    def test_predict_returns_valid_result(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test that predict returns valid ForecastResult."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        result = forecaster.predict(
            date.today() + timedelta(days=1),
            12,
        )

        assert isinstance(result, ForecastResult)
        assert result.venue_id == sample_venue.id
        assert result.point_estimate >= 0
        assert result.confidence_interval_80[0] <= result.point_estimate
        assert result.confidence_interval_80[1] >= result.point_estimate
        assert 0 <= result.confidence_score <= 1

    def test_predict_with_weather(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
        sample_weather: WeatherForecast,
    ):
        """Test prediction with weather modifier."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        result = forecaster.predict(
            date.today() + timedelta(days=1),
            12,
            weather=sample_weather,
        )

        assert result.components["weather"] > 0
        assert result.components["weather"] <= 1.5

    def test_forecast_week(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test full-week forecast generation."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        week_start = date.today() + timedelta(days=1)
        if week_start.weekday() != 0:
            week_start = week_start - timedelta(days=week_start.weekday())

        results = forecaster.forecast_week(week_start)

        # Should have 7 days × 24 hours = 168 forecasts
        assert len(results) == 168
        assert all(isinstance(r, ForecastResult) for r in results)

    def test_staffing_needs(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test staffing recommendation conversion."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        target_date = date.today() + timedelta(days=1)
        recommendations = forecaster.forecast_staffing_needs(
            target_date,
            covers_per_staff=20.0,
        )

        assert len(recommendations) == 24
        assert all("recommended_staff" in rec for rec in recommendations)
        assert all(rec["recommended_staff"] >= 0 for rec in recommendations)

    def test_accuracy_report_empty_log(
        self,
        sample_venue: VenueConfig,
    ):
        """Test accuracy report when no predictions logged."""
        forecaster = EnhancedForecaster(sample_venue)

        report = forecaster.evaluate_accuracy()

        assert report.sample_count == 0
        assert report.mape == 0.0

    def test_log_prediction(
        self,
        sample_venue: VenueConfig,
    ):
        """Test prediction logging."""
        forecaster = EnhancedForecaster(sample_venue)

        forecaster.log_prediction(50.0, 48.5, date.today())

        assert len(forecaster.predictions_log) == 1
        assert forecaster.predictions_log[0]["predicted"] == 50.0
        assert forecaster.predictions_log[0]["actual"] == 48.5

    def test_should_retrain_on_empty(
        self,
        sample_venue: VenueConfig,
    ):
        """Test that retrain is needed when no components."""
        forecaster = EnhancedForecaster(sample_venue)

        assert forecaster.should_retrain() is True

    def test_should_not_retrain_if_recent(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test that retrain is not needed if recently trained."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)
        forecaster.retrain()

        assert forecaster.should_retrain() is False


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_full_forecast_pipeline(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test complete forecasting pipeline."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        # Train
        forecaster.retrain()
        assert forecaster.seasonal_components is not None

        # Predict
        target_date = date.today() + timedelta(days=5)
        result = forecaster.predict(target_date, 12)
        assert result.point_estimate > 0

        # Get staffing
        staffing = forecaster.forecast_staffing_needs(target_date)
        assert len(staffing) == 24

        # Evaluate (with logged predictions)
        forecaster.log_prediction(result.point_estimate, result.point_estimate * 0.95, target_date)
        report = forecaster.evaluate_accuracy()
        assert report.sample_count == 1

    def test_confidence_intervals_narrow_with_data(
        self,
        sample_venue: VenueConfig,
        historical_data: list[dict],
    ):
        """Test that confidence intervals narrow with more data."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(historical_data)

        target_date = date.today() + timedelta(days=1)
        result = forecaster.predict(target_date, 12)

        # With 90 days of data, confidence should be decent
        assert result.confidence_score > 0.5

        # 95% interval should be wider than 80%
        interval_80_width = result.confidence_interval_80[1] - result.confidence_interval_80[0]
        interval_95_width = result.confidence_interval_95[1] - result.confidence_interval_95[0]
        assert interval_95_width > interval_80_width


# ============================================================================
# Confidence Interval Invariants
# ============================================================================


@pytest.fixture
def dow_hour_history() -> list[dict]:
    """
    120 days of synthetic demand that varies strongly by day-of-week and
    hour-of-day, with a little deterministic noise so the residual
    distribution is non-degenerate.
    """
    data = []
    base_date = date.today() - timedelta(days=120)

    for day_offset in range(120):
        current_date = base_date + timedelta(days=day_offset)
        weekday = current_date.weekday()

        # Weekends much busier than weekdays.
        dow_level = 30.0 if weekday < 5 else 90.0

        for hour in range(6, 23):
            # Three peaks: breakfast, lunch, dinner.
            if 7 <= hour <= 9 or 12 <= hour <= 14 or 18 <= hour <= 20:
                hour_mult = 1.6
            elif hour < 8 or hour > 21:
                hour_mult = 0.3
            else:
                hour_mult = 0.8

            # Deterministic but varied noise so residuals form a spread.
            noise = ((day_offset * 7 + hour * 3) % 11 - 5) * 0.04  # ±20%
            covers = dow_level * hour_mult * (1.0 + noise)

            data.append({
                "date": current_date,
                "hour": hour,
                "covers": max(0.0, covers),
            })

    return data


class TestConfidenceIntervalInvariants:
    """
    Regression guard for the confidence-interval band.

    The previous implementation compared every raw history row against the
    single current seasonal point estimate, so the bounds were a fixed cover
    count (e.g. high_80 == 80) decoupled from the point — frequently leaving
    the point estimate OUTSIDE its own interval. The band is now a
    multiplicative envelope around the point estimate derived from the model's
    relative error distribution.
    """

    def _all_forecasts(self, forecaster) -> list:
        results = []
        target_date = date.today() + timedelta(days=1)
        # Sweep a full week and the open hours so we cover quiet + busy slots
        # across every day-of-week multiplier.
        for day in range(7):
            d = target_date + timedelta(days=day)
            for hour in range(6, 23):
                results.append(forecaster.predict(d, hour))
        return results

    def test_point_estimate_always_inside_intervals(
        self,
        sample_venue: VenueConfig,
        dow_hour_history: list[dict],
    ):
        """(a) lower <= point <= upper for EVERY forecast, both bands."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(dow_hour_history)

        results = self._all_forecasts(forecaster)
        assert len(results) >= 100  # many hours exercised

        for r in results:
            lo80, hi80 = r.confidence_interval_80
            lo95, hi95 = r.confidence_interval_95
            assert lo80 <= r.point_estimate <= hi80, (
                f"80% band {r.confidence_interval_80} excludes point "
                f"{r.point_estimate} at hour {r.hour}"
            )
            assert lo95 <= r.point_estimate <= hi95, (
                f"95% band {r.confidence_interval_95} excludes point "
                f"{r.point_estimate} at hour {r.hour}"
            )

    def test_95_band_at_least_as_wide_as_80(
        self,
        sample_venue: VenueConfig,
        dow_hour_history: list[dict],
    ):
        """(b) ci95 width >= ci80 width for every forecast."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(dow_hour_history)

        for r in self._all_forecasts(forecaster):
            w80 = r.confidence_interval_80[1] - r.confidence_interval_80[0]
            w95 = r.confidence_interval_95[1] - r.confidence_interval_95[0]
            assert w95 >= w80, (
                f"95% width {w95} < 80% width {w80} at hour {r.hour}"
            )

    def test_lower_bound_never_negative(
        self,
        sample_venue: VenueConfig,
        dow_hour_history: list[dict],
    ):
        """(c) lower >= 0 for both bands."""
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(dow_hour_history)

        for r in self._all_forecasts(forecaster):
            assert r.confidence_interval_80[0] >= 0.0
            assert r.confidence_interval_95[0] >= 0.0

    def test_band_scales_with_point_estimate(
        self,
        sample_venue: VenueConfig,
        dow_hour_history: list[dict],
    ):
        """
        (d) The band is multiplicative: a busy slot's absolute band width is
        wider than a quiet slot's. Compare a weekend dinner peak against a
        weekday off-peak hour and assert both the point estimate and the
        absolute 80% band width are larger for the busy slot.
        """
        forecaster = EnhancedForecaster(sample_venue)
        forecaster.add_historical_data(dow_hour_history)

        next_monday = date.today() + timedelta(days=1)
        while next_monday.weekday() != 0:
            next_monday += timedelta(days=1)
        next_saturday = next_monday + timedelta(days=5)

        quiet = forecaster.predict(next_monday, 6)      # weekday, off-peak
        busy = forecaster.predict(next_saturday, 19)    # weekend, dinner peak

        assert busy.point_estimate > quiet.point_estimate

        quiet_width = (
            quiet.confidence_interval_80[1] - quiet.confidence_interval_80[0]
        )
        busy_width = (
            busy.confidence_interval_80[1] - busy.confidence_interval_80[0]
        )
        assert busy_width > quiet_width, (
            f"busy band width {busy_width} not wider than quiet "
            f"{quiet_width} despite higher point estimate"
        )
