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
