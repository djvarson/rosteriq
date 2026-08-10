"""
Enhanced demand forecasting engine for RosterIQ v2.

Combines seasonal decomposition, weather impact modeling, event overlays,
and confidence interval calculation with the existing ensemble forecaster.

Features:
- Day-of-week and month-of-year seasonality learned from historical data
- Time-of-day curves per venue (peak hours differ by venue type)
- Weather impact modifiers (rain, temperature, wind)
- Event overlays (holidays, sports, school breaks)
- Bootstrap confidence intervals at 80% and 95%
- Auto-retraining when data becomes stale or error rate exceeds threshold
- Accuracy evaluation (MAPE, bias, worst days)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from statistics import mean, stdev
from typing import Optional, NamedTuple
from collections import defaultdict
from decimal import Decimal

from rosteriq.models import VenueConfig, SignalType, DemandForecast

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================


class SeasonalComponents(NamedTuple):
    """Extracted seasonal patterns from historical data."""
    day_of_week: dict[int, float]      # 0=Mon, 6=Sun → multiplier
    month_of_year: dict[int, float]    # 1-12 → multiplier
    hour_of_day: dict[int, float]      # 0-23 → multiplier
    trend: float                         # growth rate (% per week)
    training_days: int
    last_retrain: datetime


class ForecastResult(NamedTuple):
    """Enhanced forecast result with confidence intervals."""
    venue_id: str
    date: date
    hour: int
    point_estimate: float               # central prediction
    confidence_interval_80: tuple[float, float]  # (low, high)
    confidence_interval_95: tuple[float, float]  # (low, high)
    components: dict[str, float]        # contribution breakdown
    confidence_score: float              # 0-1, based on data availability
    model_version: str
    signals_used: list[SignalType]
    confidence: str = "medium"           # "none", "low", "medium", "high"
    warning: Optional[str] = None        # cold-start or data quality warning


class WeatherForecast(NamedTuple):
    """Weather data for impact calculation."""
    date: date
    hour: int
    temp_celsius: float
    wind_kmh: float
    rain_mm: float
    cloud_cover_pct: int


class AccuracyReport(NamedTuple):
    """Model accuracy metrics."""
    mape: float                    # Mean Absolute Percentage Error
    bias: float                    # Average prediction bias
    worst_days: list[tuple[date, float]]  # (date, error_pct)
    evaluation_period_days: int
    sample_count: int


# ============================================================================
# Seasonal Decomposition
# ============================================================================


class SeasonalDecomposer:
    """Extract seasonal patterns from historical demand data."""

    def __init__(self):
        """Initialize the decomposer."""
        self.components: Optional[SeasonalComponents] = None

    def decompose(
        self,
        historical_data: list[dict],
        lookback_days: int = 180,
    ) -> SeasonalComponents:
        """
        Extract seasonal components from historical data.

        Args:
            historical_data: List of {date, hour, covers} dicts
            lookback_days: How many days back to use for learning

        Returns:
            SeasonalComponents with learned patterns
        """
        if not historical_data:
            return self._default_components()

        # Filter to lookback period
        cutoff_date = date.today() - timedelta(days=lookback_days)
        recent_data = [
            d for d in historical_data
            if d.get("date") >= cutoff_date
        ]

        if not recent_data:
            return self._default_components()

        # Extract day-of-week seasonality
        dow_sums: dict[int, list[float]] = defaultdict(list)
        month_sums: dict[int, list[float]] = defaultdict(list)
        hour_sums: dict[int, list[float]] = defaultdict(list)

        for point in recent_data:
            dt = point["date"]
            covers = float(point.get("covers", 0))
            hour = int(point.get("hour", 0))

            dow_sums[dt.weekday()].append(covers)
            month_sums[dt.month].append(covers)
            hour_sums[hour].append(covers)

        # Overall mean covers across every observation — used as the baseline
        # to normalise the seasonal factors so each comes out around 1.0.
        #
        # NOTE: the previous expression iterated over the {key: [covers]} dicts
        # themselves, so `sum(v)` summed the dict KEYS (weekday/month/hour
        # indices) and `len(v)` counted distinct keys. That produced a tiny,
        # meaningless baseline (~6-8) instead of the true cover mean (~60),
        # which inflated every seasonal factor by ~10x and made point estimates
        # explode into the millions.
        all_covers = [
            float(p.get("covers", 0)) for p in recent_data
        ]
        overall_avg = (mean(all_covers) if all_covers else 0.0) or 50.0

        day_of_week = {}
        for dow in range(7):
            values = dow_sums.get(dow, [overall_avg])
            avg = mean(values) if values else overall_avg
            day_of_week[dow] = avg / overall_avg if overall_avg > 0 else 1.0

        month_of_year = {}
        for month in range(1, 13):
            values = month_sums.get(month, [overall_avg])
            avg = mean(values) if values else overall_avg
            month_of_year[month] = avg / overall_avg if overall_avg > 0 else 1.0

        hour_of_day = {}
        for hour in range(24):
            values = hour_sums.get(hour, [overall_avg * 0.5])
            avg = mean(values) if values else overall_avg * 0.5
            hour_of_day[hour] = avg / overall_avg if overall_avg > 0 else 0.5

        # Calculate trend (weekly growth rate)
        week_sums: dict[str, float] = defaultdict(float)
        week_counts: dict[str, int] = defaultdict(int)

        for point in recent_data:
            dt = point["date"]
            week_key = dt.isocalendar()[1]
            week_sums[week_key] += float(point.get("covers", 0))
            week_counts[week_key] += 1

        weekly_avgs = [
            week_sums[w] / week_counts[w]
            for w in sorted(week_counts.keys())
            if week_counts[w] > 0
        ]

        trend = 0.0
        if len(weekly_avgs) >= 2:
            # Calculate average weekly growth rate
            growth_rates = []
            for i in range(1, len(weekly_avgs)):
                if weekly_avgs[i - 1] > 0:
                    rate = (weekly_avgs[i] - weekly_avgs[i - 1]) / weekly_avgs[i - 1]
                    growth_rates.append(rate)
            trend = mean(growth_rates) if growth_rates else 0.0

        self.components = SeasonalComponents(
            day_of_week=day_of_week,
            month_of_year=month_of_year,
            hour_of_day=hour_of_day,
            trend=trend,
            training_days=len(recent_data),
            last_retrain=datetime.now(),
        )
        return self.components

    def _default_components(self) -> SeasonalComponents:
        """Return sensible defaults when no data available."""
        return SeasonalComponents(
            day_of_week={
                0: 0.95,  # Monday
                1: 0.98,  # Tuesday
                2: 1.0,   # Wednesday
                3: 1.05,  # Thursday
                4: 1.2,   # Friday
                5: 1.35,  # Saturday
                6: 1.15,  # Sunday
            },
            month_of_year={
                1: 0.75,   # January (quiet)
                2: 0.8,    # February
                3: 0.95,   # March
                4: 1.0,    # April
                5: 0.95,   # May
                6: 0.9,    # June (mid-year slump)
                7: 0.85,   # July (school holidays)
                8: 0.9,    # August
                9: 1.0,    # September
                10: 1.05,  # October
                11: 1.15,  # November
                12: 1.5,   # December (Christmas)
            },
            hour_of_day={
                0: 0.05, 1: 0.03, 2: 0.02, 3: 0.02, 4: 0.03, 5: 0.1,
                6: 0.3, 7: 0.6, 8: 0.8, 9: 0.75, 10: 0.65,
                11: 0.9, 12: 1.2, 13: 1.1, 14: 0.8, 15: 0.6,
                16: 0.5, 17: 0.7, 18: 1.0, 19: 1.15, 20: 1.2,
                21: 1.1, 22: 0.8, 23: 0.4,
            },
            trend=0.0,
            training_days=0,
            last_retrain=datetime.now(),
        )


# ============================================================================
# Weather Impact Modeling
# ============================================================================


class WeatherModeler:
    """Calculate demand modifiers based on weather conditions."""

    def calculate_modifier(
        self,
        venue_type: str,
        weather: WeatherForecast,
    ) -> float:
        """
        Calculate weather demand modifier.

        Args:
            venue_type: "cafe", "restaurant", "bar", "pub", "beer_garden", etc.
            weather: WeatherForecast with current/forecast data

        Returns:
            Multiplier 0.5-1.5 to apply to baseline demand
        """
        modifier = 1.0
        is_outdoor = venue_type in ("beer_garden", "terrace", "rooftop")

        # Rain impact
        if weather.rain_mm > 0:
            if weather.rain_mm > 10:
                # Heavy rain
                modifier *= 0.7 if is_outdoor else 1.05
            else:
                # Light rain
                modifier *= 0.85 if is_outdoor else 1.02
        else:
            # No rain — slight boost for outdoor venues
            if is_outdoor:
                modifier *= 1.05

        # Temperature impact
        temp = weather.temp_celsius
        if temp < 5:
            modifier *= 0.75
        elif 5 <= temp < 10:
            modifier *= 0.85
        elif 10 <= temp <= 25:
            # Optimal range
            modifier *= 1.0
        elif 25 < temp <= 35:
            # Warm, good for beer gardens and outdoor
            if is_outdoor:
                modifier *= 1.15
            else:
                modifier *= 0.95
        else:
            # >35°C heat stress
            if is_outdoor:
                modifier *= 0.8
            else:
                modifier *= 0.9

        # Wind impact (outdoor venues mainly)
        if weather.wind_kmh > 40 and is_outdoor:
            modifier *= 0.85
        elif weather.wind_kmh > 50:
            modifier *= 0.75

        # Cloud cover (mostly affects outdoor
        if is_outdoor and weather.cloud_cover_pct < 20:
            modifier *= 1.1

        return max(0.5, min(modifier, 1.5))


# ============================================================================
# Event Overlay
# ============================================================================


class EventOverlay:
    """Calculate demand modifiers for events and special dates."""

    # Public holidays (date.month, date.day) → name and impact
    AUSTRALIAN_HOLIDAYS = {
        (1, 1): ("New Year's Day", 0.9),        # Most venues quiet
        (1, 26): ("Australia Day", 1.3),        # Parties
        (2, 14): ("Valentine's Day", 1.15),
        (4, 25): ("Anzac Day", 1.2),
        (6, 10): ("Queen's Birthday (approx)", 1.1),  # Varies by state
        (12, 25): ("Christmas Day", 0.5),       # Most venues closed
        (12, 26): ("Boxing Day", 1.1),
    }

    def calculate_modifier(
        self,
        venue_id: str,
        target_date: date,
        venue_type: str = "restaurant",
        custom_events: Optional[list[dict]] = None,
    ) -> float:
        """
        Calculate event/date modifier.

        Args:
            venue_id: For venue-specific customization
            target_date: Date to evaluate
            venue_type: Type of venue
            custom_events: List of {date, name, multiplier} dicts

        Returns:
            Multiplier 0.5-1.5
        """
        modifier = 1.0

        # Check for public holidays
        for (month, day), (holiday_name, impact) in self.AUSTRALIAN_HOLIDAYS.items():
            if target_date.month == month and target_date.day == day:
                # Adjust impact based on venue type
                if venue_type in ("cafe", "lunch_spot"):
                    # CBD lunch spots quiet on holidays
                    if impact > 1.0:
                        modifier = 0.9
                else:
                    modifier = impact
                break

        # School holidays (rough approximation)
        # NSW/VIC: typically 2 weeks in April, July, September, December
        if target_date.month in (4, 7, 9, 12):
            if 1 <= target_date.day <= 15:
                # Venues with families benefit
                if venue_type in ("cafe", "family_restaurant"):
                    modifier *= 1.1
                else:
                    modifier *= 0.95

        # Custom events (if provided)
        if custom_events:
            for event in custom_events:
                if event.get("date") == target_date:
                    modifier *= event.get("multiplier", 1.0)

        return max(0.5, min(modifier, 1.5))


# ============================================================================
# Enhanced Forecaster
# ============================================================================


class EnhancedForecaster:
    """
    Enhanced demand forecaster with seasonal decomposition, weather, and events.
    """

    def __init__(self, venue_config: VenueConfig, base_forecaster=None):
        """
        Initialize enhanced forecaster.

        Args:
            venue_config: Venue configuration
            base_forecaster: Optional ensemble forecaster (falls back to heuristics if None)
        """
        self.venue_config = venue_config
        self.base_forecaster = base_forecaster
        self.decomposer = SeasonalDecomposer()
        self.weather_modeler = WeatherModeler()
        self.event_overlay = EventOverlay()

        self.seasonal_components: Optional[SeasonalComponents] = None
        self.historical_data: list[dict] = []
        self.predictions_log: list[dict] = []  # For accuracy tracking

    def add_historical_data(self, data: list[dict]) -> None:
        """Add historical demand data."""
        self.historical_data.extend(data)

    def should_retrain(self) -> bool:
        """
        Determine if seasonal components should be retrained.

        Returns True if:
        - >7 days since last training
        - Prediction error rate >15%
        """
        if not self.seasonal_components:
            return True

        days_since = (datetime.now() - self.seasonal_components.last_retrain).days
        if days_since >= 7:
            return True

        # Check error rate
        if len(self.predictions_log) >= 50:
            recent = self.predictions_log[-50:]
            mape = sum(
                abs((p["actual"] - p["predicted"]) / p["actual"])
                for p in recent if p["actual"] > 0
            ) / len(recent)
            if mape > 0.15:
                return True

        return False

    def retrain(self) -> SeasonalComponents:
        """Retrain seasonal components."""
        self.seasonal_components = self.decomposer.decompose(self.historical_data)
        return self.seasonal_components

    def predict(
        self,
        target_date: date,
        target_hour: int,
        weather: Optional[WeatherForecast] = None,
        custom_events: Optional[list[dict]] = None,
    ) -> ForecastResult:
        """
        Generate enhanced forecast with confidence intervals.

        Args:
            target_date: Date to forecast
            target_hour: Hour (0-23)
            weather: Optional weather forecast
            custom_events: Optional list of custom events

        Returns:
            ForecastResult with point estimate and intervals
        """
        if not self.seasonal_components:
            self.retrain()

        # Base prediction from ensemble or default
        if self.base_forecaster:
            base_forecasts = self.base_forecaster.predict(target_date, [target_hour])
            if base_forecasts:
                base_pred = base_forecasts[0].predicted_covers
            else:
                base_pred = 50.0
        else:
            # Fallback heuristic
            base_pred = self._heuristic_predict(target_date, target_hour)

        # Apply seasonal components
        dow_factor = self.seasonal_components.day_of_week.get(target_date.weekday(), 1.0)
        month_factor = self.seasonal_components.month_of_year.get(target_date.month, 1.0)
        hour_factor = self.seasonal_components.hour_of_day.get(target_hour, 0.5)
        trend_factor = 1.0 + (self.seasonal_components.trend * 0.05)  # 5% per growth point

        seasonal_pred = base_pred * dow_factor * month_factor * hour_factor * trend_factor

        # Apply weather modifier
        weather_mod = 1.0
        if weather:
            weather_mod = self.weather_modeler.calculate_modifier(
                self.venue_config.pos_system or "restaurant",
                weather,
            )

        # Apply event modifier
        event_mod = self.event_overlay.calculate_modifier(
            self.venue_config.id,
            target_date,
            self.venue_config.pos_system or "restaurant",
            custom_events,
        )

        # Combined prediction
        point_estimate = seasonal_pred * weather_mod * event_mod
        point_estimate = max(0.0, point_estimate)

        # Calculate confidence intervals via bootstrap
        interval_80, interval_95 = self._calculate_confidence_intervals(
            point_estimate, seasonal_pred
        )
        # The reported point is rounded to 1dp below; when the raw point sits
        # exactly on a band edge, rounding can nudge it just OUTSIDE the raw
        # interval (e.g. point 71.653 -> 71.7 vs upper bound 71.653). An
        # interval that excludes its own point estimate is nonsense, so widen
        # each band to contain the rounded point.
        _pt = round(point_estimate, 1)
        interval_80 = (min(interval_80[0], _pt), max(interval_80[1], _pt))
        interval_95 = (min(interval_95[0], _pt), max(interval_95[1], _pt))

        # Confidence score
        confidence_score = self._calculate_confidence_score()

        # Classify confidence level and generate warnings based on data availability
        confidence_level, cold_start_warning = self._classify_confidence(confidence_score)

        components = {
            "base": base_pred,
            "day_of_week": dow_factor,
            "month_of_year": month_factor,
            "hour_of_day": hour_factor,
            "trend": trend_factor,
            "weather": weather_mod,
            "event": event_mod,
        }

        return ForecastResult(
            venue_id=self.venue_config.id,
            date=target_date,
            hour=target_hour,
            point_estimate=round(point_estimate, 1),
            confidence_interval_80=interval_80,
            confidence_interval_95=interval_95,
            components=components,
            confidence_score=confidence_score,
            model_version="enhanced_v2",
            signals_used=[SignalType.historical, SignalType.weather, SignalType.events],
            confidence=confidence_level,
            warning=cold_start_warning,
        )

    def forecast_week(
        self,
        week_start: date,
        weather_forecasts: Optional[dict[tuple[date, int], WeatherForecast]] = None,
        custom_events: Optional[list[dict]] = None,
    ) -> list[ForecastResult]:
        """
        Generate forecasts for a full week.

        Args:
            week_start: Monday of target week
            weather_forecasts: Optional dict of (date, hour) → WeatherForecast
            custom_events: Optional custom events

        Returns:
            List of 168 hourly forecasts (7 days × 24 hours)
        """
        results = []
        for day_offset in range(7):
            target_date = week_start + timedelta(days=day_offset)
            for hour in range(24):
                weather = None
                if weather_forecasts:
                    weather = weather_forecasts.get((target_date, hour))

                forecast = self.predict(
                    target_date,
                    hour,
                    weather=weather,
                    custom_events=custom_events,
                )
                results.append(forecast)

        return results

    def forecast_staffing_needs(
        self,
        target_date: date,
        # Keep in step with roster_optimiser.DEFAULT_COVERS_PER_STAFF (15.0):
        # the forecast→staffing ratio must match the optimiser, else recommended
        # vs rostered headcount disagree by ~25%.
        covers_per_staff: float = 15.0,
        weather: Optional[WeatherForecast] = None,
    ) -> list[dict]:
        """
        Convert demand forecasts to staffing recommendations.

        Args:
            target_date: Date to forecast
            covers_per_staff: Covers per full-time equivalent
            weather: Optional weather forecast

        Returns:
            List of {hour, recommended_staff, confidence} for each hour
        """
        results = []
        for hour in range(24):
            forecast = self.predict(target_date, hour, weather=weather)
            recommended = forecast.point_estimate / covers_per_staff

            result = {
                "date": str(target_date),
                "hour": hour,
                "recommended_staff": round(recommended, 1),
                "confidence": forecast.confidence_score,
                "confidence_level": forecast.confidence,
                "low": round(forecast.confidence_interval_80[0] / covers_per_staff, 1),
                "high": round(forecast.confidence_interval_80[1] / covers_per_staff, 1),
            }
            if forecast.warning:
                result["warning"] = forecast.warning
            results.append(result)

        return results

    def evaluate_accuracy(self, period_days: int = 30) -> AccuracyReport:
        """
        Evaluate prediction accuracy over a period.

        Args:
            period_days: How many recent days to evaluate

        Returns:
            AccuracyReport with MAPE, bias, worst days
        """
        if not self.predictions_log:
            return AccuracyReport(
                mape=0.0,
                bias=0.0,
                worst_days=[],
                evaluation_period_days=period_days,
                sample_count=0,
            )

        # Filter to period
        cutoff = (datetime.now() - timedelta(days=period_days)).timestamp()
        recent = [
            p for p in self.predictions_log
            if p.get("timestamp", 0) >= cutoff
        ]

        if not recent:
            return AccuracyReport(
                mape=0.0,
                bias=0.0,
                worst_days=[],
                evaluation_period_days=period_days,
                sample_count=0,
            )

        # Calculate MAPE and bias
        errors = []
        date_errors: dict[date, list[float]] = defaultdict(list)

        for point in recent:
            actual = float(point.get("actual", 0))
            predicted = float(point.get("predicted", 0))
            if actual > 0:
                pct_error = abs(actual - predicted) / actual
                errors.append(pct_error)
                date_errors[point.get("date")].append(pct_error)

        mape = mean(errors) if errors else 0.0
        bias = mean(
            (float(p["actual"]) - float(p["predicted"])) / float(p["actual"])
            for p in recent if p.get("actual", 0) > 0
        ) if recent else 0.0

        # Worst days
        worst_days = sorted(
            [(dt, mean(errs)) for dt, errs in date_errors.items()],
            key=lambda x: x[1],
            reverse=True,
        )[:5]

        return AccuracyReport(
            mape=mape,
            bias=bias,
            worst_days=worst_days,
            evaluation_period_days=period_days,
            sample_count=len(recent),
        )

    def log_prediction(
        self,
        predicted: float,
        actual: float,
        target_date: date,
    ) -> None:
        """Log a prediction for accuracy tracking."""
        self.predictions_log.append({
            "predicted": predicted,
            "actual": actual,
            "date": target_date,
            "timestamp": datetime.now().timestamp(),
        })

    # ======================================================================
    # Private Helpers
    # ======================================================================

    def _heuristic_predict(self, target_date: date, hour: int) -> float:
        """
        Fallback baseline prediction when no base forecaster is configured.

        This returns a seasonally-NEUTRAL demand level (the overall average
        covers across the training data). The day-of-week, hour-of-day, month
        and trend seasonal factors are applied once by ``predict`` on top of
        this baseline — so they must NOT be applied here as well, otherwise the
        seasonal signal is double-counted and point estimates drift well above
        the range the historical residuals were measured against.
        """
        if not self.seasonal_components:
            self.retrain()

        if self.historical_data:
            covers = [float(p.get("covers", 0)) for p in self.historical_data]
            covers = [c for c in covers if c >= 0]
            if covers:
                return mean(covers)

        # No history at all — fall back to a neutral default.
        return 50.0

    def _calculate_confidence_intervals(
        self,
        point_estimate: float,
        seasonal_pred: float,
    ) -> tuple[tuple[float, float], tuple[float, float]]:
        """
        Calculate confidence intervals as a multiplicative band around the
        point estimate, derived from the model's RELATIVE error distribution.

        Returns ((low_80, high_80), (low_95, high_95)).

        The key invariant is ``lower <= point_estimate <= upper`` for both
        bands, and the 95% band is at least as wide as the 80% band. To get
        that, the residuals must be relative errors of *comparable* predictions:
        for each recent history row we compute that row's OWN seasonal
        expectation and take ``residual = (actual - expected) / expected``.
        Because each row is compared against the prediction for its OWN
        day-of-week / hour-of-day, the residuals describe how far reality
        typically lands above or below the model — a scale-free quantity we can
        re-apply to *any* point estimate via ``point_estimate * (1 + r)``.

        Previously the code compared every raw history row (all hours/days
        mixed) against the single current ``seasonal_pred``, so the residuals
        captured the spread of the whole history rather than the model error.
        The resulting bounds were a fixed cover count (e.g. high_80==80 for
        every hour) decoupled from the point estimate, frequently leaving the
        point estimate outside its own interval.
        """
        if not self.historical_data:
            # No data: use ±25% and ±50%
            return (
                (max(0.0, point_estimate * 0.75), point_estimate * 1.25),
                (max(0.0, point_estimate * 0.5), point_estimate * 1.5),
            )

        # Relative residuals of comparable predictions: compare each recent row
        # against the seasonal expectation FOR THAT row's day/hour. The seasonal
        # factors are normalised around the overall mean cover count, so the
        # expectation for a row is overall_avg * dow * month * hour. Recompute
        # overall_avg the same way the decomposer does (mean covers, default 50).
        sc = self.seasonal_components
        _covers = [
            c for c in (float(p.get("covers", 0)) for p in self.historical_data)
            if c >= 0
        ]
        overall_avg = (mean(_covers) if _covers else 0.0) or 50.0
        residuals = []
        for point in self.historical_data[-100:]:  # Last 100 data points
            actual = float(point.get("covers", 0))
            dt = point.get("date")
            hour = int(point.get("hour", 0))
            if actual <= 0 or dt is None or sc is None:
                continue

            dow_factor = sc.day_of_week.get(dt.weekday(), 1.0)
            month_factor = sc.month_of_year.get(dt.month, 1.0)
            hour_factor = sc.hour_of_day.get(hour, 0.5)
            expected = overall_avg * dow_factor * month_factor * hour_factor
            if expected <= 0:
                continue

            residual = (actual - expected) / expected
            residuals.append(residual)

        if not residuals or len(residuals) < 10:
            # Fallback: use ±25% and ±50% (already correctly ordered).
            return (
                (max(0.0, point_estimate * 0.75), point_estimate * 1.25),
                (max(0.0, point_estimate * 0.5), point_estimate * 1.5),
            )

        # Percentile bounds of the relative-error distribution. The 80% band
        # spans the 10th–90th percentiles; the 95% band spans the wider
        # 2.5th–97.5th percentiles.
        n = len(residuals)
        residuals.sort()
        idx_2_5 = int(n * 0.025)
        idx_10 = int(n * 0.10)
        idx_90 = min(n - 1, int(n * 0.90))
        idx_97_5 = min(n - 1, int(n * 0.975))

        r_2_5 = residuals[idx_2_5]
        r_10 = residuals[idx_10]
        r_90 = residuals[idx_90]
        r_97_5 = residuals[idx_97_5]

        # Enforce the band always brackets the point estimate. The lower
        # percentile must not push the band above the point (r <= 0) and the
        # upper percentile must not pull it below (r >= 0); a perfectly
        # unbiased model already satisfies this, but clamp to be safe so the
        # ``lower <= point <= upper`` invariant cannot be violated by sampling
        # noise. The 95% band is then guaranteed at least as wide as the 80%.
        low_80 = point_estimate * (1 + min(0.0, r_10))
        high_80 = point_estimate * (1 + max(0.0, r_90))
        low_95 = point_estimate * (1 + min(0.0, r_2_5, r_10))
        high_95 = point_estimate * (1 + max(0.0, r_97_5, r_90))

        return (
            (max(0.0, low_80), max(point_estimate, high_80)),
            (max(0.0, low_95), max(point_estimate, high_95)),
        )

    def _calculate_confidence_score(self) -> float:
        """
        Calculate overall confidence (0-1).

        Based on training data availability and recentness.
        """
        if not self.seasonal_components:
            return 0.3

        training_days = self.seasonal_components.training_days
        days_since_train = (datetime.now() - self.seasonal_components.last_retrain).days

        # Base: more data = higher confidence
        data_confidence = min(training_days / 180, 1.0)

        # Freshness: newer training = higher confidence
        freshness = max(0.0, 1.0 - (days_since_train / 30))

        return (data_confidence * 0.7 + freshness * 0.3)

    def _classify_confidence(self, confidence_score: float) -> tuple[str, Optional[str]]:
        """
        Classify confidence level and generate cold-start warnings.

        Returns:
            (confidence_level, warning_message) tuple.
            confidence_level is one of "none", "low", "medium", "high".
            warning_message is None when data is sufficient.
        """
        training_days = 0
        if self.seasonal_components:
            training_days = self.seasonal_components.training_days

        cold_start_warning = (
            "Insufficient historical data for accurate predictions. "
            "Using venue baseline estimates."
        )

        if training_days < 7:
            # Less than 1 week of data
            return "none", cold_start_warning

        if training_days < 28:
            # 1-4 weeks of data
            return "low", cold_start_warning

        # 4+ weeks: evaluate MAPE to determine medium vs high
        accuracy = self.evaluate_accuracy(period_days=min(training_days, 30))
        if accuracy.sample_count > 0 and accuracy.mape < 0.15:
            return "high", None
        return "medium", None
