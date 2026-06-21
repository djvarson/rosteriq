"""
Ensemble demand forecasting module for RosterIQ.

Combines XGBoost (70% weight) and Prophet (30% weight) for demand prediction.
Falls back to cold-start heuristics when insufficient training data is available.

Cold-start mode activates when < 4 weeks of historical data exists,
using day-of-week averages and hour-of-day multipliers.

XGBoost and Prophet imports are conditional — the module works without them
installed, falling back to cold-start mode automatically.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Optional

from rosteriq.models import DemandForecast, SignalType, VenueConfig

# Conditional imports for ML libraries.
# Catch Exception, not just ImportError: native-backed libs (xgboost, prophet)
# can be installed but fail to load their shared objects (e.g. missing
# libomp/libgomp), which raises OSError at import. Treat that as "unavailable"
# and degrade gracefully rather than crashing the whole app at startup.
try:
    import xgboost as xgb
    HAS_XGBOOST = True
except Exception:
    HAS_XGBOOST = False

try:
    from prophet import Prophet
    HAS_PROPHET = True
except Exception:
    HAS_PROPHET = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


# Ensemble weights
XGBOOST_WEIGHT = 0.70
PROPHET_WEIGHT = 0.30
MIN_TRAINING_WEEKS = 4

# Cold-start defaults — average covers by day of week (Mon=0, Sun=6)
COLD_START_DAY_DEFAULTS = {
    0: 40,   # Monday
    1: 45,   # Tuesday
    2: 50,   # Wednesday
    3: 60,   # Thursday
    4: 80,   # Friday
    5: 90,   # Saturday
    6: 70,   # Sunday
}

# Hour-of-day multipliers for cold-start
COLD_START_HOUR_MULTIPLIERS = {
    range(0, 6): 0.1,     # late night / early morning
    range(6, 11): 0.5,    # morning
    range(11, 12): 0.9,   # pre-lunch
    range(12, 14): 1.3,   # lunch peak
    range(14, 15): 0.9,   # early afternoon
    range(15, 18): 0.7,   # afternoon
    range(18, 22): 1.5,   # dinner peak
    range(22, 24): 0.4,   # late night
}

# Default trading hours
DEFAULT_TRADING_HOURS = list(range(6, 24))  # 6am to midnight


def _get_hour_multiplier(hour: int) -> float:
    """Get the cold-start hour multiplier for a given hour."""
    for hour_range, multiplier in COLD_START_HOUR_MULTIPLIERS.items():
        if hour in hour_range:
            return multiplier
    return 0.5


class EnsembleForecaster:
    """
    Ensemble demand forecaster combining XGBoost and Prophet.

    Uses a 70/30 XGBoost/Prophet split for trained predictions,
    with automatic cold-start fallback when insufficient data exists.
    """

    def __init__(self, venue_config: VenueConfig):
        """
        Initialise the forecaster for a venue.

        Args:
            venue_config: Configuration for the venue being forecast.
        """
        self.venue_config = venue_config
        self.xgb_model = None
        self.prophet_model = None
        self.is_trained = False
        self.training_weeks = 0
        self.historical_data: list[dict] = []  # {date, hour, covers}
        self._day_averages: dict[int, dict[int, float]] = {}  # weekday -> hour -> avg covers

    def add_historical_data(self, data: list[dict]) -> None:
        """
        Add historical covers data for training.

        Each dict must contain:
        - date: date object
        - hour: int (0-23)
        - covers: float (number of covers/customers)

        Args:
            data: List of historical data points.
        """
        for entry in data:
            if not all(k in entry for k in ("date", "hour", "covers")):
                raise ValueError("Each data point must have 'date', 'hour', and 'covers'")
            self.historical_data.append(entry)

        # Recalculate day averages for cold-start
        self._recalculate_averages()

    def _recalculate_averages(self) -> None:
        """Recalculate day-of-week and hour averages from historical data."""
        from collections import defaultdict

        totals: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))

        for entry in self.historical_data:
            weekday = entry["date"].weekday()
            hour = entry["hour"]
            totals[weekday][hour].append(entry["covers"])

        self._day_averages = {}
        for weekday, hours in totals.items():
            self._day_averages[weekday] = {}
            for hour, values in hours.items():
                self._day_averages[weekday][hour] = sum(values) / len(values)

    def train(self) -> bool:
        """
        Train both models on historical data.

        Returns True if the full ensemble was trained (>= MIN_TRAINING_WEEKS
        of data). Returns False if falling back to cold-start heuristics.

        Returns:
            True if full ensemble trained, False if cold-start fallback.
        """
        if not self.historical_data:
            self.is_trained = False
            self.training_weeks = 0
            return False

        # Calculate how many weeks of data we have
        dates = sorted(set(entry["date"] for entry in self.historical_data))
        if len(dates) < 2:
            self.training_weeks = 0
            self.is_trained = False
            return False

        date_range = (dates[-1] - dates[0]).days
        self.training_weeks = max(1, date_range // 7)

        if self.training_weeks < MIN_TRAINING_WEEKS:
            self.is_trained = False
            return False

        # Train XGBoost if available
        if HAS_XGBOOST and HAS_PANDAS and HAS_NUMPY:
            self._train_xgboost()

        # Train Prophet if available
        if HAS_PROPHET and HAS_PANDAS:
            self._train_prophet()

        self.is_trained = (self.xgb_model is not None) or (self.prophet_model is not None)
        return self.is_trained

    def _train_xgboost(self) -> None:
        """Train XGBoost model on historical data."""
        if not HAS_PANDAS or not HAS_NUMPY or not HAS_XGBOOST:
            return

        df = pd.DataFrame(self.historical_data)
        df["weekday"] = df["date"].apply(lambda d: d.weekday())
        df["month"] = df["date"].apply(lambda d: d.month)
        df["day_of_month"] = df["date"].apply(lambda d: d.day)

        features = ["weekday", "hour", "month", "day_of_month"]
        X = df[features].values
        y = df["covers"].values

        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            objective="reg:squarederror",
        )
        model.fit(X, y)
        self.xgb_model = model

    def _train_prophet(self) -> None:
        """Train Prophet model on historical data."""
        if not HAS_PANDAS or not HAS_PROPHET:
            return

        # Prophet needs 'ds' and 'y' columns — aggregate by date
        from collections import defaultdict
        daily_totals = defaultdict(float)
        for entry in self.historical_data:
            daily_totals[entry["date"]] += entry["covers"]

        df = pd.DataFrame([
            {"ds": d, "y": total}
            for d, total in sorted(daily_totals.items())
        ])

        model = Prophet(daily_seasonality=False, weekly_seasonality=True)
        model.fit(df)
        self.prophet_model = model

    def predict(
        self,
        target_date: date,
        hours: list[int] = None,
    ) -> list[DemandForecast]:
        """
        Generate demand forecasts for a date.

        Uses the full ensemble if trained, or cold-start heuristics otherwise.

        Args:
            target_date: Date to forecast.
            hours: Specific hours to forecast (default: all trading hours).

        Returns:
            List of DemandForecast objects, one per hour.
        """
        if hours is None:
            hours = DEFAULT_TRADING_HOURS

        forecasts = []
        for hour in hours:
            if self.is_trained:
                predicted, confidence = self._ensemble_predict(target_date, hour)
                signals = [SignalType.historical]
                version = f"ensemble_v1_w{self.training_weeks}"
            else:
                predicted = self._cold_start_predict(target_date, hour)
                confidence = max(0.3, min(0.6, self.training_weeks * 0.1 + 0.2))
                signals = [SignalType.historical]
                version = "cold_start_v1"

            forecasts.append(
                DemandForecast(
                    id=str(uuid.uuid4()),
                    venue_id=self.venue_config.id,
                    date=target_date,
                    hour=hour,
                    predicted_covers=max(0.0, round(predicted, 1)),
                    confidence=round(confidence, 2),
                    signals_used=signals,
                    model_version=version,
                )
            )

        return forecasts

    def predict_week(self, week_start: date) -> list[DemandForecast]:
        """
        Generate forecasts for an entire week.

        Args:
            week_start: Monday of the target week.

        Returns:
            List of DemandForecast objects for all trading hours across 7 days.
        """
        all_forecasts = []
        for day_offset in range(7):
            target_date = week_start + timedelta(days=day_offset)
            all_forecasts.extend(self.predict(target_date))
        return all_forecasts

    def _cold_start_predict(self, target_date: date, hour: int) -> float:
        """
        Heuristic prediction when insufficient training data.

        Uses day-of-week averages if historical data exists,
        otherwise falls back to venue-type defaults.

        Args:
            target_date: Date to predict.
            hour: Hour to predict.

        Returns:
            Predicted covers count.
        """
        weekday = target_date.weekday()
        hour_mult = _get_hour_multiplier(hour)

        # Use historical averages if we have any data for this day/hour
        if weekday in self._day_averages and hour in self._day_averages[weekday]:
            return self._day_averages[weekday][hour]

        # Use historical day average with hour multiplier
        if weekday in self._day_averages:
            day_avg = sum(self._day_averages[weekday].values()) / max(
                len(self._day_averages[weekday]), 1
            )
            return day_avg * hour_mult

        # Pure default
        base = COLD_START_DAY_DEFAULTS.get(weekday, 50)
        return base * hour_mult

    def _ensemble_predict(
        self, target_date: date, hour: int
    ) -> tuple[float, float]:
        """
        Combined prediction from both models.

        Returns (predicted_covers, confidence). Confidence is based on model
        agreement — closer predictions yield higher confidence.

        Args:
            target_date: Date to predict.
            hour: Hour to predict.

        Returns:
            Tuple of (predicted_covers, confidence).
        """
        xgb_pred = None
        prophet_pred = None

        # XGBoost prediction
        if self.xgb_model is not None and HAS_NUMPY:
            features = np.array([[
                target_date.weekday(),
                hour,
                target_date.month,
                target_date.day,
            ]])
            xgb_pred = float(self.xgb_model.predict(features)[0])

        # Prophet prediction
        if self.prophet_model is not None and HAS_PANDAS:
            future = pd.DataFrame({"ds": [target_date]})
            forecast = self.prophet_model.predict(future)
            daily_total = float(forecast["yhat"].iloc[0])
            # Distribute daily total across hours using hour multiplier
            prophet_pred = daily_total * _get_hour_multiplier(hour)

        # Combine predictions
        if xgb_pred is not None and prophet_pred is not None:
            combined = xgb_pred * XGBOOST_WEIGHT + prophet_pred * PROPHET_WEIGHT

            # Confidence based on model agreement
            diff = abs(xgb_pred - prophet_pred)
            avg = (xgb_pred + prophet_pred) / 2.0
            if avg > 0:
                agreement = 1.0 - min(diff / avg, 1.0)
            else:
                agreement = 0.5
            confidence = 0.5 + agreement * 0.4  # range 0.5 to 0.9

            return combined, confidence
        elif xgb_pred is not None:
            return xgb_pred, 0.65
        elif prophet_pred is not None:
            return prophet_pred, 0.55
        else:
            # Fallback to cold start
            return self._cold_start_predict(target_date, hour), 0.3

    def get_model_status(self) -> dict:
        """
        Get current model status.

        Returns:
            Dict with: is_trained, training_weeks, has_xgboost, has_prophet,
            data_points, model_version.
        """
        return {
            "is_trained": self.is_trained,
            "training_weeks": self.training_weeks,
            "has_xgboost": self.xgb_model is not None,
            "has_prophet": self.prophet_model is not None,
            "data_points": len(self.historical_data),
            "model_version": (
                f"ensemble_v1_w{self.training_weeks}" if self.is_trained
                else "cold_start_v1"
            ),
            "xgboost_available": HAS_XGBOOST,
            "prophet_available": HAS_PROPHET,
        }
