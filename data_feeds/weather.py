"""
Weather data feed adapters for RosterIQ.

Integrates with:
- Bureau of Meteorology (BOM) - Australia's official weather service
- OpenWeatherMap - fallback global weather provider

Weather signals map directly to demand impacts:
- Heavy rain/storms → strong negative (people stay home)
- Light rain → weak negative
- Extreme heat (>40°C) → moderate negative (too hot for beer gardens)
- Perfect weather (20-28°C, sunny) → moderate positive (outdoor dining booms)
- Cold (<10°C) → weak negative
- Mild and clear → weak positive

Confidence decreases for forecasts further in the future:
- Today: 0.95
- Tomorrow: 0.8
- Up to 7 days: 0.5
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx

from rosteriq.data_feeds.base import (
    DataFeedAdapter,
    FeedCategory,
    FeedSignal,
    Location,
    SignalCache,
    SignalStrength,
)

logger = logging.getLogger(__name__)


# ============================================================================
# BOM Weather Adapter
# ============================================================================


class BOMWeatherAdapter(DataFeedAdapter):
    """
    Australian Bureau of Meteorology weather data adapter.

    Fetches forecast data from BOM's public API (no authentication required).
    Provides hourly and daily forecasts with high accuracy for Australian venues.

    API Base: http://www.bom.gov.au/fwo/
    """

    BOM_BASE_URL = "http://www.bom.gov.au/fwo"
    REQUEST_TIMEOUT = 10
    MAX_FORECAST_DAYS = 7

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize BOM weather adapter.

        Args:
            cache: Optional SignalCache instance (defaults to new one).
        """
        super().__init__(api_key=None, cache=cache)
        self.client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        """BOM weather is a weather feed."""
        return FeedCategory.weather

    @property
    def source_name(self) -> str:
        """Unique identifier for BOM data source."""
        return "bom_weather"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch weather signals from BOM for a location and date range.

        Args:
            location: Venue geographic location (lat/lon required).
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects with weather data and demand impacts.
        """
        signals: list[FeedSignal] = []

        try:
            # Check cache first
            cached = self._check_cache(location, start_date, end_date)
            if cached:
                logger.debug(f"BOM: {len(cached)} signals from cache for {location.address}")
                return cached

            # Fetch from BOM API
            async with httpx.AsyncClient(timeout=self.REQUEST_TIMEOUT) as client:
                forecast_data = await self._fetch_forecast(client, location)

            if not forecast_data:
                logger.warning(f"BOM: No data returned for {location.suburb}, {location.state}")
                return []

            # Parse forecast and convert to signals
            current_date = start_date
            while current_date <= end_date:
                day_signals = self._parse_forecast_day(
                    forecast_data, current_date, location, venue_id
                )
                signals.extend(day_signals)
                current_date += timedelta(days=1)

            logger.info(f"BOM: Generated {len(signals)} signals for {location.address}")
            return signals

        except httpx.TimeoutException:
            logger.warning(f"BOM API timeout for {location.address}")
            return []
        except httpx.RequestError as e:
            logger.warning(f"BOM API error for {location.address}: {e}")
            return []
        except Exception as e:
            logger.error(f"BOM adapter error for {location.address}: {e}", exc_info=True)
            return []

    async def _fetch_forecast(self, client: httpx.AsyncClient, location: Location) -> dict[str, Any]:
        """
        Fetch raw forecast data from BOM API.

        Args:
            client: httpx AsyncClient for making requests.
            location: Venue location with latitude/longitude.

        Returns:
            Parsed JSON response from BOM API, or empty dict if failed.
        """
        try:
            # BOM API pattern: /[product_id]/[state_area]/latest.json
            # For simplicity, we construct a reasonable API endpoint.
            # In production, you'd map lat/lon to the correct BOM product ID.
            state = location.state.upper() if location.state else "NSW"
            suburb = location.suburb.replace(" ", "_") if location.suburb else "SYDNEY"

            # BOM URL format (simplified for demonstration)
            url = f"{self.BOM_BASE_URL}/IDN10064/latest.json"

            response = await client.get(url)
            response.raise_for_status()

            data = response.json()
            logger.debug(f"BOM API response received for {location.address}")
            return data

        except Exception as e:
            logger.warning(f"Failed to fetch BOM forecast for {location.address}: {e}")
            return {}

    def _parse_forecast_day(
        self,
        forecast_data: dict[str, Any],
        target_date: date,
        location: Location,
        venue_id: Optional[str],
    ) -> list[FeedSignal]:
        """
        Parse one day of forecast data and generate signals.

        Args:
            forecast_data: Raw forecast data from BOM API.
            target_date: Date to parse signals for.
            location: Venue location.
            venue_id: Optional venue ID.

        Returns:
            List of FeedSignal objects for this date.
        """
        signals: list[FeedSignal] = []

        try:
            # Extract forecast entries for target_date from BOM data
            # BOM response typically includes "observations" and forecast arrays
            forecasts = forecast_data.get("forecasts", [])

            for forecast_entry in forecasts:
                forecast_date_str = forecast_entry.get("date", "")
                if not forecast_date_str:
                    continue

                try:
                    forecast_date = datetime.strptime(forecast_date_str, "%Y%m%d").date()
                except (ValueError, TypeError):
                    continue

                if forecast_date != target_date:
                    continue

                # Extract weather parameters
                temp_min = float(forecast_entry.get("min_temp", 20))
                temp_max = float(forecast_entry.get("max_temp", 25))
                temp_avg = (temp_min + temp_max) / 2

                rain_prob = float(forecast_entry.get("rain_prob", 0)) / 100.0
                rain_amount = float(forecast_entry.get("rain", 0))
                wind_speed = float(forecast_entry.get("wind_speed_kmh", 0))
                wind_gust = float(forecast_entry.get("wind_gust_kmh", 0))
                uv_index = float(forecast_entry.get("uv", 0))
                condition = forecast_entry.get("condition", "unknown").lower()

                # Days into forecast
                today = date.today()
                days_ahead = (forecast_date - today).days
                confidence = self._calculate_confidence(days_ahead)

                # Determine signal strength and description based on conditions
                strength, description = self._assess_weather_impact(
                    temp_avg, temp_max, rain_prob, rain_amount, wind_speed, condition
                )

                # Build raw_data with all weather parameters
                raw_data = {
                    "temperature_min": temp_min,
                    "temperature_max": temp_max,
                    "temperature_avg": temp_avg,
                    "rain_probability": rain_prob,
                    "rain_amount_mm": rain_amount,
                    "wind_speed_kmh": wind_speed,
                    "wind_gust_kmh": wind_gust,
                    "uv_index": uv_index,
                    "condition": condition,
                    "forecast_date": forecast_date_str,
                }

                signal = self._make_signal(
                    signal_date=forecast_date,
                    strength=strength,
                    description=description,
                    confidence=confidence,
                    hour=None,  # All-day signal
                    raw_data=raw_data,
                    venue_id=venue_id,
                    ttl_minutes=60 if days_ahead == 0 else 360,  # Hourly for today, 6h for future
                )
                signals.append(signal)
                logger.debug(f"BOM signal: {forecast_date} {strength.value} ({description})")

        except Exception as e:
            logger.error(f"Error parsing BOM forecast day: {e}", exc_info=True)

        return signals

    def _assess_weather_impact(
        self,
        temp_avg: float,
        temp_max: float,
        rain_prob: float,
        rain_amount: float,
        wind_speed: float,
        condition: str,
    ) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand impact from weather conditions.

        Args:
            temp_avg: Average temperature (°C).
            temp_max: Maximum temperature (°C).
            rain_prob: Probability of rain (0-1).
            rain_amount: Expected rain amount (mm).
            wind_speed: Wind speed (km/h).
            condition: Weather condition string (e.g., "rainy", "sunny").

        Returns:
            Tuple of (SignalStrength, description string).
        """
        # Check for heavy rain or storms
        if rain_amount > 20 or (rain_prob > 0.8 and rain_amount > 5):
            return (
                SignalStrength.strong_negative,
                f"Heavy rain ({rain_amount:.1f}mm expected) - people stay home",
            )

        # Check for light rain
        if rain_amount > 1 or (rain_prob > 0.5 and rain_amount > 0):
            return (
                SignalStrength.weak_negative,
                f"Light rain ({rain_amount:.1f}mm, {rain_prob*100:.0f}% chance)",
            )

        # Check for extreme heat
        if temp_max > 40:
            return (
                SignalStrength.moderate_negative,
                f"Extreme heat ({temp_max:.1f}°C) - too hot for outdoor dining",
            )

        # Check for moderate heat
        if temp_max > 35:
            return (
                SignalStrength.weak_negative,
                f"Hot weather ({temp_max:.1f}°C) - moderate impact on outdoor venues",
            )

        # Check for cold
        if temp_avg < 10:
            return (
                SignalStrength.weak_negative,
                f"Cold ({temp_avg:.1f}°C) - fewer outdoor diners",
            )

        # Perfect weather for outdoor hospitality
        if 20 <= temp_avg <= 28 and rain_prob < 0.2 and "sunny" in condition:
            return (
                SignalStrength.moderate_positive,
                f"Perfect weather ({temp_avg:.1f}°C, sunny) - outdoor dining booms",
            )

        # Mild and clear
        if 15 <= temp_avg <= 25 and rain_prob < 0.3 and "clear" in condition:
            return (
                SignalStrength.weak_positive,
                f"Mild and clear ({temp_avg:.1f}°C) - good outdoor conditions",
            )

        # Mild weather (default)
        if 12 <= temp_avg <= 28 and rain_prob < 0.4:
            return (
                SignalStrength.weak_positive,
                f"Mild weather ({temp_avg:.1f}°C) - decent outdoor dining",
            )

        # Default neutral
        return (SignalStrength.neutral, f"Neutral conditions ({temp_avg:.1f}°C, {rain_prob*100:.0f}% rain)")

    def _calculate_confidence(self, days_ahead: int) -> float:
        """
        Calculate forecast confidence based on how far in the future.

        Args:
            days_ahead: Number of days from today (0 = today, 1 = tomorrow, etc.).

        Returns:
            Confidence value (0-1).
        """
        if days_ahead < 0:
            return 0.95  # Historical data
        if days_ahead == 0:
            return 0.95  # Today
        if days_ahead == 1:
            return 0.8  # Tomorrow
        if days_ahead <= 3:
            return 0.7  # Next 3 days
        if days_ahead <= 7:
            return 0.5  # Up to 7 days
        return 0.3  # Beyond 7 days (low confidence)

    def _check_cache(
        self, location: Location, start_date: date, end_date: date
    ) -> list[FeedSignal]:
        """
        Check cache for existing signals in date range.

        Args:
            location: Venue location.
            start_date: First date.
            end_date: Last date.

        Returns:
            List of cached signals if all dates covered, else empty list.
        """
        # Simplified cache check - in production, could be more sophisticated
        return []


# ============================================================================
# OpenWeatherMap Weather Adapter
# ============================================================================


class OpenWeatherMapAdapter(DataFeedAdapter):
    """
    OpenWeatherMap weather data adapter.

    Fallback weather provider using OpenWeatherMap API.
    Requires API key (free tier available).

    API Base: https://api.openweathermap.org/data/2.5/
    """

    OWM_BASE_URL = "https://api.openweathermap.org/data/2.5"
    REQUEST_TIMEOUT = 10
    MAX_FORECAST_DAYS = 5  # Free tier limitation

    def __init__(self, api_key: str, cache: Optional[SignalCache] = None):
        """
        Initialize OpenWeatherMap adapter.

        Args:
            api_key: OpenWeatherMap API key (required).
            cache: Optional SignalCache instance (defaults to new one).

        Raises:
            ValueError: If api_key is empty or None.
        """
        if not api_key:
            raise ValueError("OpenWeatherMap API key is required")
        super().__init__(api_key=api_key, cache=cache)

    @property
    def category(self) -> FeedCategory:
        """OpenWeatherMap is a weather feed."""
        return FeedCategory.weather

    @property
    def source_name(self) -> str:
        """Unique identifier for OpenWeatherMap data source."""
        return "openweathermap"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch weather signals from OpenWeatherMap for a location and date range.

        Args:
            location: Venue geographic location (lat/lon required).
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects with weather data and demand impacts.
        """
        signals: list[FeedSignal] = []

        try:
            # Validate coordinates
            if not location.latitude or not location.longitude:
                logger.warning(f"OpenWeatherMap: Missing coordinates for {location.address}")
                return []

            # Fetch from OWM API
            async with httpx.AsyncClient(timeout=self.REQUEST_TIMEOUT) as client:
                forecast_data = await self._fetch_forecast(client, location)

            if not forecast_data:
                logger.warning(f"OpenWeatherMap: No data returned for {location.address}")
                return []

            # Parse forecast and convert to signals
            current_date = start_date
            while current_date <= end_date:
                day_signals = self._parse_forecast_day(
                    forecast_data, current_date, location, venue_id
                )
                signals.extend(day_signals)
                current_date += timedelta(days=1)

            logger.info(f"OpenWeatherMap: Generated {len(signals)} signals for {location.address}")
            return signals

        except httpx.TimeoutException:
            logger.warning(f"OpenWeatherMap API timeout for {location.address}")
            return []
        except httpx.RequestError as e:
            logger.warning(f"OpenWeatherMap API error for {location.address}: {e}")
            return []
        except Exception as e:
            logger.error(f"OpenWeatherMap adapter error for {location.address}: {e}", exc_info=True)
            return []

    async def _fetch_forecast(self, client: httpx.AsyncClient, location: Location) -> dict[str, Any]:
        """
        Fetch raw forecast data from OpenWeatherMap API.

        Args:
            client: httpx AsyncClient for making requests.
            location: Venue location with latitude/longitude.

        Returns:
            Parsed JSON response from OWM API, or empty dict if failed.
        """
        try:
            # Use forecast endpoint (5-day, 3-hour intervals on free tier)
            url = f"{self.OWM_BASE_URL}/forecast"
            params = {
                "lat": location.latitude,
                "lon": location.longitude,
                "appid": self.api_key,
                "units": "metric",  # Celsius
            }

            response = await client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            logger.debug(f"OpenWeatherMap API response received for {location.address}")
            return data

        except Exception as e:
            logger.warning(f"Failed to fetch OpenWeatherMap forecast for {location.address}: {e}")
            return {}

    def _parse_forecast_day(
        self,
        forecast_data: dict[str, Any],
        target_date: date,
        location: Location,
        venue_id: Optional[str],
    ) -> list[FeedSignal]:
        """
        Parse one day of forecast data and generate signals.

        Args:
            forecast_data: Raw forecast data from OWM API.
            target_date: Date to parse signals for.
            location: Venue location.
            venue_id: Optional venue ID.

        Returns:
            List of FeedSignal objects for this date.
        """
        signals: list[FeedSignal] = []

        try:
            # OWM response includes "list" of 3-hour forecasts
            forecast_list = forecast_data.get("list", [])
            day_forecasts = []

            for forecast_entry in forecast_list:
                timestamp = forecast_entry.get("dt", 0)
                forecast_datetime = datetime.fromtimestamp(timestamp)
                forecast_date = forecast_datetime.date()

                if forecast_date == target_date:
                    day_forecasts.append(forecast_entry)

            if not day_forecasts:
                return signals

            # Aggregate day's forecast data
            temps = [f.get("main", {}).get("temp", 20) for f in day_forecasts]
            rain_amounts = [f.get("rain", {}).get("3h", 0) for f in day_forecasts]
            wind_speeds = [f.get("wind", {}).get("speed", 0) for f in day_forecasts]
            conditions = [f.get("weather", [{}])[0].get("main", "Unknown") for f in day_forecasts]

            temp_min = min(temps) if temps else 20
            temp_max = max(temps) if temps else 25
            temp_avg = sum(temps) / len(temps) if temps else 22.5

            total_rain = sum(rain_amounts)
            rain_prob = min(1.0, len([r for r in rain_amounts if r > 0]) / len(day_forecasts))
            avg_wind = sum(wind_speeds) / len(wind_speeds) if wind_speeds else 0
            dominant_condition = max(set(conditions), key=conditions.count) if conditions else "Unknown"

            # Days into forecast
            today = date.today()
            days_ahead = (target_date - today).days
            confidence = self._calculate_confidence(days_ahead)

            # Determine signal strength and description
            strength, description = self._assess_weather_impact(
                temp_avg, temp_max, rain_prob, total_rain, avg_wind, dominant_condition.lower()
            )

            # Build raw_data with all weather parameters
            raw_data = {
                "temperature_min": temp_min,
                "temperature_max": temp_max,
                "temperature_avg": temp_avg,
                "rain_probability": rain_prob,
                "rain_amount_mm": total_rain,
                "wind_speed_ms": avg_wind,
                "wind_speed_kmh": avg_wind * 3.6,  # Convert m/s to km/h
                "condition": dominant_condition,
                "forecast_count": len(day_forecasts),
            }

            signal = self._make_signal(
                signal_date=target_date,
                strength=strength,
                description=description,
                confidence=confidence,
                hour=None,  # All-day signal
                raw_data=raw_data,
                venue_id=venue_id,
                ttl_minutes=60 if days_ahead == 0 else 360,  # Hourly for today, 6h for future
            )
            signals.append(signal)
            logger.debug(f"OpenWeatherMap signal: {target_date} {strength.value} ({description})")

        except Exception as e:
            logger.error(f"Error parsing OpenWeatherMap forecast day: {e}", exc_info=True)

        return signals

    def _assess_weather_impact(
        self,
        temp_avg: float,
        temp_max: float,
        rain_prob: float,
        rain_amount: float,
        wind_speed: float,
        condition: str,
    ) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand impact from weather conditions.

        Args:
            temp_avg: Average temperature (°C).
            temp_max: Maximum temperature (°C).
            rain_prob: Probability of rain (0-1).
            rain_amount: Expected rain amount (mm).
            wind_speed: Wind speed (m/s).
            condition: Weather condition string (e.g., "rainy", "sunny").

        Returns:
            Tuple of (SignalStrength, description string).
        """
        # Check for heavy rain or storms
        if rain_amount > 20 or (rain_prob > 0.8 and rain_amount > 5):
            return (
                SignalStrength.strong_negative,
                f"Heavy rain ({rain_amount:.1f}mm expected) - people stay home",
            )

        # Check for light rain
        if rain_amount > 1 or (rain_prob > 0.5 and rain_amount > 0):
            return (
                SignalStrength.weak_negative,
                f"Light rain ({rain_amount:.1f}mm, {rain_prob*100:.0f}% chance)",
            )

        # Check for extreme heat
        if temp_max > 40:
            return (
                SignalStrength.moderate_negative,
                f"Extreme heat ({temp_max:.1f}°C) - too hot for outdoor dining",
            )

        # Check for moderate heat
        if temp_max > 35:
            return (
                SignalStrength.weak_negative,
                f"Hot weather ({temp_max:.1f}°C) - moderate impact on outdoor venues",
            )

        # Check for cold
        if temp_avg < 10:
            return (
                SignalStrength.weak_negative,
                f"Cold ({temp_avg:.1f}°C) - fewer outdoor diners",
            )

        # Perfect weather for outdoor hospitality
        if 20 <= temp_avg <= 28 and rain_prob < 0.2 and "sunny" in condition:
            return (
                SignalStrength.moderate_positive,
                f"Perfect weather ({temp_avg:.1f}°C, sunny) - outdoor dining booms",
            )

        # Mild and clear
        if 15 <= temp_avg <= 25 and rain_prob < 0.3 and "clear" in condition:
            return (
                SignalStrength.weak_positive,
                f"Mild and clear ({temp_avg:.1f}°C) - good outdoor conditions",
            )

        # Mild weather (default)
        if 12 <= temp_avg <= 28 and rain_prob < 0.4:
            return (
                SignalStrength.weak_positive,
                f"Mild weather ({temp_avg:.1f}°C) - decent outdoor dining",
            )

        # Default neutral
        return (SignalStrength.neutral, f"Neutral conditions ({temp_avg:.1f}°C, {rain_prob*100:.0f}% rain)")

    def _calculate_confidence(self, days_ahead: int) -> float:
        """
        Calculate forecast confidence based on how far in the future.

        Args:
            days_ahead: Number of days from today (0 = today, 1 = tomorrow, etc.).

        Returns:
            Confidence value (0-1).
        """
        if days_ahead < 0:
            return 0.95  # Historical data
        if days_ahead == 0:
            return 0.95  # Today
        if days_ahead == 1:
            return 0.8  # Tomorrow
        if days_ahead <= 3:
            return 0.7  # Next 3 days
        if days_ahead <= 5:
            return 0.5  # Up to 5 days (OWM free tier limit)
        return 0.3  # Beyond 5 days (low confidence)


# ============================================================================
# Factory function
# ============================================================================


def get_weather_adapter(api_key: Optional[str] = None) -> DataFeedAdapter:
    """
    Factory function to get appropriate weather adapter.

    Returns BOM adapter by default (no API key required).
    Returns OpenWeatherMap adapter if api_key is provided.

    Args:
        api_key: Optional OpenWeatherMap API key. If provided, OWM adapter is used.

    Returns:
        Configured DataFeedAdapter instance (BOM or OpenWeatherMap).

    Examples:
        # Use Bureau of Meteorology (default, no auth required)
        adapter = get_weather_adapter()

        # Use OpenWeatherMap with API key
        adapter = get_weather_adapter(api_key="your_api_key_here")
    """
    if api_key:
        logger.info("Creating OpenWeatherMap weather adapter")
        return OpenWeatherMapAdapter(api_key=api_key)
    else:
        logger.info("Creating Bureau of Meteorology weather adapter (default)")
        return BOMWeatherAdapter()
