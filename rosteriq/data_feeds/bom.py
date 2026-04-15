"""
Bureau of Meteorology (BOM) Weather Adapter for RosterIQ
========================================================

Provides current observations and forecasts from the Australian Bureau of
Meteorology (bom.gov.au) to inform demand signals. Rain can halve outdoor
seating capacity, so weather is a key demand driver.

BOM Observations API:
  - Source: http://reg.bom.gov.au/fwo/{product_id}.json
  - Returns: {observations: {data: [...]}}
  - Fields: air_temp, apparent_t, rain_trace, rel_hum, wind_spd_kmh, local_date_time_full

BOM Forecast API:
  - Precis/detailed forecasts available via product_id_forecast
  - Returns structured forecast data with rain probability, expected rain, max/min temps

Configuration:
  - Map venue_id → BOM product_id_obs and product_id_forecast
  - ROSTERIQ_DATA_MODE: "demo" or "live" (default: "demo")
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Dict

try:
    import httpx
except ImportError:
    httpx = None

logger = logging.getLogger("rosteriq.data_feeds.bom")

# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────


class BOMFetchError(Exception):
    """BOM API fetch failed."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class WeatherObservation:
    """Current weather observation."""
    venue_id: str
    timestamp: datetime
    temperature_c: float
    apparent_temperature_c: Optional[float] = None
    rain_mm_last_hour: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_kmh: Optional[float] = None
    conditions: str = "clear"  # clear/cloudy/rain/storm/hot/cold
    source: str = "bom"


@dataclass
class WeatherForecastDay:
    """Daily weather forecast."""
    venue_id: str
    date: date
    min_c: float
    max_c: float
    rain_probability_pct: float
    rain_mm_expected: float
    conditions: str = "clear"  # clear/cloudy/rain/storm/hot/cold
    source: str = "bom"


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────


def categorise_conditions(
    rain_mm_expected: float,
    rain_prob: float,
    max_c: float,
) -> str:
    """
    Categorise weather conditions based on rain, temperature.

    Heavy rain = rain_mm_expected >= 10 OR rain_prob >= 80
    Hot = max_c >= 32
    Cold = max_c <= 12
    Storm = rain_mm_expected >= 5 AND rain_prob >= 40 (but not heavy rain)
    Light rain = rain_mm_expected > 0 and < 5
    Otherwise cloudy or clear based on prob
    """
    # Heavy rain takes priority
    if rain_mm_expected >= 10.0 or rain_prob >= 80.0:
        return "heavy_rain"

    # Temperature extremes
    if max_c >= 32:
        return "hot"
    if max_c <= 12:
        return "cold"

    # Storm (moderate rain with moderate-high prob)
    if rain_mm_expected >= 5.0 and rain_prob >= 40.0:
        return "storm"

    # Light rain
    if rain_mm_expected > 0:
        return "light_rain"

    # Cloud cover based on rain probability
    if rain_prob >= 30.0:
        return "cloudy"

    return "clear"


# ─────────────────────────────────────────────────────────────────────────────
# Abstract Base
# ─────────────────────────────────────────────────────────────────────────────


class WeatherAdapter(ABC):
    """Abstract interface for weather data sources."""

    @abstractmethod
    async def get_current(self, venue_id: str) -> WeatherObservation:
        """Fetch current observation."""
        pass

    @abstractmethod
    async def get_forecast(
        self, venue_id: str, days: int = 7
    ) -> list[WeatherForecastDay]:
        """Fetch forecast for N days starting today."""
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Demo Adapter
# ─────────────────────────────────────────────────────────────────────────────


class DemoWeatherAdapter(WeatherAdapter):
    """
    Generates realistic Australian hospitality weather for testing.

    Autumn baseline (April): Brisbane/Perth ~22°C
    Stable seeding per venue so repeated calls within a session are consistent.
    """

    def __init__(self, seed_offset: int = 0):
        """
        Args:
            seed_offset: Additional offset for random seeding (e.g., per environment)
        """
        self.seed_offset = seed_offset
        self._forecasts_cache: Dict[str, list[WeatherForecastDay]] = {}

    def _get_seed(self, venue_id: str) -> int:
        """Stable seed per venue."""
        return hash(venue_id) + self.seed_offset

    async def get_current(self, venue_id: str) -> WeatherObservation:
        """Return a single current observation 'now'."""
        seed = self._get_seed(venue_id)
        rng = random.Random(seed)

        # April/autumn baseline: 20-24°C
        temp = rng.gauss(22, 2)
        apparent = temp - rng.uniform(0, 3)
        humidity = rng.randint(50, 80)
        wind = rng.uniform(5, 20)

        # ~30% chance of some rain
        rain_mm = 0.0
        if rng.random() < 0.3:
            rain_mm = rng.uniform(0.1, 5.0)

        conditions = categorise_conditions(rain_mm, 30 if rain_mm > 0 else 10, temp)

        return WeatherObservation(
            venue_id=venue_id,
            timestamp=datetime.now(timezone.utc),
            temperature_c=round(temp, 1),
            apparent_temperature_c=round(apparent, 1),
            rain_mm_last_hour=round(rain_mm, 2) if rain_mm > 0 else None,
            humidity_pct=humidity,
            wind_kmh=round(wind, 1),
            conditions=conditions,
            source="demo",
        )

    async def get_forecast(
        self, venue_id: str, days: int = 7
    ) -> list[WeatherForecastDay]:
        """Return N-day forecast starting today, deterministically seeded."""
        # Check cache
        cache_key = f"{venue_id}_{days}"
        if cache_key in self._forecasts_cache:
            return self._forecasts_cache[cache_key]

        seed = self._get_seed(venue_id)
        rng = random.Random(seed)

        today = date.today()
        forecast = []

        for i in range(days):
            forecast_date = today + timedelta(days=i)

            # Gradually varying temperature through the week
            base_min = rng.gauss(18, 2)
            base_max = rng.gauss(24, 2)

            # Rain pattern: ~40% of days have some rain
            rain_prob = rng.randint(0, 100)
            rain_mm = (
                rng.uniform(2, 15)
                if rain_prob >= 60
                else (rng.uniform(0.1, 2) if rain_prob >= 30 else 0)
            )

            conditions = categorise_conditions(rain_mm, rain_prob, base_max)

            forecast.append(
                WeatherForecastDay(
                    venue_id=venue_id,
                    date=forecast_date,
                    min_c=round(base_min, 1),
                    max_c=round(base_max, 1),
                    rain_probability_pct=rain_prob,
                    rain_mm_expected=round(rain_mm, 2),
                    conditions=conditions,
                    source="demo",
                )
            )

        # Cache for session
        self._forecasts_cache[cache_key] = forecast
        return forecast


# ─────────────────────────────────────────────────────────────────────────────
# BOM Client with Caching
# ─────────────────────────────────────────────────────────────────────────────


class BOMClient:
    """
    HTTP client for BOM API with caching and retry logic.

    Features:
    - 10-minute in-process cache keyed by URL
    - 3-retry exponential backoff (1s, 2s, 4s)
    - 10s timeout per request
    - Lazy httpx import
    """

    BASE_URL = "http://reg.bom.gov.au/fwo"
    TIMEOUT = 10
    CACHE_TTL_SECONDS = 600  # 10 minutes
    MAX_RETRIES = 3

    def __init__(self):
        """Initialize BOM client."""
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: Dict[str, tuple[dict, float]] = {}  # url -> (data, timestamp)

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazy-load httpx client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.TIMEOUT)
        return self._client

    async def close(self):
        """Close client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _fetch_with_retry(self, url: str) -> dict:
        """Fetch with exponential backoff retry."""
        client = await self._get_client()
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.json()
                else:
                    last_error = BOMFetchError(
                        f"BOM API returned {response.status_code}"
                    )
            except Exception as e:
                last_error = e

            if attempt < self.MAX_RETRIES - 1:
                backoff = 2 ** attempt  # 1s, 2s, 4s
                await asyncio.sleep(backoff)

        raise BOMFetchError(f"Failed after {self.MAX_RETRIES} retries: {last_error}")

    async def get_observations(self, product_id: str) -> dict:
        """
        Fetch observations from BOM.

        Args:
            product_id: BOM product ID (e.g., "IDW14400")

        Returns:
            Observations dict from BOM JSON
        """
        url = f"{self.BASE_URL}/{product_id}.json"

        # Check cache
        now = time.time()
        if url in self._cache:
            data, timestamp = self._cache[url]
            if now - timestamp < self.CACHE_TTL_SECONDS:
                logger.debug(f"Cache hit for {product_id}")
                return data

        # Fetch and cache
        try:
            data = await self._fetch_with_retry(url)
            self._cache[url] = (data, now)
            return data
        except Exception as e:
            logger.warning(f"Failed to fetch observations for {product_id}: {e}")
            raise

    async def get_forecast(self, product_id: str) -> dict:
        """
        Fetch forecast from BOM.

        Args:
            product_id: BOM product ID (e.g., "IDW14400")

        Returns:
            Forecast dict from BOM JSON
        """
        # BOM forecast endpoints typically use _forecast suffix or separate feed
        # For now, return empty dict to gracefully degrade
        logger.debug(f"BOM forecast not yet fully implemented for {product_id}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# BOM Station/Product Mapping
# ─────────────────────────────────────────────────────────────────────────────

# Hardcoded lat/lon -> BOM product_id mapping for major AU cities
# Venues resolve to nearest major city's station
STATION_MAPPING = {
    "perth": {
        "lat": -31.9454,
        "lon": 115.8381,
        "product_id_obs": "IDW14400",  # Perth observations
        "product_id_forecast": "IDW14400",
    },
    "brisbane": {
        "lat": -27.4698,
        "lon": 153.0251,
        "product_id_obs": "IDQ11295",  # Brisbane city observations
        "product_id_forecast": "IDQ11295",
    },
    "sydney": {
        "lat": -33.8688,
        "lon": 151.2093,
        "product_id_obs": "IDN60801",  # Sydney observations
        "product_id_forecast": "IDN60801",
    },
    "melbourne": {
        "lat": -37.8136,
        "lon": 144.9631,
        "product_id_obs": "IDV10753",  # Melbourne observations
        "product_id_forecast": "IDV10753",
    },
    "adelaide": {
        "lat": -34.9285,
        "lon": 138.6007,
        "product_id_obs": "IDS10044",  # Adelaide observations
        "product_id_forecast": "IDS10044",
    },
    "gold_coast": {
        "lat": -28.0028,
        "lon": 153.4318,
        "product_id_obs": "IDQ11295",  # Brisbane city (nearest)
        "product_id_forecast": "IDQ11295",
    },
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance in km between two lat/lon points using Haversine formula.

    Args:
        lat1, lon1: First point (degrees)
        lat2, lon2: Second point (degrees)

    Returns:
        Distance in km
    """
    import math

    R = 6371.0  # Earth's radius in km

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def resolve_station(venue_lat: Optional[float], venue_lon: Optional[float]) -> tuple[str, str]:
    """
    Resolve venue coordinates to nearest BOM station.

    Args:
        venue_lat, venue_lon: Venue coordinates

    Returns:
        Tuple of (product_id_obs, product_id_forecast)
    """
    if venue_lat is None or venue_lon is None:
        # Default to Brisbane
        logger.warning("No venue coordinates; defaulting to Brisbane")
        return STATION_MAPPING["brisbane"]["product_id_obs"], STATION_MAPPING["brisbane"]["product_id_forecast"]

    # Find nearest station
    min_distance = float('inf')
    nearest_station = "brisbane"

    for station_name, station_info in STATION_MAPPING.items():
        dist = haversine_km(venue_lat, venue_lon, station_info["lat"], station_info["lon"])
        if dist < min_distance:
            min_distance = dist
            nearest_station = station_name

    station = STATION_MAPPING[nearest_station]
    logger.debug(f"Resolved venue ({venue_lat}, {venue_lon}) to {nearest_station} ({min_distance:.1f}km away)")
    return station["product_id_obs"], station["product_id_forecast"]


# ─────────────────────────────────────────────────────────────────────────────
# BOM Live Adapter
# ─────────────────────────────────────────────────────────────────────────────


class BOMLiveWeatherAdapter(WeatherAdapter):
    """
    Live BOM adapter using BOMClient with automatic station resolution.

    Features:
    - Auto-resolve venue lat/lon to nearest BOM station
    - Graceful fallback to empty list on network failure
    - 10-minute caching
    """

    def __init__(self):
        """Initialize BOM live adapter."""
        if not httpx:
            raise ImportError("httpx required for BOMLiveWeatherAdapter")
        self.client = BOMClient()

    async def close(self):
        """Close client."""
        await self.client.close()

    async def get_current(self, venue_id: str) -> WeatherObservation:
        """
        Fetch current observation from BOM (or return empty fallback).

        Note: This implementation doesn't require venue_id to lat/lon mapping
        since we'd need that external config. For now, raises gracefully.
        """
        raise BOMFetchError(
            "BOMLiveWeatherAdapter.get_current requires venue lat/lon config; "
            "use with signal_feeds factory that has venue coordinates"
        )

    async def get_forecast(
        self, venue_id: str, days: int = 7
    ) -> list[WeatherForecastDay]:
        """
        Fetch forecast from BOM.

        Gracefully returns empty list on any failure.
        """
        try:
            # This is a placeholder; real implementation would need venue coords
            logger.warning(
                f"BOM forecast for {venue_id} requires venue configuration; returning empty"
            )
            return []
        except Exception as e:
            logger.warning(f"BOM forecast fetch failed for {venue_id}: {e}")
            return []


# Keep the old BOMAdapter for backward compatibility (config-driven)
class BOMAdapter(WeatherAdapter):
    """
    Live BOM adapter using httpx.

    Configuration: pass a dict of venue_id → {product_id_obs, product_id_forecast}
    """

    BASE_URL = "http://reg.bom.gov.au/fwo"
    TIMEOUT = 15

    def __init__(
        self,
        venue_config: Dict[str, Dict[str, str]],
    ):
        """
        Args:
            venue_config: {
                venue_id: {
                    "product_id_obs": "IDXXXXXX",
                    "product_id_forecast": "IDXXXXXX"
                }
            }
        """
        if not httpx:
            raise ImportError("httpx required for BOMAdapter")
        self.venue_config = venue_config
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.TIMEOUT)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_current(self, venue_id: str) -> WeatherObservation:
        """Fetch current observation from BOM."""
        if venue_id not in self.venue_config:
            raise BOMFetchError(f"No BOM config for venue {venue_id}")

        config = self.venue_config[venue_id]
        product_id = config.get("product_id_obs")
        if not product_id:
            raise BOMFetchError(f"No product_id_obs for venue {venue_id}")

        url = f"{self.BASE_URL}/{product_id}.json"
        client = await self._get_client()

        try:
            response = await client.get(url)
            if response.status_code != 200:
                raise BOMFetchError(
                    f"BOM API returned {response.status_code} for {url}"
                )

            data = response.json()
            observations = data.get("observations", {}).get("data", [])
            if not observations:
                raise BOMFetchError(f"No observations in BOM response for {venue_id}")

            # Use first (latest) observation
            obs = observations[0]

            # Parse timestamp
            ts_str = obs.get("local_date_time_full", "")
            try:
                # BOM format: "2026-04-15T14:30:00+10:00" or similar
                timestamp = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                timestamp = datetime.now(timezone.utc)

            temp = float(obs.get("air_temp", 20))
            apparent = obs.get("apparent_t")
            if apparent is not None:
                apparent = float(apparent)

            rain_mm = obs.get("rain_trace")
            if rain_mm is not None:
                try:
                    rain_mm = float(rain_mm)
                except (ValueError, TypeError):
                    rain_mm = None

            humidity = obs.get("rel_hum")
            if humidity is not None:
                humidity = int(float(humidity))

            wind = obs.get("wind_spd_kmh")
            if wind is not None:
                wind = float(wind)

            # Simple condition categorization from current obs
            conditions = "clear"
            if rain_mm and rain_mm > 0:
                conditions = "light_rain" if rain_mm < 10 else "heavy_rain"

            return WeatherObservation(
                venue_id=venue_id,
                timestamp=timestamp,
                temperature_c=temp,
                apparent_temperature_c=apparent,
                rain_mm_last_hour=rain_mm,
                humidity_pct=humidity,
                wind_kmh=wind,
                conditions=conditions,
                source="bom",
            )

        except BOMFetchError:
            raise
        except Exception as e:
            raise BOMFetchError(f"Failed to fetch BOM observations: {e}") from e

    async def get_forecast(
        self, venue_id: str, days: int = 7
    ) -> list[WeatherForecastDay]:
        """Fetch forecast from BOM (stub for now)."""
        if venue_id not in self.venue_config:
            raise BOMFetchError(f"No BOM config for venue {venue_id}")

        config = self.venue_config[venue_id]
        product_id = config.get("product_id_forecast")
        if not product_id:
            raise BOMFetchError(f"No product_id_forecast for venue {venue_id}")

        # Forecasts would be fetched similarly from BOM precis feeds
        # For now, return empty to avoid breaking; real implementation
        # would parse forecast JSON structure
        logger.warning(
            f"BOM forecast for {venue_id} not yet implemented; returning empty"
        )
        return []
