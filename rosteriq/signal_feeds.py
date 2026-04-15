"""
RosterIQ Signal Feeds - External Data Integration

Provides real-time external signal feeds for AI rostering demand prediction:
- WeatherFeed: BOM (Bureau of Meteorology) weather forecasts
- EventsFeed: PredictHQ API for events and conferences
- BookingsFeed: NowBookIt reservation data
- FootTrafficFeed: Google Places API for venue foot traffic
- DeliveryFeed: Merchant delivery platform analytics
- SignalAggregator: Unified orchestration and signal scoring

Each feed automatically uses real APIs when credentials are available,
falls back to demo data when not configured.

All methods are async. Full type hints and docstrings included.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Optional, Any
from decimal import Decimal

try:
    import aiohttp
except ImportError:
    aiohttp = None

logger = logging.getLogger("rosteriq.signal_feeds")


# ============================================================================
# Enums & Constants
# ============================================================================

class SignalSourceType(str, Enum):
    """Source of external signal."""
    WEATHER = "weather"
    EVENTS = "events"
    BOOKINGS = "bookings"
    FOOT_TRAFFIC = "foot_traffic"
    DELIVERY = "delivery"
    PATTERN = "pattern"


class SignalImpactType(str, Enum):
    """Type of impact on demand."""
    POSITIVE = "positive"  # Increases demand
    NEGATIVE = "negative"  # Decreases demand
    NEUTRAL = "neutral"    # No significant impact


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Signal:
    """
    Represents a single external signal affecting venue demand.

    Attributes:
        source: Source of the signal (weather, events, bookings, etc.)
        signal_type: Type of impact (positive, negative, neutral)
        impact_score: Strength of impact (0.0 = no impact, 1.0 = extreme)
        confidence: Confidence in this signal (0.0 = unsure, 1.0 = certain)
        description: Human-readable summary of the signal
        raw_data: Original data from source API
        timestamp: When this signal was generated
    """
    source: SignalSourceType
    signal_type: SignalImpactType
    impact_score: float  # 0.0-1.0
    confidence: float    # 0.0-1.0
    description: str
    raw_data: dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        """Validate score ranges."""
        if not 0.0 <= self.impact_score <= 1.0:
            raise ValueError(f"impact_score must be 0.0-1.0, got {self.impact_score}")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be 0.0-1.0, got {self.confidence}")


@dataclass
class WeatherForecast:
    """Single day weather forecast."""
    date: date
    high_temp_c: float
    low_temp_c: float
    condition: str  # "sunny", "cloudy", "rainy", "partly_cloudy"
    rain_probability: float  # 0-1
    wind_speed_kmh: float


@dataclass
class Event:
    """External event that may affect venue demand."""
    event_id: str
    name: str
    date: date
    time: Optional[str]  # HH:MM format
    category: str  # "sports", "music", "festival", "comedy", "conference"
    expected_attendance: int
    distance_km: float
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    venue_name: Optional[str] = None


@dataclass
class Booking:
    """Venue booking/reservation."""
    booking_id: str
    date: date
    time: str  # HH:MM format
    covers: int
    name: Optional[str] = None
    notes: Optional[str] = None


# ============================================================================
# Base Feed Class
# ============================================================================

class BaseFeed(ABC):
    """Abstract base class for all signal feeds."""

    def __init__(self, name: str):
        """
        Initialize feed.

        Args:
            name: Human-readable feed name
        """
        self.name = name
        self.logger = logging.getLogger(f"rosteriq.signal_feeds.{name}")

    @abstractmethod
    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Fetch signals for a venue and date range.

        Args:
            venue_id: Venue identifier
            date_range: Tuple of (start_date, end_date) inclusive
            location_lat: Venue latitude (if applicable)
            location_lng: Venue longitude (if applicable)

        Returns:
            List of Signal objects
        """
        pass

    async def _demo_signals(self) -> list[Signal]:
        """Override to provide demo data when API not configured."""
        return []


# ============================================================================
# 1. Weather Feed
# ============================================================================

class WeatherFeed(BaseFeed):
    """
    Bureau of Meteorology (BOM) weather forecast integration.

    Real API: ftp://ftp.bom.gov.au/anon/gen/fwo/
    Demo: 7-day Brisbane forecast

    Impact scoring:
    - Rain > 60% probability: -0.6 (outdoor capacity -50%)
    - Temperature 22-26°C sunny: +0.3 (pleasant, drives demand)
    - Extreme heat > 35°C: -0.4 (keeps customers away)
    """

    def __init__(self):
        """Initialize weather feed."""
        super().__init__("WeatherFeed")
        self.api_key = os.getenv("BOM_API_KEY")
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_forecast(
        self,
        location: str,
        days: int = 7,
    ) -> list[WeatherForecast]:
        """
        Get weather forecast for a location.

        Args:
            location: Location name or code (e.g., "Brisbane", "SYD")
            days: Number of forecast days (1-7)

        Returns:
            List of WeatherForecast objects
        """
        if self.api_key:
            return await self._fetch_real_forecast(location, days)
        else:
            return await self._demo_forecast(location, days)

    async def _fetch_real_forecast(
        self,
        location: str,
        days: int,
    ) -> list[WeatherForecast]:
        """Fetch real forecast from BOM API."""
        try:
            # BOM uses FTP - in production, would use their XML product API
            # This is a simplified HTTP fallback
            url = f"https://api.weatherapi.com/v1/forecast.json"
            params = {
                "q": location,
                "days": min(days, 10),
                "aqi": "no"
            }

            if not aiohttp:
                self.logger.warning("aiohttp not installed, falling back to demo data")
                return await self._demo_forecast(location, days)

            if not self.session:
                self.session = aiohttp.ClientSession()

            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_forecast(data)
                else:
                    self.logger.warning(
                        f"Weather API returned {resp.status}, using demo data"
                    )
                    return await self._demo_forecast(location, days)
        except Exception as e:
            self.logger.error(f"Error fetching weather: {e}, using demo data")
            return await self._demo_forecast(location, days)

    def _parse_forecast(self, api_data: dict) -> list[WeatherForecast]:
        """Parse weather API response."""
        forecasts = []
        try:
            for day_data in api_data.get("forecast", {}).get("forecastday", []):
                day = day_data["date"]
                condition = day_data["day"]["condition"]["text"].lower()

                forecasts.append(WeatherForecast(
                    date=datetime.fromisoformat(day).date(),
                    high_temp_c=day_data["day"]["maxtemp_c"],
                    low_temp_c=day_data["day"]["mintemp_c"],
                    condition=condition,
                    rain_probability=day_data["day"].get("daily_chance_of_rain", 0) / 100,
                    wind_speed_kmh=day_data["day"]["maxwind_kph"],
                ))
        except Exception as e:
            self.logger.error(f"Error parsing weather data: {e}")
            return []

        return forecasts

    async def _demo_forecast(
        self,
        location: str,
        days: int,
    ) -> list[WeatherForecast]:
        """
        Demo forecast: Brisbane 7 days.

        Mon sunny 26°C, Tue sunny 24°C, Wed partly cloudy 22°C,
        Thu sunny 27°C, Fri sunny 25°C, Sat cloudy 20°C 70% rain,
        Sun sunny 23°C
        """
        today = date.today()
        demo_data = [
            (today + timedelta(0), 26, 18, "sunny", 0.0, 15),
            (today + timedelta(1), 24, 17, "sunny", 0.0, 12),
            (today + timedelta(2), 22, 16, "partly_cloudy", 0.2, 14),
            (today + timedelta(3), 27, 19, "sunny", 0.0, 16),
            (today + timedelta(4), 25, 18, "sunny", 0.0, 13),
            (today + timedelta(5), 20, 15, "cloudy", 0.7, 18),
            (today + timedelta(6), 23, 16, "sunny", 0.1, 12),
        ]

        return [
            WeatherForecast(
                date=d, high_temp_c=h, low_temp_c=l,
                condition=c, rain_probability=r, wind_speed_kmh=w
            )
            for d, h, l, c, r, w in demo_data[:days]
        ]

    def get_impact_score(self, forecast: WeatherForecast) -> Signal:
        """
        Calculate demand impact from weather forecast.

        Args:
            forecast: Weather forecast for a single day

        Returns:
            Signal with impact assessment
        """
        impact_score = 0.0
        impact_type = SignalImpactType.NEUTRAL
        description = ""

        # Rain reduces demand (especially outdoor seating)
        if forecast.rain_probability > 0.6:
            impact_score = -0.6
            impact_type = SignalImpactType.NEGATIVE
            description = f"Heavy rain expected ({forecast.rain_probability:.0%}), outdoor capacity -50%"

        # Extreme heat keeps customers away
        elif forecast.high_temp_c > 35:
            impact_score = -0.4
            impact_type = SignalImpactType.NEGATIVE
            description = f"Extreme heat {forecast.high_temp_c}°C, reduced foot traffic expected"

        # Pleasant temperature range boosts demand
        elif 22 <= forecast.high_temp_c <= 26 and forecast.condition == "sunny":
            impact_score = 0.3
            impact_type = SignalImpactType.POSITIVE
            description = f"Pleasant weather {forecast.high_temp_c}°C sunny, expect higher demand"

        return Signal(
            source=SignalSourceType.WEATHER,
            signal_type=impact_type,
            impact_score=abs(impact_score),
            confidence=0.85,
            description=description,
            raw_data=vars(forecast),
        )

    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Get weather signals for date range.

        Args:
            venue_id: Venue identifier
            date_range: (start_date, end_date) tuple
            location_lat: Latitude (unused, location inferred from venue)
            location_lng: Longitude (unused, location inferred from venue)

        Returns:
            List of weather Signal objects
        """
        start_date, end_date = date_range
        days = (end_date - start_date).days + 1

        forecasts = await self.get_forecast("Brisbane", min(days, 7))

        signals = []
        for forecast in forecasts:
            if start_date <= forecast.date <= end_date:
                signal = self.get_impact_score(forecast)
                signals.append(signal)

        return signals

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# ============================================================================
# 2. Events Feed
# ============================================================================

class EventsFeed(BaseFeed):
    """
    PredictHQ API integration for events and conferences.

    Real API: https://api.predicthq.com/v1/
    Requires: PREDICTHQ_API_KEY environment variable

    Demo: Brisbane 7-day events
    - Wed: Comedy night at venue (150 ppl, internal)
    - Fri: University graduation nearby (2,000 ppl, 1.5km)
    - Sat: AFL at Gabba (35,000, 3km) + Riverside Festival (8,000, 2km)
    - Sun: Farmers markets (3,000, 1km)

    Impact calculation: (attendance / distance / category_weight)
    Category weights: sports 0.8, music 0.7, festival 0.9, comedy 0.5, conference 0.4
    """

    def __init__(self):
        """Initialize events feed."""
        super().__init__("EventsFeed")
        self.api_key = os.getenv("PREDICTHQ_API_KEY")
        self.base_url = "https://api.predicthq.com/v1"
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_events(
        self,
        lat: float,
        lng: float,
        date_range: tuple[date, date],
        radius_km: float = 5.0,
    ) -> list[Event]:
        """
        Get events near a location.

        Args:
            lat: Latitude
            lng: Longitude
            date_range: (start_date, end_date) tuple
            radius_km: Search radius

        Returns:
            List of Event objects
        """
        if self.api_key:
            return await self._fetch_real_events(lat, lng, date_range, radius_km)
        else:
            return await self._demo_events(lat, lng, date_range)

    async def _fetch_real_events(
        self,
        lat: float,
        lng: float,
        date_range: tuple[date, date],
        radius_km: float,
    ) -> list[Event]:
        """Fetch real events from PredictHQ API."""
        try:
            if not aiohttp:
                self.logger.warning("aiohttp not installed, using demo data")
                return await self._demo_events(lat, lng, date_range)

            if not self.session:
                self.session = aiohttp.ClientSession()

            start_date, end_date = date_range
            url = f"{self.base_url}/events"
            params = {
                "active.gte": start_date.isoformat(),
                "active.lte": end_date.isoformat(),
                "location.origin": f"{lat},{lng}",
                "location.radius": f"{radius_km}km",
            }
            headers = {"Authorization": f"Bearer {self.api_key}"}

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_events(data)
                else:
                    self.logger.warning(
                        f"PredictHQ API returned {resp.status}, using demo data"
                    )
                    return await self._demo_events(lat, lng, date_range)
        except Exception as e:
            self.logger.error(f"Error fetching events: {e}, using demo data")
            return await self._demo_events(lat, lng, date_range)

    def _parse_events(self, api_data: dict) -> list[Event]:
        """Parse PredictHQ API response."""
        events = []
        try:
            for event_data in api_data.get("results", []):
                event_date = datetime.fromisoformat(
                    event_data["active"]["start"]
                ).date()
                event_time = datetime.fromisoformat(
                    event_data["active"]["start"]
                ).strftime("%H:%M")

                events.append(Event(
                    event_id=event_data["id"],
                    name=event_data.get("title", "Unknown Event"),
                    date=event_date,
                    time=event_time,
                    category=event_data.get("category", "other"),
                    expected_attendance=event_data.get("expected_attendance", 0),
                    distance_km=event_data.get("distance_km", 0),
                    latitude=event_data.get("location", {}).get("lat"),
                    longitude=event_data.get("location", {}).get("lng"),
                ))
        except Exception as e:
            self.logger.error(f"Error parsing events: {e}")
            return []

        return events

    async def _demo_events(
        self,
        lat: float,
        lng: float,
        date_range: tuple[date, date],
    ) -> list[Event]:
        """Demo events: Brisbane next 7 days."""
        start_date, end_date = date_range
        today = start_date

        return [
            Event(
                event_id="demo_comedy_001",
                name="Comedy Night",
                date=today + timedelta(2),  # Wed
                time="19:00",
                category="comedy",
                expected_attendance=150,
                distance_km=0.0,
                venue_name="Internal",
            ),
            Event(
                event_id="demo_graduation_001",
                name="University Graduation",
                date=today + timedelta(4),  # Fri
                time="14:00",
                category="conference",
                expected_attendance=2000,
                distance_km=1.5,
            ),
            Event(
                event_id="demo_afl_001",
                name="AFL at the Gabba",
                date=today + timedelta(5),  # Sat
                time="16:00",
                category="sports",
                expected_attendance=35000,
                distance_km=3.0,
            ),
            Event(
                event_id="demo_festival_001",
                name="Riverside Festival",
                date=today + timedelta(5),  # Sat
                time="10:00",
                category="festival",
                expected_attendance=8000,
                distance_km=2.0,
            ),
            Event(
                event_id="demo_farmers_001",
                name="Farmers Markets",
                date=today + timedelta(6),  # Sun
                time="08:00",
                category="festival",
                expected_attendance=3000,
                distance_km=1.0,
            ),
        ]

    def get_impact_score(self, event: Event, venue_distance_km: Optional[float] = None) -> Signal:
        """
        Calculate demand impact from event.

        Args:
            event: Event object
            venue_distance_km: Distance from venue to event (overrides event distance)

        Returns:
            Signal with impact assessment
        """
        distance = venue_distance_km or event.distance_km or 1.0

        # Category weights
        category_weights = {
            "sports": 0.8,
            "music": 0.7,
            "festival": 0.9,
            "comedy": 0.5,
            "conference": 0.4,
        }
        weight = category_weights.get(event.category, 0.3)

        # Impact = (attendance / distance^1.5) * weight
        # Normalize to 0-1 range
        impact = (event.expected_attendance / (distance ** 1.5)) * weight
        normalized_impact = min(impact / 10000, 1.0)  # Cap at 1.0

        description = (
            f"{event.name}: {event.expected_attendance:,} people, "
            f"{distance}km away ({event.category})"
        )

        return Signal(
            source=SignalSourceType.EVENTS,
            signal_type=SignalImpactType.POSITIVE,
            impact_score=normalized_impact,
            confidence=0.75,
            description=description,
            raw_data=vars(event),
        )

    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Get event signals for date range.

        Args:
            venue_id: Venue identifier
            date_range: (start_date, end_date) tuple
            location_lat: Venue latitude
            location_lng: Venue longitude

        Returns:
            List of event Signal objects
        """
        if not location_lat or not location_lng:
            # Default to Brisbane if not provided
            location_lat, location_lng = -27.4698, 153.0251

        events = await self.get_events(location_lat, location_lng, date_range)

        signals = []
        for event in events:
            signal = self.get_impact_score(event)
            signals.append(signal)

        return signals

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# ============================================================================
# 3. Bookings Feed
# ============================================================================

class BookingsFeed(BaseFeed):
    """
    NowBookIt reservation system integration.

    Real API: https://secure.nowbookit.com/api/
    Requires: NOWBOOKIT_API_KEY environment variable

    Demo: Brisbane bookings (walk-in ratio 40%)
    - Mon: 8 bookings/42 covers
    - Tue: 10/55, Wed: 18/85 (comedy), Thu: 12/62, Fri: 28/145,
    - Sat: 35/180 + 60-person function at 7pm, Sun: 15/78
    """

    def __init__(self):
        """Initialize bookings feed."""
        super().__init__("BookingsFeed")
        self.api_key = os.getenv("NOWBOOKIT_API_KEY")
        self.base_url = "https://secure.nowbookit.com/api"
        self.session: Optional[aiohttp.ClientSession] = None
        self.walk_in_ratio = 0.4  # 40% of total covers are walk-ins

    async def get_bookings(
        self,
        venue_id: str,
        date_range: tuple[date, date],
    ) -> list[Booking]:
        """
        Get bookings for a venue and date range.

        Args:
            venue_id: NowBookIt venue ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Booking objects
        """
        if self.api_key:
            return await self._fetch_real_bookings(venue_id, date_range)
        else:
            return await self._demo_bookings(date_range)

    async def _fetch_real_bookings(
        self,
        venue_id: str,
        date_range: tuple[date, date],
    ) -> list[Booking]:
        """Fetch real bookings from NowBookIt API."""
        try:
            if not aiohttp:
                self.logger.warning("aiohttp not installed, using demo data")
                return await self._demo_bookings(date_range)

            if not self.session:
                self.session = aiohttp.ClientSession()

            start_date, end_date = date_range
            url = f"{self.base_url}/bookings"
            params = {
                "venue_id": venue_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
            headers = {"Authorization": f"Bearer {self.api_key}"}

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_bookings(data)
                else:
                    self.logger.warning(
                        f"NowBookIt API returned {resp.status}, using demo data"
                    )
                    return await self._demo_bookings(date_range)
        except Exception as e:
            self.logger.error(f"Error fetching bookings: {e}, using demo data")
            return await self._demo_bookings(date_range)

    def _parse_bookings(self, api_data: dict) -> list[Booking]:
        """Parse NowBookIt API response."""
        bookings = []
        try:
            for booking_data in api_data.get("bookings", []):
                booking_date = datetime.fromisoformat(
                    booking_data["date"]
                ).date()
                booking_time = booking_data.get("time", "18:00")

                bookings.append(Booking(
                    booking_id=booking_data["id"],
                    date=booking_date,
                    time=booking_time,
                    covers=booking_data.get("covers", 0),
                    name=booking_data.get("name"),
                    notes=booking_data.get("notes"),
                ))
        except Exception as e:
            self.logger.error(f"Error parsing bookings: {e}")
            return []

        return bookings

    async def _demo_bookings(self, date_range: tuple[date, date]) -> list[Booking]:
        """Demo bookings: Brisbane next week."""
        start_date, end_date = date_range
        today = start_date

        demo_data = [
            (today, 8, 42, "18:30"),      # Mon
            (today + timedelta(1), 10, 55, "18:30"),   # Tue
            (today + timedelta(2), 18, 85, "18:30"),   # Wed (comedy)
            (today + timedelta(3), 12, 62, "18:30"),   # Thu
            (today + timedelta(4), 28, 145, "18:30"),  # Fri
            (today + timedelta(5), 35, 180, "18:30"),  # Sat
            (today + timedelta(5), 1, 60, "19:00"),    # Sat function
            (today + timedelta(6), 15, 78, "18:30"),   # Sun
        ]

        bookings = []
        booking_id = 1
        for d, num_bookings, total_covers, time in demo_data:
            covers_per_booking = total_covers // num_bookings
            for i in range(num_bookings):
                bookings.append(Booking(
                    booking_id=f"demo_{booking_id}",
                    date=d,
                    time=time,
                    covers=covers_per_booking,
                    name=f"Demo Booking {booking_id}",
                ))
                booking_id += 1

        return bookings

    async def get_booking_forecast(
        self,
        venue_id: str,
        target_date: date,
    ) -> dict[str, Any]:
        """
        Get booking forecast for a specific date.

        Args:
            venue_id: Venue identifier
            target_date: Date to forecast for

        Returns:
            Dict with expected_covers and walk_in_estimate
        """
        date_range = (target_date, target_date)
        bookings = await self.get_bookings(venue_id, date_range)

        booked_covers = sum(b.covers for b in bookings)
        walk_in_covers = int(booked_covers * self.walk_in_ratio / (1 - self.walk_in_ratio))

        return {
            "date": target_date.isoformat(),
            "expected_covers": booked_covers + walk_in_covers,
            "booked_covers": booked_covers,
            "walk_in_estimate": walk_in_covers,
            "booking_count": len(bookings),
        }

    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Get booking signals for date range.

        Args:
            venue_id: Venue identifier
            date_range: (start_date, end_date) tuple
            location_lat: Unused
            location_lng: Unused

        Returns:
            List of booking Signal objects
        """
        bookings = await self.get_bookings(venue_id, date_range)

        signals = []
        for target_date in self._iter_date_range(date_range):
            day_bookings = [b for b in bookings if b.date == target_date]
            day_covers = sum(b.covers for b in day_bookings)

            if day_covers > 0:
                # Normalize to 0-1 range (max 300 covers)
                impact = min(day_covers / 300, 1.0)

                signals.append(Signal(
                    source=SignalSourceType.BOOKINGS,
                    signal_type=SignalImpactType.POSITIVE,
                    impact_score=impact,
                    confidence=0.95,
                    description=f"{len(day_bookings)} bookings, {day_covers} covers",
                    raw_data={
                        "date": target_date.isoformat(),
                        "bookings": len(day_bookings),
                        "covers": day_covers,
                    },
                ))

        return signals

    def _iter_date_range(self, date_range: tuple[date, date]):
        """Helper to iterate over date range."""
        start_date, end_date = date_range
        current = start_date
        while current <= end_date:
            yield current
            current += timedelta(days=1)

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# ============================================================================
# 4. Foot Traffic Feed
# ============================================================================

class FootTrafficFeed(BaseFeed):
    """
    Google Places API foot traffic and busyness integration.

    Real API: Google Places API (GOOGLE_PLACES_API_KEY)

    Demo: Hourly busyness curves (0-100)
    - Lunch: 60-75 (11am-2pm)
    - Afternoon dip: 20-30 (2pm-5pm)
    - Dinner: 70-90 (5pm-10pm, higher Fri/Sat)
    """

    def __init__(self):
        """Initialize foot traffic feed."""
        super().__init__("FootTrafficFeed")
        self.api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_current_busyness(self, place_id: str) -> dict[str, Any]:
        """
        Get current busyness percentage for a place.

        Args:
            place_id: Google Places place ID

        Returns:
            Dict with busyness_percentage and typical_percentage
        """
        if self.api_key:
            return await self._fetch_real_busyness(place_id)
        else:
            return self._demo_busyness()

    async def _fetch_real_busyness(self, place_id: str) -> dict[str, Any]:
        """Fetch real busyness from Google Places API."""
        try:
            if not aiohttp:
                return self._demo_busyness()

            if not self.session:
                self.session = aiohttp.ClientSession()

            url = "https://maps.googleapis.com/maps/api/place/details/json"
            params = {
                "place_id": place_id,
                "fields": "current_opening_hours",
                "key": self.api_key,
            }

            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_busyness(data)
                else:
                    return self._demo_busyness()
        except Exception as e:
            self.logger.error(f"Error fetching busyness: {e}, using demo data")
            return self._demo_busyness()

    def _parse_busyness(self, api_data: dict) -> dict[str, Any]:
        """Parse Google Places busyness response."""
        try:
            result = api_data.get("result", {})
            hours = result.get("current_opening_hours", {})
            busyness = hours.get("current_busyness_level", None)

            level_to_percentage = {
                "NOT_BUSY": 20,
                "LESS_BUSY": 35,
                "BUSY": 65,
                "VERY_BUSY": 85,
            }

            return {
                "busyness_percentage": level_to_percentage.get(busyness, 50),
                "typical_percentage": 50,
            }
        except Exception as e:
            self.logger.error(f"Error parsing busyness: {e}")
            return self._demo_busyness()

    def _demo_busyness(self) -> dict[str, Any]:
        """Demo busyness data."""
        hour = datetime.now(timezone.utc).hour

        # Simulate typical hourly curve
        if 11 <= hour < 14:
            current = 65  # Lunch
        elif 14 <= hour < 17:
            current = 25  # Afternoon dip
        elif 17 <= hour < 22:
            current = 75  # Dinner
        else:
            current = 15  # Off hours

        return {
            "busyness_percentage": current,
            "typical_percentage": 50,
        }

    async def get_typical_busyness(
        self,
        place_id: str,
        day: int,  # 0=Monday, 6=Sunday
    ) -> dict[int, int]:
        """
        Get typical busyness curve for a day of week.

        Args:
            place_id: Google Places place ID
            day: Day of week (0-6)

        Returns:
            Dict mapping hour (0-23) to busyness percentage
        """
        # Demo data: hour -> busyness
        base_curve = {
            0: 5, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,           # 12am-6am
            6: 10, 7: 20, 8: 30,                            # 6am-9am
            9: 45, 10: 55, 11: 65, 12: 70, 13: 65,        # 9am-1pm (lunch)
            14: 30, 15: 25, 16: 30,                         # 1pm-4pm (dip)
            17: 50, 18: 70, 19: 75, 20: 80, 21: 75,       # 4pm-9pm (dinner)
            22: 60, 23: 30,                                 # 10pm-11pm
        }

        # Boost Fri/Sat evenings
        if day in [4, 5]:  # Fri, Sat
            for hour in range(17, 23):
                base_curve[hour] = min(base_curve[hour] + 15, 90)

        return base_curve

    async def get_live_vs_typical(self, place_id: str) -> dict[str, Any]:
        """
        Compare live busyness to typical for this hour.

        Args:
            place_id: Google Places place ID

        Returns:
            Dict with comparison metrics
        """
        current = await self.get_current_busyness(place_id)
        hour = datetime.now(timezone.utc).hour
        day = datetime.now(timezone.utc).weekday()

        typical_curve = await self.get_typical_busyness(place_id, day)
        typical = typical_curve.get(hour, 50)

        diff = current["busyness_percentage"] - typical

        return {
            "current_busyness": current["busyness_percentage"],
            "typical_busyness": typical,
            "difference": diff,
            "variance_percent": (diff / typical * 100) if typical > 0 else 0,
        }

    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Get foot traffic signals for date range.

        Args:
            venue_id: Venue identifier (used as place_id if available)
            date_range: (start_date, end_date) tuple
            location_lat: Unused
            location_lng: Unused

        Returns:
            List of foot traffic Signal objects
        """
        # For demo, generate signals based on typical patterns
        signals = []
        start_date, end_date = date_range

        for offset in range((end_date - start_date).days + 1):
            target_date = start_date + timedelta(days=offset)
            day_of_week = target_date.weekday()

            typical_curve = await self.get_typical_busyness(venue_id, day_of_week)
            peak_hour = max(typical_curve, key=typical_curve.get)
            peak_busyness = typical_curve[peak_hour]

            # Normalize to 0-1
            impact = peak_busyness / 100

            signals.append(Signal(
                source=SignalSourceType.FOOT_TRAFFIC,
                signal_type=SignalImpactType.POSITIVE,
                impact_score=impact,
                confidence=0.70,
                description=f"Peak busyness ~{peak_busyness}% at {peak_hour:02d}:00",
                raw_data={
                    "date": target_date.isoformat(),
                    "day_of_week": day_of_week,
                    "peak_hour": peak_hour,
                    "peak_busyness": peak_busyness,
                    "typical_curve": typical_curve,
                },
            ))

        return signals

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# ============================================================================
# 5. Delivery Feed
# ============================================================================

class DeliveryFeed(BaseFeed):
    """
    Merchant delivery platform analytics (DoorDash, Uber Eats, Menulog equivalent).

    Real API: Merchant delivery platform API (DELIVERY_API_KEY)

    Demo: 15-30 orders/day, higher on rain days and weeknights.
    Surge on rainy Saturday.
    """

    def __init__(self):
        """Initialize delivery feed."""
        super().__init__("DeliveryFeed")
        self.api_key = os.getenv("DELIVERY_API_KEY")
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_order_volume(
        self,
        venue_id: str,
        date_range: tuple[date, date],
    ) -> dict[date, int]:
        """
        Get delivery order volume for a venue and date range.

        Args:
            venue_id: Venue identifier
            date_range: (start_date, end_date) tuple

        Returns:
            Dict mapping date to order count
        """
        if self.api_key:
            return await self._fetch_real_orders(venue_id, date_range)
        else:
            return await self._demo_orders(date_range)

    async def _fetch_real_orders(
        self,
        venue_id: str,
        date_range: tuple[date, date],
    ) -> dict[date, int]:
        """Fetch real order data from delivery API."""
        try:
            if not aiohttp:
                return await self._demo_orders(date_range)

            if not self.session:
                self.session = aiohttp.ClientSession()

            start_date, end_date = date_range
            url = f"https://api.deliveryplatform.com/v1/orders"
            params = {
                "venue_id": venue_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
            headers = {"Authorization": f"Bearer {self.api_key}"}

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_orders(data)
                else:
                    return await self._demo_orders(date_range)
        except Exception as e:
            self.logger.error(f"Error fetching order volume: {e}, using demo data")
            return await self._demo_orders(date_range)

    def _parse_orders(self, api_data: dict) -> dict[date, int]:
        """Parse delivery API response."""
        orders_by_date = {}
        try:
            for order in api_data.get("orders", []):
                order_date = datetime.fromisoformat(order["date"]).date()
                orders_by_date[order_date] = orders_by_date.get(order_date, 0) + 1
        except Exception as e:
            self.logger.error(f"Error parsing orders: {e}")
            return {}

        return orders_by_date

    async def _demo_orders(self, date_range: tuple[date, date]) -> dict[date, int]:
        """Demo order volume: 15-30 orders/day."""
        start_date, end_date = date_range
        orders = {}
        today = start_date

        for offset in range((end_date - start_date).days + 1):
            d = today + timedelta(days=offset)
            day_of_week = d.weekday()

            # Higher orders on weeknights, rain days
            if day_of_week < 4:  # Mon-Thu
                orders[d] = 20
            elif day_of_week == 4:  # Fri
                orders[d] = 25
            else:  # Sat-Sun
                orders[d] = 28

            # Boost for rain (Sat in demo)
            if day_of_week == 5:  # Sat
                orders[d] = 35

        return orders

    async def get_surge_status(self, venue_id: str) -> dict[str, Any]:
        """
        Get current delivery surge pricing status.

        Args:
            venue_id: Venue identifier

        Returns:
            Dict with surge_level and surge_multiplier
        """
        # Demo: return random surge status
        import random
        surge_level = random.choice(["low", "normal", "high"])
        multiplier = {"low": 1.0, "normal": 1.1, "high": 1.3}[surge_level]

        return {
            "surge_level": surge_level,
            "surge_multiplier": multiplier,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def get_signals(
        self,
        venue_id: str,
        date_range: tuple[date, date],
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Get delivery signals for date range.

        Args:
            venue_id: Venue identifier
            date_range: (start_date, end_date) tuple
            location_lat: Unused
            location_lng: Unused

        Returns:
            List of delivery Signal objects
        """
        orders = await self.get_order_volume(venue_id, date_range)

        signals = []
        for target_date in orders:
            order_count = orders[target_date]

            # Normalize to 0-1 (max 50 orders/day)
            impact = min(order_count / 50, 1.0)

            signals.append(Signal(
                source=SignalSourceType.DELIVERY,
                signal_type=SignalImpactType.POSITIVE,
                impact_score=impact,
                confidence=0.60,  # Delivery orders are less reliable indicator
                description=f"{order_count} delivery orders",
                raw_data={
                    "date": target_date.isoformat(),
                    "order_count": order_count,
                },
            ))

        return signals

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()


# ============================================================================
# Signal Aggregator
# ============================================================================

class SignalAggregator:
    """
    Unified orchestrator for all signal feeds.

    Collects signals from all sources, calculates demand multipliers,
    and provides human-readable summaries.
    """

    def __init__(self):
        """Initialize aggregator with all feeds."""
        self.weather = WeatherFeed()
        self.events = EventsFeed()
        self.bookings = BookingsFeed()
        self.foot_traffic = FootTrafficFeed()
        self.delivery = DeliveryFeed()
        self.logger = logging.getLogger("rosteriq.signal_feeds.aggregator")

    async def collect_all_signals(
        self,
        venue_id: str,
        target_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Collect all signals for a venue and date.

        Args:
            venue_id: Venue identifier
            target_date: Date to collect signals for
            location_lat: Venue latitude
            location_lng: Venue longitude

        Returns:
            List of Signal objects from all feeds
        """
        date_range = (target_date, target_date)

        # Fetch from all feeds concurrently
        results = await asyncio.gather(
            self.weather.get_signals(venue_id, date_range, location_lat, location_lng),
            self.events.get_signals(venue_id, date_range, location_lat, location_lng),
            self.bookings.get_signals(venue_id, date_range, location_lat, location_lng),
            self.foot_traffic.get_signals(venue_id, date_range, location_lat, location_lng),
            self.delivery.get_signals(venue_id, date_range, location_lat, location_lng),
            return_exceptions=True,
        )

        signals = []
        for result in results:
            if isinstance(result, list):
                signals.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Error collecting signals: {result}")

        return signals

    async def get_demand_multiplier(
        self,
        venue_id: str,
        target_date: date,
        hour: Optional[int] = None,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> float:
        """
        Calculate overall demand multiplier from all signals.

        Args:
            venue_id: Venue identifier
            target_date: Date to calculate for
            hour: Optional specific hour (0-23)
            location_lat: Venue latitude
            location_lng: Venue longitude

        Returns:
            Multiplier (1.0 = normal, 1.3 = 30% busier, 0.7 = 30% quieter)
        """
        signals = await self.collect_all_signals(
            venue_id, target_date, location_lat, location_lng
        )

        if not signals:
            return 1.0

        # Aggregate signals
        positive_impact = 0.0
        negative_impact = 0.0
        total_confidence = 0.0

        for signal in signals:
            weighted_score = signal.impact_score * signal.confidence

            if signal.signal_type == SignalImpactType.POSITIVE:
                positive_impact += weighted_score
            elif signal.signal_type == SignalImpactType.NEGATIVE:
                negative_impact += weighted_score

            total_confidence += signal.confidence

        # Normalize and apply
        avg_confidence = total_confidence / len(signals) if signals else 0.5

        # Multiplier formula: 1.0 + (positive - negative) capped at 0.5 to 1.5
        multiplier = 1.0 + (positive_impact - negative_impact) * 0.5
        multiplier = max(0.5, min(multiplier, 1.5))

        return multiplier

    async def get_signal_summary(
        self,
        venue_id: str,
        target_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> str:
        """
        Get human-readable summary of all signals.

        Example: "AFL game at Gabba + fine weather = expect 30% above normal"

        Args:
            venue_id: Venue identifier
            target_date: Date to summarize
            location_lat: Venue latitude
            location_lng: Venue longitude

        Returns:
            Human-readable summary string
        """
        signals = await self.collect_all_signals(
            venue_id, target_date, location_lat, location_lng
        )

        if not signals:
            return "No external signals detected. Normal demand expected."

        # Sort by impact score
        signals_sorted = sorted(signals, key=lambda s: s.impact_score, reverse=True)

        # Build summary
        top_signals = signals_sorted[:3]
        descriptions = [s.description for s in top_signals]
        summary_text = " + ".join(descriptions)

        multiplier = await self.get_demand_multiplier(
            venue_id, target_date, location_lat=location_lat, location_lng=location_lng
        )

        if multiplier >= 1.2:
            conclusion = "expect 20%+ above normal"
        elif multiplier >= 1.1:
            conclusion = "expect 10%+ above normal"
        elif multiplier >= 0.9:
            conclusion = "expect normal demand"
        else:
            conclusion = "expect below normal"

        return f"{summary_text} = {conclusion}"

    async def get_weekly_outlook(
        self,
        venue_id: str,
        week_start: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> dict[date, dict[str, Any]]:
        """
        Get 7-day signal summary.

        Args:
            venue_id: Venue identifier
            week_start: Start date of week
            location_lat: Venue latitude
            location_lng: Venue longitude

        Returns:
            Dict mapping date to signal summary dict
        """
        outlook = {}

        for offset in range(7):
            target_date = week_start + timedelta(days=offset)

            signals = await self.collect_all_signals(
                venue_id, target_date, location_lat, location_lng
            )

            multiplier = await self.get_demand_multiplier(
                venue_id, target_date, location_lat=location_lat, location_lng=location_lng
            )

            summary = await self.get_signal_summary(
                venue_id, target_date, location_lat=location_lat, location_lng=location_lng
            )

            outlook[target_date] = {
                "date": target_date.isoformat(),
                "day_name": target_date.strftime("%A"),
                "signal_count": len(signals),
                "demand_multiplier": round(multiplier, 2),
                "summary": summary,
                "signals": [
                    {
                        "source": s.source.value,
                        "type": s.signal_type.value,
                        "impact": round(s.impact_score, 2),
                        "description": s.description,
                    }
                    for s in signals
                ],
            }

        return outlook

    async def close(self):
        """Close all feed connections."""
        await asyncio.gather(
            self.weather.close(),
            self.events.close(),
            self.bookings.close(),
            self.foot_traffic.close(),
            self.delivery.close(),
            return_exceptions=True,
        )


# ============================================================================
# Convenience Exports
# ============================================================================

__all__ = [
    "SignalSourceType",
    "SignalImpactType",
    "Signal",
    "WeatherForecast",
    "Event",
    "Booking",
    "WeatherFeed",
    "EventsFeed",
    "BookingsFeed",
    "FootTrafficFeed",
    "DeliveryFeed",
    "SignalAggregator",
]
