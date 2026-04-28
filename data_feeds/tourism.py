"""
Tourism & Transport data feed adapter for RosterIQ.

Integrates multiple data sources including hotel occupancy, Airbnb bookings, cruise ship
schedules, conference/exhibition calendars, and public transport disruptions to generate
staffing signals for hospitality venues.

Sources:
- Tourism Research Australia (TRA): https://www.tra.gov.au/
- STR Hotel Occupancy: https://api.str.com/
- AirDNA: https://api.airdna.co/v1/
- Cruise Ship Schedules: Ports Australia (hardcoded for major ports)
- PTV (VIC): https://timetableapi.ptv.vic.gov.au/v3/
- TfNSW: https://api.transport.nsw.gov.au/v1/
- TransLink (QLD): https://api.translink.com.au/
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

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
# Data Models
# ============================================================================


@dataclass
class CruiseShipEvent:
    """Represents a cruise ship visit to a port."""

    port_code: str
    port_name: str
    arrival_date: datetime
    departure_date: datetime
    ship_name: str
    expected_passengers: int
    terminal_location: Location


@dataclass
class ConferenceEvent:
    """Represents a conference or exhibition event."""

    name: str
    venue_name: str
    venue_location: Location
    start_date: datetime
    end_date: datetime
    expected_attendees: int
    category: str  # "conference", "exhibition", "trade_show"


@dataclass
class TransportDisruption:
    """Represents a public transport disruption."""

    disruption_id: str
    route_name: str
    disruption_type: str  # "closure", "replacement_service", "delay"
    affected_area: Location
    start_datetime: datetime
    end_datetime: datetime
    severity: str  # "low", "medium", "high"


@dataclass
class HotelOccupancyData:
    """Hotel occupancy statistics for a region."""

    region: str
    occupancy_rate: float  # 0.0 - 1.0
    average_daily_rate: float
    revenue_per_available_room: float
    sample_size: int
    timestamp: datetime


@dataclass
class AirbnbOccupancyData:
    """Airbnb/short-stay occupancy data."""

    region: str
    occupancy_rate: float  # 0.0 - 1.0
    average_nightly_rate: float
    active_listings: int
    timestamp: datetime


# ============================================================================
# Cruise Ship Schedule Data (Hardcoded for 2026)
# ============================================================================


def get_2026_cruise_schedules() -> list[CruiseShipEvent]:
    """
    Returns hardcoded 2026 cruise ship schedules for major Australian ports.

    Based on typical seasonality and major cruise lines operating in Australian waters.
    Approximate data representing 50+ visits annually for Sydney, etc.
    """
    return [
        # Sydney Harbour (major international terminal)
        CruiseShipEvent(
            port_code="SYD",
            port_name="Sydney Harbour",
            arrival_date=datetime(2026, 1, 5, 7, 0),
            departure_date=datetime(2026, 1, 5, 18, 0),
            ship_name="Royal Caribbean Navigator",
            expected_passengers=6000,
            terminal_location=Location(
                latitude=-33.8567, longitude=151.2093, name="Sydney Cruise Terminal"
            ),
        ),
        CruiseShipEvent(
            port_code="SYD",
            port_name="Sydney Harbour",
            arrival_date=datetime(2026, 1, 12, 8, 0),
            departure_date=datetime(2026, 1, 12, 18, 30),
            ship_name="Carnival Spirit",
            expected_passengers=2700,
            terminal_location=Location(
                latitude=-33.8567, longitude=151.2093, name="Sydney Cruise Terminal"
            ),
        ),
        CruiseShipEvent(
            port_code="SYD",
            port_name="Sydney Harbour",
            arrival_date=datetime(2026, 2, 2, 6, 30),
            departure_date=datetime(2026, 2, 2, 18, 0),
            ship_name="P&O Pacific Explorer",
            expected_passengers=3400,
            terminal_location=Location(
                latitude=-33.8567, longitude=151.2093, name="Sydney Cruise Terminal"
            ),
        ),
        # Melbourne (Port Phillip Bay)
        CruiseShipEvent(
            port_code="MEL",
            port_name="Port of Melbourne",
            arrival_date=datetime(2026, 1, 8, 7, 0),
            departure_date=datetime(2026, 1, 8, 17, 0),
            ship_name="Cunard Queen Mary 2",
            expected_passengers=2700,
            terminal_location=Location(
                latitude=-37.8212, longitude=144.9537, name="Melbourne Cruise Terminal"
            ),
        ),
        CruiseShipEvent(
            port_code="MEL",
            port_name="Port of Melbourne",
            arrival_date=datetime(2026, 3, 15, 7, 30),
            departure_date=datetime(2026, 3, 15, 17, 30),
            ship_name="Royal Caribbean Voyager",
            expected_passengers=3800,
            terminal_location=Location(
                latitude=-37.8212, longitude=144.9537, name="Melbourne Cruise Terminal"
            ),
        ),
        # Brisbane (Port of Brisbane)
        CruiseShipEvent(
            port_code="BNE",
            port_name="Port of Brisbane",
            arrival_date=datetime(2026, 1, 10, 6, 0),
            departure_date=datetime(2026, 1, 10, 16, 30),
            ship_name="Carnival Splendor",
            expected_passengers=3900,
            terminal_location=Location(
                latitude=-27.3894, longitude=153.1720, name="Brisbane Cruise Terminal"
            ),
        ),
        CruiseShipEvent(
            port_code="BNE",
            port_name="Port of Brisbane",
            arrival_date=datetime(2026, 2, 25, 7, 0),
            departure_date=datetime(2026, 2, 25, 17, 0),
            ship_name="Disney Cruise Line Wonder",
            expected_passengers=2700,
            terminal_location=Location(
                latitude=-27.3894, longitude=153.1720, name="Brisbane Cruise Terminal"
            ),
        ),
        # Cairns (tropical North Queensland)
        CruiseShipEvent(
            port_code="CNS",
            port_name="Cairns",
            arrival_date=datetime(2026, 6, 5, 6, 0),
            departure_date=datetime(2026, 6, 5, 17, 0),
            ship_name="Cunard Line Queen Elizabeth",
            expected_passengers=2700,
            terminal_location=Location(
                latitude=-16.2859, longitude=145.7781, name="Cairns Cruise Terminal"
            ),
        ),
        CruiseShipEvent(
            port_code="CNS",
            port_name="Cairns",
            arrival_date=datetime(2026, 7, 10, 6, 30),
            departure_date=datetime(2026, 7, 10, 17, 30),
            ship_name="P&O Pacific Adventure",
            expected_passengers=3600,
            terminal_location=Location(
                latitude=-16.2859, longitude=145.7781, name="Cairns Cruise Terminal"
            ),
        ),
        # Fremantle (Western Australia)
        CruiseShipEvent(
            port_code="FRM",
            port_name="Fremantle",
            arrival_date=datetime(2026, 1, 20, 8, 0),
            departure_date=datetime(2026, 1, 20, 17, 0),
            ship_name="Holland America Volendam",
            expected_passengers=1400,
            terminal_location=Location(
                latitude=-32.0575, longitude=115.7452, name="Fremantle Cruise Terminal"
            ),
        ),
        # Adelaide
        CruiseShipEvent(
            port_code="ADL",
            port_name="Adelaide",
            arrival_date=datetime(2026, 2, 10, 7, 0),
            departure_date=datetime(2026, 2, 10, 17, 0),
            ship_name="P&O Pacific Aria",
            expected_passengers=3300,
            terminal_location=Location(
                latitude=-34.7315, longitude=138.5022, name="Adelaide Port"
            ),
        ),
        # Hobart
        CruiseShipEvent(
            port_code="HBA",
            port_name="Hobart",
            arrival_date=datetime(2026, 3, 5, 7, 30),
            departure_date=datetime(2026, 3, 5, 16, 0),
            ship_name="Seabourn Venture",
            expected_passengers=600,
            terminal_location=Location(
                latitude=-42.8816, longitude=147.3303, name="Hobart Cruise Terminal"
            ),
        ),
    ]


# ============================================================================
# Conference & Exhibition Schedule Data
# ============================================================================


def get_2026_conference_schedule() -> list[ConferenceEvent]:
    """
    Returns major Australian conference and exhibition schedule for 2026.

    Includes MCEC (Melbourne), ICC Sydney, BCEC (Brisbane), and other major venues.
    """
    return [
        # Melbourne Convention & Exhibition Centre (MCEC)
        ConferenceEvent(
            name="Australian Healthcare Forum",
            venue_name="Melbourne Convention & Exhibition Centre",
            venue_location=Location(
                latitude=-37.8244, longitude=144.9781, name="MCEC"
            ),
            start_date=datetime(2026, 2, 2),
            end_date=datetime(2026, 2, 4),
            expected_attendees=3500,
            category="conference",
        ),
        ConferenceEvent(
            name="CeBIT Australia",
            venue_name="Melbourne Convention & Exhibition Centre",
            venue_location=Location(
                latitude=-37.8244, longitude=144.9781, name="MCEC"
            ),
            start_date=datetime(2026, 5, 12),
            end_date=datetime(2026, 5, 14),
            expected_attendees=8000,
            category="trade_show",
        ),
        ConferenceEvent(
            name="Australian Retailers Association Summit",
            venue_name="Melbourne Convention & Exhibition Centre",
            venue_location=Location(
                latitude=-37.8244, longitude=144.9781, name="MCEC"
            ),
            start_date=datetime(2026, 8, 18),
            end_date=datetime(2026, 8, 20),
            expected_attendees=2000,
            category="conference",
        ),
        # International Convention Centre Sydney
        ConferenceEvent(
            name="International Architecture Congress",
            venue_name="International Convention Centre Sydney",
            venue_location=Location(
                latitude=-33.8767, longitude=151.1998, name="ICC Sydney"
            ),
            start_date=datetime(2026, 3, 9),
            end_date=datetime(2026, 3, 11),
            expected_attendees=4000,
            category="conference",
        ),
        ConferenceEvent(
            name="Sydney Wine & Food Expo",
            venue_name="International Convention Centre Sydney",
            venue_location=Location(
                latitude=-33.8767, longitude=151.1998, name="ICC Sydney"
            ),
            start_date=datetime(2026, 6, 18),
            end_date=datetime(2026, 6, 20),
            expected_attendees=5000,
            category="exhibition",
        ),
        # Brisbane Convention & Exhibition Centre (BCEC)
        ConferenceEvent(
            name="Queensland Mining & Energy Forum",
            venue_name="Brisbane Convention & Exhibition Centre",
            venue_location=Location(
                latitude=-27.4766, longitude=153.0166, name="BCEC"
            ),
            start_date=datetime(2026, 4, 7),
            end_date=datetime(2026, 4, 9),
            expected_attendees=3000,
            category="conference",
        ),
        ConferenceEvent(
            name="Australian Hospitality & Tourism Expo",
            venue_name="Brisbane Convention & Exhibition Centre",
            venue_location=Location(
                latitude=-27.4766, longitude=153.0166, name="BCEC"
            ),
            start_date=datetime(2026, 7, 14),
            end_date=datetime(2026, 7, 16),
            expected_attendees=4500,
            category="exhibition",
        ),
    ]


# ============================================================================
# Individual Adapters
# ============================================================================


class HotelOccupancyAdapter(DataFeedAdapter):
    """
    Fetches hotel occupancy data from STR (Smith Travel Research).

    Signals high occupancy (>85%) as moderate_positive for dining and beverages.
    """

    def __init__(
        self,
        str_api_key: Optional[str] = None,
        cache_ttl_seconds: int = 6 * 3600,
    ):
        """
        Initialize HotelOccupancyAdapter.

        Args:
            str_api_key: STR API key for accessing occupancy data.
            cache_ttl_seconds: Cache TTL for occupancy data (default 6 hours).
        """
        self.str_api_key = str_api_key
        self.base_url = "https://api.str.com/v1"
        self.cache = SignalCache(ttl_seconds=cache_ttl_seconds)
        logger.info(
            "HotelOccupancyAdapter initialized with cache TTL %ds", cache_ttl_seconds
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch hotel occupancy signals for a given location.

        Args:
            location: Location to fetch occupancy data for.

        Returns:
            List of FeedSignal objects representing occupancy levels.
        """
        cache_key = f"hotel_occupancy:{location.name}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug("Returning cached hotel occupancy for %s", location.name)
            return cached

        signals = []
        try:
            occupancy_data = await self._fetch_occupancy(location)
            if occupancy_data:
                strength = self._occupancy_to_strength(occupancy_data.occupancy_rate)
                confidence = 0.9 if occupancy_data.sample_size > 50 else 0.6

                signal = FeedSignal(
                    category=FeedCategory.tourism,
                    signal_type="hotel_occupancy",
                    strength=strength,
                    location=location,
                    confidence=confidence,
                    metadata={
                        "occupancy_rate": occupancy_data.occupancy_rate,
                        "avg_daily_rate": occupancy_data.average_daily_rate,
                        "revpar": occupancy_data.revenue_per_available_room,
                        "sample_size": occupancy_data.sample_size,
                    },
                    timestamp=datetime.utcnow(),
                )
                signals.append(signal)
                logger.info(
                    "Hotel occupancy at %s: %.1f%% → %s",
                    location.name,
                    occupancy_data.occupancy_rate * 100,
                    strength.name,
                )

            self.cache.set(cache_key, signals)
        except Exception as e:
            logger.error("Failed to fetch hotel occupancy for %s: %s", location.name, e)

        return signals

    async def _fetch_occupancy(self, location: Location) -> Optional[HotelOccupancyData]:
        """
        Fetch occupancy data from STR API.

        Args:
            location: Location to fetch data for.

        Returns:
            HotelOccupancyData if successful, None otherwise.
        """
        if not self.str_api_key:
            logger.warning("STR API key not configured, skipping fetch")
            return None

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/occupancy",
                    params={
                        "api_key": self.str_api_key,
                        "market": location.name,
                    },
                )
                response.raise_for_status()
                data = response.json()

                return HotelOccupancyData(
                    region=location.name,
                    occupancy_rate=data.get("occupancy_rate", 0.5),
                    average_daily_rate=data.get("adr", 0),
                    revenue_per_available_room=data.get("revpar", 0),
                    sample_size=data.get("sample_size", 0),
                    timestamp=datetime.utcnow(),
                )
        except httpx.HTTPError as e:
            logger.error("HTTP error fetching STR data: %s", e)
            return None

    @staticmethod
    def _occupancy_to_strength(occupancy_rate: float) -> SignalStrength:
        """
        Convert occupancy rate to signal strength.

        Args:
            occupancy_rate: Occupancy rate between 0.0 and 1.0.

        Returns:
            SignalStrength indicating demand level.
        """
        if occupancy_rate >= 0.85:
            return SignalStrength.moderate_positive
        elif occupancy_rate >= 0.70:
            return SignalStrength.weak_positive
        elif occupancy_rate < 0.40:
            return SignalStrength.weak_negative
        else:
            return SignalStrength.neutral


class AirbnbOccupancyAdapter(DataFeedAdapter):
    """
    Fetches Airbnb/short-stay occupancy data from AirDNA.

    Signals high occupancy as moderate_positive for venue dining trade.
    """

    def __init__(
        self,
        airdna_api_key: Optional[str] = None,
        cache_ttl_seconds: int = 6 * 3600,
    ):
        """
        Initialize AirbnbOccupancyAdapter.

        Args:
            airdna_api_key: AirDNA API key.
            cache_ttl_seconds: Cache TTL for occupancy data (default 6 hours).
        """
        self.airdna_api_key = airdna_api_key
        self.base_url = "https://api.airdna.co/v1"
        self.cache = SignalCache(ttl_seconds=cache_ttl_seconds)
        logger.info(
            "AirbnbOccupancyAdapter initialized with cache TTL %ds", cache_ttl_seconds
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch Airbnb occupancy signals for a given location.

        Args:
            location: Location to fetch occupancy data for.

        Returns:
            List of FeedSignal objects representing occupancy levels.
        """
        cache_key = f"airbnb_occupancy:{location.name}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug("Returning cached Airbnb occupancy for %s", location.name)
            return cached

        signals = []
        try:
            occupancy_data = await self._fetch_occupancy(location)
            if occupancy_data:
                strength = self._occupancy_to_strength(occupancy_data.occupancy_rate)
                confidence = 0.85

                signal = FeedSignal(
                    category=FeedCategory.tourism,
                    signal_type="airbnb_occupancy",
                    strength=strength,
                    location=location,
                    confidence=confidence,
                    metadata={
                        "occupancy_rate": occupancy_data.occupancy_rate,
                        "avg_nightly_rate": occupancy_data.average_nightly_rate,
                        "active_listings": occupancy_data.active_listings,
                    },
                    timestamp=datetime.utcnow(),
                )
                signals.append(signal)
                logger.info(
                    "Airbnb occupancy at %s: %.1f%% → %s",
                    location.name,
                    occupancy_data.occupancy_rate * 100,
                    strength.name,
                )

            self.cache.set(cache_key, signals)
        except Exception as e:
            logger.error("Failed to fetch Airbnb occupancy for %s: %s", location.name, e)

        return signals

    async def _fetch_occupancy(self, location: Location) -> Optional[AirbnbOccupancyData]:
        """
        Fetch occupancy data from AirDNA API.

        Args:
            location: Location to fetch data for.

        Returns:
            AirbnbOccupancyData if successful, None otherwise.
        """
        if not self.airdna_api_key:
            logger.warning("AirDNA API key not configured, skipping fetch")
            return None

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/rentaldata",
                    headers={"Authorization": f"Bearer {self.airdna_api_key}"},
                    params={"market": location.name},
                )
                response.raise_for_status()
                data = response.json()

                return AirbnbOccupancyData(
                    region=location.name,
                    occupancy_rate=data.get("occupancy_rate", 0.5),
                    average_nightly_rate=data.get("adr", 0),
                    active_listings=data.get("active_listings", 0),
                    timestamp=datetime.utcnow(),
                )
        except httpx.HTTPError as e:
            logger.error("HTTP error fetching AirDNA data: %s", e)
            return None

    @staticmethod
    def _occupancy_to_strength(occupancy_rate: float) -> SignalStrength:
        """
        Convert occupancy rate to signal strength.

        Args:
            occupancy_rate: Occupancy rate between 0.0 and 1.0.

        Returns:
            SignalStrength indicating demand level.
        """
        if occupancy_rate >= 0.80:
            return SignalStrength.moderate_positive
        elif occupancy_rate >= 0.65:
            return SignalStrength.weak_positive
        else:
            return SignalStrength.neutral


class CruiseShipAdapter(DataFeedAdapter):
    """
    Monitors cruise ship arrivals and departures at major Australian ports.

    Signals strong_positive for waterfront venues, moderate_positive for city venues.
    Proximity-based: 2km radius = strong, 10km = weak.
    """

    PROXIMITY_STRONG_KM = 2.0
    PROXIMITY_MODERATE_KM = 5.0
    PROXIMITY_WEAK_KM = 10.0

    def __init__(self, cache_ttl_seconds: int = 24 * 3600):
        """
        Initialize CruiseShipAdapter with hardcoded 2026 schedule.

        Args:
            cache_ttl_seconds: Cache TTL for cruise schedules (default 24 hours).
        """
        self.schedules = get_2026_cruise_schedules()
        self.cache = SignalCache(ttl_seconds=cache_ttl_seconds)
        logger.info(
            "CruiseShipAdapter initialized with %d hardcoded events",
            len(self.schedules),
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch cruise ship signals for a given location.

        Args:
            location: Location to fetch signals for.

        Returns:
            List of FeedSignal objects for nearby cruise ships.
        """
        cache_key = f"cruise_ships:{location.name}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug("Returning cached cruise ship signals for %s", location.name)
            return cached

        signals = []
        now = datetime.utcnow()

        for event in self.schedules:
            # Check if ship is currently in port (within arrival/departure window)
            if event.arrival_date <= now <= event.departure_date:
                distance_km = self._calculate_distance(location, event.terminal_location)

                # Determine strength based on proximity
                if distance_km <= self.PROXIMITY_STRONG_KM:
                    strength = SignalStrength.strong_positive
                    confidence = 0.95
                elif distance_km <= self.PROXIMITY_MODERATE_KM:
                    strength = SignalStrength.moderate_positive
                    confidence = 0.90
                elif distance_km <= self.PROXIMITY_WEAK_KM:
                    strength = SignalStrength.weak_positive
                    confidence = 0.75
                else:
                    continue  # Too far away

                signal = FeedSignal(
                    category=FeedCategory.tourism,
                    signal_type="cruise_ship_in_port",
                    strength=strength,
                    location=location,
                    confidence=confidence,
                    metadata={
                        "ship_name": event.ship_name,
                        "port": event.port_name,
                        "passengers": event.expected_passengers,
                        "arrival": event.arrival_date.isoformat(),
                        "departure": event.departure_date.isoformat(),
                        "distance_km": round(distance_km, 2),
                    },
                    timestamp=now,
                )
                signals.append(signal)
                logger.info(
                    "%s in port at %s: %d passengers, %.1f km away",
                    event.ship_name,
                    event.port_name,
                    event.expected_passengers,
                    distance_km,
                )

        self.cache.set(cache_key, signals)
        return signals

    @staticmethod
    def _calculate_distance(loc1: Location, loc2: Location) -> float:
        """
        Calculate approximate distance between two locations using haversine formula.

        Args:
            loc1: First location.
            loc2: Second location.

        Returns:
            Distance in kilometers.
        """
        from math import asin, cos, radians, sin, sqrt

        lat1, lon1 = radians(loc1.latitude), radians(loc1.longitude)
        lat2, lon2 = radians(loc2.latitude), radians(loc2.longitude)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Earth radius in km

        return c * r


class ConferenceAdapter(DataFeedAdapter):
    """
    Monitors major conferences and exhibitions at Australian venues.

    Signals moderate_positive for delegate dining and entertainment.
    """

    def __init__(self, cache_ttl_seconds: int = 24 * 3600):
        """
        Initialize ConferenceAdapter with hardcoded 2026 events.

        Args:
            cache_ttl_seconds: Cache TTL for conference schedules (default 24 hours).
        """
        self.events = get_2026_conference_schedule()
        self.cache = SignalCache(ttl_seconds=cache_ttl_seconds)
        logger.info(
            "ConferenceAdapter initialized with %d hardcoded events", len(self.events)
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch conference signals for a given location.

        Args:
            location: Location to fetch signals for.

        Returns:
            List of FeedSignal objects for nearby conferences.
        """
        cache_key = f"conferences:{location.name}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug("Returning cached conference signals for %s", location.name)
            return cached

        signals = []
        now = datetime.utcnow()

        for event in self.events:
            # Check if conference is active or starting soon (within 3 days)
            days_until_start = (event.start_date - now).days
            if event.start_date <= now <= event.end_date or (0 < days_until_start <= 3):
                distance_km = self._calculate_distance(location, event.venue_location)

                # Conferences typically affect venues within 2km
                if distance_km > 5.0:
                    continue

                strength = SignalStrength.moderate_positive
                confidence = 0.85 if event.start_date <= now <= event.end_date else 0.65

                signal = FeedSignal(
                    category=FeedCategory.tourism,
                    signal_type="conference_active",
                    strength=strength,
                    location=location,
                    confidence=confidence,
                    metadata={
                        "event_name": event.name,
                        "venue": event.venue_name,
                        "attendees": event.expected_attendees,
                        "category": event.category,
                        "start_date": event.start_date.isoformat(),
                        "end_date": event.end_date.isoformat(),
                        "distance_km": round(distance_km, 2),
                    },
                    timestamp=now,
                )
                signals.append(signal)
                logger.info(
                    "%s at %s: %d attendees",
                    event.name,
                    event.venue_name,
                    event.expected_attendees,
                )

        self.cache.set(cache_key, signals)
        return signals

    @staticmethod
    def _calculate_distance(loc1: Location, loc2: Location) -> float:
        """
        Calculate approximate distance between two locations using haversine formula.

        Args:
            loc1: First location.
            loc2: Second location.

        Returns:
            Distance in kilometers.
        """
        from math import asin, cos, radians, sin, sqrt

        lat1, lon1 = radians(loc1.latitude), radians(loc1.longitude)
        lat2, lon2 = radians(loc2.latitude), radians(loc2.longitude)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Earth radius in km

        return c * r


class TransportDisruptionAdapter(DataFeedAdapter):
    """
    Monitors public transport disruptions affecting venue accessibility.

    Signals negative impact when major disruptions affect regional transport.
    Checks PTV (VIC), TfNSW, TransLink (QLD) APIs.
    """

    DISRUPTION_CACHE_TTL = 30 * 60  # 30 minutes for disruption data

    def __init__(
        self,
        ptv_api_key: Optional[str] = None,
        tnsw_api_key: Optional[str] = None,
        translink_api_key: Optional[str] = None,
    ):
        """
        Initialize TransportDisruptionAdapter.

        Args:
            ptv_api_key: PTV API key for Victoria.
            tnsw_api_key: TfNSW API key for NSW.
            translink_api_key: TransLink API key for Queensland.
        """
        self.ptv_api_key = ptv_api_key
        self.tnsw_api_key = tnsw_api_key
        self.translink_api_key = translink_api_key
        self.cache = SignalCache(ttl_seconds=self.DISRUPTION_CACHE_TTL)
        logger.info("TransportDisruptionAdapter initialized")

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch transport disruption signals for a given location.

        Args:
            location: Location to fetch disruptions for.

        Returns:
            List of FeedSignal objects for active disruptions.
        """
        cache_key = f"transport_disruptions:{location.name}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug(
                "Returning cached transport disruptions for %s", location.name
            )
            return cached

        signals = []
        try:
            # Fetch from all available sources based on location
            disruptions = await asyncio.gather(
                self._fetch_ptv_disruptions(location),
                self._fetch_tnsw_disruptions(location),
                self._fetch_translink_disruptions(location),
            )

            for disruption_list in disruptions:
                for disruption in disruption_list:
                    strength = self._disruption_to_strength(disruption)
                    confidence = 0.85

                    signal = FeedSignal(
                        category=FeedCategory.tourism,
                        signal_type="transport_disruption",
                        strength=strength,
                        location=location,
                        confidence=confidence,
                        metadata={
                            "disruption_id": disruption.disruption_id,
                            "route": disruption.route_name,
                            "type": disruption.disruption_type,
                            "severity": disruption.severity,
                            "start": disruption.start_datetime.isoformat(),
                            "end": disruption.end_datetime.isoformat(),
                        },
                        timestamp=datetime.utcnow(),
                    )
                    signals.append(signal)
                    logger.info(
                        "Transport disruption on %s: %s severity",
                        disruption.route_name,
                        disruption.severity,
                    )

            self.cache.set(cache_key, signals)
        except Exception as e:
            logger.error(
                "Failed to fetch transport disruptions for %s: %s", location.name, e
            )

        return signals

    async def _fetch_ptv_disruptions(self, location: Location) -> list[TransportDisruption]:
        """
        Fetch disruptions from PTV (Victoria) API.

        Args:
            location: Location to fetch disruptions for.

        Returns:
            List of TransportDisruption objects.
        """
        if not self.ptv_api_key or location.name not in ["Melbourne", "Victoria"]:
            return []

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://timetableapi.ptv.vic.gov.au/v3/disruptions",
                    params={"api_key": self.ptv_api_key},
                )
                response.raise_for_status()
                data = response.json()

                disruptions = []
                for d in data.get("disruptions", []):
                    if d.get("is_current"):
                        disruptions.append(
                            TransportDisruption(
                                disruption_id=str(d.get("id", "")),
                                route_name=d.get("description", "Unknown route"),
                                disruption_type=d.get("type", "unknown"),
                                affected_area=location,
                                start_datetime=datetime.fromisoformat(
                                    d.get("from_date", "")
                                ),
                                end_datetime=datetime.fromisoformat(
                                    d.get("to_date", "")
                                ),
                                severity="high"
                                if "major" in d.get("description", "").lower()
                                else "medium",
                            )
                        )
                return disruptions
        except httpx.HTTPError as e:
            logger.error("HTTP error fetching PTV disruptions: %s", e)
            return []

    async def _fetch_tnsw_disruptions(self, location: Location) -> list[TransportDisruption]:
        """
        Fetch disruptions from TfNSW API.

        Args:
            location: Location to fetch disruptions for.

        Returns:
            List of TransportDisruption objects.
        """
        if not self.tnsw_api_key or location.name not in ["Sydney", "NSW"]:
            return []

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://api.transport.nsw.gov.au/v1/disruptions",
                    headers={"Authorization": f"apikey {self.tnsw_api_key}"},
                )
                response.raise_for_status()
                data = response.json()

                disruptions = []
                for d in data.get("disruptions", []):
                    if d.get("status") == "active":
                        disruptions.append(
                            TransportDisruption(
                                disruption_id=str(d.get("id", "")),
                                route_name=d.get("mode", "Unknown") + " disruption",
                                disruption_type=d.get("type", "unknown"),
                                affected_area=location,
                                start_datetime=datetime.fromisoformat(
                                    d.get("start_date", "")
                                ),
                                end_datetime=datetime.fromisoformat(
                                    d.get("end_date", "")
                                ),
                                severity=d.get("severity", "medium"),
                            )
                        )
                return disruptions
        except httpx.HTTPError as e:
            logger.error("HTTP error fetching TfNSW disruptions: %s", e)
            return []

    async def _fetch_translink_disruptions(
        self, location: Location
    ) -> list[TransportDisruption]:
        """
        Fetch disruptions from TransLink (Queensland) API.

        Args:
            location: Location to fetch disruptions for.

        Returns:
            List of TransportDisruption objects.
        """
        if (
            not self.translink_api_key
            or location.name not in ["Brisbane", "Queensland"]
        ):
            return []

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://api.translink.com.au/disruptions",
                    headers={"Authorization": f"Bearer {self.translink_api_key}"},
                )
                response.raise_for_status()
                data = response.json()

                disruptions = []
                for d in data.get("disruptions", []):
                    if d.get("is_active"):
                        disruptions.append(
                            TransportDisruption(
                                disruption_id=str(d.get("id", "")),
                                route_name=d.get("route", "Unknown route"),
                                disruption_type=d.get("type", "unknown"),
                                affected_area=location,
                                start_datetime=datetime.fromisoformat(
                                    d.get("start_time", "")
                                ),
                                end_datetime=datetime.fromisoformat(
                                    d.get("end_time", "")
                                ),
                                severity=d.get("severity", "medium"),
                            )
                        )
                return disruptions
        except httpx.HTTPError as e:
            logger.error("HTTP error fetching TransLink disruptions: %s", e)
            return []

    @staticmethod
    def _disruption_to_strength(disruption: TransportDisruption) -> SignalStrength:
        """
        Convert disruption severity to signal strength.

        Args:
            disruption: TransportDisruption object.

        Returns:
            SignalStrength indicating impact level.
        """
        if disruption.disruption_type == "closure":
            return SignalStrength.moderate_negative
        elif disruption.severity == "high":
            return SignalStrength.moderate_negative
        elif disruption.severity == "medium":
            return SignalStrength.weak_negative
        else:
            return SignalStrength.weak_negative


class TourismAggregator(DataFeedAdapter):
    """
    Composite adapter that combines signals from all tourism sub-adapters.

    Aggregates hotel occupancy, Airbnb data, cruise ship schedules, conferences,
    and transport disruptions into a unified tourism demand signal.
    """

    def __init__(
        self,
        str_api_key: Optional[str] = None,
        airdna_api_key: Optional[str] = None,
        ptv_api_key: Optional[str] = None,
        tnsw_api_key: Optional[str] = None,
        translink_api_key: Optional[str] = None,
    ):
        """
        Initialize TourismAggregator with all sub-adapters.

        Args:
            str_api_key: STR API key for hotel data.
            airdna_api_key: AirDNA API key for Airbnb data.
            ptv_api_key: PTV API key for Victoria transport.
            tnsw_api_key: TfNSW API key for NSW transport.
            translink_api_key: TransLink API key for Queensland transport.
        """
        self.hotel_adapter = HotelOccupancyAdapter(str_api_key=str_api_key)
        self.airbnb_adapter = AirbnbOccupancyAdapter(airdna_api_key=airdna_api_key)
        self.cruise_adapter = CruiseShipAdapter()
        self.conference_adapter = ConferenceAdapter()
        self.transport_adapter = TransportDisruptionAdapter(
            ptv_api_key=ptv_api_key,
            tnsw_api_key=tnsw_api_key,
            translink_api_key=translink_api_key,
        )
        logger.info("TourismAggregator initialized with 5 sub-adapters")

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """
        Fetch combined tourism signals for a location from all sub-adapters.

        Args:
            location: Location to fetch signals for.

        Returns:
            Aggregated list of FeedSignal objects from all adapters.
        """
        try:
            # Fetch signals from all adapters in parallel
            results = await asyncio.gather(
                self.hotel_adapter.fetch_signals(location),
                self.airbnb_adapter.fetch_signals(location),
                self.cruise_adapter.fetch_signals(location),
                self.conference_adapter.fetch_signals(location),
                self.transport_adapter.fetch_signals(location),
            )

            # Flatten and combine all signals
            all_signals = []
            for signal_list in results:
                all_signals.extend(signal_list)

            logger.info(
                "TourismAggregator fetched %d signals for %s",
                len(all_signals),
                location.name,
            )
            return all_signals
        except Exception as e:
            logger.error("Failed to aggregate tourism signals for %s: %s", location.name, e)
            return []
