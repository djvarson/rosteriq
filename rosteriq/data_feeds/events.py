"""
Events Adapter for RosterIQ
============================

Pulls event data (stadium games, concerts, festivals, etc.) from various sources
to provide demand spikes for the RosterIQ variance engine.

Sources:
  - PerthIsOK: Perth events calendar (assumes JSON endpoint at https://www.perthisok.com/events.json)
  - Stadium Schedules: Static JSON feeds for stadium games

Event categories: stadium, concert, festival, comedy, community, other

VenueEvent captures:
  - event_id: Unique identifier
  - title: Event name
  - start_time, end_time: tz-aware datetimes
  - location_name: Venue/location name (if different from venue being analysed)
  - lat, lon: Coordinates for distance calculation
  - distance_km_from_venue: Computed distance if venue has lat/lon config
  - expected_attendance: Estimated attendance (useful for demand signal)
  - category: Event type
  - source: Data source (perthisok, stadium, demo)
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional
from abc import ABC, abstractmethod

try:
    import httpx
except ImportError:
    httpx = None

logger = logging.getLogger("rosteriq.data_feeds.events")

AU_TZ = timezone(timedelta(hours=10))  # AEST


class EventCategory(str, Enum):
    """Supported event categories."""
    STADIUM = "stadium"
    CONCERT = "concert"
    FESTIVAL = "festival"
    COMEDY = "comedy"
    COMMUNITY = "community"
    OTHER = "other"


class EventsAdapterError(Exception):
    """Base exception for events adapter errors."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class VenueEvent:
    """Single event that may drive demand at a venue."""
    event_id: str
    title: str
    start_time: datetime  # tz-aware
    end_time: Optional[datetime] = None  # tz-aware
    location_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    distance_km_from_venue: Optional[float] = None
    expected_attendance: Optional[int] = None
    category: str = EventCategory.OTHER.value
    source: str = "unknown"

    def __post_init__(self):
        """Ensure datetimes are tz-aware."""
        if self.start_time.tzinfo is None:
            self.start_time = self.start_time.replace(tzinfo=AU_TZ)
        if self.end_time and self.end_time.tzinfo is None:
            self.end_time = self.end_time.replace(tzinfo=AU_TZ)


# ---------------------------------------------------------------------------
# Distance Calculation
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance in km between two lat/lon points using Haversine formula.

    Args:
        lat1, lon1: First point (degrees)
        lat2, lon2: Second point (degrees)

    Returns:
        Distance in km
    """
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


# ---------------------------------------------------------------------------
# Abstract Events Adapter
# ---------------------------------------------------------------------------

class EventsAdapter(ABC):
    """Abstract base class for event data sources."""

    @abstractmethod
    async def get_events(
        self,
        venue_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueEvent]:
        """
        Fetch events for a venue within a time window.

        Args:
            venue_id: RosterIQ venue identifier
            window_start: Start of window (tz-aware)
            window_end: End of window (tz-aware)

        Returns:
            List of VenueEvent objects with start_time in [window_start, window_end]
        """
        pass


# ---------------------------------------------------------------------------
# PerthIsOK Adapter
# ---------------------------------------------------------------------------

class PerthIsOKAdapter(EventsAdapter):
    """
    Fetches events from perthisok.com.

    Endpoint assumption: https://www.perthisok.com/events.json returns JSON array:
      [
        {
          "id": "event_123",
          "title": "Perth Comedy Night",
          "start_date": "2026-04-20",
          "start_time": "19:30",
          "end_date": "2026-04-20",
          "end_time": "21:00",
          "venue_name": "Comedy Store Perth",
          "latitude": -31.9505,
          "longitude": 115.8605,
          "category": "comedy",
          "expected_attendance": 150
        },
        ...
      ]

    If the real endpoint differs in shape, this parser can be adapted.
    """

    BASE_URL = "https://www.perthisok.com"
    EVENTS_ENDPOINT = "/events.json"
    TIMEOUT = 30

    # Perth venues reference coordinates (optional fallback)
    PERTH_VENUES = {
        "perth_cbd": (-31.9505, 115.8605),
        "northbridge": (-31.9430, 115.8634),
        "subiaco": (-31.9809, 115.8159),
    }

    def __init__(self):
        if not httpx:
            raise ImportError("httpx is required. Install with: pip install httpx")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.TIMEOUT)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_events(
        self,
        venue_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueEvent]:
        """
        Fetch events from PerthIsOK and filter to time window.

        Args:
            venue_id: RosterIQ venue ID (for distance filtering)
            window_start: Start of window (tz-aware)
            window_end: End of window (tz-aware)

        Returns:
            List of VenueEvent objects
        """
        try:
            client = await self._get_client()
            url = f"{self.BASE_URL}{self.EVENTS_ENDPOINT}"
            resp = await client.get(url)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list):
                logger.warning(f"PerthIsOK returned non-list: {type(data)}")
                return []

            events = []
            for raw in data:
                try:
                    event = self._parse_event(raw)
                    # Filter to window
                    if window_start <= event.start_time <= window_end:
                        events.append(event)
                except Exception as e:
                    logger.warning(f"Failed to parse PerthIsOK event: {e}")
                    continue

            return events

        except Exception as e:
            logger.error(f"Failed to fetch PerthIsOK events: {e}")
            raise EventsAdapterError(f"PerthIsOK fetch failed: {e}") from e

    def _parse_event(self, raw: dict) -> VenueEvent:
        """
        Parse a single event from PerthIsOK JSON.

        Assumes fields: id, title, start_date, start_time, end_date, end_time,
        venue_name, latitude, longitude, category, expected_attendance.
        """
        event_id = raw.get("id", "")
        title = raw.get("title", "")

        # Parse start datetime
        start_date_str = raw.get("start_date", "")
        start_time_str = raw.get("start_time", "00:00")
        try:
            start_dt = datetime.fromisoformat(f"{start_date_str}T{start_time_str}")
            start_dt = start_dt.replace(tzinfo=AU_TZ)
        except (ValueError, TypeError):
            start_dt = datetime.now(AU_TZ)

        # Parse end datetime
        end_dt = None
        end_date_str = raw.get("end_date")
        end_time_str = raw.get("end_time")
        if end_date_str and end_time_str:
            try:
                end_dt = datetime.fromisoformat(f"{end_date_str}T{end_time_str}")
                end_dt = end_dt.replace(tzinfo=AU_TZ)
            except (ValueError, TypeError):
                pass

        lat = raw.get("latitude")
        lon = raw.get("longitude")
        if lat and lon:
            try:
                lat = float(lat)
                lon = float(lon)
            except (ValueError, TypeError):
                lat = lon = None

        location_name = raw.get("venue_name")
        category = raw.get("category", EventCategory.OTHER.value)
        expected_attendance = raw.get("expected_attendance")
        if expected_attendance:
            try:
                expected_attendance = int(expected_attendance)
            except (ValueError, TypeError):
                expected_attendance = None

        return VenueEvent(
            event_id=event_id,
            title=title,
            start_time=start_dt,
            end_time=end_dt,
            location_name=location_name,
            lat=lat,
            lon=lon,
            expected_attendance=expected_attendance,
            category=category,
            source="perthisok",
        )


# ---------------------------------------------------------------------------
# Stadium Schedule Adapter
# ---------------------------------------------------------------------------

class StadiumScheduleAdapter(EventsAdapter):
    """
    Polls static JSON schedule URLs for stadium games.

    Config format:
      {
        "venue_id_1": {
          "name": "Optus Stadium",
          "lat": -31.945,
          "lon": 115.836,
          "schedule_url": "https://example.com/optus_schedule.json",
          "attendance_capacity": 60000
        },
        ...
      }

    Schedule JSON endpoint format (assumed):
      [
        {
          "id": "game_123",
          "title": "Perth Glory vs Western United",
          "start_time": "2026-04-18T19:30:00+10:00",
          "end_time": "2026-04-18T21:30:00+10:00",
          "attendance": 45000
        },
        ...
      ]
    """

    def __init__(self, venue_config: dict[str, dict[str, Any]]):
        """
        Args:
            venue_config: Dict mapping venue_id to config (name, lat, lon, schedule_url, attendance_capacity)
        """
        if not httpx:
            raise ImportError("httpx is required. Install with: pip install httpx")
        self.venue_config = venue_config
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_events(
        self,
        venue_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueEvent]:
        """Fetch stadium games for a venue."""
        if venue_id not in self.venue_config:
            return []

        config = self.venue_config[venue_id]
        schedule_url = config.get("schedule_url")
        if not schedule_url:
            return []

        try:
            client = await self._get_client()
            resp = await client.get(schedule_url)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list):
                logger.warning(f"Stadium schedule returned non-list: {type(data)}")
                return []

            events = []
            capacity = config.get("attendance_capacity", 0)
            venue_lat = config.get("lat")
            venue_lon = config.get("lon")

            for raw in data:
                try:
                    event = self._parse_game(raw, capacity, venue_lat, venue_lon)
                    # Filter to window
                    if window_start <= event.start_time <= window_end:
                        events.append(event)
                except Exception as e:
                    logger.warning(f"Failed to parse stadium game: {e}")
                    continue

            return events

        except Exception as e:
            logger.error(f"Failed to fetch stadium schedule for {venue_id}: {e}")
            raise EventsAdapterError(f"Stadium schedule fetch failed: {e}") from e

    def _parse_game(
        self,
        raw: dict,
        capacity: int,
        venue_lat: Optional[float],
        venue_lon: Optional[float],
    ) -> VenueEvent:
        """Parse a single game from stadium schedule JSON."""
        event_id = raw.get("id", "")
        title = raw.get("title", "")

        # Parse ISO datetime
        try:
            start_dt = datetime.fromisoformat(raw.get("start_time", ""))
        except (ValueError, TypeError):
            start_dt = datetime.now(AU_TZ)

        try:
            end_dt = datetime.fromisoformat(raw.get("end_time", ""))
        except (ValueError, TypeError):
            end_dt = None

        # Attendance: use provided or fallback to capacity
        attendance = raw.get("attendance")
        if not attendance:
            attendance = capacity if capacity > 0 else None
        else:
            try:
                attendance = int(attendance)
            except (ValueError, TypeError):
                attendance = capacity if capacity > 0 else None

        return VenueEvent(
            event_id=event_id,
            title=title,
            start_time=start_dt,
            end_time=end_dt,
            expected_attendance=attendance,
            category=EventCategory.STADIUM.value,
            source="stadium",
            lat=venue_lat,
            lon=venue_lon,
        )


# ---------------------------------------------------------------------------
# Composite Adapter (deduplicates events)
# ---------------------------------------------------------------------------

class CompositeEventsAdapter(EventsAdapter):
    """
    Merges results from multiple adapters, deduping by (title, start_time).
    """

    def __init__(self, adapters: list[EventsAdapter]):
        self.adapters = adapters

    async def get_events(
        self,
        venue_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueEvent]:
        """Fetch and merge events from all adapters."""
        all_events = []

        for adapter in self.adapters:
            try:
                events = await adapter.get_events(venue_id, window_start, window_end)
                all_events.extend(events)
            except Exception as e:
                logger.warning(f"Adapter {adapter.__class__.__name__} failed: {e}")
                continue

        # Dedupe by (title, start_time)
        seen = set()
        deduped = []
        for event in all_events:
            key = (event.title, event.start_time)
            if key not in seen:
                seen.add(key)
                deduped.append(event)

        return deduped

    async def close(self):
        for adapter in self.adapters:
            if hasattr(adapter, "close"):
                try:
                    await adapter.close()
                except Exception as e:
                    logger.warning(f"Failed to close {adapter.__class__.__name__}: {e}")


# ---------------------------------------------------------------------------
# Demo Adapter
# ---------------------------------------------------------------------------

class DemoEventsAdapter(EventsAdapter):
    """
    Generates realistic demo events for a Brisbane venue.

    Always returns: HBF Park game (Sat 7pm, 40k attendance, 2km away),
    comedy night (weekend 8pm, 150 attendance, 0.5km away),
    festival (Sun 2pm, 2k attendance, 3km away).

    Deterministic based on venue_id seed.
    """

    # Brisbane CBD coordinates
    BRISBANE_CBD_LAT = -27.4698
    BRISBANE_CBD_LON = 153.0251

    def __init__(self):
        pass

    async def get_events(
        self,
        venue_id: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueEvent]:
        """Generate demo events seeded by venue_id."""
        # Use venue_id as seed for determinism
        seed = sum(ord(c) for c in venue_id) % 1000

        # Find next Saturday and Sunday from window_start
        days_until_sat = (5 - window_start.weekday()) % 7
        if days_until_sat == 0:
            days_until_sat = 7
        next_sat = window_start + timedelta(days=days_until_sat)
        next_sun = next_sat + timedelta(days=1)

        events = []

        # Stadium event: HBF Park game Sat 7pm
        if window_start <= next_sat.replace(hour=19, minute=0) <= window_end:
            events.append(VenueEvent(
                event_id=f"hbf_park_{seed}",
                title="HBF Park - Stadium Game",
                start_time=next_sat.replace(hour=19, minute=0, second=0, microsecond=0),
                end_time=next_sat.replace(hour=21, minute=0, second=0, microsecond=0),
                lat=-31.9435,  # HBF Park, Perth
                lon=115.8159,
                distance_km_from_venue=2.0,
                expected_attendance=40000,
                category=EventCategory.STADIUM.value,
                source="demo",
            ))

        # Comedy: weekend evening
        comedy_time = next_sat.replace(hour=20, minute=0, second=0, microsecond=0)
        if window_start <= comedy_time <= window_end:
            events.append(VenueEvent(
                event_id=f"comedy_{seed}",
                title="Comedy Night - Local Venue",
                start_time=comedy_time,
                end_time=comedy_time + timedelta(hours=2),
                distance_km_from_venue=0.5,
                expected_attendance=150,
                category=EventCategory.COMEDY.value,
                source="demo",
            ))

        # Festival: Sunday afternoon
        festival_time = next_sun.replace(hour=14, minute=0, second=0, microsecond=0)
        if window_start <= festival_time <= window_end:
            events.append(VenueEvent(
                event_id=f"festival_{seed}",
                title="Local Community Festival",
                start_time=festival_time,
                end_time=festival_time + timedelta(hours=4),
                distance_km_from_venue=3.0,
                expected_attendance=2000,
                category=EventCategory.FESTIVAL.value,
                source="demo",
            ))

        return events
