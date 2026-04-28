"""
Events data feed adapter for RosterIQ.

Aggregates event data from multiple sources (Ticketmaster, Eventbrite, PredictHQ,
local council feeds) to predict demand spikes. Events are classified by type and
expected attendance, with signals mapped to hour-specific demand patterns
(pre-event dining, post-event drinks).

Supports deduplication across sources and caching of results.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

from rosteriq.data_feeds.base import (
    FeedSignal,
    Location,
    FeedCategory,
    SignalStrength,
    DataFeedAdapter,
    SignalCache,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Event classification helpers
# ============================================================================


@dataclass
class EventData:
    """Parsed event information from any source."""

    event_id: str
    name: str
    event_type: str  # e.g. "concert", "sports", "festival", "protest"
    venue_name: str
    location: Location
    start_time: datetime
    end_time: datetime
    estimated_attendance: int
    url: str = ""
    raw_data: dict[str, Any] = None

    def __post_init__(self):
        if self.raw_data is None:
            self.raw_data = {}

    def dedupe_key(self) -> str:
        """Generate a deduplication key for this event across sources."""
        # Key based on name, date, venue proximity
        parts = f"{self.name}:{self.start_time.date()}:{self.location.latitude:.4f}:{self.location.longitude:.4f}"
        return hashlib.md5(parts.encode()).hexdigest()

    def classify_strength(self) -> SignalStrength:
        """Classify event by expected attendance and type."""
        attendance = self.estimated_attendance
        event_type_lower = self.event_type.lower()

        # Negative events
        if any(neg in event_type_lower for neg in ["protest", "funeral", "roadwork", "closure"]):
            return SignalStrength.moderate_negative

        # Positive events by attendance
        if attendance > 5000:
            # Major festivals, concerts
            if any(major in event_type_lower for major in ["festival", "concert", "sporting"]):
                return SignalStrength.strong_positive
            if any(major in event_type_lower for major in ["music", "sports", "marathon"]):
                return SignalStrength.strong_positive
            return SignalStrength.moderate_positive

        if 500 <= attendance <= 5000:
            return SignalStrength.moderate_positive

        if 100 <= attendance < 500:
            return SignalStrength.weak_positive

        return SignalStrength.neutral


# ============================================================================
# Ticketmaster AU Adapter
# ============================================================================


class TicketmasterAdapter(DataFeedAdapter):
    """Fetches events from Ticketmaster AU Discovery API v2."""

    TICKETMASTER_URL = "https://app.ticketmaster.com/discovery/v2/events.json"
    DEFAULT_RADIUS_KM = 5.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        radius_km: float = DEFAULT_RADIUS_KM,
    ):
        super().__init__(api_key=api_key, cache=cache)
        self.radius_km = radius_km
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.events

    @property
    def source_name(self) -> str:
        return "ticketmaster_au"

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def is_available(self) -> bool:
        """Check if Ticketmaster API is reachable."""
        if not self.api_key:
            return False
        try:
            client = await self._get_http_client()
            params = {"apikey": self.api_key, "size": 1}
            response = await client.get(self.TICKETMASTER_URL, params=params)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Ticketmaster availability check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch events from Ticketmaster within the specified radius and date range.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch events for.
            end_date: Last date to fetch events for.
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects for events found.
        """
        if not self.api_key:
            logger.warning("Ticketmaster API key not configured")
            return []

        signals = []
        try:
            client = await self._get_http_client()

            # Query for events near location
            params = {
                "apikey": self.api_key,
                "latlong": f"{location.latitude},{location.longitude}",
                "radius": int(self.radius_km),
                "unit": "km",
                "size": 200,
                "sort": "date,asc",
            }

            response = await client.get(self.TICKETMASTER_URL, params=params)
            response.raise_for_status()

            data = response.json()
            events = data.get("_embedded", {}).get("events", [])

            for event in events:
                try:
                    event_data = self._parse_ticketmaster_event(event, location)
                    if event_data and self._is_in_date_range(event_data, start_date, end_date):
                        signal = self._event_to_signal(
                            event_data, start_date, end_date, venue_id
                        )
                        signals.extend(signal)
                except Exception as e:
                    logger.error(f"Error parsing Ticketmaster event: {e}", exc_info=True)

        except httpx.RequestError as e:
            logger.error(f"Ticketmaster API request failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching Ticketmaster events: {e}", exc_info=True)

        return signals

    def _parse_ticketmaster_event(self, event: dict, base_location: Location) -> Optional[EventData]:
        """Parse a Ticketmaster event JSON object."""
        try:
            event_id = event.get("id", "")
            name = event.get("name", "")
            if not event_id or not name:
                return None

            # Extract dates
            dates = event.get("dates", {})
            start_str = dates.get("start", {}).get("dateTime")
            if not start_str:
                return None

            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            # Extract location
            venue = event.get("_embedded", {}).get("venues", [{}])[0]
            venue_name = venue.get("name", "Unknown venue")
            venue_location_data = venue.get("location", {})

            try:
                event_location = Location(
                    latitude=float(venue_location_data.get("latitude", 0)),
                    longitude=float(venue_location_data.get("longitude", 0)),
                    address=venue.get("address", {}).get("line1", ""),
                    suburb=venue.get("city", {}).get("name", ""),
                )
            except (ValueError, TypeError):
                event_location = base_location

            # Classify event type
            classifications = event.get("classifications", [{}])
            event_type = classifications[0].get("genre", {}).get("name", "other").lower()

            # Estimate attendance (Ticketmaster doesn't always provide this)
            estimated_attendance = self._estimate_attendance_ticketmaster(event)

            end_time = start_time + timedelta(hours=3)  # Default 3-hour event

            return EventData(
                event_id=event_id,
                name=name,
                event_type=event_type,
                venue_name=venue_name,
                location=event_location,
                start_time=start_time,
                end_time=end_time,
                estimated_attendance=estimated_attendance,
                url=event.get("url", ""),
                raw_data=event,
            )
        except Exception as e:
            logger.debug(f"Failed to parse Ticketmaster event: {e}")
            return None

    def _estimate_attendance_ticketmaster(self, event: dict) -> int:
        """Estimate attendance for a Ticketmaster event."""
        # Check for capacity info
        priceranges = event.get("priceRanges", [])
        if priceranges:
            # High price -> likely big venue/event
            max_price = max(pr.get("max", 0) for pr in priceranges)
            if max_price > 200:
                return 5000  # Major event
            elif max_price > 100:
                return 2000
            elif max_price > 50:
                return 1000
            return 500

        # Check classification
        classifications = event.get("classifications", [{}])
        genre = classifications[0].get("genre", {}).get("name", "").lower()
        if "music" in genre or "festival" in genre:
            return 2000
        if "sports" in genre:
            return 3000

        return 500  # Default

    def _is_in_date_range(self, event_data: EventData, start_date: date, end_date: date) -> bool:
        """Check if event falls within the requested date range."""
        event_date = event_data.start_time.date()
        return start_date <= event_date <= end_date

    def _event_to_signal(
        self,
        event_data: EventData,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Convert an EventData to one or more FeedSignal objects."""
        signals = []
        strength = event_data.classify_strength()

        # Create hour-specific signals for pre-event and post-event impact
        event_date = event_data.start_time.date()
        start_hour = event_data.start_time.hour
        end_hour = event_data.end_time.hour

        # Pre-event dining (2 hours before)
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            pre_hour = (start_hour - 2) % 24
            if pre_hour >= 0:
                signal = self._make_signal(
                    signal_date=event_date,
                    strength=strength if strength.value.endswith("positive") else SignalStrength.weak_positive,
                    description=f"Pre-event dining before: {event_data.name}",
                    hour=pre_hour,
                    raw_data={
                        "event_name": event_data.name,
                        "venue": event_data.venue_name,
                        "attendance": event_data.estimated_attendance,
                        "type": "pre_event",
                    },
                    venue_id=venue_id,
                    ttl_minutes=120,
                )
                signals.append(signal)

        # Event-time impact
        signal = self._make_signal(
            signal_date=event_date,
            strength=strength,
            description=f"Event: {event_data.name} ({event_data.estimated_attendance} people)",
            hour=start_hour,
            raw_data={
                "event_name": event_data.name,
                "venue": event_data.venue_name,
                "attendance": event_data.estimated_attendance,
                "type": "event",
                "start_time": event_data.start_time.isoformat(),
                "end_time": event_data.end_time.isoformat(),
            },
            venue_id=venue_id,
            ttl_minutes=120,
        )
        signals.append(signal)

        # Post-event drinks/food (directly after event ends, +1 to +2 hours)
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            post_hour = (end_hour + 1) % 24
            signal = self._make_signal(
                signal_date=event_date,
                strength=SignalStrength.moderate_positive if strength == SignalStrength.strong_positive else SignalStrength.weak_positive,
                description=f"Post-event drinks after: {event_data.name}",
                hour=post_hour,
                raw_data={
                    "event_name": event_data.name,
                    "venue": event_data.venue_name,
                    "attendance": event_data.estimated_attendance,
                    "type": "post_event",
                },
                venue_id=venue_id,
                ttl_minutes=120,
            )
            signals.append(signal)

        return signals


# ============================================================================
# Eventbrite Adapter
# ============================================================================


class EventbriteAdapter(DataFeedAdapter):
    """Fetches events from Eventbrite REST API."""

    EVENTBRITE_URL = "https://www.eventbriteapi.com/v3/events/search/"
    DEFAULT_RADIUS_KM = 5.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        radius_km: float = DEFAULT_RADIUS_KM,
    ):
        super().__init__(api_key=api_key, cache=cache)
        self.radius_km = radius_km
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.events

    @property
    def source_name(self) -> str:
        return "eventbrite"

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            self._http_client = httpx.AsyncClient(headers=headers, timeout=30.0)
        return self._http_client

    async def is_available(self) -> bool:
        """Check if Eventbrite API is reachable."""
        if not self.api_key:
            return False
        try:
            client = await self._get_http_client()
            response = await client.get(self.EVENTBRITE_URL, params={"max": 1})
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Eventbrite availability check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch events from Eventbrite within the specified radius and date range.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch events for.
            end_date: Last date to fetch events for.
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects for events found.
        """
        if not self.api_key:
            logger.warning("Eventbrite API key not configured")
            return []

        signals = []
        try:
            client = await self._get_http_client()

            # Query for events near location
            params = {
                "location.latitude": location.latitude,
                "location.longitude": location.longitude,
                "location.within": f"{int(self.radius_km)}km",
                "start_date.range_start": start_date.isoformat() + "T00:00:00Z",
                "start_date.range_end": end_date.isoformat() + "T23:59:59Z",
                "max": 100,
                "status": "live",
            }

            response = await client.get(self.EVENTBRITE_URL, params=params)
            response.raise_for_status()

            data = response.json()
            events = data.get("events", [])

            for event in events:
                try:
                    event_data = self._parse_eventbrite_event(event, location)
                    if event_data:
                        signal = self._event_to_signal(
                            event_data, start_date, end_date, venue_id
                        )
                        signals.extend(signal)
                except Exception as e:
                    logger.error(f"Error parsing Eventbrite event: {e}", exc_info=True)

        except httpx.RequestError as e:
            logger.error(f"Eventbrite API request failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching Eventbrite events: {e}", exc_info=True)

        return signals

    def _parse_eventbrite_event(self, event: dict, base_location: Location) -> Optional[EventData]:
        """Parse an Eventbrite event JSON object."""
        try:
            event_id = event.get("id", "")
            name = event.get("name", {}).get("text", "")
            if not event_id or not name:
                return None

            # Extract dates
            start_str = event.get("start", {}).get("utc")
            if not start_str:
                return None

            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            end_str = event.get("end", {}).get("utc")
            end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else start_time + timedelta(hours=2)

            # Extract location
            venue_info = event.get("venue", {})
            venue_name = venue_info.get("name", "Unknown venue")
            venue_location_data = venue_info.get("address", {})

            try:
                event_location = Location(
                    latitude=float(venue_location_data.get("latitude", base_location.latitude)),
                    longitude=float(venue_location_data.get("longitude", base_location.longitude)),
                    address=venue_location_data.get("address_1", ""),
                    suburb=venue_location_data.get("city", ""),
                )
            except (ValueError, TypeError):
                event_location = base_location

            # Classify event type
            category = event.get("category", {}).get("name", "other").lower()

            # Get estimated capacity
            capacity = event.get("capacity", 0)
            estimated_attendance = min(capacity, 500) if capacity > 0 else 300

            return EventData(
                event_id=event_id,
                name=name,
                event_type=category,
                venue_name=venue_name,
                location=event_location,
                start_time=start_time,
                end_time=end_time,
                estimated_attendance=estimated_attendance,
                url=event.get("url", ""),
                raw_data=event,
            )
        except Exception as e:
            logger.debug(f"Failed to parse Eventbrite event: {e}")
            return None

    def _event_to_signal(
        self,
        event_data: EventData,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Convert an EventData to one or more FeedSignal objects."""
        signals = []
        strength = event_data.classify_strength()

        event_date = event_data.start_time.date()
        start_hour = event_data.start_time.hour
        end_hour = event_data.end_time.hour

        # Pre-event dining
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            pre_hour = (start_hour - 2) % 24
            signal = self._make_signal(
                signal_date=event_date,
                strength=SignalStrength.weak_positive,
                description=f"Pre-event dining before: {event_data.name}",
                hour=pre_hour,
                raw_data={
                    "event_name": event_data.name,
                    "venue": event_data.venue_name,
                    "attendance": event_data.estimated_attendance,
                    "type": "pre_event",
                },
                venue_id=venue_id,
                ttl_minutes=120,
            )
            signals.append(signal)

        # Event-time impact
        signal = self._make_signal(
            signal_date=event_date,
            strength=strength,
            description=f"Event: {event_data.name} ({event_data.estimated_attendance} attendees)",
            hour=start_hour,
            raw_data={
                "event_name": event_data.name,
                "venue": event_data.venue_name,
                "attendance": event_data.estimated_attendance,
                "type": "event",
                "start_time": event_data.start_time.isoformat(),
                "end_time": event_data.end_time.isoformat(),
            },
            venue_id=venue_id,
            ttl_minutes=120,
        )
        signals.append(signal)

        # Post-event
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            post_hour = (end_hour + 1) % 24
            signal = self._make_signal(
                signal_date=event_date,
                strength=SignalStrength.weak_positive,
                description=f"Post-event drinks after: {event_data.name}",
                hour=post_hour,
                raw_data={
                    "event_name": event_data.name,
                    "venue": event_data.venue_name,
                    "type": "post_event",
                },
                venue_id=venue_id,
                ttl_minutes=120,
            )
            signals.append(signal)

        return signals


# ============================================================================
# PredictHQ Adapter
# ============================================================================


class PredictHQAdapter(DataFeedAdapter):
    """Fetches events from PredictHQ event intelligence API."""

    PREDICTHQ_URL = "https://api.predicthq.com/v1/events/"
    DEFAULT_RADIUS_KM = 5.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        radius_km: float = DEFAULT_RADIUS_KM,
    ):
        super().__init__(api_key=api_key, cache=cache)
        self.radius_km = radius_km
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.events

    @property
    def source_name(self) -> str:
        return "predicthq"

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            self._http_client = httpx.AsyncClient(headers=headers, timeout=30.0)
        return self._http_client

    async def is_available(self) -> bool:
        """Check if PredictHQ API is reachable."""
        if not self.api_key:
            return False
        try:
            client = await self._get_http_client()
            params = {"limit": 1}
            response = await client.get(self.PREDICTHQ_URL, params=params)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"PredictHQ availability check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch events from PredictHQ within the specified radius and date range.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch events for.
            end_date: Last date to fetch events for.
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects for events found.
        """
        if not self.api_key:
            logger.warning("PredictHQ API key not configured")
            return []

        signals = []
        try:
            client = await self._get_http_client()

            # Query for events near location
            params = {
                "within": f"{self.radius_km}km@{location.latitude},{location.longitude}",
                "active.gte": start_date.isoformat(),
                "active.lte": end_date.isoformat(),
                "limit": 100,
            }

            response = await client.get(self.PREDICTHQ_URL, params=params)
            response.raise_for_status()

            data = response.json()
            events = data.get("results", [])

            for event in events:
                try:
                    event_data = self._parse_predicthq_event(event, location)
                    if event_data:
                        signal = self._event_to_signal(
                            event_data, start_date, end_date, venue_id
                        )
                        signals.extend(signal)
                except Exception as e:
                    logger.error(f"Error parsing PredictHQ event: {e}", exc_info=True)

        except httpx.RequestError as e:
            logger.error(f"PredictHQ API request failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching PredictHQ events: {e}", exc_info=True)

        return signals

    def _parse_predicthq_event(self, event: dict, base_location: Location) -> Optional[EventData]:
        """Parse a PredictHQ event JSON object."""
        try:
            event_id = event.get("id", "")
            name = event.get("title", "")
            if not event_id or not name:
                return None

            # Extract dates
            start_str = event.get("start")
            if not start_str:
                return None

            start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            end_str = event.get("end")
            end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else start_time + timedelta(hours=3)

            # Extract location
            location_data = event.get("location", [])
            venue_name = event.get("venue", {}).get("name", "Unknown venue")

            try:
                event_location = Location(
                    latitude=location_data[0] if location_data else base_location.latitude,
                    longitude=location_data[1] if len(location_data) > 1 else base_location.longitude,
                )
            except (ValueError, TypeError, IndexError):
                event_location = base_location

            # Use PredictHQ's impact score to estimate attendance
            impact = event.get("impact", {})
            rank = impact.get("rank", 100)  # 1-100, lower is bigger

            # Map rank to estimated attendance
            if rank <= 20:
                estimated_attendance = 5000
            elif rank <= 50:
                estimated_attendance = 2000
            elif rank <= 75:
                estimated_attendance = 500
            else:
                estimated_attendance = 200

            event_type = ", ".join(event.get("category", []))

            return EventData(
                event_id=event_id,
                name=name,
                event_type=event_type,
                venue_name=venue_name,
                location=event_location,
                start_time=start_time,
                end_time=end_time,
                estimated_attendance=estimated_attendance,
                url=event.get("links", [{}])[0].get("href", ""),
                raw_data=event,
            )
        except Exception as e:
            logger.debug(f"Failed to parse PredictHQ event: {e}")
            return None

    def _event_to_signal(
        self,
        event_data: EventData,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """Convert an EventData to one or more FeedSignal objects."""
        signals = []
        strength = event_data.classify_strength()

        event_date = event_data.start_time.date()
        start_hour = event_data.start_time.hour
        end_hour = event_data.end_time.hour

        # Pre-event impact
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            pre_hour = (start_hour - 2) % 24
            signal = self._make_signal(
                signal_date=event_date,
                strength=SignalStrength.weak_positive,
                description=f"Pre-event demand before: {event_data.name}",
                hour=pre_hour,
                raw_data={
                    "event_name": event_data.name,
                    "venue": event_data.venue_name,
                    "attendance": event_data.estimated_attendance,
                    "type": "pre_event",
                },
                venue_id=venue_id,
                ttl_minutes=120,
            )
            signals.append(signal)

        # Event-time impact
        signal = self._make_signal(
            signal_date=event_date,
            strength=strength,
            description=f"Event: {event_data.name} (rank-based attendance ~{event_data.estimated_attendance})",
            hour=start_hour,
            raw_data={
                "event_name": event_data.name,
                "venue": event_data.venue_name,
                "attendance": event_data.estimated_attendance,
                "type": "event",
                "start_time": event_data.start_time.isoformat(),
                "end_time": event_data.end_time.isoformat(),
            },
            venue_id=venue_id,
            ttl_minutes=120,
        )
        signals.append(signal)

        # Post-event impact
        if strength != SignalStrength.neutral and strength != SignalStrength.moderate_negative:
            post_hour = (end_hour + 1) % 24
            signal = self._make_signal(
                signal_date=event_date,
                strength=SignalStrength.weak_positive,
                description=f"Post-event impact after: {event_data.name}",
                hour=post_hour,
                raw_data={
                    "event_name": event_data.name,
                    "venue": event_data.venue_name,
                    "type": "post_event",
                },
                venue_id=venue_id,
                ttl_minutes=120,
            )
            signals.append(signal)

        return signals


# ============================================================================
# Events Aggregator
# ============================================================================


class EventsAggregator(DataFeedAdapter):
    """
    Composite adapter that aggregates events from all configured sources.

    Deduplicates events across sources based on name, date, and location proximity.
    """

    def __init__(
        self,
        cache: Optional[SignalCache] = None,
        radius_km: float = 5.0,
        ticketmaster_key: Optional[str] = None,
        eventbrite_key: Optional[str] = None,
        predicthq_key: Optional[str] = None,
    ):
        super().__init__(cache=cache)
        self.radius_km = radius_km
        self._adapters: list[DataFeedAdapter] = []
        self._seen_events: set[str] = set()

        # Initialize adapters
        if ticketmaster_key:
            self._adapters.append(
                TicketmasterAdapter(api_key=ticketmaster_key, cache=cache, radius_km=radius_km)
            )
        if eventbrite_key:
            self._adapters.append(
                EventbriteAdapter(api_key=eventbrite_key, cache=cache, radius_km=radius_km)
            )
        if predicthq_key:
            self._adapters.append(
                PredictHQAdapter(api_key=predicthq_key, cache=cache, radius_km=radius_km)
            )

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.events

    @property
    def source_name(self) -> str:
        return "events_aggregator"

    def add_adapter(self, adapter: DataFeedAdapter) -> None:
        """Add a custom event adapter."""
        self._adapters.append(adapter)

    async def is_available(self) -> bool:
        """Check if at least one event source is available."""
        if not self._adapters:
            return False
        for adapter in self._adapters:
            try:
                if await adapter.is_available():
                    return True
            except Exception as e:
                logger.debug(f"Adapter {adapter.source_name} check failed: {e}")
        return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch and aggregate events from all configured sources.

        Deduplicates events across sources based on name, date, and location.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch events for.
            end_date: Last date to fetch events for.
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of deduplicated FeedSignal objects from all sources.
        """
        if not self._adapters:
            logger.warning("No event adapters configured")
            return []

        all_signals = []
        seen_dedupes = set()

        # Fetch from all adapters in parallel
        try:
            results = await asyncio.gather(
                *[
                    adapter.fetch_signals(location, start_date, end_date, venue_id)
                    for adapter in self._adapters
                ],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Adapter fetch failed: {result}")
                    continue

                for signal in result:
                    # Deduplicate based on event name and date+hour
                    dedupe_key = f"{signal.raw_data.get('event_name')}:{signal.signal_date}:{signal.signal_hour}"
                    if dedupe_key not in seen_dedupes:
                        all_signals.append(signal)
                        seen_dedupes.add(dedupe_key)
                    else:
                        logger.debug(f"Skipped duplicate event: {dedupe_key}")

        except Exception as e:
            logger.error(f"Error aggregating event signals: {e}", exc_info=True)

        return all_signals
