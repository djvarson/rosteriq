"""
Function Tracker event/function management data feed adapter for RosterIQ.

Integrates with Function Tracker (functiontracker.com), a cloud-based
venue and event management system used by Australian hospitality venues
including function centres, hotels, clubs, restaurants, and conference venues.

Translates upcoming function/event bookings into demand signals weighted
by guest count, event type, and timing. This is distinct from table-level
reservations — Function Tracker events are large-format (weddings, conferences,
corporate dinners, etc.) that significantly impact staffing requirements.

API: https://api.functiontracker.com/v1/
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
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
# Data classes
# ============================================================================


class EventType(str, Enum):
    """Categories of function/event bookings."""

    wedding = "wedding"
    corporate = "corporate"
    conference = "conference"
    birthday = "birthday"
    cocktail = "cocktail"
    sit_down_dinner = "sit_down_dinner"
    buffet = "buffet"
    private_function = "private_function"
    christmas_party = "christmas_party"
    fundraiser = "fundraiser"
    other = "other"


# Staff-per-guest ratios by event type (approximate)
EVENT_STAFF_RATIOS: dict[str, float] = {
    "wedding": 0.08,          # 1 staff per 12 guests
    "corporate": 0.06,        # 1 staff per 16 guests
    "conference": 0.04,       # 1 staff per 25 guests
    "birthday": 0.07,         # 1 staff per 14 guests
    "cocktail": 0.10,         # 1 staff per 10 guests (more service-heavy)
    "sit_down_dinner": 0.08,  # 1 staff per 12 guests
    "buffet": 0.05,           # 1 staff per 20 guests
    "private_function": 0.07, # 1 staff per 14 guests
    "christmas_party": 0.08,  # 1 staff per 12 guests
    "fundraiser": 0.06,       # 1 staff per 16 guests
    "other": 0.06,            # Default
}


@dataclass
class FunctionSnapshot:
    """
    Point-in-time snapshot of function/event data for a specific date.

    Captures event metrics used to generate demand signals. Unlike
    reservation snapshots (many small bookings), function snapshots
    typically represent fewer, larger events.
    """

    date: date
    total_events: int = 0
    total_guests: int = 0
    event_types: list[str] = None
    largest_event_guests: int = 0
    estimated_extra_staff: int = 0
    events: list[dict] = None

    def __post_init__(self) -> None:
        if self.event_types is None:
            self.event_types = []
        if self.events is None:
            self.events = []
        # Calculate estimated extra staff needed
        self.estimated_extra_staff = 0
        for event in self.events:
            event_type = event.get("event_type", "other")
            guests = event.get("guests", 0)
            ratio = EVENT_STAFF_RATIOS.get(event_type, 0.06)
            self.estimated_extra_staff += max(1, round(guests * ratio))


# ============================================================================
# Rate limiter
# ============================================================================


class _RateLimiter:
    """Simple token-bucket rate limiter for Function Tracker API calls."""

    def __init__(self, max_requests: int = 30, window_seconds: float = 60.0):
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            self._timestamps = [t for t in self._timestamps if t > cutoff]

            if len(self._timestamps) >= self._max_requests:
                sleep_time = self._timestamps[0] - cutoff
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self._timestamps = [
                    t for t in self._timestamps
                    if t > time.monotonic() - self._window_seconds
                ]

            self._timestamps.append(time.monotonic())


# ============================================================================
# Function Tracker Adapter
# ============================================================================


class FunctionTrackerAdapter(DataFeedAdapter):
    """
    Adapter for Function Tracker cloud-based event management system.

    Pulls upcoming events/functions and converts them into demand signals
    that the roster engine uses to adjust staffing levels. A 200-guest
    wedding generates a much stronger demand signal than a 20-person
    birthday — the adapter accounts for event type, guest count, and
    timing to produce calibrated signals.

    Key differences from reservation adapters:
    - Fewer events but much larger guest counts
    - Event type matters (wedding vs conference vs cocktail)
    - Staff-per-guest ratios vary by event type
    - Events often require specific roles (kitchen, bar, floor)
    - Longer lead times = higher confidence signals
    """

    category = FeedCategory.events
    base_url = "https://api.functiontracker.com/v1"
    source_name = "function_tracker"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        super().__init__(
            api_key=api_key,
            cache=cache or SignalCache(default_ttl_minutes=30),
        )
        self._http_client = http_client
        self._owns_client = http_client is None
        self._rate_limiter = _RateLimiter(max_requests=30, window_seconds=60.0)

    async def __aenter__(self) -> FunctionTrackerAdapter:
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            self._owns_client = True
        return self._http_client

    async def _rate_limited_get(self, url: str, **kwargs: Any) -> httpx.Response:
        await self._rate_limiter.acquire()
        return await self.http_client.get(url, **kwargs)

    # ── Core interface ─────────────────────────────────────────────

    async def is_available(self) -> bool:
        """Check if Function Tracker API is reachable and credentials are valid."""
        try:
            resp = await self._rate_limited_get(
                f"{self.base_url}/venues",
                headers=self._auth_headers(),
            )
            return resp.status_code in (200, 401)  # 401 = reachable but bad key
        except Exception as e:
            logger.warning(f"Function Tracker availability check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch function/event signals from Function Tracker.

        Pulls all events in the date range, groups by date, and produces
        demand signals calibrated by guest count and event type.
        """
        if not venue_id:
            logger.warning("Function Tracker requires venue_id; skipping fetch")
            return []

        signals = []

        try:
            events = await self._fetch_events_range(venue_id, start_date, end_date)

            # Group events by date
            events_by_date: dict[date, list[dict]] = defaultdict(list)
            for event in events:
                event_date = self._parse_event_date(event)
                if event_date:
                    events_by_date[event_date].append(event)

            # Generate signals per date
            for event_date in sorted(events_by_date.keys()):
                day_events = events_by_date[event_date]
                snapshot = self._build_snapshot(event_date, day_events)
                strength = self._calculate_strength(snapshot)
                confidence = self._calculate_confidence(event_date)

                # Build description
                type_summary = ", ".join(set(snapshot.event_types)) if snapshot.event_types else "events"
                desc = (
                    f"Function Tracker: {snapshot.total_events} event(s), "
                    f"{snapshot.total_guests} guests ({type_summary}). "
                    f"Est. +{snapshot.estimated_extra_staff} extra staff needed."
                )

                signal = self._make_signal(
                    signal_date=event_date,
                    strength=strength,
                    description=desc,
                    confidence=confidence,
                    hour=18,  # Functions typically peak at evening service
                    raw_data={
                        "snapshot": {
                            "total_events": snapshot.total_events,
                            "total_guests": snapshot.total_guests,
                            "event_types": snapshot.event_types,
                            "largest_event_guests": snapshot.largest_event_guests,
                            "estimated_extra_staff": snapshot.estimated_extra_staff,
                            "events": snapshot.events,
                        }
                    },
                    venue_id=venue_id,
                    ttl_minutes=30,
                )
                signals.append(signal)

        except Exception as e:
            logger.error(f"Function Tracker fetch failed for venue {venue_id}: {e}")

        return signals

    # ── API interaction ────────────────────────────────────────────

    def _auth_headers(self) -> dict[str, str]:
        """Build authentication headers for Function Tracker API."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _fetch_events_range(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch events from Function Tracker for a date range.

        Returns:
            List of event dictionaries with keys:
            - id, name, event_type, status
            - start_datetime, end_datetime
            - guests (expected guest count)
            - rooms (list of room names)
            - catering (bool)
            - notes
        """
        url = f"{self.base_url}/venues/{venue_id}/events"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "status": "confirmed,tentative",
        }

        try:
            resp = await self._rate_limited_get(
                url,
                headers=self._auth_headers(),
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

            # Function Tracker may return events in a wrapper
            if isinstance(data, dict):
                return data.get("events", data.get("data", []))
            return data if isinstance(data, list) else []

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error(f"Function Tracker auth failed for venue {venue_id}")
                raise ValueError("Function Tracker API key invalid or expired")
            logger.error(f"Function Tracker HTTP error: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Function Tracker API error for venue {venue_id}: {e}")
            raise

    async def fetch_event_detail(
        self,
        venue_id: str,
        event_id: str,
    ) -> dict[str, Any]:
        """
        Fetch detailed information for a single event.

        Returns full event data including catering, beverage, equipment,
        running sheet, and staff requirements.
        """
        url = f"{self.base_url}/venues/{venue_id}/events/{event_id}"

        try:
            resp = await self._rate_limited_get(
                url,
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Function Tracker event detail fetch failed: {e}")
            raise

    async def fetch_upcoming_events(
        self,
        venue_id: str,
        days: int = 14,
    ) -> list[dict[str, Any]]:
        """
        Convenience method: fetch events for the next N days.

        Used by the demand intelligence panel to show upcoming functions.
        """
        today = date.today()
        end_date = today + timedelta(days=days)
        return await self._fetch_events_range(venue_id, today, end_date)

    # ── Signal processing ──────────────────────────────────────────

    def _build_snapshot(
        self,
        event_date: date,
        events: list[dict],
    ) -> FunctionSnapshot:
        """Build a FunctionSnapshot from raw event data."""
        total_guests = 0
        largest = 0
        event_types = []
        processed_events = []

        for event in events:
            guests = (
                event.get("guests")
                or event.get("attendees")
                or event.get("pax")
                or event.get("expected_guests")
                or 0
            )
            if isinstance(guests, str):
                try:
                    guests = int(guests)
                except ValueError:
                    guests = 0

            event_type = (
                event.get("event_type")
                or event.get("type")
                or event.get("category")
                or "other"
            ).lower().replace(" ", "_")

            # Normalise event type
            if event_type not in EVENT_STAFF_RATIOS:
                event_type = "other"

            total_guests += guests
            if guests > largest:
                largest = guests
            event_types.append(event_type)

            processed_events.append({
                "id": event.get("id", ""),
                "name": event.get("name", event.get("title", "Unnamed Event")),
                "event_type": event_type,
                "guests": guests,
                "status": event.get("status", "confirmed"),
                "start_time": event.get("start_datetime", event.get("start_time", "")),
                "end_time": event.get("end_datetime", event.get("end_time", "")),
                "rooms": event.get("rooms", []),
                "catering": event.get("catering", False),
            })

        return FunctionSnapshot(
            date=event_date,
            total_events=len(events),
            total_guests=total_guests,
            event_types=event_types,
            largest_event_guests=largest,
            events=processed_events,
        )

    def _calculate_strength(self, snapshot: FunctionSnapshot) -> SignalStrength:
        """
        Map a function snapshot to a signal strength.

        Based on total guest count across all events on that date:
        - 0-20 guests: weak positive (small private event)
        - 21-50: moderate positive (medium function)
        - 51-100: strong positive (large function)
        - 101+: extreme positive (major event, e.g. wedding, conference)
        """
        guests = snapshot.total_guests

        if guests <= 0:
            return SignalStrength.neutral
        elif guests <= 20:
            return SignalStrength.weak_positive
        elif guests <= 50:
            return SignalStrength.moderate_positive
        elif guests <= 100:
            return SignalStrength.strong_positive
        else:
            return SignalStrength.extreme_positive

    def _calculate_confidence(self, event_date: date) -> float:
        """
        Calculate confidence based on how far in advance the event is.

        Functions booked well in advance are high-confidence signals.
        - Same day: 0.95 (it's definitely happening)
        - 1-3 days: 0.90
        - 4-7 days: 0.85
        - 8-14 days: 0.80
        - 15-30 days: 0.70
        - 30+ days: 0.60 (may still change)
        """
        days_ahead = (event_date - date.today()).days

        if days_ahead <= 0:
            return 0.95
        elif days_ahead <= 3:
            return 0.90
        elif days_ahead <= 7:
            return 0.85
        elif days_ahead <= 14:
            return 0.80
        elif days_ahead <= 30:
            return 0.70
        else:
            return 0.60

    # ── Helpers ────────────────────────────────────────────────────

    def _parse_event_date(self, event: dict) -> Optional[date]:
        """Extract the date from an event dict, trying multiple field names."""
        for field in ("start_datetime", "start_date", "date", "event_date", "start_time"):
            val = event.get(field)
            if not val:
                continue
            if isinstance(val, date):
                return val
            if isinstance(val, datetime):
                return val.date()
            if isinstance(val, str):
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                            "%d/%m/%Y", "%d-%m-%Y"):
                    try:
                        return datetime.strptime(val[:19], fmt).date()
                    except ValueError:
                        continue
        return None

    def _make_signal(
        self,
        signal_date: date,
        strength: SignalStrength,
        description: str,
        confidence: float,
        hour: int,
        raw_data: dict,
        venue_id: str,
        ttl_minutes: int = 30,
    ) -> FeedSignal:
        """Create a FeedSignal with Function Tracker metadata."""
        return FeedSignal(
            source=self.source_name,
            category=self.category,
            signal_date=signal_date,
            signal_hour=hour,
            strength=strength,
            confidence=confidence,
            description=description,
            value=raw_data.get("snapshot", {}).get("total_guests", 0),
            raw_data=raw_data,
        )
