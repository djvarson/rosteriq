"""
Reservations/Bookings data feed adapter for RosterIQ.

Integrates with major Australian reservation platforms (ResDiary, Now Book It,
OpenTable, SevenRooms, BookitLive) and generic direct booking systems. Translates booking
patterns into demand signals weighted by party size, no-show history, and
booking-to-forecast ratios.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
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
# Data classes
# ============================================================================


@dataclass
class BookingSnapshot:
    """
    Point-in-time snapshot of reservation data for a specific date/hour.

    Captures raw booking metrics used to generate demand signals.
    """

    date: date
    hour: Optional[int] = None  # None = all-day aggregate
    total_bookings: int = 0
    total_covers: int = 0
    avg_party_size: float = 0.0
    large_parties: int = 0  # Count of parties with 10+ pax
    no_show_adjusted_covers: float = 0.0  # Covers adjusted for expected no-shows

    def __post_init__(self) -> None:
        """Calculate derived metrics on instantiation."""
        if self.total_bookings > 0:
            self.avg_party_size = self.total_covers / self.total_bookings


# ============================================================================
# Base Reservations Adapter
# ============================================================================


class ReservationsAdapter(DataFeedAdapter):
    """
    Abstract base class for reservation platform adapters.

    Provides common logic for:
    - Confidence scoring based on booking lead time
    - No-show rate adjustment (default 15%)
    - Booking-to-forecast ratio calculation
    - Signal strength mapping from occupancy metrics
    """

    category = FeedCategory.reservations

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        no_show_rate: float = 0.15,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize the reservations adapter.

        Args:
            api_key: API credentials for the platform.
            cache: Signal cache instance (TTL=10 minutes by default for reservations).
            no_show_rate: Expected no-show rate as decimal (0.15 = 15%).
            http_client: Reusable httpx AsyncClient for pooled connections.
        """
        super().__init__(api_key=api_key, cache=cache or SignalCache(default_ttl_minutes=10))
        self.no_show_rate = no_show_rate
        self._http_client = http_client
        self._owns_client = http_client is None

    async def __aenter__(self) -> ReservationsAdapter:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit: clean up client if we created it."""
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize or return existing httpx client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            self._owns_client = True
        return self._http_client

    def _calculate_confidence(self, target_date: date) -> float:
        """
        Calculate confidence score based on booking lead time.

        Bookings further in the future are less reliable:
        - Today: 0.95 (confirmed)
        - Tomorrow: 0.85 (very likely)
        - This week: 0.7 (likely)
        - Next week: 0.5 (more volatility)

        Args:
            target_date: The date the signal applies to.

        Returns:
            Confidence score (0.0 to 1.0).
        """
        today = date.today()
        days_ahead = (target_date - today).days

        if days_ahead == 0:
            return 0.95
        elif days_ahead == 1:
            return 0.85
        elif days_ahead <= 6:
            return 0.7
        else:
            return 0.5

    def _calculate_signal_strength(
        self,
        snapshot: BookingSnapshot,
        historical_average: float,
    ) -> SignalStrength:
        """
        Map booking metrics to signal strength.

        Ratio = (adjusted_covers) / (historical_average)
        - > 1.5: strong_positive
        - 1.15 to 1.5: moderate_positive
        - 0.85 to 1.15: neutral
        - 0.65 to 0.85: moderate_negative
        - < 0.65: strong_negative

        Large parties (10+ pax) add weight to positive signals.

        Args:
            snapshot: Booking snapshot with covers and party data.
            historical_average: Expected covers for this date/hour historically.

        Returns:
            SignalStrength enum value.
        """
        if historical_average == 0:
            # No historical baseline; use cover count as proxy
            if snapshot.no_show_adjusted_covers >= 50:
                return SignalStrength.strong_positive
            elif snapshot.no_show_adjusted_covers >= 30:
                return SignalStrength.moderate_positive
            elif snapshot.no_show_adjusted_covers >= 10:
                return SignalStrength.neutral
            else:
                return SignalStrength.moderate_negative

        ratio = snapshot.no_show_adjusted_covers / historical_average

        # Boost positive signals if many large parties present
        large_party_boost = min(0.15, snapshot.large_parties * 0.02)

        if ratio >= (1.5 - large_party_boost):
            return SignalStrength.strong_positive
        elif ratio >= 1.15:
            return SignalStrength.moderate_positive
        elif ratio <= 0.65:
            return SignalStrength.strong_negative
        elif ratio <= 0.85:
            return SignalStrength.moderate_negative
        else:
            return SignalStrength.neutral

    def _adjust_for_no_shows(self, total_covers: int) -> float:
        """
        Adjust expected covers down by the no-show rate.

        Args:
            total_covers: Raw booked covers.

        Returns:
            Adjusted cover count (float).
        """
        return total_covers * (1.0 - self.no_show_rate)


# ============================================================================
# ResDiary Adapter (AU/NZ market leader)
# ============================================================================


class ResDiaryAdapter(ReservationsAdapter):
    """
    Adapter for ResDiary, the AU/NZ market leader in reservation management.

    API: https://api.rfresdiary.com/api/v2/
    """

    base_url = "https://api.rfresdiary.com/api/v2"
    source_name = "resdiary"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch reservation signals from ResDiary.

        ResDiary requires a venue ID. Retrieves bookings for each day in the
        range and creates hourly signals.

        Args:
            location: Venue location (used for logging only).
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: ResDiary venue ID (required).

        Returns:
            List of FeedSignal objects with hourly breakdowns.
        """
        if not venue_id:
            logger.warning("ResDiary requires venue_id; skipping fetch")
            return []

        signals = []
        current_date = start_date

        while current_date <= end_date:
            # Check cache first
            cache_key = f"resdiary:{venue_id}:{current_date}"
            cached = self.cache.get(cache_key)
            if cached:
                signals.append(cached)
                current_date += timedelta(days=1)
                continue

            try:
                # Fetch bookings for this date
                bookings = await self._fetch_bookings_for_date(venue_id, current_date)
                snapshot = self._process_bookings(bookings, current_date)

                # Create signal (assuming 8pm average booking time for restaurants)
                signal = self._make_signal(
                    signal_date=current_date,
                    strength=self._calculate_signal_strength(snapshot, 50.0),
                    description=(
                        f"ResDiary: {snapshot.total_bookings} bookings, "
                        f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                    ),
                    confidence=self._calculate_confidence(current_date),
                    hour=20,  # Evening service
                    raw_data={"snapshot": vars(snapshot)},
                    venue_id=venue_id,
                    ttl_minutes=10,
                )
                signals.append(signal)

            except Exception as e:
                logger.error(f"ResDiary fetch failed for {venue_id} on {current_date}: {e}")

            current_date += timedelta(days=1)

        return signals

    async def _fetch_bookings_for_date(
        self,
        venue_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch bookings from ResDiary for a single date.

        Args:
            venue_id: ResDiary venue ID.
            target_date: Date to fetch bookings for.

        Returns:
            List of booking dictionaries from API.
        """
        url = f"{self.base_url}/venue/{venue_id}/bookings"
        params = {
            "date": target_date.isoformat(),
            "api_token": self.api_key,
        }

        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("bookings", [])
        except httpx.HTTPError as e:
            logger.error(f"ResDiary API error: {e}")
            return []

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
    ) -> BookingSnapshot:
        """
        Process raw ResDiary booking data into a BookingSnapshot.

        Args:
            bookings: List of booking dictionaries.
            booking_date: Date of bookings.

        Returns:
            BookingSnapshot with aggregated metrics.
        """
        snapshot = BookingSnapshot(date=booking_date)

        for booking in bookings:
            party_size = booking.get("guests", 1)
            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    async def is_available(self) -> bool:
        """Check if ResDiary API is reachable."""
        if not self.api_key:
            return False

        try:
            response = await self.http_client.get(
                f"{self.base_url}/health",
                params={"api_token": self.api_key},
                timeout=5.0,
            )
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"ResDiary health check failed: {e}")
            return False


# ============================================================================
# Now Book It Adapter
# ============================================================================


class NowBookItAdapter(ReservationsAdapter):
    """
    Adapter for Now Book It, a popular Australian booking platform.

    API: https://api.nowbookit.com/api/v1/
    """

    base_url = "https://api.nowbookit.com/api/v1"
    source_name = "nowbookit"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch reservation signals from Now Book It.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Now Book It venue/account ID.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("Now Book It requires venue_id; skipping fetch")
            return []

        signals = []

        try:
            bookings = await self._fetch_bookings_range(venue_id, start_date, end_date)

            # Group bookings by date
            bookings_by_date: dict[date, list[dict]] = {}
            for booking in bookings:
                booking_date = self._parse_date(booking.get("reservation_date", ""))
                if booking_date:
                    bookings_by_date.setdefault(booking_date, []).append(booking)

            # Create signals per date
            for booking_date in sorted(bookings_by_date.keys()):
                snapshot = self._process_bookings(bookings_by_date[booking_date], booking_date)

                signal = self._make_signal(
                    signal_date=booking_date,
                    strength=self._calculate_signal_strength(snapshot, 40.0),
                    description=(
                        f"Now Book It: {snapshot.total_bookings} bookings, "
                        f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                    ),
                    confidence=self._calculate_confidence(booking_date),
                    hour=19,  # Early evening
                    raw_data={"snapshot": vars(snapshot)},
                    venue_id=venue_id,
                    ttl_minutes=10,
                )
                signals.append(signal)

        except Exception as e:
            logger.error(f"Now Book It fetch failed for {venue_id}: {e}")

        return signals

    async def _fetch_bookings_range(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch bookings from Now Book It for a date range.

        Args:
            venue_id: Venue ID.
            start_date: Start date.
            end_date: End date.

        Returns:
            List of booking dictionaries.
        """
        url = f"{self.base_url}/bookings"
        params = {
            "venue_id": venue_id,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "api_key": self.api_key,
        }

        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            return response.json().get("bookings", [])
        except httpx.HTTPError as e:
            logger.error(f"Now Book It API error: {e}")
            return []

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
    ) -> BookingSnapshot:
        """
        Process Now Book It booking data.

        Args:
            bookings: List of booking dictionaries.
            booking_date: Date of bookings.

        Returns:
            BookingSnapshot.
        """
        snapshot = BookingSnapshot(date=booking_date)

        for booking in bookings:
            party_size = booking.get("party_size", 1)
            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    @staticmethod
    def _parse_date(date_str: str) -> Optional[date]:
        """Parse ISO date string to date object."""
        try:
            if "T" in date_str:
                return datetime.fromisoformat(date_str).date()
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, AttributeError):
            return None

    async def is_available(self) -> bool:
        """Check if Now Book It API is reachable."""
        if not self.api_key:
            return False

        try:
            response = await self.http_client.get(
                f"{self.base_url}/health",
                params={"api_key": self.api_key},
                timeout=5.0,
            )
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Now Book It health check failed: {e}")
            return False


# ============================================================================
# OpenTable Adapter
# ============================================================================


class OpenTableAdapter(ReservationsAdapter):
    """
    Adapter for OpenTable, the global reservation platform.

    API: https://platform.opentable.com/booking/v2/
    """

    base_url = "https://platform.opentable.com/booking/v2"
    source_name = "opentable"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch reservation signals from OpenTable.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: OpenTable restaurant ID.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("OpenTable requires venue_id; skipping fetch")
            return []

        signals = []

        try:
            bookings = await self._fetch_reservations(venue_id, start_date, end_date)

            # Group by date
            bookings_by_date: dict[date, list[dict]] = {}
            for booking in bookings:
                booking_time = booking.get("reservation_time", "")
                booking_date = self._parse_datetime(booking_time).date() if booking_time else start_date
                bookings_by_date.setdefault(booking_date, []).append(booking)

            # Create daily signals
            for booking_date in sorted(bookings_by_date.keys()):
                snapshot = self._process_bookings(bookings_by_date[booking_date], booking_date)

                signal = self._make_signal(
                    signal_date=booking_date,
                    strength=self._calculate_signal_strength(snapshot, 45.0),
                    description=(
                        f"OpenTable: {snapshot.total_bookings} reservations, "
                        f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                    ),
                    confidence=self._calculate_confidence(booking_date),
                    hour=19,
                    raw_data={"snapshot": vars(snapshot)},
                    venue_id=venue_id,
                    ttl_minutes=10,
                )
                signals.append(signal)

        except Exception as e:
            logger.error(f"OpenTable fetch failed for {venue_id}: {e}")

        return signals

    async def _fetch_reservations(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch reservations from OpenTable.

        Args:
            venue_id: Restaurant ID.
            start_date: Start date.
            end_date: End date.

        Returns:
            List of reservation dictionaries.
        """
        url = f"{self.base_url}/restaurants/{venue_id}/reservations"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}

        try:
            response = await self.http_client.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json().get("reservations", [])
        except httpx.HTTPError as e:
            logger.error(f"OpenTable API error: {e}")
            return []

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
    ) -> BookingSnapshot:
        """
        Process OpenTable reservation data.

        Args:
            bookings: List of reservation dictionaries.
            booking_date: Date of bookings.

        Returns:
            BookingSnapshot.
        """
        snapshot = BookingSnapshot(date=booking_date)

        for booking in bookings:
            party_size = booking.get("party_size", 1)
            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """Parse ISO datetime string."""
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return datetime.now()

    async def is_available(self) -> bool:
        """Check if OpenTable API is reachable."""
        if not self.api_key:
            return False

        try:
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = await self.http_client.get(
                f"{self.base_url}/health",
                headers=headers,
                timeout=5.0,
            )
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"OpenTable health check failed: {e}")
            return False


# ============================================================================
# SevenRooms Adapter
# ============================================================================


class SevenRoomsAdapter(ReservationsAdapter):
    """
    Adapter for SevenRooms, a growing reservation platform in Australia.

    API: https://api.sevenrooms.com/2_4/
    """

    base_url = "https://api.sevenrooms.com/2_4"
    source_name = "sevenrooms"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch reservation signals from SevenRooms.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: SevenRooms venue ID.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("SevenRooms requires venue_id; skipping fetch")
            return []

        signals = []

        try:
            bookings = await self._fetch_reservations(venue_id, start_date, end_date)

            # Group by date and hour for hourly signals
            bookings_by_datetime: dict[tuple[date, int], list[dict]] = {}

            for booking in bookings:
                reservation_time = booking.get("reservation_datetime", "")
                if reservation_time:
                    dt = self._parse_datetime(reservation_time)
                    key = (dt.date(), dt.hour)
                    bookings_by_datetime.setdefault(key, []).append(booking)

            # Create hourly signals
            for (booking_date, hour), hour_bookings in sorted(bookings_by_datetime.items()):
                snapshot = self._process_bookings(hour_bookings, booking_date, hour=hour)

                signal = self._make_signal(
                    signal_date=booking_date,
                    strength=self._calculate_signal_strength(snapshot, 15.0),
                    description=(
                        f"SevenRooms {hour:02d}:00: {snapshot.total_bookings} bookings, "
                        f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                    ),
                    confidence=self._calculate_confidence(booking_date),
                    hour=hour,
                    raw_data={"snapshot": vars(snapshot)},
                    venue_id=venue_id,
                    ttl_minutes=10,
                )
                signals.append(signal)

        except Exception as e:
            logger.error(f"SevenRooms fetch failed for {venue_id}: {e}")

        return signals

    async def _fetch_reservations(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch reservations from SevenRooms.

        Args:
            venue_id: Venue ID.
            start_date: Start date.
            end_date: End date.

        Returns:
            List of reservation dictionaries.
        """
        url = f"{self.base_url}/venues/{venue_id}/reservations"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "api_key": self.api_key,
        }

        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            return response.json().get("reservations", [])
        except httpx.HTTPError as e:
            logger.error(f"SevenRooms API error: {e}")
            return []

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
        hour: Optional[int] = None,
    ) -> BookingSnapshot:
        """
        Process SevenRooms reservation data.

        Args:
            bookings: List of reservation dictionaries.
            booking_date: Date of bookings.
            hour: Hour of bookings (optional for breakdown).

        Returns:
            BookingSnapshot.
        """
        snapshot = BookingSnapshot(date=booking_date, hour=hour)

        for booking in bookings:
            party_size = booking.get("party_size", 1)
            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """Parse ISO datetime string."""
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return datetime.now()

    async def is_available(self) -> bool:
        """Check if SevenRooms API is reachable."""
        if not self.api_key:
            return False

        try:
            response = await self.http_client.get(
                f"{self.base_url}/health",
                params={"api_key": self.api_key},
                timeout=5.0,
            )
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"SevenRooms health check failed: {e}")
            return False


# ============================================================================
# Direct Booking Importer
# ============================================================================


class DirectBookingImporter(ReservationsAdapter):
    """
    Generic importer for venues with their own booking system.

    Supports webhook ingestion and CSV imports. Venues without a dedicated
    platform adapter can use this to feed bookings directly.
    """

    source_name = "direct_bookings"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        no_show_rate: float = 0.15,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize direct booking importer.

        Args:
            api_key: Optional webhook token for validation.
            cache: Signal cache instance.
            no_show_rate: Expected no-show rate.
            http_client: Shared httpx client.
        """
        super().__init__(
            api_key=api_key,
            cache=cache,
            no_show_rate=no_show_rate,
            http_client=http_client,
        )
        self._bookings_store: dict[str, list[dict]] = {}

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch signals from directly ingested bookings.

        Args:
            location: Venue location.
            start_date: Start date.
            end_date: End date.
            venue_id: Venue identifier.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("DirectBookingImporter requires venue_id")
            return []

        signals = []
        venue_bookings = self._bookings_store.get(venue_id, [])

        # Filter bookings to requested date range
        filtered_bookings = [
            b for b in venue_bookings
            if start_date <= self._parse_date(b.get("date", "")) <= end_date
        ]

        # Group by date
        bookings_by_date: dict[date, list[dict]] = {}
        for booking in filtered_bookings:
            booking_date = self._parse_date(booking.get("date", ""))
            if booking_date:
                bookings_by_date.setdefault(booking_date, []).append(booking)

        # Create signals
        for booking_date in sorted(bookings_by_date.keys()):
            snapshot = self._process_bookings(bookings_by_date[booking_date], booking_date)

            signal = self._make_signal(
                signal_date=booking_date,
                strength=self._calculate_signal_strength(snapshot, 35.0),
                description=(
                    f"Direct: {snapshot.total_bookings} bookings, "
                    f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                ),
                confidence=self._calculate_confidence(booking_date),
                hour=None,  # All-day aggregate
                raw_data={"snapshot": vars(snapshot)},
                venue_id=venue_id,
                ttl_minutes=10,
            )
            signals.append(signal)

        return signals

    def ingest_bookings(
        self,
        venue_id: str,
        bookings: list[dict[str, Any]],
    ) -> None:
        """
        Ingest booking data directly (e.g., from webhook or CSV).

        Expected booking dict keys: date, party_size, time (optional).

        Args:
            venue_id: Venue identifier.
            bookings: List of booking dictionaries.
        """
        if venue_id not in self._bookings_store:
            self._bookings_store[venue_id] = []

        self._bookings_store[venue_id].extend(bookings)
        logger.info(f"Ingested {len(bookings)} bookings for venue {venue_id}")

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
    ) -> BookingSnapshot:
        """
        Process direct booking data.

        Args:
            bookings: List of booking dictionaries.
            booking_date: Date of bookings.

        Returns:
            BookingSnapshot.
        """
        snapshot = BookingSnapshot(date=booking_date)

        for booking in bookings:
            party_size = booking.get("party_size", 1)
            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    @staticmethod
    def _parse_date(date_str: str) -> date:
        """Parse date string in ISO format."""
        try:
            if "T" in date_str:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, AttributeError, TypeError):
            return date.today()

    async def is_available(self) -> bool:
        """Direct importer is always available if configured."""
        return True


# ============================================================================
# Book It Live Adapter
# ============================================================================


class BookitLiveAdapter(ReservationsAdapter):
    """
    Adapter for BookitLive, an Australian online booking platform.

    BookitLive is used across hospitality, health, fitness, and services.
    Supports appointment-style and table bookings via REST API.

    API: https://api.bookitlive.net/api/v1/
    """

    base_url = "https://api.bookitlive.net/api/v1"
    source_name = "bookitlive"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch reservation signals from BookitLive.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: BookitLive venue/account ID.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("BookitLive requires venue_id; skipping fetch")
            return []

        signals = []

        try:
            bookings = await self._fetch_bookings_range(venue_id, start_date, end_date)

            # Group bookings by date and hour for granular signals
            bookings_by_datetime: dict[tuple[date, int], list[dict]] = {}
            bookings_by_date: dict[date, list[dict]] = {}

            for booking in bookings:
                booking_time = booking.get("booking_datetime") or booking.get("start_time", "")
                booking_date = self._parse_date(booking_time)

                if booking_date:
                    bookings_by_date.setdefault(booking_date, []).append(booking)

                    # Extract hour if available
                    hour = self._extract_hour(booking_time)
                    if hour is not None:
                        key = (booking_date, hour)
                        bookings_by_datetime.setdefault(key, []).append(booking)

            # Prefer hourly signals if time data is available, else daily
            if bookings_by_datetime:
                for (bdate, hour), hour_bookings in sorted(bookings_by_datetime.items()):
                    snapshot = self._process_bookings(hour_bookings, bdate, hour=hour)

                    signal = self._make_signal(
                        signal_date=bdate,
                        strength=self._calculate_signal_strength(snapshot, 30.0),
                        description=(
                            f"BookitLive {hour:02d}:00: {snapshot.total_bookings} bookings, "
                            f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                        ),
                        confidence=self._calculate_confidence(bdate),
                        hour=hour,
                        raw_data={"snapshot": vars(snapshot)},
                        venue_id=venue_id,
                        ttl_minutes=10,
                    )
                    signals.append(signal)
            else:
                for bdate in sorted(bookings_by_date.keys()):
                    snapshot = self._process_bookings(bookings_by_date[bdate], bdate)

                    signal = self._make_signal(
                        signal_date=bdate,
                        strength=self._calculate_signal_strength(snapshot, 30.0),
                        description=(
                            f"BookitLive: {snapshot.total_bookings} bookings, "
                            f"{snapshot.no_show_adjusted_covers:.0f} adjusted covers"
                        ),
                        confidence=self._calculate_confidence(bdate),
                        hour=19,  # Default evening
                        raw_data={"snapshot": vars(snapshot)},
                        venue_id=venue_id,
                        ttl_minutes=10,
                    )
                    signals.append(signal)

        except Exception as e:
            logger.error(f"BookitLive fetch failed for {venue_id}: {e}")

        return signals

    async def _fetch_bookings_range(
        self,
        venue_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch bookings from BookitLive for a date range.

        Args:
            venue_id: Venue/account ID.
            start_date: Start date.
            end_date: End date.

        Returns:
            List of booking dictionaries.
        """
        url = f"{self.base_url}/bookings"
        params = {
            "account_id": venue_id,
            "date_from": start_date.isoformat(),
            "date_to": end_date.isoformat(),
            "status": "confirmed,pending",
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        try:
            response = await self.http_client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get("bookings", data.get("data", []))
        except httpx.HTTPError as e:
            logger.error(f"BookitLive API error: {e}")
            return []

    def _process_bookings(
        self,
        bookings: list[dict[str, Any]],
        booking_date: date,
        hour: Optional[int] = None,
    ) -> BookingSnapshot:
        """
        Process BookitLive booking data into a snapshot.

        BookitLive bookings may use 'guests', 'party_size', or 'num_people'
        for party size depending on venue configuration.

        Args:
            bookings: List of booking dictionaries.
            booking_date: Date of bookings.
            hour: Hour of bookings (optional).

        Returns:
            BookingSnapshot.
        """
        snapshot = BookingSnapshot(date=booking_date, hour=hour)

        for booking in bookings:
            # BookitLive uses varying field names for party size
            party_size = (
                booking.get("party_size")
                or booking.get("guests")
                or booking.get("num_people")
                or 1
            )
            party_size = int(party_size)

            snapshot.total_bookings += 1
            snapshot.total_covers += party_size

            if party_size >= 10:
                snapshot.large_parties += 1

        snapshot.no_show_adjusted_covers = self._adjust_for_no_shows(snapshot.total_covers)
        return snapshot

    @staticmethod
    def _parse_date(date_str: str) -> Optional[date]:
        """Parse date/datetime string to date object."""
        if not date_str:
            return None
        try:
            if "T" in date_str:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _extract_hour(dt_str: str) -> Optional[int]:
        """Extract hour from a datetime string."""
        if not dt_str:
            return None
        try:
            if "T" in dt_str:
                return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).hour
            return None
        except (ValueError, AttributeError):
            return None

    async def is_available(self) -> bool:
        """Check if BookitLive API is reachable."""
        if not self.api_key:
            return False

        try:
            response = await self.http_client.get(
                f"{self.base_url}/health",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=5.0,
            )
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"BookitLive health check failed: {e}")
            return False


# ============================================================================
# Reservations Aggregator
# ============================================================================


class ReservationsAggregator(DataFeedAdapter):
    """
    Composite adapter that combines signals from multiple reservation platforms.

    Merges signals from ResDiary, Now Book It, OpenTable, SevenRooms,
    BookitLive, and direct bookings into a unified view of reservation demand.
    """

    category = FeedCategory.reservations
    source_name = "reservations_aggregated"

    def __init__(
        self,
        adapters: Optional[list[ReservationsAdapter]] = None,
        cache: Optional[SignalCache] = None,
    ):
        """
        Initialize the aggregator.

        Args:
            adapters: List of reservation adapters to aggregate.
            cache: Shared signal cache.
        """
        super().__init__(cache=cache or SignalCache(default_ttl_minutes=10))
        self.adapters = adapters or []

    def add_adapter(self, adapter: ReservationsAdapter) -> None:
        """
        Register a reservation adapter.

        Args:
            adapter: ReservationsAdapter instance.
        """
        self.adapters.append(adapter)

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch and aggregate signals from all configured adapters.

        Merges signals from multiple platforms by:
        1. Collecting signals from all adapters in parallel
        2. Grouping by date and hour
        3. Averaging strength and confidence across sources
        4. Creating unified signal with combined description

        Args:
            location: Venue location.
            start_date: Start date.
            end_date: End date.
            venue_id: Venue identifier.

        Returns:
            List of aggregated FeedSignal objects.
        """
        if not self.adapters:
            logger.warning("ReservationsAggregator has no adapters configured")
            return []

        # Fetch signals from all adapters in parallel
        tasks = [
            adapter.fetch_signals(location, start_date, end_date, venue_id)
            for adapter in self.adapters
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results and filter exceptions
        all_signals: list[FeedSignal] = []
        for result in results:
            if isinstance(result, list):
                all_signals.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Adapter fetch failed: {result}")

        # Group signals by (date, hour)
        signals_by_key: dict[tuple[date, Optional[int]], list[FeedSignal]] = {}
        for signal in all_signals:
            key = (signal.signal_date, signal.signal_hour)
            signals_by_key.setdefault(key, []).append(signal)

        # Merge signals for each (date, hour)
        merged_signals = []
        for (signal_date, signal_hour), signals in sorted(signals_by_key.items()):
            merged = self._merge_signals(signals, signal_date, signal_hour, venue_id)
            merged_signals.append(merged)

        return merged_signals

    def _merge_signals(
        self,
        signals: list[FeedSignal],
        signal_date: date,
        signal_hour: Optional[int],
        venue_id: Optional[str],
    ) -> FeedSignal:
        """
        Merge multiple signals from different sources into one.

        Args:
            signals: Signals to merge.
            signal_date: Date of signal.
            signal_hour: Hour of signal.
            venue_id: Venue identifier.

        Returns:
            Merged FeedSignal.
        """
        if not signals:
            return FeedSignal(
                category=self.category,
                source=self.source_name,
                signal_date=signal_date,
                signal_hour=signal_hour,
            )

        # Average confidence
        avg_confidence = sum(s.confidence for s in signals) / len(signals)

        # Average value
        avg_value = sum(s.value for s in signals) / len(signals)

        # Infer merged strength from value
        from rosteriq.data_feeds.base import STRENGTH_MULTIPLIERS

        merged_strength = SignalStrength.neutral
        for strength, multiplier in STRENGTH_MULTIPLIERS.items():
            if abs(multiplier - avg_value) < 0.1:
                merged_strength = strength
                break

        # Combine descriptions
        sources = [s.source for s in signals]
        combined_desc = f"Aggregated ({', '.join(sources)}): {len(signals)} sources"

        merged = FeedSignal(
            category=self.category,
            source=self.source_name,
            signal_date=signal_date,
            signal_hour=signal_hour,
            strength=merged_strength,
            value=avg_value,
            confidence=avg_confidence,
            description=combined_desc,
            raw_data={"source_signals": [s.cache_key for s in signals]},
            venue_id=venue_id,
        )

        self.cache.put(merged, ttl_minutes=10)
        return merged

    async def is_available(self) -> bool:
        """Check if at least one adapter is available."""
        if not self.adapters:
            return False

        availability = await asyncio.gather(
            *[a.is_available() for a in self.adapters],
            return_exceptions=True,
        )

        return any(
            a is True for a in availability if not isinstance(a, Exception)
        )
