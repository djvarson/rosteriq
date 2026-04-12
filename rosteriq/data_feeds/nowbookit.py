"""
NowBookIt Reservation Feed Adapter for RosterIQ
================================================

Pulls reservation data from NowBookIt (Australian restaurant reservation system)
to provide booking demand signals for the RosterIQ variance engine.

NowBookIt API requires:
  - api_key:      API authentication key
  - venue_id:     NowBookIt venue identifier
  - base_url:     API base URL (default: https://api.nowbookit.com/v1)

Endpoints used:
  - GET  /venues/{venue_id}/reservations  → List reservations
  - GET  /venues/{venue_id}                → Venue details
  - GET  /health                           → API health check

Generates signals in format:
  {
    "signal_type": "reservations",
    "value": float (0.0-1.0),
    "confidence": float (0.0-1.0),
    "source": "nowbookit",
    "timestamp": ISO datetime,
    "metadata": {...}
  }
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Optional

try:
    import httpx
except ImportError:
    httpx = None

logger = logging.getLogger("rosteriq.data_feeds.nowbookit")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NOWBOOKIT_DEFAULT_TIMEOUT = 30  # seconds
NOWBOOKIT_MAX_RETRIES = 3
NOWBOOKIT_RETRY_DELAY = 2  # seconds

AU_TZ = timezone(timedelta(hours=10))  # AEST


class NowBookItError(Exception):
    """Base exception for NowBookIt adapter errors."""
    pass


class NowBookItAuthError(NowBookItError):
    """Authentication failed."""
    pass


class NowBookItRateLimitError(NowBookItError):
    """Rate limit exceeded."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class NowBookItCredentials:
    """NowBookIt API credentials."""
    api_key: str
    venue_id: str
    base_url: str = "https://api.nowbookit.com/v1"

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")


@dataclass
class Reservation:
    """Single reservation record."""
    reservation_id: str
    date: date
    time: str  # HH:MM format
    covers: int
    name: Optional[str] = None
    notes: Optional[str] = None
    special_requests: Optional[str] = None
    status: str = "confirmed"  # confirmed, cancelled, no-show


@dataclass
class ReservationSnapshot:
    """Point-in-time reservation data snapshot."""
    timestamp: datetime
    venue_id: str
    venue_name: str
    total_covers: int
    reservation_count: int
    avg_covers_per_reservation: float
    booking_dates: dict[str, int] = field(default_factory=dict)  # date -> covers
    hourly_breakdown: dict[int, int] = field(default_factory=dict)  # hour -> covers


@dataclass
class BookingPattern:
    """Analysed booking pattern for demand forecasting."""
    day_of_week: int  # 0=Monday
    hour: int  # 0-23
    avg_covers: int
    avg_reservations: int
    fill_rate: float  # as percentage
    peak_indicator: bool  # True if typical peak period


# ---------------------------------------------------------------------------
# NowBookIt API Client
# ---------------------------------------------------------------------------

class NowBookItClient:
    """Low-level async client for NowBookIt REST API."""

    def __init__(self, credentials: NowBookItCredentials):
        if not httpx:
            raise ImportError("httpx is required for NowBookItClient. Install with: pip install httpx")
        self.creds = credentials
        self._client: Optional[httpx.AsyncClient] = None
        self._auth_token: Optional[str] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.creds.base_url,
                timeout=NOWBOOKIT_DEFAULT_TIMEOUT,
                headers={
                    "Content-Type": "application/json",
                    "X-API-Key": self.creds.api_key,
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make authenticated API request with retries."""
        client = await self._get_client()

        for attempt in range(NOWBOOKIT_MAX_RETRIES):
            try:
                resp = await client.request(method, path, **kwargs)

                if resp.status_code == 401:
                    raise NowBookItAuthError("Invalid NowBookIt API key")

                if resp.status_code == 429:
                    wait = NOWBOOKIT_RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json() if resp.content else {}

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    raise NowBookItAuthError("Authentication failed") from e
                elif e.response.status_code == 429:
                    continue
                raise NowBookItError(f"API error {e.response.status_code}: {e}") from e
            except httpx.RequestError as e:
                if attempt == NOWBOOKIT_MAX_RETRIES - 1:
                    raise NowBookItError(f"Request failed after {NOWBOOKIT_MAX_RETRIES} attempts: {e}") from e
                await asyncio.sleep(NOWBOOKIT_RETRY_DELAY)

        raise NowBookItError("Max retries exceeded")

    async def get(self, path: str, params: dict = None) -> dict:
        return await self._request("GET", path, params=params)

    async def get_reservations(
        self,
        from_date: date,
        to_date: date,
    ) -> list[dict]:
        """Get reservations for a date range."""
        path = f"/venues/{self.creds.venue_id}/reservations"
        params = {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
        }
        data = await self.get(path, params=params)
        return data if isinstance(data, list) else data.get("reservations", [])

    async def get_venue(self) -> dict:
        """Get venue details."""
        path = f"/venues/{self.creds.venue_id}"
        return await self.get(path)

    async def health_check(self) -> dict:
        """Check API health."""
        return await self.get("/health")


# ---------------------------------------------------------------------------
# Reservation Data Analyser
# ---------------------------------------------------------------------------

class ReservationAnalyser:
    """Analyses NowBookIt reservation data to derive demand signals."""

    def __init__(self, lookback_weeks: int = 8):
        self.lookback_weeks = lookback_weeks
        self._booking_patterns: dict[str, list[BookingPattern]] = {}

    def build_reservation_snapshot(
        self,
        venue_id: str,
        venue_name: str,
        reservations: list[dict],
        timestamp: datetime = None,
    ) -> ReservationSnapshot:
        """Build a ReservationSnapshot from raw reservation data."""
        if timestamp is None:
            timestamp = datetime.now(AU_TZ)

        total_covers = 0
        reservation_count = len(reservations)
        booking_dates: dict[str, int] = {}
        hourly_totals: dict[int, int] = {}

        for res in reservations:
            # Skip cancelled/no-show
            status = res.get("status", "confirmed").lower()
            if status in ["cancelled", "no-show"]:
                continue

            covers = int(res.get("covers", 0) or res.get("party_size", 0) or 0)
            if covers == 0:
                continue

            total_covers += covers

            # Date tracking
            res_date = res.get("reservation_date", res.get("date", ""))
            if res_date:
                date_key = res_date if isinstance(res_date, str) else res_date.isoformat()
                booking_dates[date_key] = booking_dates.get(date_key, 0) + covers

            # Hourly breakdown
            res_time = res.get("reservation_time", res.get("time", ""))
            if res_time:
                try:
                    if isinstance(res_time, str):
                        hour = int(res_time.split(":")[0])
                    else:
                        hour = res_time.hour
                    hourly_totals[hour] = hourly_totals.get(hour, 0) + covers
                except (ValueError, AttributeError, IndexError):
                    pass

        avg_covers = total_covers / max(reservation_count, 1)

        return ReservationSnapshot(
            timestamp=timestamp,
            venue_id=venue_id,
            venue_name=venue_name,
            total_covers=total_covers,
            reservation_count=reservation_count,
            avg_covers_per_reservation=round(avg_covers, 1),
            booking_dates=booking_dates,
            hourly_breakdown=hourly_totals,
        )

    def analyse_booking_patterns(
        self,
        historical_snapshots: list[ReservationSnapshot],
    ) -> list[BookingPattern]:
        """Analyse historical snapshots to derive booking patterns."""
        # Group by day_of_week + hour
        buckets: dict[tuple[int, int], list[ReservationSnapshot]] = {}

        for snap in historical_snapshots:
            dow = snap.timestamp.weekday()
            for hour, covers in snap.hourly_breakdown.items():
                key = (dow, hour)
                if key not in buckets:
                    buckets[key] = []
                buckets[key].append(snap)

        patterns = []
        for (dow, hour), snaps in sorted(buckets.items()):
            cover_counts = []
            res_counts = []

            for s in snaps:
                covers = s.hourly_breakdown.get(hour, 0)
                cover_counts.append(covers)
                # Estimate hourly reservations proportionally
                if s.total_covers > 0:
                    res_pct = covers / s.total_covers
                    res_counts.append(int(s.reservation_count * res_pct))
                else:
                    res_counts.append(0)

            avg_covers = sum(cover_counts) // max(len(cover_counts), 1)
            avg_res = sum(res_counts) // max(len(res_counts), 1)

            # Determine if peak period (simple heuristic: dinner hours)
            is_peak = 17 <= hour <= 21

            patterns.append(BookingPattern(
                day_of_week=dow,
                hour=hour,
                avg_covers=avg_covers,
                avg_reservations=avg_res,
                fill_rate=0.85 if is_peak else 0.65,
                peak_indicator=is_peak,
            ))

        return patterns

    def get_demand_signal(
        self,
        current_snapshot: ReservationSnapshot,
        patterns: list[BookingPattern],
    ) -> tuple[float, float]:
        """
        Compare current bookings against historical patterns to derive
        a demand signal and confidence score.

        Returns (demand_multiplier, confidence) tuple.
        Multiplier ranges from -1.0 (very quiet) to +1.0 (very busy).
        """
        now = current_snapshot.timestamp
        dow = now.weekday()
        hour = now.hour

        # Find matching pattern
        matching = [p for p in patterns if p.day_of_week == dow and p.hour == hour]
        if not matching:
            return 0.0, 0.5  # Default signal

        pattern = matching[0]
        if pattern.avg_covers == 0:
            return 0.0, 0.3

        # Compare current hourly covers to historical average
        current_hourly = current_snapshot.hourly_breakdown.get(hour, 0)
        deviation = (current_hourly - pattern.avg_covers) / max(pattern.avg_covers, 1)

        # Scale to -1.0 to +1.0 range
        if deviation > 0.5:
            multiplier = 1.0
        elif deviation > 0.25:
            multiplier = 0.5
        elif deviation > 0:
            multiplier = 0.25
        elif deviation > -0.25:
            multiplier = -0.1
        elif deviation > -0.5:
            multiplier = -0.5
        else:
            multiplier = -1.0

        return multiplier, 0.85


# ---------------------------------------------------------------------------
# NowBookIt Data Feed Adapter
# ---------------------------------------------------------------------------

class NowBookItAdapter:
    """
    High-level adapter that connects NowBookIt reservation data to the
    RosterIQ variance engine via signal format.

    Usage:
        adapter = NowBookItAdapter(credentials)
        signals = await adapter.fetch_reservations()
        demand_signal = await adapter.get_demand_signal(target_date)
    """

    SIGNAL_TYPE = "reservations"
    SOURCE = "nowbookit"

    def __init__(
        self,
        credentials: NowBookItCredentials,
        lookback_weeks: int = 8,
        walk_in_ratio: float = 0.4,
    ):
        self.client = NowBookItClient(credentials)
        self.analyser = ReservationAnalyser(lookback_weeks)
        self.venue_id = credentials.venue_id
        self.lookback_weeks = lookback_weeks
        self.walk_in_ratio = walk_in_ratio  # Proportion of covers that are walk-ins
        self._patterns: list[BookingPattern] = []
        self._last_pattern_build: Optional[datetime] = None
        self._venue_cache: Optional[dict] = None

    async def initialise(self):
        """
        Build historical booking patterns. Call once on startup,
        then periodically (e.g. daily) to refresh.
        """
        logger.info(f"Building booking patterns for venue {self.venue_id}")
        now = datetime.now(AU_TZ)
        snapshots = []

        # Fetch venue details
        try:
            venue = await self.client.get_venue()
            self._venue_cache = venue
            venue_name = venue.get("name", "Venue")
        except NowBookItError:
            venue_name = "Venue"

        # Fetch last N weeks of daily data
        for week in range(self.lookback_weeks):
            for day in range(7):
                target_date = (now - timedelta(weeks=week, days=day)).date()
                start = target_date
                end = target_date

                try:
                    reservations = await self.client.get_reservations(start, end)
                    if reservations:
                        snap = self.analyser.build_reservation_snapshot(
                            self.venue_id, venue_name,
                            reservations, datetime.combine(target_date, datetime.min.time()).replace(tzinfo=AU_TZ),
                        )
                        snapshots.append(snap)
                except NowBookItError as e:
                    logger.warning(f"Failed to fetch {target_date}: {e}")
                    continue

                # Small delay to avoid rate limits
                await asyncio.sleep(0.1)

        self._patterns = self.analyser.analyse_booking_patterns(snapshots)
        self._last_pattern_build = now
        logger.info(f"Built {len(self._patterns)} booking patterns from {len(snapshots)} snapshots")

    async def fetch_reservations(
        self,
        from_date: date,
        to_date: date,
    ) -> list[Reservation]:
        """
        Fetch reservations for a date range.

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            List of Reservation objects
        """
        try:
            raw = await self.client.get_reservations(from_date, to_date)
            reservations = []
            for res in raw:
                reservations.append(Reservation(
                    reservation_id=res.get("id", res.get("reservation_id", "")),
                    date=datetime.fromisoformat(res.get("date", res.get("reservation_date", ""))).date()
                          if res.get("date") or res.get("reservation_date") else from_date,
                    time=res.get("time", res.get("reservation_time", "18:00")),
                    covers=int(res.get("covers", res.get("party_size", 0)) or 0),
                    name=res.get("name", res.get("guest_name")),
                    notes=res.get("notes", res.get("special_requests")),
                    special_requests=res.get("special_requests"),
                    status=res.get("status", "confirmed"),
                ))
            return reservations
        except NowBookItError as e:
            logger.error(f"Failed to fetch reservations: {e}")
            return []

    async def get_demand_signal(self, target_date: date) -> dict:
        """
        Get demand signal for a specific date.

        Args:
            target_date: Date to forecast for

        Returns:
            Signal dict with demand metrics
        """
        # Rebuild patterns daily if needed
        if (
            not self._patterns
            or not self._last_pattern_build
            or (datetime.now(AU_TZ) - self._last_pattern_build) > timedelta(hours=24)
        ):
            await self.initialise()

        # Fetch current day's reservations
        try:
            reservations = await self.fetch_reservations(target_date, target_date)
        except NowBookItError as e:
            logger.error(f"Failed to fetch demand signal: {e}")
            return {}

        booked_covers = sum(r.covers for r in reservations)
        walk_in_covers = int(booked_covers * self.walk_in_ratio / (1 - self.walk_in_ratio))
        total_covers = booked_covers + walk_in_covers

        # Build snapshot
        venue_name = self._venue_cache.get("name", "Venue") if self._venue_cache else "Venue"
        snapshot = self.analyser.build_reservation_snapshot(
            self.venue_id, venue_name,
            [{"covers": r.covers, "reservation_date": r.date, "reservation_time": r.time,
              "status": r.status, "id": r.reservation_id} for r in reservations],
        )

        # Get demand multiplier
        multiplier, confidence = self.analyser.get_demand_signal(snapshot, self._patterns)

        return {
            "signal_type": self.SIGNAL_TYPE,
            "value": multiplier,
            "confidence": confidence,
            "source": self.SOURCE,
            "timestamp": datetime.now(AU_TZ).isoformat(),
            "metadata": {
                "date": target_date.isoformat(),
                "booked_covers": booked_covers,
                "walk_in_estimate": walk_in_covers,
                "total_expected_covers": total_covers,
                "reservation_count": len(reservations),
                "venue": venue_name,
            },
        }

    async def analyse_booking_patterns(self, from_date: date = None, to_date: date = None) -> list[BookingPattern]:
        """
        Analyse booking patterns for a date range.

        Args:
            from_date: Start date (defaults to 8 weeks ago)
            to_date: End date (defaults to today)

        Returns:
            List of BookingPattern objects
        """
        if not self._patterns or not self._last_pattern_build:
            await self.initialise()

        return self._patterns

    async def health_check(self) -> dict:
        """Check NowBookIt API connectivity and auth."""
        try:
            await self.client.health_check()
            venue = await self.client.get_venue()
            return {
                "status": "healthy",
                "connected": True,
                "venue_id": self.venue_id,
                "venue_name": venue.get("name", "Unknown"),
            }
        except NowBookItAuthError:
            return {"status": "auth_failed", "connected": False}
        except NowBookItError as e:
            return {"status": "error", "connected": False, "error": str(e)}

    async def close(self):
        """Close the HTTP client."""
        await self.client.close()


# ---------------------------------------------------------------------------
# Factory function for easy setup
# ---------------------------------------------------------------------------

def create_nowbookit_adapter(
    api_key: str,
    venue_id: str,
    base_url: str = "https://api.nowbookit.com/v1",
) -> NowBookItAdapter:
    """
    Factory function to create a NowBookIt adapter.

    Example:
        adapter = create_nowbookit_adapter(
            api_key="sk_test_abc123",
            venue_id="venue_001",
        )
        signals = await adapter.fetch_reservations(from_date, to_date)
    """
    creds = NowBookItCredentials(
        api_key=api_key,
        venue_id=venue_id,
        base_url=base_url,
    )
    return NowBookItAdapter(credentials=creds)
