"""
Base classes and shared types for all RosterIQ data feed adapters.

Every external data source adapter inherits from DataFeedAdapter and
produces FeedSignal objects. The SignalAggregator collects signals from
all adapters, normalises them, and feeds them into the variance engine.
"""

from __future__ import annotations

import abc
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Enums
# ============================================================================


class FeedCategory(str, Enum):
    """Categories of external data feeds."""

    weather = "weather"
    events = "events"
    foot_traffic = "foot_traffic"
    reservations = "reservations"
    school_holidays = "school_holidays"
    sports = "sports"
    nearby_venues = "nearby_venues"
    delivery = "delivery"
    tourism = "tourism"
    transport = "transport"
    economic = "economic"
    calendar = "calendar"


class SignalStrength(str, Enum):
    """How strongly a signal affects demand. Maps to multiplier ranges."""

    extreme_negative = "extreme_negative"  # e.g. cyclone, total road closure
    strong_negative = "strong_negative"    # e.g. heavy rain all day, major roadworks
    moderate_negative = "moderate_negative"  # e.g. cold & windy, nearby venue promo
    weak_negative = "weak_negative"
    neutral = "neutral"
    weak_positive = "weak_positive"
    moderate_positive = "moderate_positive"  # e.g. nice weather, school holidays start
    strong_positive = "strong_positive"      # e.g. AFL grand final, concert nearby
    extreme_positive = "extreme_positive"    # e.g. NYE, major festival on doorstep


STRENGTH_MULTIPLIERS: dict[SignalStrength, float] = {
    SignalStrength.extreme_negative: -1.0,
    SignalStrength.strong_negative: -0.6,
    SignalStrength.moderate_negative: -0.3,
    SignalStrength.weak_negative: -0.1,
    SignalStrength.neutral: 0.0,
    SignalStrength.weak_positive: 0.1,
    SignalStrength.moderate_positive: 0.3,
    SignalStrength.strong_positive: 0.6,
    SignalStrength.extreme_positive: 1.0,
}


# ============================================================================
# Data classes
# ============================================================================


@dataclass(frozen=True)
class Location:
    """Venue geographic location for proximity-based lookups."""

    latitude: float
    longitude: float
    address: str = ""
    suburb: str = ""
    state: str = ""
    postcode: str = ""

    @property
    def coords(self) -> tuple[float, float]:
        return (self.latitude, self.longitude)

    def distance_km(self, other: Location) -> float:
        """Haversine distance in km to another location."""
        import math
        R = 6371.0  # Earth radius in km
        lat1, lon1 = math.radians(self.latitude), math.radians(self.longitude)
        lat2, lon2 = math.radians(other.latitude), math.radians(other.longitude)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclass
class FeedSignal:
    """
    Normalised signal from any external data feed.

    This is the universal currency between data adapters and the
    variance engine. Every adapter produces FeedSignal objects.
    """

    category: FeedCategory
    source: str               # e.g. "bom_weather", "afl_schedule", "resdiary"
    signal_date: date         # The date this signal applies to
    signal_hour: Optional[int] = None  # Hour (0-23) if time-specific, None if all-day
    strength: SignalStrength = SignalStrength.neutral
    value: float = 0.0       # Normalised value (-1.0 to 1.0)
    confidence: float = 0.8  # How confident we are in this signal (0-1)
    description: str = ""    # Human-readable description
    raw_data: dict[str, Any] = field(default_factory=dict)  # Original API response
    fetched_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None  # When this signal becomes stale
    venue_id: Optional[str] = None  # Specific venue, or None for area-wide

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at

    @property
    def cache_key(self) -> str:
        """Unique key for deduplication and caching."""
        parts = f"{self.category}:{self.source}:{self.signal_date}:{self.signal_hour}:{self.venue_id}"
        return hashlib.md5(parts.encode()).hexdigest()

    def to_variance_value(self) -> float:
        """Convert to value compatible with variance engine (-1 to 1)."""
        if self.value != 0.0:
            return max(-1.0, min(1.0, self.value))
        return STRENGTH_MULTIPLIERS.get(self.strength, 0.0)


# ============================================================================
# Signal cache (in-memory TTL cache for API responses)
# ============================================================================


class SignalCache:
    """Simple in-memory cache for feed signals with TTL expiry."""

    def __init__(self, default_ttl_minutes: int = 30):
        self._cache: dict[str, FeedSignal] = {}
        self._default_ttl = timedelta(minutes=default_ttl_minutes)

    def get(self, key: str) -> Optional[FeedSignal]:
        signal = self._cache.get(key)
        if signal and not signal.is_expired:
            return signal
        if signal:
            del self._cache[key]
        return None

    def put(self, signal: FeedSignal, ttl_minutes: Optional[int] = None) -> None:
        if signal.expires_at is None:
            ttl = timedelta(minutes=ttl_minutes) if ttl_minutes else self._default_ttl
            signal.expires_at = datetime.now() + ttl
        self._cache[signal.cache_key] = signal

    def clear_expired(self) -> int:
        expired_keys = [k for k, v in self._cache.items() if v.is_expired]
        for k in expired_keys:
            del self._cache[k]
        return len(expired_keys)

    @property
    def size(self) -> int:
        return len(self._cache)


# ============================================================================
# Abstract adapter base
# ============================================================================


class DataFeedAdapter(abc.ABC):
    """
    Abstract base class for all external data feed adapters.

    Subclasses must implement:
    - fetch_signals(): async method to retrieve signals for a date range
    - category: the FeedCategory this adapter serves

    Optional overrides:
    - is_available(): check if the API/data source is reachable
    - configure(): set API keys and other config
    """

    def __init__(self, api_key: Optional[str] = None, cache: Optional[SignalCache] = None):
        self.api_key = api_key
        self.cache = cache or SignalCache()
        self._configured = False

    @property
    @abc.abstractmethod
    def category(self) -> FeedCategory:
        """The feed category this adapter serves."""
        ...

    @property
    @abc.abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source (e.g. 'bom_weather')."""
        ...

    @abc.abstractmethod
    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch signals for a location and date range.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects, one per date (or date+hour).
        """
        ...

    async def is_available(self) -> bool:
        """Check if the data source is reachable. Override for real health checks."""
        return self.api_key is not None or self._configured

    def configure(self, **kwargs: Any) -> None:
        """Set adapter-specific configuration. Override as needed."""
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._configured = True

    def _make_signal(
        self,
        signal_date: date,
        strength: SignalStrength,
        description: str,
        value: float = 0.0,
        confidence: float = 0.8,
        hour: Optional[int] = None,
        raw_data: Optional[dict] = None,
        venue_id: Optional[str] = None,
        ttl_minutes: int = 30,
    ) -> FeedSignal:
        """Helper to create a FeedSignal with standard defaults."""
        signal = FeedSignal(
            category=self.category,
            source=self.source_name,
            signal_date=signal_date,
            signal_hour=hour,
            strength=strength,
            value=value if value != 0.0 else STRENGTH_MULTIPLIERS.get(strength, 0.0),
            confidence=confidence,
            description=description,
            raw_data=raw_data or {},
            venue_id=venue_id,
        )
        self.cache.put(signal, ttl_minutes=ttl_minutes)
        return signal
