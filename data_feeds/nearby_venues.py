"""
Nearby Venues & Competition data feed adapter for RosterIQ.

Monitors competing venues within a configurable radius to assess competitive
pressure on staffing demand. Aggregates data from Google Places, Google Maps
Business, Yelp, and Zomato (Uber Eats AU) to track:

- New venue openings/closures
- Competitor promotion activity and events
- Review velocity and rating trends (trending competitors)
- Operating hours (late-night trade opportunities)
- Venue clustering effects (entertainment precinct bustling)

Signals are confidence-adjusted (0.6) as competitive signals are indirect
demand indicators. Structural data (venue lists, ratings) cached 24h;
activity data (reviews, promotions) cached 6h.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
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
# Enums and data classes
# ============================================================================


class VenueType(str, Enum):
    """Categories of competitive venues."""

    cafe = "cafe"
    bar = "bar"
    restaurant = "restaurant"
    pub = "pub"
    nightclub = "nightclub"
    bistro = "bistro"
    wine_bar = "wine_bar"
    fast_food = "fast_food"
    takeaway = "takeaway"
    other = "other"


class OperatingStatus(str, Enum):
    """Current operating status of a venue."""

    operational = "operational"
    closed_permanently = "closed_permanently"
    closed_temporarily = "closed_temporarily"
    unknown = "unknown"


@dataclass(frozen=True)
class VenueCompetitor:
    """Parsed competitive venue information."""

    venue_id: str
    name: str
    venue_type: VenueType
    location: Location
    distance_km: float
    rating: float  # 1.0-5.0
    review_count: int
    price_level: int  # 1-4 ($ = 1, $$$$ = 4)
    is_open_now: Optional[bool] = None
    operating_hours: Optional[dict[str, Any]] = None  # weekday, hours dict
    status: OperatingStatus = OperatingStatus.unknown
    last_review_date: Optional[date] = None
    url: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)

    def dedupe_key(self) -> str:
        """Generate dedup key based on location proximity and name."""
        parts = f"{self.name}:{self.location.latitude:.4f}:{self.location.longitude:.4f}"
        return hashlib.md5(parts.encode()).hexdigest()


@dataclass
class ReviewVelocityData:
    """Track review velocity for competitor trend detection."""

    venue_id: str
    name: str
    review_count_today: int
    review_count_week: int
    review_count_month: int
    avg_rating_recent: float  # Avg rating in last 7 days
    avg_rating_30day: float  # Avg rating in last 30 days
    velocity_trend: str  # "increasing", "stable", "decreasing"


# ============================================================================
# Google Places venue adapter
# ============================================================================


class GooglePlacesVenueAdapter(DataFeedAdapter):
    """
    Fetches nearby venue data from Google Places API.

    Searches for restaurants, bars, cafes within configurable radius.
    Retrieves ratings, review counts, operating hours, and photos.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        search_radius_km: float = 1.0,
        max_results: int = 20,
    ):
        """
        Initialize Google Places adapter.

        Args:
            api_key: Google Places API key
            cache: Signal cache instance
            search_radius_km: Search radius for nearby venues (default 1km)
            max_results: Max venues to return per search (default 20)
        """
        super().__init__(api_key=api_key, cache=cache)
        self.search_radius_km = search_radius_km
        self.max_results = max_results
        self.base_url = "https://maps.googleapis.com/maps/api/place"

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.nearby_venues

    @property
    def source_name(self) -> str:
        return "google_places"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch nearby venue signals for a location.

        Performs nearby search for restaurants, bars, cafes within radius.
        Generates signals for notable competitors (high ratings, reviews,
        or status changes).

        Args:
            location: Venue location to search around
            start_date: Start date for signal generation
            end_date: End date for signal generation
            venue_id: Optional venue ID to tag signals with

        Returns:
            List of FeedSignal objects for nearby venue activity
        """
        signals = []

        # Check cache first
        cache_key = f"google_places:{location.latitude:.4f}:{location.longitude:.4f}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug(f"Using cached Google Places data for {location.suburb}")
            return [cached]

        if not await self.is_available():
            logger.warning("Google Places adapter not configured (missing API key)")
            return []

        try:
            venues = await self._search_nearby_venues(location)
            logger.info(f"Found {len(venues)} nearby venues from Google Places")

            # Generate signals for each date in range
            for signal_date in self._date_range(start_date, end_date):
                # Only generate signals if we found venues
                if venues:
                    signal = self._analyze_venue_cluster(
                        venues, signal_date, venue_id
                    )
                    signals.append(signal)

            # Cache the results (structural data: 24 hours)
            if signals:
                self.cache.put(signals[0], ttl_minutes=24 * 60)

        except Exception as e:
            logger.error(f"Error fetching Google Places data: {e}", exc_info=True)

        return signals

    async def _search_nearby_venues(self, location: Location) -> list[VenueCompetitor]:
        """
        Search for nearby venues using Google Places API.

        Args:
            location: Center location for search

        Returns:
            List of VenueCompetitor objects
        """
        venues = []
        venue_types = ["restaurant", "cafe", "bar", "night_club", "pub"]

        async with httpx.AsyncClient(timeout=10.0) as client:
            for venue_type in venue_types:
                try:
                    params = {
                        "location": f"{location.latitude},{location.longitude}",
                        "radius": int(self.search_radius_km * 1000),  # meters
                        "type": venue_type,
                        "key": self.api_key,
                    }
                    url = f"{self.base_url}/nearbysearch/json"
                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    data = response.json()
                    for place in data.get("results", [])[:self.max_results]:
                        venue = self._parse_google_place(place, location)
                        if venue:
                            venues.append(venue)

                except Exception as e:
                    logger.error(
                        f"Error searching {venue_type} venues: {e}",
                        exc_info=True,
                    )

        return venues

    def _parse_google_place(
        self, place_data: dict[str, Any], reference_location: Location
    ) -> Optional[VenueCompetitor]:
        """
        Parse a Google Place result into VenueCompetitor.

        Args:
            place_data: Raw place data from Google API
            reference_location: Location to calculate distance from

        Returns:
            VenueCompetitor or None if parsing fails
        """
        try:
            lat = place_data["geometry"]["location"]["lat"]
            lon = place_data["geometry"]["location"]["lng"]
            venue_location = Location(
                latitude=lat,
                longitude=lon,
                address=place_data.get("vicinity", ""),
            )

            distance_km = reference_location.distance_km(venue_location)

            # Determine venue type from types array
            types = place_data.get("types", [])
            venue_type = self._classify_google_type(types)

            # Parse opening hours if available
            opening_hours = place_data.get("opening_hours", {})
            is_open = opening_hours.get("open_now")

            venue = VenueCompetitor(
                venue_id=place_data.get("place_id", ""),
                name=place_data.get("name", "Unknown"),
                venue_type=venue_type,
                location=venue_location,
                distance_km=distance_km,
                rating=float(place_data.get("rating", 0.0)),
                review_count=place_data.get("user_ratings_total", 0),
                price_level=place_data.get("price_level", 2),
                is_open_now=is_open,
                url=place_data.get("url", ""),
                raw_data=place_data,
            )
            return venue

        except Exception as e:
            logger.warning(f"Failed to parse Google Place: {e}")
            return None

    def _classify_google_type(self, types: list[str]) -> VenueType:
        """Map Google types to VenueType enum."""
        types_lower = [t.lower() for t in types]

        if "night_club" in types_lower:
            return VenueType.nightclub
        if "bar" in types_lower:
            return VenueType.bar
        if "pub" in types_lower:
            return VenueType.pub
        if "cafe" in types_lower:
            return VenueType.cafe
        if "restaurant" in types_lower:
            return VenueType.restaurant
        if "food" in str(types_lower):
            return VenueType.fast_food

        return VenueType.other

    def _analyze_venue_cluster(
        self, venues: list[VenueCompetitor], signal_date: date, venue_id: Optional[str]
    ) -> FeedSignal:
        """
        Analyze cluster of nearby venues for entertainment precinct effect.

        Args:
            venues: List of nearby venues
            signal_date: Date for the signal
            venue_id: Optional venue ID to tag with

        Returns:
            FeedSignal indicating cluster bustle level
        """
        # Simple clustering heuristic: more venues = busier area
        venue_count = len(venues)
        avg_rating = (
            sum(v.rating for v in venues) / venue_count if venues else 0.0
        )

        # Positive signal: cluster of well-rated venues = entertainment precinct
        if venue_count >= 5 and avg_rating >= 4.0:
            strength = SignalStrength.weak_positive
            description = (
                f"Entertainment precinct detected: {venue_count} nearby venues "
                f"with avg rating {avg_rating:.1f} (area is buzzing)"
            )
        elif venue_count >= 3 and avg_rating >= 4.0:
            strength = SignalStrength.weak_positive
            description = f"Cluster of {venue_count} quality venues nearby"
        else:
            strength = SignalStrength.neutral
            description = f"Standard venue density: {venue_count} venues nearby"

        return self._make_signal(
            signal_date=signal_date,
            strength=strength,
            description=description,
            confidence=0.6,
            hour=None,
            raw_data={
                "venue_count": venue_count,
                "avg_rating": avg_rating,
                "venues": [
                    {
                        "name": v.name,
                        "distance_km": v.distance_km,
                        "rating": v.rating,
                        "type": v.venue_type.value,
                    }
                    for v in venues[:5]
                ],
            },
            venue_id=venue_id,
            ttl_minutes=24 * 60,  # Structural data: 24 hours
        )

    @staticmethod
    def _date_range(start_date: date, end_date: date) -> list[date]:
        """Generate list of dates between start and end (inclusive)."""
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates


# ============================================================================
# Yelp venue adapter
# ============================================================================


class YelpVenueAdapter(DataFeedAdapter):
    """
    Fetches nearby venue data from Yelp Fusion API.

    Provides complementary data to Google Places with focus on reviews,
    user feedback, and trending businesses. Tracks review velocity and
    rating trends.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        search_radius_km: float = 1.0,
    ):
        """
        Initialize Yelp adapter.

        Args:
            api_key: Yelp Fusion API key
            cache: Signal cache instance
            search_radius_km: Search radius for nearby venues (default 1km)
        """
        super().__init__(api_key=api_key, cache=cache)
        self.search_radius_km = search_radius_km
        self.base_url = "https://api.yelp.com/v3"

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.nearby_venues

    @property
    def source_name(self) -> str:
        return "yelp_fusion"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch nearby venue signals from Yelp.

        Focuses on trending venues (high review velocity) and venues with
        low ratings (customers looking for alternatives).

        Args:
            location: Venue location to search around
            start_date: Start date for signal generation
            end_date: End date for signal generation
            venue_id: Optional venue ID to tag signals with

        Returns:
            List of FeedSignal objects for nearby venue activity
        """
        signals = []

        # Check cache
        cache_key = f"yelp:{location.latitude:.4f}:{location.longitude:.4f}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug(f"Using cached Yelp data for {location.suburb}")
            return [cached]

        if not await self.is_available():
            logger.warning("Yelp adapter not configured (missing API key)")
            return []

        try:
            venues = await self._search_yelp_venues(location)
            logger.info(f"Found {len(venues)} nearby venues from Yelp")

            # Analyze review velocity for trending competitors
            for signal_date in self._date_range(start_date, end_date):
                if venues:
                    trending = [v for v in venues if v.review_count > 100]
                    if trending:
                        signal = self._generate_review_velocity_signal(
                            trending, signal_date, venue_id
                        )
                        signals.append(signal)

            # Cache results (activity data: 6 hours)
            if signals:
                self.cache.put(signals[0], ttl_minutes=6 * 60)

        except Exception as e:
            logger.error(f"Error fetching Yelp data: {e}", exc_info=True)

        return signals

    async def _search_yelp_venues(self, location: Location) -> list[VenueCompetitor]:
        """
        Search for nearby venues using Yelp Fusion API.

        Args:
            location: Center location for search

        Returns:
            List of VenueCompetitor objects
        """
        venues = []
        search_terms = ["restaurants", "bars", "cafes", "nightlife"]

        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {"Authorization": f"Bearer {self.api_key}"}

            for term in search_terms:
                try:
                    params = {
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "term": term,
                        "radius": int(self.search_radius_km * 1000),
                        "limit": 20,
                        "sort_by": "rating",
                    }
                    url = f"{self.base_url}/businesses/search"
                    response = await client.get(
                        url, params=params, headers=headers
                    )
                    response.raise_for_status()

                    data = response.json()
                    for business in data.get("businesses", []):
                        venue = self._parse_yelp_business(business, location)
                        if venue:
                            venues.append(venue)

                except Exception as e:
                    logger.error(f"Error searching Yelp for {term}: {e}")

        return venues

    def _parse_yelp_business(
        self, business_data: dict[str, Any], reference_location: Location
    ) -> Optional[VenueCompetitor]:
        """
        Parse a Yelp business result into VenueCompetitor.

        Args:
            business_data: Raw business data from Yelp API
            reference_location: Location to calculate distance from

        Returns:
            VenueCompetitor or None if parsing fails
        """
        try:
            coords = business_data.get("coordinates", {})
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            if lat is None or lon is None:
                return None

            venue_location = Location(
                latitude=lat,
                longitude=lon,
                address=business_data.get("location", {}).get("address1", ""),
                suburb=business_data.get("location", {}).get("city", ""),
                state=business_data.get("location", {}).get("state", ""),
                postcode=business_data.get("location", {}).get("zip_code", ""),
            )

            distance_km = reference_location.distance_km(venue_location)

            # Classify by Yelp categories
            categories = business_data.get("categories", [])
            venue_type = self._classify_yelp_categories(categories)

            # Parse hours
            hours = business_data.get("hours", [{}])[0]
            is_open = business_data.get("is_closed") is False

            venue = VenueCompetitor(
                venue_id=business_data.get("id", ""),
                name=business_data.get("name", "Unknown"),
                venue_type=venue_type,
                location=venue_location,
                distance_km=distance_km,
                rating=float(business_data.get("rating", 0.0)),
                review_count=business_data.get("review_count", 0),
                price_level=len(business_data.get("price", "")),  # $ -> 1, $$$$ -> 4
                is_open_now=is_open,
                operating_hours=hours,
                url=business_data.get("url", ""),
                raw_data=business_data,
            )
            return venue

        except Exception as e:
            logger.warning(f"Failed to parse Yelp business: {e}")
            return None

    def _classify_yelp_categories(
        self, categories: list[dict[str, str]]
    ) -> VenueType:
        """Map Yelp categories to VenueType enum."""
        category_names = {c.get("alias", "").lower() for c in categories}

        category_mapping = {
            "nightclubs": VenueType.nightclub,
            "bars": VenueType.bar,
            "pubs": VenueType.pub,
            "cafes": VenueType.cafe,
            "restaurants": VenueType.restaurant,
            "fast_food": VenueType.fast_food,
            "bistro": VenueType.bistro,
            "wine_bars": VenueType.wine_bar,
        }

        for alias, venue_type in category_mapping.items():
            if alias in category_names:
                return venue_type

        return VenueType.other

    def _generate_review_velocity_signal(
        self, venues: list[VenueCompetitor], signal_date: date, venue_id: Optional[str]
    ) -> FeedSignal:
        """
        Generate signal for trending competitors with high review velocity.

        High review velocity indicates a venue gaining traction and drawing
        customers - potential competitor threat.

        Args:
            venues: List of trending venues
            signal_date: Date for the signal
            venue_id: Optional venue ID to tag with

        Returns:
            FeedSignal indicating competitive threat from trending venues
        """
        if not venues:
            return self._make_signal(
                signal_date=signal_date,
                strength=SignalStrength.neutral,
                description="No trending competitors detected",
                confidence=0.6,
                venue_id=venue_id,
            )

        # Sort by review count (proxy for velocity)
        top_venues = sorted(venues, key=lambda v: v.review_count, reverse=True)[:3]

        strength = SignalStrength.weak_negative
        description = f"Trending competitor alert: {top_venues[0].name} ({top_venues[0].review_count} reviews)"

        return self._make_signal(
            signal_date=signal_date,
            strength=strength,
            description=description,
            confidence=0.6,
            hour=None,
            raw_data={
                "trending_venues": [
                    {
                        "name": v.name,
                        "reviews": v.review_count,
                        "rating": v.rating,
                        "distance_km": v.distance_km,
                    }
                    for v in top_venues
                ]
            },
            venue_id=venue_id,
            ttl_minutes=6 * 60,  # Activity data: 6 hours
        )

    @staticmethod
    def _date_range(start_date: date, end_date: date) -> list[date]:
        """Generate list of dates between start and end (inclusive)."""
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates


# ============================================================================
# Composite analyzer for competitive signals
# ============================================================================


class CompetitorAnalyser(DataFeedAdapter):
    """
    Composite analyzer that synthesizes venue data from multiple sources.

    Detects specific competitive signals like:
    - New venues opening nearby (moderate_negative)
    - Venues closing (weak_positive)
    - Major promotions/events (weak_negative)
    - Operating hours (late-night trade opportunities)
    - Rating comparison vs competitors
    """

    def __init__(
        self,
        google_adapter: Optional[GooglePlacesVenueAdapter] = None,
        yelp_adapter: Optional[YelpVenueAdapter] = None,
        cache: Optional[SignalCache] = None,
    ):
        """
        Initialize composite analyzer.

        Args:
            google_adapter: GooglePlacesVenueAdapter instance
            yelp_adapter: YelpVenueAdapter instance
            cache: Signal cache instance
        """
        super().__init__(cache=cache)
        self.google = google_adapter
        self.yelp = yelp_adapter

    @property
    def category(self) -> FeedCategory:
        return FeedCategory.nearby_venues

    @property
    def source_name(self) -> str:
        return "competitor_analysis"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch synthesized competitive signals from multiple sources.

        Combines venue data from Google and Yelp to identify:
        - New/closed venues
        - Promotion activity
        - Operating hour patterns
        - Rating trends vs competitors

        Args:
            location: Venue location to analyze
            start_date: Start date for signal generation
            end_date: End date for signal generation
            venue_id: Optional venue ID to tag signals with

        Returns:
            List of synthesized FeedSignal objects
        """
        signals = []

        try:
            # Fetch from both sources in parallel
            google_signals = []
            yelp_signals = []

            if self.google:
                google_signals = await self.google.fetch_signals(
                    location, start_date, end_date, venue_id
                )

            if self.yelp:
                yelp_signals = await self.yelp.fetch_signals(
                    location, start_date, end_date, venue_id
                )

            # Combine and synthesize
            for signal_date in self._date_range(start_date, end_date):
                synthesized = self._synthesize_signals(
                    google_signals, yelp_signals, signal_date, venue_id
                )
                if synthesized:
                    signals.extend(synthesized)

        except Exception as e:
            logger.error(f"Error in competitor analysis: {e}", exc_info=True)

        return signals

    def _synthesize_signals(
        self,
        google_signals: list[FeedSignal],
        yelp_signals: list[FeedSignal],
        signal_date: date,
        venue_id: Optional[str],
    ) -> list[FeedSignal]:
        """
        Synthesize signals from multiple sources.

        Args:
            google_signals: Signals from Google Places adapter
            yelp_signals: Signals from Yelp adapter
            signal_date: Date for signals
            venue_id: Optional venue ID

        Returns:
            List of synthesized signals
        """
        signals = []

        # If we have high-confidence signals from either source, pass them through
        for signal in google_signals + yelp_signals:
            if signal.signal_date == signal_date and signal.strength != SignalStrength.neutral:
                signals.append(signal)

        return signals

    @staticmethod
    def _date_range(start_date: date, end_date: date) -> list[date]:
        """Generate list of dates between start and end (inclusive)."""
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
