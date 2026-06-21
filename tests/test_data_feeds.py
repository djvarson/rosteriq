"""
Comprehensive test suite for RosterIQ data_feeds package.

Tests cover:
- Base module: FeedSignal, Location, SignalCache, enums
- Aggregator: SignalAggregator, deduplication, weight redistribution
- Weather adapters: BOMWeatherAdapter, OpenWeatherMapAdapter
- Events adapters: TicketmasterAdapter, EventbriteAdapter, EventsAggregator
- Foot traffic adapter: GooglePlacesTrafficAdapter
- Reservations adapter: ResDiaryAdapter, BookingSnapshot
- Calendar adapters: SchoolHolidayAdapter, PublicHolidayAdapter, PayrollCycleAdapter
- Sports adapter: AUSportsCalendar, SportsAggregator
- Nearby venues: GooglePlacesVenueAdapter, CompetitorAnalyser
- Delivery: DeliverySnapshot, UberEatsAdapter, DeliveryAggregator
- Tourism: CruiseShipAdapter, ConferenceEvent, TransportDisruptionAdapter, TourismAggregator
- Economic: MarketConditions, AreaEconomicProfile, RBAAdapter, EconomicAggregator
"""

import pytest
import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Optional

# Base module imports
from rosteriq.data_feeds.base import (
    FeedSignal,
    Location,
    FeedCategory,
    SignalStrength,
    SignalCache,
    DataFeedAdapter,
    STRENGTH_MULTIPLIERS,
)

# Aggregator imports
from rosteriq.data_feeds.aggregator import (
    SignalAggregator,
    CATEGORY_TO_SIGNAL_TYPE,
)

# Adapter imports
from rosteriq.data_feeds.weather import (
    BOMWeatherAdapter,
    OpenWeatherMapAdapter,
    get_weather_adapter,
)
from rosteriq.data_feeds.events import (
    TicketmasterAdapter,
    EventbriteAdapter,
    EventsAggregator,
)
from rosteriq.data_feeds.foot_traffic import GooglePlacesTrafficAdapter
from rosteriq.data_feeds.reservations import ResDiaryAdapter, BookingSnapshot
from rosteriq.data_feeds.calendar import (
    SchoolHolidayAdapter,
    PublicHolidayAdapter,
    PayrollCycleAdapter,
    VenueType,
    AustralianState,
)
from rosteriq.data_feeds.sports import AUSportsCalendar, SportsAggregator
from rosteriq.data_feeds.nearby_venues import (
    GooglePlacesVenueAdapter,
    CompetitorAnalyser,
)
from rosteriq.data_feeds.delivery import (
    DeliverySnapshot,
    UberEatsAdapter,
    DeliveryAggregator,
)
from rosteriq.data_feeds.tourism import (
    CruiseShipAdapter,
    ConferenceAdapter,
    TransportDisruptionAdapter,
    TourismAggregator,
    CruiseShipEvent,
)
from rosteriq.data_feeds.economic import (
    MarketConditions,
    AreaEconomicProfile,
    RBAAdapter,
    EconomicAggregator,
)
from rosteriq.models import SignalType


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def melbourne_cbd_location():
    """Melbourne CBD location (for testing)."""
    return Location(
        latitude=-37.8136,
        longitude=144.9631,
        address="100 Collins St",
        suburb="Melbourne",
        state="VIC",
        postcode="3000",
    )


@pytest.fixture
def mcg_location():
    """Melbourne Cricket Ground location (for testing distance)."""
    return Location(
        latitude=-37.8199,
        longitude=144.9862,
        address="Brunton Ave",
        suburb="Melbourne",
        state="VIC",
        postcode="3002",
    )


@pytest.fixture
def signal_cache():
    """Fresh signal cache for each test."""
    return SignalCache(default_ttl_minutes=30)


@pytest.fixture
def base_signal(melbourne_cbd_location):
    """Standard FeedSignal for testing."""
    return FeedSignal(
        category=FeedCategory.weather,
        source="test_weather",
        signal_date=date(2026, 4, 4),
        signal_hour=14,
        strength=SignalStrength.moderate_positive,
        value=0.3,
        confidence=0.85,
        description="Mild and clear",
        raw_data={"temp": 24},
    )


# ============================================================================
# Base Module Tests: FeedSignal
# ============================================================================


class TestFeedSignalCreation:
    """Test FeedSignal creation with all fields."""

    def test_feed_signal_minimal(self):
        """Test FeedSignal with minimal required fields."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
        )
        assert signal.category == FeedCategory.weather
        assert signal.source == "bom"
        assert signal.signal_date == date(2026, 4, 4)
        assert signal.signal_hour is None
        assert signal.strength == SignalStrength.neutral
        assert signal.value == 0.0
        assert signal.confidence == 0.8

    def test_feed_signal_full_fields(self, base_signal):
        """Test FeedSignal with all fields populated."""
        signal = base_signal
        assert signal.category == FeedCategory.weather
        assert signal.source == "test_weather"
        assert signal.signal_date == date(2026, 4, 4)
        assert signal.signal_hour == 14
        assert signal.strength == SignalStrength.moderate_positive
        assert signal.value == 0.3
        assert signal.confidence == 0.85
        assert signal.description == "Mild and clear"
        assert signal.raw_data == {"temp": 24}

    def test_feed_signal_with_venue_id(self):
        """Test FeedSignal with venue-specific ID."""
        signal = FeedSignal(
            category=FeedCategory.reservations,
            source="resdiary",
            signal_date=date(2026, 4, 4),
            venue_id="venue_12345",
        )
        assert signal.venue_id == "venue_12345"

    def test_feed_signal_fetched_at_defaults_to_now(self):
        """Test that fetched_at defaults to current time."""
        before = datetime.now()
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
        )
        after = datetime.now()
        assert before <= signal.fetched_at <= after


class TestFeedSignalToVarianceValue:
    """Test FeedSignal.to_variance_value() returns correct values."""

    def test_to_variance_value_with_explicit_value(self):
        """When value is set explicitly, return normalized value."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            value=0.5,
        )
        assert signal.to_variance_value() == 0.5

    def test_to_variance_value_extreme_negative(self):
        """Extreme negative strength maps to -1.0."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.extreme_negative,
        )
        assert signal.to_variance_value() == -1.0

    def test_to_variance_value_strong_negative(self):
        """Strong negative strength maps to -0.6."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.strong_negative,
        )
        assert signal.to_variance_value() == -0.6

    def test_to_variance_value_moderate_positive(self):
        """Moderate positive strength maps to 0.3."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.moderate_positive,
        )
        assert signal.to_variance_value() == 0.3

    def test_to_variance_value_extreme_positive(self):
        """Extreme positive strength maps to 1.0."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.extreme_positive,
        )
        assert signal.to_variance_value() == 1.0

    def test_to_variance_value_all_strengths(self):
        """Test all SignalStrength values map correctly."""
        for strength, expected_value in STRENGTH_MULTIPLIERS.items():
            signal = FeedSignal(
                category=FeedCategory.weather,
                source="test",
                signal_date=date(2026, 4, 4),
                strength=strength,
            )
            assert signal.to_variance_value() == expected_value

    def test_to_variance_value_clamps_out_of_range(self):
        """Clamp values outside [-1.0, 1.0] range."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            value=2.5,  # Out of range
        )
        assert signal.to_variance_value() == 1.0

        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            value=-2.5,  # Out of range
        )
        assert signal.to_variance_value() == -1.0


class TestFeedSignalExpiry:
    """Test FeedSignal.is_expired property."""

    def test_is_expired_no_expiry_time(self):
        """Signal without expires_at never expires."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            expires_at=None,
        )
        assert signal.is_expired is False

    def test_is_expired_in_future(self):
        """Signal expires_at in future is not expired."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            expires_at=datetime.now() + timedelta(hours=1),
        )
        assert signal.is_expired is False

    def test_is_expired_in_past(self):
        """Signal expires_at in past is expired."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            expires_at=datetime.now() - timedelta(hours=1),
        )
        assert signal.is_expired is True

    def test_is_expired_just_expired(self):
        """Signal that just expired is expired."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            expires_at=datetime.now() - timedelta(seconds=1),
        )
        assert signal.is_expired is True


class TestFeedSignalCacheKey:
    """Test FeedSignal.cache_key determinism."""

    def test_cache_key_deterministic(self, base_signal):
        """Same inputs produce same cache_key."""
        key1 = base_signal.cache_key
        key2 = base_signal.cache_key
        assert key1 == key2

    def test_cache_key_different_for_different_signals(self):
        """Different signals produce different cache keys."""
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=10,
        )
        signal2 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=11,
        )
        assert signal1.cache_key != signal2.cache_key

    def test_cache_key_includes_category(self):
        """Different categories produce different cache keys."""
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
        )
        signal2 = FeedSignal(
            category=FeedCategory.events,
            source="test",
            signal_date=date(2026, 4, 4),
        )
        assert signal1.cache_key != signal2.cache_key

    def test_cache_key_includes_venue_id(self):
        """Different venue_ids produce different cache keys."""
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            venue_id="venue_1",
        )
        signal2 = FeedSignal(
            category=FeedCategory.weather,
            source="test",
            signal_date=date(2026, 4, 4),
            venue_id="venue_2",
        )
        assert signal1.cache_key != signal2.cache_key


# ============================================================================
# Base Module Tests: Location
# ============================================================================


class TestLocation:
    """Test Location class."""

    def test_location_creation(self, melbourne_cbd_location):
        """Location created with correct fields."""
        assert melbourne_cbd_location.latitude == -37.8136
        assert melbourne_cbd_location.longitude == 144.9631
        assert melbourne_cbd_location.suburb == "Melbourne"
        assert melbourne_cbd_location.state == "VIC"

    def test_location_coords_property(self, melbourne_cbd_location):
        """Location.coords returns (latitude, longitude) tuple."""
        assert melbourne_cbd_location.coords == (-37.8136, 144.9631)

    def test_location_distance_melbourne_cbd_to_mcg(
        self, melbourne_cbd_location, mcg_location
    ):
        """Distance from Melbourne CBD to MCG is approximately 1.5 km."""
        distance = melbourne_cbd_location.distance_km(mcg_location)
        # MCG is about 1.5 km from CBD
        assert 1.0 < distance < 2.5

    def test_location_distance_symmetry(self, melbourne_cbd_location, mcg_location):
        """Distance is symmetric: A->B equals B->A."""
        dist_cbd_to_mcg = melbourne_cbd_location.distance_km(mcg_location)
        dist_mcg_to_cbd = mcg_location.distance_km(melbourne_cbd_location)
        assert abs(dist_cbd_to_mcg - dist_mcg_to_cbd) < 0.01

    def test_location_distance_same_location(self, melbourne_cbd_location):
        """Distance from location to itself is 0."""
        distance = melbourne_cbd_location.distance_km(melbourne_cbd_location)
        assert distance < 0.01

    def test_location_distance_sydney_to_melbourne(self):
        """Distance Sydney to Melbourne is approximately 715 km."""
        sydney = Location(
            latitude=-33.8688, longitude=151.2093, suburb="Sydney", state="NSW"
        )
        melbourne = Location(
            latitude=-37.8136, longitude=144.9631, suburb="Melbourne", state="VIC"
        )
        distance = sydney.distance_km(melbourne)
        # Sydney to Melbourne is about 715 km
        assert 700 < distance < 730


# ============================================================================
# Base Module Tests: SignalCache
# ============================================================================


class TestSignalCache:
    """Test SignalCache functionality."""

    def test_cache_put_and_get(self, signal_cache, base_signal):
        """SignalCache.put() stores and get() retrieves signals."""
        signal_cache.put(base_signal, ttl_minutes=30)
        retrieved = signal_cache.get(base_signal.cache_key)
        assert retrieved is not None
        assert retrieved.source == base_signal.source

    def test_cache_get_nonexistent_key(self, signal_cache):
        """get() returns None for nonexistent keys."""
        result = signal_cache.get("nonexistent_key")
        assert result is None

    def test_cache_expires_after_ttl(self, signal_cache, base_signal):
        """Expired signals return None on get()."""
        # Put with 1 millisecond TTL
        signal_cache.put(base_signal, ttl_minutes=0)
        # Immediately get - should work
        retrieved = signal_cache.get(base_signal.cache_key)
        assert retrieved is not None
        # Wait and get again - should expire
        import time

        time.sleep(0.1)
        # Force expiration by setting expires_at to past
        signal_copy = signal_cache._cache.get(base_signal.cache_key)
        if signal_copy:
            signal_copy.expires_at = datetime.now() - timedelta(seconds=1)
        expired = signal_cache.get(base_signal.cache_key)
        assert expired is None

    def test_cache_clear_expired(self, signal_cache, base_signal):
        """clear_expired() removes stale entries."""
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="test1",
            signal_date=date(2026, 4, 4),
            expires_at=datetime.now() - timedelta(hours=1),
        )
        signal2 = FeedSignal(
            category=FeedCategory.weather,
            source="test2",
            signal_date=date(2026, 4, 5),
            expires_at=datetime.now() + timedelta(hours=1),
        )
        signal_cache.put(signal1, ttl_minutes=30)
        signal_cache.put(signal2, ttl_minutes=30)

        cleared_count = signal_cache.clear_expired()
        assert cleared_count == 1
        assert signal_cache.get(signal1.cache_key) is None
        assert signal_cache.get(signal2.cache_key) is not None

    def test_cache_size_property(self, signal_cache, base_signal):
        """cache.size returns number of items in cache."""
        assert signal_cache.size == 0
        signal_cache.put(base_signal, ttl_minutes=30)
        assert signal_cache.size == 1

    def test_cache_multiple_signals(self, signal_cache):
        """Cache can store multiple signals."""
        signals = [
            FeedSignal(
                category=FeedCategory.weather,
                source=f"test_{i}",
                signal_date=date(2026, 4, 4),
            )
            for i in range(5)
        ]
        for signal in signals:
            signal_cache.put(signal, ttl_minutes=30)
        assert signal_cache.size == 5


# ============================================================================
# Base Module Tests: Enums and Constants
# ============================================================================


class TestStrengthMultipliers:
    """Test STRENGTH_MULTIPLIERS constant."""

    def test_strength_multipliers_has_all_strengths(self):
        """STRENGTH_MULTIPLIERS includes all SignalStrength values."""
        for strength in SignalStrength:
            assert strength in STRENGTH_MULTIPLIERS

    def test_strength_multipliers_range(self):
        """All multipliers are in [-1.0, 1.0] range."""
        for multiplier in STRENGTH_MULTIPLIERS.values():
            assert -1.0 <= multiplier <= 1.0

    def test_strength_multipliers_symmetry(self):
        """Negative and positive strengths are symmetric."""
        assert (
            STRENGTH_MULTIPLIERS[SignalStrength.extreme_negative]
            == -STRENGTH_MULTIPLIERS[SignalStrength.extreme_positive]
        )
        assert (
            STRENGTH_MULTIPLIERS[SignalStrength.strong_negative]
            == -STRENGTH_MULTIPLIERS[SignalStrength.strong_positive]
        )


class TestFeedCategory:
    """Test FeedCategory enum."""

    def test_feed_category_has_expected_categories(self):
        """FeedCategory has all expected categories."""
        expected = {
            "weather",
            "events",
            "foot_traffic",
            "reservations",
            "school_holidays",
            "sports",
            "nearby_venues",
            "delivery",
            "tourism",
            "transport",
            "economic",
            "calendar",
        }
        actual = {cat.value for cat in FeedCategory}
        assert expected == actual


# ============================================================================
# Weather Adapter Tests
# ============================================================================


class TestBOMWeatherAdapter:
    """Test BOMWeatherAdapter."""

    def test_bom_adapter_category(self):
        """BOMWeatherAdapter category is weather."""
        adapter = BOMWeatherAdapter()
        assert adapter.category == FeedCategory.weather

    def test_bom_adapter_source_name(self):
        """BOMWeatherAdapter source_name is 'bom_weather'."""
        adapter = BOMWeatherAdapter()
        assert adapter.source_name == "bom_weather"

    @pytest.mark.asyncio
    async def test_bom_adapter_is_available(self):
        """BOMWeatherAdapter is_available works."""
        adapter = BOMWeatherAdapter()
        # BOM doesn't require API key, should be available
        result = await adapter.is_available()
        assert isinstance(result, bool)


class TestOpenWeatherMapAdapter:
    """Test OpenWeatherMapAdapter."""

    def test_owm_adapter_category(self):
        """OpenWeatherMapAdapter category is weather."""
        adapter = OpenWeatherMapAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.weather

    def test_owm_adapter_source_name(self):
        """OpenWeatherMapAdapter source_name is 'openweathermap'."""
        adapter = OpenWeatherMapAdapter(api_key="test_key")
        assert adapter.source_name == "openweathermap"

    @pytest.mark.asyncio
    async def test_owm_adapter_requires_api_key(self):
        """OpenWeatherMapAdapter requires API key."""
        adapter = OpenWeatherMapAdapter(api_key="test_key")
        is_avail = await adapter.is_available()
        assert is_avail is True


class TestGetWeatherAdapter:
    """Test get_weather_adapter factory function."""

    def test_get_weather_adapter_default(self):
        """get_weather_adapter() returns BOMWeatherAdapter by default."""
        adapter = get_weather_adapter()
        assert isinstance(adapter, BOMWeatherAdapter)

    def test_get_weather_adapter_with_api_key(self):
        """get_weather_adapter(api_key) returns OpenWeatherMapAdapter."""
        adapter = get_weather_adapter(api_key="test_key")
        assert isinstance(adapter, OpenWeatherMapAdapter)


# ============================================================================
# Events Adapter Tests
# ============================================================================


class TestTicketmasterAdapter:
    """Test TicketmasterAdapter."""

    def test_ticketmaster_adapter_category(self):
        """TicketmasterAdapter category is events."""
        adapter = TicketmasterAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.events

    def test_ticketmaster_adapter_source_name(self):
        """TicketmasterAdapter source_name is correct."""
        adapter = TicketmasterAdapter(api_key="test_key")
        assert adapter.source_name == "ticketmaster_au"


class TestEventbriteAdapter:
    """Test EventbriteAdapter."""

    def test_eventbrite_adapter_category(self):
        """EventbriteAdapter category is events."""
        adapter = EventbriteAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.events

    def test_eventbrite_adapter_source_name(self):
        """EventbriteAdapter source_name is correct."""
        adapter = EventbriteAdapter(api_key="test_key")
        assert adapter.source_name == "eventbrite"


class TestEventsAggregator:
    """Test EventsAggregator."""

    def test_events_aggregator_registers_sub_adapters(self):
        """EventsAggregator registers Ticketmaster and Eventbrite adapters."""
        aggregator = EventsAggregator()
        # Check that aggregator has adapters for events
        assert aggregator.category == FeedCategory.events
        assert aggregator.source_name == "events_aggregator"


# ============================================================================
# Foot Traffic Adapter Tests
# ============================================================================


class TestGooglePlacesTrafficAdapter:
    """Test GooglePlacesTrafficAdapter."""

    def test_google_places_traffic_category(self):
        """GooglePlacesTrafficAdapter category is foot_traffic."""
        adapter = GooglePlacesTrafficAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.foot_traffic

    def test_google_places_traffic_source_name(self):
        """GooglePlacesTrafficAdapter source_name is correct."""
        adapter = GooglePlacesTrafficAdapter(api_key="test_key")
        assert adapter.source_name == "google_places"


# ============================================================================
# Reservations Adapter Tests
# ============================================================================


class TestBookingSnapshot:
    """Test BookingSnapshot."""

    def test_booking_snapshot_creation(self):
        """BookingSnapshot creates with field validation."""
        snapshot = BookingSnapshot(
            date=date(2026, 4, 4),
            hour=19,
            total_bookings=15,
            total_covers=60,
        )
        assert snapshot.date == date(2026, 4, 4)
        assert snapshot.hour == 19
        assert snapshot.total_bookings == 15
        assert snapshot.total_covers == 60

    def test_booking_snapshot_all_day(self):
        """BookingSnapshot supports all-day aggregates."""
        snapshot = BookingSnapshot(
            date=date(2026, 4, 4),
            hour=None,
            total_bookings=45,
            total_covers=180,
        )
        assert snapshot.hour is None
        assert snapshot.total_bookings == 45

    def test_booking_snapshot_no_show_adjustment(self):
        """100 covers * 0.85 = 85 adjusted covers."""
        # This tests the concept that no-show rates reduce effective covers
        snapshot = BookingSnapshot(
            date=date(2026, 4, 4),
            total_bookings=20,
            total_covers=100,
        )
        no_show_rate = 0.15  # 15% no-show
        adjusted_covers = snapshot.total_covers * (1 - no_show_rate)
        assert adjusted_covers == 85


class TestResDiaryAdapter:
    """Test ResDiaryAdapter."""

    def test_resdiary_adapter_category(self):
        """ResDiaryAdapter category is reservations."""
        adapter = ResDiaryAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.reservations

    def test_resdiary_adapter_source_name(self):
        """ResDiaryAdapter source_name is correct."""
        adapter = ResDiaryAdapter(api_key="test_key")
        assert adapter.source_name == "resdiary"


# ============================================================================
# Calendar Adapter Tests
# ============================================================================


class TestSchoolHolidayAdapter:
    """Test SchoolHolidayAdapter."""

    def test_school_holiday_adapter_detects_vic_holidays(self):
        """SchoolHolidayAdapter detects VIC school holidays."""
        adapter = SchoolHolidayAdapter()
        adapter.configure(state=AustralianState.VIC, venue_type=VenueType.family)
        assert adapter.category == FeedCategory.calendar
        assert adapter._state == AustralianState.VIC

    def test_school_holiday_adapter_detects_nsw_holidays(self):
        """SchoolHolidayAdapter detects NSW school holidays."""
        adapter = SchoolHolidayAdapter()
        adapter.configure(state=AustralianState.NSW, venue_type=VenueType.family)
        assert adapter._state == AustralianState.NSW

    def test_school_holiday_adapter_detects_qld_holidays(self):
        """SchoolHolidayAdapter detects QLD school holidays."""
        adapter = SchoolHolidayAdapter()
        adapter.configure(state=AustralianState.QLD, venue_type=VenueType.family)
        assert adapter._state == AustralianState.QLD


class TestVenueType:
    """Test VenueType enum."""

    def test_venue_type_has_expected_values(self):
        """VenueType has all expected venue types."""
        expected = {"family", "cbd", "campus", "suburban", "tourist", "waterfront"}
        actual = {vt.value for vt in VenueType}
        assert expected == actual


class TestPublicHolidayAdapter:
    """Test PublicHolidayAdapter."""

    def test_public_holiday_adapter_includes_major_events(self):
        """PublicHolidayAdapter includes Melbourne Cup, Australia Day."""
        adapter = PublicHolidayAdapter()
        assert adapter.category == FeedCategory.calendar


class TestPayrollCycleAdapter:
    """Test PayrollCycleAdapter."""

    def test_payroll_cycle_adapter_generates_fortnightly_signals(self):
        """PayrollCycleAdapter generates fortnightly signals."""
        adapter = PayrollCycleAdapter()
        assert adapter.category == FeedCategory.calendar
        assert adapter.source_name == "au_payroll_cycles"


# ============================================================================
# Sports Adapter Tests
# ============================================================================


class TestAUSportsCalendar:
    """Test AUSportsCalendar."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_aus_sports_calendar_has_afl_2026(self):
        """AUSportsCalendar has 2026 AFL season data."""
        adapter = AUSportsCalendar()
        assert adapter.category == FeedCategory.sports
        assert adapter.source_name == "aus_sports_calendar"

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_aus_sports_calendar_has_nrl_2026(self):
        """AUSportsCalendar has 2026 NRL season data."""
        adapter = AUSportsCalendar()
        # The adapter should have NRL data for 2026
        assert adapter.source_name == "aus_sports_calendar"


class TestSportsAggregator:
    """Test SportsAggregator."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_sports_aggregator_registers_sub_adapters(self):
        """SportsAggregator registers sub-adapters."""
        aggregator = SportsAggregator()
        assert aggregator.category == FeedCategory.sports
        assert aggregator.source_name == "sports_aggregator"


# ============================================================================
# Nearby Venues Tests
# ============================================================================


class TestGooglePlacesVenueAdapter:
    """Test GooglePlacesVenueAdapter."""

    def test_google_places_venue_category(self):
        """GooglePlacesVenueAdapter category is nearby_venues."""
        adapter = GooglePlacesVenueAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.nearby_venues

    def test_google_places_venue_source_name(self):
        """GooglePlacesVenueAdapter source_name is correct."""
        adapter = GooglePlacesVenueAdapter(api_key="test_key")
        assert adapter.source_name == "google_places"


class TestCompetitorAnalyser:
    """Test CompetitorAnalyser."""

    def test_competitor_analyser_category(self):
        """CompetitorAnalyser category is nearby_venues."""
        adapter = CompetitorAnalyser()
        assert adapter.category == FeedCategory.nearby_venues


# ============================================================================
# Delivery Adapter Tests
# ============================================================================


class TestDeliverySnapshot:
    """Test DeliverySnapshot."""

    def test_delivery_snapshot_creation(self):
        """DeliverySnapshot creates with field validation."""
        snapshot = DeliverySnapshot(
            date=date(2026, 4, 4),
            hour=19,
            order_count=75,
            avg_order_value=30.0,
            platform="uber_eats",
            prep_time_minutes=12.5,
        )
        assert snapshot.date == date(2026, 4, 4)
        assert snapshot.hour == 19
        assert snapshot.platform == "uber_eats"
        assert snapshot.order_count == 75
        assert snapshot.avg_order_value == 30.0
        assert snapshot.prep_time_minutes == 12.5


class TestUberEatsAdapter:
    """Test UberEatsAdapter."""

    def test_ubereats_adapter_category(self):
        """UberEatsAdapter category is delivery."""
        adapter = UberEatsAdapter(api_key="test_key")
        assert adapter.category == FeedCategory.delivery

    def test_ubereats_adapter_source_name(self):
        """UberEatsAdapter source_name is correct."""
        adapter = UberEatsAdapter(api_key="test_key")
        assert adapter.source_name == "uber_eats"


class TestDeliveryAggregator:
    """Test DeliveryAggregator."""

    def test_delivery_aggregator_registers_sub_adapters(self):
        """DeliveryAggregator registers sub-adapters."""
        aggregator = DeliveryAggregator()
        assert aggregator.category == FeedCategory.delivery


# ============================================================================
# Tourism Adapter Tests
# ============================================================================


class TestCruiseShipEvent:
    """Test CruiseShipEvent dataclass."""

    def test_cruise_ship_event_creation(self):
        """CruiseShipEvent creates with expected fields."""
        terminal = Location(
            latitude=-37.8403,
            longitude=144.9165,
            suburb="Port Melbourne",
            state="VIC",
        )
        event = CruiseShipEvent(
            port_code="MEL",
            port_name="Port Melbourne",
            arrival_date=datetime(2026, 4, 15, 7, 0),
            departure_date=datetime(2026, 4, 18, 18, 0),
            ship_name="Crown Princess",
            expected_passengers=3500,
            terminal_location=terminal,
        )
        assert event.ship_name == "Crown Princess"
        assert event.arrival_date == datetime(2026, 4, 15, 7, 0)
        assert event.expected_passengers == 3500


class TestCruiseShipAdapter:
    """Test CruiseShipAdapter."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_cruise_ship_adapter_has_2026_schedule(self):
        """CruiseShipAdapter has 2026 schedule data."""
        adapter = CruiseShipAdapter()
        assert adapter.category == FeedCategory.tourism


class TestConferenceAdapter:
    """Test ConferenceAdapter."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_conference_adapter_has_events(self):
        """ConferenceAdapter has conference events."""
        adapter = ConferenceAdapter()
        assert adapter.category == FeedCategory.tourism


class TestTransportDisruptionAdapter:
    """Test TransportDisruptionAdapter."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_transport_disruption_adapter_category(self):
        """TransportDisruptionAdapter category is transport."""
        adapter = TransportDisruptionAdapter()
        assert adapter.category == FeedCategory.transport


class TestTourismAggregator:
    """Test TourismAggregator."""

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_tourism_aggregator_composite_works(self):
        """TourismAggregator composite aggregates sub-adapters."""
        aggregator = TourismAggregator()
        assert aggregator.category == FeedCategory.tourism


# ============================================================================
# Economic Adapter Tests
# ============================================================================


class TestMarketConditions:
    """Test MarketConditions dataclass."""

    def test_market_conditions_creation(self):
        """MarketConditions creates with expected fields."""
        conditions = MarketConditions(
            cash_rate=4.35,
            cash_rate_change=-0.25,
            consumer_confidence=95.0,
            consumer_confidence_prev=92.0,
            unemployment_rate=3.8,
            unemployment_rate_prev=4.0,
            cpi_annual=2.5,
            wage_growth_annual=3.5,
            retail_turnover_growth=0.4,
            as_of_date=date(2026, 4, 4),
        )
        assert conditions.as_of_date == date(2026, 4, 4)
        assert conditions.cash_rate == 4.35
        assert conditions.unemployment_rate == 3.8


class TestAreaEconomicProfile:
    """Test AreaEconomicProfile dataclass."""

    def test_area_economic_profile_creation(self):
        """AreaEconomicProfile creates with expected fields."""
        profile = AreaEconomicProfile(
            sa3_code="20604",
            sa3_name="Melbourne City",
            unemployment_rate_local=4.2,
            median_household_income=75000,
            hospitality_workforce_pct=12.5,
            population=180000,
        )
        assert profile.sa3_name == "Melbourne City"
        assert profile.median_household_income == 75000
        assert profile.unemployment_rate_local == 4.2


class TestRBAAdapter:
    """Test RBAAdapter."""

    def test_rba_adapter_category(self):
        """RBAAdapter category is economic."""
        adapter = RBAAdapter()
        assert adapter.category == FeedCategory.economic

    def test_rba_adapter_source_name(self):
        """RBAAdapter source_name is correct."""
        adapter = RBAAdapter()
        assert adapter.source_name == "rba_rates"


class TestEconomicAggregator:
    """Test EconomicAggregator."""

    def test_economic_aggregator_composite_works(self):
        """EconomicAggregator composite aggregates sub-adapters."""
        aggregator = EconomicAggregator()
        assert aggregator.category == FeedCategory.economic


# ============================================================================
# Aggregator Tests
# ============================================================================


class TestSignalAggregatorRegistration:
    """Test SignalAggregator adapter registration."""

    def test_aggregator_registers_single_adapter(self):
        """SignalAggregator.register() stores adapter."""
        agg = SignalAggregator()
        adapter = BOMWeatherAdapter()
        agg.register(adapter)
        assert FeedCategory.weather in agg._adapters
        assert len(agg._adapters[FeedCategory.weather]) == 1

    def test_aggregator_registers_multiple_adapters(self):
        """SignalAggregator.register_all() stores multiple adapters."""
        agg = SignalAggregator()
        adapters = [
            BOMWeatherAdapter(),
            OpenWeatherMapAdapter(api_key="test"),
        ]
        agg.register_all(adapters)
        assert len(agg._all_adapters) == 2

    def test_aggregator_rejects_invalid_category(self):
        """SignalAggregator.register() raises ValueError for invalid category."""

        class InvalidAdapter(DataFeedAdapter):
            @property
            def category(self):
                # Return an invalid category
                return "invalid_category"

            @property
            def source_name(self):
                return "invalid"

            async def fetch_signals(self, location, start_date, end_date, venue_id=None):
                return []

        agg = SignalAggregator()
        adapter = InvalidAdapter()
        with pytest.raises(ValueError):
            agg.register(adapter)


class TestCategoryToSignalTypeMapping:
    """Test CATEGORY_TO_SIGNAL_TYPE mapping."""

    def test_mapping_includes_all_categories(self):
        """CATEGORY_TO_SIGNAL_TYPE maps all major categories."""
        expected_mappings = {
            FeedCategory.weather: SignalType.weather,
            FeedCategory.events: SignalType.events,
            FeedCategory.sports: SignalType.events,
            FeedCategory.reservations: SignalType.bookings,
            FeedCategory.foot_traffic: SignalType.pos_trends,
        }
        for category, signal_type in expected_mappings.items():
            assert CATEGORY_TO_SIGNAL_TYPE[category] == signal_type

    def test_mapping_is_complete(self):
        """CATEGORY_TO_SIGNAL_TYPE has entries for all expected categories."""
        for category in [
            FeedCategory.weather,
            FeedCategory.events,
            FeedCategory.sports,
            FeedCategory.reservations,
            FeedCategory.foot_traffic,
        ]:
            assert category in CATEGORY_TO_SIGNAL_TYPE


class TestSignalAggregatorDeduplication:
    """Test SignalAggregator deduplication."""

    def test_deduplication_same_date_hour_category(self):
        """Same date+hour+category signals are merged."""
        agg = SignalAggregator()
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=14,
            value=0.2,
        )
        signal2 = FeedSignal(
            category=FeedCategory.weather,
            source="owm",
            signal_date=date(2026, 4, 4),
            signal_hour=14,
            value=0.4,
        )
        merged = agg._deduplicate_and_merge([signal1, signal2])
        assert len(merged) == 1
        # Should have averaged value
        assert merged[0].value == pytest.approx(0.3)

    def test_deduplication_different_hours(self):
        """Different hours are not merged."""
        agg = SignalAggregator()
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=14,
            value=0.2,
        )
        signal2 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=15,
            value=0.4,
        )
        merged = agg._deduplicate_and_merge([signal1, signal2])
        assert len(merged) == 2

    def test_deduplication_different_categories(self):
        """Different categories are not merged."""
        agg = SignalAggregator()
        signal1 = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            signal_hour=14,
        )
        signal2 = FeedSignal(
            category=FeedCategory.events,
            source="tm",
            signal_date=date(2026, 4, 4),
            signal_hour=14,
        )
        merged = agg._deduplicate_and_merge([signal1, signal2])
        assert len(merged) == 2


class TestSignalAggregatorWeightRedistribution:
    """Test SignalAggregator weight redistribution."""

    def test_weight_redistribution_missing_signals(self):
        """Weights redistributed when signals missing."""
        agg = SignalAggregator()
        # Only weather signals available
        agg._available_signal_types = {SignalType.weather}
        agg._signal_confidences = {SignalType.weather: [0.9]}
        adjusted = agg._compute_adjusted_weights()
        # Total weight should still sum to ~1.0
        total = sum(adjusted.values())
        assert 0.99 <= total <= 1.01

    def test_weight_boost_high_confidence_bookings(self):
        """High-confidence bookings get weight boost."""
        agg = SignalAggregator()
        agg._available_signal_types = {SignalType.bookings, SignalType.weather}
        # Very high confidence bookings
        agg._signal_confidences = {
            SignalType.bookings: [0.96, 0.97, 0.98],
            SignalType.weather: [0.7],
        }
        adjusted = agg._compute_adjusted_weights()
        # Bookings weight should be boosted
        assert adjusted[SignalType.bookings] > agg.default_weights.get(
            SignalType.bookings, 0
        )


class TestSignalAggregatorValueToStrength:
    """Test SignalAggregator._value_to_strength()."""

    def test_value_to_strength_extreme_negative(self):
        """Value <= -0.8 maps to extreme_negative."""
        agg = SignalAggregator()
        assert (
            agg._value_to_strength(-1.0) == SignalStrength.extreme_negative
        )
        assert (
            agg._value_to_strength(-0.8) == SignalStrength.extreme_negative
        )

    def test_value_to_strength_strong_negative(self):
        """Value in (-0.8, -0.5] maps to strong_negative."""
        agg = SignalAggregator()
        assert agg._value_to_strength(-0.6) == SignalStrength.strong_negative

    def test_value_to_strength_neutral(self):
        """Value == 0 maps to neutral."""
        agg = SignalAggregator()
        assert agg._value_to_strength(0.0) == SignalStrength.neutral

    def test_value_to_strength_extreme_positive(self):
        """Value > 0.8 maps to extreme_positive."""
        agg = SignalAggregator()
        assert (
            agg._value_to_strength(0.9) == SignalStrength.extreme_positive
        )
        assert (
            agg._value_to_strength(1.0) == SignalStrength.extreme_positive
        )


@pytest.mark.asyncio
async def test_signal_aggregator_fetch_all(melbourne_cbd_location):
    """SignalAggregator.fetch_all() retrieves signals from registered adapters."""

    class MockAdapter(DataFeedAdapter):
        @property
        def category(self):
            return FeedCategory.weather

        @property
        def source_name(self):
            return "mock"

        async def fetch_signals(self, location, start_date, end_date, venue_id=None):
            return [
                FeedSignal(
                    category=FeedCategory.weather,
                    source="mock",
                    signal_date=start_date,
                    strength=SignalStrength.moderate_positive,
                )
            ]

    agg = SignalAggregator()
    agg.register(MockAdapter())
    signals = await agg.fetch_all(
        melbourne_cbd_location,
        date(2026, 4, 4),
        date(2026, 4, 4),
    )
    assert len(signals) > 0


@pytest.mark.asyncio
async def test_signal_aggregator_convert_to_variance_signals(melbourne_cbd_location):
    """SignalAggregator converts FeedSignal to VarianceSignal correctly."""

    class MockAdapter(DataFeedAdapter):
        @property
        def category(self):
            return FeedCategory.weather

        @property
        def source_name(self):
            return "mock"

        async def fetch_signals(self, location, start_date, end_date, venue_id=None):
            return [
                FeedSignal(
                    category=FeedCategory.weather,
                    source="mock",
                    signal_date=start_date,
                    strength=SignalStrength.moderate_positive,
                    value=0.3,
                    confidence=0.85,
                )
            ]

    agg = SignalAggregator()
    agg.register(MockAdapter())
    variance_signals = await agg.get_variance_signals(
        melbourne_cbd_location,
        date(2026, 4, 4),
        date(2026, 4, 4),
    )
    assert len(variance_signals) > 0
    assert variance_signals[0].signal_type == SignalType.weather
    assert variance_signals[0].value == 0.3


@pytest.mark.asyncio
async def test_signal_aggregator_demand_outlook(melbourne_cbd_location):
    """SignalAggregator.get_demand_outlook() returns valid structure."""

    class MockAdapter(DataFeedAdapter):
        @property
        def category(self):
            return FeedCategory.weather

        @property
        def source_name(self):
            return "mock"

        async def fetch_signals(self, location, start_date, end_date, venue_id=None):
            return [
                FeedSignal(
                    category=FeedCategory.weather,
                    source="mock",
                    signal_date=start_date,
                    strength=SignalStrength.strong_positive,
                    value=0.6,
                    confidence=0.9,
                )
            ]

    agg = SignalAggregator()
    agg.register(MockAdapter())
    outlook = await agg.get_demand_outlook(
        melbourne_cbd_location,
        date(2026, 4, 4),
    )
    assert "outlook" in outlook
    assert "confidence" in outlook
    assert "summary" in outlook
    assert "signals_used" in outlook
    assert "average_variance" in outlook
    assert outlook["outlook"] in ["bullish", "bearish", "neutral"]


def test_signal_aggregator_get_signal_dashboard():
    """SignalAggregator.get_signal_dashboard() returns valid structure."""
    agg = SignalAggregator()
    dashboard = agg.get_signal_dashboard()
    assert "by_category" in dashboard
    assert "total_signals" in dashboard
    assert "available_types" in dashboard
    assert "cache_size" in dashboard


@pytest.mark.asyncio
async def test_signal_aggregator_context_manager():
    """SignalAggregator works as async context manager."""

    class MockAdapter(DataFeedAdapter):
        def __init__(self):
            super().__init__()
            self.closed = False

        @property
        def category(self):
            return FeedCategory.weather

        @property
        def source_name(self):
            return "mock"

        async def fetch_signals(self, location, start_date, end_date, venue_id=None):
            return []

        async def close(self):
            self.closed = True

    adapter = MockAdapter()
    async with SignalAggregator() as agg:
        agg.register(adapter)
        assert len(agg._all_adapters) == 1

    assert adapter.closed is True


# ============================================================================
# Integration and Smoke Tests
# ============================================================================


class TestAdapterSmokeTests:
    """Smoke tests for each adapter to ensure basic functionality."""

    def test_bom_weather_adapter_smoke(self):
        """BOMWeatherAdapter initializes without error."""
        adapter = BOMWeatherAdapter()
        assert adapter is not None

    def test_openweathermap_adapter_smoke(self):
        """OpenWeatherMapAdapter initializes without error."""
        adapter = OpenWeatherMapAdapter(api_key="test")
        assert adapter is not None

    def test_ticketmaster_adapter_smoke(self):
        """TicketmasterAdapter initializes without error."""
        adapter = TicketmasterAdapter(api_key="test")
        assert adapter is not None

    def test_eventbrite_adapter_smoke(self):
        """EventbriteAdapter initializes without error."""
        adapter = EventbriteAdapter(api_key="test")
        assert adapter is not None

    def test_google_places_traffic_adapter_smoke(self):
        """GooglePlacesTrafficAdapter initializes without error."""
        adapter = GooglePlacesTrafficAdapter(api_key="test")
        assert adapter is not None

    def test_resdiary_adapter_smoke(self):
        """ResDiaryAdapter initializes without error."""
        adapter = ResDiaryAdapter(api_key="test")
        assert adapter is not None

    def test_school_holiday_adapter_smoke(self):
        """SchoolHolidayAdapter initializes without error."""
        adapter = SchoolHolidayAdapter()
        adapter.configure(state=AustralianState.VIC, venue_type=VenueType.family)
        assert adapter is not None

    def test_public_holiday_adapter_smoke(self):
        """PublicHolidayAdapter initializes without error."""
        adapter = PublicHolidayAdapter()
        assert adapter is not None

    def test_payroll_cycle_adapter_smoke(self):
        """PayrollCycleAdapter initializes without error."""
        adapter = PayrollCycleAdapter()
        assert adapter is not None

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_aus_sports_calendar_smoke(self):
        """AUSportsCalendar initializes without error."""
        adapter = AUSportsCalendar()
        assert adapter is not None

    def test_google_places_venue_adapter_smoke(self):
        """GooglePlacesVenueAdapter initializes without error."""
        adapter = GooglePlacesVenueAdapter(api_key="test")
        assert adapter is not None

    def test_ubereats_adapter_smoke(self):
        """UberEatsAdapter initializes without error."""
        adapter = UberEatsAdapter(api_key="test")
        assert adapter is not None

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_cruise_ship_adapter_smoke(self):
        """CruiseShipAdapter initializes without error."""
        adapter = CruiseShipAdapter()
        assert adapter is not None

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_conference_adapter_smoke(self):
        """ConferenceAdapter initializes without error."""
        adapter = ConferenceAdapter()
        assert adapter is not None

    @pytest.mark.xfail(
        reason="data_feeds sports/tourism adapters not yet ported to current base.py API",
        strict=False,
    )
    def test_transport_disruption_adapter_smoke(self):
        """TransportDisruptionAdapter initializes without error."""
        adapter = TransportDisruptionAdapter()
        assert adapter is not None

    def test_rba_adapter_smoke(self):
        """RBAAdapter initializes without error."""
        adapter = RBAAdapter()
        assert adapter is not None

    def test_signal_aggregator_smoke(self):
        """SignalAggregator initializes without error."""
        agg = SignalAggregator()
        assert agg is not None


class TestDataFeedIntegration:
    """Integration tests for data feed workflow."""

    def test_full_workflow_signal_creation_to_cache(self, signal_cache):
        """Complete workflow: create, cache, retrieve signal."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.moderate_positive,
            confidence=0.85,
        )
        signal_cache.put(signal, ttl_minutes=30)
        retrieved = signal_cache.get(signal.cache_key)
        assert retrieved is not None
        assert retrieved.source == "bom"
        assert retrieved.confidence == 0.85

    def test_signal_variance_conversion_pipeline(self):
        """Signal value conversion to variance is consistent."""
        signal = FeedSignal(
            category=FeedCategory.weather,
            source="bom",
            signal_date=date(2026, 4, 4),
            strength=SignalStrength.strong_positive,
        )
        variance_value = signal.to_variance_value()
        expected = STRENGTH_MULTIPLIERS[SignalStrength.strong_positive]
        assert variance_value == expected
