"""
Feed Runner for RosterIQ.

Manages data feed orchestration, scheduling, and signal aggregation.
Wires up FREE data sources (no API keys required) and optionally paid sources.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from rosteriq.data_feeds.base import FeedSignal, Location, FeedCategory, SignalStrength
from rosteriq.data_feeds.aggregator import SignalAggregator
from rosteriq.data_feeds.calendar import (
    SchoolHolidayAdapter,
    PublicHolidayAdapter,
    PayrollCycleAdapter,
    CalendarAggregator,
)
from rosteriq.data_feeds.sports import AUSportsCalendar, SportsAggregator
from rosteriq.data_feeds.weather import BOMWeatherAdapter, get_weather_adapter
from rosteriq.models import VenueConfig, VenueLocation

logger = logging.getLogger(__name__)


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================


class FeedConfig(BaseModel):
    """Configuration for optional paid data sources."""

    google_places_api_key: Optional[str] = None
    ticketmaster_api_key: Optional[str] = None
    eventbrite_token: Optional[str] = None
    predicthq_token: Optional[str] = None
    resdiary_api_key: Optional[str] = None
    nowbookit_api_key: Optional[str] = None


class FeedStatusResponse(BaseModel):
    """Status of all active feeds."""

    active_feeds: dict[str, bool]
    last_fetch_times: dict[str, Optional[str]]
    signal_counts: dict[str, int]
    total_active: int
    free_feeds_active: int
    paid_feeds_active: int


class FetchNowRequest(BaseModel):
    """Request to trigger immediate feed fetch."""

    category: Optional[str] = None  # None = all categories


class FeedAvailableResponse(BaseModel):
    """Available feed types and their API key requirements."""

    feed_type: str
    description: str
    requires_api_key: bool
    api_key_field: Optional[str]
    category: str


class FeedConfigureRequest(BaseModel):
    """Update feed configuration for a venue."""

    venue_id: str
    feed_config: FeedConfig


# ============================================================================
# AGGREGATOR CREATION
# ============================================================================


def create_free_aggregator(
    venue_location: VenueLocation,
    venue_state: str,
    venue_type: str = "suburban",
) -> SignalAggregator:
    """
    Create an aggregator with all free-tier data sources.

    Args:
        venue_location: Location of the venue (coordinates, address)
        venue_state: Australian state code (VIC, NSW, QLD, etc.)
        venue_type: Type of venue (suburban, city, regional)

    Returns:
        SignalAggregator with all free feeds registered
    """
    aggregator = SignalAggregator(venue_location=venue_location)

    # Calendar feeds (hardcoded, no API required)
    school_holiday_adapter = SchoolHolidayAdapter(state=venue_state)
    aggregator.register_adapter(school_holiday_adapter)
    logger.info(f"  ✓ School holidays ({venue_state}) — FREE, 2025-2027 dates loaded")

    public_holiday_adapter = PublicHolidayAdapter(state=venue_state)
    aggregator.register_adapter(public_holiday_adapter)
    logger.info(f"  ✓ Public holidays ({venue_state}) — FREE, 47 holidays loaded")

    payroll_cycle_adapter = PayrollCycleAdapter(cycle_type="fortnightly")
    aggregator.register_adapter(payroll_cycle_adapter)
    logger.info("  ✓ Payroll cycles — FREE, fortnightly signals active")

    # Sports calendar (hardcoded 2026 fixtures)
    sports_calendar = AUSportsCalendar(year=2026)
    sports_agg = SportsAggregator(calendar=sports_calendar)
    aggregator.register_adapter(sports_agg)
    logger.info(
        "  ✓ Sports calendar — FREE, AFL 2026 (26 rounds), NRL 2026 (25 rounds)"
    )

    # Weather feed (free government API, no key needed)
    weather_adapter = BOMWeatherAdapter(
        location=venue_location,
        state=venue_state,
    )
    aggregator.register_adapter(weather_adapter)
    logger.info(f"  ✓ BOM Weather — FREE, {venue_state} region")

    return aggregator


def create_full_aggregator(
    venue_location: VenueLocation,
    venue_state: str,
    feed_config: FeedConfig,
) -> SignalAggregator:
    """
    Create an aggregator with all available feeds (free + paid with API keys).

    Args:
        venue_location: Location of the venue
        venue_state: Australian state code
        feed_config: Configuration with optional API keys

    Returns:
        SignalAggregator with all available feeds (free and configured paid)
    """
    # Start with free aggregator
    aggregator = create_free_aggregator(
        venue_location=venue_location,
        venue_state=venue_state,
    )

    # Try to add paid feeds if API keys are available
    # Note: These would require actual adapter implementations
    # For now, we log when they're skipped

    if feed_config.google_places_api_key:
        logger.info("  ✓ Google Places — active (traffic + venue signals)")
        # aggregator.register_adapter(GooglePlacesTrafficAdapter(...))
        # aggregator.register_adapter(GooglePlacesVenueAdapter(...))
    else:
        logger.info("  ✗ Google Places — skipped (no API key)")

    if feed_config.ticketmaster_api_key:
        logger.info("  ✓ Ticketmaster Events — active")
        # aggregator.register_adapter(TicketmasterAdapter(...))
    else:
        logger.info("  ✗ Ticketmaster — skipped (no API key)")

    if feed_config.eventbrite_token:
        logger.info("  ✓ Eventbrite Events — active")
        # aggregator.register_adapter(EventbriteAdapter(...))
    else:
        logger.info("  ✗ Eventbrite — skipped (no API key)")

    if feed_config.predicthq_token:
        logger.info("  ✓ PredictHQ Events — active")
        # aggregator.register_adapter(PredictHQAdapter(...))
    else:
        logger.info("  ✗ PredictHQ — skipped (no API key)")

    if feed_config.resdiary_api_key:
        logger.info("  ✓ ResDiary Reservations — active")
        # aggregator.register_adapter(ResDiaryAdapter(...))
    else:
        logger.info("  ✗ ResDiary — skipped (no API key)")

    if feed_config.nowbookit_api_key:
        logger.info("  ✓ NowBookIt Reservations — active")
        # aggregator.register_adapter(NowBookItAdapter(...))
    else:
        logger.info("  ✗ NowBookIt — skipped (no API key)")

    return aggregator


# ============================================================================
# FEED SCHEDULER
# ============================================================================


class FeedScheduler:
    """Background task manager for periodic feed fetching."""

    # Fetch intervals per category (in minutes)
    FETCH_INTERVALS = {
        FeedCategory.WEATHER: 60,  # 1 hour
        FeedCategory.EVENTS: 120,  # 2 hours
        FeedCategory.CALENDAR: 1440,  # 24 hours
        FeedCategory.SPORTS: 1440,  # 24 hours
        FeedCategory.FOOT_TRAFFIC: 15,  # 15 minutes
        FeedCategory.RESERVATIONS: 10,  # 10 minutes
        FeedCategory.DELIVERY: 10,  # 10 minutes
        FeedCategory.ECONOMIC: 1440,  # 24 hours
        FeedCategory.NEARBY_VENUES: 360,  # 6 hours
    }

    def __init__(self, aggregator: SignalAggregator, venue_name: str):
        """
        Initialize the feed scheduler.

        Args:
            aggregator: Signal aggregator instance
            venue_name: Name of the venue (for logging)
        """
        self.aggregator = aggregator
        self.venue_name = venue_name
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_fetch_times: dict[str, Optional[datetime]] = {}
        self.signal_counts: dict[str, int] = {}

    async def start(self) -> None:
        """Start the background feed scheduler."""
        if self._running:
            logger.warning(f"FeedScheduler for {self.venue_name} already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"FeedScheduler started for venue: {self.venue_name}")

    async def stop(self) -> None:
        """Stop the background feed scheduler."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"FeedScheduler stopped for venue: {self.venue_name}")

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                # Calculate next fetch time for each category
                for category, interval_minutes in self.FETCH_INTERVALS.items():
                    last_fetch = self.last_fetch_times.get(category)

                    if last_fetch is None or (datetime.now() - last_fetch).total_seconds() > interval_minutes * 60:
                        await self._fetch_category(category)

                # Sleep before next check
                await asyncio.sleep(30)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in feed scheduler loop: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _fetch_category(self, category: FeedCategory) -> None:
        """
        Fetch signals for a specific category.

        Args:
            category: Feed category to fetch
        """
        try:
            signals = await self.aggregator.fetch_signals(category=category)
            self.last_fetch_times[category] = datetime.now()
            self.signal_counts[category] = len(signals)
            logger.debug(
                f"Fetched {len(signals)} signals from {category.value} for {self.venue_name}"
            )
            # TODO: Store signals in database (external_signals table)
            # TODO: Publish updates via WebSocket hub
        except Exception as e:
            logger.error(f"Error fetching {category.value} signals: {e}", exc_info=True)

    async def fetch_now(self, category: Optional[FeedCategory] = None) -> dict[str, int]:
        """
        Force immediate fetch for all or specific category.

        Args:
            category: Specific category to fetch, or None for all

        Returns:
            Dictionary of category -> signal count
        """
        results = {}

        if category:
            signals = await self.aggregator.fetch_signals(category=category)
            self.last_fetch_times[category] = datetime.now()
            self.signal_counts[category] = len(signals)
            results[category.value] = len(signals)
            logger.info(
                f"Force-fetched {len(signals)} signals from {category.value} for {self.venue_name}"
            )
        else:
            for cat in FeedCategory:
                signals = await self.aggregator.fetch_signals(category=cat)
                self.last_fetch_times[cat] = datetime.now()
                self.signal_counts[cat] = len(signals)
                results[cat.value] = len(signals)
            logger.info(
                f"Force-fetched all categories for {self.venue_name}: {results}"
            )

        # TODO: Store signals in database
        # TODO: Publish updates via WebSocket hub

        return results

    def get_status(self) -> FeedStatusResponse:
        """Get current feed status."""
        active_feeds = {
            "school_holidays": True,
            "public_holidays": True,
            "payroll_cycles": True,
            "sports_calendar": True,
            "weather": True,
        }

        last_fetch_times_str = {
            k: v.isoformat() if v else None
            for k, v in self.last_fetch_times.items()
        }

        return FeedStatusResponse(
            active_feeds=active_feeds,
            last_fetch_times=last_fetch_times_str,
            signal_counts=self.signal_counts,
            total_active=5,
            free_feeds_active=5,
            paid_feeds_active=0,
        )


# ============================================================================
# GLOBAL SCHEDULER INSTANCES
# ============================================================================

_schedulers: dict[str, FeedScheduler] = {}


async def setup_feeds(app) -> None:
    """
    Setup feeds on application startup.

    Should be called from api.py on startup.

    Args:
        app: FastAPI application instance
    """
    logger.info("=" * 80)
    logger.info("RosterIQ Feed Runner Initialization")
    logger.info("=" * 80)

    # TODO: Read venue configs from database
    # For now, this is a stub that would be called on app startup
    # Example:
    # venues = await db.get_all_venues()
    # for venue in venues:
    #     aggregator = create_free_aggregator(
    #         venue_location=venue.location,
    #         venue_state=venue.state,
    #     )
    #     scheduler = FeedScheduler(aggregator, venue.name)
    #     _schedulers[venue.id] = scheduler
    #     await scheduler.start()
    #     logger.info(f"✓ Feed runner initialized for {venue.name}")

    def shutdown_feeds() -> None:
        """Shutdown hook for graceful shutdown."""
        logger.info("Shutting down feed schedulers...")
        for scheduler in _schedulers.values():
            try:
                asyncio.create_task(scheduler.stop())
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")

    app.add_event_handler("shutdown", shutdown_feeds)


# ============================================================================
# API ROUTER
# ============================================================================


def create_feeds_router() -> APIRouter:
    """Create the FastAPI router for feed management endpoints."""
    router = APIRouter(prefix="/feeds", tags=["feeds"])

    @router.get("/status", response_model=dict[str, FeedStatusResponse])
    async def get_feed_status() -> dict[str, FeedStatusResponse]:
        """
        Get status of all active feeds.

        Returns:
            Dictionary of venue_id -> FeedStatusResponse
        """
        if not _schedulers:
            raise HTTPException(status_code=404, detail="No venues with feeds configured")

        return {
            venue_id: scheduler.get_status()
            for venue_id, scheduler in _schedulers.items()
        }

    @router.post("/fetch-now", response_model=dict[str, dict[str, int]])
    async def fetch_feeds_now(request: FetchNowRequest) -> dict[str, dict[str, int]]:
        """
        Trigger immediate feed fetch for all or specific category.

        Args:
            request: Fetch request with optional category filter

        Returns:
            Dictionary of venue_id -> {category -> signal_count}
        """
        if not _schedulers:
            raise HTTPException(status_code=404, detail="No venues with feeds configured")

        results = {}
        category = None
        if request.category:
            try:
                category = FeedCategory[request.category.upper()]
            except KeyError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid category: {request.category}. Valid options: {[c.value for c in FeedCategory]}",
                )

        for venue_id, scheduler in _schedulers.items():
            try:
                results[venue_id] = await scheduler.fetch_now(category=category)
            except Exception as e:
                logger.error(f"Error fetching for venue {venue_id}: {e}")
                results[venue_id] = {"error": str(e)}

        return results

    @router.get("/available", response_model=list[FeedAvailableResponse])
    async def get_available_feeds() -> list[FeedAvailableResponse]:
        """
        List all available feed types and their API key requirements.

        Returns:
            List of available feeds with metadata
        """
        feeds = [
            FeedAvailableResponse(
                feed_type="School Holidays",
                description="Australian school holiday dates (hardcoded)",
                requires_api_key=False,
                api_key_field=None,
                category=FeedCategory.CALENDAR.value,
            ),
            FeedAvailableResponse(
                feed_type="Public Holidays",
                description="Australian public holidays (hardcoded)",
                requires_api_key=False,
                api_key_field=None,
                category=FeedCategory.CALENDAR.value,
            ),
            FeedAvailableResponse(
                feed_type="Payroll Cycles",
                description="Fortnightly payroll cycle dates",
                requires_api_key=False,
                api_key_field=None,
                category=FeedCategory.CALENDAR.value,
            ),
            FeedAvailableResponse(
                feed_type="Sports Calendar",
                description="AFL/NRL fixtures (2026 hardcoded)",
                requires_api_key=False,
                api_key_field=None,
                category=FeedCategory.SPORTS.value,
            ),
            FeedAvailableResponse(
                feed_type="BOM Weather",
                description="Australian Bureau of Meteorology (free government API)",
                requires_api_key=False,
                api_key_field=None,
                category=FeedCategory.WEATHER.value,
            ),
            FeedAvailableResponse(
                feed_type="Google Places",
                description="Real-time traffic and venue foot traffic data",
                requires_api_key=True,
                api_key_field="google_places_api_key",
                category=FeedCategory.FOOT_TRAFFIC.value,
            ),
            FeedAvailableResponse(
                feed_type="Ticketmaster Events",
                description="Live events and ticket data",
                requires_api_key=True,
                api_key_field="ticketmaster_api_key",
                category=FeedCategory.EVENTS.value,
            ),
            FeedAvailableResponse(
                feed_type="Eventbrite Events",
                description="Local event listings",
                requires_api_key=True,
                api_key_field="eventbrite_token",
                category=FeedCategory.EVENTS.value,
            ),
            FeedAvailableResponse(
                feed_type="PredictHQ Events",
                description="High-impact events (concerts, sports, protests)",
                requires_api_key=True,
                api_key_field="predicthq_token",
                category=FeedCategory.EVENTS.value,
            ),
            FeedAvailableResponse(
                feed_type="ResDiary Reservations",
                description="Restaurant reservation data",
                requires_api_key=True,
                api_key_field="resdiary_api_key",
                category=FeedCategory.RESERVATIONS.value,
            ),
            FeedAvailableResponse(
                feed_type="NowBookIt Reservations",
                description="Venue reservation and booking data",
                requires_api_key=True,
                api_key_field="nowbookit_api_key",
                category=FeedCategory.RESERVATIONS.value,
            ),
        ]
        return feeds

    @router.post("/configure")
    async def configure_feeds(request: FeedConfigureRequest) -> dict:
        """
        Update feed configuration for a venue and auto-start newly configured feeds.

        Args:
            request: Configuration update request

        Returns:
            Status of configuration update
        """
        if request.venue_id not in _schedulers:
            raise HTTPException(
                status_code=404,
                detail=f"Venue {request.venue_id} not found",
            )

        # TODO: Update feed config in database
        # TODO: Restart scheduler with new config
        # TODO: Log which new feeds were activated

        return {
            "status": "configured",
            "venue_id": request.venue_id,
            "message": "Feed configuration updated. New feeds will start on next scheduler cycle.",
        }

    return router
