"""Sports Schedules data feed adapter for RosterIQ.

Monitors AFL, NRL, Cricket, A-League, F1, Tennis, and major sporting events
that significantly impact Australian pub traffic and staffing requirements.

Sports are CRITICAL for Australian pubs - AFL/NRL game days can double or triple
covers, and events like State of Origin and the Melbourne Cup are essentially
national holidays in certain regions.

Data sources:
- SportRadar API for comprehensive sports data
- The Odds API for schedule information
- ESPN API for event details
- Hardcoded Australian season data as fallback
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, time
from typing import Any
from zoneinfo import ZoneInfo

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

# Australian timezone
TIMEZONE_AU = ZoneInfo("Australia/Sydney")

# Major annual sporting events with typical dates and nationwide impact
MAJOR_EVENTS = {
    "Melbourne Cup": {
        "date_pattern": "first Tuesday November",
        "month": 11,
        "day_of_week": 1,  # Tuesday
        "signal_strength": SignalStrength.strong_positive,
        "scope": "nationwide",
        "notes": "Public holiday in Victoria, major pub draw nationwide",
    },
    "AFL Grand Final": {
        "date_pattern": "last Saturday September",
        "month": 9,
        "day_of_week": 5,  # Saturday
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "National holiday atmosphere, massive pub traffic",
    },
    "NRL Grand Final": {
        "date_pattern": "first Sunday October",
        "month": 10,
        "day_of_week": 6,  # Sunday
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "Huge in NSW/QLD, packs all pubs",
    },
    "State of Origin Game 1": {
        "date_pattern": "May",
        "month": 5,
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "NSW vs QLD, extreme draw in all pubs",
    },
    "State of Origin Game 2": {
        "date_pattern": "June",
        "month": 6,
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "NSW vs QLD, extreme draw in all pubs",
    },
    "State of Origin Game 3": {
        "date_pattern": "July",
        "month": 7,
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "NSW vs QLD, extreme draw in all pubs",
    },
    "Australian Open": {
        "date_pattern": "mid-January, 2 weeks",
        "month": 1,
        "signal_strength": SignalStrength.strong_positive,
        "scope": "nationwide",
        "notes": "Melbourne-based, tourism boost, good pub traffic",
    },
    "F1 Australian Grand Prix": {
        "date_pattern": "March, Melbourne",
        "month": 3,
        "signal_strength": SignalStrength.strong_positive,
        "scope": "Victoria",
        "notes": "Melbourne buzzes, excellent pub traffic",
    },
    "BBL Finals": {
        "date_pattern": "December-January",
        "month": 1,
        "signal_strength": SignalStrength.strong_positive,
        "scope": "nationwide",
        "notes": "Summer cricket, huge pub draw",
    },
    "The Ashes": {
        "date_pattern": "Winter (June-July if in Australia)",
        "month": 6,
        "signal_strength": SignalStrength.extreme_positive,
        "scope": "nationwide",
        "notes": "Australia vs England, massive pub traffic",
    },
}

# AFL 2026 Season approximate schedule (rounds with typical dates)
AFL_2026_SEASON = {
    "Round 1": {"start_date": "2026-03-26", "rounds": 1},
    "Round 2": {"start_date": "2026-04-02", "rounds": 1},
    "Round 3": {"start_date": "2026-04-09", "rounds": 1},
    "Round 4": {"start_date": "2026-04-16", "rounds": 1},
    "Round 5": {"start_date": "2026-04-23", "rounds": 1},
    "Round 6": {"start_date": "2026-04-30", "rounds": 1},
    "Round 7": {"start_date": "2026-05-07", "rounds": 1},
    "Round 8": {"start_date": "2026-05-14", "rounds": 1},
    "Round 9": {"start_date": "2026-05-21", "rounds": 1},
    "Round 10": {"start_date": "2026-05-28", "rounds": 1},
    "Round 11": {"start_date": "2026-06-04", "rounds": 1},
    "Round 12": {"start_date": "2026-06-11", "rounds": 1},
    "Round 13": {"start_date": "2026-06-18", "rounds": 1},
    "Round 14": {"start_date": "2026-06-25", "rounds": 1},
    "Round 15": {"start_date": "2026-07-02", "rounds": 1},
    "Round 16": {"start_date": "2026-07-09", "rounds": 1},
    "Round 17": {"start_date": "2026-07-16", "rounds": 1},
    "Round 18": {"start_date": "2026-07-23", "rounds": 1},
    "Round 19": {"start_date": "2026-07-30", "rounds": 1},
    "Round 20": {"start_date": "2026-08-06", "rounds": 1},
    "Round 21": {"start_date": "2026-08-13", "rounds": 1},
    "Round 22": {"start_date": "2026-08-20", "rounds": 1},
    "Round 23": {"start_date": "2026-08-27", "rounds": 1},
    "Finals Week 1": {"start_date": "2026-09-03", "rounds": 1},
    "Finals Week 2": {"start_date": "2026-09-10", "rounds": 1},
    "Finals Week 3": {"start_date": "2026-09-17", "rounds": 1},
    "Grand Final": {"start_date": "2026-09-26", "rounds": 1},
}

# NRL 2026 Season approximate schedule
NRL_2026_SEASON = {
    "Round 1": {"start_date": "2026-03-12", "rounds": 1},
    "Round 2": {"start_date": "2026-03-19", "rounds": 1},
    "Round 3": {"start_date": "2026-03-26", "rounds": 1},
    "Round 4": {"start_date": "2026-04-02", "rounds": 1},
    "Round 5": {"start_date": "2026-04-09", "rounds": 1},
    "Round 6": {"start_date": "2026-04-16", "rounds": 1},
    "Round 7": {"start_date": "2026-04-23", "rounds": 1},
    "Round 8": {"start_date": "2026-04-30", "rounds": 1},
    "Round 9": {"start_date": "2026-05-07", "rounds": 1},
    "Round 10": {"start_date": "2026-05-14", "rounds": 1},
    "Round 11": {"start_date": "2026-05-21", "rounds": 1},
    "Round 12": {"start_date": "2026-05-28", "rounds": 1},
    "Round 13": {"start_date": "2026-06-04", "rounds": 1},
    "Round 14": {"start_date": "2026-06-11", "rounds": 1},
    "Round 15": {"start_date": "2026-06-18", "rounds": 1},
    "Round 16": {"start_date": "2026-06-25", "rounds": 1},
    "Round 17": {"start_date": "2026-07-02", "rounds": 1},
    "Round 18": {"start_date": "2026-07-09", "rounds": 1},
    "Round 19": {"start_date": "2026-07-16", "rounds": 1},
    "Round 20": {"start_date": "2026-07-23", "rounds": 1},
    "Round 21": {"start_date": "2026-07-30", "rounds": 1},
    "Round 22": {"start_date": "2026-08-06", "rounds": 1},
    "Round 23": {"start_date": "2026-08-13", "rounds": 1},
    "Round 24": {"start_date": "2026-08-20", "rounds": 1},
    "Round 25": {"start_date": "2026-08-27", "rounds": 1},
    "Finals Week 1": {"start_date": "2026-09-03", "rounds": 1},
    "Finals Week 2": {"start_date": "2026-09-10", "rounds": 1},
    "Grand Final": {"start_date": "2026-10-04", "rounds": 1},
}

# Australian teams and their home venues
AU_TEAMS = {
    "AFL": {
        "Adelaide Crows": {"state": "SA", "venue": "Adelaide Oval"},
        "Brisbane Lions": {"state": "QLD", "venue": "The Gabba"},
        "Carlton Blues": {"state": "VIC", "venue": "Marvel Stadium"},
        "Collingwood Magpies": {"state": "VIC", "venue": "MCG"},
        "Essendon Bombers": {"state": "VIC", "venue": "MCG"},
        "Fremantle Dockers": {"state": "WA", "venue": "Optus Stadium"},
        "Geelong Cats": {"state": "VIC", "venue": "GMHBA Stadium"},
        "Gold Coast Suns": {"state": "QLD", "venue": "Metricon Stadium"},
        "Greater Western Sydney Giants": {"state": "NSW", "venue": "Spotless Stadium"},
        "Hawthorn Hawks": {"state": "VIC", "venue": "MCG"},
        "Melbourne Demons": {"state": "VIC", "venue": "MCG"},
        "North Melbourne Kangaroos": {"state": "VIC", "venue": "Marvel Stadium"},
        "Port Adelaide Power": {"state": "SA", "venue": "Adelaide Oval"},
        "Richmond Tigers": {"state": "VIC", "venue": "MCG"},
        "St Kilda Saints": {"state": "VIC", "venue": "Marvel Stadium"},
        "Sydney Swans": {"state": "NSW", "venue": "SCG"},
        "West Coast Eagles": {"state": "WA", "venue": "Optus Stadium"},
        "Western Bulldogs": {"state": "VIC", "venue": "Marvel Stadium"},
    },
    "NRL": {
        "Canberra Raiders": {"state": "ACT", "venue": "GIO Stadium"},
        "Cronulla-Sutherland Sharks": {"state": "NSW", "venue": "PointsBet Stadium"},
        "Gold Coast Titans": {"state": "QLD", "venue": "Skilled Park"},
        "Manly-Warringah Sea Eagles": {"state": "NSW", "venue": "Brookvale Oval"},
        "Melbourne Storm": {"state": "VIC", "venue": "AAMI Park"},
        "New Zealand Warriors": {"state": "overseas", "venue": "Mt Smart Stadium"},
        "Newcastle Knights": {"state": "NSW", "venue": "McDonald Jones Stadium"},
        "North Queensland Cowboys": {"state": "QLD", "venue": "Townsville Stadium"},
        "Parramatta Eels": {"state": "NSW", "venue": "Bankwest Stadium"},
        "Penrith Panthers": {"state": "NSW", "venue": "Panthers Stadium"},
        "South Sydney Rabbitohs": {"state": "NSW", "venue": "Redfern Oval"},
        "Sydney Roosters": {"state": "NSW", "venue": "Allianz Stadium"},
        "Wests Tigers": {"state": "NSW", "venue": "Leichhardt Oval"},
        "Brisbane Broncos": {"state": "QLD", "venue": "Suncorp Stadium"},
    },
}


class SportRadarAdapter(DataFeedAdapter):
    """Adapter for SportRadar API sports data integration.

    SportRadar provides comprehensive sports coverage including AFL, NRL, cricket,
    tennis, and international events. Requires API authentication.
    """

    def __init__(
        self,
        api_key: str,
        cache: SignalCache | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        """Initialize SportRadar adapter.

        Args:
            api_key: SportRadar API key for authentication
            cache: Optional signal cache for deduplication
            http_client: Optional httpx AsyncClient for custom configuration
        """
        super().__init__(
            name="SportRadar",
            category=FeedCategory.sports,
            cache=cache,
            cache_ttl_minutes=360,
        )
        self.api_key = api_key
        self.http_client = http_client
        self.base_url = "https://api.sportradar.com/"

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """Fetch sports signals from SportRadar API.

        Args:
            location: The pub location to fetch sports data for

        Returns:
            List of FeedSignal objects for upcoming sports events
        """
        signals: list[FeedSignal] = []

        try:
            if not self.http_client:
                async with httpx.AsyncClient() as client:
                    return await self._fetch_from_api(client, location)
            else:
                return await self._fetch_from_api(self.http_client, location)

        except Exception as e:
            logger.error(
                f"Error fetching from SportRadar for {location.name}: {e}",
                exc_info=True,
            )
            return signals

    async def _fetch_from_api(
        self, client: httpx.AsyncClient, location: Location
    ) -> list[FeedSignal]:
        """Internal method to fetch from SportRadar API.

        Args:
            client: httpx AsyncClient for requests
            location: The pub location

        Returns:
            List of FeedSignal objects
        """
        # This is a placeholder implementation - actual SportRadar integration
        # would require valid API endpoints and authentication
        logger.info(
            f"SportRadar adapter initialized for {location.name} "
            f"(requires valid API key)"
        )
        return []


class ESPNAdapter(DataFeedAdapter):
    """Adapter for ESPN API sports data integration.

    ESPN provides free public endpoints for sports schedules including
    cricket, soccer, and international events.
    """

    def __init__(
        self,
        cache: SignalCache | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        """Initialize ESPN adapter.

        Args:
            cache: Optional signal cache for deduplication
            http_client: Optional httpx AsyncClient for custom configuration
        """
        super().__init__(
            name="ESPN",
            category=FeedCategory.sports,
            cache=cache,
            cache_ttl_minutes=360,
        )
        self.http_client = http_client
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports/"

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """Fetch sports signals from ESPN API.

        Args:
            location: The pub location to fetch sports data for

        Returns:
            List of FeedSignal objects for upcoming sports events
        """
        signals: list[FeedSignal] = []

        try:
            if not self.http_client:
                async with httpx.AsyncClient() as client:
                    return await self._fetch_from_api(client, location)
            else:
                return await self._fetch_from_api(self.http_client, location)

        except Exception as e:
            logger.error(
                f"Error fetching from ESPN for {location.name}: {e}",
                exc_info=True,
            )
            return signals

    async def _fetch_from_api(
        self, client: httpx.AsyncClient, location: Location
    ) -> list[FeedSignal]:
        """Internal method to fetch from ESPN API.

        Args:
            client: httpx AsyncClient for requests
            location: The pub location

        Returns:
            List of FeedSignal objects
        """
        # This is a placeholder implementation - actual ESPN integration
        # would require parsing specific sport endpoints
        logger.info(f"ESPN adapter initialized for {location.name}")
        return []


class AUSportsCalendar(DataFeedAdapter):
    """Hardcoded fallback adapter for Australian sports schedules.

    Provides reliable data for AFL, NRL, and major sporting events
    without external API dependencies. Uses 2026 season data with
    estimated match times based on typical scheduling patterns.
    """

    def __init__(self, cache: SignalCache | None = None):
        """Initialize Australian sports calendar adapter.

        Args:
            cache: Optional signal cache for deduplication
        """
        super().__init__(
            name="AUSportsCalendar",
            category=FeedCategory.sports,
            cache=cache,
            cache_ttl_minutes=360,
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """Fetch sports signals from hardcoded Australian calendar.

        Args:
            location: The pub location to fetch sports data for

        Returns:
            List of FeedSignal objects for upcoming sports events
        """
        signals: list[FeedSignal] = []
        now = datetime.now(TIMEZONE_AU)

        try:
            # Fetch AFL signals
            signals.extend(
                await self._fetch_afl_signals(location, now)
            )

            # Fetch NRL signals
            signals.extend(
                await self._fetch_nrl_signals(location, now)
            )

            # Fetch major event signals
            signals.extend(
                await self._fetch_major_event_signals(location, now)
            )

            logger.info(
                f"Generated {len(signals)} sports signals for {location.name}"
            )

        except Exception as e:
            logger.error(
                f"Error fetching from AUSportsCalendar for {location.name}: {e}",
                exc_info=True,
            )

        return signals

    async def _fetch_afl_signals(
        self, location: Location, now: datetime
    ) -> list[FeedSignal]:
        """Generate AFL match signals for upcoming rounds.

        Args:
            location: The pub location
            now: Current datetime

        Returns:
            List of AFL-related FeedSignal objects
        """
        signals: list[FeedSignal] = []

        for round_name, round_data in AFL_2026_SEASON.items():
            try:
                round_start = datetime.fromisoformat(
                    round_data["start_date"]
                ).replace(tzinfo=TIMEZONE_AU)

                # Skip past rounds
                if round_start < now:
                    continue

                # Cap to 90 days ahead
                if round_start > now + timedelta(days=90):
                    break

                # Create signals for typical match times
                # AFL matches: Thu/Fri night (20:00), Sat afternoon (14:00),
                # Sat night (19:30), Sun afternoon (15:30)
                match_times = [
                    (20, 0),  # Thu/Fri night
                    (14, 0),  # Sat afternoon
                    (19, 30),  # Sat night
                    (15, 30),  # Sun afternoon
                ]

                day_offset = 0
                for hour, minute in match_times:
                    match_datetime = round_start.replace(
                        hour=hour, minute=minute, second=0
                    ) + timedelta(days=day_offset)

                    if match_datetime < now:
                        day_offset += 1
                        continue

                    if match_datetime > now + timedelta(days=7):
                        break

                    # Determine signal strength based on round
                    if "Grand Final" in round_name:
                        signal_strength = SignalStrength.extreme_positive
                    elif "Finals" in round_name:
                        signal_strength = SignalStrength.strong_positive
                    else:
                        signal_strength = SignalStrength.moderate_positive

                    signal = FeedSignal(
                        source=self.name,
                        category=self.category,
                        strength=signal_strength,
                        confidence=0.95,
                        start_time=match_datetime,
                        end_time=match_datetime + timedelta(hours=3),
                        raw_data={
                            "sport": "AFL",
                            "competition": "AFL",
                            "round": round_name,
                            "event_type": "match",
                            "typical_crowd_impact": "high"
                            if "Grand Final" in round_name
                            else "medium",
                        },
                    )

                    # Cache if available
                    if self.cache:
                        self.cache.add_signal(signal)

                    signals.append(signal)
                    day_offset += 1

            except Exception as e:
                logger.warning(f"Error processing AFL round {round_name}: {e}")

        return signals

    async def _fetch_nrl_signals(
        self, location: Location, now: datetime
    ) -> list[FeedSignal]:
        """Generate NRL match signals for upcoming rounds.

        Args:
            location: The pub location
            now: Current datetime

        Returns:
            List of NRL-related FeedSignal objects
        """
        signals: list[FeedSignal] = []

        for round_name, round_data in NRL_2026_SEASON.items():
            try:
                round_start = datetime.fromisoformat(
                    round_data["start_date"]
                ).replace(tzinfo=TIMEZONE_AU)

                # Skip past rounds
                if round_start < now:
                    continue

                # Cap to 90 days ahead
                if round_start > now + timedelta(days=90):
                    break

                # NRL matches: typically Thu night (20:00), Fri night (20:00),
                # Sat afternoon (15:00), Sun afternoon (16:00)
                match_times = [
                    (20, 0),  # Thu/Fri night
                    (15, 0),  # Sat afternoon
                    (16, 0),  # Sun afternoon
                ]

                day_offset = 0
                for hour, minute in match_times:
                    match_datetime = round_start.replace(
                        hour=hour, minute=minute, second=0
                    ) + timedelta(days=day_offset)

                    if match_datetime < now:
                        day_offset += 1
                        continue

                    if match_datetime > now + timedelta(days=7):
                        break

                    # Determine signal strength
                    if "Grand Final" in round_name:
                        signal_strength = SignalStrength.extreme_positive
                    elif "Finals" in round_name:
                        signal_strength = SignalStrength.strong_positive
                    else:
                        signal_strength = SignalStrength.moderate_positive

                    signal = FeedSignal(
                        source=self.name,
                        category=self.category,
                        strength=signal_strength,
                        confidence=0.95,
                        start_time=match_datetime,
                        end_time=match_datetime + timedelta(hours=2, minutes=15),
                        raw_data={
                            "sport": "NRL",
                            "competition": "NRL",
                            "round": round_name,
                            "event_type": "match",
                            "typical_crowd_impact": "high"
                            if "Grand Final" in round_name
                            else "medium",
                        },
                    )

                    # Cache if available
                    if self.cache:
                        self.cache.add_signal(signal)

                    signals.append(signal)
                    day_offset += 1

            except Exception as e:
                logger.warning(f"Error processing NRL round {round_name}: {e}")

        return signals

    async def _fetch_major_event_signals(
        self, location: Location, now: datetime
    ) -> list[FeedSignal]:
        """Generate signals for major annual sporting events.

        Args:
            location: The pub location
            now: Current datetime

        Returns:
            List of FeedSignal objects for major events
        """
        signals: list[FeedSignal] = []

        for event_name, event_data in MAJOR_EVENTS.items():
            try:
                month = event_data.get("month")
                if not month:
                    continue

                # Estimate event date (simplified - actual date calculation
                # would handle "first Tuesday", "last Saturday", etc.)
                event_date = datetime(now.year, month, 15, tzinfo=TIMEZONE_AU)

                # Skip past events
                if event_date < now:
                    # Try next year
                    event_date = datetime(now.year + 1, month, 15, tzinfo=TIMEZONE_AU)

                # Cap to 12 months ahead
                if event_date > now + timedelta(days=365):
                    continue

                signal_strength = event_data.get(
                    "signal_strength", SignalStrength.moderate_positive
                )

                signal = FeedSignal(
                    source=self.name,
                    category=self.category,
                    strength=signal_strength,
                    confidence=0.9,
                    start_time=event_date,
                    end_time=event_date + timedelta(hours=4),
                    raw_data={
                        "event_name": event_name,
                        "event_type": "major_event",
                        "scope": event_data.get("scope", "unknown"),
                        "notes": event_data.get("notes", ""),
                    },
                )

                # Cache if available
                if self.cache:
                    self.cache.add_signal(signal)

                signals.append(signal)

            except Exception as e:
                logger.warning(f"Error processing major event {event_name}: {e}")

        return signals


class SportsAggregator(DataFeedAdapter):
    """Composite adapter that aggregates multiple sports data sources.

    Combines SportRadar, ESPN, and hardcoded Australian calendar data
    to provide comprehensive sports coverage with fallback capability.
    Deduplicates signals and returns the most confident predictions.
    """

    def __init__(
        self,
        sportradar_key: str | None = None,
        cache: SignalCache | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        """Initialize sports aggregator with multiple adapters.

        Args:
            sportradar_key: Optional SportRadar API key. If not provided,
                SportRadar adapter is skipped.
            cache: Optional signal cache for deduplication
            http_client: Optional httpx AsyncClient for custom configuration
        """
        super().__init__(
            name="SportsAggregator",
            category=FeedCategory.sports,
            cache=cache,
            cache_ttl_minutes=360,
        )

        self.adapters: list[DataFeedAdapter] = []

        # Add ESPN adapter (no auth required)
        self.adapters.append(
            ESPNAdapter(cache=cache, http_client=http_client)
        )

        # Add SportRadar adapter if key provided
        if sportradar_key:
            self.adapters.append(
                SportRadarAdapter(
                    api_key=sportradar_key,
                    cache=cache,
                    http_client=http_client,
                )
            )

        # Always add hardcoded fallback
        self.adapters.append(AUSportsCalendar(cache=cache))

        logger.info(
            f"SportsAggregator initialized with {len(self.adapters)} adapters"
        )

    async def fetch_signals(self, location: Location) -> list[FeedSignal]:
        """Fetch and aggregate sports signals from all sources.

        Attempts to fetch from all configured adapters and deduplicates
        results, preferring signals with higher confidence scores.

        Args:
            location: The pub location to fetch sports data for

        Returns:
            Deduplicated list of FeedSignal objects sorted by start time
        """
        all_signals: list[FeedSignal] = []

        # Fetch from all adapters in parallel
        for adapter in self.adapters:
            try:
                signals = await adapter.fetch_signals(location)
                all_signals.extend(signals)
                logger.debug(
                    f"{adapter.name} returned {len(signals)} signals for "
                    f"{location.name}"
                )
            except Exception as e:
                logger.warning(
                    f"Adapter {adapter.name} failed for {location.name}: {e}"
                )
                continue

        # Deduplicate signals (keep highest confidence for same event)
        deduped: dict[str, FeedSignal] = {}
        for signal in all_signals:
            # Create a key based on timing and raw data
            key = (
                f"{signal.start_time.isoformat()}_"
                f"{signal.raw_data.get('sport', 'unknown')}_"
                f"{signal.raw_data.get('round', 'unknown')}"
            )

            if key not in deduped or signal.confidence > deduped[key].confidence:
                deduped[key] = signal

        # Sort by start time
        final_signals = sorted(deduped.values(), key=lambda s: s.start_time)

        logger.info(
            f"SportsAggregator returning {len(final_signals)} deduplicated "
            f"signals for {location.name}"
        )

        return final_signals
