"""
RosterIQ Demand Forecasting Engine

Converts historical POS data and real-time signals into hourly demand forecasts
for roster generation. Combines:

1. Historical baseline: Learn from past revenue patterns by day-of-week
2. Signal adjustment: Apply demand multipliers from weather, events, bookings
3. Weekly forecast generation: 7-day hourly demand forecasts
4. Confidence scoring: Higher when signals agree, lower when sparse/conflicting
5. Demo mode: Realistic Brisbane hospitality patterns when no real data

The engine outputs DemandForecast objects that feed into roster_engine.py.

All methods are async. Full type hints and docstrings included.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time
from typing import Dict, List, Optional, Tuple
from enum import Enum

from rosteriq.roster_engine import DemandForecast, Role

logger = logging.getLogger("rosteriq.forecast_engine")


# ============================================================================
# Data Models
# ============================================================================

class ForecastTimeSlot(str, Enum):
    """Service time slots for demand patterns."""
    BREAKFAST = "breakfast"      # 6am-11am
    LUNCH = "lunch"              # 11am-3pm
    AFTERNOON = "afternoon"      # 3pm-5pm
    DINNER = "dinner"            # 5pm-10pm
    LATE_NIGHT = "late_night"    # 10pm-12am


@dataclass
class DailyBaseline:
    """Historical baseline for a single day of week."""
    day_of_week: int  # 0=Monday, 6=Sunday
    day_name: str
    total_covers: float  # Average covers for this day
    hourly_covers: Dict[int, float]  # Hour 0-23 → average covers
    hourly_revenue: Dict[int, float]  # Hour 0-23 → average revenue AUD
    confidence: float  # 0-1, based on data points


# ============================================================================
# Forecasting Ratios & Constants
# ============================================================================

# Revenue to covers conversion
REVENUE_PER_COVER = {
    "lunch": 45.0,    # $45 avg spend at lunch
    "dinner": 65.0,   # $65 avg spend at dinner
}

# Covers per staff member (inverse ratio)
COVERS_PER_STAFF = {
    Role.FLOOR: 17.5,      # 1 floor staff per 15-20 covers (midpoint 17.5)
    Role.BAR: 27.5,        # 1 bar staff per 25-30 covers (midpoint 27.5)
    Role.KITCHEN: 22.5,    # 1 kitchen staff per 20-25 covers (midpoint 22.5)
}

# Opening hours: when staff are rostered for each role
VENUE_HOURS = {
    Role.FLOOR: (11, 22),      # 11am-10pm
    Role.BAR: (11, 24),        # 11am-12am
    Role.KITCHEN: (11, 22),    # 11am-10pm (same as floor)
    Role.MANAGER: (11, 22),    # 11am-10pm
}

# Minimum staff required for shift (even if low demand)
MIN_STAFF_PER_SHIFT = {
    Role.FLOOR: 1,
    Role.BAR: 1,
    Role.KITCHEN: 1,
    Role.MANAGER: 1,  # Always 1 manager per shift
}


# ============================================================================
# Demo Data Patterns (Brisbane Hospitality)
# ============================================================================

DEMO_HOURLY_COVERS = {
    # Monday-Wednesday: quieter
    "quiet": {
        11: 5, 12: 12, 13: 8, 14: 3, 15: 2, 16: 2, 17: 3,
        18: 8, 19: 15, 20: 12, 21: 5,
    },
    # Thursday: picks up
    "medium": {
        11: 8, 12: 18, 13: 12, 14: 5, 15: 3, 16: 3, 17: 5,
        18: 12, 19: 22, 20: 18, 21: 8,
    },
    # Friday: busy
    "busy": {
        11: 12, 12: 25, 13: 18, 14: 8, 15: 5, 16: 5, 17: 8,
        18: 18, 19: 30, 20: 25, 21: 12,
    },
    # Saturday: busiest
    "peak": {
        11: 18, 12: 35, 13: 25, 14: 10, 15: 8, 16: 8, 17: 12,
        18: 25, 19: 40, 20: 35, 21: 18,
    },
    # Sunday: moderate
    "medium_eve": {
        11: 15, 12: 30, 13: 20, 14: 8, 15: 5, 16: 5, 17: 8,
        18: 12, 19: 18, 20: 12, 21: 5,
    },
}


# ============================================================================
# Forecast Engine
# ============================================================================

class ForecastEngine:
    """
    Demand forecasting engine for hourly rostering.

    Converts POS historical data and real-time signals into hourly demand
    forecasts for each role. Supports both real data sources and demo mode.

    Public API:
        forecast_week(venue_id, week_start_date) → List[DemandForecast]
        get_daily_forecast(venue_id, date) → DemandForecast
    """

    def __init__(
        self,
        pos_adapter: Optional[object] = None,
        signal_aggregator: Optional[object] = None,
        demo_mode: bool = False,
    ):
        """
        Initialize forecast engine.

        Args:
            pos_adapter: POSAdapter instance for historical POS data.
                        If None and demo_mode=False, will use DemoSwiftPOSAdapter.
            signal_aggregator: SignalAggregator instance for demand signals.
                              If None and demo_mode=False, will create one.
            demo_mode: Force demo data (realistic Brisbane patterns) regardless
                      of adapter availability.
        """
        self.pos_adapter = pos_adapter
        self.signal_aggregator = signal_aggregator
        self.demo_mode = demo_mode
        self._baseline_cache: Dict[int, DailyBaseline] = {}
        self._cache_valid_until: Optional[datetime] = None

        logger.info(
            f"ForecastEngine initialized (demo_mode={demo_mode}, "
            f"has_pos={'yes' if pos_adapter else 'no'}, "
            f"has_signals={'yes' if signal_aggregator else 'no'})"
        )

    async def forecast_week(
        self,
        venue_id: str,
        week_start_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> List[DemandForecast]:
        """
        Generate hourly demand forecasts for a full week (7 days).

        Args:
            venue_id: Venue identifier
            week_start_date: Monday start date (YYYY-MM-DD format or date object)
            location_lat: Optional venue latitude for signal feeds
            location_lng: Optional venue longitude for signal feeds

        Returns:
            List of 7 DemandForecast objects (one per day of week)
        """
        if isinstance(week_start_date, str):
            week_start_date = datetime.strptime(week_start_date, "%Y-%m-%d").date()

        forecasts = []
        for offset in range(7):
            target_date = week_start_date + timedelta(days=offset)
            forecast = await self.get_daily_forecast(
                venue_id,
                target_date,
                location_lat=location_lat,
                location_lng=location_lng,
            )
            forecasts.append(forecast)

        logger.info(
            f"Generated week forecast for {venue_id} starting {week_start_date}"
        )
        return forecasts

    async def get_daily_forecast(
        self,
        venue_id: str,
        target_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> DemandForecast:
        """
        Generate hourly demand forecast for a single day.

        Process:
        1. Retrieve historical baseline for this day of week
        2. Get signal multipliers from weather, events, bookings
        3. Apply role-specific adjustments
        4. Validate minimum staffing constraints
        5. Calculate confidence score

        Args:
            venue_id: Venue identifier
            target_date: Date to forecast (YYYY-MM-DD format or date object)
            location_lat: Optional venue latitude for signal feeds
            location_lng: Optional venue longitude for signal feeds

        Returns:
            DemandForecast object with hourly_demand Dict[hour → {role: count}]
        """
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()

        date_str = target_date.isoformat()
        day_of_week = target_date.weekday()  # 0=Monday, 6=Sunday

        # Step 1: Get historical baseline
        baseline = await self._get_daily_baseline(day_of_week)

        # Step 2: Get demand multipliers from signals
        multiplier, signal_descriptions = await self._get_demand_multipliers(
            venue_id,
            target_date,
            location_lat=location_lat,
            location_lng=location_lng,
        )

        # Step 3: Calculate hourly demand with role split
        hourly_demand: Dict[int, Dict[Role, float]] = {}
        total_covers = 0.0

        for hour in range(24):
            base_covers = baseline.hourly_covers.get(hour, 0.0)
            adjusted_covers = base_covers * multiplier

            # Skip hours with no demand
            if adjusted_covers < 0.1:
                continue

            # Role-specific allocation based on time of day
            role_demand = self._allocate_roles(hour, adjusted_covers)
            hourly_demand[hour] = role_demand
            total_covers += adjusted_covers

        # Step 4: Apply minimum staffing constraints
        self._apply_minimum_staffing(hourly_demand, baseline.day_of_week)

        # Step 5: Calculate confidence score
        confidence = self._calculate_confidence(
            baseline, len(signal_descriptions), multiplier
        )

        forecast = DemandForecast(
            date=date_str,
            hourly_demand=hourly_demand,
            total_covers_expected=total_covers,
            signals=signal_descriptions,
            confidence=confidence,
        )

        logger.debug(
            f"Forecast for {venue_id} on {date_str}: "
            f"{int(total_covers)} covers, confidence={confidence:.2f}, "
            f"multiplier={multiplier:.2f}"
        )

        return forecast

    async def _get_daily_baseline(self, day_of_week: int) -> DailyBaseline:
        """
        Get or compute historical baseline for a day of week.

        Queries historical POS data for 4-8 weeks of patterns and averages
        by hour. Falls back to demo data if no real data available.

        Args:
            day_of_week: 0=Monday, 6=Sunday

        Returns:
            DailyBaseline with hourly_covers and hourly_revenue
        """
        # Check cache
        if day_of_week in self._baseline_cache:
            if self._cache_valid_until and datetime.now() < self._cache_valid_until:
                return self._baseline_cache[day_of_week]

        if self.demo_mode or self.pos_adapter is None:
            baseline = self._demo_baseline(day_of_week)
        else:
            baseline = await self._real_baseline(day_of_week)

        self._baseline_cache[day_of_week] = baseline
        self._cache_valid_until = datetime.now() + timedelta(hours=24)

        return baseline

    def _demo_baseline(self, day_of_week: int) -> DailyBaseline:
        """Generate demo baseline for Brisbane hospitality patterns."""
        day_names = [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ]
        day_name = day_names[day_of_week]

        # Select pattern based on day of week
        if day_of_week <= 2:  # Mon-Wed: quiet
            pattern = DEMO_HOURLY_COVERS["quiet"]
            base_covers = 65.0
        elif day_of_week == 3:  # Thu: medium
            pattern = DEMO_HOURLY_COVERS["medium"]
            base_covers = 85.0
        elif day_of_week == 4:  # Fri: busy
            pattern = DEMO_HOURLY_COVERS["busy"]
            base_covers = 150.0
        elif day_of_week == 5:  # Sat: peak
            pattern = DEMO_HOURLY_COVERS["peak"]
            base_covers = 195.0
        else:  # Sun: medium evening
            pattern = DEMO_HOURLY_COVERS["medium_eve"]
            base_covers = 135.0

        # Build hourly breakdown
        hourly_covers: Dict[int, float] = {}
        hourly_revenue: Dict[int, float] = {}

        for hour in range(24):
            if hour not in pattern:
                hourly_covers[hour] = 0.0
                hourly_revenue[hour] = 0.0
            else:
                covers = float(pattern[hour])
                hourly_covers[hour] = covers

                # Estimate revenue: lunch ~$45/cover, dinner ~$65/cover
                if 11 <= hour < 15:  # Lunch window
                    revenue = covers * REVENUE_PER_COVER["lunch"]
                elif 17 <= hour < 22:  # Dinner window
                    revenue = covers * REVENUE_PER_COVER["dinner"]
                else:
                    # Bar/drink only periods
                    revenue = covers * 20.0

                hourly_revenue[hour] = revenue

        return DailyBaseline(
            day_of_week=day_of_week,
            day_name=day_name,
            total_covers=base_covers,
            hourly_covers=hourly_covers,
            hourly_revenue=hourly_revenue,
            confidence=0.9,  # Demo data has high confidence
        )

    async def _real_baseline(self, day_of_week: int) -> DailyBaseline:
        """
        Query real POS data to build historical baseline.

        Fetches last 4-8 weeks of data, filters by day of week,
        and averages hourly patterns.
        """
        try:
            # Query last 8 weeks of history
            end_date = datetime.now()
            start_date = end_date - timedelta(days=56)

            daily_summaries = await self.pos_adapter.get_daily_summary(
                start_date, end_date
            )

            # Filter by day of week and extract patterns
            matching_days = [
                d for d in daily_summaries
                if d.date.weekday() == day_of_week
            ]

            if not matching_days:
                logger.warning(
                    f"No POS data for day {day_of_week}, using demo baseline"
                )
                return self._demo_baseline(day_of_week)

            # Average hourly patterns
            hourly_covers: Dict[int, float] = {}
            hourly_revenue: Dict[int, float] = {}

            for hour in range(24):
                hour_values = []
                hour_revenues = []

                for summary in matching_days:
                    for hourly in summary.hourly_breakdown:
                        if hourly.hour == hour:
                            hour_values.append(hourly.covers)
                            hour_revenues.append(hourly.revenue)

                if hour_values:
                    hourly_covers[hour] = sum(hour_values) / len(hour_values)
                    hourly_revenue[hour] = sum(hour_revenues) / len(hour_revenues)
                else:
                    hourly_covers[hour] = 0.0
                    hourly_revenue[hour] = 0.0

            total_covers = sum(hourly_covers.values())
            confidence = min(len(matching_days) / 8.0, 1.0)  # Max confidence at 8+ samples

            day_names = [
                "Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday",
            ]

            return DailyBaseline(
                day_of_week=day_of_week,
                day_name=day_names[day_of_week],
                total_covers=total_covers,
                hourly_covers=hourly_covers,
                hourly_revenue=hourly_revenue,
                confidence=confidence,
            )

        except Exception as e:
            logger.error(f"Error fetching real baseline: {e}, using demo")
            return self._demo_baseline(day_of_week)

    async def _get_demand_multipliers(
        self,
        venue_id: str,
        target_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> Tuple[float, List[str]]:
        """
        Calculate demand multiplier from all signal feeds.

        Aggregates signals from:
        - Weather (rain/extreme temps reduce demand)
        - Events (sports, festivals increase demand)
        - Bookings (confirmed bookings boost demand)
        - Foot traffic trends
        - Delivery analytics

        Returns:
            (multiplier: 0.5-1.5, signal_descriptions: list of strings)
        """
        if self.demo_mode or self.signal_aggregator is None:
            return self._demo_multiplier(target_date)

        try:
            multiplier = await self.signal_aggregator.get_demand_multiplier(
                venue_id,
                target_date,
                location_lat=location_lat,
                location_lng=location_lng,
            )

            summary = await self.signal_aggregator.get_signal_summary(
                venue_id,
                target_date,
                location_lat=location_lat,
                location_lng=location_lng,
            )

            signals = [summary] if summary else []
            return multiplier, signals

        except Exception as e:
            logger.warning(f"Error getting signals: {e}, using neutral multiplier")
            return 1.0, []

    def _demo_multiplier(self, target_date: date) -> Tuple[float, List[str]]:
        """Generate demo multipliers with realistic variations."""
        day_of_week = target_date.weekday()

        # Stable base patterns
        if day_of_week == 4:  # Friday
            return 1.2, ["Friday premium: +20% expected"]
        elif day_of_week == 5:  # Saturday
            return 1.3, ["Saturday peak: +30% expected"]
        elif day_of_week in [0, 1, 2]:  # Mon-Wed
            return 0.9, ["Weekday discount: -10% expected"]
        else:
            return 1.0, []

    def _allocate_roles(self, hour: int, total_covers: float) -> Dict[Role, float]:
        """
        Allocate covers to roles based on time of day and business patterns.

        Lunch/dinner typically: 60% floor, 25% bar, 15% kitchen
        Late night: 70% bar, 30% kitchen (minimal food)
        All hours: 1 manager minimum
        """
        if total_covers < 0.1:
            return {
                Role.FLOOR: 0.0,
                Role.BAR: 0.0,
                Role.KITCHEN: 0.0,
                Role.MANAGER: 1.0,
            }

        # Determine service period
        if 11 <= hour < 15:  # Lunch
            # Lunch: heavy on floor service
            allocation = {
                Role.FLOOR: total_covers / COVERS_PER_STAFF[Role.FLOOR],
                Role.BAR: total_covers / COVERS_PER_STAFF[Role.BAR] * 0.6,
                Role.KITCHEN: total_covers / COVERS_PER_STAFF[Role.KITCHEN],
                Role.MANAGER: 1.0,
            }
        elif 17 <= hour < 22:  # Dinner
            # Dinner: balanced service
            allocation = {
                Role.FLOOR: total_covers / COVERS_PER_STAFF[Role.FLOOR],
                Role.BAR: total_covers / COVERS_PER_STAFF[Role.BAR],
                Role.KITCHEN: total_covers / COVERS_PER_STAFF[Role.KITCHEN],
                Role.MANAGER: 1.0,
            }
        elif 22 <= hour or hour < 11:  # Before 11am or after 10pm
            # Late night: minimal service
            allocation = {
                Role.FLOOR: 0.0,
                Role.BAR: total_covers / COVERS_PER_STAFF[Role.BAR] * 0.8,
                Role.KITCHEN: 0.0,
                Role.MANAGER: 0.0,
            }
        else:  # 15-17 afternoon
            # Afternoon lull: minimal staff
            allocation = {
                Role.FLOOR: total_covers / COVERS_PER_STAFF[Role.FLOOR] * 0.5,
                Role.BAR: total_covers / COVERS_PER_STAFF[Role.BAR] * 0.4,
                Role.KITCHEN: total_covers / COVERS_PER_STAFF[Role.KITCHEN] * 0.3,
                Role.MANAGER: 0.0,
            }

        return allocation

    def _apply_minimum_staffing(
        self,
        hourly_demand: Dict[int, Dict[Role, float]],
        day_of_week: int,
    ) -> None:
        """
        Enforce minimum staff levels per role per hour.

        Ensures every staffed hour has minimum viable team.
        """
        for hour in hourly_demand:
            for role, min_staff in MIN_STAFF_PER_SHIFT.items():
                if hour in hourly_demand and role in hourly_demand[hour]:
                    current = hourly_demand[hour][role]
                    if current > 0:
                        # If staffed, ensure minimum
                        hourly_demand[hour][role] = max(current, min_staff)
                elif VENUE_HOURS[role][0] <= hour < VENUE_HOURS[role][1]:
                    # If within venue operating hours for this role
                    if hour not in hourly_demand:
                        hourly_demand[hour] = {}

                    if (hourly_demand[hour].get(role, 0) == 0 and
                        role == Role.MANAGER and
                        any(hourly_demand[hour].values())):
                        # Always have manager if any other staff
                        hourly_demand[hour][role] = min_staff

    def _calculate_confidence(
        self,
        baseline: DailyBaseline,
        signal_count: int,
        multiplier: float,
    ) -> float:
        """
        Calculate confidence score for forecast.

        Factors:
        - Historical data confidence (more samples = higher)
        - Signal agreement (more signals = higher, unless conflicting)
        - Multiplier magnitude (extreme values = lower confidence)

        Returns:
            Float 0.0-1.0
        """
        # Base confidence from historical data
        base_confidence = baseline.confidence

        # Signal agreement boost
        signal_boost = min(signal_count * 0.05, 0.15)

        # Multiplier confidence: extreme values are less certain
        if 0.9 <= multiplier <= 1.1:
            multiplier_confidence = 1.0
        elif 0.7 <= multiplier <= 1.3:
            multiplier_confidence = 0.9
        else:
            multiplier_confidence = 0.75

        # Combined confidence
        confidence = (base_confidence * 0.6 +
                     (0.5 + signal_boost) * 0.25 +
                     multiplier_confidence * 0.15)

        return min(max(confidence, 0.5), 1.0)  # Clamp to 0.5-1.0


# ============================================================================
# Factory Function
# ============================================================================

async def get_forecast_engine(
    pos_adapter: Optional[object] = None,
    signal_aggregator: Optional[object] = None,
    demo_mode: bool = False,
) -> ForecastEngine:
    """
    Factory function to create a ForecastEngine with real or demo data.

    If credentials are available for POS and signals APIs, real data is used.
    Otherwise, defaults to realistic Brisbane hospitality demo patterns.

    Args:
        pos_adapter: POSAdapter instance (e.g., SwiftPOSClient, DemoSwiftPOSAdapter).
                    If None, attempts to auto-detect from environment.
        signal_aggregator: SignalAggregator instance. If None, creates default.
        demo_mode: Force demo data regardless of adapter availability.

    Returns:
        Initialized ForecastEngine ready for forecast_week() calls
    """
    # Auto-detect POS adapter if not provided
    if pos_adapter is None and not demo_mode:
        try:
            from rosteriq.pos_adapter import SwiftPOSClient, DemoSwiftPOSAdapter

            # Try real API first
            if os.getenv("SWIFTPOS_API_KEY"):
                pos_adapter = SwiftPOSClient()
                logger.info("Using real SwiftPOS adapter")
            else:
                pos_adapter = DemoSwiftPOSAdapter()
                logger.info("Using demo POS adapter")
        except Exception as e:
            logger.warning(f"Could not init POS adapter: {e}, using demo mode")
            demo_mode = True

    # Auto-detect signal aggregator if not provided
    if signal_aggregator is None and not demo_mode:
        try:
            from rosteriq.signal_feeds import SignalAggregator

            signal_aggregator = SignalAggregator()
            logger.info("Using real signal aggregator")
        except Exception as e:
            logger.warning(f"Could not init signal aggregator: {e}")
            signal_aggregator = None

    engine = ForecastEngine(
        pos_adapter=pos_adapter,
        signal_aggregator=signal_aggregator,
        demo_mode=demo_mode,
    )

    logger.info(f"ForecastEngine created (demo_mode={demo_mode})")
    return engine


# ============================================================================
# Utility Functions
# ============================================================================

def get_covers_to_staff_ratio(role: Role) -> float:
    """
    Get covers-per-staff ratio for a role (inverse of COVERS_PER_STAFF).

    Example: Role.FLOOR has ratio 17.5 covers/staff, so 1/17.5 = 0.057 staff/cover.

    Args:
        role: Role enum value

    Returns:
        Staff count per single cover (0.05-0.067)
    """
    return 1.0 / COVERS_PER_STAFF.get(role, 20.0)


def estimate_staff_for_covers(covers: float, role: Role) -> float:
    """
    Estimate staff needed for a given number of covers.

    Args:
        covers: Number of customer covers
        role: Role enum value

    Returns:
        Estimated staff count (fractional)
    """
    return covers * get_covers_to_staff_ratio(role)


# Import os at module level for factory function
import os
