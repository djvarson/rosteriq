"""
Signal Bridge for RosterIQ
===========================

Bridges between data adapters (BOM weather, events) and Signal objects consumed
by the forecast engine. Converts WeatherForecastDay and VenueEvent objects into
standardized Signal objects with impact scoring, confidence, and descriptions.

Key functions:
  - weather_to_signals: WeatherAdapter forecast → list[Signal]
  - events_to_signals: EventsAdapter events → list[Signal]

Impact scoring rationale:
  - Heavy rain (>=10mm expected) → NEGATIVE, 0.6 impact (outdoor seating killer)
  - Rain probability >=60% → NEGATIVE, 0.3 impact (moderate concern)
  - Hot (>=32°C) → POSITIVE, 0.2 impact (drinks demand driver, but also cooling cost)
  - Cold (<=12°C) → NEGATIVE, 0.2 impact (indoor venues unaffected, outdoor suffers)
  - Events: impact scales with attendance/distance (haversine formula)

Confidence scoring:
  - Weather: 0.85 for <=3 days, 0.65 beyond (forecast uncertainty grows)
  - Events: 0.8 stadium, 0.65 concert/festival, 0.55 other (fixture variability)
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date, datetime, timedelta
from typing import Optional

from rosteriq.data_feeds.bom import WeatherForecastDay, WeatherAdapter
from rosteriq.data_feeds.events import VenueEvent, EventsAdapter
from rosteriq.signal_feeds import Signal, SignalSourceType, SignalImpactType

logger = logging.getLogger("rosteriq.signal_bridge")


async def weather_to_signals(
    adapter: WeatherAdapter,
    venue_id: str,
    date_range: tuple[date, date],
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> list[Signal]:
    """
    Convert weather forecast days to Signal objects.

    Pulls forecast from WeatherAdapter (e.g., DemoWeatherAdapter, BOMAdapter)
    and converts each WeatherForecastDay in the date range to a Signal with:
    - source=SignalSourceType.WEATHER
    - Heavy rain (>=10mm expected) → NEGATIVE, impact_score 0.6 (outdoor seating killer)
    - Rain probability >=60% → NEGATIVE, impact_score 0.3 (moderate risk)
    - Hot (>=32°C) → POSITIVE, impact_score 0.2 (drinks demand driver)
    - Cold (<=12°C) → NEGATIVE, impact_score 0.2 (outdoor venues suffer)
    - Otherwise NEUTRAL with impact_score 0.05
    - confidence = 0.85 for forecasts <=3 days out, 0.65 beyond
    - description = human-readable string like "Heavy rain forecast: 15mm expected"
    - raw_data = asdict(forecast_day)

    Args:
        adapter: WeatherAdapter instance (DemoWeatherAdapter, BOMAdapter, etc.)
        venue_id: Venue identifier (passed to adapter)
        date_range: (start_date, end_date) tuple, inclusive
        lat: Venue latitude (optional, not used by adapter but may be in future)
        lng: Venue longitude (optional, not used by adapter but may be in future)

    Returns:
        List of Signal objects, one per day in date_range with forecast data
    """
    start_date, end_date = date_range
    days = (end_date - start_date).days + 1

    try:
        forecasts = await adapter.get_forecast(venue_id, days)
    except Exception as e:
        logger.warning(f"Failed to fetch forecast for {venue_id}: {e}")
        return []

    signals = []
    today = date.today()

    for forecast in forecasts:
        # Filter to date_range
        if not (start_date <= forecast.date <= end_date):
            continue

        # Determine days ahead for confidence decay
        days_ahead = (forecast.date - today).days

        # Default: neutral, low impact
        impact_type = SignalImpactType.NEUTRAL
        impact_score = 0.05
        description = f"Conditions: {forecast.conditions}"

        # Heavy rain (>=10mm expected) → NEGATIVE, 0.6 impact (outdoor seating killer)
        if forecast.rain_mm_expected >= 10.0:
            impact_type = SignalImpactType.NEGATIVE
            impact_score = 0.6
            description = f"Heavy rain forecast: {forecast.rain_mm_expected:.1f}mm expected"

        # Rain probability >=60% → NEGATIVE, 0.3 impact
        elif forecast.rain_probability_pct >= 60.0:
            impact_type = SignalImpactType.NEGATIVE
            impact_score = 0.3
            description = (
                f"Significant rain probability: {forecast.rain_probability_pct:.0f}% chance"
            )

        # Hot (>=32°C) → POSITIVE, 0.2 impact (drinks demand + cooling concerns)
        elif forecast.max_c >= 32.0:
            impact_type = SignalImpactType.POSITIVE
            impact_score = 0.2
            description = f"Hot weather: {forecast.max_c:.1f}°C (drinks demand driver)"

        # Cold (<=12°C) → NEGATIVE, 0.2 impact
        elif forecast.max_c <= 12.0:
            impact_type = SignalImpactType.NEGATIVE
            impact_score = 0.2
            description = f"Cold weather: {forecast.max_c:.1f}°C (outdoor demand impact)"

        # Confidence decay: 0.85 for <=3 days, 0.65 beyond
        confidence = 0.85 if days_ahead <= 3 else 0.65

        signal = Signal(
            source=SignalSourceType.WEATHER,
            signal_type=impact_type,
            impact_score=impact_score,
            confidence=confidence,
            description=description,
            raw_data=asdict(forecast),
        )
        signals.append(signal)

    logger.debug(f"weather_to_signals: {venue_id} produced {len(signals)} signals")
    return signals


async def events_to_signals(
    adapter: EventsAdapter,
    venue_id: str,
    date_range: tuple[date, date],
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> list[Signal]:
    """
    Convert nearby events to Signal objects.

    Fetches events from EventsAdapter (e.g., DemoEventsAdapter, PerthIsOKAdapter)
    for a date range and converts each VenueEvent to a POSITIVE Signal with:
    - impact_score scales with expected_attendance and distance from venue
      - base = min(expected_attendance / 20000.0, 1.0)
      - if distance_km_from_venue is set: multiply by max(0, 1 - distance_km/10.0)
      - cap at 1.0
    - skip events with impact_score < 0.05 (too small/far to matter)
    - confidence based on category:
      - 0.8 for stadium events (fixed schedule, high capacity)
      - 0.65 for concert/festival (moderate fixture certainty)
      - 0.55 otherwise (less predictable)
    - description like "HBF Park game: 40000 attendance, 2.1km away"
    - raw_data = asdict(event) with datetimes isoformatted

    Args:
        adapter: EventsAdapter instance (DemoEventsAdapter, CompositeEventsAdapter, etc.)
        venue_id: Venue identifier
        date_range: (start_date, end_date) tuple, inclusive
        lat: Venue latitude (optional, for distance calculation)
        lng: Venue longitude (optional, for distance calculation)

    Returns:
        List of POSITIVE Signal objects, one per event with impact_score >= 0.05
    """
    start_date, end_date = date_range

    # Convert date_range to datetime window (start of day to end of day)
    window_start = datetime.combine(start_date, datetime.min.time()).replace(
        tzinfo=datetime.now().astimezone().tzinfo
    )
    window_end = datetime.combine(end_date, datetime.max.time()).replace(
        tzinfo=datetime.now().astimezone().tzinfo
    )

    try:
        events = await adapter.get_events(venue_id, window_start, window_end)
    except Exception as e:
        logger.warning(f"Failed to fetch events for {venue_id}: {e}")
        return []

    signals = []

    for event in events:
        # Impact scoring: base on attendance, adjust for distance
        base_impact = min((event.expected_attendance or 0) / 20000.0, 1.0)

        # Distance adjustment: 1 - distance_km/10.0 (max 0)
        # Event at 0km = 1.0x, at 5km = 0.5x, at 10km+ = 0.0x
        distance = event.distance_km_from_venue or 0.0
        distance_factor = max(0, 1.0 - (distance / 10.0))
        impact_score = min(base_impact * distance_factor, 1.0)

        # Skip if too small/far
        if impact_score < 0.05:
            continue

        # Category-based confidence
        category = event.category or "other"
        if category == "stadium":
            confidence = 0.8
        elif category in ("concert", "festival"):
            confidence = 0.65
        else:
            confidence = 0.55

        # Description
        description = f"{event.title}"
        if event.expected_attendance:
            description += f": {event.expected_attendance:,} expected"
        if distance > 0:
            description += f", {distance:.1f}km away"

        # Raw data: convert event to dict, isoformat datetimes
        raw_dict = asdict(event)
        if "start_time" in raw_dict and isinstance(raw_dict["start_time"], datetime):
            raw_dict["start_time"] = raw_dict["start_time"].isoformat()
        if "end_time" in raw_dict and isinstance(raw_dict["end_time"], datetime):
            raw_dict["end_time"] = raw_dict["end_time"].isoformat()

        signal = Signal(
            source=SignalSourceType.EVENTS,
            signal_type=SignalImpactType.POSITIVE,
            impact_score=impact_score,
            confidence=confidence,
            description=description,
            raw_data=raw_dict,
        )
        signals.append(signal)

    logger.debug(f"events_to_signals: {venue_id} produced {len(signals)} signals")
    return signals
