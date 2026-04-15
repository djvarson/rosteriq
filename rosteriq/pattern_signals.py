"""Pattern-to-Signals Bridge (Moment 7b: pattern learning → demand forecasting).

Converts learned patterns from ShiftEventStore into Signal objects that feed
the forecast engine. Enables patterns discovered from staff-logged events to
influence demand multipliers in real time.

The bridge filters patterns by venue, target date, and optionally hour,
then converts each to a Signal with appropriate impact type and scoring.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Optional

from rosteriq.shift_events import ShiftEventStore, PatternLearner, EventCategory, Pattern
from rosteriq.signal_feeds import Signal, SignalSourceType, SignalImpactType


# Mapping of EventCategory to SignalImpactType
_CATEGORY_TO_IMPACT = {
    EventCategory.WALK_IN_SURGE: SignalImpactType.POSITIVE,
    EventCategory.PUB_GROUP: SignalImpactType.POSITIVE,
    EventCategory.BUS_GROUP: SignalImpactType.POSITIVE,
    EventCategory.STADIUM_SPILLBACK: SignalImpactType.POSITIVE,
    EventCategory.WEATHER_SHIFT: SignalImpactType.NEGATIVE,
    EventCategory.BOOKING_NO_SHOW: SignalImpactType.NEGATIVE,
    EventCategory.STAFF_SHORTAGE: SignalImpactType.NEGATIVE,
    EventCategory.EQUIPMENT_ISSUE: SignalImpactType.NEGATIVE,
    EventCategory.CUSTOMER_INCIDENT: SignalImpactType.NEGATIVE,
    EventCategory.KITCHEN_BACKUP: SignalImpactType.NEGATIVE,
    EventCategory.OTHER: SignalImpactType.NEUTRAL,
}


async def patterns_to_signals(
    store: Optional[ShiftEventStore],
    venue_id: str,
    target_date: date,
    hour: Optional[int] = None,
) -> list[Signal]:
    """
    Convert patterns from ShiftEventStore to Signal objects.

    For a given venue and target date, queries all events, analyzes them for
    patterns, and filters by weekday (and hour, if provided). Converts each
    pattern to a Signal with appropriate impact type, scoring, and description.

    Args:
        store: ShiftEventStore instance. If None, returns empty list.
        venue_id: Venue identifier.
        target_date: Target date for pattern filtering (weekday-matched).
        hour: Optional hour (0-23). If provided, only patterns covering this hour
              are returned. If None, all patterns for that weekday are returned.

    Returns:
        List of Signal objects, one per pattern.
    """
    if store is None:
        return []

    # Query all events for this venue
    events = store.for_venue(venue_id)
    if not events:
        return []

    # Analyze to get all patterns
    all_patterns = PatternLearner.analyse(events)
    if not all_patterns:
        return []

    # Filter patterns by target weekday and optionally hour
    target_weekday = target_date.weekday()
    applicable_patterns = [
        p for p in all_patterns
        if p.weekday == target_weekday
        and (hour is None or (p.hour_window[0] <= hour < p.hour_window[1]))
    ]

    # Convert each pattern to a Signal
    signals = []
    for pattern in applicable_patterns:
        impact_type = _CATEGORY_TO_IMPACT.get(
            pattern.category, SignalImpactType.NEUTRAL
        )

        # Impact score: cap at 0.8 so patterns never dominate
        impact_score = min(pattern.confidence * 0.6, 0.8)

        # Description: include confidence and occurrence count
        description = f"{pattern.description} (observed {pattern.occurrences}x)"

        # Raw data: pattern as dict, stringify any non-serializable fields
        raw_dict = asdict(pattern)
        raw_dict["category"] = pattern.category.value
        raw_dict["hour_window"] = list(pattern.hour_window)

        signal = Signal(
            source=SignalSourceType.PATTERN,
            signal_type=impact_type,
            impact_score=impact_score,
            confidence=pattern.confidence,
            description=description,
            raw_data=raw_dict,
        )
        signals.append(signal)

    return signals
