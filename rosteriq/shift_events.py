"""In-memory shift event logger + pattern learner (Moment 7: event capture).

Pure-stdlib module for capturing on-shift events (surges, groups, incidents,
weather shifts) and learning repeating patterns over time. No FastAPI/Pydantic.
The FastAPI layer in api_v2 imports and delegates to the helpers here.

Why this matters: Staff logs events during shift. Over time the system learns
patterns like "every Friday you get a pub group surge at 6pm". When patterns
hit confidence threshold (3+ occurrences across distinct weeks), they are
queryable for forecasting and scheduling decisions.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, date, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Event enums and labels
# ---------------------------------------------------------------------------


class EventCategory(str, Enum):
    """Event categories staff can log during shift."""

    WALK_IN_SURGE = "walk_in_surge"
    PUB_GROUP = "pub_group"
    BUS_GROUP = "bus_group"
    STADIUM_SPILLBACK = "stadium_spillback"
    WEATHER_SHIFT = "weather_shift"
    BOOKING_NO_SHOW = "booking_no_show"
    STAFF_SHORTAGE = "staff_shortage"
    EQUIPMENT_ISSUE = "equipment_issue"
    CUSTOMER_INCIDENT = "customer_incident"
    KITCHEN_BACKUP = "kitchen_backup"
    OTHER = "other"


# Human-readable labels for each category
EVENT_CATEGORY_LABELS = {
    EventCategory.WALK_IN_SURGE: "Walk-in surge",
    EventCategory.PUB_GROUP: "Pub group",
    EventCategory.BUS_GROUP: "Bus group",
    EventCategory.STADIUM_SPILLBACK: "Stadium spillback",
    EventCategory.WEATHER_SHIFT: "Weather shift",
    EventCategory.BOOKING_NO_SHOW: "Booking no-show",
    EventCategory.STAFF_SHORTAGE: "Staff shortage",
    EventCategory.EQUIPMENT_ISSUE: "Equipment issue",
    EventCategory.CUSTOMER_INCIDENT: "Customer incident",
    EventCategory.KITCHEN_BACKUP: "Kitchen backup",
    EventCategory.OTHER: "Other",
}


# ---------------------------------------------------------------------------
# Data models (pure dataclasses)
# ---------------------------------------------------------------------------


@dataclass
class ShiftEvent:
    """A single event logged during a shift."""

    event_id: str
    venue_id: str
    category: EventCategory
    description: str
    timestamp: datetime
    headcount_at_time: Optional[int]
    logged_by: Optional[str]
    shift_date: date
    day_of_week: int  # 0=Monday, 6=Sunday
    hour_of_day: int
    weather_condition: Optional[str]
    active_event_ids: List[str]
    tags: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for API responses."""
        return {
            "event_id": self.event_id,
            "venue_id": self.venue_id,
            "category": self.category.value,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "headcount_at_time": self.headcount_at_time,
            "logged_by": self.logged_by,
            "shift_date": self.shift_date.isoformat(),
            "day_of_week": self.day_of_week,
            "hour_of_day": self.hour_of_day,
            "weather_condition": self.weather_condition,
            "active_event_ids": self.active_event_ids,
            "tags": self.tags,
        }


@dataclass
class Pattern:
    """A learned pattern from analysing events."""

    description: str
    category: EventCategory
    weekday: int  # 0=Monday, 6=Sunday
    hour_window: Tuple[int, int]  # (start_hour, end_hour)
    occurrences: int  # count of distinct events in this pattern
    confidence: float  # 0.0 to 1.0, based on (occurrences / weeks_observed)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for API responses."""
        return {
            "description": self.description,
            "category": self.category.value,
            "weekday": self.weekday,
            "hour_window": self.hour_window,
            "occurrences": self.occurrences,
            "confidence": round(self.confidence, 4),
        }


# ---------------------------------------------------------------------------
# In-memory store class
# ---------------------------------------------------------------------------

MAX_EVENTS = 1000  # per venue, bounds memory


class ShiftEventStore:
    """In-memory store for shift events, keyed by venue_id."""

    def __init__(self):
        self._store: Dict[str, List[ShiftEvent]] = {}

    def _venue_history(self, venue_id: str) -> List[ShiftEvent]:
        """Ensure venue exists in store and return its event list."""
        if venue_id not in self._store:
            self._store[venue_id] = []
        return self._store[venue_id]

    def record(self, event: ShiftEvent) -> ShiftEvent:
        """Store an event and return it."""
        hist = self._venue_history(event.venue_id)
        hist.append(event)
        if len(hist) > MAX_EVENTS:
            del hist[: len(hist) - MAX_EVENTS]
        return event

    def for_venue(self, venue_id: str, since: Optional[datetime] = None) -> List[ShiftEvent]:
        """List all events for a venue, optionally filtered by timestamp."""
        hist = self._venue_history(venue_id)
        if since is None:
            return list(hist)
        return [e for e in hist if e.timestamp >= since]

    def for_shift(self, venue_id: str, shift_date: date) -> List[ShiftEvent]:
        """List all events logged during a specific shift date."""
        hist = self._venue_history(venue_id)
        return [e for e in hist if e.shift_date == shift_date]

    def recent(self, venue_id: str, hours: int = 24) -> List[ShiftEvent]:
        """List events from the last N hours."""
        now = datetime.now(timezone.utc)
        cutoff = datetime.fromtimestamp(
            now.timestamp() - (hours * 3600), tz=timezone.utc
        )
        return self.for_venue(venue_id, since=cutoff)

    def all(self) -> List[ShiftEvent]:
        """Return all events across all venues (for pattern learning)."""
        result = []
        for venue_events in self._store.values():
            result.extend(venue_events)
        return result

    def clear_venue(self, venue_id: str) -> None:
        """Clear all events for a venue. Test helper."""
        if venue_id in self._store:
            del self._store[venue_id]

    def clear(self) -> None:
        """Wipe the entire store. Used by tests."""
        self._store.clear()

    def store(self) -> Dict[str, List[ShiftEvent]]:
        """Return the raw store dict. For diagnostics and tests."""
        return self._store


# ---------------------------------------------------------------------------
# Pattern learning
# ---------------------------------------------------------------------------


class PatternLearner:
    """Analyze events to learn repeating patterns."""

    @staticmethod
    def analyse(events: List[ShiftEvent]) -> List[Pattern]:
        """
        Analyse events and emit patterns with confidence >= 0.0.

        Groups events by (category, day_of_week, hour_bucket).
        If occurrences >= 3 across distinct weeks (i.e. 3+ weeks have
        seen an event in that bucket), emits a pattern.

        Confidence = min(occurrences / weeks_observed, 1.0).
        """
        if not events:
            return []

        # Group by (category, day_of_week, hour_bucket)
        # hour_bucket: group hours into 2-hour windows for readability
        buckets: Dict[Tuple[EventCategory, int, Tuple[int, int]], List[ShiftEvent]] = {}

        for event in events:
            hour_bucket_start = (event.hour_of_day // 2) * 2
            hour_bucket_end = min(hour_bucket_start + 2, 24)
            hour_window = (hour_bucket_start, hour_bucket_end)
            key = (event.category, event.day_of_week, hour_window)

            if key not in buckets:
                buckets[key] = []
            buckets[key].append(event)

        # Emit patterns for buckets with 3+ occurrences
        patterns: List[Pattern] = []
        for (category, day_of_week, hour_window), bucket_events in buckets.items():
            occurrences = len(bucket_events)
            if occurrences >= 3:
                # Count distinct weeks to compute confidence
                # (Use shift_date week number as proxy for week)
                weeks_seen = set()
                for event in bucket_events:
                    iso_cal = event.shift_date.isocalendar()
                    week_key = (iso_cal.year, iso_cal.week)
                    weeks_seen.add(week_key)

                weeks_observed = len(weeks_seen)
                confidence = min(occurrences / max(weeks_observed, 1), 1.0)

                day_names = [
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                    "Sunday",
                ]
                day_name = day_names[day_of_week]
                category_label = EVENT_CATEGORY_LABELS.get(
                    category, category.value
                )

                description = f"{category_label} on {day_name}s, {hour_window[0]:02d}:00–{hour_window[1]:02d}:00"

                patterns.append(
                    Pattern(
                        description=description,
                        category=category,
                        weekday=day_of_week,
                        hour_window=hour_window,
                        occurrences=occurrences,
                        confidence=confidence,
                    )
                )

        # Sort by confidence descending
        patterns.sort(key=lambda p: p.confidence, reverse=True)
        return patterns

    @staticmethod
    def predict_for(
        venue_id: str,
        target_date: date,
        hour: int,
        events: List[ShiftEvent],
    ) -> List[Pattern]:
        """
        Return patterns that apply to a specific weekday, hour, and venue.

        Filters all patterns to those matching:
        - Same day_of_week as target_date
        - hour falls within the pattern's hour_window
        - Same venue (implicit: only analyse events from that venue)
        """
        venue_events = [e for e in events if e.venue_id == venue_id]
        all_patterns = PatternLearner.analyse(venue_events)

        target_weekday = target_date.weekday()
        applicable = [
            p
            for p in all_patterns
            if p.weekday == target_weekday
            and p.hour_window[0] <= hour < p.hour_window[1]
        ]

        return applicable
