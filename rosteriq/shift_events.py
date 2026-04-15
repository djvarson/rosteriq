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

import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, date, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.shift_events")


# Round 12 — SQLite schema
_SCHEMA = """
CREATE TABLE IF NOT EXISTS shift_events (
    event_id           TEXT PRIMARY KEY,
    venue_id           TEXT NOT NULL,
    category           TEXT NOT NULL,
    description        TEXT NOT NULL,
    timestamp          TEXT NOT NULL,
    headcount_at_time  INTEGER,
    logged_by          TEXT,
    shift_date         TEXT NOT NULL,
    day_of_week        INTEGER NOT NULL,
    hour_of_day        INTEGER NOT NULL,
    weather_condition  TEXT,
    active_event_ids   TEXT NOT NULL,  -- JSON array
    tags               TEXT NOT NULL   -- JSON array
);
CREATE INDEX IF NOT EXISTS ix_shift_events_venue ON shift_events(venue_id);
CREATE INDEX IF NOT EXISTS ix_shift_events_ts ON shift_events(timestamp);
"""
_p.register_schema("shift_events", _SCHEMA)


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
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Persistence (Round 12)
    # ------------------------------------------------------------------

    def _event_to_row(self, e: ShiftEvent) -> Dict[str, Any]:
        return {
            "event_id": e.event_id,
            "venue_id": e.venue_id,
            "category": e.category.value,
            "description": e.description,
            "timestamp": e.timestamp.isoformat(),
            "headcount_at_time": e.headcount_at_time,
            "logged_by": e.logged_by,
            "shift_date": e.shift_date.isoformat(),
            "day_of_week": e.day_of_week,
            "hour_of_day": e.hour_of_day,
            "weather_condition": e.weather_condition,
            "active_event_ids": _p.json_dumps(e.active_event_ids),
            "tags": _p.json_dumps(e.tags),
        }

    def _row_to_event(self, r) -> ShiftEvent:
        return ShiftEvent(
            event_id=r["event_id"],
            venue_id=r["venue_id"],
            category=EventCategory(r["category"]),
            description=r["description"],
            timestamp=datetime.fromisoformat(r["timestamp"]),
            headcount_at_time=r["headcount_at_time"],
            logged_by=r["logged_by"],
            shift_date=date.fromisoformat(r["shift_date"]),
            day_of_week=r["day_of_week"],
            hour_of_day=r["hour_of_day"],
            weather_condition=r["weather_condition"],
            active_event_ids=_p.json_loads(r["active_event_ids"], default=[]) or [],
            tags=_p.json_loads(r["tags"], default=[]) or [],
        )

    def _persist(self, e: ShiftEvent) -> None:
        _p.upsert("shift_events", self._event_to_row(e), pk="event_id")

    def rehydrate(self) -> None:
        if not _p.is_persistence_enabled():
            return
        rows = _p.fetchall("SELECT * FROM shift_events ORDER BY timestamp ASC")
        with self._lock:
            for r in rows:
                try:
                    ev = self._row_to_event(r)
                    self._store.setdefault(ev.venue_id, []).append(ev)
                except Exception as ex:
                    logger.warning("rehydrate shift event failed: %s", ex)
        logger.info("Shift events rehydrated: %d rows", len(rows))

    def _venue_history(self, venue_id: str) -> List[ShiftEvent]:
        """Ensure venue exists in store and return its event list.

        Caller must hold self._lock when mutating the returned list.
        """
        if venue_id not in self._store:
            self._store[venue_id] = []
        return self._store[venue_id]

    def record(self, event: ShiftEvent) -> ShiftEvent:
        """Store an event and return it. Thread-safe."""
        with self._lock:
            hist = self._venue_history(event.venue_id)
            hist.append(event)
            if len(hist) > MAX_EVENTS:
                del hist[: len(hist) - MAX_EVENTS]
        self._persist(event)
        return event

    def for_venue(self, venue_id: str, since: Optional[datetime] = None) -> List[ShiftEvent]:
        """List all events for a venue, optionally filtered by timestamp."""
        with self._lock:
            hist = self._venue_history(venue_id)
            snapshot = list(hist)
        if since is None:
            return snapshot
        return [e for e in snapshot if e.timestamp >= since]

    def for_shift(self, venue_id: str, shift_date: date) -> List[ShiftEvent]:
        """List all events logged during a specific shift date."""
        with self._lock:
            hist = self._venue_history(venue_id)
            snapshot = list(hist)
        return [e for e in snapshot if e.shift_date == shift_date]

    def recent(self, venue_id: str, hours: int = 24) -> List[ShiftEvent]:
        """List events from the last N hours."""
        now = datetime.now(timezone.utc)
        cutoff = datetime.fromtimestamp(
            now.timestamp() - (hours * 3600), tz=timezone.utc
        )
        return self.for_venue(venue_id, since=cutoff)

    def all(self) -> List[ShiftEvent]:
        """Return all events across all venues (for pattern learning)."""
        with self._lock:
            result = []
            for venue_events in self._store.values():
                result.extend(venue_events)
        return result

    def clear_venue(self, venue_id: str) -> None:
        """Clear all events for a venue. Test helper."""
        with self._lock:
            if venue_id in self._store:
                del self._store[venue_id]
        if _p.is_persistence_enabled():
            try:
                with _p.write_txn() as c:
                    c.execute("DELETE FROM shift_events WHERE venue_id = ?", [venue_id])
            except Exception as e:
                logger.warning("shift_events venue delete failed: %s", e)

    def clear(self) -> None:
        """Wipe the entire store. Used by tests."""
        with self._lock:
            self._store.clear()
        if _p.is_persistence_enabled():
            try:
                with _p.write_txn() as c:
                    c.execute("DELETE FROM shift_events")
            except Exception as e:
                logger.warning("shift_events clear failed: %s", e)

    def store(self) -> Dict[str, List[ShiftEvent]]:
        """Return the raw store dict. For diagnostics and tests."""
        return self._store


# Module-level singleton (use this instead of constructing directly so
# persistence rehydration can be wired up).
_store_singleton: Optional[ShiftEventStore] = None


def get_shift_event_store() -> ShiftEventStore:
    global _store_singleton
    if _store_singleton is None:
        _store_singleton = ShiftEventStore()
    return _store_singleton


@_p.on_init
def _rehydrate_shift_events_on_init() -> None:
    get_shift_event_store().rehydrate()


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
