"""Pattern detection and learning loop module (Round 13).

Analyzes accumulated shift notes, headcount data, and Tanda history to surface
recurring patterns that feed back into the forecast engine. Implements the
"give it data so it pumps out better data" concept.

Key features:
- LearnedPattern dataclass with confidence, evidence, and impact metrics
- PatternStore: thread-safe registry persisted via rosteriq.persistence
- Detectors for day-of-week, headcount trends, note tags, and weather impacts
- Pattern deduplication: updates existing patterns rather than creating duplicates
- Access control integration: API endpoints gated by access level

Why this matters: historical patterns enable the forecast engine to predict
demand spikes (Friday evenings), staff peaks (dinner services), and external
events more accurately than a naive algorithm would.
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.pattern_learner")

AU_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Schema registration
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS learned_patterns (
    pattern_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    pattern_type TEXT NOT NULL,
    description TEXT NOT NULL,
    confidence REAL NOT NULL,
    evidence_count INTEGER NOT NULL,
    day_of_week INTEGER,
    hour_start INTEGER,
    hour_end INTEGER,
    impact_pct REAL NOT NULL DEFAULT 0,
    tags TEXT NOT NULL DEFAULT '[]',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_patterns_venue ON learned_patterns(venue_id);
CREATE INDEX IF NOT EXISTS ix_patterns_type ON learned_patterns(pattern_type);
"""
_p.register_schema("learned_patterns", _SCHEMA)


# ---------------------------------------------------------------------------
# Dataclass: LearnedPattern
# ---------------------------------------------------------------------------


@dataclass
class LearnedPattern:
    """A detected recurring pattern in venue operations."""

    pattern_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    venue_id: str = ""
    pattern_type: str = ""  # recurring_event, day_of_week, weather_impact, seasonal, headcount_trend, tag_cluster
    description: str = ""  # Human-readable e.g. "Friday evenings see 40% more patrons"
    confidence: float = 0.0  # 0.0 to 1.0
    evidence_count: int = 0  # How many data points support this
    day_of_week: Optional[int] = None  # 0=Monday..6=Sunday
    hour_range: Optional[Tuple[int, int]] = None  # (start_hour, end_hour)
    impact_pct: float = 0.0  # Estimated % impact on demand (+40, -30)
    tags: List[str] = field(default_factory=list)  # Associated tags from shift notes
    first_seen: date = field(default_factory=date.today)
    last_seen: date = field(default_factory=date.today)
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON/storage."""
        return {
            "pattern_id": self.pattern_id,
            "venue_id": self.venue_id,
            "pattern_type": self.pattern_type,
            "description": self.description,
            "confidence": self.confidence,
            "evidence_count": self.evidence_count,
            "day_of_week": self.day_of_week,
            "hour_range": self.hour_range,
            "impact_pct": self.impact_pct,
            "tags": self.tags,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "active": self.active,
        }


# ---------------------------------------------------------------------------
# PatternStore: Thread-safe registry
# ---------------------------------------------------------------------------


class PatternStore:
    """Thread-safe registry of learned patterns per venue."""

    def __init__(self) -> None:
        self._patterns: Dict[str, List[LearnedPattern]] = {}  # keyed by venue_id
        self._lock = threading.Lock()

    def add(self, pattern: LearnedPattern) -> LearnedPattern:
        """Add or update a pattern. Returns the stored pattern."""
        with self._lock:
            patterns = self._patterns.setdefault(pattern.venue_id, [])
            # Check if pattern with same type/day_of_week already exists
            for i, existing in enumerate(patterns):
                if (
                    existing.pattern_type == pattern.pattern_type
                    and existing.day_of_week == pattern.day_of_week
                    and existing.hour_range == pattern.hour_range
                ):
                    # Update existing pattern
                    pattern.first_seen = existing.first_seen
                    patterns[i] = pattern
                    snapshot = pattern
                    break
            else:
                # New pattern
                patterns.append(pattern)
                snapshot = pattern

        # Persist outside lock
        self._persist(snapshot)
        return snapshot

    def get(self, pattern_id: str) -> Optional[LearnedPattern]:
        """Retrieve a pattern by ID."""
        with self._lock:
            for patterns in self._patterns.values():
                for p in patterns:
                    if p.pattern_id == pattern_id:
                        return p
        return None

    def list_for_venue(
        self, venue_id: str, active_only: bool = True
    ) -> List[LearnedPattern]:
        """List all patterns for a venue, optionally filtering by active."""
        with self._lock:
            patterns = self._patterns.get(venue_id, [])
            if active_only:
                return [p for p in patterns if p.active]
            return list(patterns)

    def deactivate(self, pattern_id: str) -> bool:
        """Deactivate a pattern (sets active=False). Returns True if found."""
        snapshot = None
        with self._lock:
            for patterns in self._patterns.values():
                for p in patterns:
                    if p.pattern_id == pattern_id:
                        p.active = False
                        snapshot = p
                        break
                if snapshot is not None:
                    break

        if snapshot is None:
            return False

        # Persist outside lock
        self._persist(snapshot)
        return True

    def get_for_day(self, venue_id: str, day_of_week: int) -> List[LearnedPattern]:
        """Get patterns relevant to a specific day_of_week (0-6)."""
        with self._lock:
            patterns = self._patterns.get(venue_id, [])
            return [
                p
                for p in patterns
                if p.active and (p.day_of_week is None or p.day_of_week == day_of_week)
            ]

    # -- Persistence --

    def _persist(self, pattern: LearnedPattern) -> None:
        """Store pattern to SQLite."""
        _p.upsert(
            "learned_patterns",
            {
                "pattern_id": pattern.pattern_id,
                "venue_id": pattern.venue_id,
                "pattern_type": pattern.pattern_type,
                "description": pattern.description,
                "confidence": pattern.confidence,
                "evidence_count": pattern.evidence_count,
                "day_of_week": pattern.day_of_week,
                "hour_start": pattern.hour_range[0] if pattern.hour_range else None,
                "hour_end": pattern.hour_range[1] if pattern.hour_range else None,
                "impact_pct": pattern.impact_pct,
                "tags": _p.json_dumps(pattern.tags),
                "first_seen": pattern.first_seen.isoformat(),
                "last_seen": pattern.last_seen.isoformat(),
                "active": 1 if pattern.active else 0,
            },
            pk="pattern_id",
        )

    def rehydrate(self) -> None:
        """Rehydrate patterns from SQLite at startup."""
        if not _p.is_persistence_enabled():
            return
        rows = _p.fetchall("SELECT * FROM learned_patterns ORDER BY last_seen DESC")
        with self._lock:
            for r in rows:
                try:
                    hour_range = None
                    if r["hour_start"] is not None and r["hour_end"] is not None:
                        hour_range = (r["hour_start"], r["hour_end"])
                    pattern = LearnedPattern(
                        pattern_id=r["pattern_id"],
                        venue_id=r["venue_id"],
                        pattern_type=r["pattern_type"],
                        description=r["description"],
                        confidence=r["confidence"],
                        evidence_count=r["evidence_count"],
                        day_of_week=r["day_of_week"],
                        hour_range=hour_range,
                        impact_pct=r["impact_pct"],
                        tags=_p.json_loads(r["tags"], []),
                        first_seen=date.fromisoformat(r["first_seen"]),
                        last_seen=date.fromisoformat(r["last_seen"]),
                        active=bool(r["active"]),
                    )
                    self._patterns.setdefault(r["venue_id"], []).append(pattern)
                except Exception as e:
                    logger.warning("pattern rehydrate failed for %s: %s", r["pattern_id"], e)
        logger.info("Patterns rehydrated: %d rows", len(rows))


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[PatternStore] = None


def get_pattern_store() -> PatternStore:
    """Get or create the module-level pattern store."""
    global _store
    if _store is None:
        _store = PatternStore()
    return _store


@_p.on_init
def _rehydrate_patterns() -> None:
    """Rehydrate patterns from SQLite at startup."""
    store = get_pattern_store()
    store.rehydrate()


# ---------------------------------------------------------------------------
# Pattern detectors
# ---------------------------------------------------------------------------


def detect_day_of_week_patterns(
    venue_id: str, days: int = 56, history_store=None
) -> List[LearnedPattern]:
    """Detect recurring day-of-week patterns in revenue/performance.

    Analyzes daily actuals over the past N days, groups by day_of_week,
    and flags days that are significantly above/below the average.
    """
    # Lazy import to avoid circular deps
    if history_store is None:
        try:
            from rosteriq.tanda_history import get_history_store

            history_store = get_history_store()
        except Exception:
            history_store = None

    if history_store is None:
        return []

    patterns = []
    today = date.today()
    start = today - timedelta(days=days)

    # Get daily actuals
    try:
        daily_rows = history_store.daily_range(venue_id, start, today)
    except Exception:
        return []

    if not daily_rows:
        return []

    # Group by day_of_week
    by_day: Dict[int, List[Any]] = defaultdict(list)
    for row in daily_rows:
        dow = row.day.weekday()  # 0=Monday..6=Sunday
        by_day[dow].append(row)

    # Calculate stats
    all_revenues = [r.actual_revenue for r in daily_rows if r.actual_revenue > 0]
    overall_avg = sum(all_revenues) / len(all_revenues) if all_revenues else 0

    # Find significant deviations
    for dow, rows in by_day.items():
        if not rows:
            continue

        revenues = [r.actual_revenue for r in rows if r.actual_revenue > 0]
        if not revenues:
            continue

        avg_revenue = sum(revenues) / len(revenues)
        pct_deviation = ((avg_revenue - overall_avg) / overall_avg * 100) if overall_avg > 0 else 0

        # Flag if >20% deviation and enough evidence
        if abs(pct_deviation) > 20 and len(revenues) >= 4:
            dow_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dow]
            description = (
                f"{dow_name}days average ${avg_revenue:,.0f} revenue "
                f"({pct_deviation:+.0f}% vs weekday average)"
            )

            pattern = LearnedPattern(
                venue_id=venue_id,
                pattern_type="day_of_week",
                description=description,
                confidence=min(1.0, len(revenues) / 8.0),  # 8+ weeks = high confidence
                evidence_count=len(revenues),
                day_of_week=dow,
                impact_pct=pct_deviation,
                first_seen=min(r.day for r in rows),
                last_seen=max(r.day for r in rows),
            )
            patterns.append(pattern)

    return patterns


def detect_headcount_trends(
    venue_id: str, days: int = 28, headcount_store=None
) -> List[LearnedPattern]:
    """Detect consistent headcount patterns by day and hour.

    Looks for peaks like "Friday dinner (17-21) averages 145 patrons".
    """
    if headcount_store is None:
        try:
            from rosteriq.headcount import get_headcount_store

            headcount_store = get_headcount_store()
        except Exception:
            headcount_store = None

    if headcount_store is None:
        return []

    patterns = []
    today = date.today()
    start = today - timedelta(days=days)

    # Get all entries for venue
    try:
        entries = headcount_store.get_venue_entries(venue_id, limit=10000)
    except Exception:
        return []

    # Filter to date range
    entries = [e for e in entries if start <= e.recorded_at.date() <= today]
    if not entries:
        return []

    # Group by day_of_week + hour bucket
    by_dow_hour: Dict[Tuple[int, str], List[int]] = defaultdict(list)
    hour_buckets = {"lunch": (11, 14), "afternoon": (14, 17), "dinner": (17, 21), "late": (21, 23)}

    for entry in entries:
        # Assume UTC if no timezone
        if entry.recorded_at.tzinfo is None:
            recorded_at = entry.recorded_at.replace(tzinfo=timezone.utc)
        else:
            recorded_at = entry.recorded_at

        dow = recorded_at.astimezone(AU_TZ).weekday()
        hour = recorded_at.astimezone(AU_TZ).hour

        # Find bucket
        bucket_name = None
        for name, (start_h, end_h) in hour_buckets.items():
            if start_h <= hour < end_h:
                bucket_name = name
                break

        if bucket_name:
            by_dow_hour[(dow, bucket_name)].append(entry.count)

    # Find consistent peaks
    for (dow, bucket_name), counts in by_dow_hour.items():
        if len(counts) < 3:  # Need at least 3 observations
            continue

        avg_count = sum(counts) / len(counts)
        std_dev = (sum((c - avg_count) ** 2 for c in counts) / len(counts)) ** 0.5 if counts else 0

        # If consistently high, flag it
        if std_dev < avg_count * 0.3 and avg_count > 50:  # Low variance, decent count
            dow_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dow]
            description = (
                f"{dow_name} {bucket_name} service averages {avg_count:.0f} patrons "
                f"(consistent within {std_dev:.0f})"
            )

            pattern = LearnedPattern(
                venue_id=venue_id,
                pattern_type="headcount_trend",
                description=description,
                confidence=min(1.0, len(counts) / 8.0),
                evidence_count=len(counts),
                day_of_week=dow,
                hour_range=hour_buckets[bucket_name],
                impact_pct=0,  # Headcount trends inform staffing, not demand %
                tags=[bucket_name],
            )
            patterns.append(pattern)

    return patterns


def detect_note_tag_patterns(
    venue_id: str, days: int = 56, note_store=None
) -> List[LearnedPattern]:
    """Detect recurring themes in shift notes.

    Counts tag frequency by day_of_week and looks for patterns like
    "Friday shifts frequently tagged 'event' (12 of 16 Fridays)".
    """
    if note_store is None:
        try:
            from rosteriq.headcount import get_shift_note_store

            note_store = get_shift_note_store()
        except Exception:
            note_store = None

    if note_store is None:
        return []

    patterns = []
    today = date.today()
    start = today - timedelta(days=days)

    # Get all notes for venue
    try:
        notes = note_store.get_venue_notes(venue_id, limit=10000)
    except Exception:
        return []

    # Filter to date range
    notes = [n for n in notes if start <= n.created_at.date() <= today]
    if not notes:
        return []

    # Count tags by day_of_week
    by_dow: Dict[int, List[str]] = defaultdict(list)
    by_dow_tags: Dict[Tuple[int, str], int] = defaultdict(int)

    for note in notes:
        # Assume UTC if no timezone
        if note.created_at.tzinfo is None:
            created_at = note.created_at.replace(tzinfo=timezone.utc)
        else:
            created_at = note.created_at

        dow = created_at.astimezone(AU_TZ).weekday()
        by_dow[dow].append(note.content)

        for tag in note.tags:
            by_dow_tags[(dow, tag)] += 1

    # Find frequent tags on specific days
    for (dow, tag), count in by_dow_tags.items():
        total_notes_that_day = len(by_dow.get(dow, []))
        if total_notes_that_day < 4:
            continue

        frequency = count / total_notes_that_day
        if frequency >= 0.5 and count >= 3:  # 50%+ of notes on that day have this tag
            dow_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dow]
            description = (
                f"{dow_name} shifts frequently tagged '{tag}' "
                f"({count} of {total_notes_that_day} shifts)"
            )

            pattern = LearnedPattern(
                venue_id=venue_id,
                pattern_type="tag_cluster",
                description=description,
                confidence=min(1.0, count / 8.0),
                evidence_count=count,
                day_of_week=dow,
                impact_pct=0,
                tags=[tag],
            )
            patterns.append(pattern)

    return patterns


def detect_weather_impact(
    venue_id: str, days: int = 56, history_store=None
) -> List[LearnedPattern]:
    """Detect weather-related demand patterns.

    Compares revenue on days tagged 'weather' vs without, and flags
    if there's a significant difference.
    """
    if history_store is None:
        try:
            from rosteriq.tanda_history import get_history_store

            history_store = get_history_store()
        except Exception:
            history_store = None

    if history_store is None:
        return []

    try:
        from rosteriq.headcount import get_shift_note_store

        note_store = get_shift_note_store()
    except Exception:
        note_store = None

    if note_store is None:
        return []

    patterns = []
    today = date.today()
    start = today - timedelta(days=days)

    # Get daily actuals and shift notes
    try:
        daily_rows = history_store.daily_range(venue_id, start, today)
        notes = note_store.get_venue_notes(venue_id, limit=10000)
    except Exception:
        return []

    if not daily_rows:
        return []

    # Filter notes to date range
    notes = [n for n in notes if start <= n.created_at.date() <= today]

    # Collect dates with 'weather' tag
    weather_dates = set()
    for note in notes:
        if "weather" in [t.lower() for t in note.tags]:
            weather_dates.add(note.created_at.date())

    if len(weather_dates) < 3:  # Not enough observations
        return []

    # Compare revenues
    weather_revenues = []
    normal_revenues = []

    for row in daily_rows:
        if row.actual_revenue > 0:
            if row.day in weather_dates:
                weather_revenues.append(row.actual_revenue)
            else:
                normal_revenues.append(row.actual_revenue)

    if not normal_revenues or not weather_revenues:
        return []

    avg_weather = sum(weather_revenues) / len(weather_revenues)
    avg_normal = sum(normal_revenues) / len(normal_revenues)
    pct_impact = ((avg_weather - avg_normal) / avg_normal * 100) if avg_normal > 0 else 0

    # Flag if significant impact
    if abs(pct_impact) > 15:
        direction = "lower" if pct_impact < 0 else "higher"
        description = (
            f"Revenue on weather-tagged days is {direction} by {abs(pct_impact):.0f}% "
            f"(${avg_weather:,.0f} vs ${avg_normal:,.0f} normal)"
        )

        pattern = LearnedPattern(
            venue_id=venue_id,
            pattern_type="weather_impact",
            description=description,
            confidence=min(1.0, len(weather_revenues) / 8.0),
            evidence_count=len(weather_revenues),
            impact_pct=pct_impact,
            tags=["weather"],
        )
        patterns.append(pattern)

    return patterns


# ---------------------------------------------------------------------------
# Main detection runner
# ---------------------------------------------------------------------------


def run_detection(venue_id: str, days: int = 56) -> Dict[str, Any]:
    """Run all pattern detectors and store results.

    Returns a summary dict with pattern counts and the patterns themselves.
    """
    store = get_pattern_store()

    all_patterns = []
    all_patterns.extend(detect_day_of_week_patterns(venue_id, days=days))
    all_patterns.extend(detect_headcount_trends(venue_id, days=days))
    all_patterns.extend(detect_note_tag_patterns(venue_id, days=days))
    all_patterns.extend(detect_weather_impact(venue_id, days=days))

    # Store patterns (deduplication happens inside store.add)
    new_count = 0
    updated_count = 0
    for pattern in all_patterns:
        existing = store.get(pattern.pattern_id)
        stored = store.add(pattern)
        if existing:
            updated_count += 1
        else:
            new_count += 1

    return {
        "venue_id": venue_id,
        "patterns_found": len(all_patterns),
        "new": new_count,
        "updated": updated_count,
        "patterns": [p.to_dict() for p in all_patterns],
    }
