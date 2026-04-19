"""Headcount clicker and shift notes module (on-shift features).

Provides:
- HeadcountEntry — individual patron count tap with delta tracking
- HeadcountStore — thread-safe registry of headcount history per venue/shift
- ShiftNote — end-of-shift observations (tagged, searchable)
- ShiftNoteStore — thread-safe registry of shift notes per venue

Why this matters: duty managers tap to count patrons (instead of walking to
security), and shift notes capture end-of-shift context that feeds back to
the roster maker (weather impacts, events, staffing events). SQLite-backed
persistence via the rosteriq.persistence layer.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.headcount")


# ---------------------------------------------------------------------------
# Schemas — register with persistence layer
# ---------------------------------------------------------------------------

_HEADCOUNT_SCHEMA = """
CREATE TABLE IF NOT EXISTS headcount_entries (
    entry_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    shift_id TEXT NOT NULL,
    count INTEGER NOT NULL,
    delta INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    note TEXT
);
CREATE INDEX IF NOT EXISTS ix_headcount_venue ON headcount_entries(venue_id);
CREATE INDEX IF NOT EXISTS ix_headcount_shift ON headcount_entries(shift_id);
"""
_p.register_schema("headcount_entries", _HEADCOUNT_SCHEMA)

_SHIFT_NOTES_SCHEMA = """
CREATE TABLE IF NOT EXISTS shift_notes (
    note_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    shift_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    author_name TEXT NOT NULL,
    content TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_shift_notes_shift ON shift_notes(shift_id);
CREATE INDEX IF NOT EXISTS ix_shift_notes_venue ON shift_notes(venue_id);
"""
_p.register_schema("shift_notes", _SHIFT_NOTES_SCHEMA)


# ---------------------------------------------------------------------------
# Headcount Entry
# ---------------------------------------------------------------------------


@dataclass
class HeadcountEntry:
    """Single patron count tap."""

    entry_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    venue_id: str = ""
    shift_id: str = ""
    count: int = 0
    delta: int = 0  # change from previous entry, 0 for first
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    recorded_by: str = ""  # manager id
    note: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "venue_id": self.venue_id,
            "shift_id": self.shift_id,
            "count": self.count,
            "delta": self.delta,
            "recorded_at": self.recorded_at.isoformat(),
            "recorded_by": self.recorded_by,
            "note": self.note,
        }


# ---------------------------------------------------------------------------
# Shift Note
# ---------------------------------------------------------------------------


@dataclass
class ShiftNote:
    """End-of-shift observation."""

    note_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    venue_id: str = ""
    shift_id: str = ""
    author_id: str = ""
    author_name: str = ""
    content: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "note_id": self.note_id,
            "venue_id": self.venue_id,
            "shift_id": self.shift_id,
            "author_id": self.author_id,
            "author_name": self.author_name,
            "content": self.content,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Headcount Store
# ---------------------------------------------------------------------------


class HeadcountStore:
    """Thread-safe registry of headcount entries per venue/shift."""

    def __init__(self) -> None:
        self._entries: Dict[str, List[HeadcountEntry]] = {}  # keyed by shift_id
        self._lock = threading.Lock()

    def record(
        self,
        venue_id: str,
        shift_id: str,
        count: int,
        recorded_by: str,
        note: Optional[str] = None,
    ) -> HeadcountEntry:
        """Record a headcount tap, auto-calculating delta from previous entry."""
        with self._lock:
            entries = self._entries.setdefault(shift_id, [])
            # Calculate delta from previous entry for same shift
            delta = 0
            if entries:
                previous_count = entries[-1].count
                delta = count - previous_count

            entry = HeadcountEntry(
                venue_id=venue_id,
                shift_id=shift_id,
                count=count,
                delta=delta,
                recorded_at=datetime.now(timezone.utc),
                recorded_by=recorded_by,
                note=note,
            )
            entries.append(entry)
            snapshot = entry

        # Persist outside lock
        self._persist(snapshot)
        return snapshot

    def get_shift_entries(self, shift_id: str) -> List[HeadcountEntry]:
        """Returns entries for shift in chronological order."""
        with self._lock:
            return list(self._entries.get(shift_id, []))

    def get_venue_entries(self, venue_id: str, limit: int = 100) -> List[HeadcountEntry]:
        """Returns all entries for venue, newest first."""
        with self._lock:
            all_entries = []
            for entries in self._entries.values():
                all_entries.extend(e for e in entries if e.venue_id == venue_id)
        # Sort newest first
        all_entries.sort(key=lambda e: e.recorded_at, reverse=True)
        return all_entries[:limit]

    def get_latest(self, venue_id: str) -> Optional[HeadcountEntry]:
        """Return latest entry across all shifts for venue."""
        entries = self.get_venue_entries(venue_id, limit=1)
        return entries[0] if entries else None

    def clear_shift(self, shift_id: str) -> None:
        """Clear all entries for a shift."""
        with self._lock:
            self._entries.pop(shift_id, None)

    def clear_venue(self, venue_id: str) -> None:
        """Clear all entries for a venue."""
        with self._lock:
            for entries in self._entries.values():
                # Filter out entries for this venue
                self._entries[shift_id] = [
                    e for e in entries if e.venue_id != venue_id
                ]

    # -- Persistence --

    def _persist(self, entry: HeadcountEntry) -> None:
        _p.upsert(
            "headcount_entries",
            {
                "entry_id": entry.entry_id,
                "venue_id": entry.venue_id,
                "shift_id": entry.shift_id,
                "count": entry.count,
                "delta": entry.delta,
                "recorded_at": entry.recorded_at.isoformat(),
                "recorded_by": entry.recorded_by,
                "note": entry.note,
            },
            pk="entry_id",
        )

    def rehydrate(self) -> None:
        """Rehydrate from SQLite."""
        if not _p.is_persistence_enabled():
            return
        rows = _p.fetchall("SELECT * FROM headcount_entries ORDER BY recorded_at ASC")
        with self._lock:
            for r in rows:
                try:
                    entry = HeadcountEntry(
                        entry_id=r["entry_id"],
                        venue_id=r["venue_id"],
                        shift_id=r["shift_id"],
                        count=r["count"],
                        delta=r["delta"],
                        recorded_at=datetime.fromisoformat(r["recorded_at"]),
                        recorded_by=r["recorded_by"],
                        note=r["note"],
                    )
                    self._entries.setdefault(r["shift_id"], []).append(entry)
                except Exception as e:
                    logger.warning("headcount rehydrate failed for %s: %s", r["entry_id"], e)
        logger.info("Headcount entries rehydrated: %d rows", len(rows))


# ---------------------------------------------------------------------------
# Shift Note Store
# ---------------------------------------------------------------------------


class ShiftNoteStore:
    """Thread-safe registry of shift notes per venue."""

    def __init__(self) -> None:
        self._notes: Dict[str, List[ShiftNote]] = {}  # keyed by shift_id
        self._lock = threading.Lock()

    def add(
        self,
        venue_id: str,
        shift_id: str,
        author_id: str,
        author_name: str,
        content: str,
        tags: Optional[List[str]] = None,
    ) -> ShiftNote:
        """Add a shift note."""
        if tags is None:
            tags = []

        note = ShiftNote(
            venue_id=venue_id,
            shift_id=shift_id,
            author_id=author_id,
            author_name=author_name,
            content=content,
            tags=tags,
            created_at=datetime.now(timezone.utc),
        )

        with self._lock:
            self._notes.setdefault(shift_id, []).append(note)
            snapshot = note

        # Persist outside lock
        self._persist(snapshot)
        return snapshot

    def get_shift_notes(self, shift_id: str) -> List[ShiftNote]:
        """Returns notes for shift in chronological order."""
        with self._lock:
            return list(self._notes.get(shift_id, []))

    def get_venue_notes(self, venue_id: str, limit: int = 50) -> List[ShiftNote]:
        """Returns notes for venue, newest first."""
        with self._lock:
            all_notes = []
            for notes in self._notes.values():
                all_notes.extend(n for n in notes if n.venue_id == venue_id)
        # Sort newest first
        all_notes.sort(key=lambda n: n.created_at, reverse=True)
        return all_notes[:limit]

    def search_by_tag(self, venue_id: str, tag: str) -> List[ShiftNote]:
        """Search notes for venue by tag."""
        with self._lock:
            all_notes = []
            for notes in self._notes.values():
                all_notes.extend(
                    n for n in notes
                    if n.venue_id == venue_id and tag in n.tags
                )
        # Sort newest first
        all_notes.sort(key=lambda n: n.created_at, reverse=True)
        return all_notes

    # -- Persistence --

    def _persist(self, note: ShiftNote) -> None:
        _p.upsert(
            "shift_notes",
            {
                "note_id": note.note_id,
                "venue_id": note.venue_id,
                "shift_id": note.shift_id,
                "author_id": note.author_id,
                "author_name": note.author_name,
                "content": note.content,
                "tags": _p.json_dumps(note.tags),
                "created_at": note.created_at.isoformat(),
            },
            pk="note_id",
        )

    def rehydrate(self) -> None:
        """Rehydrate from SQLite."""
        if not _p.is_persistence_enabled():
            return
        rows = _p.fetchall("SELECT * FROM shift_notes ORDER BY created_at ASC")
        with self._lock:
            for r in rows:
                try:
                    note = ShiftNote(
                        note_id=r["note_id"],
                        venue_id=r["venue_id"],
                        shift_id=r["shift_id"],
                        author_id=r["author_id"],
                        author_name=r["author_name"],
                        content=r["content"],
                        tags=_p.json_loads(r["tags"], default=[]) or [],
                        created_at=datetime.fromisoformat(r["created_at"]),
                    )
                    self._notes.setdefault(r["shift_id"], []).append(note)
                except Exception as e:
                    logger.warning("shift note rehydrate failed for %s: %s", r["note_id"], e)
        logger.info("Shift notes rehydrated: %d rows", len(rows))


# ---------------------------------------------------------------------------
# Module singletons
# ---------------------------------------------------------------------------

_headcount_store_instance: Optional[HeadcountStore] = None
_shift_note_store_instance: Optional[ShiftNoteStore] = None


def get_headcount_store() -> HeadcountStore:
    """Return the module-level HeadcountStore singleton."""
    global _headcount_store_instance
    if _headcount_store_instance is None:
        _headcount_store_instance = HeadcountStore()
    return _headcount_store_instance


def get_shift_note_store() -> ShiftNoteStore:
    """Return the module-level ShiftNoteStore singleton."""
    global _shift_note_store_instance
    if _shift_note_store_instance is None:
        _shift_note_store_instance = ShiftNoteStore()
    return _shift_note_store_instance


# ---------------------------------------------------------------------------
# Persistence hooks
# ---------------------------------------------------------------------------


@_p.on_init
def _rehydrate_headcount() -> None:
    """Rehydrate headcount store from SQLite."""
    store = get_headcount_store()
    store.rehydrate()


@_p.on_init
def _rehydrate_shift_notes() -> None:
    """Rehydrate shift note store from SQLite."""
    store = get_shift_note_store()
    store.rehydrate()
