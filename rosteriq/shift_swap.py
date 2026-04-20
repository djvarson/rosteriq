"""Shift swap / offer-up module for RosterIQ.

Pure stdlib module for shift swap management. Staff can offer up shifts they can't
work, other staff claim them, managers approve. This reduces no-shows and last-minute
scrambles.

Lifecycle: OFFERED -> CLAIMED -> APPROVED|REJECTED, or OFFERED -> CANCELLED.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.shift_swap")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class SwapStatus(str, Enum):
    """Shift swap lifecycle states."""
    OFFERED = "offered"
    CLAIMED = "claimed"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class ShiftSwap:
    """A single shift swap offer."""

    swap_id: str
    venue_id: str
    shift_id: str
    shift_date: str  # ISO date (YYYY-MM-DD)
    shift_start: str  # HH:MM
    shift_end: str  # HH:MM
    role: str  # e.g., "bartender", "floor", "kitchen"
    offered_by: str  # employee id
    offered_by_name: str
    reason: str
    status: SwapStatus = SwapStatus.OFFERED
    claimed_by: Optional[str] = None
    claimed_by_name: Optional[str] = None
    claimed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None  # manager id
    reviewed_at: Optional[datetime] = None
    review_note: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON responses."""
        return {
            "swap_id": self.swap_id,
            "venue_id": self.venue_id,
            "shift_id": self.shift_id,
            "shift_date": self.shift_date,
            "shift_start": self.shift_start,
            "shift_end": self.shift_end,
            "role": self.role,
            "offered_by": self.offered_by,
            "offered_by_name": self.offered_by_name,
            "reason": self.reason,
            "status": self.status.value,
            "claimed_by": self.claimed_by,
            "claimed_by_name": self.claimed_by_name,
            "claimed_at": self.claimed_at.isoformat() if self.claimed_at else None,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "review_note": self.review_note,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------

# Lazy import of persistence module to avoid circular deps
def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


# Register schema on module load
_SHIFT_SWAPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS shift_swaps (
    swap_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    shift_id TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    shift_start TEXT NOT NULL,
    shift_end TEXT NOT NULL,
    role TEXT NOT NULL,
    offered_by TEXT NOT NULL,
    offered_by_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'offered',
    claimed_by TEXT,
    claimed_by_name TEXT,
    claimed_at TEXT,
    reviewed_by TEXT,
    reviewed_at TEXT,
    review_note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_swap_venue ON shift_swaps(venue_id);
CREATE INDEX IF NOT EXISTS ix_swap_status ON shift_swaps(status);
CREATE INDEX IF NOT EXISTS ix_swap_created ON shift_swaps(created_at DESC);
"""

def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("shift_swaps", _SHIFT_SWAPS_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_swap_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# ShiftSwapStore
# ---------------------------------------------------------------------------


class ShiftSwapStore:
    """Thread-safe in-memory store for shift swaps with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._swaps: Dict[str, ShiftSwap] = {}
        self._lock = threading.Lock()

    def _persist(self, swap: ShiftSwap) -> None:
        """Persist a swap to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "swap_id": swap.swap_id,
            "venue_id": swap.venue_id,
            "shift_id": swap.shift_id,
            "shift_date": swap.shift_date,
            "shift_start": swap.shift_start,
            "shift_end": swap.shift_end,
            "role": swap.role,
            "offered_by": swap.offered_by,
            "offered_by_name": swap.offered_by_name,
            "reason": swap.reason,
            "status": swap.status.value,
            "claimed_by": swap.claimed_by,
            "claimed_by_name": swap.claimed_by_name,
            "claimed_at": swap.claimed_at.isoformat() if swap.claimed_at else None,
            "reviewed_by": swap.reviewed_by,
            "reviewed_at": swap.reviewed_at.isoformat() if swap.reviewed_at else None,
            "review_note": swap.review_note,
            "created_at": swap.created_at.isoformat(),
            "updated_at": swap.updated_at.isoformat(),
        }
        try:
            _p.upsert("shift_swaps", row, pk="swap_id")
        except Exception as e:
            logger.warning("Failed to persist shift swap %s: %s", swap.swap_id, e)

    def _rehydrate(self) -> None:
        """Load all swaps from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            rows = _p.fetchall("SELECT * FROM shift_swaps")
            for row in rows:
                swap = self._row_to_swap(dict(row))
                self._swaps[swap.swap_id] = swap
            logger.info("Rehydrated %d shift swaps from persistence", len(self._swaps))
        except Exception as e:
            logger.warning("Failed to rehydrate shift swaps: %s", e)

    @staticmethod
    def _row_to_swap(row: Dict[str, Any]) -> ShiftSwap:
        """Reconstruct a ShiftSwap from a DB row."""
        # Parse ISO datetime strings back to datetime objects
        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        return ShiftSwap(
            swap_id=row["swap_id"],
            venue_id=row["venue_id"],
            shift_id=row["shift_id"],
            shift_date=row["shift_date"],
            shift_start=row["shift_start"],
            shift_end=row["shift_end"],
            role=row["role"],
            offered_by=row["offered_by"],
            offered_by_name=row["offered_by_name"],
            reason=row["reason"],
            status=SwapStatus(row.get("status", "offered")),
            claimed_by=row.get("claimed_by"),
            claimed_by_name=row.get("claimed_by_name"),
            claimed_at=parse_iso(row.get("claimed_at")),
            reviewed_by=row.get("reviewed_by"),
            reviewed_at=parse_iso(row.get("reviewed_at")),
            review_note=row.get("review_note"),
            created_at=parse_iso(row.get("created_at")) or datetime.now(timezone.utc),
            updated_at=parse_iso(row.get("updated_at")) or datetime.now(timezone.utc),
        )

    def offer(
        self,
        venue_id: str,
        shift_id: str,
        shift_date: str,
        shift_start: str,
        shift_end: str,
        role: str,
        offered_by: str,
        offered_by_name: str,
        reason: str,
    ) -> ShiftSwap:
        """Create a new shift swap offer. Returns the ShiftSwap."""
        swap_id = f"swap_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        swap = ShiftSwap(
            swap_id=swap_id,
            venue_id=venue_id,
            shift_id=shift_id,
            shift_date=shift_date,
            shift_start=shift_start,
            shift_end=shift_end,
            role=role,
            offered_by=offered_by,
            offered_by_name=offered_by_name,
            reason=reason,
            status=SwapStatus.OFFERED,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._swaps[swap_id] = swap
        self._persist(swap)
        return swap

    def claim(self, swap_id: str, claimed_by: str, claimed_by_name: str) -> ShiftSwap:
        """Claim a shift swap. Must be in OFFERED status.

        Raises ValueError if swap not found or not in OFFERED status.
        """
        with self._lock:
            swap = self._swaps.get(swap_id)
            if not swap:
                raise ValueError(f"Shift swap {swap_id} not found")
            if swap.status != SwapStatus.OFFERED:
                raise ValueError(
                    f"Cannot claim swap {swap_id}: status is {swap.status}, "
                    f"must be {SwapStatus.OFFERED}"
                )
            swap.claimed_by = claimed_by
            swap.claimed_by_name = claimed_by_name
            swap.claimed_at = datetime.now(timezone.utc)
            swap.status = SwapStatus.CLAIMED
            swap.updated_at = datetime.now(timezone.utc)

        self._persist(swap)
        return swap

    def approve(
        self, swap_id: str, reviewed_by: str, note: Optional[str] = None
    ) -> ShiftSwap:
        """Approve a claimed swap. Must be in CLAIMED status.

        Raises ValueError if swap not found or not in CLAIMED status.
        """
        with self._lock:
            swap = self._swaps.get(swap_id)
            if not swap:
                raise ValueError(f"Shift swap {swap_id} not found")
            if swap.status != SwapStatus.CLAIMED:
                raise ValueError(
                    f"Cannot approve swap {swap_id}: status is {swap.status}, "
                    f"must be {SwapStatus.CLAIMED}"
                )
            swap.status = SwapStatus.APPROVED
            swap.reviewed_by = reviewed_by
            swap.reviewed_at = datetime.now(timezone.utc)
            swap.review_note = note
            swap.updated_at = datetime.now(timezone.utc)

        self._persist(swap)
        return swap

    def reject(
        self, swap_id: str, reviewed_by: str, note: Optional[str] = None
    ) -> ShiftSwap:
        """Reject a claimed swap. Must be in CLAIMED status.

        Raises ValueError if swap not found or not in CLAIMED status.
        """
        with self._lock:
            swap = self._swaps.get(swap_id)
            if not swap:
                raise ValueError(f"Shift swap {swap_id} not found")
            if swap.status != SwapStatus.CLAIMED:
                raise ValueError(
                    f"Cannot reject swap {swap_id}: status is {swap.status}, "
                    f"must be {SwapStatus.CLAIMED}"
                )
            swap.status = SwapStatus.REJECTED
            swap.reviewed_by = reviewed_by
            swap.reviewed_at = datetime.now(timezone.utc)
            swap.review_note = note
            swap.updated_at = datetime.now(timezone.utc)

        self._persist(swap)
        return swap

    def cancel(self, swap_id: str, cancelled_by: str) -> ShiftSwap:
        """Cancel a swap. Only works if status is OFFERED or CLAIMED, and only
        if the canceller is the original offerer.

        Raises ValueError if swap not found, not in cancellable status, or if
        cancelled_by is not the offerer.
        """
        with self._lock:
            swap = self._swaps.get(swap_id)
            if not swap:
                raise ValueError(f"Shift swap {swap_id} not found")
            if swap.status not in (SwapStatus.OFFERED, SwapStatus.CLAIMED):
                raise ValueError(
                    f"Cannot cancel swap {swap_id}: status is {swap.status}, "
                    f"must be {SwapStatus.OFFERED} or {SwapStatus.CLAIMED}"
                )
            if swap.offered_by != cancelled_by:
                raise ValueError(
                    f"Cannot cancel swap {swap_id}: only the offerer "
                    f"({swap.offered_by}) can cancel"
                )
            swap.status = SwapStatus.CANCELLED
            swap.updated_at = datetime.now(timezone.utc)

        self._persist(swap)
        return swap

    def get(self, swap_id: str) -> Optional[ShiftSwap]:
        """Get a shift swap by ID. Returns None if not found."""
        with self._lock:
            return self._swaps.get(swap_id)

    def list_for_venue(
        self, venue_id: str, status: Optional[SwapStatus] = None, limit: int = 50
    ) -> List[ShiftSwap]:
        """List shift swaps for a venue, newest first.

        Optionally filter by status. Limited to `limit` results.
        """
        with self._lock:
            swaps = [s for s in self._swaps.values() if s.venue_id == venue_id]
            if status is not None:
                swaps = [s for s in swaps if s.status == status]
            # Sort newest first
            swaps.sort(key=lambda s: s.created_at, reverse=True)
            return swaps[:limit]

    def list_available(self, venue_id: str) -> List[ShiftSwap]:
        """List available swaps for claiming (OFFERED status only).

        Returns newest first, up to 50.
        """
        return self.list_for_venue(venue_id, status=SwapStatus.OFFERED, limit=50)

    def list_pending_review(self, venue_id: str) -> List[ShiftSwap]:
        """List swaps pending manager review (CLAIMED status only).

        Returns newest first, up to 50.
        """
        return self.list_for_venue(venue_id, status=SwapStatus.CLAIMED, limit=50)


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_swap_store_singleton: Optional[ShiftSwapStore] = None
_singleton_lock = threading.Lock()


def get_swap_store() -> ShiftSwapStore:
    """Get the module-level shift swap store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _swap_store_singleton
    if _swap_store_singleton is None:
        with _singleton_lock:
            if _swap_store_singleton is None:
                _swap_store_singleton = ShiftSwapStore()
    return _swap_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _swap_store_singleton
    _swap_store_singleton = None
