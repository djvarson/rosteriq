"""In-memory head-count store for the on-shift clicker (Moment 6).

Pure-stdlib module so tests can exercise the logic without needing
FastAPI/Pydantic in the environment. The FastAPI layer in api_v2 imports
and delegates to the helpers here.

Each venue's history is append-only and ordered oldest → newest. The
`count_after` on the last entry IS the current count. The first access to
a venue seeds the history with a single 'start of shift' entry at count 0.

Head counts can never go negative — we clamp at 0 and the stored delta is
the *actual* change (may differ from the requested delta), so
accountability remains honest.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_HEADCOUNT_STORE: Dict[str, List[Dict[str, Any]]] = {}
MAX_HISTORY = 200  # per venue, bounds memory on a long shift


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clear() -> None:
    """Wipe the entire store. Used by tests."""
    _HEADCOUNT_STORE.clear()


def store() -> Dict[str, List[Dict[str, Any]]]:
    """Return the raw store dict. For tests and diagnostics only."""
    return _HEADCOUNT_STORE


# ---------------------------------------------------------------------------
# Public helpers (called by api_v2 endpoints)
# ---------------------------------------------------------------------------

def seed(venue_id: str) -> List[Dict[str, Any]]:
    """Initialise an empty history for a venue with a 'start of shift' entry."""
    entry = {
        "timestamp": _now_iso(),
        "delta": 0,
        "count_after": 0,
        "note": "Start of shift",
        "source": "reset",
    }
    _HEADCOUNT_STORE[venue_id] = [entry]
    return _HEADCOUNT_STORE[venue_id]


def history(venue_id: str) -> List[Dict[str, Any]]:
    """Return the mutable history list for a venue, seeding if needed."""
    if venue_id not in _HEADCOUNT_STORE:
        return seed(venue_id)
    return _HEADCOUNT_STORE[venue_id]


def apply_delta(
    venue_id: str,
    delta: int,
    note: Optional[str],
    source: str,
) -> Dict[str, Any]:
    """Append a delta entry and return the new entry dict.

    Clamps the resulting count at zero; the stored `delta` on the entry
    is the *actual* change (may differ from the requested delta), so
    accountability remains honest.
    """
    hist = history(venue_id)
    current = hist[-1]["count_after"]
    new_count = max(0, current + delta)
    actual_delta = new_count - current
    entry = {
        "timestamp": _now_iso(),
        "delta": actual_delta,
        "count_after": new_count,
        "note": note,
        "source": source,
    }
    hist.append(entry)
    if len(hist) > MAX_HISTORY:
        del hist[: len(hist) - MAX_HISTORY]
    return entry


def reset(venue_id: str, count: int, note: Optional[str]) -> Dict[str, Any]:
    """Hard-reset the count, appending a 'reset' entry rather than wiping history."""
    hist = history(venue_id)
    current = hist[-1]["count_after"] if hist else 0
    new_count = max(0, int(count))
    entry = {
        "timestamp": _now_iso(),
        "delta": new_count - current,
        "count_after": new_count,
        "note": note or "Reset",
        "source": "reset",
    }
    hist.append(entry)
    if len(hist) > MAX_HISTORY:
        del hist[: len(hist) - MAX_HISTORY]
    return entry


def state(venue_id: str, recent_limit: int = 12) -> Dict[str, Any]:
    """Build the on-the-wire state view for the dashboard.

    `recent` is newest first, matching the UI render order. `total_logged_today`
    counts entries stamped with today's UTC date — pilot pragmatism; we can
    switch to venue-local day boundaries when we wire venue timezones.
    """
    hist = history(venue_id)
    last = hist[-1]
    recent = list(hist[-recent_limit:])
    recent.reverse()
    today_str = datetime.now(timezone.utc).date().isoformat()
    total_logged_today = sum(1 for e in hist if e["timestamp"][:10] == today_str)
    return {
        "venue_id": venue_id,
        "current": last["count_after"],
        "updated_at": last["timestamp"],
        "recent": recent,
        "total_logged_today": total_logged_today,
    }
