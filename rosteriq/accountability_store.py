"""In-memory accountability store (Moment 8: accountability layer).

Pure-stdlib, no FastAPI / Pydantic imports. The FastAPI layer in api_v2
delegates to the helpers here.

Each recommendation is an event with a stable `id` (uuid), a source
(e.g. "wage_pulse", "scenario_solver", "signal"), the recommendation
text, an optional estimated $ impact if the action *is not* taken,
and a mutable status that moves through pending → accepted | dismissed
(with an optional response note and timestamp).

Why this matters: Dale's explicit line from the meetings is
    "You had all this data and kept people on — why?"
The accountability store is the ledger that answers that question.
The shift-recap composer reads this store and surfaces the count of
dismissed-or-ignored recommendations plus the estimated missed impact,
so the post-shift screen can't be gamed.

Data shape of a recommendation event:

    {
        "id": "rec_<uuid>",
        "venue_id": "...",
        "recorded_at": "ISO-8601 Z",
        "source": "wage_pulse" | "scenario_solver" | "signal" | "solver" | "manual",
        "text": "Cut 2 bartenders and 1 floor",
        "impact_estimate_aud": 420.0,    # positive = $ at risk if dismissed
        "priority": "low" | "med" | "high",
        "status": "pending" | "accepted" | "dismissed",
        "responded_at": "ISO-8601 Z" | None,
        "response_note": str | None,
    }
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_ACCOUNTABILITY_STORE: Dict[str, List[Dict[str, Any]]] = {}
MAX_HISTORY = 300  # per venue — generous; a shift rarely generates more than ~30


VALID_STATUSES = ("pending", "accepted", "dismissed")
VALID_PRIORITIES = ("low", "med", "high")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clear() -> None:
    """Wipe the entire store. Used by tests."""
    _ACCOUNTABILITY_STORE.clear()


def store() -> Dict[str, List[Dict[str, Any]]]:
    """Return the raw store dict. For tests and diagnostics only."""
    return _ACCOUNTABILITY_STORE


# ---------------------------------------------------------------------------
# Public helpers (called by api_v2 endpoints AND by shift_recap composer)
# ---------------------------------------------------------------------------

def history(venue_id: str) -> List[Dict[str, Any]]:
    """Return the mutable history list for a venue, creating an empty
    bucket on first access (unlike headcount_store, we do NOT seed a
    placeholder event — an empty list is the correct 'no suggestions yet'
    state)."""
    if venue_id not in _ACCOUNTABILITY_STORE:
        _ACCOUNTABILITY_STORE[venue_id] = []
    return _ACCOUNTABILITY_STORE[venue_id]


def record(
    venue_id: str,
    *,
    text: str,
    source: str = "manual",
    impact_estimate_aud: Optional[float] = None,
    priority: str = "med",
    rec_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Record a new recommendation in the pending state.

    Returns the new event dict. If `rec_id` is passed and an event with
    that id already exists for the venue, this is a no-op and returns the
    existing event — useful for the dashboard 'auto-register recs on
    render' pattern where the same rec gets re-seen on every poll.
    """
    hist = history(venue_id)

    if rec_id:
        for ev in hist:
            if ev.get("id") == rec_id:
                return ev

    pri = priority if priority in VALID_PRIORITIES else "med"

    event = {
        "id": rec_id or f"rec_{uuid.uuid4().hex[:12]}",
        "venue_id": venue_id,
        "recorded_at": _now_iso(),
        "source": source or "manual",
        "text": str(text or "").strip(),
        "impact_estimate_aud": (
            float(impact_estimate_aud) if impact_estimate_aud is not None else None
        ),
        "priority": pri,
        "status": "pending",
        "responded_at": None,
        "response_note": None,
    }
    hist.append(event)

    if len(hist) > MAX_HISTORY:
        del hist[: len(hist) - MAX_HISTORY]

    return event


def respond(
    venue_id: str,
    rec_id: str,
    *,
    status: str,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    """Move a pending recommendation to accepted or dismissed.

    Raises KeyError if the rec_id doesn't exist for the venue.
    Raises ValueError if `status` isn't 'accepted' or 'dismissed'.
    """
    if status not in ("accepted", "dismissed"):
        raise ValueError(
            f"Invalid status '{status}' — must be 'accepted' or 'dismissed'"
        )

    hist = history(venue_id)
    for ev in hist:
        if ev.get("id") == rec_id:
            ev["status"] = status
            ev["responded_at"] = _now_iso()
            ev["response_note"] = note
            return ev

    raise KeyError(f"Recommendation '{rec_id}' not found for venue '{venue_id}'")


def summary(venue_id: str) -> Dict[str, Any]:
    """Compute a summary roll-up for the venue's accountability log.

    Shape:
        {
            "total": int,
            "pending": int,
            "accepted": int,
            "dismissed": int,
            "estimated_impact_missed_aud": float,   # sum of dismissed events' impact
            "estimated_impact_pending_aud": float,  # sum of pending events' impact
            "acceptance_rate": float,               # accepted / (accepted + dismissed)
        }
    """
    hist = history(venue_id)
    total = len(hist)
    pending = accepted = dismissed = 0
    missed_aud = 0.0
    pending_aud = 0.0

    for ev in hist:
        status = ev.get("status", "pending")
        impact = ev.get("impact_estimate_aud") or 0.0
        try:
            impact = float(impact)
        except (TypeError, ValueError):
            impact = 0.0
        if status == "pending":
            pending += 1
            pending_aud += impact
        elif status == "accepted":
            accepted += 1
        elif status == "dismissed":
            dismissed += 1
            missed_aud += impact

    responded = accepted + dismissed
    acceptance_rate = (accepted / responded) if responded > 0 else 0.0

    return {
        "total": total,
        "pending": pending,
        "accepted": accepted,
        "dismissed": dismissed,
        "estimated_impact_missed_aud": round(missed_aud, 2),
        "estimated_impact_pending_aud": round(pending_aud, 2),
        "acceptance_rate": round(acceptance_rate, 4),
    }


def state(venue_id: str, recent_limit: int = 20) -> Dict[str, Any]:
    """Build the on-the-wire state view for the dashboard.

    `recent` is newest first, matching the UI render order.
    """
    hist = history(venue_id)
    recent = list(hist[-recent_limit:])
    recent.reverse()
    return {
        "venue_id": venue_id,
        "summary": summary(venue_id),
        "recent": recent,
        "generated_at": _now_iso(),
    }
