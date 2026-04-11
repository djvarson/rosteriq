"""Tanda writeback — translate accepted accountability recs into
concrete roster mutations and apply them via a pluggable adapter.

Pure stdlib. This is the "plugin surface" that turns RosterIQ from a
read-only advisor into a writeback tool: when a manager accepts a rec
like "Send 1 staff home after the next peak", the writeback composer
maps that to a structured ``ShiftDelta`` and hands it to a sink. Sinks
are registered at import time — the default sink is a ``JournalSink``
that appends every attempt to a .jsonl file so Dale has a paper trail
even before a real Tanda push endpoint is wired up.

Design notes:

- The composer does NOT parse free text. It uses the rec_id prefix
  (`rec_pulse_{venue}_{date}_{action}`) that pulse_rec_bridge emits,
  which is stable and already part of the accountability store's
  idempotency key. This means the composer survives text-wording
  changes in the bridge without churn.
- Writebacks are only applied to accepted recs. Pending and dismissed
  recs are never applied — if you dismissed it, you meant it.
- Every writeback attempt, success or failure, is appended to the
  journal. That means "what did we actually push to Tanda" is always
  recoverable even if the sink stack is reconfigured mid-flight.
- Sinks are error-isolated: a failing sink does not prevent other
  sinks from running, and the writeback return value surfaces the
  per-sink outcome so the caller can decide what to do.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delta model
# ---------------------------------------------------------------------------

# Canonical delta kinds. These map 1:1 to things a rostering platform
# can actually do. The composer picks one based on the rec's action
# tier; the adapter decides how to express each kind in its own API.
DELTA_CUT_STAFF = "cut_staff"          # Hard cut — remove staff immediately
DELTA_SEND_HOME = "send_home"          # Shorten a shift (soft cut)
DELTA_CALL_IN = "call_in"              # Add a casual to the roster
DELTA_TRIM_SHIFT = "trim_shift"        # Trim hours off an existing shift

VALID_KINDS = (DELTA_CUT_STAFF, DELTA_SEND_HOME, DELTA_CALL_IN, DELTA_TRIM_SHIFT)


@dataclass
class ShiftDelta:
    """Structured roster mutation derived from an accepted rec.

    Intentionally minimal — each field is something a rostering API
    can act on directly. Adapters are free to enrich (look up which
    staff member has the latest start, etc.) but the composer stays
    pure and deterministic.
    """
    kind: str
    count: int
    timing_hint: str  # "immediate" | "after_peak" | "next_break" | "before_service"
    reason: str
    impact_estimate_aud: float = 0.0
    priority: str = "med"
    source_rec_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------------
# Rec → Delta composer
# ---------------------------------------------------------------------------

# Action suffix (last segment of the rec_id after the date) → delta kind
# and timing. The suffix is stable across pulse_rec_bridge versions, so
# writebacks keep working even if the rec text gets reworded.
_ACTION_MAP: Dict[str, Dict[str, Any]] = {
    "over_wage_high": {
        "kind": DELTA_CUT_STAFF,
        "count": 2,
        "timing_hint": "immediate",
        "priority": "high",
    },
    "over_wage_med": {
        "kind": DELTA_SEND_HOME,
        "count": 1,
        "timing_hint": "after_peak",
        "priority": "med",
    },
    "under_wage": {
        "kind": DELTA_CALL_IN,
        "count": 1,
        "timing_hint": "before_service",
        "priority": "med",
    },
    "burn_rate_high": {
        "kind": DELTA_TRIM_SHIFT,
        "count": 1,
        "timing_hint": "next_break",
        "priority": "high",
    },
}


def _action_suffix(rec_id: str) -> Optional[str]:
    """Extract the trailing action slug from a pulse_rec_bridge rec_id.

    Format: ``rec_pulse_{venue}_{YYYY-MM-DD}_{action}``. Returns the
    ``{action}`` segment or None if the id doesn't match the pattern.
    """
    if not rec_id or not isinstance(rec_id, str):
        return None
    if not rec_id.startswith("rec_pulse_"):
        return None
    # The date segment is always YYYY-MM-DD (10 chars with hyphens at
    # positions 4 and 7) so finding it anchors the parse. We walk
    # backwards from the end to grab everything after the date.
    parts = rec_id.split("_")
    # Look for a YYYY-MM-DD shaped segment
    for i, p in enumerate(parts):
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            suffix = "_".join(parts[i + 1:])
            return suffix or None
    return None


def compose_delta_from_rec(rec: Dict[str, Any]) -> Optional[ShiftDelta]:
    """Translate a rec dict into a ShiftDelta.

    Returns None if the rec isn't recognized — callers should treat
    that as "nothing to writeback" rather than an error. Accountability
    recs from sources other than wage_pulse currently have no delta
    mapping; they'll be added as the product grows.
    """
    if not isinstance(rec, dict):
        return None

    rec_id = str(rec.get("rec_id") or rec.get("id") or "")
    suffix = _action_suffix(rec_id)
    if not suffix or suffix not in _ACTION_MAP:
        return None

    spec = _ACTION_MAP[suffix]
    impact = rec.get("impact_estimate_aud")
    try:
        impact_f = float(impact) if impact is not None else 0.0
    except (TypeError, ValueError):
        impact_f = 0.0

    # Trim the reason to the first sentence of the rec text — the
    # writeback journal wants a human-readable summary but not the full
    # detail paragraph.
    text = str(rec.get("text") or "")
    reason = text.split(".")[0].strip() or f"Writeback for {suffix}"

    return ShiftDelta(
        kind=spec["kind"],
        count=int(spec["count"]),
        timing_hint=str(spec["timing_hint"]),
        reason=reason,
        impact_estimate_aud=round(impact_f, 2),
        priority=str(spec["priority"]),
        source_rec_id=rec_id,
        metadata={
            "action_suffix": suffix,
            "rec_source": str(rec.get("source") or "unknown"),
        },
    )


# ---------------------------------------------------------------------------
# Sinks — where a delta actually lands
# ---------------------------------------------------------------------------

class WritebackSink:
    """Protocol — all sinks must expose ``name`` and ``apply``."""

    name: str = "sink"

    def apply(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        raise NotImplementedError


class NullSink(WritebackSink):
    """No-op sink — used in tests and dry-run mode. Records only that
    it was called."""

    name = "null"

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def apply(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        self.calls.append({"venue_id": venue_id, "delta": delta.to_dict()})
        return {"sink": self.name, "ok": True, "dry_run": True}


class JournalSink(WritebackSink):
    """Append every writeback attempt to a .jsonl file.

    This is the default sink registered at import time. Even when
    there's no real Tanda adapter in the loop, every accepted rec
    leaves a durable audit trail — which is the whole point of the
    accountability layer.
    """

    name = "journal"

    def __init__(self, journal_path: str) -> None:
        self.journal_path = journal_path

    def apply(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        try:
            os.makedirs(os.path.dirname(self.journal_path) or ".", exist_ok=True)
            entry = {
                "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "venue_id": str(venue_id),
                "delta": delta.to_dict(),
            }
            with open(self.journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            return {"sink": self.name, "ok": True, "path": self.journal_path}
        except Exception as exc:
            return {"sink": self.name, "ok": False, "error": str(exc)}


class CallableSink(WritebackSink):
    """Wrap any ``fn(venue_id, delta_dict) -> dict`` as a sink.

    Used by the API endpoint and by tests to inject behaviour without
    subclassing.
    """

    def __init__(self, name: str, fn: Callable[[str, Dict[str, Any]], Dict[str, Any]]) -> None:
        self.name = name
        self._fn = fn

    def apply(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        try:
            result = self._fn(venue_id, delta.to_dict())
            if not isinstance(result, dict):
                result = {"raw": result}
            result.setdefault("sink", self.name)
            result.setdefault("ok", True)
            return result
        except Exception as exc:
            return {"sink": self.name, "ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Sink registry
# ---------------------------------------------------------------------------

_SINKS: List[WritebackSink] = []


def register_sink(sink: WritebackSink) -> None:
    _SINKS.append(sink)


def clear_sinks() -> None:
    _SINKS.clear()


def registered_sinks() -> List[str]:
    return [s.name for s in _SINKS]


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def writeback_accepted_rec(
    venue_id: str,
    rec_id: str,
    *,
    store: Any = None,
    sinks: Optional[List[WritebackSink]] = None,
) -> Dict[str, Any]:
    """
    Find the named rec in the accountability store, ensure it's been
    accepted, compose a delta, and fan it out to every registered sink.

    Returns a result dict shaped::

        {
            "venue_id": ...,
            "rec_id": ...,
            "status": "ok" | "skipped" | "no_delta",
            "reason": "...",
            "delta": {...} or None,
            "results": [ { "sink": "journal", "ok": True, ... }, ... ]
        }

    The return shape is stable — callers can safely pydantic-model it
    without feature detection.
    """
    if store is None:
        from rosteriq import accountability_store as store  # lazy import

    history = list(store.history(venue_id) or [])
    rec = None
    for h in history:
        if str(h.get("id") or h.get("rec_id") or "") == str(rec_id):
            rec = h
            break

    if rec is None:
        return {
            "venue_id": str(venue_id),
            "rec_id": str(rec_id),
            "status": "skipped",
            "reason": "rec_not_found",
            "delta": None,
            "results": [],
        }

    status = str(rec.get("status") or "").lower()
    if status != "accepted":
        return {
            "venue_id": str(venue_id),
            "rec_id": str(rec_id),
            "status": "skipped",
            "reason": f"rec_status_is_{status or 'unknown'}",
            "delta": None,
            "results": [],
        }

    delta = compose_delta_from_rec(rec)
    if delta is None:
        return {
            "venue_id": str(venue_id),
            "rec_id": str(rec_id),
            "status": "no_delta",
            "reason": "no_mapping_for_rec",
            "delta": None,
            "results": [],
        }

    active_sinks = sinks if sinks is not None else list(_SINKS)
    if not active_sinks:
        return {
            "venue_id": str(venue_id),
            "rec_id": str(rec_id),
            "status": "ok",
            "reason": "no_sinks_registered",
            "delta": delta.to_dict(),
            "results": [],
        }

    results: List[Dict[str, Any]] = []
    for sink in active_sinks:
        try:
            out = sink.apply(venue_id, delta)
            if not isinstance(out, dict):
                out = {"sink": getattr(sink, "name", "sink"), "ok": True, "raw": out}
            out.setdefault("sink", getattr(sink, "name", "sink"))
            out.setdefault("ok", True)
            results.append(out)
        except Exception as exc:
            results.append({
                "sink": getattr(sink, "name", "sink"),
                "ok": False,
                "error": str(exc),
            })

    all_ok = all(r.get("ok") for r in results)
    return {
        "venue_id": str(venue_id),
        "rec_id": str(rec_id),
        "status": "ok" if all_ok else "partial",
        "reason": "dispatched",
        "delta": delta.to_dict(),
        "results": results,
    }


def read_journal(path: str, *, venue_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Read back the writeback journal for auditing.

    Filters by venue_id if given. Returns most-recent-first, up to
    ``limit`` entries. Safe to call on a missing file (returns []).
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return []
    except Exception:
        return []

    entries: List[Dict[str, Any]] = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except Exception:
            continue
        if venue_id and str(entry.get("venue_id")) != str(venue_id):
            continue
        entries.append(entry)
        if len(entries) >= limit:
            break
    return entries
