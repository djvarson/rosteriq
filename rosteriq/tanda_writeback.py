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


class TandaApiSink(WritebackSink):
    """POST accepted-rec writebacks to a real Tanda API endpoint.

    This is the "live plugin" sink. It's shaped to be dropped in behind
    the default JournalSink so every writeback is both logged locally
    and forwarded upstream. Production deployments flip the env vars
    ``TANDA_WRITEBACK_URL`` / ``TANDA_WRITEBACK_TOKEN`` and api_v2
    wires this sink into the registry on startup.

    Moment 14-follow-on 2 carried over the hardening pattern from
    Moment 14c's ``brief_dispatcher.WebhookSink``:

    - **Retry with exponential backoff.** Configurable attempts
      (default 3) and base delay (default 0.25s). Transient 5xx,
      408, 429, timeouts, and connection errors all retry. Other
      4xx fail fast — retrying a 401 is worse than useless, and a
      400 means the payload was wrong, not that the network was.
    - **Idempotency key.** Every request carries an
      ``Idempotency-Key`` header derived from the delta's source
      rec_id, so a retry that lands alongside the original never
      double-applies on the Tanda side.
    - **Dead-letter journal.** If all retries are exhausted, the
      payload is appended to a .jsonl dead-letter file so the caller
      can inspect, re-drive, or alert on it. Defaults to None so
      tests don't accidentally litter.
    - **Injectable sleep + transform.** Tests pass
      ``sleep=lambda s: None`` to skip real waits. ``transform`` lets
      a caller adapt the payload shape without subclassing, e.g. if
      Tanda's internal API eventually wants ``{"shift_delta": {...}}``
      instead of ``{"delta": {...}}``.

    Contract: ``apply`` never raises. It always returns a result dict
    so ``writeback_accepted_rec``'s per-sink error isolation stays
    intact.
    """

    name = "tanda_api"

    RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(
        self,
        url: str,
        *,
        api_token: Optional[str] = None,
        timeout_s: float = 5.0,
        transform: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
        max_attempts: int = 3,
        backoff_base_s: float = 0.25,
        backoff_cap_s: float = 8.0,
        dead_letter_path: Optional[str] = None,
        sleep: Optional[Callable[[float], None]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
    ) -> None:
        if name:
            self.name = str(name)
        self.url = url
        self.api_token = api_token
        self.timeout_s = float(timeout_s)
        self.transform = transform
        self.max_attempts = max(1, int(max_attempts))
        self.backoff_base_s = max(0.0, float(backoff_base_s))
        self.backoff_cap_s = max(0.0, float(backoff_cap_s))
        self.dead_letter_path = dead_letter_path
        self.extra_headers = dict(extra_headers or {})
        if sleep is None:
            import time as _time
            self._sleep = _time.sleep
        else:
            self._sleep = sleep

    # -- payload --------------------------------------------------------------

    def _build_payload(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        delta_dict = delta.to_dict()
        if self.transform:
            out = self.transform(str(venue_id), delta_dict)
            return out if isinstance(out, dict) else {"raw": out}
        return {
            "venue_id": str(venue_id),
            "rec_id": str(delta.source_rec_id or ""),
            "delta": delta_dict,
        }

    def _build_headers(self, delta: ShiftDelta) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        # Idempotency key — the accountability rec_id is already unique
        # per (venue, day, action_suffix) and stable across retries.
        idem = str(delta.source_rec_id or "").strip()
        if idem:
            headers["Idempotency-Key"] = idem
        headers.update(self.extra_headers)
        return headers

    # -- retry/backoff --------------------------------------------------------

    def _compute_backoff(self, attempt_index: int) -> float:
        if self.backoff_base_s <= 0:
            return 0.0
        delay = self.backoff_base_s * (2 ** attempt_index)
        if self.backoff_cap_s > 0 and delay > self.backoff_cap_s:
            delay = self.backoff_cap_s
        return delay

    def _write_dead_letter(
        self,
        venue_id: str,
        payload: Dict[str, Any],
        attempts: List[Dict[str, Any]],
    ) -> Optional[str]:
        if not self.dead_letter_path:
            return None
        try:
            directory = os.path.dirname(self.dead_letter_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            entry = {
                "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "url": self.url,
                "venue_id": str(venue_id),
                "payload": payload,
                "attempts": attempts,
            }
            with open(self.dead_letter_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            return self.dead_letter_path
        except Exception:
            return None

    def _attempt_once(self, data: bytes, headers: Dict[str, str]) -> Dict[str, Any]:
        """Single POST attempt. Returns a result dict with `retryable`."""
        import urllib.request  # lazy
        import urllib.error
        try:
            req = urllib.request.Request(
                self.url,
                data=data,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = getattr(resp, "status", 200)
                body_bytes = resp.read() if hasattr(resp, "read") else b""
            # Try to parse JSON body for the upstream sink detail — but
            # do not fail the apply if the response isn't JSON.
            body: Any = None
            try:
                body = json.loads(body_bytes.decode("utf-8")) if body_bytes else None
            except Exception:
                body = None
            return {
                "ok": True,
                "status_code": code,
                "retryable": False,
                "response": body,
            }
        except urllib.error.HTTPError as e:
            code = getattr(e, "code", 0)
            retry = code in self.RETRYABLE_STATUSES
            return {
                "ok": False,
                "status_code": code,
                "error": f"HTTP {code}",
                "retryable": retry,
            }
        except urllib.error.URLError as e:
            return {
                "ok": False,
                "status_code": 0,
                "error": f"URLError: {e}",
                "retryable": True,
            }
        except Exception as e:
            return {
                "ok": False,
                "status_code": 0,
                "error": str(e),
                "retryable": True,
            }

    # -- main apply -----------------------------------------------------------

    def apply(self, venue_id: str, delta: ShiftDelta) -> Dict[str, Any]:
        try:
            payload = self._build_payload(venue_id, delta)
            data = json.dumps(payload).encode("utf-8")
            headers = self._build_headers(delta)
        except Exception as exc:
            return {
                "sink": self.name,
                "ok": False,
                "error": f"payload_build_failed: {exc}",
            }

        attempts: List[Dict[str, Any]] = []
        last_error: Optional[str] = None
        last_status: int = 0
        last_response: Any = None

        for i in range(self.max_attempts):
            result = self._attempt_once(data, headers)
            attempts.append({
                "attempt": i + 1,
                "ok": bool(result.get("ok")),
                "status_code": int(result.get("status_code") or 0),
                "error": result.get("error"),
            })
            if result.get("ok"):
                return {
                    "sink": self.name,
                    "ok": True,
                    "status_code": int(result.get("status_code") or 0),
                    "response": result.get("response"),
                    "attempts": attempts,
                    "idempotency_key": headers.get("Idempotency-Key", ""),
                }
            last_error = str(result.get("error") or "unknown")
            last_status = int(result.get("status_code") or 0)
            last_response = result.get("response")
            if not result.get("retryable"):
                break
            if i < self.max_attempts - 1:
                delay = self._compute_backoff(i)
                if delay > 0:
                    try:
                        self._sleep(delay)
                    except Exception:
                        pass

        dl_path = self._write_dead_letter(venue_id, payload, attempts)
        return {
            "sink": self.name,
            "ok": False,
            "status_code": last_status,
            "error": f"tanda_api_failed:{last_status}:{last_error}",
            "attempts": attempts,
            "dead_lettered_to": dl_path,
            "response": last_response,
            "idempotency_key": headers.get("Idempotency-Key", ""),
        }


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


def read_dead_letter(
    path: str,
    *,
    venue_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Read back the Tanda writeback dead-letter file, most recent first.

    Mirrors ``read_journal`` — filters by venue_id if given, returns
    empty list on missing file, tolerates malformed lines.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return []
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
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
        out.append(entry)
        if len(out) >= limit:
            break
    return out


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
