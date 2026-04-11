"""Morning Brief dispatcher — route the daily digest to its audience.

Moment 12. The brief composer (``rosteriq.morning_brief``) produces a
pure-data dict; this module takes that dict and actually *delivers*
it — to disk, stdout, a webhook, or any custom sink a caller plugs in.

The module is deliberately tiny and sink-based instead of baking in
SMTP/Slack/Twilio bindings. Reasons:

1. Each transport has its own secrets (API keys, tokens, from-addresses)
   that belong in a deploy config, not in the library layer.
2. Different venues may want different transports — Mojo's to email,
   Earl's to a Slack channel, Francine's to both.
3. Tests should not need network or a file system of a specific shape.
   The ``MemorySink`` lives here for exactly that reason.

Pure stdlib. No fastapi, no pydantic, no http client deps beyond the
optional ``WebhookSink`` which uses ``urllib.request``. Tests live in
``tests/test_brief_dispatcher.py``.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol

from rosteriq import morning_brief as _morning_brief
from rosteriq import weekly_digest as _weekly_digest


# ---------------------------------------------------------------------------
# Protocols / data shapes
# ---------------------------------------------------------------------------

class Sink(Protocol):
    """A sink receives a rendered brief and delivers it somewhere.

    Implementations must be safe to call multiple times per dispatch
    cycle — dispatch is fan-out, not fan-in.
    """

    name: str

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        """Deliver the brief. Return a result dict with at least
        ``{"status": "ok"|"error", "detail": "..."}``. Implementations
        MUST NOT raise — they should catch and return error details."""
        ...


# ---------------------------------------------------------------------------
# Built-in sinks
# ---------------------------------------------------------------------------

class MemorySink:
    """Collects briefs in-memory. Used by tests and by the debug UI."""

    name = "memory"

    def __init__(self) -> None:
        self.delivered: List[Dict[str, Any]] = []

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        self.delivered.append({
            "venue_id": venue_id,
            "brief": brief,
            "text_body": text_body,
            "delivered_at": _now_iso(),
        })
        return {"status": "ok", "detail": f"memory:{len(self.delivered)}"}


class StdoutSink:
    """Pipes the text body to stdout. Useful for local dev and the
    ``/text`` curl-in-cron pattern."""

    name = "stdout"

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        try:
            sys.stdout.write(text_body)
            if not text_body.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()
            return {"status": "ok", "detail": "stdout"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}


class FileSink:
    """Writes both a text and a JSON copy of the payload to ``dir_path``.

    The filename template makes it easy for a human to scan the
    directory chronologically:

        {kind}_{venue_id}_{date}.txt
        {kind}_{venue_id}_{date}.json

    ``kind`` is read from ``brief["_kind"]`` and defaults to
    ``morning_brief`` so existing morning-brief dispatch behavior is
    unchanged. Moment 14-follow-on 1 introduces ``weekly_digest`` as
    a second kind so a weekly dispatch can share the same FileSink
    without clobbering the daily file.

    Previous runs on the same (kind, venue, date) are overwritten —
    dispatch is idempotent by design, and we don't want a full day of
    noisy polls leaving a crumb trail.
    """

    name = "file"

    def __init__(self, dir_path: str) -> None:
        self.dir_path = str(dir_path)

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        try:
            os.makedirs(self.dir_path, exist_ok=True)
            kind = _slug(str(brief.get("_kind") or "morning_brief"))
            date = brief.get("date") or "unknown"
            base = os.path.join(
                self.dir_path,
                f"{kind}_{_slug(venue_id)}_{date}",
            )
            txt_path = base + ".txt"
            json_path = base + ".json"
            with open(txt_path, "w", encoding="utf-8") as fh:
                fh.write(text_body)
            with open(json_path, "w", encoding="utf-8") as fh:
                json.dump(brief, fh, indent=2, sort_keys=True)
            return {"status": "ok", "detail": txt_path}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}


class WebhookSink:
    """POSTs a JSON brief to a URL. Handy for Slack Incoming Webhooks,
    Zapier catches, Discord bots, etc.

    Moment 14c made this production-safe:

    - **Retry with exponential backoff.** Configurable attempts
      (default 3) and base delay (default 0.25s). Transient 5xx
      responses, timeouts, and connection errors all retry. 4xx
      (except 408 and 429) fail fast — a 400 won't get better with
      a retry, and retrying a 401 is worse than useless.
    - **Dead-letter file.** If all retries are exhausted, the
      payload is appended to a .jsonl dead-letter file so the caller
      can inspect, re-drive, or alert on it. Defaults to None (no
      dead-letter) to keep existing behavior, but production should
      always set this.
    - **Injected sleep hook.** Tests can pass ``sleep=lambda s: None``
      to skip the real time.sleep and still drive the retry loop
      deterministically.

    Contract: ``send`` never raises. It always returns a result dict
    so the dispatcher's per-sink error isolation stays intact.
    """

    name = "webhook"

    # Status codes that should trigger a retry. 5xx are transient by
    # spec; 408 (Request Timeout) and 429 (Too Many Requests) are
    # explicitly retryable even though they're 4xx.
    RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(
        self,
        url: str,
        *,
        timeout_s: float = 5.0,
        transform: Optional[Callable[[Dict[str, Any], str], Dict[str, Any]]] = None,
        max_attempts: int = 3,
        backoff_base_s: float = 0.25,
        backoff_cap_s: float = 8.0,
        dead_letter_path: Optional[str] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.url = url
        self.timeout_s = float(timeout_s)
        # Optional transform lets the caller adapt the payload for
        # Slack's {"text": "..."} shape without subclassing.
        self.transform = transform
        self.max_attempts = max(1, int(max_attempts))
        self.backoff_base_s = max(0.0, float(backoff_base_s))
        self.backoff_cap_s = max(0.0, float(backoff_cap_s))
        self.dead_letter_path = dead_letter_path
        # Default to real time.sleep but allow tests to inject a no-op.
        if sleep is None:
            import time as _time
            self._sleep = _time.sleep
        else:
            self._sleep = sleep

    def _build_payload(
        self,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        if self.transform:
            out = self.transform(brief, text_body)
            return out if isinstance(out, dict) else {"raw": out}
        return {"venue_id": venue_id, "brief": brief, "text": text_body}

    def _compute_backoff(self, attempt_index: int) -> float:
        """Exponential backoff: base * 2^n, capped. ``attempt_index``
        is 0-based — the delay AFTER attempt 0 (before attempt 1).
        """
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
        """Append a dead-letter entry and return the path used.

        Catches and swallows I/O errors so the main send() return
        path stays clean — a dead-letter write failure should never
        take down the dispatcher.
        """
        if not self.dead_letter_path:
            return None
        try:
            directory = os.path.dirname(self.dead_letter_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            entry = {
                "ts": _now_iso(),
                "url": self.url,
                "venue_id": venue_id,
                "payload": payload,
                "attempts": attempts,
            }
            with open(self.dead_letter_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            return self.dead_letter_path
        except Exception:
            return None

    def _attempt_once(self, data: bytes) -> Dict[str, Any]:
        """Single POST attempt. Returns a result dict with a `retryable`
        flag so the caller knows whether to burn another attempt.
        """
        import urllib.request  # lazy
        import urllib.error
        try:
            req = urllib.request.Request(
                self.url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = getattr(resp, "status", 200)
            return {"ok": True, "status_code": code, "retryable": False}
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
            # Timeouts (socket.timeout) and anything else: treat as
            # retryable — the request didn't complete.
            return {
                "ok": False,
                "status_code": 0,
                "error": str(e),
                "retryable": True,
            }

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        try:
            payload = self._build_payload(venue_id, brief, text_body)
            data = json.dumps(payload).encode("utf-8")
        except Exception as exc:
            # Payload could not even be built — nothing retry will fix.
            return {"status": "error", "detail": f"payload_build_failed: {exc}"}

        attempts: List[Dict[str, Any]] = []
        last_error: Optional[str] = None
        last_status: int = 0

        for i in range(self.max_attempts):
            result = self._attempt_once(data)
            attempts.append({
                "attempt": i + 1,
                "ok": bool(result.get("ok")),
                "status_code": int(result.get("status_code") or 0),
                "error": result.get("error"),
            })
            if result.get("ok"):
                return {
                    "status": "ok",
                    "detail": f"webhook:{result.get('status_code')}",
                    "attempts": attempts,
                }
            last_error = str(result.get("error") or "unknown")
            last_status = int(result.get("status_code") or 0)
            if not result.get("retryable"):
                # Non-retryable — bail out immediately and dead-letter.
                break
            # Don't sleep after the final attempt — it'd just waste time.
            if i < self.max_attempts - 1:
                delay = self._compute_backoff(i)
                if delay > 0:
                    try:
                        self._sleep(delay)
                    except Exception:
                        pass

        dl_path = self._write_dead_letter(venue_id, payload, attempts)
        return {
            "status": "error",
            "detail": f"webhook_failed:{last_status}:{last_error}",
            "attempts": attempts,
            "dead_lettered_to": dl_path,
        }


def read_dead_letter(
    path: str,
    *,
    venue_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Read back the webhook dead-letter file, most recent first.

    Mirrors ``tanda_writeback.read_journal`` — filters by venue_id if
    given, returns empty list on missing file, tolerates malformed
    lines in the middle of the file.
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


# ---------------------------------------------------------------------------
# Registry + dispatch
# ---------------------------------------------------------------------------

# Global in-memory registry: { venue_id -> {label, sink_names} }
# In production a persistence layer would own this, but for the
# initial ship a manager can POST to /api/v1/brief-dispatch/register
# and the registry is enough to drive the 7am cron.
_VENUE_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Sink lookup: name -> Sink instance
_SINKS: Dict[str, Sink] = {}


def register_sink(sink: Sink) -> None:
    """Register a sink instance so venues can reference it by name."""
    if not hasattr(sink, "name") or not sink.name:
        raise ValueError("Sink must have a non-empty 'name' attribute")
    _SINKS[sink.name] = sink


def unregister_sink(name: str) -> None:
    _SINKS.pop(name, None)


def get_sinks() -> Dict[str, Sink]:
    return _SINKS


def clear_sinks() -> None:
    _SINKS.clear()


def register_venue(
    venue_id: str,
    *,
    label: Optional[str] = None,
    sinks: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Register or update a venue's dispatch entry."""
    vid = str(venue_id or "").strip()
    if not vid:
        raise ValueError("venue_id is required")
    entry = {
        "venue_id": vid,
        "label": (label or vid).strip() or vid,
        "sinks": list(sinks or []),
        "registered_at": _now_iso(),
    }
    _VENUE_REGISTRY[vid] = entry
    return entry


def unregister_venue(venue_id: str) -> None:
    _VENUE_REGISTRY.pop(str(venue_id or ""), None)


def get_registry() -> Dict[str, Dict[str, Any]]:
    return _VENUE_REGISTRY


def clear_registry() -> None:
    _VENUE_REGISTRY.clear()


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def dispatch_brief(
    venue_id: str,
    *,
    target_date: Optional[str] = None,
    yesterday_recap: Optional[Dict[str, Any]] = None,
    venue_label: Optional[str] = None,
    sinks: Optional[Iterable[Sink]] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """Compose + deliver a single venue's brief.

    Args:
        venue_id: The venue to brief.
        target_date: YYYY-MM-DD; defaults to yesterday UTC.
        yesterday_recap: Optional recap dict to enrich the brief.
        venue_label: Optional human label for the header.
        sinks: Explicit list of sinks to fan out to. When omitted,
            the function looks up the venue's registered sink names
            and pulls the corresponding instances from the module
            registry.
        store: Injectable accountability-store stub for tests.

    Returns:
        Dict shaped:
            {
                "venue_id": str,
                "brief": {...the composed brief...},
                "delivered": [{"sink": name, "status": "ok"|"error", "detail": str}, ...],
                "text_body": str,
            }
    """
    # Fall back on the registry's label if the caller didn't pass one
    # explicitly — this is what lets dispatch_all hand off the label
    # via the registry without every caller threading it through.
    registry_entry = _VENUE_REGISTRY.get(str(venue_id), {})
    effective_label = venue_label or registry_entry.get("label")

    brief = _morning_brief.compose_brief_from_store(
        venue_id,
        target_date=target_date,
        yesterday_recap=yesterday_recap,
        venue_label=effective_label,
        store=store,
    )
    text_body = _morning_brief.render_text(brief)

    # Resolve sinks: explicit > venue registry > empty
    sink_list: List[Sink] = []
    if sinks is not None:
        sink_list = list(sinks)
    else:
        for name in registry_entry.get("sinks") or []:
            s = _SINKS.get(name)
            if s is not None:
                sink_list.append(s)

    delivered: List[Dict[str, Any]] = []
    for sink in sink_list:
        try:
            res = sink.send(venue_id=venue_id, brief=brief, text_body=text_body)
        except Exception as exc:
            # Belt-and-braces — sinks shouldn't raise, but if they do,
            # one broken sink must not black out the others.
            res = {"status": "error", "detail": f"uncaught: {exc}"}
        name = getattr(sink, "name", "?")
        delivered.append({
            "sink": name,
            "status": res.get("status", "error"),
            "detail": res.get("detail", ""),
        })

    return {
        "venue_id": venue_id,
        "brief": brief,
        "delivered": delivered,
        "text_body": text_body,
    }


def dispatch_all(
    *,
    target_date: Optional[str] = None,
    recap_fetcher: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    store: Any = None,
) -> Dict[str, Any]:
    """Walk the venue registry and dispatch a brief for every venue.

    Intended for use as a cron/scheduled-task target:

        from rosteriq.brief_dispatcher import dispatch_all
        result = dispatch_all()

    Args:
        target_date: YYYY-MM-DD override. Defaults to yesterday UTC.
        recap_fetcher: Optional callable that takes a venue_id and
            returns a yesterday-recap dict (or None if unavailable).
            When absent, briefs go out without recap context.
        store: Injectable accountability store.

    Returns:
        Dict with a ``results`` list (one entry per venue) and a
        ``summary`` tally. Never raises.
    """
    results: List[Dict[str, Any]] = []
    ok_count = err_count = 0

    for vid, entry in list(_VENUE_REGISTRY.items()):
        label = entry.get("label") or vid
        try:
            recap = recap_fetcher(vid) if recap_fetcher else None
        except Exception:
            recap = None
        try:
            res = dispatch_brief(
                vid,
                target_date=target_date,
                yesterday_recap=recap,
                venue_label=label,
                store=store,
            )
            for d in res["delivered"]:
                if d.get("status") == "ok":
                    ok_count += 1
                else:
                    err_count += 1
            results.append(res)
        except Exception as exc:
            err_count += 1
            results.append({
                "venue_id": vid,
                "error": str(exc),
                "delivered": [],
            })

    return {
        "results": results,
        "summary": {
            "venues": len(results),
            "deliveries_ok": ok_count,
            "deliveries_error": err_count,
            "dispatched_at": _now_iso(),
        },
    }


# ---------------------------------------------------------------------------
# Weekly digest dispatch (Moment 14-follow-on 1)
# ---------------------------------------------------------------------------

WEEKLY_DIGEST_KIND = "weekly_digest"


def _stamp_kind(payload: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """Return a shallow copy of ``payload`` with ``_kind`` set.

    We copy instead of mutating the composer output so callers that
    hold the original dict are not surprised by an out-of-band
    ``_kind`` key appearing in it.
    """
    out = dict(payload or {})
    out["_kind"] = kind
    return out


def dispatch_weekly_digest(
    venue_id: str,
    *,
    week_ending: Optional[str] = None,
    window_days: int = 7,
    venue_label: Optional[str] = None,
    sinks: Optional[Iterable[Sink]] = None,
    store: Any = None,
    only_when_should_send: bool = False,
) -> Dict[str, Any]:
    """Compose + deliver a single venue's weekly digest.

    Reuses the same sink registry as ``dispatch_brief``: if a venue
    wants its weekly digest to go to the same Slack webhook as its
    daily brief, nothing needs to change — the digest flows through
    the same fan-out as a fresh payload with ``_kind="weekly_digest"``.

    Args:
        venue_id: The venue to digest.
        week_ending: YYYY-MM-DD; defaults to yesterday UTC.
        window_days: 7, 14, or 28. Anything else is clamped by the
            composer.
        venue_label: Optional human label for the header. Falls back
            to the registry entry when absent.
        sinks: Explicit list of sinks. When omitted, the function uses
            the venue's registered sink names from the module registry.
        store: Injectable accountability-store stub for tests.
        only_when_should_send: When True, skip dispatch entirely if
            the composer's ``should_send`` heuristic is False (e.g. a
            clean week with no dismissed dollars — you don't need to
            wake the whole team up for that).

    Returns:
        Dict shaped like ``dispatch_brief`` but with ``digest`` and
        ``weekly`` keys:
            {
                "venue_id": str,
                "digest": {...composed digest...},
                "delivered": [{"sink": name, "status": ..., "detail": ...}, ...],
                "text_body": str,
                "skipped": bool,  # True when only_when_should_send skipped it
            }
    """
    registry_entry = _VENUE_REGISTRY.get(str(venue_id), {})
    effective_label = venue_label or registry_entry.get("label")

    digest = _weekly_digest.compose_weekly_digest_from_store(
        venue_id,
        week_ending=week_ending,
        window_days=window_days,
        venue_label=effective_label,
        store=store,
    )
    text_body = _weekly_digest.render_text(digest)

    should_send = bool(digest.get("should_send"))
    if only_when_should_send and not should_send:
        return {
            "venue_id": venue_id,
            "digest": digest,
            "delivered": [],
            "text_body": text_body,
            "skipped": True,
        }

    # Stamp the payload so FileSink (and any future kind-aware sink)
    # can route the file name without clobbering the daily brief.
    stamped = _stamp_kind(digest, WEEKLY_DIGEST_KIND)

    # Resolve sinks: explicit > venue registry > empty
    sink_list: List[Sink] = []
    if sinks is not None:
        sink_list = list(sinks)
    else:
        for name in registry_entry.get("sinks") or []:
            s = _SINKS.get(name)
            if s is not None:
                sink_list.append(s)

    delivered: List[Dict[str, Any]] = []
    for sink in sink_list:
        try:
            res = sink.send(venue_id=venue_id, brief=stamped, text_body=text_body)
        except Exception as exc:
            res = {"status": "error", "detail": f"uncaught: {exc}"}
        sink_name = getattr(sink, "name", "?")
        delivered.append({
            "sink": sink_name,
            "status": res.get("status", "error"),
            "detail": res.get("detail", ""),
        })

    return {
        "venue_id": venue_id,
        "digest": digest,
        "delivered": delivered,
        "text_body": text_body,
        "skipped": False,
    }


def dispatch_all_weekly_digests(
    *,
    week_ending: Optional[str] = None,
    window_days: int = 7,
    store: Any = None,
    only_when_should_send: bool = False,
) -> Dict[str, Any]:
    """Walk the venue registry and dispatch a weekly digest for every
    venue. Intended for a Monday-morning cron/scheduled-task target:

        from rosteriq.brief_dispatcher import dispatch_all_weekly_digests
        result = dispatch_all_weekly_digests()

    Args:
        week_ending: YYYY-MM-DD override. Defaults to yesterday UTC.
        window_days: 7, 14, or 28.
        store: Injectable accountability store.
        only_when_should_send: Skip venues whose composer returns
            ``should_send=False`` (a clean week with nothing dismissed
            and acceptance in the green).

    Returns:
        Dict with a ``results`` list (one entry per venue) and a
        ``summary`` tally. Never raises.
    """
    results: List[Dict[str, Any]] = []
    ok_count = err_count = skipped_count = 0

    for vid, entry in list(_VENUE_REGISTRY.items()):
        label = entry.get("label") or vid
        try:
            res = dispatch_weekly_digest(
                vid,
                week_ending=week_ending,
                window_days=window_days,
                venue_label=label,
                store=store,
                only_when_should_send=only_when_should_send,
            )
            if res.get("skipped"):
                skipped_count += 1
            for d in res.get("delivered") or []:
                if d.get("status") == "ok":
                    ok_count += 1
                else:
                    err_count += 1
            results.append(res)
        except Exception as exc:
            err_count += 1
            results.append({
                "venue_id": vid,
                "error": str(exc),
                "delivered": [],
            })

    return {
        "results": results,
        "summary": {
            "venues": len(results),
            "deliveries_ok": ok_count,
            "deliveries_error": err_count,
            "skipped": skipped_count,
            "dispatched_at": _now_iso(),
            "kind": WEEKLY_DIGEST_KIND,
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _slug(s: str) -> str:
    """Lowercase, filesystem-safe slug for filenames."""
    out = []
    for ch in str(s or "").lower():
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out) or "unknown"
