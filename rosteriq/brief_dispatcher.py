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
    """Writes both a text and a JSON copy of the brief to ``dir_path``.

    The filename template makes it easy for a human to scan the
    directory chronologically:

        morning_brief_{venue_id}_{date}.txt
        morning_brief_{venue_id}_{date}.json

    Previous runs on the same day are overwritten — dispatch is
    idempotent by design, and we don't want a full day of noisy polls
    leaving a crumb trail.
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
            date = brief.get("date") or "unknown"
            base = os.path.join(
                self.dir_path,
                f"morning_brief_{_slug(venue_id)}_{date}",
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

    This is the only sink that touches the network. Tests avoid
    instantiating it so the suite stays offline; the unit tests use
    ``MemorySink`` instead.
    """

    name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        timeout_s: float = 5.0,
        transform: Optional[Callable[[Dict[str, Any], str], Dict[str, Any]]] = None,
    ) -> None:
        self.url = url
        self.timeout_s = float(timeout_s)
        # Optional transform lets the caller adapt the payload for
        # Slack's {"text": "..."} shape without subclassing.
        self.transform = transform

    def send(
        self,
        *,
        venue_id: str,
        brief: Dict[str, Any],
        text_body: str,
    ) -> Dict[str, Any]:
        try:
            import urllib.request  # lazy
            payload = (
                self.transform(brief, text_body)
                if self.transform
                else {"venue_id": venue_id, "brief": brief, "text": text_body}
            )
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = getattr(resp, "status", 200)
            return {"status": "ok", "detail": f"webhook:{code}"}
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}


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
