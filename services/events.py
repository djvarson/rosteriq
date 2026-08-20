"""
Event log — the ONE place RosterIQ records what happened, who did it, and
whether it worked. Seven categories, one table (audit_logs), one helper:

  audit        business actions a venue owner must be able to answer for later:
               who approved that timesheet, published that roster, changed that
               pay rate, pushed that bill to Xero, retired that procedure
  security     things the platform owner watches: failed logins, lockouts,
               denied access (401/403), rate limiting, cross-tenant attempts,
               demo-token misuse, credential changes
  error        unhandled exceptions with the correlation id so a support
               conversation ("it broke at 2:14pm") can be traced to a stack,
               plus a FINGERPRINT so the same bug hit 400 times is one row to
               fix, not 400 to read
  perf         requests/queries that crossed a slowness threshold — the log
               only keeps what is worth acting on, never every request
  integration  every outbound call to Xero/MYOB/Tanda/POS/AI: provider,
               operation, outcome, duration — so "is Xero down or are we?"
               is answerable after the fact
  ai           model calls: model, latency, tools used, outcome, and the
               thumbs-up/down a human gave the answer (the training signal)
  job          background/scheduled work: did it run, how long, did it fail

Every event automatically carries the caller (from the auth/tenant context),
the request correlation id, the client IP and a UTC timestamp. Payloads are
scrubbed of secrets/PII-shaped keys before they are stored.

Recording an event must NEVER break the action being recorded: every write is
best-effort, swallowed and logged at WARNING on failure. The structured stdout
log still receives every event as well (Railway drain / grep), so the DB row
and the log line are two views of one fact.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger("rosteriq.events")

CATEGORIES = ("audit", "security", "error", "perf", "integration", "ai", "job")

# Categories whose volume is driven by traffic rather than by human action:
# throttled per (action, minute) so a storm costs a handful of rows, not a table.
HIGH_VOLUME = ("perf", "integration", "ai")
MAX_PER_ACTION_PER_MINUTE = 20

# Keys whose VALUES are never stored (case-insensitive substring match).
_SECRET_KEY_HINTS = (
    "password", "passwd", "secret", "token", "authorization", "api_key",
    "apikey", "client_secret", "refresh_token", "access_token", "cf_password",
    "card", "cvv", "tfn", "tax_file",
)
_MAX_DETAIL_CHARS = 4000


def _scrub(value: Any, depth: int = 0) -> Any:
    """Redact secret-shaped keys and cap size; keeps events safe to store."""
    if depth > 4:
        return "…"
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            ks = str(k)
            if any(h in ks.lower() for h in _SECRET_KEY_HINTS):
                out[ks] = "[redacted]"
            else:
                out[ks] = _scrub(v, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        return [_scrub(v, depth + 1) for v in list(value)[:50]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and len(value) > 500:
            return value[:497] + "..."
        return value
    return str(value)[:200]


def _caller() -> dict:
    """Best-effort actor + request context from whatever middleware set."""
    ctx: dict = {}
    try:
        from rosteriq.middleware.tenant import get_tenant_context_optional
        t = get_tenant_context_optional()
        if t is not None:
            ctx["user_id"] = getattr(t, "user_id", None)
            ctx["role"] = "owner" if getattr(t, "is_owner", False) else getattr(t, "role", None)
    except Exception:
        pass
    try:
        from rosteriq.middleware.logging import get_correlation_id, get_request_user
        ctx["correlation_id"] = get_correlation_id()
        if not ctx.get("user_id"):
            ctx["user_id"] = get_request_user()
    except Exception:
        pass
    try:
        from rosteriq.middleware.logging import get_request_ip  # optional
        ctx["ip"] = get_request_ip()
    except Exception:
        pass
    return ctx


_throttle_window: dict = {}      # (action, minute) -> count


def _throttled(category: str, action: str) -> bool:
    """True when this action has already been recorded MAX_PER_ACTION_PER_MINUTE
    times this minute. Only applies to traffic-driven categories — an audit or
    security event is NEVER dropped."""
    if category not in HIGH_VOLUME:
        return False
    try:
        minute = int(datetime.utcnow().timestamp() // 60)
        key = (action, minute)
        n = _throttle_window.get(key, 0) + 1
        _throttle_window[key] = n
        if len(_throttle_window) > 512:            # keep the dict from growing
            for k in [k for k in _throttle_window if k[1] < minute - 2]:
                _throttle_window.pop(k, None)
        return n > MAX_PER_ACTION_PER_MINUTE
    except Exception:
        return False


def fingerprint(*parts) -> str:
    """Stable 10-char id for 'the same problem'. Ids, numbers, quoted strings
    and paths are normalised out, so 'Employee abc123 not found' and
    'Employee zz9987 not found' fingerprint identically — one bug is one row
    to fix, not four hundred to read."""
    import hashlib
    import re
    raw = " ".join(str(p) for p in parts if p)
    raw = re.sub(r"'[^']*'|\"[^\"]*\"", "'*'", raw)      # quoted values
    raw = re.sub(r"/[\w.\-]+", "/*", raw)                  # paths / urls
    # Identifier-shaped tokens: anything >=6 chars mixing letters and digits,
    # or long hex, or a bare number. Keeps real words, drops ids.
    raw = re.sub(r"\b(?=[\w-]*\d)[\w-]{6,}\b", "#", raw)
    raw = re.sub(r"\b[0-9a-fA-F]{8,}\b|\b\d+\b", "#", raw)
    return hashlib.sha256(re.sub(r"\s+", " ", raw).strip().lower().encode()).hexdigest()[:10]


def record_event(
    category: str,
    action: str,
    *,
    venue_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    outcome: str = "ok",
    details: Optional[dict] = None,
    user_id: Optional[str] = None,
    db=None,
) -> None:
    """Record one event. Never raises.

    action is a stable dotted verb like "timesheet.approve", "roster.publish",
    "auth.login_failed", "access.denied", "sop.delete", "xero.bill_push".
    outcome is "ok" | "denied" | "failed" | "error".
    """
    if category not in CATEGORIES:
        category = "audit"
    if _throttled(category, action):
        return
    caller = _caller()
    payload = {
        "category": category,
        "outcome": outcome,
        "correlation_id": caller.get("correlation_id"),
        "ip": caller.get("ip"),
        "role": caller.get("role"),
        **(_scrub(details or {})),
    }
    # Hard cap so a runaway payload can't bloat the table
    encoded = json.dumps(payload, default=str)
    if len(encoded) > _MAX_DETAIL_CHARS:
        payload = {"category": category, "outcome": outcome,
                   "correlation_id": caller.get("correlation_id"),
                   "truncated": True, "preview": encoded[:_MAX_DETAIL_CHARS - 200]}

    entry = {
        "venue_id": venue_id,
        "user_id": user_id or caller.get("user_id"),
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "details": payload,
        "created_at": datetime.utcnow(),
    }
    # 1) structured stdout — always
    try:
        level = logging.WARNING if category in ("security", "error") or outcome != "ok" else logging.INFO
        logger.log(level, f"event {category}:{action} {outcome}",
                   extra={"event": {k: v for k, v in entry.items() if k != "created_at"}})
    except Exception:
        pass
    # 2) durable row — best effort
    try:
        if db is None:
            from rosteriq.database import get_db
            db = get_db()
        db.save_audit_log(entry)
    except Exception as e:  # noqa: BLE001 — recording must never break the action
        logger.warning(f"event not persisted ({category}:{action}): {e}")


# Convenience wrappers — keep call sites one line and the vocabulary stable.

def audit(action: str, venue_id: Optional[str], resource_type: str = None,
          resource_id: str = None, **details) -> None:
    record_event("audit", action, venue_id=venue_id, resource_type=resource_type,
                 resource_id=resource_id, details=details or None)


def security(action: str, outcome: str = "denied", venue_id: Optional[str] = None,
             user_id: Optional[str] = None, **details) -> None:
    record_event("security", action, venue_id=venue_id, outcome=outcome,
                 details=details or None, user_id=user_id)


def error(action: str, exc: BaseException, venue_id: Optional[str] = None, **details) -> None:
    d = {
        "exception": type(exc).__name__,
        "message": str(exc)[:500],
        # Group repeats: the same bug hit all day is ONE thing to fix.
        "fingerprint": fingerprint(type(exc).__name__, action, str(exc)[:200]),
    }
    d.update(details)
    record_event("error", action, venue_id=venue_id, outcome="error", details=d)


def perf(action: str, duration_ms: float, venue_id: Optional[str] = None,
         threshold_ms: float = 0, **details) -> None:
    """Record something that was SLOW. Below threshold_ms nothing is written —
    the log keeps what is worth acting on, not every request."""
    if threshold_ms and duration_ms < threshold_ms:
        return
    d = {"duration_ms": round(float(duration_ms), 1)}
    d.update(details)
    record_event("perf", action, venue_id=venue_id,
                 outcome="slow" if duration_ms else "ok", details=d)


def integration(provider: str, operation: str, outcome: str = "ok",
                duration_ms: Optional[float] = None, venue_id: Optional[str] = None,
                **details) -> None:
    """One outbound call to someone else's system (Xero, MYOB, Tanda, POS, AI).
    Answers 'is their API down or is it us?' after the fact."""
    d = {"provider": provider, "operation": operation}
    if duration_ms is not None:
        d["duration_ms"] = round(float(duration_ms), 1)
    d.update(details)
    record_event("integration", f"{provider}.{operation}", venue_id=venue_id,
                 outcome=outcome, details=d)


def ai(action: str, outcome: str = "ok", venue_id: Optional[str] = None, **details) -> None:
    """A model call or a human's verdict on one (action 'ai.chat',
    'ai.feedback', 'ai.insights'). The thumbs are the training signal."""
    record_event("ai", action, venue_id=venue_id, outcome=outcome, details=details or None)


def job(name: str, outcome: str = "ok", duration_ms: Optional[float] = None,
        venue_id: Optional[str] = None, **details) -> None:
    """Background/scheduled work. A job that stops running is invisible without
    this — the digest flags 'last ran 3 days ago'."""
    d = {"job": name}
    if duration_ms is not None:
        d["duration_ms"] = round(float(duration_ms), 1)
    d.update(details)
    record_event("job", f"job.{name}", venue_id=venue_id, outcome=outcome, details=d)


class track:
    """Time a call and record it, however it ends. Never swallows the exception.

        with track("xero", "push_bill", venue_id=vid) as t:
            resp = client.post(...)
            t.detail(invoice_id=inv.id)

    On an exception the event is recorded with outcome="failed" and the
    exception type, then the exception continues to propagate.
    """

    def __init__(self, provider: str, operation: str, venue_id: Optional[str] = None,
                 slow_ms: float = 3000, **details):
        self.provider, self.operation = provider, operation
        self.venue_id, self.slow_ms = venue_id, slow_ms
        self.details = dict(details)
        self._t0 = None

    def detail(self, **kw):
        """Add fields known only once the call succeeded."""
        self.details.update(kw)
        return self

    def __enter__(self):
        import time as _t
        self._t0 = _t.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        import time as _t
        # `self._t0 or ...` would re-read the clock when t0 is exactly 0.0
        # and report a 0ms call; test it for None, not for truthiness.
        start = self._t0 if self._t0 is not None else _t.perf_counter()
        ms = (_t.perf_counter() - start) * 1000
        if exc is None:
            outcome = "slow" if ms >= self.slow_ms else "ok"
        else:
            outcome = "failed"
            self.details["exception"] = exc_type.__name__ if exc_type else "?"
            self.details["message"] = str(exc)[:300]
            self.details["fingerprint"] = fingerprint(
                exc_type.__name__ if exc_type else "?", self.provider, self.operation, str(exc)[:200])
        # Successes are recorded too — without them a failure rate has no
        # denominator and "Xero has been fine all week" is unprovable. The
        # HIGH_VOLUME throttle (20/min/action) is what keeps this cheap.
        integration(self.provider, self.operation, outcome=outcome,
                    duration_ms=ms, venue_id=self.venue_id, **self.details)
        return False        # never suppress
