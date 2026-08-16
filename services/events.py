"""
Event log — the ONE place RosterIQ records what happened, who did it, and
whether it worked. Three categories, one table (audit_logs), one helper:

  audit        business actions a venue owner must be able to answer for later:
               who approved that timesheet, published that roster, changed that
               pay rate, pushed that bill to Xero, retired that procedure
  security     things the platform owner watches: failed logins, lockouts,
               denied access (401/403), rate limiting, cross-tenant attempts,
               demo-token misuse, credential changes
  error        unhandled exceptions with the correlation id so a support
               conversation ("it broke at 2:14pm") can be traced to a stack

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

CATEGORIES = ("audit", "security", "error")

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
    d = {"exception": type(exc).__name__, "message": str(exc)[:500]}
    d.update(details)
    record_event("error", action, venue_id=venue_id, outcome="error", details=d)
