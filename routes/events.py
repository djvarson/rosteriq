"""
Event log API — read side of ``rosteriq.services.events``.

Every audit / security / error event lands in the ``audit_logs`` table via the
spine (``services/events.py``). These routes let the dashboard show them:

    GET /api/events?venue_id=&category=&action_prefix=&since=&limit=&offset=
        Newest-first list. Managers/staff MUST pass a venue_id they can access
        (403 otherwise, and 403 when omitted). Owners may omit venue_id for a
        platform-wide view (that is how the "Platform security & errors" card
        on the dashboard is fed).
    GET /api/events/summary?venue_id=&days=7
        Counts by category + top 10 actions over the window. Same scoping.

Nothing here writes; the only side effect is the cross-tenant audit row that
``enforce_venue_access`` records on a denial.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/events", tags=["events"])

CATEGORIES = ("audit", "security", "error")
MAX_LIMIT = 500
# Keys the spine stores inside details that are surfaced as top-level fields.
_META_KEYS = ("category", "outcome", "correlation_id", "ip", "role")
_RELATIVE_SINCE = re.compile(r"^\s*(\d{1,4})\s*([hdmw])\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scope(current_user: UserContext, venue_id: Optional[str]) -> Optional[str]:
    """Resolve + enforce the venue scope for the caller.

    Returns the venue_id to query with (None == platform-wide, owners only).
    """
    venue_id = (venue_id or "").strip() or None
    if venue_id is None:
        if current_user.is_owner:
            return None
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="venue_id is required — only owners may view platform-wide events",
        )
    # Fail closed on the user context, then let the tenant guard record any
    # cross-tenant attempt the standard way.
    if not current_user.is_owner and venue_id not in (current_user.venue_ids or []):
        enforce_venue_access(venue_id)  # records the attempt + raises 403 when context exists
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this venue",
        )
    enforce_venue_access(venue_id)
    return venue_id


def _parse_since(since: Optional[str]) -> Optional[datetime]:
    """Accept an ISO-8601 datetime OR a relative window like ``24h`` / ``7d``."""
    if not since:
        return None
    s = str(since).strip()
    m = _RELATIVE_SINCE.match(s)
    if m:
        n, unit = int(m.group(1)), m.group(2).lower()
        delta = {"h": timedelta(hours=n), "d": timedelta(days=n),
                 "w": timedelta(weeks=n), "m": timedelta(minutes=n)}[unit]
        return datetime.utcnow() - delta
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=422, detail="since must be an ISO datetime or a window like 24h / 7d / 30d")
    if dt.tzinfo is not None:
        # The spine stores naive UTC; compare like with like.
        dt = (dt - dt.utcoffset()).replace(tzinfo=None)
    return dt


def _iso(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class _NameCache:
    """Resolve user ids to display names once per request."""

    def __init__(self, db):
        self.db = db
        self._cache: dict = {}

    def name(self, user_id) -> Optional[str]:
        if not user_id:
            return None
        key = str(user_id)
        if key in self._cache:
            return self._cache[key]
        resolved = key
        try:
            u = self.db.get_user_by_id(key)
            if u:
                resolved = u.get("name") or u.get("email") or key
        except Exception:  # noqa: BLE001 — a bad id must never break the listing
            resolved = key
        self._cache[key] = resolved
        return resolved


def _shape(row: dict, names: _NameCache) -> dict:
    details = row.get("details") or {}
    if not isinstance(details, dict):
        details = {"raw": details}
    rest = {k: v for k, v in details.items() if k not in _META_KEYS}
    return {
        "id": row.get("id"),
        "venue_id": row.get("venue_id"),
        "user_id": row.get("user_id"),
        "user_name": names.name(row.get("user_id")),
        "action": row.get("action"),
        "resource_type": row.get("resource_type"),
        "resource_id": row.get("resource_id"),
        "outcome": details.get("outcome") or "ok",
        "category": details.get("category") or "audit",
        "correlation_id": details.get("correlation_id"),
        "ip": details.get("ip"),
        "role": details.get("role"),
        "details": rest,
        "created_at": _iso(row.get("created_at")),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("")
async def list_events(
    venue_id: Optional[str] = Query(None),
    category: Optional[str] = Query(None, description="audit | security | error"),
    action_prefix: Optional[str] = Query(None, description="e.g. timesheet. or auth."),
    since: Optional[str] = Query(None, description="ISO datetime or 24h / 7d / 30d"),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
    current_user: UserContext = Depends(get_current_user),
):
    scope = _scope(current_user, venue_id)
    if category:
        category = category.strip().lower()
        if category not in CATEGORIES:
            raise HTTPException(status_code=422, detail="category must be one of audit, security, error")
    limit = min(int(limit), MAX_LIMIT)
    since_dt = _parse_since(since)
    db = get_db()
    rows = db.list_events(
        venue_id=scope, category=category or None,
        action_prefix=(action_prefix or "").strip() or None,
        since=since_dt, limit=limit, offset=offset,
    )
    names = _NameCache(db)
    events = [_shape(r, names) for r in rows]
    return {"count": len(events), "events": events, "venue_id": scope, "limit": limit, "offset": offset}


@router.get("/summary")
async def events_summary(
    venue_id: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
    current_user: UserContext = Depends(get_current_user),
):
    scope = _scope(current_user, venue_id)
    since_dt = datetime.utcnow() - timedelta(days=days)
    db = get_db()
    # One bounded pull, counted in Python — the events table is small per venue
    # and this keeps the memory + PG stores on identical code paths.
    rows = db.list_events(venue_id=scope, since=since_dt, limit=5000, offset=0)
    by_category = {c: 0 for c in CATEGORIES}
    by_outcome: dict = {}
    actions: dict = {}
    for r in rows:
        d = r.get("details") or {}
        if not isinstance(d, dict):
            d = {}
        cat = d.get("category") or "audit"
        by_category[cat] = by_category.get(cat, 0) + 1
        out = d.get("outcome") or "ok"
        by_outcome[out] = by_outcome.get(out, 0) + 1
        a = r.get("action") or "?"
        actions[a] = actions.get(a, 0) + 1
    top = sorted(actions.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    return {
        "venue_id": scope,
        "days": days,
        "since": since_dt.isoformat(),
        "total": len(rows),
        "by_category": by_category,
        "by_outcome": by_outcome,
        "top_actions": [{"action": a, "count": n} for a, n in top],
    }
