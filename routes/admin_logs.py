"""
Admin logging endpoints for RosterIQ (owner only).

Two views of one fact, both returned by every endpoint:

  * ``events`` (source=db)     — PRIMARY. The durable audit / security / error
    rows written by ``rosteriq.services.events`` into ``audit_logs``. Survives
    restarts, shared across workers, filterable by venue / category / action /
    since / correlation id.
  * ``buffer`` (source=buffer) — the raw stdout tail of THIS worker's in-memory
    ring buffer (last 1000 log records) for ad-hoc debugging of a live request.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from collections import deque
from functools import lru_cache

from fastapi import APIRouter, Query, HTTPException, Depends, Request
from pydantic import BaseModel, Field

from rosteriq.middleware.logging import get_structured_logger, get_correlation_id
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.services.events import security as _security


# ============================================================================
# Ring buffer for in-memory log storage
# ============================================================================

class LogEntry(BaseModel):
    """Structured log entry."""
    timestamp: datetime
    level: str
    logger: str
    message: str
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    method: Optional[str] = None
    path: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None
    extra: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        from_attributes = True


class LogBuffer:
    """Ring buffer for storing recent log entries."""

    def __init__(self, max_size: int = 1000):
        """
        Initialize log buffer.

        Args:
            max_size: Maximum number of log entries to store
        """
        self.max_size = max_size
        self.buffer: deque = deque(maxlen=max_size)
        self._lock = None  # Could use asyncio.Lock if needed

    def append(self, entry: Dict[str, Any]) -> None:
        """
        Add a log entry to the buffer.

        Args:
            entry: Log entry dictionary
        """
        self.buffer.append(entry)

    def get_all(self) -> List[Dict[str, Any]]:
        """Get all entries in buffer."""
        return list(self.buffer)

    def filter_by_level(self, entries: List[Dict], level: str) -> List[Dict]:
        """Filter entries by log level."""
        return [e for e in entries if e.get('level') == level]

    def filter_by_correlation_id(self, entries: List[Dict], correlation_id: str) -> List[Dict]:
        """Filter entries by correlation ID."""
        return [e for e in entries if e.get('correlation_id') == correlation_id]

    def filter_by_path_contains(self, entries: List[Dict], path_contains: str) -> List[Dict]:
        """Filter entries by path substring match."""
        return [e for e in entries if path_contains in e.get('path', '')]

    def filter_by_time_range(self, entries: List[Dict], start_time: Optional[datetime],
                            end_time: Optional[datetime]) -> List[Dict]:
        """Filter entries by timestamp range."""
        filtered = []
        for e in entries:
            ts = e.get('timestamp')
            if not isinstance(ts, datetime):
                try:
                    ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except Exception:
                    continue

            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue
            filtered.append(e)

        return filtered


# Global log buffer instance
_log_buffer = LogBuffer(max_size=1000)

# Custom handler to feed logs into buffer
class BufferingLogHandler(logging.Handler):
    """Log handler that writes to in-memory ring buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the buffer.

        Args:
            record: LogRecord to buffer
        """
        try:
            entry = {
                'timestamp': datetime.fromtimestamp(record.created),
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'correlation_id': getattr(record, 'correlation_id', None),
                'user_id': getattr(record, 'user_id', None),
                'method': getattr(record, 'method', None),
                'path': getattr(record, 'path', None),
                'status_code': getattr(record, 'status_code', None),
                'response_time_ms': getattr(record, 'response_time_ms', None),
                'extra': {
                    k: v for k, v in record.__dict__.items()
                    if k not in {
                        'name', 'msg', 'args', 'created', 'filename', 'funcName',
                        'levelname', 'levelno', 'lineno', 'module', 'msecs',
                        'message', 'pathname', 'process', 'processName', 'relativeCreated',
                        'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info',
                        'correlation_id', 'user_id', 'method', 'path', 'status_code',
                        'response_time_ms',
                    }
                }
            }
            _log_buffer.append(entry)
        except Exception:
            # Don't let logging errors break the app
            pass


# ============================================================================
# Dependencies (auth checks)
# ============================================================================

async def require_admin(request: Request, current_user: UserContext = Depends(get_current_user)) -> bool:
    """
    Dependency guarding every admin log endpoint.

    The routes expose PLATFORM-WIDE events (every venue), so a valid JWT is not
    enough: the caller must be a platform owner. Anything else is 403 and the
    attempt is recorded as a security event.
    """
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not current_user.is_owner:
        try:
            _security("access.denied", venue_id=None, user_id=current_user.user_id,
                      path=str(request.url.path), reason="admin logs require owner role")
        except Exception:
            pass
        raise HTTPException(status_code=403, detail="Admin logs require the owner role")
    return True


# ============================================================================
# Router setup
# ============================================================================

admin_logs_router = APIRouter(prefix='/api/v1/admin', tags=['admin'],
                              dependencies=[Depends(require_admin)])

logger = get_structured_logger(__name__)

_CATEGORIES = ("audit", "security", "error")
_META_KEYS = ("category", "outcome", "correlation_id", "ip", "role")


def _buffer_entry(e: Dict[str, Any]) -> LogEntry:
    return LogEntry(
        timestamp=e['timestamp'],
        level=e['level'],
        logger=e['logger'],
        message=e['message'],
        correlation_id=e.get('correlation_id'),
        user_id=e.get('user_id'),
        method=e.get('method'),
        path=e.get('path'),
        status_code=e.get('status_code'),
        response_time_ms=e.get('response_time_ms'),
        extra=e.get('extra', {}),
    )


def _db_event(row: Dict[str, Any]) -> Dict[str, Any]:
    """Shape one audit_logs row like /api/events does, tagged source=db."""
    d = row.get("details") or {}
    if not isinstance(d, dict):
        d = {"raw": d}
    ts = row.get("created_at")
    if isinstance(ts, datetime):
        ts = ts.isoformat()
    cat = d.get("category") or "audit"
    outcome = d.get("outcome") or "ok"
    level = "ERROR" if cat == "error" else ("WARNING" if cat == "security" or outcome != "ok" else "INFO")
    return {
        "source": "db",
        "id": row.get("id"),
        "timestamp": ts,
        "level": level,
        "category": cat,
        "outcome": outcome,
        "action": row.get("action"),
        "venue_id": row.get("venue_id"),
        "user_id": row.get("user_id"),
        "resource_type": row.get("resource_type"),
        "resource_id": row.get("resource_id"),
        "correlation_id": d.get("correlation_id"),
        "ip": d.get("ip"),
        "role": d.get("role"),
        "details": {k: v for k, v in d.items() if k not in _META_KEYS},
    }


def _db_events(venue_id=None, category=None, action_prefix=None, since=None,
               limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Primary source: durable events from the DB. Never raises (falls back to [])."""
    try:
        from rosteriq.database import get_db
        rows = get_db().list_events(venue_id=venue_id, category=category or None,
                                    action_prefix=action_prefix or None, since=since,
                                    limit=limit, offset=offset)
        return [_db_event(r) for r in rows]
    except Exception as e:  # noqa: BLE001 — admin view must still show the buffer
        logger.warning(f"admin logs: DB events unavailable: {e}")
        return []


# ============================================================================
# Endpoints
# ============================================================================

@admin_logs_router.get(
    '/logs',
    summary='Query recent events + logs',
    description='Durable events from the DB (primary) plus the per-worker stdout ring buffer',
    response_model=Dict[str, Any],
)
async def get_logs(
    level: Optional[str] = Query(None, description="Buffer only: filter by log level (INFO, WARNING, ERROR, DEBUG)"),
    start_time: Optional[datetime] = Query(None, description="Filter after this timestamp (events + buffer)"),
    end_time: Optional[datetime] = Query(None, description="Buffer only: filter before this timestamp"),
    correlation_id: Optional[str] = Query(None, description="Filter by request correlation ID (events + buffer)"),
    path_contains: Optional[str] = Query(None, description="Buffer only: filter by path substring"),
    venue_id: Optional[str] = Query(None, description="Events only: restrict to one venue"),
    category: Optional[str] = Query(None, description="Events only: audit | security | error"),
    action_prefix: Optional[str] = Query(None, description="Events only: e.g. auth. or timesheet."),
    since: Optional[datetime] = Query(None, description="Events only: alias for start_time"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of entries to return (per source)"),
    offset: int = Query(0, ge=0, description="Events only: pagination offset"),
):
    """
    Query recent activity. Two views of one fact, both returned:

    - ``events`` (source=db): the durable audit/security/error rows written by
      services/events.py — survives restarts and is shared across workers.
      This is the primary source.
    - ``buffer`` (source=buffer): the raw stdout tail of THIS worker's ring
      buffer (last 1000 log records) for ad-hoc debugging.
    """
    if category:
        category = category.strip().lower()
        if category not in _CATEGORIES:
            raise HTTPException(status_code=422, detail="category must be one of audit, security, error")
    since_dt = since or start_time
    if since_dt is not None and since_dt.tzinfo is not None:
        since_dt = (since_dt - since_dt.utcoffset()).replace(tzinfo=None)

    # --- primary: DB events -------------------------------------------------
    if correlation_id:
        # list_events has no correlation filter; pull a bounded window and match.
        events = [e for e in _db_events(venue_id=venue_id, category=category,
                                        action_prefix=action_prefix, since=since_dt,
                                        limit=2000, offset=0)
                  if e.get("correlation_id") == correlation_id][:limit]
    else:
        events = _db_events(venue_id=venue_id, category=category, action_prefix=action_prefix,
                            since=since_dt, limit=limit, offset=offset)

    # --- secondary: this worker's stdout ring buffer -----------------------
    entries = _log_buffer.get_all()
    if level:
        entries = _log_buffer.filter_by_level(entries, level.upper())
    if correlation_id:
        entries = _log_buffer.filter_by_correlation_id(entries, correlation_id)
    if path_contains:
        entries = _log_buffer.filter_by_path_contains(entries, path_contains)
    if start_time or end_time:
        entries = _log_buffer.filter_by_time_range(entries, start_time, end_time)
    entries = list(reversed(entries))[:limit]
    buffer = [_buffer_entry(e).model_dump() for e in entries]

    return {
        "source": "db+buffer",
        "count": len(events),
        "events": events,
        "buffer_count": len(buffer),
        "buffer": buffer,
        "buffer_size_max": _log_buffer.max_size,
    }


@admin_logs_router.get(
    '/logs/{correlation_id}',
    summary='Get all events + logs for a request',
    description='Retrieve every DB event and buffered log line for a specific request by correlation ID',
    response_model=Dict[str, Any],
)
async def get_logs_by_correlation_id(
    correlation_id: str,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of entries"),
):
    """
    Trace one request. Useful for a support conversation ("it broke at 2:14pm"):
    the correlation id from the error toast / X-Correlation-ID header finds the
    durable error event AND the surrounding log lines (if this worker served it).
    """
    events = [e for e in _db_events(limit=5000) if e.get("correlation_id") == correlation_id]
    events = list(reversed(events))[:limit]  # chronological (oldest first)

    entries = _log_buffer.get_all()
    entries = _log_buffer.filter_by_correlation_id(entries, correlation_id)
    entries = list(entries)[:limit]
    buffer = [_buffer_entry(e).model_dump() for e in entries]

    if not events and not buffer:
        raise HTTPException(
            status_code=404,
            detail=f"No logs found for correlation_id: {correlation_id}"
        )
    return {
        "source": "db+buffer",
        "correlation_id": correlation_id,
        "count": len(events),
        "events": events,
        "buffer_count": len(buffer),
        "buffer": buffer,
    }


@admin_logs_router.get(
    '/logs/stats/summary',
    summary='Get log statistics',
    description='Summary statistics of recent events (DB, last 7 days) and the buffer',
    response_model=Dict[str, Any],
)
async def get_log_stats(days: int = Query(7, ge=1, le=365)):
    """
    Get summary statistics: DB event counts by category/outcome for the window
    (primary) plus level counts for this worker's ring buffer.
    """
    since_dt = datetime.utcnow() - timedelta(days=days)
    events = _db_events(since=since_dt, limit=5000)
    by_category = {c: 0 for c in _CATEGORIES}
    by_outcome: Dict[str, int] = {}
    actions: Dict[str, int] = {}
    for e in events:
        by_category[e["category"]] = by_category.get(e["category"], 0) + 1
        by_outcome[e["outcome"]] = by_outcome.get(e["outcome"], 0) + 1
        actions[e["action"] or "?"] = actions.get(e["action"] or "?", 0) + 1
    top = sorted(actions.items(), key=lambda kv: (-kv[1], kv[0]))[:10]

    entries = _log_buffer.get_all()
    levels: Dict[str, int] = {}
    for entry in entries:
        level = entry.get('level', 'UNKNOWN')
        levels[level] = levels.get(level, 0) + 1

    return {
        'source': 'db+buffer',
        'days': days,
        'since': since_dt.isoformat(),
        'events_total': len(events),
        'by_category': by_category,
        'by_outcome': by_outcome,
        'top_actions': [{"action": a, "count": n} for a, n in top],
        # buffer (kept for backwards compatibility)
        'total_entries': len(entries),
        'levels': levels,
        'error_count': levels.get('ERROR', 0),
        'warning_count': levels.get('WARNING', 0),
        'buffer_size_max': _log_buffer.max_size,
        'buffer_utilization': f"{(len(entries) / _log_buffer.max_size * 100):.1f}%",
    }


# ============================================================================
# Initialization helper
# ============================================================================

def init_log_buffering():
    """
    Initialize log buffering by adding the handler to root logger.

    Call this during app startup to enable log collection.
    """
    handler = BufferingLogHandler()
    handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)
    logger.info("Log buffering initialized")
