"""
Admin logging endpoints for RosterIQ.

Provides access to recent logs and request traces via correlation IDs.
Includes filtering by level, time range, path, and correlation ID.

Uses an in-memory ring buffer (last 1000 entries) for quick access to
recent logs without requiring external log storage.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from collections import deque
from functools import lru_cache

from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel, Field

from rosteriq.middleware.logging import get_structured_logger, get_correlation_id


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

async def require_admin(request) -> bool:
    """
    Dependency to check admin authorization.

    In a real implementation, this would check JWT claims or session.
    For now, basic checks for Authorization header.

    Args:
        request: FastAPI Request

    Returns:
        True if authorized, else raises HTTPException
    """
    # This is a placeholder. Customize based on your auth system.
    # In production, verify JWT token has admin scope.
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


# ============================================================================
# Router setup
# ============================================================================

admin_logs_router = APIRouter(prefix='/api/v1/admin', tags=['admin'])

logger = get_structured_logger(__name__)


# ============================================================================
# Endpoints
# ============================================================================

@admin_logs_router.get(
    '/logs',
    summary='Query recent logs',
    description='Get recent log entries with optional filtering',
    response_model=List[LogEntry],
)
async def get_logs(
    level: Optional[str] = Query(None, description="Filter by log level (INFO, WARNING, ERROR, DEBUG)"),
    start_time: Optional[datetime] = Query(None, description="Filter logs after this timestamp"),
    end_time: Optional[datetime] = Query(None, description="Filter logs before this timestamp"),
    correlation_id: Optional[str] = Query(None, description="Filter by request correlation ID"),
    path_contains: Optional[str] = Query(None, description="Filter by path substring"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of entries to return"),
):
    """
    Query recent logs with optional filtering.

    Parameters:
    - level: Filter by log level (INFO, WARNING, ERROR, DEBUG)
    - start_time: Only logs after this datetime (ISO format)
    - end_time: Only logs before this datetime (ISO format)
    - correlation_id: Filter by request correlation ID
    - path_contains: Filter endpoints by path substring
    - limit: Maximum results (1-1000, default 100)

    Returns:
        List of log entries matching criteria
    """
    # Get all entries from buffer
    entries = _log_buffer.get_all()

    # Apply filters in order
    if level:
        entries = _log_buffer.filter_by_level(entries, level.upper())

    if correlation_id:
        entries = _log_buffer.filter_by_correlation_id(entries, correlation_id)

    if path_contains:
        entries = _log_buffer.filter_by_path_contains(entries, path_contains)

    if start_time or end_time:
        entries = _log_buffer.filter_by_time_range(entries, start_time, end_time)

    # Return most recent entries first (reverse order)
    entries = list(reversed(entries))

    # Apply limit
    entries = entries[:limit]

    # Convert to LogEntry models
    return [
        LogEntry(
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
        for e in entries
    ]


@admin_logs_router.get(
    '/logs/{correlation_id}',
    summary='Get all logs for a request',
    description='Retrieve all log entries for a specific request by correlation ID',
    response_model=List[LogEntry],
)
async def get_logs_by_correlation_id(
    correlation_id: str = "Request correlation ID (UUID format)",
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of entries"),
):
    """
    Get all logs related to a specific request.

    Useful for debugging a single request across multiple service calls.

    Parameters:
    - correlation_id: The request's correlation ID (typically returned in X-Correlation-ID header)
    - limit: Maximum results to return

    Returns:
        List of all log entries for this correlation ID
    """
    entries = _log_buffer.get_all()
    entries = _log_buffer.filter_by_correlation_id(entries, correlation_id)

    if not entries:
        raise HTTPException(
            status_code=404,
            detail=f"No logs found for correlation_id: {correlation_id}"
        )

    # Maintain chronological order (oldest first)
    entries = list(entries)[:limit]

    # Convert to LogEntry models
    return [
        LogEntry(
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
        for e in entries
    ]


@admin_logs_router.get(
    '/logs/stats/summary',
    summary='Get log statistics',
    description='Summary statistics of recent logs',
    response_model=Dict[str, Any],
)
async def get_log_stats():
    """
    Get summary statistics of logs in the buffer.

    Returns:
        Dictionary with counts by level, error breakdown, etc.
    """
    entries = _log_buffer.get_all()

    if not entries:
        return {
            'total_entries': 0,
            'levels': {},
            'error_count': 0,
            'warning_count': 0,
        }

    levels = {}
    for entry in entries:
        level = entry.get('level', 'UNKNOWN')
        levels[level] = levels.get(level, 0) + 1

    return {
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
