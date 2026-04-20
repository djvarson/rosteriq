"""Audit Trail / Activity Log for Australian hospitality venues.

Provides an immutable, append-only record of all system changes.
Under the Fair Work Act, employers must keep employee records for 7 years.
This module stores comprehensive audit entries for compliance.

Design:
- APPEND-ONLY: no update or delete methods
- SQLite-persisted via rosteriq/persistence.py
- Thread-safe with threading.Lock
- Indexed by venue_id, timestamp, entity_type, actor_id for fast queries
- Supports pagination, filtering, and aggregation
"""

from __future__ import annotations

import copy
import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.audit_trail")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class AuditAction(str, Enum):
    """System actions that generate audit entries."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    APPROVE = "approve"
    REJECT = "reject"
    SIGN_OFF = "sign_off"
    LOGIN = "login"
    EXPORT = "export"
    SEND = "send"
    ASSIGN = "assign"
    CANCEL = "cancel"


class AuditEntityType(str, Enum):
    """Types of entities that can be audited."""
    ROSTER = "roster"
    SHIFT = "shift"
    EMPLOYEE = "employee"
    LEAVE_REQUEST = "leave_request"
    SHIFT_SWAP = "shift_swap"
    INCIDENT = "incident"
    CERTIFICATION = "certification"
    CLOSE_OF_DAY = "close_of_day"
    VENUE_CONFIG = "venue_config"
    BUDGET = "budget"
    COMMUNICATION = "communication"
    TIMESHEET = "timesheet"


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass
class AuditEntry:
    """A single audit log entry."""

    entry_id: str
    venue_id: str
    timestamp: datetime  # UTC only
    actor_id: str  # who did it
    actor_name: str
    action: AuditAction
    entity_type: AuditEntityType
    entity_id: str  # ID of the affected record
    description: str  # human-readable summary
    changes: Optional[Dict[str, Any]] = None  # {"field": {"old": x, "new": y}} for updates
    ip_address: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None  # extra context

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON responses."""
        return {
            "entry_id": self.entry_id,
            "venue_id": self.venue_id,
            "timestamp": self.timestamp.isoformat(),
            "actor_id": self.actor_id,
            "actor_name": self.actor_name,
            "action": self.action.value,
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "description": self.description,
            "changes": self.changes,
            "ip_address": self.ip_address,
            "metadata": self.metadata,
        }


@dataclass
class AuditQuery:
    """Query parameters for audit log search."""

    venue_id: str
    actor_id: Optional[str] = None
    entity_type: Optional[AuditEntityType] = None
    entity_id: Optional[str] = None
    action: Optional[AuditAction] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    limit: int = 100
    offset: int = 0


@dataclass
class AuditSummary:
    """Aggregate statistics for audit period."""

    venue_id: str
    period_start: datetime
    period_end: datetime
    total_entries: int
    by_action: Dict[str, int]  # action -> count
    by_entity_type: Dict[str, int]  # entity_type -> count
    by_actor: Dict[str, int]  # actor_name -> count
    most_active_actor: Optional[str]
    most_changed_entity_type: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON responses."""
        return {
            "venue_id": self.venue_id,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "total_entries": self.total_entries,
            "by_action": self.by_action,
            "by_entity_type": self.by_entity_type,
            "by_actor": self.by_actor,
            "most_active_actor": self.most_active_actor,
            "most_changed_entity_type": self.most_changed_entity_type,
        }


# ---------------------------------------------------------------------------
# AuditStore (SQLite-backed)
# ---------------------------------------------------------------------------


class AuditStore:
    """APPEND-ONLY audit log backed by SQLite."""

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: List[AuditEntry] = []
        self._load_from_persistence()

    def _load_from_persistence(self):
        """Load entries from SQLite on initialization."""
        try:
            from rosteriq import persistence as _p
        except ImportError:
            return

        if not _p.is_persistence_enabled():
            return

        try:
            conn = _p.connection()
            cursor = conn.execute(
                "SELECT entry_id, venue_id, timestamp, actor_id, actor_name, "
                "action, entity_type, entity_id, description, changes, ip_address, metadata "
                "FROM audit_entries ORDER BY timestamp ASC"
            )
            for row in cursor:
                entry = AuditEntry(
                    entry_id=row[0],
                    venue_id=row[1],
                    timestamp=datetime.fromisoformat(row[2]).replace(tzinfo=timezone.utc),
                    actor_id=row[3],
                    actor_name=row[4],
                    action=AuditAction(row[5]),
                    entity_type=AuditEntityType(row[6]),
                    entity_id=row[7],
                    description=row[8],
                    changes=json.loads(row[9]) if row[9] else None,
                    ip_address=row[10],
                    metadata=json.loads(row[11]) if row[11] else None,
                )
                self._entries.append(entry)
            logger.info(f"Loaded {len(self._entries)} audit entries from persistence")
        except Exception as e:
            logger.warning(f"Failed to load audit entries from persistence: {e}")

    def _persist_entry(self, entry: AuditEntry):
        """Write entry to SQLite."""
        try:
            from rosteriq import persistence as _p
        except ImportError:
            return

        if not _p.is_persistence_enabled():
            return

        try:
            with _p.write_txn() as conn:
                conn.execute(
                    """
                    INSERT INTO audit_entries
                    (entry_id, venue_id, timestamp, actor_id, actor_name, action, entity_type,
                     entity_id, description, changes, ip_address, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.entry_id,
                        entry.venue_id,
                        entry.timestamp.isoformat(),
                        entry.actor_id,
                        entry.actor_name,
                        entry.action.value,
                        entry.entity_type.value,
                        entry.entity_id,
                        entry.description,
                        json.dumps(entry.changes) if entry.changes else None,
                        entry.ip_address,
                        json.dumps(entry.metadata) if entry.metadata else None,
                    ),
                )
        except Exception as e:
            logger.warning(f"Failed to persist audit entry: {e}")

    def append(self, entry: AuditEntry) -> AuditEntry:
        """
        Append an entry to the audit log (APPEND-ONLY).

        Args:
            entry: AuditEntry to append

        Returns:
            A shallow copy of the appended entry
        """
        with self._lock:
            self._entries.append(entry)
            self._persist_entry(entry)
        # Return a copy to prevent callers from mutating the stored entry
        return copy.copy(entry)

    def query(self, q: AuditQuery) -> List[AuditEntry]:
        """
        Query audit entries with filters and pagination.

        Args:
            q: AuditQuery with filter criteria

        Returns:
            List of AuditEntry matching filters
        """
        with self._lock:
            results = self._entries
            # Filter by venue_id
            results = [e for e in results if e.venue_id == q.venue_id]
            # Filter by actor_id if provided
            if q.actor_id:
                results = [e for e in results if e.actor_id == q.actor_id]
            # Filter by entity_type if provided
            if q.entity_type:
                results = [e for e in results if e.entity_type == q.entity_type]
            # Filter by entity_id if provided
            if q.entity_id:
                results = [e for e in results if e.entity_id == q.entity_id]
            # Filter by action if provided
            if q.action:
                results = [e for e in results if e.action == q.action]
            # Filter by date range if provided
            if q.date_from:
                results = [e for e in results if e.timestamp >= q.date_from]
            if q.date_to:
                results = [e for e in results if e.timestamp <= q.date_to]
            # Apply limit and offset
            total = len(results)
            results = results[q.offset : q.offset + q.limit]
            return results

    def get_entity_history(self, entity_type: AuditEntityType, entity_id: str) -> List[AuditEntry]:
        """
        Get all audit entries for a specific entity.

        Args:
            entity_type: Type of entity
            entity_id: ID of the entity

        Returns:
            List of AuditEntry for this entity, chronologically ordered
        """
        with self._lock:
            return [
                e for e in self._entries
                if e.entity_type == entity_type and e.entity_id == entity_id
            ]

    def get_actor_activity(
        self,
        actor_id: str,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
    ) -> List[AuditEntry]:
        """
        Get all actions by a specific user.

        Args:
            actor_id: ID of the actor/user
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of AuditEntry for this actor
        """
        with self._lock:
            results = [e for e in self._entries if e.actor_id == actor_id]
            if date_from:
                results = [e for e in results if e.timestamp >= date_from]
            if date_to:
                results = [e for e in results if e.timestamp <= date_to]
            return results


# ---------------------------------------------------------------------------
# Singleton Store & Main Entry Points
# ---------------------------------------------------------------------------

_store = AuditStore()
_store_lock = threading.Lock()


def get_audit_store() -> AuditStore:
    """Get the singleton AuditStore."""
    global _store
    with _store_lock:
        if _store is None:
            _store = AuditStore()
    return _store


def _reset_for_tests():
    """Test helper: clear the in-memory store (skips persistence reload)."""
    global _store
    with _store_lock:
        _store = AuditStore.__new__(AuditStore)
        _store._lock = threading.Lock()
        _store._entries = []


def log_event(
    venue_id: str,
    actor_id: str,
    actor_name: str,
    action: AuditAction,
    entity_type: AuditEntityType,
    entity_id: str,
    description: str,
    changes: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> AuditEntry:
    """
    Log an audit event (main entry point for other modules).

    Args:
        venue_id: Venue identifier
        actor_id: User/system ID who performed the action
        actor_name: Human-readable name of actor
        action: Type of action (CREATE, UPDATE, DELETE, etc.)
        entity_type: Type of entity being modified
        entity_id: ID of the affected entity
        description: Human-readable summary
        changes: Optional dict of field changes {"field": {"old": x, "new": y}}
        ip_address: Optional IP address of the actor
        metadata: Optional extra context

    Returns:
        The AuditEntry that was logged
    """
    entry = AuditEntry(
        entry_id=str(uuid.uuid4()),
        venue_id=venue_id,
        timestamp=datetime.now(timezone.utc),
        actor_id=actor_id,
        actor_name=actor_name,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        description=description,
        changes=changes,
        ip_address=ip_address,
        metadata=metadata,
    )
    store = get_audit_store()
    return store.append(entry)


def query_audit(query: AuditQuery) -> List[AuditEntry]:
    """
    Query the audit log with filters and pagination.

    Args:
        query: AuditQuery with filter criteria

    Returns:
        List of matching AuditEntry
    """
    store = get_audit_store()
    return store.query(query)


def get_entity_history(entity_type: AuditEntityType, entity_id: str) -> List[AuditEntry]:
    """
    Get all audit entries for a specific entity.

    Args:
        entity_type: Type of entity
        entity_id: ID of the entity

    Returns:
        List of AuditEntry for this entity
    """
    store = get_audit_store()
    return store.get_entity_history(entity_type, entity_id)


def get_actor_activity(
    actor_id: str,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> List[AuditEntry]:
    """
    Get all actions by a specific user.

    Args:
        actor_id: ID of the actor/user
        date_from: Optional start date filter
        date_to: Optional end date filter

    Returns:
        List of AuditEntry for this actor
    """
    store = get_audit_store()
    return store.get_actor_activity(actor_id, date_from, date_to)


def build_audit_summary(
    venue_id: str,
    date_from: datetime,
    date_to: datetime,
) -> AuditSummary:
    """
    Build aggregate statistics for a venue's audit period.

    Args:
        venue_id: Venue identifier
        date_from: Start of period (UTC)
        date_to: End of period (UTC)

    Returns:
        AuditSummary with aggregated stats
    """
    query = AuditQuery(
        venue_id=venue_id,
        date_from=date_from,
        date_to=date_to,
        limit=10000,  # fetch all in period
    )
    entries = query_audit(query)

    by_action: Dict[str, int] = {}
    by_entity_type: Dict[str, int] = {}
    by_actor: Dict[str, int] = {}

    for entry in entries:
        action_key = entry.action.value
        by_action[action_key] = by_action.get(action_key, 0) + 1

        entity_key = entry.entity_type.value
        by_entity_type[entity_key] = by_entity_type.get(entity_key, 0) + 1

        actor_key = entry.actor_name
        by_actor[actor_key] = by_actor.get(actor_key, 0) + 1

    # Find most active actor and most changed entity type
    most_active_actor = max(by_actor, key=by_actor.get) if by_actor else None
    most_changed_entity_type = (
        max(by_entity_type, key=by_entity_type.get) if by_entity_type else None
    )

    return AuditSummary(
        venue_id=venue_id,
        period_start=date_from,
        period_end=date_to,
        total_entries=len(entries),
        by_action=by_action,
        by_entity_type=by_entity_type,
        by_actor=by_actor,
        most_active_actor=most_active_actor,
        most_changed_entity_type=most_changed_entity_type,
    )


def diff_changes(old_dict: Dict[str, Any], new_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute the changes dict by comparing old and new values.

    Args:
        old_dict: Previous state
        new_dict: New state

    Returns:
        Dict mapping field names to {"old": x, "new": y} for changed fields
    """
    changes: Dict[str, Any] = {}
    all_keys = set(old_dict.keys()) | set(new_dict.keys())

    for key in all_keys:
        old_val = old_dict.get(key)
        new_val = new_dict.get(key)
        if old_val != new_val:
            changes[key] = {"old": old_val, "new": new_val}

    return changes


def format_audit_entry(entry: AuditEntry) -> str:
    """
    Format an audit entry as a human-readable one-line string.

    Args:
        entry: AuditEntry to format

    Returns:
        Human-readable summary (e.g., "Dale approved leave request LR-123 for Alice on 2026-04-20")
    """
    date_str = entry.timestamp.strftime("%Y-%m-%d")
    action_str = entry.action.value.replace("_", " ").title()

    return (
        f"{entry.actor_name} {action_str.lower()} {entry.entity_type.value} "
        f"{entry.entity_id} on {date_str}: {entry.description}"
    )


# ---------------------------------------------------------------------------
# SQLite Schema Registration
# ---------------------------------------------------------------------------

def _register_schema():
    """Register audit_entries table schema with persistence layer."""
    try:
        from rosteriq import persistence as _p
    except ImportError:
        return

    schema = """
    CREATE TABLE IF NOT EXISTS audit_entries (
        entry_id TEXT PRIMARY KEY,
        venue_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        actor_id TEXT NOT NULL,
        actor_name TEXT NOT NULL,
        action TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        description TEXT NOT NULL,
        changes TEXT,
        ip_address TEXT,
        metadata TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_audit_venue_ts ON audit_entries(venue_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_entries(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_entries(actor_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_entries(action);
    """

    _p.register_schema("audit_entries", schema)


# Register schema on module load
_register_schema()
