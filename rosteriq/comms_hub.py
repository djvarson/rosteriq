"""Staff Communication Hub — send targeted messages to hospitality staff.

Core module for managing staff communications:
- Templates: roster published, shift changes, shift offers, announcements, reminders, leave approvals
- Channels: SMS, EMAIL, PUSH, IN_APP
- Priorities: LOW, NORMAL, HIGH, URGENT
- Notification preferences: per-employee control over channels, message types, quiet hours
- Message tracking: delivery status, timestamps, error logs
- Bulk send: respects preferences and quiet hours

SQLite-persisted message log + preference store. Thread-safe with lock-protected store.
Pure stdlib + optional lazy httpx for future real providers.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("rosteriq.comms_hub")

# Wire in persistence layer
try:
    from rosteriq import persistence as _p
except ImportError:
    _p = None


# ============================================================================
# Enums
# ============================================================================

class Channel(str, Enum):
    """Communication channel."""
    SMS = "sms"
    EMAIL = "email"
    PUSH = "push"
    IN_APP = "in_app"


class MessagePriority(str, Enum):
    """Message priority level."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class MessageStatus(str, Enum):
    """Message delivery status."""
    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    READ = "read"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class MessageTemplate:
    """A communication template with variable substitution."""
    template_id: str
    name: str
    channel: Channel
    subject_template: str
    body_template: str
    variables: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "channel": self.channel.value if isinstance(self.channel, Channel) else self.channel,
            "subject_template": self.subject_template,
            "body_template": self.body_template,
            "variables": self.variables,
        }


@dataclass
class StaffMessage:
    """A message to be sent to a staff member."""
    message_id: str
    venue_id: str
    recipient_id: str
    recipient_name: str
    recipient_contact: str  # phone or email
    channel: Channel
    priority: MessagePriority
    subject: str
    body: str
    status: MessageStatus
    template_id: Optional[str] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    read_at: Optional[datetime] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "venue_id": self.venue_id,
            "recipient_id": self.recipient_id,
            "recipient_name": self.recipient_name,
            "recipient_contact": self.recipient_contact,
            "channel": self.channel.value if isinstance(self.channel, Channel) else self.channel,
            "priority": self.priority.value if isinstance(self.priority, MessagePriority) else self.priority,
            "subject": self.subject,
            "body": self.body,
            "status": self.status.value if isinstance(self.status, MessageStatus) else self.status,
            "template_id": self.template_id,
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None,
            "error": self.error,
        }


@dataclass
class NotificationPreference:
    """Per-employee notification preferences."""
    employee_id: str
    venue_id: str
    preferred_channel: Channel
    roster_changes: bool = True
    shift_offers: bool = True
    announcements: bool = True
    reminders: bool = True
    quiet_hours_start: int = 22  # 10 PM (0-23)
    quiet_hours_end: int = 8  # 8 AM (0-23)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "employee_id": self.employee_id,
            "venue_id": self.venue_id,
            "preferred_channel": self.preferred_channel.value if isinstance(self.preferred_channel, Channel) else self.preferred_channel,
            "roster_changes": self.roster_changes,
            "shift_offers": self.shift_offers,
            "announcements": self.announcements,
            "reminders": self.reminders,
            "quiet_hours_start": self.quiet_hours_start,
            "quiet_hours_end": self.quiet_hours_end,
        }


@dataclass
class BulkSendResult:
    """Result of a bulk send operation."""
    total: int
    sent: int
    failed: int
    messages: List[StaffMessage] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "sent": self.sent,
            "failed": self.failed,
            "messages": [m.to_dict() for m in self.messages],
        }


# ============================================================================
# Built-in Templates
# ============================================================================

BUILTIN_TEMPLATES = {
    "roster_published": MessageTemplate(
        template_id="roster_published",
        name="Roster Published",
        channel=Channel.SMS,
        subject_template="Roster for {week_start}",
        body_template="Your roster for {week_start} is ready. {shift_count} shifts, {total_hours}h.",
        variables=["week_start", "shift_count", "total_hours"],
    ),
    "shift_change": MessageTemplate(
        template_id="shift_change",
        name="Shift Change",
        channel=Channel.SMS,
        subject_template="Shift change: {date}",
        body_template="Shift change: {date} {old_time} → {new_time}. Please confirm.",
        variables=["date", "old_time", "new_time"],
    ),
    "shift_offer": MessageTemplate(
        template_id="shift_offer",
        name="Shift Offer",
        channel=Channel.SMS,
        subject_template="Open shift: {date}",
        body_template="Open shift available: {date} {start}-{end} ({role}). Reply YES to claim.",
        variables=["date", "start", "end", "role"],
    ),
    "announcement": MessageTemplate(
        template_id="announcement",
        name="Announcement",
        channel=Channel.EMAIL,
        subject_template="Announcement from {venue_name}",
        body_template="{venue_name}: {message}",
        variables=["venue_name", "message"],
    ),
    "reminder": MessageTemplate(
        template_id="reminder",
        name="Shift Reminder",
        channel=Channel.SMS,
        subject_template="Shift reminder",
        body_template="Reminder: You're rostered {date} {start_time}-{end_time} ({role}).",
        variables=["date", "start_time", "end_time", "role"],
    ),
    "leave_approved": MessageTemplate(
        template_id="leave_approved",
        name="Leave Approved",
        channel=Channel.EMAIL,
        subject_template="Your leave has been approved",
        body_template="Your {leave_type} leave ({start_date} to {end_date}) has been approved.",
        variables=["leave_type", "start_date", "end_date"],
    ),
}


# ============================================================================
# Template Functions
# ============================================================================

def render_template(template: MessageTemplate, variables: Dict[str, str]) -> Tuple[str, str]:
    """Render a template with variable substitution.

    Args:
        template: MessageTemplate object
        variables: Dict of {placeholder: value}

    Returns:
        Tuple of (subject, body) with placeholders replaced

    Raises:
        KeyError if a required variable is missing
    """
    subject = template.subject_template.format(**variables)
    body = template.body_template.format(**variables)
    return subject, body


# ============================================================================
# CommsStore
# ============================================================================

class CommsStore:
    """Thread-safe SQLite-backed store for messages and preferences."""

    def __init__(self):
        self._lock = threading.Lock()
        self._messages: List[StaffMessage] = []
        self._preferences: Dict[str, NotificationPreference] = {}
        self._templates: Dict[str, MessageTemplate] = dict(BUILTIN_TEMPLATES)

        # Register schema and init DB if persistence enabled
        if _p is not None:
            _p.register_schema(
                "comms_hub_messages",
                """
                CREATE TABLE IF NOT EXISTS comms_hub_messages (
                    message_id TEXT PRIMARY KEY,
                    venue_id TEXT NOT NULL,
                    recipient_id TEXT NOT NULL,
                    recipient_name TEXT NOT NULL,
                    recipient_contact TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    status TEXT NOT NULL,
                    template_id TEXT,
                    sent_at TEXT,
                    delivered_at TEXT,
                    read_at TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL
                )
                """,
            )
            _p.register_schema(
                "comms_hub_preferences",
                """
                CREATE TABLE IF NOT EXISTS comms_hub_preferences (
                    employee_id TEXT NOT NULL,
                    venue_id TEXT NOT NULL,
                    preferred_channel TEXT NOT NULL,
                    roster_changes INTEGER NOT NULL,
                    shift_offers INTEGER NOT NULL,
                    announcements INTEGER NOT NULL,
                    reminders INTEGER NOT NULL,
                    quiet_hours_start INTEGER NOT NULL,
                    quiet_hours_end INTEGER NOT NULL,
                    PRIMARY KEY (employee_id, venue_id)
                )
                """,
            )
            # Rehydrate from DB if available
            self._rehydrate()

    def _rehydrate(self) -> None:
        """Load messages and preferences from DB on startup."""
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            conn = _p.connection()
            # Load messages
            rows = conn.execute(
                "SELECT * FROM comms_hub_messages ORDER BY created_at DESC LIMIT 10000"
            ).fetchall()
            for row in rows:
                msg = StaffMessage(
                    message_id=row["message_id"],
                    venue_id=row["venue_id"],
                    recipient_id=row["recipient_id"],
                    recipient_name=row["recipient_name"],
                    recipient_contact=row["recipient_contact"],
                    channel=Channel(row["channel"]),
                    priority=MessagePriority(row["priority"]),
                    subject=row["subject"],
                    body=row["body"],
                    status=MessageStatus(row["status"]),
                    template_id=row["template_id"],
                    sent_at=datetime.fromisoformat(row["sent_at"]) if row["sent_at"] else None,
                    delivered_at=datetime.fromisoformat(row["delivered_at"]) if row["delivered_at"] else None,
                    read_at=datetime.fromisoformat(row["read_at"]) if row["read_at"] else None,
                    error=row["error"],
                )
                self._messages.append(msg)

            # Load preferences
            rows = conn.execute("SELECT * FROM comms_hub_preferences").fetchall()
            for row in rows:
                pref = NotificationPreference(
                    employee_id=row["employee_id"],
                    venue_id=row["venue_id"],
                    preferred_channel=Channel(row["preferred_channel"]),
                    roster_changes=bool(row["roster_changes"]),
                    shift_offers=bool(row["shift_offers"]),
                    announcements=bool(row["announcements"]),
                    reminders=bool(row["reminders"]),
                    quiet_hours_start=row["quiet_hours_start"],
                    quiet_hours_end=row["quiet_hours_end"],
                )
                key = f"{pref.employee_id}:{pref.venue_id}"
                self._preferences[key] = pref
        except Exception as e:
            logger.warning(f"Failed to rehydrate comms store: {e}")

    def add_message(self, message: StaffMessage) -> StaffMessage:
        """Add a message to the store and persist it."""
        with self._lock:
            self._messages.append(message)

        if _p and _p.is_persistence_enabled():
            try:
                conn = _p.connection()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO comms_hub_messages
                    (message_id, venue_id, recipient_id, recipient_name, recipient_contact,
                     channel, priority, subject, body, status, template_id,
                     sent_at, delivered_at, read_at, error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message.message_id,
                        message.venue_id,
                        message.recipient_id,
                        message.recipient_name,
                        message.recipient_contact,
                        message.channel.value,
                        message.priority.value,
                        message.subject,
                        message.body,
                        message.status.value,
                        message.template_id,
                        message.sent_at.isoformat() if message.sent_at else None,
                        message.delivered_at.isoformat() if message.delivered_at else None,
                        message.read_at.isoformat() if message.read_at else None,
                        message.error,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            except Exception as e:
                logger.error(f"Failed to persist message {message.message_id}: {e}")

        return message

    def get_messages(
        self,
        venue_id: str,
        employee_id: Optional[str] = None,
        status: Optional[MessageStatus] = None,
        channel: Optional[Channel] = None,
        limit: int = 100,
    ) -> List[StaffMessage]:
        """Get messages for a venue with optional filtering."""
        with self._lock:
            results = self._messages

        # Filter by venue
        results = [m for m in results if m.venue_id == venue_id]

        # Optional filters
        if employee_id:
            results = [m for m in results if m.recipient_id == employee_id]
        if status:
            results = [m for m in results if m.status == status]
        if channel:
            results = [m for m in results if m.channel == channel]

        # Return most recent, up to limit
        return sorted(results, key=lambda m: m.sent_at or datetime.min, reverse=True)[:limit]

    def get_unread_count(self, employee_id: str) -> int:
        """Count unread messages for an employee."""
        with self._lock:
            return sum(
                1 for m in self._messages
                if m.recipient_id == employee_id and m.status == MessageStatus.QUEUED
            )

    def save_preference(self, preference: NotificationPreference) -> NotificationPreference:
        """Save or update a notification preference."""
        key = f"{preference.employee_id}:{preference.venue_id}"
        with self._lock:
            self._preferences[key] = preference

        if _p and _p.is_persistence_enabled():
            try:
                conn = _p.connection()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO comms_hub_preferences
                    (employee_id, venue_id, preferred_channel, roster_changes, shift_offers,
                     announcements, reminders, quiet_hours_start, quiet_hours_end)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        preference.employee_id,
                        preference.venue_id,
                        preference.preferred_channel.value,
                        int(preference.roster_changes),
                        int(preference.shift_offers),
                        int(preference.announcements),
                        int(preference.reminders),
                        preference.quiet_hours_start,
                        preference.quiet_hours_end,
                    ),
                )
            except Exception as e:
                logger.error(f"Failed to persist preference for {key}: {e}")

        return preference

    def get_preference(
        self,
        employee_id: str,
        venue_id: str,
    ) -> NotificationPreference:
        """Get a preference, with defaults if not found."""
        key = f"{employee_id}:{venue_id}"
        with self._lock:
            if key in self._preferences:
                return self._preferences[key]

        # Return defaults
        return NotificationPreference(
            employee_id=employee_id,
            venue_id=venue_id,
            preferred_channel=Channel.SMS,
        )

    def get_template(self, template_id: str) -> Optional[MessageTemplate]:
        """Get a template by ID."""
        with self._lock:
            return self._templates.get(template_id)

    def list_templates(self) -> List[MessageTemplate]:
        """List all available templates."""
        with self._lock:
            return list(self._templates.values())


# ============================================================================
# Module Singleton
# ============================================================================

_store: Optional[CommsStore] = None
_store_lock = threading.Lock()


def get_comms_store() -> CommsStore:
    """Get the module-level singleton CommsStore."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = CommsStore()
    return _store


def _reset_for_tests() -> None:
    """Reset singleton for testing."""
    global _store
    with _store_lock:
        _store = None


# ============================================================================
# Core Functions
# ============================================================================

def check_quiet_hours(preference: NotificationPreference, current_hour: int) -> bool:
    """Check if current hour is within quiet hours.

    Args:
        preference: NotificationPreference with quiet hour range
        current_hour: Hour of day (0-23)

    Returns:
        True if currently in quiet hours, False otherwise
    """
    start = preference.quiet_hours_start
    end = preference.quiet_hours_end

    # Handle wrap-around (e.g., 22 to 8 next day)
    if start < end:
        return start <= current_hour < end
    else:
        return current_hour >= start or current_hour < end


def send_message(message: StaffMessage) -> StaffMessage:
    """Send a single message (sandbox: update status to SENT).

    Args:
        message: StaffMessage to send

    Returns:
        Updated message with status and timestamps
    """
    # In sandbox, just mark as sent
    message.status = MessageStatus.SENT
    message.sent_at = datetime.now(timezone.utc)

    # Persist
    store = get_comms_store()
    return store.add_message(message)


def send_bulk(
    venue_id: str,
    recipient_ids: List[str],
    template_id: str,
    variables: Dict[str, str],
    channel: Optional[Channel] = None,
    priority: MessagePriority = MessagePriority.NORMAL,
) -> BulkSendResult:
    """Send a message to multiple staff members.

    Respects notification preferences and quiet hours.

    Args:
        venue_id: Venue ID
        recipient_ids: List of employee IDs to send to
        template_id: Template ID from BUILTIN_TEMPLATES
        variables: Dict of template variables
        channel: Override preferred channel (optional)
        priority: Message priority

    Returns:
        BulkSendResult with counts and messages
    """
    store = get_comms_store()
    template = store.get_template(template_id)

    if not template:
        return BulkSendResult(
            total=len(recipient_ids),
            sent=0,
            failed=len(recipient_ids),
            messages=[],
        )

    if not recipient_ids:
        return BulkSendResult(total=0, sent=0, failed=0, messages=[])

    # Render template
    try:
        subject, body = render_template(template, variables)
    except KeyError as e:
        return BulkSendResult(
            total=len(recipient_ids),
            sent=0,
            failed=len(recipient_ids),
            messages=[],
        )

    result = BulkSendResult(total=len(recipient_ids), sent=0, failed=0, messages=[])
    current_hour = datetime.now(timezone.utc).hour

    for recipient_id in recipient_ids:
        preference = store.get_preference(recipient_id, venue_id)

        # Check if user has opted out of this message type
        # (For now, we don't have a message_type parameter, so skip this check)

        # Check quiet hours
        if check_quiet_hours(preference, current_hour):
            result.failed += 1
            continue

        # Determine channel
        send_channel = channel or preference.preferred_channel

        # Create and send message
        message = StaffMessage(
            message_id=f"msg_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            recipient_id=recipient_id,
            recipient_name="",  # Would come from HR system
            recipient_contact="",  # Would come from HR system
            channel=send_channel,
            priority=priority,
            subject=subject,
            body=body,
            status=MessageStatus.QUEUED,
            template_id=template_id,
        )

        sent_msg = send_message(message)
        result.messages.append(sent_msg)
        if sent_msg.status == MessageStatus.SENT:
            result.sent += 1
        else:
            result.failed += 1

    return result


def get_delivery_stats(
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    """Get delivery statistics for a venue.

    Args:
        venue_id: Venue ID
        date_from: ISO date string (optional)
        date_to: ISO date string (optional)

    Returns:
        Dict with sent, delivered, failed, read counts
    """
    store = get_comms_store()
    messages = store.get_messages(venue_id, limit=10000)

    # Filter by date if provided
    if date_from:
        try:
            from_dt = datetime.fromisoformat(date_from)
            messages = [m for m in messages if m.sent_at and m.sent_at >= from_dt]
        except (ValueError, AttributeError):
            pass

    if date_to:
        try:
            to_dt = datetime.fromisoformat(date_to)
            messages = [m for m in messages if m.sent_at and m.sent_at <= to_dt]
        except (ValueError, AttributeError):
            pass

    sent = sum(1 for m in messages if m.status in (MessageStatus.SENT, MessageStatus.DELIVERED, MessageStatus.READ))
    delivered = sum(1 for m in messages if m.status in (MessageStatus.DELIVERED, MessageStatus.READ))
    failed = sum(1 for m in messages if m.status == MessageStatus.FAILED)
    read = sum(1 for m in messages if m.status == MessageStatus.READ)

    return {
        "total": len(messages),
        "sent": sent,
        "delivered": delivered,
        "failed": failed,
        "read": read,
    }
