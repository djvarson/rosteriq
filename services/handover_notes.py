"""
RosterIQ Digital Shift Handover Notes System

Enables outgoing staff to document shift status, handover tasks, and critical
information for incoming staff. Tracks acknowledgment and sends notifications
30 minutes before incoming shifts.

Sections:
- PrepStatus: Menu items, prep items completed/pending
- VIPInfo: Special guest tables, dietary needs, seating preferences
- MaintenanceIssues: Equipment breakdowns, reported problems
- StockAlerts: Low stock items, supply issues
- GeneralNotes: Free-form notes, general information

Usage:
    from rosteriq.services.handover_notes import HandoverService
    service = HandoverService()
    note = service.create_note(shift_id, author_id, sections, priority)
    service.acknowledge_note(note.id, incoming_employee_id)
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict, field
from enum import Enum

from rosteriq.database import get_db
from rosteriq.services.notification_hub import get_notification_hub

logger = logging.getLogger(__name__)


class HandoverPriority(str, Enum):
    """Priority levels for handover notes."""
    normal = "normal"
    important = "important"
    urgent = "urgent"


@dataclass
class PrepItem:
    """Individual prep item in handover."""
    name: str
    status: str  # "done" | "in_progress" | "pending"
    notes: str = ""
    due_time: Optional[str] = None


@dataclass
class PrepStatus:
    """Prep status section of handover note."""
    items: List[PrepItem] = field(default_factory=list)


@dataclass
class VIPTable:
    """VIP table or guest information."""
    table_number: str
    guest_name: str
    party_size: int
    arrival_time: Optional[str] = None
    dietary_restrictions: str = ""
    special_requests: str = ""
    seating_preference: str = ""


@dataclass
class VIPInfo:
    """VIP/special guest information section."""
    tables: List[VIPTable] = field(default_factory=list)


@dataclass
class Issue:
    """Maintenance or operational issue."""
    category: str  # "equipment" | "plumbing" | "electrical" | "other"
    description: str
    severity: str  # "low" | "medium" | "high"
    location: str
    reported_time: Optional[str] = None
    action_required: str = ""


@dataclass
class MaintenanceIssues:
    """Maintenance and operational issues section."""
    issues: List[Issue] = field(default_factory=list)


@dataclass
class StockItem:
    """Stock or supply alert."""
    item_name: str
    current_level: str  # "low" | "critical" | "sufficient"
    quantity_remaining: Optional[str] = None
    reorder_needed: bool = False
    notes: str = ""


@dataclass
class StockAlerts:
    """Stock and supply alerts section."""
    items: List[StockItem] = field(default_factory=list)


@dataclass
class GeneralNotes:
    """Free-form general notes section."""
    text: str = ""


@dataclass
class HandoverNote:
    """Complete shift handover note."""
    id: str
    shift_id: str
    venue_id: str
    author_id: str
    author_name: str
    created_at: str
    sections: Dict[str, Any]  # Contains PrepStatus, VIPInfo, etc.
    priority: str  # "normal" | "important" | "urgent"
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None


class HandoverService:
    """Service for managing shift handover notes."""

    def __init__(self, db=None, notification_hub=None):
        """Initialize handover service.

        Args:
            db: Database instance (uses get_db() if not provided)
            notification_hub: Notification hub (uses get_notification_hub() if not provided)
        """
        self.db = db or get_db()
        self.hub = notification_hub or get_notification_hub()

    def create_note(
        self,
        shift_id: str,
        author_id: str,
        author_name: str,
        venue_id: str,
        sections: Dict[str, Any],
        priority: str = "normal",
    ) -> HandoverNote:
        """Create a new handover note for a shift.

        Args:
            shift_id: ID of the shift being handed over
            author_id: ID of the employee writing the note (outgoing staff)
            author_name: Name of the employee writing the note
            venue_id: ID of the venue
            sections: Dictionary containing PrepStatus, VIPInfo, MaintenanceIssues, etc.
            priority: Priority level ("normal", "important", "urgent")

        Returns:
            HandoverNote: Created handover note

        Raises:
            ValueError: If shift_id not found or invalid priority
        """
        if priority not in ["normal", "important", "urgent"]:
            raise ValueError(f"Invalid priority: {priority}")

        # Verify shift exists
        shift = self.db.get_shift(shift_id)
        if not shift:
            raise ValueError(f"Shift {shift_id} not found")

        # Generate unique ID
        note_id = f"hn_{shift_id}_{int(datetime.utcnow().timestamp())}"
        created_at = datetime.utcnow().isoformat() + "Z"

        note = HandoverNote(
            id=note_id,
            shift_id=shift_id,
            venue_id=venue_id,
            author_id=author_id,
            author_name=author_name,
            created_at=created_at,
            sections=sections,
            priority=priority,
            acknowledged_by=None,
            acknowledged_at=None,
        )

        # Store in database
        self._save_note(note)
        logger.info(f"Created handover note {note_id} for shift {shift_id}")

        return note

    def get_note(self, note_id: str) -> Optional[HandoverNote]:
        """Retrieve a handover note by ID.

        Args:
            note_id: ID of the handover note

        Returns:
            HandoverNote or None if not found
        """
        return self._load_note(note_id)

    def get_note_by_shift(self, shift_id: str) -> Optional[HandoverNote]:
        """Retrieve the handover note for a specific shift.

        Args:
            shift_id: ID of the shift

        Returns:
            HandoverNote or None if no note exists for this shift
        """
        # Load note from database by shift_id
        note_data = self.db.load_from_key(f"handover_shift_{shift_id}")
        if not note_data:
            return None
        return self._deserialize_note(note_data)

    def get_incoming_handovers(
        self, employee_id: str, venue_id: str, limit: int = 10
    ) -> List[HandoverNote]:
        """Get handover notes for my upcoming shifts.

        Fetches unacknowledged handover notes for shifts where employee_id is scheduled
        in the next 7 days.

        Args:
            employee_id: ID of the incoming employee
            venue_id: ID of the venue
            limit: Maximum number of handovers to return

        Returns:
            List of HandoverNote objects for upcoming shifts
        """
        incoming_shifts = self.db.get_employee_shifts(
            employee_id=employee_id,
            venue_id=venue_id,
            start_date=datetime.utcnow().date(),
            end_date=(datetime.utcnow() + timedelta(days=7)).date(),
            status_filter=["scheduled", "confirmed"],
        )

        handovers = []
        for shift in incoming_shifts[:limit]:
            note = self.get_note_by_shift(shift.id)
            if note and not note.acknowledged_by:
                handovers.append(note)

        # Sort by shift time (most recent first)
        return sorted(handovers, key=lambda n: n.created_at, reverse=True)

    def acknowledge_note(self, note_id: str, employee_id: str) -> HandoverNote:
        """Mark a handover note as acknowledged by incoming staff.

        Args:
            note_id: ID of the handover note
            employee_id: ID of the employee acknowledging receipt

        Returns:
            Updated HandoverNote with acknowledgment info

        Raises:
            ValueError: If note not found
        """
        note = self.get_note(note_id)
        if not note:
            raise ValueError(f"Handover note {note_id} not found")

        # Update acknowledgment
        note.acknowledged_by = employee_id
        note.acknowledged_at = datetime.utcnow().isoformat() + "Z"

        # Save updated note
        self._save_note(note)
        logger.info(f"Acknowledged handover note {note_id} by employee {employee_id}")

        return note

    def get_venue_handovers(self, venue_id: str, date: str) -> List[HandoverNote]:
        """Get all handover notes for a venue on a specific date.

        Args:
            venue_id: ID of the venue
            date: ISO date string (YYYY-MM-DD)

        Returns:
            List of HandoverNote objects for that date
        """
        # Get all shifts for that venue on that date
        shifts = self.db.get_venue_shifts_by_date(venue_id, date)

        handovers = []
        for shift in shifts:
            note = self.get_note_by_shift(shift.id)
            if note:
                handovers.append(note)

        return handovers

    def get_unacknowledged(self, venue_id: str) -> List[HandoverNote]:
        """Get all unacknowledged handover notes for a venue.

        Outstanding handovers that haven't been read by incoming staff.

        Args:
            venue_id: ID of the venue

        Returns:
            List of unacknowledged HandoverNote objects
        """
        # Get recent shifts (past 3 days, next 7 days)
        recent_shifts = self.db.get_venue_shifts_by_date_range(
            venue_id,
            start_date=(datetime.utcnow() - timedelta(days=3)).date(),
            end_date=(datetime.utcnow() + timedelta(days=7)).date(),
        )

        unacknowledged = []
        for shift in recent_shifts:
            note = self.get_note_by_shift(shift.id)
            if note and not note.acknowledged_by:
                unacknowledged.append(note)

        return unacknowledged

    async def notify_incoming(self, note_id: str) -> bool:
        """Send push notification to incoming staff 30 min before their shift.

        This is typically called by a scheduled task 30 minutes before shift start.

        Args:
            note_id: ID of the handover note

        Returns:
            True if notification sent successfully
        """
        note = self.get_note(note_id)
        if not note:
            logger.warning(f"Handover note {note_id} not found for notification")
            return False

        # Get shift details to find incoming employee
        shift = self.db.get_shift(note.shift_id)
        if not shift:
            logger.warning(f"Shift {note.shift_id} not found")
            return False

        # Build notification message
        priority_label = note.priority.upper()
        message = (
            f"Shift handover from {note.author_name} "
            f"[{priority_label}] - Ready to view"
        )

        try:
            # Send notification via hub
            await self.hub.dispatch(
                "HANDOVER_READY",
                note.venue_id,
                {
                    "note_id": note_id,
                    "shift_id": note.shift_id,
                    "author": note.author_name,
                    "priority": note.priority,
                    "message": message,
                },
            )
            logger.info(f"Sent handover notification for note {note_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send handover notification: {e}")
            return False

    def _save_note(self, note: HandoverNote) -> None:
        """Save handover note to database.

        Args:
            note: HandoverNote to save
        """
        note_data = {
            "id": note.id,
            "shift_id": note.shift_id,
            "venue_id": note.venue_id,
            "author_id": note.author_id,
            "author_name": note.author_name,
            "created_at": note.created_at,
            "sections": note.sections,
            "priority": note.priority,
            "acknowledged_by": note.acknowledged_by,
            "acknowledged_at": note.acknowledged_at,
        }

        # Save by note ID
        self.db.save_to_key(f"handover_{note.id}", note_data)

        # Save by shift ID for quick lookup
        self.db.save_to_key(f"handover_shift_{note.shift_id}", note_data)

    def _load_note(self, note_id: str) -> Optional[HandoverNote]:
        """Load handover note from database by ID.

        Args:
            note_id: ID of the handover note

        Returns:
            HandoverNote or None if not found
        """
        note_data = self.db.load_from_key(f"handover_{note_id}")
        if not note_data:
            return None
        return self._deserialize_note(note_data)

    def _deserialize_note(self, note_data: Dict[str, Any]) -> HandoverNote:
        """Deserialize stored note data back to HandoverNote.

        Args:
            note_data: Dictionary with note data

        Returns:
            HandoverNote object
        """
        return HandoverNote(
            id=note_data["id"],
            shift_id=note_data["shift_id"],
            venue_id=note_data["venue_id"],
            author_id=note_data["author_id"],
            author_name=note_data["author_name"],
            created_at=note_data["created_at"],
            sections=note_data["sections"],
            priority=note_data["priority"],
            acknowledged_by=note_data.get("acknowledged_by"),
            acknowledged_at=note_data.get("acknowledged_at"),
        )


# Singleton instance
_service_instance: Optional[HandoverService] = None


def get_handover_service(db=None, notification_hub=None) -> HandoverService:
    """Get or create HandoverService singleton.

    Args:
        db: Database instance (optional, uses get_db() by default)
        notification_hub: Notification hub (optional, uses get_notification_hub() by default)

    Returns:
        HandoverService instance
    """
    global _service_instance
    if _service_instance is None:
        _service_instance = HandoverService(db=db, notification_hub=notification_hub)
    return _service_instance
