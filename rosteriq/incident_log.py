"""Incident/Safety Log for Australian WHS compliance (Round 35).

Venues under AU Work Health and Safety Act 2011 must:
- Record all workplace incidents
- Report "notifiable incidents" to SafeWork regulator
- Keep records for at least 5 years
- Track corrective actions and compliance

Data model:
- Incident: core safety event with severity, category, location, injuries
- CorrectiveAction: follow-up task to prevent recurrence
- IncidentSummary: aggregated statistics for reporting

Thread-safe singleton store backed by SQLite persistence.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, date, timezone
from enum import Enum
from typing import List, Optional, Dict, Any

from rosteriq import persistence as _p

logger = logging.getLogger("rosteriq.incident_log")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class IncidentSeverity(Enum):
    """Severity levels per AU WHS guidelines."""
    NEAR_MISS = "near_miss"
    MINOR = "minor"
    MODERATE = "moderate"
    SERIOUS = "serious"
    CRITICAL = "critical"
    NOTIFIABLE = "notifiable"  # Must be reported to SafeWork


class IncidentCategory(Enum):
    """Incident classification per AU WHS."""
    SLIP_TRIP_FALL = "slip_trip_fall"
    BURN = "burn"
    CUT_LACERATION = "cut_laceration"
    CHEMICAL_EXPOSURE = "chemical_exposure"
    MANUAL_HANDLING = "manual_handling"
    ASSAULT = "assault"
    FOOD_SAFETY = "food_safety"
    EQUIPMENT_FAILURE = "equipment_failure"
    ELECTRICAL = "electrical"
    OTHER = "other"


class IncidentStatus(Enum):
    """Incident lifecycle status."""
    REPORTED = "reported"
    UNDER_INVESTIGATION = "under_investigation"
    CORRECTIVE_ACTION = "corrective_action"
    RESOLVED = "resolved"
    CLOSED = "closed"


class CorrectiveActionStatus(Enum):
    """Corrective action lifecycle."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    OVERDUE = "overdue"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Incident:
    """Core incident record — immutable after creation, mutable via updates."""
    incident_id: str
    venue_id: str
    reported_by: str  # employee_id
    reported_by_name: str
    date_occurred: datetime
    date_reported: datetime
    location: str  # e.g. "kitchen", "bar area", "loading dock"
    category: IncidentCategory
    severity: IncidentSeverity
    description: str
    injured_person: Optional[str] = None
    injury_description: Optional[str] = None
    witnesses: List[str] = field(default_factory=list)
    immediate_action: str = ""
    status: IncidentStatus = IncidentStatus.REPORTED
    is_notifiable: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        d = asdict(self)
        d["date_occurred"] = self.date_occurred.isoformat()
        d["date_reported"] = self.date_reported.isoformat()
        d["category"] = self.category.value
        d["severity"] = self.severity.value
        d["status"] = self.status.value
        d["witnesses"] = json.dumps(self.witnesses)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Incident:
        """Reconstruct from dict (e.g. from SQLite)."""
        d = dict(d)  # copy
        d["date_occurred"] = datetime.fromisoformat(d["date_occurred"])
        d["date_reported"] = datetime.fromisoformat(d["date_reported"])
        d["category"] = IncidentCategory(d["category"])
        d["severity"] = IncidentSeverity(d["severity"])
        d["status"] = IncidentStatus(d["status"])
        d["witnesses"] = json.loads(d["witnesses"]) if isinstance(d["witnesses"], str) else d["witnesses"]
        return cls(**d)


@dataclass
class CorrectiveAction:
    """Follow-up action to prevent recurrence."""
    action_id: str
    incident_id: str
    description: str
    assigned_to: str  # responsible person
    due_date: date
    completed_date: Optional[date] = None
    status: CorrectiveActionStatus = CorrectiveActionStatus.PENDING

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        d = asdict(self)
        d["due_date"] = self.due_date.isoformat()
        d["completed_date"] = self.completed_date.isoformat() if self.completed_date else None
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CorrectiveAction:
        """Reconstruct from dict."""
        d = dict(d)
        d["due_date"] = datetime.fromisoformat(d["due_date"]).date()
        if d["completed_date"]:
            d["completed_date"] = datetime.fromisoformat(d["completed_date"]).date()
        d["status"] = CorrectiveActionStatus(d["status"])
        return cls(**d)


@dataclass
class IncidentSummary:
    """Aggregated incident statistics for a venue over a period."""
    venue_id: str
    period_start: date
    period_end: date
    total_incidents: int
    by_severity: Dict[str, int]
    by_category: Dict[str, int]
    by_location: Dict[str, int]
    by_status: Dict[str, int]
    notifiable_count: int
    open_actions: int
    overdue_actions: int
    incident_rate: Optional[float] = None  # per 1000 hours worked


# ---------------------------------------------------------------------------
# Incident Store — thread-safe singleton backed by SQLite
# ---------------------------------------------------------------------------


class IncidentStore:
    """Thread-safe storage for incidents and corrective actions."""

    _INCIDENT_TABLE = """
        CREATE TABLE IF NOT EXISTS incidents (
            incident_id TEXT PRIMARY KEY,
            venue_id TEXT NOT NULL,
            reported_by TEXT NOT NULL,
            reported_by_name TEXT NOT NULL,
            date_occurred TEXT NOT NULL,
            date_reported TEXT NOT NULL,
            location TEXT NOT NULL,
            category TEXT NOT NULL,
            severity TEXT NOT NULL,
            description TEXT NOT NULL,
            injured_person TEXT,
            injury_description TEXT,
            witnesses TEXT,
            immediate_action TEXT,
            status TEXT NOT NULL,
            is_notifiable INTEGER NOT NULL DEFAULT 0
        )
    """

    _ACTION_TABLE = """
        CREATE TABLE IF NOT EXISTS corrective_actions (
            action_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            description TEXT NOT NULL,
            assigned_to TEXT NOT NULL,
            due_date TEXT NOT NULL,
            completed_date TEXT,
            status TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id)
        )
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.incidents: Dict[str, Incident] = {}
        self.actions: Dict[str, CorrectiveAction] = {}

    def report_incident(self, incident: Incident) -> Incident:
        """Validate and store a new incident. Auto-flag notifiable.

        Args:
            incident: Incident with incident_id, venue_id, severity, etc.

        Returns:
            The stored incident (may have is_notifiable updated).
        """
        # Determine if notifiable
        incident.is_notifiable = self._flag_notifiable(incident)

        with self.lock:
            self.incidents[incident.incident_id] = incident
            _p.upsert("incidents", incident.to_dict(), pk="incident_id")

        logger.info(
            "incident reported: %s (venue=%s, severity=%s, notifiable=%s)",
            incident.incident_id,
            incident.venue_id,
            incident.severity.value,
            incident.is_notifiable,
        )
        return incident

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Fetch an incident by ID."""
        with self.lock:
            return self.incidents.get(incident_id)

    def list_incidents(
        self,
        venue_id: str,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        severity: Optional[IncidentSeverity] = None,
        category: Optional[IncidentCategory] = None,
        status: Optional[IncidentStatus] = None,
    ) -> List[Incident]:
        """Query incidents with optional filters."""
        with self.lock:
            results = [
                inc
                for inc in self.incidents.values()
                if inc.venue_id == venue_id
                and (date_from is None or inc.date_occurred >= date_from)
                and (date_to is None or inc.date_occurred <= date_to)
                and (severity is None or inc.severity == severity)
                and (category is None or inc.category == category)
                and (status is None or inc.status == status)
            ]
        return sorted(results, key=lambda i: i.date_occurred, reverse=True)

    def update_incident(self, incident_id: str, **updates) -> Optional[Incident]:
        """Update an incident's status, details, etc.

        Args:
            incident_id: Incident to update
            **updates: Fields to update (status, severity, immediate_action, etc.)

        Returns:
            Updated incident, or None if not found.
        """
        with self.lock:
            incident = self.incidents.get(incident_id)
            if not incident:
                return None

            # Apply updates
            for key, value in updates.items():
                if key == "status" and isinstance(value, str):
                    value = IncidentStatus(value)
                if key == "category" and isinstance(value, str):
                    value = IncidentCategory(value)
                if key == "severity" and isinstance(value, str):
                    value = IncidentSeverity(value)
                if hasattr(incident, key):
                    setattr(incident, key, value)

            # Re-check notifiable flag
            incident.is_notifiable = self._flag_notifiable(incident)

            # Persist
            _p.upsert("incidents", incident.to_dict(), pk="incident_id")

        logger.info("incident updated: %s", incident_id)
        return incident

    def add_corrective_action(self, action: CorrectiveAction) -> CorrectiveAction:
        """Add a corrective action to an incident."""
        with self.lock:
            self.actions[action.action_id] = action
            _p.upsert("corrective_actions", action.to_dict(), pk="action_id")

        logger.info(
            "corrective action added: %s (incident=%s, due=%s)",
            action.action_id,
            action.incident_id,
            action.due_date,
        )
        return action

    def get_corrective_action(self, action_id: str) -> Optional[CorrectiveAction]:
        """Fetch an action by ID."""
        with self.lock:
            return self.actions.get(action_id)

    def complete_corrective_action(
        self, action_id: str, completed_date: Optional[date] = None
    ) -> Optional[CorrectiveAction]:
        """Mark an action as completed.

        Args:
            action_id: Action to complete
            completed_date: Completion date (default: today)

        Returns:
            Updated action, or None if not found.
        """
        if completed_date is None:
            completed_date = date.today()

        with self.lock:
            action = self.actions.get(action_id)
            if not action:
                return None

            action.completed_date = completed_date
            action.status = CorrectiveActionStatus.COMPLETED

            _p.upsert("corrective_actions", action.to_dict(), pk="action_id")

        logger.info("corrective action completed: %s", action_id)
        return action

    def check_overdue_actions(self, venue_id: str) -> List[CorrectiveAction]:
        """Find all overdue corrective actions for a venue.

        Marks them as OVERDUE if they're past due_date and not COMPLETED.

        Returns:
            List of overdue actions, sorted by due_date.
        """
        today = date.today()
        overdue = []

        with self.lock:
            for action in self.actions.values():
                # Check if action belongs to venue (via incident)
                incident = self.incidents.get(action.incident_id)
                if not incident or incident.venue_id != venue_id:
                    continue

                # Mark as overdue if past due_date and not completed
                if (
                    action.due_date < today
                    and action.status != CorrectiveActionStatus.COMPLETED
                ):
                    if action.status != CorrectiveActionStatus.OVERDUE:
                        action.status = CorrectiveActionStatus.OVERDUE
                        _p.upsert("corrective_actions", action.to_dict(), pk="action_id")
                    overdue.append(action)

        return sorted(overdue, key=lambda a: a.due_date)

    def get_incident_timeline(self, incident_id: str) -> List[Dict[str, Any]]:
        """Return chronological timeline of events for an incident.

        Includes: incident reported, status changes, corrective actions added/completed.

        Returns:
            List of {timestamp, event_type, description} dicts.
        """
        timeline = []

        with self.lock:
            incident = self.incidents.get(incident_id)
            if not incident:
                return timeline

            # Incident reported
            timeline.append(
                {
                    "timestamp": incident.date_reported.isoformat(),
                    "event_type": "reported",
                    "description": f"Incident reported by {incident.reported_by_name}",
                }
            )

            # Current status
            if incident.status != IncidentStatus.REPORTED:
                timeline.append(
                    {
                        "timestamp": incident.date_reported.isoformat(),
                        "event_type": "status_change",
                        "description": f"Status: {incident.status.value}",
                    }
                )

            # Corrective actions
            for action in self.actions.values():
                if action.incident_id == incident_id:
                    timeline.append(
                        {
                            "timestamp": action.due_date.isoformat(),
                            "event_type": "corrective_action_added",
                            "description": f"{action.description} (assigned to {action.assigned_to})",
                        }
                    )
                    if action.completed_date:
                        timeline.append(
                            {
                                "timestamp": action.completed_date.isoformat(),
                                "event_type": "corrective_action_completed",
                                "description": f"{action.description}",
                            }
                        )

        return sorted(timeline, key=lambda e: e["timestamp"])

    def build_incident_summary(
        self,
        venue_id: str,
        incidents: Optional[List[Incident]] = None,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None,
        hours_worked: Optional[float] = None,
    ) -> IncidentSummary:
        """Aggregate incident statistics for a venue over a period.

        Args:
            venue_id: Venue to summarize
            incidents: Pre-filtered incident list (default: all for venue)
            period_start: Start of period (default: 1 month ago)
            period_end: End of period (default: today)
            hours_worked: Total hours worked (for incident_rate per 1000h)

        Returns:
            IncidentSummary with aggregated stats.
        """
        from datetime import timedelta

        if period_end is None:
            period_end = date.today()
        if period_start is None:
            period_start = period_end - timedelta(days=30)

        if incidents is None:
            start_dt = datetime.combine(period_start, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(period_end, datetime.max.time(), tzinfo=timezone.utc)
            incidents = self.list_incidents(
                venue_id, date_from=start_dt, date_to=end_dt
            )

        by_severity = {}
        by_category = {}
        by_location = {}
        by_status = {}
        notifiable = 0

        for inc in incidents:
            # Count by severity
            sev = inc.severity.value
            by_severity[sev] = by_severity.get(sev, 0) + 1

            # Count by category
            cat = inc.category.value
            by_category[cat] = by_category.get(cat, 0) + 1

            # Count by location
            loc = inc.location
            by_location[loc] = by_location.get(loc, 0) + 1

            # Count by status
            st = inc.status.value
            by_status[st] = by_status.get(st, 0) + 1

            # Count notifiable
            if inc.is_notifiable:
                notifiable += 1

        # Count open corrective actions
        open_actions = 0
        overdue_actions = 0
        with self.lock:
            for action in self.actions.values():
                incident = self.incidents.get(action.incident_id)
                if not incident or incident.venue_id != venue_id:
                    continue
                if action.status != CorrectiveActionStatus.COMPLETED:
                    open_actions += 1
                if action.status == CorrectiveActionStatus.OVERDUE:
                    overdue_actions += 1

        # Calculate incident rate (per 1000 hours)
        incident_rate = None
        if hours_worked and hours_worked > 0:
            incident_rate = (len(incidents) / hours_worked) * 1000

        return IncidentSummary(
            venue_id=venue_id,
            period_start=period_start,
            period_end=period_end,
            total_incidents=len(incidents),
            by_severity=by_severity,
            by_category=by_category,
            by_location=by_location,
            by_status=by_status,
            notifiable_count=notifiable,
            open_actions=open_actions,
            overdue_actions=overdue_actions,
            incident_rate=incident_rate,
        )

    @staticmethod
    def _flag_notifiable(incident: Incident) -> bool:
        """Determine if incident must be reported to SafeWork.

        AU WHS: notifiable incidents include:
        - CRITICAL severity (death, permanent disability, significant hospitalisation)
        - NOTIFIABLE severity (defined above)
        - Any serious injury requiring hospitalization
        """
        if incident.severity in (IncidentSeverity.CRITICAL, IncidentSeverity.NOTIFIABLE):
            return True
        if incident.severity == IncidentSeverity.SERIOUS and incident.injured_person:
            return True
        return False

    def _rehydrate(self) -> None:
        """Load all incidents and actions from SQLite on startup."""
        try:
            incidents_rows = _p.fetchall("SELECT * FROM incidents")
            for row in incidents_rows:
                inc = Incident.from_dict(dict(row))
                self.incidents[inc.incident_id] = inc

            actions_rows = _p.fetchall("SELECT * FROM corrective_actions")
            for row in actions_rows:
                act = CorrectiveAction.from_dict(dict(row))
                self.actions[act.action_id] = act

            logger.info(
                "rehydrated incident store: %d incidents, %d actions",
                len(self.incidents),
                len(self.actions),
            )
        except Exception as e:
            logger.warning("rehydrate failed (may be first run): %s", e)


# ---------------------------------------------------------------------------
# Singleton accessor + lifecycle
# ---------------------------------------------------------------------------

_store: Optional[IncidentStore] = None
_store_lock = threading.Lock()


def get_incident_store() -> IncidentStore:
    """Get or create the global IncidentStore singleton."""
    global _store
    with _store_lock:
        if _store is None:
            _store = IncidentStore()
    return _store


def _reset_for_tests() -> None:
    """Test helper — reset the singleton."""
    global _store
    with _store_lock:
        _store = None


# Register schema + rehydrate callback
_p.register_schema("incident_log", IncidentStore._INCIDENT_TABLE)
_p.register_schema("incident_log_actions", IncidentStore._ACTION_TABLE)


@_p.on_init
def _rehydrate_incidents() -> None:
    """Rehydrate incident store from SQLite at startup."""
    get_incident_store()._rehydrate()


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------


def report_incident(
    venue_id: str,
    reported_by: str,
    reported_by_name: str,
    date_occurred: datetime,
    location: str,
    category: IncidentCategory,
    severity: IncidentSeverity,
    description: str,
    injured_person: Optional[str] = None,
    injury_description: Optional[str] = None,
    witnesses: Optional[List[str]] = None,
    immediate_action: str = "",
) -> Incident:
    """Report a new incident.

    Args:
        venue_id: Venue where incident occurred
        reported_by: Employee ID of reporter
        reported_by_name: Name of reporter
        date_occurred: When incident happened
        location: Where in venue (e.g. "kitchen", "bar")
        category: IncidentCategory enum
        severity: IncidentSeverity enum
        description: What happened
        injured_person: Name/ID of injured person (if applicable)
        injury_description: Nature of injury
        witnesses: List of witness names/IDs
        immediate_action: What was done immediately

    Returns:
        The stored Incident (with is_notifiable flag set).
    """
    incident = Incident(
        incident_id=f"inc_{uuid.uuid4().hex[:12]}",
        venue_id=venue_id,
        reported_by=reported_by,
        reported_by_name=reported_by_name,
        date_occurred=date_occurred,
        date_reported=datetime.now(timezone.utc),
        location=location,
        category=category,
        severity=severity,
        description=description,
        injured_person=injured_person,
        injury_description=injury_description,
        witnesses=witnesses or [],
        immediate_action=immediate_action,
    )
    return get_incident_store().report_incident(incident)


def update_incident(incident_id: str, **updates) -> Optional[Incident]:
    """Update an incident's details.

    Args:
        incident_id: Incident to update
        **updates: Fields to update (e.g., status, severity, etc.)

    Returns:
        Updated incident, or None if not found.
    """
    return get_incident_store().update_incident(incident_id, **updates)


def add_corrective_action(
    incident_id: str,
    description: str,
    assigned_to: str,
    due_date: date,
) -> CorrectiveAction:
    """Add a corrective action to an incident.

    Args:
        incident_id: Parent incident
        description: Action description
        assigned_to: Responsible person
        due_date: When it should be completed

    Returns:
        The stored CorrectiveAction.
    """
    action = CorrectiveAction(
        action_id=f"act_{uuid.uuid4().hex[:12]}",
        incident_id=incident_id,
        description=description,
        assigned_to=assigned_to,
        due_date=due_date,
    )
    return get_incident_store().add_corrective_action(action)


def complete_corrective_action(
    action_id: str, completed_date: Optional[date] = None
) -> Optional[CorrectiveAction]:
    """Mark a corrective action as completed.

    Args:
        action_id: Action to complete
        completed_date: Completion date (default: today)

    Returns:
        Updated action, or None if not found.
    """
    return get_incident_store().complete_corrective_action(action_id, completed_date)


def check_overdue_actions(venue_id: str) -> List[CorrectiveAction]:
    """Get all overdue corrective actions for a venue.

    Args:
        venue_id: Venue to check

    Returns:
        List of overdue actions, sorted by due_date.
    """
    return get_incident_store().check_overdue_actions(venue_id)


def get_incident_timeline(incident_id: str) -> List[Dict[str, Any]]:
    """Get chronological timeline for an incident.

    Args:
        incident_id: Incident to timeline

    Returns:
        List of {timestamp, event_type, description} dicts.
    """
    return get_incident_store().get_incident_timeline(incident_id)


def build_incident_summary(
    venue_id: str,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
    hours_worked: Optional[float] = None,
) -> IncidentSummary:
    """Build aggregated incident summary for a venue.

    Args:
        venue_id: Venue to summarize
        period_start: Start of period (default: 1 month ago)
        period_end: End of period (default: today)
        hours_worked: Total hours worked (for incident_rate per 1000h)

    Returns:
        IncidentSummary with aggregated statistics.
    """
    return get_incident_store().build_incident_summary(
        venue_id,
        period_start=period_start,
        period_end=period_end,
        hours_worked=hours_worked,
    )
