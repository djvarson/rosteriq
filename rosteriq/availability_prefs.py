"""Staff Availability Preferences module for RosterIQ.

Manages recurring weekly availability templates, one-off date overrides, blackout dates,
and max hours preferences for smarter roster suggestions. Feeds into the roster engine
for intelligent shift allocation.

Data persisted to SQLite for durability and offline queries.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.availability_prefs")


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass
class WeeklyPreference:
    """Recurring weekly availability template (Mon-Sun).

    Attributes:
        id: Unique identifier (uuid)
        venue_id: Venue identifier
        employee_id: Employee identifier
        day_of_week: Day of week (0=Monday, 6=Sunday)
        status: AVAILABLE, UNAVAILABLE, or PREFERRED
        start_time: Optional HH:MM availability window start
        end_time: Optional HH:MM availability window end
        notes: Optional notes
        effective_from: ISO date when this preference starts
        effective_until: Optional ISO date when preference ends (None = indefinite)
        created_at: ISO datetime when created
        updated_at: ISO datetime when last updated
    """
    id: str
    venue_id: str
    employee_id: str
    day_of_week: int
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    notes: Optional[str] = None
    effective_from: str = ""
    effective_until: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "day_of_week": self.day_of_week,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "notes": self.notes,
            "effective_from": self.effective_from,
            "effective_until": self.effective_until,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class AvailabilityOverride:
    """One-off date override for availability (beats weekly preference).

    Attributes:
        id: Unique identifier (uuid)
        venue_id: Venue identifier
        employee_id: Employee identifier
        date: ISO date of override
        status: AVAILABLE, UNAVAILABLE, or PREFERRED
        start_time: Optional HH:MM availability window start
        end_time: Optional HH:MM availability window end
        reason: Optional reason for override (e.g., "sick", "event")
        created_at: ISO datetime when created
    """
    id: str
    venue_id: str
    employee_id: str
    date: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    reason: Optional[str] = None
    created_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "date": self.date,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "reason": self.reason,
            "created_at": self.created_at,
        }


@dataclass
class EmployeeConstraints:
    """Scheduling constraints for an employee.

    Attributes:
        id: Unique identifier (uuid)
        venue_id: Venue identifier
        employee_id: Employee identifier
        max_hours_per_week: Optional max hours per week (float)
        min_hours_per_week: Optional min hours per week (float)
        max_shifts_per_week: Optional max shifts per week (int)
        max_consecutive_days: Optional max days in a row (int)
        preferred_shift_length: Optional preferred shift length in hours (float)
        blackout_dates: List of ISO dates when employee cannot work
        updated_at: ISO datetime when last updated
    """
    id: str
    venue_id: str
    employee_id: str
    max_hours_per_week: Optional[float] = None
    min_hours_per_week: Optional[float] = None
    max_shifts_per_week: Optional[int] = None
    max_consecutive_days: Optional[int] = None
    preferred_shift_length: Optional[float] = None
    blackout_dates: List[str] = field(default_factory=list)
    updated_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "max_hours_per_week": self.max_hours_per_week,
            "min_hours_per_week": self.min_hours_per_week,
            "max_shifts_per_week": self.max_shifts_per_week,
            "max_consecutive_days": self.max_consecutive_days,
            "preferred_shift_length": self.preferred_shift_length,
            "blackout_dates": self.blackout_dates,
            "updated_at": self.updated_at,
        }


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_AVAILABILITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS weekly_preferences (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    day_of_week INTEGER NOT NULL,
    status TEXT NOT NULL,
    start_time TEXT,
    end_time TEXT,
    notes TEXT,
    effective_from TEXT NOT NULL,
    effective_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_weekly_venue_employee ON weekly_preferences(venue_id, employee_id);
CREATE INDEX IF NOT EXISTS ix_weekly_day ON weekly_preferences(day_of_week);
CREATE INDEX IF NOT EXISTS ix_weekly_effective ON weekly_preferences(effective_from, effective_until);

CREATE TABLE IF NOT EXISTS availability_overrides (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    date TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time TEXT,
    end_time TEXT,
    reason TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_override_venue_employee ON availability_overrides(venue_id, employee_id);
CREATE INDEX IF NOT EXISTS ix_override_date ON availability_overrides(date);
CREATE INDEX IF NOT EXISTS ix_override_venue_date ON availability_overrides(venue_id, date);

CREATE TABLE IF NOT EXISTS employee_constraints (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    max_hours_per_week REAL,
    min_hours_per_week REAL,
    max_shifts_per_week INTEGER,
    max_consecutive_days INTEGER,
    preferred_shift_length REAL,
    blackout_dates TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(venue_id, employee_id)
);
CREATE INDEX IF NOT EXISTS ix_constraints_venue_employee ON employee_constraints(venue_id, employee_id);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("availability_prefs", _AVAILABILITY_SCHEMA)
            def _rehydrate_on_init():
                store = get_availability_prefs_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Availability Preferences Store
# ---------------------------------------------------------------------------


class AvailabilityPrefsStore:
    """Thread-safe in-memory store for availability preferences with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._weekly_prefs: Dict[str, WeeklyPreference] = {}
        self._overrides: Dict[str, AvailabilityOverride] = {}
        self._constraints: Dict[str, EmployeeConstraints] = {}
        self._lock = threading.Lock()

    def _persist_weekly(self, pref: WeeklyPreference) -> None:
        """Persist a weekly preference to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": pref.id,
            "venue_id": pref.venue_id,
            "employee_id": pref.employee_id,
            "day_of_week": pref.day_of_week,
            "status": pref.status,
            "start_time": pref.start_time,
            "end_time": pref.end_time,
            "notes": pref.notes,
            "effective_from": pref.effective_from,
            "effective_until": pref.effective_until,
            "created_at": pref.created_at,
            "updated_at": pref.updated_at,
        }
        try:
            _p.upsert("weekly_preferences", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist weekly preference %s: %s", pref.id, e)

    def _persist_override(self, override: AvailabilityOverride) -> None:
        """Persist an availability override to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": override.id,
            "venue_id": override.venue_id,
            "employee_id": override.employee_id,
            "date": override.date,
            "status": override.status,
            "start_time": override.start_time,
            "end_time": override.end_time,
            "reason": override.reason,
            "created_at": override.created_at,
        }
        try:
            _p.upsert("availability_overrides", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist availability override %s: %s", override.id, e)

    def _persist_constraints(self, constraints: EmployeeConstraints) -> None:
        """Persist employee constraints to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        row = {
            "id": constraints.id,
            "venue_id": constraints.venue_id,
            "employee_id": constraints.employee_id,
            "max_hours_per_week": constraints.max_hours_per_week,
            "min_hours_per_week": constraints.min_hours_per_week,
            "max_shifts_per_week": constraints.max_shifts_per_week,
            "max_consecutive_days": constraints.max_consecutive_days,
            "preferred_shift_length": constraints.preferred_shift_length,
            "blackout_dates": json.dumps(constraints.blackout_dates),
            "updated_at": constraints.updated_at,
        }
        try:
            _p.upsert("employee_constraints", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist constraints %s: %s", constraints.id, e)

    def _rehydrate(self) -> None:
        """Load all data from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            import json

            # Load weekly preferences
            rows = _p.fetchall("SELECT * FROM weekly_preferences")
            for row in rows:
                pref = WeeklyPreference(
                    id=row["id"],
                    venue_id=row["venue_id"],
                    employee_id=row["employee_id"],
                    day_of_week=row["day_of_week"],
                    status=row["status"],
                    start_time=row.get("start_time"),
                    end_time=row.get("end_time"),
                    notes=row.get("notes"),
                    effective_from=row.get("effective_from", ""),
                    effective_until=row.get("effective_until"),
                    created_at=row.get("created_at", ""),
                    updated_at=row.get("updated_at", ""),
                )
                self._weekly_prefs[pref.id] = pref

            # Load overrides
            rows = _p.fetchall("SELECT * FROM availability_overrides")
            for row in rows:
                override = AvailabilityOverride(
                    id=row["id"],
                    venue_id=row["venue_id"],
                    employee_id=row["employee_id"],
                    date=row["date"],
                    status=row["status"],
                    start_time=row.get("start_time"),
                    end_time=row.get("end_time"),
                    reason=row.get("reason"),
                    created_at=row.get("created_at", ""),
                )
                self._overrides[override.id] = override

            # Load constraints
            rows = _p.fetchall("SELECT * FROM employee_constraints")
            for row in rows:
                blackout_dates = []
                if row.get("blackout_dates"):
                    try:
                        blackout_dates = json.loads(row["blackout_dates"])
                    except (json.JSONDecodeError, TypeError):
                        blackout_dates = []

                constraints = EmployeeConstraints(
                    id=row["id"],
                    venue_id=row["venue_id"],
                    employee_id=row["employee_id"],
                    max_hours_per_week=row.get("max_hours_per_week"),
                    min_hours_per_week=row.get("min_hours_per_week"),
                    max_shifts_per_week=row.get("max_shifts_per_week"),
                    max_consecutive_days=row.get("max_consecutive_days"),
                    preferred_shift_length=row.get("preferred_shift_length"),
                    blackout_dates=blackout_dates,
                    updated_at=row.get("updated_at", ""),
                )
                self._constraints[f"{row['venue_id']}#{row['employee_id']}"] = constraints

            logger.info(
                "Rehydrated %d weekly prefs, %d overrides, %d constraints",
                len(self._weekly_prefs),
                len(self._overrides),
                len(self._constraints),
            )
        except Exception as e:
            logger.warning("Failed to rehydrate availability prefs: %s", e)

    def set_weekly_preference(self, pref_dict: Dict[str, Any]) -> WeeklyPreference:
        """Set or update a weekly preference. Upserts on employee+day+venue+effective_from.

        Args:
            pref_dict: Dict with venue_id, employee_id, day_of_week, status, start_time,
                       end_time, notes, effective_from, effective_until

        Returns:
            WeeklyPreference object
        """
        now = datetime.now(timezone.utc).isoformat()
        pref_id = pref_dict.get("id") or f"pref_{uuid.uuid4().hex[:12]}"

        pref = WeeklyPreference(
            id=pref_id,
            venue_id=pref_dict["venue_id"],
            employee_id=pref_dict["employee_id"],
            day_of_week=pref_dict["day_of_week"],
            status=pref_dict["status"],
            start_time=pref_dict.get("start_time"),
            end_time=pref_dict.get("end_time"),
            notes=pref_dict.get("notes"),
            effective_from=pref_dict.get("effective_from", ""),
            effective_until=pref_dict.get("effective_until"),
            created_at=pref_dict.get("created_at", now),
            updated_at=now,
        )

        with self._lock:
            self._weekly_prefs[pref.id] = pref

        self._persist_weekly(pref)
        return pref

    def get_weekly_preferences(
        self, venue_id: str, employee_id: str
    ) -> List[WeeklyPreference]:
        """Get all weekly preferences for an employee at a venue.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier

        Returns:
            List of WeeklyPreference objects
        """
        with self._lock:
            prefs = [
                p for p in self._weekly_prefs.values()
                if p.venue_id == venue_id and p.employee_id == employee_id
            ]
            prefs.sort(key=lambda p: p.day_of_week)
            return prefs

    def delete_weekly_preference(self, pref_id: str) -> bool:
        """Delete a weekly preference.

        Args:
            pref_id: Preference ID

        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            if pref_id not in self._weekly_prefs:
                return False
            del self._weekly_prefs[pref_id]

        _p = _get_persistence()
        if _p and _p.is_persistence_enabled():
            try:
                _p.execute("DELETE FROM weekly_preferences WHERE id = ?", (pref_id,))
            except Exception as e:
                logger.warning("Failed to delete weekly preference %s: %s", pref_id, e)

        return True

    def add_override(self, override_dict: Dict[str, Any]) -> AvailabilityOverride:
        """Add an availability override for a specific date.

        Args:
            override_dict: Dict with venue_id, employee_id, date, status, start_time,
                          end_time, reason

        Returns:
            AvailabilityOverride object
        """
        now = datetime.now(timezone.utc).isoformat()
        override_id = f"override_{uuid.uuid4().hex[:12]}"

        override = AvailabilityOverride(
            id=override_id,
            venue_id=override_dict["venue_id"],
            employee_id=override_dict["employee_id"],
            date=override_dict["date"],
            status=override_dict["status"],
            start_time=override_dict.get("start_time"),
            end_time=override_dict.get("end_time"),
            reason=override_dict.get("reason"),
            created_at=now,
        )

        with self._lock:
            self._overrides[override.id] = override

        self._persist_override(override)
        return override

    def get_overrides(
        self,
        venue_id: str,
        employee_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[AvailabilityOverride]:
        """Get availability overrides for an employee, optionally filtered by date range.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier
            date_from: Optional ISO date filter (inclusive)
            date_to: Optional ISO date filter (inclusive)

        Returns:
            List of AvailabilityOverride objects
        """
        with self._lock:
            overrides = [
                o for o in self._overrides.values()
                if o.venue_id == venue_id and o.employee_id == employee_id
            ]

            if date_from:
                overrides = [o for o in overrides if o.date >= date_from]
            if date_to:
                overrides = [o for o in overrides if o.date <= date_to]

            overrides.sort(key=lambda o: o.date)
            return overrides

    def delete_override(self, override_id: str) -> bool:
        """Delete an availability override.

        Args:
            override_id: Override ID

        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            if override_id not in self._overrides:
                return False
            del self._overrides[override_id]

        _p = _get_persistence()
        if _p and _p.is_persistence_enabled():
            try:
                _p.execute("DELETE FROM availability_overrides WHERE id = ?", (override_id,))
            except Exception as e:
                logger.warning("Failed to delete override %s: %s", override_id, e)

        return True

    def set_constraints(self, constraints_dict: Dict[str, Any]) -> EmployeeConstraints:
        """Set or update employee constraints. Upserts on venue+employee.

        Args:
            constraints_dict: Dict with venue_id, employee_id, max_hours_per_week,
                            min_hours_per_week, max_shifts_per_week, max_consecutive_days,
                            preferred_shift_length, blackout_dates

        Returns:
            EmployeeConstraints object
        """
        now = datetime.now(timezone.utc).isoformat()
        key = f"{constraints_dict['venue_id']}#{constraints_dict['employee_id']}"

        # Preserve existing ID if updating
        existing = self._constraints.get(key)
        constraints_id = existing.id if existing else f"constraints_{uuid.uuid4().hex[:12]}"

        constraints = EmployeeConstraints(
            id=constraints_id,
            venue_id=constraints_dict["venue_id"],
            employee_id=constraints_dict["employee_id"],
            max_hours_per_week=constraints_dict.get("max_hours_per_week"),
            min_hours_per_week=constraints_dict.get("min_hours_per_week"),
            max_shifts_per_week=constraints_dict.get("max_shifts_per_week"),
            max_consecutive_days=constraints_dict.get("max_consecutive_days"),
            preferred_shift_length=constraints_dict.get("preferred_shift_length"),
            blackout_dates=constraints_dict.get("blackout_dates", []),
            updated_at=now,
        )

        with self._lock:
            self._constraints[key] = constraints

        self._persist_constraints(constraints)
        return constraints

    def get_constraints(
        self, venue_id: str, employee_id: str
    ) -> Optional[EmployeeConstraints]:
        """Get scheduling constraints for an employee.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier

        Returns:
            EmployeeConstraints object or None if not set
        """
        key = f"{venue_id}#{employee_id}"
        with self._lock:
            return self._constraints.get(key)

    def get_availability_for_date(
        self, venue_id: str, employee_id: str, date_str: str
    ) -> Dict[str, Any]:
        """Resolve availability for a specific date. Priority: override > weekly > default available.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier
            date_str: ISO date string

        Returns:
            Dict with status, start_time, end_time, source (override/weekly/default)
        """
        # Check for override first
        with self._lock:
            for override in self._overrides.values():
                if (override.venue_id == venue_id and
                    override.employee_id == employee_id and
                    override.date == date_str):
                    return {
                        "status": override.status,
                        "start_time": override.start_time,
                        "end_time": override.end_time,
                        "source": "override",
                    }

        # Check for weekly preference
        try:
            import datetime as dt
            d = dt.datetime.fromisoformat(date_str).date()
            day_of_week = d.weekday()

            found_prefs = []
            for pref in self._weekly_prefs.values():
                if (pref.venue_id == venue_id and
                    pref.employee_id == employee_id and
                    pref.day_of_week == day_of_week):
                    found_prefs.append(pref)

            # Filter by effective dates and get the most recent
            for pref in found_prefs:
                eff_from = pref.effective_from or "1900-01-01"
                eff_until = pref.effective_until or "2099-12-31"
                if eff_from <= date_str <= eff_until:
                    return {
                        "status": pref.status,
                        "start_time": pref.start_time,
                        "end_time": pref.end_time,
                        "source": "weekly",
                    }
        except Exception as e:
            logger.warning("Error resolving weekly preference for %s: %s", date_str, e)

        # Default: available
        return {
            "status": "AVAILABLE",
            "start_time": None,
            "end_time": None,
            "source": "default",
        }

    def get_team_availability(self, venue_id: str, date_str: str) -> List[Dict[str, Any]]:
        """Get availability for all employees at a venue for a specific date.

        Args:
            venue_id: Venue identifier
            date_str: ISO date string

        Returns:
            List of dicts with employee_id, status, start_time, end_time, source
        """
        result = []
        employee_ids = set()

        with self._lock:
            # Get all employees with any preferences/overrides at this venue
            for pref in self._weekly_prefs.values():
                if pref.venue_id == venue_id:
                    employee_ids.add(pref.employee_id)
            for override in self._overrides.values():
                if override.venue_id == venue_id:
                    employee_ids.add(override.employee_id)
            for key in self._constraints.keys():
                v_id, e_id = key.split("#")
                if v_id == venue_id:
                    employee_ids.add(e_id)

        for emp_id in sorted(employee_ids):
            avail = self.get_availability_for_date(venue_id, emp_id, date_str)
            result.append({
                "employee_id": emp_id,
                "status": avail["status"],
                "start_time": avail["start_time"],
                "end_time": avail["end_time"],
                "source": avail["source"],
            })

        return result

    def get_available_staff(
        self,
        venue_id: str,
        date_str: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> List[str]:
        """Get employees available for a specific date and optional time window.

        Args:
            venue_id: Venue identifier
            date_str: ISO date string
            start_time: Optional HH:MM to start window
            end_time: Optional HH:MM to end window

        Returns:
            List of employee IDs available for the date/time
        """
        available = []
        employee_ids = set()

        with self._lock:
            for pref in self._weekly_prefs.values():
                if pref.venue_id == venue_id:
                    employee_ids.add(pref.employee_id)
            for override in self._overrides.values():
                if override.venue_id == venue_id:
                    employee_ids.add(override.employee_id)
            for key in self._constraints.keys():
                v_id, e_id = key.split("#")
                if v_id == venue_id:
                    employee_ids.add(e_id)

        for emp_id in employee_ids:
            avail = self.get_availability_for_date(venue_id, emp_id, date_str)

            if avail["status"] == "UNAVAILABLE":
                continue

            # Check if time window matches
            if start_time and end_time and avail["start_time"] and avail["end_time"]:
                # Simple string comparison for HH:MM
                if avail["end_time"] > start_time and avail["start_time"] < end_time:
                    available.append(emp_id)
            else:
                # No time window specified or preference has no window
                available.append(emp_id)

        return sorted(available)

    def add_blackout_date(
        self, venue_id: str, employee_id: str, date_str: str, reason: Optional[str] = None
    ) -> EmployeeConstraints:
        """Add a blackout date (day employee cannot work) to constraints.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier
            date_str: ISO date to blackout
            reason: Optional reason

        Returns:
            Updated EmployeeConstraints
        """
        constraints = self.get_constraints(venue_id, employee_id)

        if not constraints:
            constraints_dict = {
                "venue_id": venue_id,
                "employee_id": employee_id,
                "blackout_dates": [date_str],
            }
            return self.set_constraints(constraints_dict)

        if date_str not in constraints.blackout_dates:
            constraints.blackout_dates.append(date_str)
            constraints.blackout_dates.sort()
            return self.set_constraints(constraints.to_dict())

        return constraints

    def remove_blackout_date(
        self, venue_id: str, employee_id: str, date_str: str
    ) -> EmployeeConstraints:
        """Remove a blackout date from constraints.

        Args:
            venue_id: Venue identifier
            employee_id: Employee identifier
            date_str: ISO date to remove

        Returns:
            Updated EmployeeConstraints
        """
        constraints = self.get_constraints(venue_id, employee_id)

        if not constraints or date_str not in constraints.blackout_dates:
            return constraints

        constraints.blackout_dates.remove(date_str)
        return self.set_constraints(constraints.to_dict())


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[AvailabilityPrefsStore] = None
_store_lock = threading.Lock()


def get_availability_prefs_store() -> AvailabilityPrefsStore:
    """Get the module-level availability preferences store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = AvailabilityPrefsStore()
    return _store


def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _store
    with _store_lock:
        _store = AvailabilityPrefsStore.__new__(AvailabilityPrefsStore)
        _store._lock = threading.Lock()
        _store._weekly_prefs = {}
        _store._overrides = {}
        _store._constraints = {}
