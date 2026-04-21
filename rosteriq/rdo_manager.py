"""Rostered Days Off (RDO) Manager for Australian hospitality venues.

Australian workplace entitlement: full-time employees accrue time toward a day off.
E.g., 19-day month = 1 RDO per 4-week cycle. Manages accrual, scheduling, balances.

Data persisted to SQLite for queries and compliance reporting.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.rdo_manager")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class RDOStatus(str, Enum):
    """Status of an RDO schedule."""
    SCHEDULED = "scheduled"
    TAKEN = "taken"
    CANCELLED = "cancelled"
    SWAPPED = "swapped"


@dataclass
class RDOPolicy:
    """Venue-level RDO policy configuration."""
    id: str
    venue_id: str
    name: str  # e.g., "Standard 28-day cycle"
    cycle_days: int  # 14 or 28
    accrual_hours_per_day: float  # Typically 0.4 (1 RDO per 4-week cycle with 19-day month)
    rdo_length_hours: float  # 7.6 for full day
    eligible_employment_types: List[str] = field(default_factory=lambda: ["FULL_TIME"])
    min_service_days: int = 0
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "name": self.name,
            "cycle_days": self.cycle_days,
            "accrual_hours_per_day": self.accrual_hours_per_day,
            "rdo_length_hours": self.rdo_length_hours,
            "eligible_employment_types": self.eligible_employment_types,
            "min_service_days": self.min_service_days,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class RDOBalance:
    """Per-employee RDO balance tracking."""
    id: str
    venue_id: str
    employee_id: str
    policy_id: str
    accrued_hours: float
    taken_hours: float
    employment_start_date: date
    employment_type: str  # e.g., "FULL_TIME", "PART_TIME"
    last_accrual_date: Optional[date] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def balance_hours(self) -> float:
        """Computed: accrued - taken."""
        return self.accrued_hours - self.taken_hours

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "policy_id": self.policy_id,
            "accrued_hours": self.accrued_hours,
            "taken_hours": self.taken_hours,
            "balance_hours": self.balance_hours,
            "employment_start_date": self.employment_start_date.isoformat(),
            "employment_type": self.employment_type,
            "last_accrual_date": self.last_accrual_date.isoformat() if self.last_accrual_date else None,
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class RDOSchedule:
    """Scheduled RDO day off."""
    id: str
    venue_id: str
    employee_id: str
    date: date
    hours: float  # 7.6 typical
    status: RDOStatus
    swap_date: Optional[date] = None  # If swapped, new date
    approved_by: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "date": self.date.isoformat(),
            "hours": self.hours,
            "status": self.status.value,
            "swap_date": self.swap_date.isoformat() if self.swap_date else None,
            "approved_by": self.approved_by,
            "notes": self.notes,
            "created_at": self.created_at.isoformat(),
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


_RDO_POLICIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS rdo_policies (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    name TEXT NOT NULL,
    cycle_days INTEGER NOT NULL,
    accrual_hours_per_day REAL NOT NULL,
    rdo_length_hours REAL NOT NULL,
    eligible_employment_types TEXT NOT NULL,
    min_service_days INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rdo_policy_venue ON rdo_policies(venue_id);
"""

_RDO_BALANCES_SCHEMA = """
CREATE TABLE IF NOT EXISTS rdo_balances (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    accrued_hours REAL NOT NULL,
    taken_hours REAL NOT NULL,
    employment_start_date TEXT NOT NULL,
    employment_type TEXT NOT NULL,
    last_accrual_date TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rdo_balance_venue_emp ON rdo_balances(venue_id, employee_id);
CREATE INDEX IF NOT EXISTS ix_rdo_balance_policy ON rdo_balances(policy_id);
"""

_RDO_SCHEDULES_SCHEMA = """
CREATE TABLE IF NOT EXISTS rdo_schedules (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    date TEXT NOT NULL,
    hours REAL NOT NULL,
    status TEXT NOT NULL,
    swap_date TEXT,
    approved_by TEXT,
    notes TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rdo_schedule_venue_emp ON rdo_schedules(venue_id, employee_id);
CREATE INDEX IF NOT EXISTS ix_rdo_schedule_date ON rdo_schedules(date);
CREATE INDEX IF NOT EXISTS ix_rdo_schedule_status ON rdo_schedules(status);
"""


def _register_schema_and_callbacks():
    """Register schemas and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("rdo_policies", _RDO_POLICIES_SCHEMA)
            _p.register_schema("rdo_balances", _RDO_BALANCES_SCHEMA)
            _p.register_schema("rdo_schedules", _RDO_SCHEDULES_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_rdo_manager_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# RDO Manager Store
# ---------------------------------------------------------------------------


class RDOManagerStore:
    """Thread-safe in-memory store for RDO policies, balances, and schedules.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._policies: Dict[str, RDOPolicy] = {}
        self._balances: Dict[str, RDOBalance] = {}
        self._schedules: Dict[str, RDOSchedule] = {}
        self._lock = threading.Lock()

    # ─────────────────────────────────────────────────────────────────────
    # Persistence helpers
    # ─────────────────────────────────────────────────────────────────────

    def _persist_policy(self, policy: RDOPolicy) -> None:
        """Persist a policy to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": policy.id,
            "venue_id": policy.venue_id,
            "name": policy.name,
            "cycle_days": policy.cycle_days,
            "accrual_hours_per_day": policy.accrual_hours_per_day,
            "rdo_length_hours": policy.rdo_length_hours,
            "eligible_employment_types": ",".join(policy.eligible_employment_types),
            "min_service_days": policy.min_service_days,
            "is_active": policy.is_active,
            "created_at": policy.created_at.isoformat(),
        }
        try:
            _p.upsert("rdo_policies", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist policy %s: %s", policy.id, e)

    def _persist_balance(self, balance: RDOBalance) -> None:
        """Persist a balance to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": balance.id,
            "venue_id": balance.venue_id,
            "employee_id": balance.employee_id,
            "policy_id": balance.policy_id,
            "accrued_hours": balance.accrued_hours,
            "taken_hours": balance.taken_hours,
            "employment_start_date": balance.employment_start_date.isoformat(),
            "employment_type": balance.employment_type,
            "last_accrual_date": balance.last_accrual_date.isoformat() if balance.last_accrual_date else None,
            "updated_at": balance.updated_at.isoformat(),
        }
        try:
            _p.upsert("rdo_balances", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist balance %s: %s", balance.id, e)

    def _persist_schedule(self, schedule: RDOSchedule) -> None:
        """Persist a schedule to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": schedule.id,
            "venue_id": schedule.venue_id,
            "employee_id": schedule.employee_id,
            "date": schedule.date.isoformat(),
            "hours": schedule.hours,
            "status": schedule.status.value,
            "swap_date": schedule.swap_date.isoformat() if schedule.swap_date else None,
            "approved_by": schedule.approved_by,
            "notes": schedule.notes,
            "created_at": schedule.created_at.isoformat(),
        }
        try:
            _p.upsert("rdo_schedules", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist schedule %s: %s", schedule.id, e)

    def _rehydrate(self) -> None:
        """Load all data from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            # Rehydrate policies
            rows = _p.fetchall("SELECT * FROM rdo_policies")
            for row in rows:
                row_dict = dict(row)
                policy = self._row_to_policy(row_dict)
                self._policies[policy.id] = policy

            # Rehydrate balances
            rows = _p.fetchall("SELECT * FROM rdo_balances")
            for row in rows:
                row_dict = dict(row)
                balance = self._row_to_balance(row_dict)
                self._balances[balance.id] = balance

            # Rehydrate schedules
            rows = _p.fetchall("SELECT * FROM rdo_schedules")
            for row in rows:
                row_dict = dict(row)
                schedule = self._row_to_schedule(row_dict)
                self._schedules[schedule.id] = schedule

            logger.info(
                "Rehydrated %d policies, %d balances, %d schedules",
                len(self._policies),
                len(self._balances),
                len(self._schedules),
            )
        except Exception as e:
            logger.warning("Failed to rehydrate RDO data: %s", e)

    @staticmethod
    def _row_to_policy(row: Dict[str, Any]) -> RDOPolicy:
        """Reconstruct RDOPolicy from DB row."""
        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        types_str = row.get("eligible_employment_types", "FULL_TIME")
        types = [t.strip() for t in types_str.split(",") if t.strip()]

        return RDOPolicy(
            id=row["id"],
            venue_id=row["venue_id"],
            name=row["name"],
            cycle_days=row["cycle_days"],
            accrual_hours_per_day=float(row["accrual_hours_per_day"]),
            rdo_length_hours=float(row["rdo_length_hours"]),
            eligible_employment_types=types,
            min_service_days=row.get("min_service_days", 0),
            is_active=row.get("is_active", True),
            created_at=parse_iso(row.get("created_at")) or datetime.now(timezone.utc),
        )

    @staticmethod
    def _row_to_balance(row: Dict[str, Any]) -> RDOBalance:
        """Reconstruct RDOBalance from DB row."""
        def parse_date(s: Optional[str]) -> Optional[date]:
            if not s:
                return None
            try:
                return date.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        return RDOBalance(
            id=row["id"],
            venue_id=row["venue_id"],
            employee_id=row["employee_id"],
            policy_id=row["policy_id"],
            accrued_hours=float(row["accrued_hours"]),
            taken_hours=float(row["taken_hours"]),
            employment_start_date=parse_date(row.get("employment_start_date")) or date.today(),
            employment_type=row.get("employment_type", "FULL_TIME"),
            last_accrual_date=parse_date(row.get("last_accrual_date")),
            updated_at=parse_iso(row.get("updated_at")) or datetime.now(timezone.utc),
        )

    @staticmethod
    def _row_to_schedule(row: Dict[str, Any]) -> RDOSchedule:
        """Reconstruct RDOSchedule from DB row."""
        def parse_date(s: Optional[str]) -> Optional[date]:
            if not s:
                return None
            try:
                return date.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        return RDOSchedule(
            id=row["id"],
            venue_id=row["venue_id"],
            employee_id=row["employee_id"],
            date=parse_date(row.get("date")) or date.today(),
            hours=float(row.get("hours", 7.6)),
            status=RDOStatus(row.get("status", "scheduled")),
            swap_date=parse_date(row.get("swap_date")),
            approved_by=row.get("approved_by"),
            notes=row.get("notes"),
            created_at=parse_iso(row.get("created_at")) or datetime.now(timezone.utc),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Policy CRUD
    # ─────────────────────────────────────────────────────────────────────

    def create_policy(
        self,
        venue_id: str,
        name: str,
        cycle_days: int,
        accrual_hours_per_day: float,
        rdo_length_hours: float = 7.6,
        eligible_employment_types: Optional[List[str]] = None,
        min_service_days: int = 0,
    ) -> RDOPolicy:
        """Create a new RDO policy."""
        if eligible_employment_types is None:
            eligible_employment_types = ["FULL_TIME"]

        policy = RDOPolicy(
            id=f"policy_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            name=name,
            cycle_days=cycle_days,
            accrual_hours_per_day=accrual_hours_per_day,
            rdo_length_hours=rdo_length_hours,
            eligible_employment_types=eligible_employment_types,
            min_service_days=min_service_days,
        )
        with self._lock:
            self._policies[policy.id] = policy
        self._persist_policy(policy)
        return policy

    def get_policy(self, policy_id: str) -> Optional[RDOPolicy]:
        """Get a policy by ID."""
        with self._lock:
            return self._policies.get(policy_id)

    def list_policies(self, venue_id: str) -> List[RDOPolicy]:
        """List all policies for a venue."""
        with self._lock:
            return [
                p for p in self._policies.values()
                if p.venue_id == venue_id
            ]

    def update_policy(self, policy_id: str, **kwargs) -> Optional[RDOPolicy]:
        """Update a policy. Raises ValueError if not found."""
        with self._lock:
            policy = self._policies.get(policy_id)
            if not policy:
                raise ValueError(f"Policy {policy_id} not found")

            for key, value in kwargs.items():
                if hasattr(policy, key):
                    setattr(policy, key, value)

        self._persist_policy(policy)
        return policy

    # ─────────────────────────────────────────────────────────────────────
    # Balance Management
    # ─────────────────────────────────────────────────────────────────────

    def enrol_employee(
        self,
        venue_id: str,
        employee_id: str,
        policy_id: str,
        employment_start_date: date,
        employment_type: str = "FULL_TIME",
    ) -> RDOBalance:
        """Enrol an employee in an RDO policy."""
        balance = RDOBalance(
            id=f"balance_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            employee_id=employee_id,
            policy_id=policy_id,
            accrued_hours=0.0,
            taken_hours=0.0,
            employment_start_date=employment_start_date,
            employment_type=employment_type,
            last_accrual_date=None,
        )
        with self._lock:
            self._balances[balance.id] = balance
        self._persist_balance(balance)
        return balance

    def get_balance(self, venue_id: str, employee_id: str) -> Optional[RDOBalance]:
        """Get balance for an employee (first match in venue)."""
        with self._lock:
            for balance in self._balances.values():
                if balance.venue_id == venue_id and balance.employee_id == employee_id:
                    return balance
        return None

    def accrue_hours(
        self,
        venue_id: str,
        employee_id: str,
        hours_worked: float,
        work_date: date,
    ) -> Optional[RDOBalance]:
        """Accrue RDO hours for an employee based on hours worked."""
        balance = self.get_balance(venue_id, employee_id)
        if not balance:
            return None

        policy = self.get_policy(balance.policy_id)
        if not policy:
            return None

        # Accrue: hours_worked * policy rate
        accrual = hours_worked * policy.accrual_hours_per_day
        balance.accrued_hours += accrual
        balance.last_accrual_date = work_date
        balance.updated_at = datetime.now(timezone.utc)

        with self._lock:
            self._balances[balance.id] = balance

        self._persist_balance(balance)
        return balance

    def bulk_accrue(
        self,
        venue_id: str,
        work_date: date,
        employee_hours: Dict[str, float],
    ) -> List[RDOBalance]:
        """Accrue hours for multiple employees in one call."""
        results = []
        for employee_id, hours in employee_hours.items():
            balance = self.accrue_hours(venue_id, employee_id, hours, work_date)
            if balance:
                results.append(balance)
        return results

    # ─────────────────────────────────────────────────────────────────────
    # Scheduling
    # ─────────────────────────────────────────────────────────────────────

    def schedule_rdo(
        self,
        venue_id: str,
        employee_id: str,
        date_: date,
        approved_by: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> Optional[RDOSchedule]:
        """Schedule an RDO. Checks balance first."""
        balance = self.get_balance(venue_id, employee_id)
        if not balance:
            raise ValueError(f"No balance found for {employee_id}")

        policy = self.get_policy(balance.policy_id)
        if not policy:
            raise ValueError(f"Policy {balance.policy_id} not found")

        # Check sufficient balance
        if balance.balance_hours < policy.rdo_length_hours:
            raise ValueError(
                f"Insufficient balance ({balance.balance_hours:.1f}h) "
                f"for RDO ({policy.rdo_length_hours}h)"
            )

        schedule = RDOSchedule(
            id=f"schedule_{uuid.uuid4().hex[:12]}",
            venue_id=venue_id,
            employee_id=employee_id,
            date=date_,
            hours=policy.rdo_length_hours,
            status=RDOStatus.SCHEDULED,
            approved_by=approved_by,
            notes=notes,
        )
        with self._lock:
            self._schedules[schedule.id] = schedule
        self._persist_schedule(schedule)
        return schedule

    def take_rdo(self, schedule_id: str) -> Optional[RDOSchedule]:
        """Mark RDO as TAKEN and deduct from balance."""
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if not schedule:
                raise ValueError(f"Schedule {schedule_id} not found")

        balance = self.get_balance(schedule.venue_id, schedule.employee_id)
        if not balance:
            raise ValueError(f"Balance not found for schedule {schedule_id}")

        # Update schedule
        schedule.status = RDOStatus.TAKEN
        with self._lock:
            self._schedules[schedule.id] = schedule

        # Deduct from balance
        balance.taken_hours += schedule.hours
        balance.updated_at = datetime.now(timezone.utc)
        with self._lock:
            self._balances[balance.id] = balance

        self._persist_schedule(schedule)
        self._persist_balance(balance)
        return schedule

    def cancel_rdo(self, schedule_id: str) -> Optional[RDOSchedule]:
        """Cancel an RDO."""
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if not schedule:
                raise ValueError(f"Schedule {schedule_id} not found")

        schedule.status = RDOStatus.CANCELLED
        with self._lock:
            self._schedules[schedule.id] = schedule

        self._persist_schedule(schedule)
        return schedule

    def swap_rdo(self, schedule_id: str, new_date: date) -> Optional[RDOSchedule]:
        """Swap RDO to a new date."""
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if not schedule:
                raise ValueError(f"Schedule {schedule_id} not found")

        schedule.status = RDOStatus.SWAPPED
        schedule.swap_date = new_date
        with self._lock:
            self._schedules[schedule.id] = schedule

        self._persist_schedule(schedule)
        return schedule

    # ─────────────────────────────────────────────────────────────────────
    # Queries
    # ─────────────────────────────────────────────────────────────────────

    def get_schedule(
        self,
        venue_id: str,
        employee_id: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        status: Optional[RDOStatus] = None,
    ) -> List[RDOSchedule]:
        """Get scheduled RDOs with optional filters."""
        with self._lock:
            schedules = [s for s in self._schedules.values() if s.venue_id == venue_id]

            if employee_id:
                schedules = [s for s in schedules if s.employee_id == employee_id]
            if date_from:
                schedules = [s for s in schedules if s.date >= date_from]
            if date_to:
                schedules = [s for s in schedules if s.date <= date_to]
            if status:
                schedules = [s for s in schedules if s.status == status]

            schedules.sort(key=lambda s: s.date)
            return schedules

    def get_upcoming_rdos(self, venue_id: str, days_ahead: int = 28) -> List[RDOSchedule]:
        """Get upcoming RDOs within days_ahead."""
        today = date.today()
        future = today + timedelta(days=days_ahead)
        return self.get_schedule(
            venue_id,
            date_from=today,
            date_to=future,
            status=RDOStatus.SCHEDULED,
        )

    def get_team_rdo_calendar(
        self,
        venue_id: str,
        month: int,
        year: int,
    ) -> Dict[str, Any]:
        """Get RDO calendar for a month."""
        # Calculate date range for month
        if month == 12:
            last_day = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(year, month + 1, 1) - timedelta(days=1)

        first_day = date(year, month, 1)

        schedules = self.get_schedule(
            venue_id,
            date_from=first_day,
            date_to=last_day,
        )

        # Group by employee
        by_employee: Dict[str, List[Dict[str, Any]]] = {}
        for sched in schedules:
            if sched.employee_id not in by_employee:
                by_employee[sched.employee_id] = []
            by_employee[sched.employee_id].append(sched.to_dict())

        return {
            "venue_id": venue_id,
            "month": month,
            "year": year,
            "first_day": first_day.isoformat(),
            "last_day": last_day.isoformat(),
            "by_employee": by_employee,
            "total_scheduled": len(schedules),
        }

    def check_eligibility(self, venue_id: str, employee_id: str) -> Dict[str, Any]:
        """Check if employee is eligible for RDO."""
        balance = self.get_balance(venue_id, employee_id)
        if not balance:
            return {
                "eligible": False,
                "reason": "No RDO enrollment found",
                "employee_id": employee_id,
            }

        policy = self.get_policy(balance.policy_id)
        if not policy:
            return {
                "eligible": False,
                "reason": "Policy not found",
                "employee_id": employee_id,
            }

        # Check employment type
        if balance.employment_type not in policy.eligible_employment_types:
            return {
                "eligible": False,
                "reason": f"Employment type {balance.employment_type} not eligible",
                "employee_id": employee_id,
            }

        # Check min service days
        today = date.today()
        service_days = (today - balance.employment_start_date).days
        if service_days < policy.min_service_days:
            return {
                "eligible": False,
                "reason": f"Minimum service {policy.min_service_days} days not met ({service_days} days)",
                "employee_id": employee_id,
            }

        # Check has balance
        if balance.balance_hours < policy.rdo_length_hours:
            return {
                "eligible": False,
                "reason": f"Insufficient balance ({balance.balance_hours:.1f}h)",
                "employee_id": employee_id,
                "current_balance": balance.balance_hours,
                "required": policy.rdo_length_hours,
            }

        return {
            "eligible": True,
            "employee_id": employee_id,
            "balance_hours": balance.balance_hours,
            "policy_name": policy.name,
        }

    def get_accrual_forecast(
        self,
        venue_id: str,
        employee_id: str,
        days_ahead: int = 28,
    ) -> Dict[str, Any]:
        """Forecast RDO balance over next N days."""
        balance = self.get_balance(venue_id, employee_id)
        if not balance:
            return {}

        policy = self.get_policy(balance.policy_id)
        if not policy:
            return {}

        # Get scheduled RDOs in period
        today = date.today()
        future = today + timedelta(days=days_ahead)
        schedules = self.get_schedule(
            venue_id,
            employee_id=employee_id,
            date_from=today,
            date_to=future,
        )

        taken_in_period = sum(
            s.hours for s in schedules if s.status == RDOStatus.TAKEN
        )
        scheduled_in_period = sum(
            s.hours for s in schedules if s.status == RDOStatus.SCHEDULED
        )

        # Assume 1 RDO per cycle
        expected_accrual = (days_ahead / policy.cycle_days) * policy.rdo_length_hours

        return {
            "employee_id": employee_id,
            "current_balance": balance.balance_hours,
            "days_ahead": days_ahead,
            "taken_in_period": taken_in_period,
            "scheduled_in_period": scheduled_in_period,
            "expected_accrual": expected_accrual,
            "forecast_balance": balance.balance_hours + expected_accrual - scheduled_in_period,
        }


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_rdo_store_singleton: Optional[RDOManagerStore] = None
_singleton_lock = threading.Lock()


def get_rdo_manager_store() -> RDOManagerStore:
    """Get the module-level RDO manager store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _rdo_store_singleton
    if _rdo_store_singleton is None:
        with _singleton_lock:
            if _rdo_store_singleton is None:
                _rdo_store_singleton = RDOManagerStore()
    return _rdo_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _rdo_store_singleton
    with _singleton_lock:
        _rdo_store_singleton = RDOManagerStore.__new__(RDOManagerStore)
        _rdo_store_singleton._lock = threading.Lock()
        _rdo_store_singleton._policies = {}
        _rdo_store_singleton._balances = {}
        _rdo_store_singleton._schedules = {}
