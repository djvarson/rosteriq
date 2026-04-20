"""Leave Management Module for Australian hospitality venues.

Implements Fair Work Act 2009 entitlements:
- Annual Leave: 4 weeks (152 hours) per year for full-time, pro-rata for part-time
- Personal/Carer's Leave: 10 days per year for full-time
- Compassionate Leave: 2 days per occasion
- Community Service Leave: as needed (jury duty etc.)
- Long Service Leave: varies by state (QLD: 8.667 weeks after 10 years)
- Unpaid Leave: by arrangement

Data is persisted to SQLite for auditing and balance tracking.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.leave_management")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class LeaveType(str, Enum):
    """Types of leave available under Fair Work."""
    ANNUAL = "annual"
    PERSONAL_CARER = "personal_carer"
    COMPASSIONATE = "compassionate"
    COMMUNITY_SERVICE = "community_service"
    LONG_SERVICE = "long_service"
    UNPAID = "unpaid"


class LeaveStatus(str, Enum):
    """Leave request lifecycle states."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


@dataclass
class LeaveRequest:
    """A single leave request."""

    request_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    leave_type: LeaveType
    start_date: str  # ISO date (YYYY-MM-DD)
    end_date: str  # ISO date (YYYY-MM-DD)
    hours_requested: float
    reason: str
    status: LeaveStatus = LeaveStatus.PENDING
    decided_by: Optional[str] = None
    decided_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON responses."""
        return {
            "request_id": self.request_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "leave_type": self.leave_type.value,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hours_requested": self.hours_requested,
            "reason": self.reason,
            "status": self.status.value,
            "decided_by": self.decided_by,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
            "created_at": self.created_at.isoformat(),
            "notes": self.notes,
        }


@dataclass
class LeaveBalance:
    """Leave balance for an employee at a venue."""

    employee_id: str
    employee_name: str
    venue_id: str
    leave_type: LeaveType
    accrued_hours: float
    used_hours: float
    pending_hours: float

    @property
    def available_hours(self) -> float:
        """Compute available hours: accrued - used - pending."""
        return self.accrued_hours - self.used_hours - self.pending_hours

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON responses."""
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "leave_type": self.leave_type.value,
            "accrued_hours": self.accrued_hours,
            "used_hours": self.used_hours,
            "pending_hours": self.pending_hours,
            "available_hours": self.available_hours,
        }


@dataclass
class LeaveConflict:
    """Information about conflicts with leave request."""

    request_id: str
    conflicting_shifts: List[Dict[str, Any]]  # [{date, start, end}, ...]
    minimum_staff_warning: bool
    message: str


# ---------------------------------------------------------------------------
# Accrual Calculations
# ---------------------------------------------------------------------------


def calculate_accrual(
    employment_type: str,
    hours_per_week: float,
    tenure_months: float,
    leave_type: LeaveType,
) -> float:
    """Calculate accrued leave hours based on Fair Work entitlements.

    Args:
        employment_type: 'full_time', 'part_time', or 'casual'
        hours_per_week: average hours worked per week
        tenure_months: months of employment
        leave_type: type of leave to calculate

    Returns:
        Accrued hours for this leave type
    """
    if employment_type == "casual":
        # Casuals don't accrue paid leave
        if leave_type in (LeaveType.ANNUAL, LeaveType.PERSONAL_CARER, LeaveType.LONG_SERVICE):
            return 0.0
        return 0.0

    if leave_type == LeaveType.ANNUAL:
        # 4 weeks per year = 152 hours for full-time (38 hrs/week)
        annual_hours = (hours_per_week * 52) * (4 / 52)  # 4 weeks per year
        years = tenure_months / 12.0
        return annual_hours * years

    elif leave_type == LeaveType.PERSONAL_CARER:
        # 10 days per year = 80 hours for full-time (38 hrs/week)
        if employment_type == "full_time":
            annual_hours = 80.0
        else:
            # Pro-rata for part-time
            annual_hours = (hours_per_week * 52) * (10 / 5)  # 10 days ≈ 80 hours at 8h/day
        years = tenure_months / 12.0
        return annual_hours * years

    elif leave_type == LeaveType.COMPASSIONATE:
        # 2 days per occasion (not accruing, per-event) - return 0
        return 0.0

    elif leave_type == LeaveType.COMMUNITY_SERVICE:
        # As needed (not accruing) - return 0
        return 0.0

    elif leave_type == LeaveType.LONG_SERVICE:
        # QLD: 8.667 weeks (≈346.67 hours) after 10 years
        # Then 1.3 weeks per year after that
        if tenure_months < 120:  # Less than 10 years
            return 0.0
        # At 10 years: 8.667 weeks
        # After 10 years: 1.3 weeks per year
        years_over_10 = (tenure_months - 120) / 12.0
        long_service_hours = (hours_per_week * 8.667) + (hours_per_week * 1.3 * years_over_10)
        return long_service_hours

    elif leave_type == LeaveType.UNPAID:
        # Unpaid leave is unlimited by arrangement
        return float("inf")

    return 0.0


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


# Register schema on module load
_LEAVE_REQUESTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS leave_requests (
    request_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    leave_type TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    hours_requested REAL NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    decided_by TEXT,
    decided_at TEXT,
    created_at TEXT NOT NULL,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_leave_employee ON leave_requests(employee_id);
CREATE INDEX IF NOT EXISTS ix_leave_venue ON leave_requests(venue_id);
CREATE INDEX IF NOT EXISTS ix_leave_status ON leave_requests(status);
CREATE INDEX IF NOT EXISTS ix_leave_dates ON leave_requests(start_date, end_date);

CREATE TABLE IF NOT EXISTS leave_balances (
    balance_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    leave_type TEXT NOT NULL,
    accrued_hours REAL NOT NULL DEFAULT 0.0,
    used_hours REAL NOT NULL DEFAULT 0.0,
    pending_hours REAL NOT NULL DEFAULT 0.0,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_balance_unique ON leave_balances(employee_id, venue_id, leave_type);
CREATE INDEX IF NOT EXISTS ix_balance_employee ON leave_balances(employee_id);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("leave_requests", _LEAVE_REQUESTS_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_leave_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# LeaveStore
# ---------------------------------------------------------------------------


class LeaveStore:
    """Thread-safe in-memory store for leave requests and balances with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._requests: Dict[str, LeaveRequest] = {}
        self._balances: Dict[str, LeaveBalance] = {}  # key: f"{emp_id}:{venue_id}:{leave_type}"
        self._lock = threading.Lock()

    def _persist_request(self, request: LeaveRequest) -> None:
        """Persist a leave request to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "request_id": request.request_id,
            "employee_id": request.employee_id,
            "employee_name": request.employee_name,
            "venue_id": request.venue_id,
            "leave_type": request.leave_type.value,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "hours_requested": request.hours_requested,
            "reason": request.reason,
            "status": request.status.value,
            "decided_by": request.decided_by,
            "decided_at": request.decided_at.isoformat() if request.decided_at else None,
            "created_at": request.created_at.isoformat(),
            "notes": request.notes,
        }
        try:
            _p.upsert("leave_requests", row, pk="request_id")
        except Exception as e:
            logger.warning("Failed to persist leave request %s: %s", request.request_id, e)

    def _persist_balance(self, balance: LeaveBalance) -> None:
        """Persist a leave balance to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        balance_id = f"{balance.employee_id}:{balance.venue_id}:{balance.leave_type.value}"
        row = {
            "balance_id": balance_id,
            "employee_id": balance.employee_id,
            "employee_name": balance.employee_name,
            "venue_id": balance.venue_id,
            "leave_type": balance.leave_type.value,
            "accrued_hours": balance.accrued_hours,
            "used_hours": balance.used_hours,
            "pending_hours": balance.pending_hours,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            _p.upsert("leave_balances", row, pk="balance_id")
        except Exception as e:
            logger.warning("Failed to persist leave balance %s: %s", balance_id, e)

    def _rehydrate(self) -> None:
        """Load all requests and balances from SQLite. Called on startup."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            # Rehydrate requests
            rows = _p.fetchall("SELECT * FROM leave_requests")
            for row in rows:
                request = self._row_to_request(dict(row))
                self._requests[request.request_id] = request
            logger.info("Rehydrated %d leave requests from persistence", len(self._requests))

            # Rehydrate balances
            rows = _p.fetchall("SELECT * FROM leave_balances")
            for row in rows:
                balance = self._row_to_balance(dict(row))
                key = f"{balance.employee_id}:{balance.venue_id}:{balance.leave_type.value}"
                self._balances[key] = balance
            logger.info("Rehydrated %d leave balances from persistence", len(self._balances))
        except Exception as e:
            logger.warning("Failed to rehydrate leave data: %s", e)

    @staticmethod
    def _row_to_request(row: Dict[str, Any]) -> LeaveRequest:
        """Reconstruct a LeaveRequest from a DB row."""
        def parse_iso(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(s)
            except (ValueError, TypeError):
                return None

        return LeaveRequest(
            request_id=row["request_id"],
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            venue_id=row["venue_id"],
            leave_type=LeaveType(row["leave_type"]),
            start_date=row["start_date"],
            end_date=row["end_date"],
            hours_requested=row["hours_requested"],
            reason=row["reason"],
            status=LeaveStatus(row.get("status", "pending")),
            decided_by=row.get("decided_by"),
            decided_at=parse_iso(row.get("decided_at")),
            created_at=parse_iso(row.get("created_at")) or datetime.now(timezone.utc),
            notes=row.get("notes"),
        )

    @staticmethod
    def _row_to_balance(row: Dict[str, Any]) -> LeaveBalance:
        """Reconstruct a LeaveBalance from a DB row."""
        return LeaveBalance(
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            venue_id=row["venue_id"],
            leave_type=LeaveType(row["leave_type"]),
            accrued_hours=row["accrued_hours"],
            used_hours=row["used_hours"],
            pending_hours=row["pending_hours"],
        )

    def submit_leave_request(
        self,
        employee_id: str,
        employee_name: str,
        venue_id: str,
        leave_type: LeaveType,
        start_date: str,
        end_date: str,
        hours_requested: float,
        reason: str,
    ) -> LeaveRequest:
        """Submit a new leave request. Must have sufficient balance.

        Raises ValueError if insufficient balance available.
        """
        request_id = f"leave_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)

        # Check balance availability
        balance = self._get_or_create_balance(employee_id, employee_name, venue_id, leave_type)
        if balance.available_hours < hours_requested:
            raise ValueError(
                f"Insufficient {leave_type.value} leave balance. "
                f"Available: {balance.available_hours}, Requested: {hours_requested}"
            )

        request = LeaveRequest(
            request_id=request_id,
            employee_id=employee_id,
            employee_name=employee_name,
            venue_id=venue_id,
            leave_type=leave_type,
            start_date=start_date,
            end_date=end_date,
            hours_requested=hours_requested,
            reason=reason,
            status=LeaveStatus.PENDING,
            created_at=now,
        )

        with self._lock:
            self._requests[request_id] = request
            # Mark hours as pending
            balance.pending_hours += hours_requested

        self._persist_request(request)
        self._persist_balance(balance)
        return request

    def approve_leave(self, request_id: str, decided_by: str) -> LeaveRequest:
        """Approve a leave request. Mark hours as used instead of pending.

        Raises ValueError if request not found or not in PENDING status.
        """
        with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ValueError(f"Leave request {request_id} not found")
            if request.status != LeaveStatus.PENDING:
                raise ValueError(
                    f"Cannot approve request {request_id}: status is {request.status}, "
                    f"must be {LeaveStatus.PENDING}"
                )

            request.status = LeaveStatus.APPROVED
            request.decided_by = decided_by
            request.decided_at = datetime.now(timezone.utc)

            # Move hours from pending to used
            balance = self._get_or_create_balance(
                request.employee_id, request.employee_name, request.venue_id, request.leave_type
            )
            balance.pending_hours -= request.hours_requested
            balance.used_hours += request.hours_requested

        self._persist_request(request)
        self._persist_balance(balance)
        return request

    def reject_leave(self, request_id: str, decided_by: str, reason: str) -> LeaveRequest:
        """Reject a leave request. Release pending hours.

        Raises ValueError if request not found or not in PENDING status.
        """
        with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ValueError(f"Leave request {request_id} not found")
            if request.status != LeaveStatus.PENDING:
                raise ValueError(
                    f"Cannot reject request {request_id}: status is {request.status}, "
                    f"must be {LeaveStatus.PENDING}"
                )

            request.status = LeaveStatus.REJECTED
            request.decided_by = decided_by
            request.decided_at = datetime.now(timezone.utc)
            request.notes = reason

            # Release pending hours
            balance = self._get_or_create_balance(
                request.employee_id, request.employee_name, request.venue_id, request.leave_type
            )
            balance.pending_hours -= request.hours_requested

        self._persist_request(request)
        self._persist_balance(balance)
        return request

    def cancel_leave(self, request_id: str) -> LeaveRequest:
        """Cancel a leave request. If approved, restore used hours.

        Raises ValueError if request not found or already cancelled.
        """
        with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ValueError(f"Leave request {request_id} not found")
            if request.status == LeaveStatus.CANCELLED:
                raise ValueError(
                    f"Leave request {request_id} is already cancelled"
                )

            # Restore hours based on current status
            balance = self._get_or_create_balance(
                request.employee_id, request.employee_name, request.venue_id, request.leave_type
            )
            if request.status == LeaveStatus.APPROVED:
                balance.used_hours -= request.hours_requested
            elif request.status == LeaveStatus.PENDING:
                balance.pending_hours -= request.hours_requested

            request.status = LeaveStatus.CANCELLED

        self._persist_request(request)
        self._persist_balance(balance)
        return request

    def check_conflicts(
        self,
        request: LeaveRequest,
        existing_shifts: List[Dict[str, Any]],
    ) -> Optional[LeaveConflict]:
        """Check if leave request conflicts with existing shifts.

        Args:
            request: Leave request to check
            existing_shifts: List of shift dicts with date, start_time, end_time

        Returns:
            LeaveConflict if conflicts exist, None otherwise
        """
        # Parse leave dates
        from datetime import datetime as dt, date as d

        try:
            start = d.fromisoformat(request.start_date)
            end = d.fromisoformat(request.end_date)
        except (ValueError, TypeError):
            return None

        # Find shifts within leave period
        conflicting = []
        for shift in existing_shifts:
            try:
                shift_date = d.fromisoformat(shift.get("date", ""))
                if start <= shift_date <= end:
                    conflicting.append({
                        "date": shift.get("date"),
                        "start": shift.get("start_time"),
                        "end": shift.get("end_time"),
                    })
            except (ValueError, TypeError, KeyError):
                continue

        if not conflicting:
            return None

        return LeaveConflict(
            request_id=request.request_id,
            conflicting_shifts=conflicting,
            minimum_staff_warning=len(conflicting) > 0,
            message=f"Leave request conflicts with {len(conflicting)} scheduled shift(s)",
        )

    def get_leave_calendar(
        self,
        venue_id: str,
        date_from: str,
        date_to: str,
    ) -> List[Dict[str, Any]]:
        """Get calendar view of who's on leave when.

        Args:
            venue_id: Venue ID to filter by
            date_from: ISO date string (inclusive)
            date_to: ISO date string (inclusive)

        Returns:
            List of dicts with date, employee_id, employee_name, leave_type
        """
        calendar = []
        with self._lock:
            for request in self._requests.values():
                if request.venue_id != venue_id or request.status != LeaveStatus.APPROVED:
                    continue

                # Check if request overlaps with date range
                if request.start_date > date_to or request.end_date < date_from:
                    continue

                # Add entry for each day within the query range
                from datetime import date as d, timedelta
                try:
                    current = d.fromisoformat(request.start_date)
                    end = d.fromisoformat(request.end_date)
                    query_from = d.fromisoformat(date_from)
                    query_to = d.fromisoformat(date_to)
                    while current <= end:
                        # Only add if within query date range
                        if query_from <= current <= query_to:
                            calendar.append({
                                "date": current.isoformat(),
                                "employee_id": request.employee_id,
                                "employee_name": request.employee_name,
                                "leave_type": request.leave_type.value,
                            })
                        current += timedelta(days=1)
                except (ValueError, TypeError):
                    continue

        return calendar

    def get_balances(
        self,
        employee_id: str,
        venue_id: str,
    ) -> List[LeaveBalance]:
        """Get all leave balances for an employee at a venue.

        Returns:
            List of LeaveBalance objects for all leave types
        """
        balances = []
        with self._lock:
            for leave_type in LeaveType:
                key = f"{employee_id}:{venue_id}:{leave_type.value}"
                if key in self._balances:
                    balances.append(self._balances[key])
        return balances

    def _get_or_create_balance(
        self,
        employee_id: str,
        employee_name: str,
        venue_id: str,
        leave_type: LeaveType,
    ) -> LeaveBalance:
        """Get or create a balance record. Must be called with lock held."""
        key = f"{employee_id}:{venue_id}:{leave_type.value}"
        if key not in self._balances:
            self._balances[key] = LeaveBalance(
                employee_id=employee_id,
                employee_name=employee_name,
                venue_id=venue_id,
                leave_type=leave_type,
                accrued_hours=0.0,
                used_hours=0.0,
                pending_hours=0.0,
            )
        return self._balances[key]

    def _list_requests(
        self,
        venue_id: Optional[str] = None,
        employee_id: Optional[str] = None,
        status: Optional[LeaveStatus] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[LeaveRequest]:
        """List leave requests with optional filtering.

        Args:
            venue_id: Filter by venue (optional)
            employee_id: Filter by employee (optional)
            status: Filter by status (optional)
            date_from: Filter by start date >= (optional, ISO string)
            date_to: Filter by end date <= (optional, ISO string)

        Returns:
            List of matching LeaveRequest objects
        """
        results = []
        with self._lock:
            for request in self._requests.values():
                if venue_id and request.venue_id != venue_id:
                    continue
                if employee_id and request.employee_id != employee_id:
                    continue
                if status and request.status != status:
                    continue
                if date_from and request.end_date < date_from:
                    continue
                if date_to and request.start_date > date_to:
                    continue
                results.append(request)
        return results


# ---------------------------------------------------------------------------
# Singleton store
# ---------------------------------------------------------------------------

_store: Optional[LeaveStore] = None
_store_lock = threading.Lock()


def get_leave_store() -> LeaveStore:
    """Get the global leave store singleton."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = LeaveStore()
    return _store


def _reset_for_tests() -> None:
    """Reset the store singleton. For tests only."""
    global _store
    _store = None
