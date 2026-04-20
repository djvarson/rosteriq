"""
Multi-Venue Staff Sharing & Cross-Venue Lending.

Enterprise-tier feature for hospitality portfolio groups.
Allows lending/borrowing of staff between venues in same portfolio.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class TransferStatus(str, Enum):
    """Transfer request lifecycle states."""
    REQUESTED = "requested"
    APPROVED = "approved"
    REJECTED = "rejected"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class TransferRequest:
    """Cross-venue staff transfer request."""
    request_id: str
    employee_id: str
    employee_name: str
    from_venue_id: str
    from_venue_name: str
    to_venue_id: str
    to_venue_name: str
    requested_by: str
    requested_at: datetime
    start_date: date
    end_date: date
    reason: str
    status: TransferStatus = TransferStatus.REQUESTED
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "from_venue_id": self.from_venue_id,
            "from_venue_name": self.from_venue_name,
            "to_venue_id": self.to_venue_id,
            "to_venue_name": self.to_venue_name,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "reason": self.reason,
            "status": self.status.value,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "notes": self.notes,
        }


@dataclass
class StaffHomeVenue:
    """Staff member's home venue and secondary venues."""
    employee_id: str
    employee_name: str
    home_venue_id: str
    home_venue_name: str
    secondary_venues: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "home_venue_id": self.home_venue_id,
            "home_venue_name": self.home_venue_name,
            "secondary_venues": self.secondary_venues,
        }


@dataclass
class CrossVenueAvailability:
    """Staff member's cross-venue work availability."""
    employee_id: str
    employee_name: str
    home_venue_id: str
    available_venues: List[str] = field(default_factory=list)
    roles: List[str] = field(default_factory=list)
    certs: List[str] = field(default_factory=list)
    max_hours_cross_venue: float = 10.0

    def to_dict(self) -> dict:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "home_venue_id": self.home_venue_id,
            "available_venues": self.available_venues,
            "roles": self.roles,
            "certs": self.certs,
            "max_hours_cross_venue": self.max_hours_cross_venue,
        }


@dataclass
class SharingStats:
    """Statistics for staff sharing in a period."""
    venue_id: str
    period_start: date
    period_end: date
    staff_lent_out: int = 0
    staff_borrowed: int = 0
    total_transfer_hours: float = 0.0
    active_transfers: int = 0
    pending_requests: int = 0

    def to_dict(self) -> dict:
        return {
            "venue_id": self.venue_id,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "staff_lent_out": self.staff_lent_out,
            "staff_borrowed": self.staff_borrowed,
            "total_transfer_hours": round(self.total_transfer_hours, 2),
            "active_transfers": self.active_transfers,
            "pending_requests": self.pending_requests,
        }


class StaffSharingStore:
    """Thread-safe staff sharing store with persistence."""

    def __init__(self):
        self._lock = threading.Lock()
        self._transfers: List[TransferRequest] = []
        self._home_venues: Dict[str, StaffHomeVenue] = {}
        self._availability: Dict[str, CrossVenueAvailability] = {}
        self._init_persistence()

    def _init_persistence(self):
        """Initialize persistence tables if enabled."""
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            conn = _p.connection()
            conn.execute("""CREATE TABLE IF NOT EXISTS transfer_requests (
                request_id TEXT PRIMARY KEY, employee_id TEXT, employee_name TEXT,
                from_venue_id TEXT, from_venue_name TEXT, to_venue_id TEXT,
                to_venue_name TEXT, requested_by TEXT, requested_at TEXT,
                start_date TEXT, end_date TEXT, reason TEXT, status TEXT,
                approved_by TEXT, approved_at TEXT, notes TEXT
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS staff_home_venues (
                employee_id TEXT PRIMARY KEY, employee_name TEXT,
                home_venue_id TEXT, home_venue_name TEXT, secondary_venues TEXT
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS cross_venue_availability (
                employee_id TEXT PRIMARY KEY, employee_name TEXT,
                home_venue_id TEXT, available_venues TEXT, roles TEXT,
                certs TEXT, max_hours_cross_venue REAL
            )""")
            conn.commit()
        except Exception:
            pass

    def request_transfer(self, employee_id: str, employee_name: str,
                        from_venue_id: str, from_venue_name: str,
                        to_venue_id: str, to_venue_name: str,
                        requested_by: str, start_date: date, end_date: date,
                        reason: str, notes: str = "") -> TransferRequest:
        """Create a new transfer request."""
        req = TransferRequest(
            request_id=str(uuid.uuid4())[:8],
            employee_id=employee_id,
            employee_name=employee_name,
            from_venue_id=from_venue_id,
            from_venue_name=from_venue_name,
            to_venue_id=to_venue_id,
            to_venue_name=to_venue_name,
            requested_by=requested_by,
            requested_at=datetime.now(timezone.utc),
            start_date=start_date,
            end_date=end_date,
            reason=reason,
            status=TransferStatus.REQUESTED,
            notes=notes,
        )
        with self._lock:
            self._transfers.append(req)
            self._persist_transfer(req)
        return req

    def _persist_transfer(self, req: TransferRequest):
        """Persist transfer request to database."""
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            with _p.write_txn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO transfer_requests VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (req.request_id, req.employee_id, req.employee_name,
                     req.from_venue_id, req.from_venue_name,
                     req.to_venue_id, req.to_venue_name,
                     req.requested_by, req.requested_at.isoformat(),
                     req.start_date.isoformat(), req.end_date.isoformat(),
                     req.reason, req.status.value, req.approved_by,
                     req.approved_at.isoformat() if req.approved_at else None,
                     req.notes))
        except Exception:
            pass

    def approve_transfer(self, request_id: str, approved_by: str) -> TransferRequest:
        """Approve a transfer request (REQUESTED -> APPROVED)."""
        with self._lock:
            req = self._find_transfer(request_id)
            if req is None:
                raise ValueError(f"Transfer request {request_id} not found")
            if req.status != TransferStatus.REQUESTED:
                raise ValueError(
                    f"Cannot approve transfer in status {req.status.value}")
            req.status = TransferStatus.APPROVED
            req.approved_by = approved_by
            req.approved_at = datetime.now(timezone.utc)
            self._persist_transfer(req)
            return req

    def reject_transfer(self, request_id: str) -> TransferRequest:
        """Reject a transfer request (REQUESTED -> REJECTED)."""
        with self._lock:
            req = self._find_transfer(request_id)
            if req is None:
                raise ValueError(f"Transfer request {request_id} not found")
            if req.status != TransferStatus.REQUESTED:
                raise ValueError(
                    f"Cannot reject transfer in status {req.status.value}")
            req.status = TransferStatus.REJECTED
            self._persist_transfer(req)
            return req

    def activate_transfer(self, request_id: str) -> TransferRequest:
        """Activate an approved transfer (APPROVED -> ACTIVE)."""
        with self._lock:
            req = self._find_transfer(request_id)
            if req is None:
                raise ValueError(f"Transfer request {request_id} not found")
            if req.status != TransferStatus.APPROVED:
                raise ValueError(
                    f"Cannot activate transfer in status {req.status.value}")
            req.status = TransferStatus.ACTIVE
            self._persist_transfer(req)
            return req

    def cancel_transfer(self, request_id: str) -> TransferRequest:
        """Cancel a transfer (REQUESTED/APPROVED -> CANCELLED)."""
        with self._lock:
            req = self._find_transfer(request_id)
            if req is None:
                raise ValueError(f"Transfer request {request_id} not found")
            if req.status not in (TransferStatus.REQUESTED, TransferStatus.APPROVED):
                raise ValueError(
                    f"Cannot cancel transfer in status {req.status.value}")
            req.status = TransferStatus.CANCELLED
            self._persist_transfer(req)
            return req

    def complete_transfer(self, request_id: str) -> TransferRequest:
        """Complete an active transfer (ACTIVE -> COMPLETED)."""
        with self._lock:
            req = self._find_transfer(request_id)
            if req is None:
                raise ValueError(f"Transfer request {request_id} not found")
            if req.status != TransferStatus.ACTIVE:
                raise ValueError(
                    f"Cannot complete transfer in status {req.status.value}")
            req.status = TransferStatus.COMPLETED
            self._persist_transfer(req)
            return req

    def register_home_venue(self, employee_id: str, employee_name: str,
                           home_venue_id: str, home_venue_name: str,
                           secondary_venues: Optional[List[str]] = None) -> StaffHomeVenue:
        """Register or update staff member's home venue."""
        with self._lock:
            home = StaffHomeVenue(
                employee_id=employee_id,
                employee_name=employee_name,
                home_venue_id=home_venue_id,
                home_venue_name=home_venue_name,
                secondary_venues=secondary_venues or [],
            )
            self._home_venues[employee_id] = home
            self._persist_home_venue(home)
            return home

    def _persist_home_venue(self, home: StaffHomeVenue):
        """Persist home venue to database."""
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            secondary = ",".join(home.secondary_venues)
            with _p.write_txn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO staff_home_venues VALUES (?,?,?,?,?)",
                    (home.employee_id, home.employee_name, home.home_venue_id,
                     home.home_venue_name, secondary))
        except Exception:
            pass

    def set_cross_venue_availability(self, employee_id: str, employee_name: str,
                                     home_venue_id: str,
                                     available_venues: List[str],
                                     roles: List[str],
                                     certs: List[str],
                                     max_hours: float = 10.0) -> CrossVenueAvailability:
        """Set staff member's cross-venue work availability."""
        avail = CrossVenueAvailability(
            employee_id=employee_id,
            employee_name=employee_name,
            home_venue_id=home_venue_id,
            available_venues=available_venues,
            roles=roles,
            certs=certs,
            max_hours_cross_venue=max_hours,
        )
        with self._lock:
            self._availability[employee_id] = avail
            self._persist_availability(avail)
        return avail

    def _persist_availability(self, avail: CrossVenueAvailability):
        """Persist availability to database."""
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            venues_str = ",".join(avail.available_venues)
            roles_str = ",".join(avail.roles)
            certs_str = ",".join(avail.certs)
            with _p.write_txn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cross_venue_availability VALUES "
                    "(?,?,?,?,?,?,?)",
                    (avail.employee_id, avail.employee_name, avail.home_venue_id,
                     venues_str, roles_str, certs_str, avail.max_hours_cross_venue))
        except Exception:
            pass

    def get_available_for_sharing(self, venue_id: str) -> List[CrossVenueAvailability]:
        """Get all staff available for sharing from a venue."""
        with self._lock:
            return [a for a in self._availability.values()
                    if a.home_venue_id == venue_id and len(a.available_venues) > 1]

    def get_borrowable_staff(self, to_venue_id: str, role: Optional[str] = None) -> List[dict]:
        """Get staff available to borrow into a venue."""
        with self._lock:
            results = []
            for avail in self._availability.values():
                if to_venue_id not in avail.available_venues:
                    continue
                if role and role not in avail.roles:
                    continue
                # Check for active transfers
                active_count = sum(1 for t in self._transfers
                                 if t.employee_id == avail.employee_id
                                 and t.status == TransferStatus.ACTIVE)
                results.append({
                    "employee_id": avail.employee_id,
                    "employee_name": avail.employee_name,
                    "home_venue_id": avail.home_venue_id,
                    "roles": avail.roles,
                    "certs": avail.certs,
                    "max_hours_cross_venue": avail.max_hours_cross_venue,
                    "active_transfers": active_count,
                })
            return results

    def get_sharing_stats(self, venue_id: str, period_start: date,
                         period_end: date) -> SharingStats:
        """Get sharing statistics for a venue in a period."""
        with self._lock:
            transfers = [t for t in self._transfers
                        if ((t.from_venue_id == venue_id or t.to_venue_id == venue_id) and
                            t.start_date <= period_end and t.end_date >= period_start)]

            lent_employees = {t.employee_id for t in transfers
                            if t.from_venue_id == venue_id}
            borrowed_employees = {t.employee_id for t in transfers
                                 if t.to_venue_id == venue_id}

            total_hours = 0.0
            for t in transfers:
                if t.status in (TransferStatus.ACTIVE, TransferStatus.COMPLETED):
                    # Calculate overlap with period
                    start = max(t.start_date, period_start)
                    end = min(t.end_date, period_end)
                    if start <= end:
                        days = (end - start).days + 1
                        total_hours += days * 8.0  # Assume 8 hr shifts

            active_count = sum(1 for t in transfers if t.status == TransferStatus.ACTIVE)
            pending_count = sum(1 for t in transfers if t.status == TransferStatus.REQUESTED)

            return SharingStats(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
                staff_lent_out=len(lent_employees),
                staff_borrowed=len(borrowed_employees),
                total_transfer_hours=total_hours,
                active_transfers=active_count,
                pending_requests=pending_count,
            )

    def get_transfer_request(self, request_id: str) -> Optional[TransferRequest]:
        """Retrieve a transfer request by ID."""
        with self._lock:
            return self._find_transfer(request_id)

    def list_transfer_requests(self, venue_id: Optional[str] = None,
                              status: Optional[TransferStatus] = None,
                              employee_id: Optional[str] = None) -> List[TransferRequest]:
        """List transfer requests with optional filters."""
        with self._lock:
            results = list(self._transfers)
            if venue_id:
                results = [t for t in results
                          if t.from_venue_id == venue_id or t.to_venue_id == venue_id]
            if status:
                results = [t for t in results if t.status == status]
            if employee_id:
                results = [t for t in results if t.employee_id == employee_id]
            return results

    def _find_transfer(self, request_id: str) -> Optional[TransferRequest]:
        """Find transfer by ID (must be called within lock)."""
        for t in self._transfers:
            if t.request_id == request_id:
                return t
        return None


_store: Optional[StaffSharingStore] = None
_store_lock = threading.Lock()


def get_staff_sharing_store() -> StaffSharingStore:
    """Get or create the singleton staff sharing store."""
    global _store
    with _store_lock:
        if _store is None:
            _store = StaffSharingStore()
    return _store


def _reset_for_tests():
    """Reset store for testing (skip persistence reload like tip_pool)."""
    global _store
    with _store_lock:
        s = StaffSharingStore.__new__(StaffSharingStore)
        s._lock = threading.Lock()
        s._transfers = []
        s._home_venues = {}
        s._availability = {}
        _store = s


# --- Public API ---

def request_transfer(employee_id: str, employee_name: str,
                    from_venue_id: str, from_venue_name: str,
                    to_venue_id: str, to_venue_name: str,
                    requested_by: str, start_date: date, end_date: date,
                    reason: str, notes: str = "") -> TransferRequest:
    """Create a new transfer request."""
    return get_staff_sharing_store().request_transfer(
        employee_id, employee_name, from_venue_id, from_venue_name,
        to_venue_id, to_venue_name, requested_by, start_date, end_date, reason, notes)


def approve_transfer(request_id: str, approved_by: str) -> TransferRequest:
    """Approve a transfer request."""
    return get_staff_sharing_store().approve_transfer(request_id, approved_by)


def reject_transfer(request_id: str) -> TransferRequest:
    """Reject a transfer request."""
    return get_staff_sharing_store().reject_transfer(request_id)


def activate_transfer(request_id: str) -> TransferRequest:
    """Activate an approved transfer."""
    return get_staff_sharing_store().activate_transfer(request_id)


def cancel_transfer(request_id: str) -> TransferRequest:
    """Cancel a transfer request."""
    return get_staff_sharing_store().cancel_transfer(request_id)


def complete_transfer(request_id: str) -> TransferRequest:
    """Complete an active transfer."""
    return get_staff_sharing_store().complete_transfer(request_id)


def register_home_venue(employee_id: str, employee_name: str,
                       home_venue_id: str, home_venue_name: str,
                       secondary_venues: Optional[List[str]] = None) -> StaffHomeVenue:
    """Register staff member's home venue."""
    return get_staff_sharing_store().register_home_venue(
        employee_id, employee_name, home_venue_id, home_venue_name, secondary_venues)


def set_cross_venue_availability(employee_id: str, employee_name: str,
                                 home_venue_id: str,
                                 available_venues: List[str],
                                 roles: List[str],
                                 certs: List[str],
                                 max_hours: float = 10.0) -> CrossVenueAvailability:
    """Set staff member's cross-venue availability."""
    return get_staff_sharing_store().set_cross_venue_availability(
        employee_id, employee_name, home_venue_id,
        available_venues, roles, certs, max_hours)


def get_available_for_sharing(venue_id: str) -> List[CrossVenueAvailability]:
    """Get all staff available for sharing from a venue."""
    return get_staff_sharing_store().get_available_for_sharing(venue_id)


def get_borrowable_staff(to_venue_id: str, role: Optional[str] = None) -> List[dict]:
    """Get staff available to borrow into a venue."""
    return get_staff_sharing_store().get_borrowable_staff(to_venue_id, role)


def get_sharing_stats(venue_id: str, period_start: date,
                      period_end: date) -> SharingStats:
    """Get sharing statistics for a venue."""
    return get_staff_sharing_store().get_sharing_stats(venue_id, period_start, period_end)


def get_transfer_request(request_id: str) -> Optional[TransferRequest]:
    """Get a transfer request by ID."""
    return get_staff_sharing_store().get_transfer_request(request_id)


def list_transfer_requests(venue_id: Optional[str] = None,
                          status: Optional[TransferStatus] = None,
                          employee_id: Optional[str] = None) -> List[TransferRequest]:
    """List transfer requests with optional filters."""
    return get_staff_sharing_store().list_transfer_requests(venue_id, status, employee_id)
