"""Staff Certification Tracker for Australian hospitality venues.

Manages legally required certifications for hospitality staff:
- RSA (Responsible Service of Alcohol) — mandatory for serving alcohol
- Food Safety Supervisor — at least one per venue, 5-year validity
- First Aid — 3-year validity
- RSG (Responsible Service of Gambling) — if gaming machines present
- Working with Children — if venue hosts children's events

Data persisted to SQLite for queries and compliance reports.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.certifications")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class CertType(str, Enum):
    """Types of required certifications for hospitality venues."""
    RSA = "rsa"
    FOOD_SAFETY = "food_safety"
    FIRST_AID = "first_aid"
    RSG = "rsg"
    WORKING_WITH_CHILDREN = "working_with_children"
    OTHER = "other"


class CertStatus(str, Enum):
    """Status of a certification."""
    VALID = "valid"
    EXPIRING_SOON = "expiring_soon"  # < 60 days
    EXPIRED = "expired"
    NOT_HELD = "not_held"


@dataclass
class Certification:
    """Record of a staff certification."""
    cert_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    cert_type: CertType
    cert_number: str  # Reference/license number
    issued_date: date
    expiry_date: Optional[date]  # None if no expiry (e.g. VIC RSA)
    state: str  # e.g. "QLD", "NSW", "VIC"
    notes: Optional[str] = None
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: CertStatus = field(init=False)

    def __post_init__(self):
        """Compute status after initialization."""
        self.status = compute_cert_status(self)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cert_id": self.cert_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "cert_type": self.cert_type.value,
            "cert_number": self.cert_number,
            "issued_date": self.issued_date.isoformat(),
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "state": self.state,
            "notes": self.notes,
            "recorded_at": self.recorded_at.isoformat(),
            "status": self.status.value,
        }


@dataclass
class CertAlert:
    """Alert for expiring or expired certifications."""
    alert_id: str
    employee_id: str
    employee_name: str
    cert_type: CertType
    expiry_date: Optional[date]
    days_until_expiry: int
    severity: str  # "WARNING" (60 days), "URGENT" (30 days), "EXPIRED" (0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "cert_type": self.cert_type.value,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "days_until_expiry": self.days_until_expiry,
            "severity": self.severity,
        }


@dataclass
class VenueComplianceStatus:
    """Venue-level certification compliance status."""
    venue_id: str
    total_staff: int  # Total staff members
    certs_valid: int  # Staff with valid certs
    certs_expiring: int  # Staff with certs expiring soon
    certs_expired: int  # Staff with expired certs
    certs_missing: int  # Staff with missing required certs
    food_safety_covered: bool  # At least one valid food safety supervisor
    compliance_pct: float  # 0-100, % of staff with valid certs
    alerts: List[CertAlert] = field(default_factory=list)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "total_staff": self.total_staff,
            "certs_valid": self.certs_valid,
            "certs_expiring": self.certs_expiring,
            "certs_expired": self.certs_expired,
            "certs_missing": self.certs_missing,
            "food_safety_covered": self.food_safety_covered,
            "compliance_pct": round(self.compliance_pct, 2),
            "alerts": [a.to_dict() for a in self.alerts],
            "checked_at": self.checked_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Core certification logic
# ---------------------------------------------------------------------------


def compute_cert_status(cert: Certification) -> CertStatus:
    """Compute current status of a certification.

    Args:
        cert: Certification to check

    Returns:
        CertStatus (VALID, EXPIRING_SOON, EXPIRED, or NOT_HELD)
    """
    if cert.expiry_date is None:
        # No expiry date (e.g. VIC RSA) — assume valid
        return CertStatus.VALID

    today = date.today()
    days_until = (cert.expiry_date - today).days

    if days_until < 0:
        return CertStatus.EXPIRED
    elif days_until < 60:
        return CertStatus.EXPIRING_SOON
    else:
        return CertStatus.VALID


def check_expiry_alerts(
    certs: List[Certification],
    warning_days: int = 60,
    urgent_days: int = 30,
) -> List[CertAlert]:
    """Scan certifications for upcoming/past expiries.

    Args:
        certs: List of certifications to check
        warning_days: Days threshold for WARNING severity
        urgent_days: Days threshold for URGENT severity

    Returns:
        List of CertAlert objects
    """
    alerts = []
    today = date.today()

    for cert in certs:
        if cert.expiry_date is None:
            continue

        days_until = (cert.expiry_date - today).days

        severity = None
        if days_until < 0:
            severity = "EXPIRED"
        elif days_until < urgent_days:
            severity = "URGENT"
        elif days_until < warning_days:
            severity = "WARNING"

        if severity:
            alert = CertAlert(
                alert_id=f"alert_{uuid.uuid4().hex[:12]}",
                employee_id=cert.employee_id,
                employee_name=cert.employee_name,
                cert_type=cert.cert_type,
                expiry_date=cert.expiry_date,
                days_until_expiry=days_until,
                severity=severity,
            )
            alerts.append(alert)

    # Sort by urgency (expired, then urgent, then warning), then by days
    severity_order = {"EXPIRED": 0, "URGENT": 1, "WARNING": 2}
    alerts.sort(
        key=lambda a: (severity_order.get(a.severity, 3), a.days_until_expiry)
    )

    return alerts


def check_venue_compliance(
    venue_id: str,
    certs: List[Certification],
    staff_ids: List[str],
) -> VenueComplianceStatus:
    """Check venue-level certification compliance.

    Args:
        venue_id: Venue identifier
        certs: All certifications for this venue
        staff_ids: List of all staff member IDs at venue

    Returns:
        VenueComplianceStatus with compliance metrics
    """
    total_staff = len(staff_ids)
    staff_with_valid_certs = set()
    staff_with_expiring_certs = set()
    staff_with_expired_certs = set()
    food_safety_supervisors = 0

    # Index certs by employee
    certs_by_employee: Dict[str, List[Certification]] = {}
    for cert in certs:
        if cert.employee_id not in certs_by_employee:
            certs_by_employee[cert.employee_id] = []
        certs_by_employee[cert.employee_id].append(cert)

    # Check each staff member
    for staff_id in staff_ids:
        staff_certs = certs_by_employee.get(staff_id, [])

        if not staff_certs:
            # No certs at all
            continue

        # Check if any certs are valid (not expired)
        has_valid = any(c.status == CertStatus.VALID for c in staff_certs)
        has_expiring = any(c.status == CertStatus.EXPIRING_SOON for c in staff_certs)
        has_expired = any(c.status == CertStatus.EXPIRED for c in staff_certs)

        if has_valid:
            staff_with_valid_certs.add(staff_id)
        elif has_expiring:
            staff_with_expiring_certs.add(staff_id)
        elif has_expired:
            staff_with_expired_certs.add(staff_id)

        # Check for food safety supervisors
        food_safety_certs = [
            c for c in staff_certs if c.cert_type == CertType.FOOD_SAFETY
        ]
        if any(c.status == CertStatus.VALID for c in food_safety_certs):
            food_safety_supervisors += 1

    certs_valid = len(staff_with_valid_certs)
    certs_expiring = len(staff_with_expiring_certs)
    certs_expired = len(staff_with_expired_certs)
    certs_missing = total_staff - certs_valid - certs_expiring - certs_expired

    compliance_pct = (certs_valid / total_staff * 100) if total_staff > 0 else 0.0

    # Generate alerts
    alerts = check_expiry_alerts(certs)

    return VenueComplianceStatus(
        venue_id=venue_id,
        total_staff=total_staff,
        certs_valid=certs_valid,
        certs_expiring=certs_expiring,
        certs_expired=certs_expired,
        certs_missing=certs_missing,
        food_safety_covered=food_safety_supervisors > 0,
        compliance_pct=compliance_pct,
        alerts=alerts,
    )


def get_missing_certs(
    employee_id: str,
    employee_certs: List[Certification],
    required_types: List[CertType],
) -> List[CertType]:
    """Get list of required certifications employee doesn't have.

    Args:
        employee_id: Employee identifier
        employee_certs: List of their certifications
        required_types: List of required cert types

    Returns:
        List of missing CertType values
    """
    held_types = {c.cert_type for c in employee_certs}
    missing = [t for t in required_types if t not in held_types]
    return missing


def get_expiry_calendar(
    certs: List[Certification],
    days_ahead: int = 90,
) -> List[Dict[str, Any]]:
    """Get upcoming expirations sorted by date.

    Args:
        certs: List of certifications
        days_ahead: Look ahead this many days (default 90)

    Returns:
        List of dicts with employee, cert_type, expiry_date, days_remaining
    """
    today = date.today()
    future = today + timedelta(days=days_ahead)

    upcoming = []
    for cert in certs:
        if cert.expiry_date is None:
            continue

        if today <= cert.expiry_date <= future:
            days_remaining = (cert.expiry_date - today).days
            upcoming.append({
                "employee_id": cert.employee_id,
                "employee_name": cert.employee_name,
                "cert_type": cert.cert_type.value,
                "expiry_date": cert.expiry_date.isoformat(),
                "days_remaining": days_remaining,
            })

    # Sort by expiry date (soonest first)
    upcoming.sort(key=lambda x: x["expiry_date"])
    return upcoming


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


_CERTIFICATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS certifications (
    cert_id TEXT PRIMARY KEY,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    cert_type TEXT NOT NULL,
    cert_number TEXT NOT NULL,
    issued_date TEXT NOT NULL,
    expiry_date TEXT,
    state TEXT NOT NULL,
    notes TEXT,
    recorded_at TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_cert_employee ON certifications(employee_id);
CREATE INDEX IF NOT EXISTS ix_cert_venue ON certifications(venue_id);
CREATE INDEX IF NOT EXISTS ix_cert_type ON certifications(cert_type);
CREATE INDEX IF NOT EXISTS ix_cert_status ON certifications(status);
CREATE INDEX IF NOT EXISTS ix_cert_expiry ON certifications(expiry_date);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("certifications", _CERTIFICATIONS_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_certification_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Certification Store
# ---------------------------------------------------------------------------


class CertificationStore:
    """Thread-safe in-memory store for certifications with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._certs: Dict[str, Certification] = {}
        self._lock = threading.Lock()

    def _persist(self, cert: Certification) -> None:
        """Persist a certification to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "cert_id": cert.cert_id,
            "employee_id": cert.employee_id,
            "employee_name": cert.employee_name,
            "venue_id": cert.venue_id,
            "cert_type": cert.cert_type.value,
            "cert_number": cert.cert_number,
            "issued_date": cert.issued_date.isoformat(),
            "expiry_date": cert.expiry_date.isoformat() if cert.expiry_date else None,
            "state": cert.state,
            "notes": cert.notes,
            "recorded_at": cert.recorded_at.isoformat(),
            "status": cert.status.value,
        }
        try:
            _p.upsert("certifications", row, pk="cert_id")
        except Exception as e:
            logger.warning("Failed to persist certification %s: %s", cert.cert_id, e)

    def _rehydrate(self) -> None:
        """Load all certifications from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            rows = _p.fetchall("SELECT * FROM certifications")
            for row in rows:
                cert = self._row_to_cert(dict(row))
                self._certs[cert.cert_id] = cert
            logger.info("Rehydrated %d certifications from persistence", len(self._certs))
        except Exception as e:
            logger.warning("Failed to rehydrate certifications: %s", e)

    @staticmethod
    def _row_to_cert(row: Dict[str, Any]) -> Certification:
        """Reconstruct a Certification from a DB row."""
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

        return Certification(
            cert_id=row["cert_id"],
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            venue_id=row["venue_id"],
            cert_type=CertType(row.get("cert_type", "other")),
            cert_number=row["cert_number"],
            issued_date=parse_date(row.get("issued_date")) or date.today(),
            expiry_date=parse_date(row.get("expiry_date")),
            state=row.get("state", ""),
            notes=row.get("notes"),
            recorded_at=parse_iso(row.get("recorded_at")) or datetime.now(timezone.utc),
        )

    def add(self, cert: Certification) -> Certification:
        """Add a new certification."""
        with self._lock:
            self._certs[cert.cert_id] = cert
        self._persist(cert)
        return cert

    def update(self, cert_id: str, **kwargs) -> Certification:
        """Update a certification.

        Raises ValueError if not found.
        """
        with self._lock:
            cert = self._certs.get(cert_id)
            if not cert:
                raise ValueError(f"Certification {cert_id} not found")

            # Update fields
            for key, value in kwargs.items():
                if hasattr(cert, key):
                    setattr(cert, key, value)

            # Recompute status
            cert.status = compute_cert_status(cert)

        self._persist(cert)
        return cert

    def delete(self, cert_id: str) -> None:
        """Delete a certification.

        Raises ValueError if not found.
        """
        with self._lock:
            if cert_id not in self._certs:
                raise ValueError(f"Certification {cert_id} not found")
            del self._certs[cert_id]

        _p = _get_persistence()
        if _p and _p.is_persistence_enabled():
            try:
                _p.execute("DELETE FROM certifications WHERE cert_id = ?", (cert_id,))
            except Exception as e:
                logger.warning("Failed to delete certification %s: %s", cert_id, e)

    def get(self, cert_id: str) -> Optional[Certification]:
        """Get a certification by ID. Returns None if not found."""
        with self._lock:
            return self._certs.get(cert_id)

    def list_by_venue(
        self,
        venue_id: str,
        employee_id: Optional[str] = None,
        cert_type: Optional[CertType] = None,
        status: Optional[CertStatus] = None,
    ) -> List[Certification]:
        """List certifications for a venue with optional filters.

        Returns sorted by employee name, then cert type.
        """
        with self._lock:
            certs = [c for c in self._certs.values() if c.venue_id == venue_id]

            if employee_id:
                certs = [c for c in certs if c.employee_id == employee_id]
            if cert_type:
                certs = [c for c in certs if c.cert_type == cert_type]
            if status:
                certs = [c for c in certs if c.status == status]

            # Sort by employee name, then cert type
            certs.sort(
                key=lambda c: (c.employee_name, c.cert_type.value)
            )
            return certs

    def list_by_employee(
        self,
        employee_id: str,
    ) -> List[Certification]:
        """List all certifications for an employee across all venues."""
        with self._lock:
            certs = [c for c in self._certs.values() if c.employee_id == employee_id]
            certs.sort(key=lambda c: (c.venue_id, c.cert_type.value))
            return certs

    def list_all(self) -> List[Certification]:
        """List all certifications."""
        with self._lock:
            return list(self._certs.values())


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_cert_store_singleton: Optional[CertificationStore] = None
_singleton_lock = threading.Lock()


def get_certification_store() -> CertificationStore:
    """Get the module-level certification store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _cert_store_singleton
    if _cert_store_singleton is None:
        with _singleton_lock:
            if _cert_store_singleton is None:
                _cert_store_singleton = CertificationStore()
    return _cert_store_singleton


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _cert_store_singleton
    _cert_store_singleton = None
