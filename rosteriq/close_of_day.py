"""
Close-of-Day (CoD) Reconciliation for RosterIQ — Round 38.

Australian hospitality venues need to reconcile POS revenue with physical cash
counts at the end of each trading day. This module provides:

- Till variance tracking (expected vs counted)
- Revenue breakdown by payment method
- Labour cost % analysis
- Discrepancy flagging and trends
- Manager sign-off workflow

Data is persisted to SQLite via rosteriq.persistence.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timezone
from enum import Enum
from typing import List, Optional, Dict, Any

from rosteriq.persistence import (
    register_schema, connection, write_txn, is_persistence_enabled,
    json_dumps, json_loads, now_iso
)

logger = logging.getLogger("rosteriq.close_of_day")


# ============================================================================
# Enums
# ============================================================================

class PaymentMethod(str, Enum):
    """Payment method categories."""
    CASH = "cash"
    CARD = "card"
    EFTPOS = "eftpos"
    ONLINE = "online"
    VOUCHER = "voucher"
    OTHER = "other"


class TillStatus(str, Enum):
    """Till reconciliation status."""
    BALANCED = "balanced"
    OVER = "over"
    SHORT = "short"
    UNRECONCILED = "unreconciled"


class SignOffStatus(str, Enum):
    """Manager sign-off workflow status."""
    PENDING = "pending"
    SIGNED_OFF = "signed_off"
    QUERIED = "queried"
    REOPENED = "reopened"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class TillCount:
    """Physical till count at close of day."""
    till_id: str
    counted_amount: float
    expected_amount: float
    counted_by: str  # employee_id
    counted_at: datetime
    notes: str = ""
    status: TillStatus = TillStatus.UNRECONCILED

    @property
    def variance(self) -> float:
        """Variance = counted - expected (positive = over, negative = short)."""
        return round(self.counted_amount - self.expected_amount, 2)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON storage."""
        return {
            "till_id": self.till_id,
            "counted_amount": self.counted_amount,
            "expected_amount": self.expected_amount,
            "counted_by": self.counted_by,
            "counted_at": self.counted_at.isoformat() if isinstance(self.counted_at, datetime) else self.counted_at,
            "notes": self.notes,
            "status": self.status.value if isinstance(self.status, Enum) else self.status,
            "variance": self.variance,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TillCount:
        """Deserialize from dict."""
        if isinstance(d.get("counted_at"), str):
            counted_at = datetime.fromisoformat(d["counted_at"])
        else:
            counted_at = d.get("counted_at")

        status_val = d.get("status", "unreconciled")
        if isinstance(status_val, str):
            status = TillStatus(status_val)
        else:
            status = status_val

        return cls(
            till_id=d["till_id"],
            counted_amount=d["counted_amount"],
            expected_amount=d["expected_amount"],
            counted_by=d["counted_by"],
            counted_at=counted_at,
            notes=d.get("notes", ""),
            status=status,
        )


@dataclass
class RevenueBreakdown:
    """Revenue by payment method."""
    payment_method: PaymentMethod
    amount: float
    transaction_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict."""
        return {
            "payment_method": self.payment_method.value if isinstance(self.payment_method, Enum) else self.payment_method,
            "amount": self.amount,
            "transaction_count": self.transaction_count,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RevenueBreakdown:
        """Deserialize from dict."""
        pm = d["payment_method"]
        if isinstance(pm, str):
            pm = PaymentMethod(pm)
        return cls(
            payment_method=pm,
            amount=d["amount"],
            transaction_count=d.get("transaction_count", 0),
        )


@dataclass
class CloseOfDay:
    """Complete close-of-day record."""
    cod_id: str
    venue_id: str
    trading_date: date
    closed_by: str  # employee_id
    closed_by_name: str
    closed_at: datetime
    pos_total: float
    till_counts: List[TillCount]
    revenue_breakdown: List[RevenueBreakdown]
    total_revenue: float = 0.0
    total_variance: float = 0.0
    labour_cost: float = 0.0
    labour_pct: float = 0.0
    covers: int = 0
    average_spend: float = 0.0
    sign_off_status: SignOffStatus = SignOffStatus.PENDING
    signed_off_by: Optional[str] = None
    signed_off_at: Optional[datetime] = None
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON storage."""
        return {
            "cod_id": self.cod_id,
            "venue_id": self.venue_id,
            "trading_date": self.trading_date.isoformat() if isinstance(self.trading_date, date) else self.trading_date,
            "closed_by": self.closed_by,
            "closed_by_name": self.closed_by_name,
            "closed_at": self.closed_at.isoformat() if isinstance(self.closed_at, datetime) else self.closed_at,
            "pos_total": self.pos_total,
            "till_counts": [t.to_dict() for t in self.till_counts],
            "revenue_breakdown": [r.to_dict() for r in self.revenue_breakdown],
            "total_revenue": self.total_revenue,
            "total_variance": self.total_variance,
            "labour_cost": self.labour_cost,
            "labour_pct": self.labour_pct,
            "covers": self.covers,
            "average_spend": self.average_spend,
            "sign_off_status": self.sign_off_status.value if isinstance(self.sign_off_status, Enum) else self.sign_off_status,
            "signed_off_by": self.signed_off_by,
            "signed_off_at": self.signed_off_at.isoformat() if isinstance(self.signed_off_at, datetime) else self.signed_off_at,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CloseOfDay:
        """Deserialize from dict."""
        trading_date = d["trading_date"]
        if isinstance(trading_date, str):
            trading_date = date.fromisoformat(trading_date)

        closed_at = d["closed_at"]
        if isinstance(closed_at, str):
            closed_at = datetime.fromisoformat(closed_at)

        signed_off_at = d.get("signed_off_at")
        if isinstance(signed_off_at, str):
            signed_off_at = datetime.fromisoformat(signed_off_at)

        sign_off_status = d.get("sign_off_status", "pending")
        if isinstance(sign_off_status, str):
            sign_off_status = SignOffStatus(sign_off_status)

        till_counts = [TillCount.from_dict(t) for t in d.get("till_counts", [])]
        revenue_breakdown = [RevenueBreakdown.from_dict(r) for r in d.get("revenue_breakdown", [])]

        return cls(
            cod_id=d["cod_id"],
            venue_id=d["venue_id"],
            trading_date=trading_date,
            closed_by=d["closed_by"],
            closed_by_name=d["closed_by_name"],
            closed_at=closed_at,
            pos_total=d["pos_total"],
            till_counts=till_counts,
            revenue_breakdown=revenue_breakdown,
            total_revenue=d.get("total_revenue", 0.0),
            total_variance=d.get("total_variance", 0.0),
            labour_cost=d.get("labour_cost", 0.0),
            labour_pct=d.get("labour_pct", 0.0),
            covers=d.get("covers", 0),
            average_spend=d.get("average_spend", 0.0),
            sign_off_status=sign_off_status,
            signed_off_by=d.get("signed_off_by"),
            signed_off_at=signed_off_at,
            notes=d.get("notes", ""),
        )


@dataclass
class CoDSummary:
    """Period-level summary of close-of-day records."""
    venue_id: str
    period_start: date
    period_end: date
    trading_days: int
    total_revenue: float
    avg_daily_revenue: float
    total_variance: float
    variance_pct: float
    avg_labour_pct: float
    total_covers: int
    avg_spend: float
    days_with_discrepancies: int
    best_day: Optional[Dict[str, Any]] = None
    worst_day: Optional[Dict[str, Any]] = None


# ============================================================================
# SQLite Store
# ============================================================================

_CLOSE_OF_DAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS close_of_day (
    cod_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    trading_date DATE NOT NULL,
    closed_by TEXT NOT NULL,
    closed_by_name TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    pos_total REAL NOT NULL,
    till_counts TEXT NOT NULL,
    revenue_breakdown TEXT NOT NULL,
    total_revenue REAL NOT NULL,
    total_variance REAL NOT NULL,
    labour_cost REAL NOT NULL,
    labour_pct REAL NOT NULL,
    covers INTEGER NOT NULL,
    average_spend REAL NOT NULL,
    sign_off_status TEXT NOT NULL,
    signed_off_by TEXT,
    signed_off_at TEXT,
    notes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cod_venue ON close_of_day(venue_id);
CREATE INDEX IF NOT EXISTS idx_cod_date ON close_of_day(trading_date);
CREATE INDEX IF NOT EXISTS idx_cod_status ON close_of_day(sign_off_status);
"""

register_schema("close_of_day", _CLOSE_OF_DAY_SCHEMA)


class CloseOfDayStore:
    """Thread-safe SQLite-backed store for CoD records."""

    def __init__(self):
        self._lock = threading.Lock()

    def create(self, cod: CloseOfDay) -> CloseOfDay:
        """Insert or update a CoD record."""
        if not is_persistence_enabled():
            return cod

        with write_txn() as c:
            c.execute("""
                INSERT OR REPLACE INTO close_of_day (
                    cod_id, venue_id, trading_date, closed_by, closed_by_name,
                    closed_at, pos_total, till_counts, revenue_breakdown,
                    total_revenue, total_variance, labour_cost, labour_pct,
                    covers, average_spend, sign_off_status, signed_off_by,
                    signed_off_at, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cod.cod_id, cod.venue_id, cod.trading_date.isoformat(),
                cod.closed_by, cod.closed_by_name, cod.closed_at.isoformat(),
                cod.pos_total, json_dumps([t.to_dict() for t in cod.till_counts]),
                json_dumps([r.to_dict() for r in cod.revenue_breakdown]),
                cod.total_revenue, cod.total_variance, cod.labour_cost, cod.labour_pct,
                cod.covers, cod.average_spend, cod.sign_off_status.value,
                cod.signed_off_by, cod.signed_off_at.isoformat() if cod.signed_off_at else None,
                cod.notes, now_iso(), now_iso()
            ))
        return cod

    def get_by_id(self, cod_id: str) -> Optional[CloseOfDay]:
        """Retrieve a single CoD record by ID."""
        if not is_persistence_enabled():
            return None

        c = connection()
        row = c.execute(
            "SELECT * FROM close_of_day WHERE cod_id = ?", (cod_id,)
        ).fetchone()
        if not row:
            return None
        return self._deserialize(dict(row))

    def get_by_venue_and_date_range(
        self, venue_id: str, date_from: date, date_to: date,
        status_filter: Optional[str] = None
    ) -> List[CloseOfDay]:
        """Retrieve all CoD records for a venue in a date range, optionally filtered by status."""
        if not is_persistence_enabled():
            return []

        c = connection()
        query = """
            SELECT * FROM close_of_day
            WHERE venue_id = ? AND trading_date >= ? AND trading_date <= ?
        """
        params = [venue_id, date_from.isoformat(), date_to.isoformat()]

        if status_filter:
            query += " AND sign_off_status = ?"
            params.append(status_filter)

        query += " ORDER BY trading_date DESC"

        rows = c.execute(query, params).fetchall()
        return [self._deserialize(dict(row)) for row in rows]

    def update_sign_off(self, cod_id: str, signed_off_by: str, status: SignOffStatus) -> Optional[CloseOfDay]:
        """Update sign-off status and signer."""
        if not is_persistence_enabled():
            return None

        with write_txn() as c:
            c.execute("""
                UPDATE close_of_day
                SET sign_off_status = ?, signed_off_by = ?, signed_off_at = ?, updated_at = ?
                WHERE cod_id = ?
            """, (status.value, signed_off_by, now_iso(), now_iso(), cod_id))

        return self.get_by_id(cod_id)

    def _deserialize(self, row: Dict[str, Any]) -> CloseOfDay:
        """Convert a database row to a CloseOfDay object."""
        till_counts = [
            TillCount.from_dict(t) for t in json_loads(row["till_counts"])
        ]
        revenue_breakdown = [
            RevenueBreakdown.from_dict(r) for r in json_loads(row["revenue_breakdown"])
        ]

        trading_date = row["trading_date"]
        if isinstance(trading_date, str):
            trading_date = date.fromisoformat(trading_date)

        closed_at = row["closed_at"]
        if isinstance(closed_at, str):
            closed_at = datetime.fromisoformat(closed_at)

        signed_off_at = row["signed_off_at"]
        if isinstance(signed_off_at, str):
            signed_off_at = datetime.fromisoformat(signed_off_at)

        return CloseOfDay(
            cod_id=row["cod_id"],
            venue_id=row["venue_id"],
            trading_date=trading_date,
            closed_by=row["closed_by"],
            closed_by_name=row["closed_by_name"],
            closed_at=closed_at,
            pos_total=row["pos_total"],
            till_counts=till_counts,
            revenue_breakdown=revenue_breakdown,
            total_revenue=row["total_revenue"],
            total_variance=row["total_variance"],
            labour_cost=row["labour_cost"],
            labour_pct=row["labour_pct"],
            covers=row["covers"],
            average_spend=row["average_spend"],
            sign_off_status=SignOffStatus(row["sign_off_status"]),
            signed_off_by=row["signed_off_by"],
            signed_off_at=signed_off_at,
            notes=row["notes"],
        )


_store_instance = None
_store_lock = threading.Lock()


def get_store() -> CloseOfDayStore:
    """Return the singleton CloseOfDayStore instance."""
    global _store_instance
    if _store_instance is None:
        with _store_lock:
            if _store_instance is None:
                _store_instance = CloseOfDayStore()
    return _store_instance


# ============================================================================
# Core Functions
# ============================================================================

def create_close_of_day(
    venue_id: str,
    trading_date: date,
    closed_by: str,
    closed_by_name: str,
    pos_total: float,
    till_counts: List[TillCount],
    revenue_breakdown: List[RevenueBreakdown],
    labour_cost: float = 0.0,
    covers: int = 0,
    notes: str = "",
) -> CloseOfDay:
    """
    Create and persist a close-of-day record.

    Args:
        venue_id: Venue identifier
        trading_date: Date of trading (YYYY-MM-DD)
        closed_by: Employee ID of person closing
        closed_by_name: Full name of closer
        pos_total: Total POS revenue
        till_counts: List of physical till counts
        revenue_breakdown: List of revenue by payment method
        labour_cost: Labour cost for the day (optional)
        covers: Number of customer covers served
        notes: Additional notes

    Returns:
        CloseOfDay record (persisted to SQLite)
    """
    # Calculate totals
    total_variance = calculate_till_variance(till_counts)
    total_revenue = sum(r.amount for r in revenue_breakdown)
    labour_pct = (labour_cost / total_revenue * 100) if total_revenue > 0 else 0.0
    average_spend = (total_revenue / covers) if covers > 0 else 0.0

    # Classify each till
    for till in till_counts:
        till.status = classify_till(till.expected_amount, till.counted_amount)

    cod = CloseOfDay(
        cod_id=str(uuid.uuid4()),
        venue_id=venue_id,
        trading_date=trading_date,
        closed_by=closed_by,
        closed_by_name=closed_by_name,
        closed_at=datetime.now(timezone.utc),
        pos_total=pos_total,
        till_counts=till_counts,
        revenue_breakdown=revenue_breakdown,
        total_revenue=total_revenue,
        total_variance=total_variance,
        labour_cost=labour_cost,
        labour_pct=round(labour_pct, 2),
        covers=covers,
        average_spend=round(average_spend, 2),
        notes=notes,
    )

    return get_store().create(cod)


def calculate_till_variance(till_counts: List[TillCount]) -> float:
    """Sum all till variances (positive = over, negative = short)."""
    return round(sum(t.variance for t in till_counts), 2)


def classify_till(
    expected_amount: float,
    counted_amount: float,
    tolerance: float = 5.0,
) -> TillStatus:
    """
    Classify till as BALANCED, OVER, or SHORT based on tolerance.

    Args:
        expected_amount: Expected amount from POS
        counted_amount: Physical count
        tolerance: Tolerance in dollars (default $5)

    Returns:
        TillStatus classification
    """
    variance = counted_amount - expected_amount
    if abs(variance) <= tolerance:
        return TillStatus.BALANCED
    elif variance > 0:
        return TillStatus.OVER
    else:
        return TillStatus.SHORT


def sign_off(cod_id: str, signed_off_by: str) -> Optional[CloseOfDay]:
    """
    Manager sign-off on a close-of-day record.

    Args:
        cod_id: Close-of-day record ID
        signed_off_by: Employee ID of manager

    Returns:
        Updated CloseOfDay record
    """
    return get_store().update_sign_off(cod_id, signed_off_by, SignOffStatus.SIGNED_OFF)


def query_cod(cod_id: str, queried_by: str) -> Optional[CloseOfDay]:
    """
    Reopen a close-of-day record for investigation (manager queries the record).

    Args:
        cod_id: Close-of-day record ID
        queried_by: Employee ID of manager querying

    Returns:
        Updated CloseOfDay record with status QUERIED
    """
    return get_store().update_sign_off(cod_id, queried_by, SignOffStatus.QUERIED)


def build_cod_summary(
    venue_id: str,
    records: List[CloseOfDay],
    period_start: date,
    period_end: date,
) -> CoDSummary:
    """
    Build a period summary from CoD records.

    Args:
        venue_id: Venue identifier
        records: List of CloseOfDay records for the period
        period_start: Start date
        period_end: End date

    Returns:
        CoDSummary with aggregated metrics
    """
    if not records:
        return CoDSummary(
            venue_id=venue_id,
            period_start=period_start,
            period_end=period_end,
            trading_days=0,
            total_revenue=0.0,
            avg_daily_revenue=0.0,
            total_variance=0.0,
            variance_pct=0.0,
            avg_labour_pct=0.0,
            total_covers=0,
            avg_spend=0.0,
            days_with_discrepancies=0,
        )

    total_revenue = sum(r.total_revenue for r in records)
    total_variance = sum(r.total_variance for r in records)
    total_labour_cost = sum(r.labour_cost for r in records)
    total_covers = sum(r.covers for r in records)
    days_with_discrepancies = sum(
        1 for r in records
        if any(t.status != TillStatus.BALANCED for t in r.till_counts)
    )

    avg_daily_revenue = total_revenue / len(records) if records else 0.0
    variance_pct = (
        (abs(total_variance) / total_revenue * 100)
        if total_revenue > 0
        else 0.0
    )
    avg_labour_pct = (
        (total_labour_cost / total_revenue * 100)
        if total_revenue > 0
        else 0.0
    )
    avg_spend = (total_revenue / total_covers) if total_covers > 0 else 0.0

    # Best and worst days
    best_day = None
    worst_day = None
    if records:
        sorted_by_revenue = sorted(records, key=lambda r: r.total_revenue)
        worst_day = {
            "date": sorted_by_revenue[0].trading_date.isoformat(),
            "revenue": sorted_by_revenue[0].total_revenue,
        }
        best_day = {
            "date": sorted_by_revenue[-1].trading_date.isoformat(),
            "revenue": sorted_by_revenue[-1].total_revenue,
        }

    return CoDSummary(
        venue_id=venue_id,
        period_start=period_start,
        period_end=period_end,
        trading_days=len(records),
        total_revenue=round(total_revenue, 2),
        avg_daily_revenue=round(avg_daily_revenue, 2),
        total_variance=round(total_variance, 2),
        variance_pct=round(variance_pct, 2),
        avg_labour_pct=round(avg_labour_pct, 2),
        total_covers=total_covers,
        avg_spend=round(avg_spend, 2),
        days_with_discrepancies=days_with_discrepancies,
        best_day=best_day,
        worst_day=worst_day,
    )


def get_discrepancy_trend(records: List[CloseOfDay]) -> List[Dict[str, Any]]:
    """
    Return daily variance trend data for charting.

    Args:
        records: List of CloseOfDay records, ordered by date

    Returns:
        List of daily variance records: {date, variance, variance_pct, status}
    """
    trend = []
    for cod in sorted(records, key=lambda r: r.trading_date):
        variance_pct = (
            (cod.total_variance / cod.total_revenue * 100)
            if cod.total_revenue > 0
            else 0.0
        )
        trend.append({
            "date": cod.trading_date.isoformat(),
            "variance": cod.total_variance,
            "variance_pct": round(variance_pct, 2),
            "status": cod.sign_off_status.value,
        })
    return trend


def flag_anomalies(
    records: List[CloseOfDay],
    threshold_pct: float = 2.0,
) -> List[Dict[str, Any]]:
    """
    Flag days where variance exceeds threshold as anomalies.

    Args:
        records: List of CloseOfDay records
        threshold_pct: Variance threshold as percentage (default 2%)

    Returns:
        List of anomalies: {date, variance, variance_pct, reason}
    """
    anomalies = []
    for cod in records:
        variance_pct = (
            (abs(cod.total_variance) / cod.total_revenue * 100)
            if cod.total_revenue > 0
            else 0.0
        )
        if variance_pct >= threshold_pct:
            anomalies.append({
                "date": cod.trading_date.isoformat(),
                "variance": cod.total_variance,
                "variance_pct": round(variance_pct, 2),
                "reason": "variance_exceeds_threshold",
                "cod_id": cod.cod_id,
            })

    return anomalies
