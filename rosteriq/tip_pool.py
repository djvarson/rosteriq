"""
Tip Pooling & Distribution for Australian Hospitality Venues.

Tracks tips by shift, pools across staff, distributes by hours worked,
equal split, or role-based point weights.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple


class DistributionMethod(str, Enum):
    HOURS_BASED = "hours_based"
    EQUAL_SPLIT = "equal_split"
    POINTS_BASED = "points_based"


@dataclass
class TipEntry:
    entry_id: str
    venue_id: str
    shift_date: date
    amount: float
    source: str  # "cash", "card", "online"
    entered_by: str
    entered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "venue_id": self.venue_id,
            "shift_date": self.shift_date.isoformat(),
            "amount": self.amount,
            "source": self.source,
            "entered_by": self.entered_by,
            "entered_at": self.entered_at.isoformat(),
            "notes": self.notes,
        }


@dataclass
class TipAllocation:
    allocation_id: str
    pool_id: str
    employee_id: str
    employee_name: str
    hours_worked: float
    points: float
    share_amount: float
    share_pct: float

    def to_dict(self) -> dict:
        return {
            "allocation_id": self.allocation_id,
            "pool_id": self.pool_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "hours_worked": self.hours_worked,
            "points": self.points,
            "share_amount": round(self.share_amount, 2),
            "share_pct": round(self.share_pct, 2),
        }


@dataclass
class TipPool:
    pool_id: str
    venue_id: str
    pool_date: date
    total_amount: float
    entries: List[TipEntry] = field(default_factory=list)
    distribution_method: DistributionMethod = DistributionMethod.HOURS_BASED
    is_distributed: bool = False
    distributed_at: Optional[datetime] = None
    distributed_by: Optional[str] = None
    allocations: List[TipAllocation] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pool_id": self.pool_id,
            "venue_id": self.venue_id,
            "pool_date": self.pool_date.isoformat(),
            "total_amount": round(self.total_amount, 2),
            "entry_count": len(self.entries),
            "distribution_method": self.distribution_method.value,
            "is_distributed": self.is_distributed,
            "distributed_at": self.distributed_at.isoformat() if self.distributed_at else None,
            "distributed_by": self.distributed_by,
            "allocations": [a.to_dict() for a in self.allocations],
        }


@dataclass
class PointWeight:
    role: str
    weight: float


DEFAULT_POINT_WEIGHTS = {
    "bar": 1.0,
    "floor": 1.0,
    "kitchen": 0.8,
    "manager": 1.5,
}


@dataclass
class TipSummary:
    venue_id: str
    period_start: date
    period_end: date
    total_tips: float
    total_distributed: float
    pools_count: int
    avg_per_pool: float
    avg_per_employee: float
    by_source: Dict[str, float] = field(default_factory=dict)
    by_method: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "venue_id": self.venue_id,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "total_tips": round(self.total_tips, 2),
            "total_distributed": round(self.total_distributed, 2),
            "pools_count": self.pools_count,
            "avg_per_pool": round(self.avg_per_pool, 2),
            "avg_per_employee": round(self.avg_per_employee, 2),
            "by_source": {k: round(v, 2) for k, v in self.by_source.items()},
            "by_method": self.by_method,
        }


class TipPoolStore:
    """Thread-safe tip pool store with SQLite persistence."""

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: List[TipEntry] = []
        self._pools: List[TipPool] = []
        self._allocations: List[TipAllocation] = []
        self._init_persistence()

    def _init_persistence(self):
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            conn = _p.connection()
            conn.execute("""CREATE TABLE IF NOT EXISTS tip_entries (
                entry_id TEXT PRIMARY KEY, venue_id TEXT, shift_date TEXT,
                amount REAL, source TEXT, entered_by TEXT, entered_at TEXT, notes TEXT
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS tip_pools (
                pool_id TEXT PRIMARY KEY, venue_id TEXT, pool_date TEXT,
                total_amount REAL, distribution_method TEXT, is_distributed INTEGER,
                distributed_at TEXT, distributed_by TEXT
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS tip_allocations (
                allocation_id TEXT PRIMARY KEY, pool_id TEXT, employee_id TEXT,
                employee_name TEXT, hours_worked REAL, points REAL,
                share_amount REAL, share_pct REAL
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS tip_pool_entries (
                pool_id TEXT, entry_id TEXT, PRIMARY KEY(pool_id, entry_id)
            )""")
            conn.commit()
        except Exception:
            pass

    def add_entry(self, entry: TipEntry) -> TipEntry:
        with self._lock:
            self._entries.append(entry)
            self._persist_entry(entry)
            return entry

    def _persist_entry(self, entry: TipEntry):
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            with _p.write_txn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO tip_entries VALUES (?,?,?,?,?,?,?,?)",
                    (entry.entry_id, entry.venue_id, entry.shift_date.isoformat(),
                     entry.amount, entry.source, entry.entered_by,
                     entry.entered_at.isoformat(), entry.notes))
        except Exception:
            pass

    def add_pool(self, pool: TipPool) -> TipPool:
        with self._lock:
            self._pools.append(pool)
            self._persist_pool(pool)
            return pool

    def _persist_pool(self, pool: TipPool):
        try:
            from rosteriq import persistence as _p
            if not _p.is_persistence_enabled():
                return
            with _p.write_txn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO tip_pools VALUES (?,?,?,?,?,?,?,?)",
                    (pool.pool_id, pool.venue_id, pool.pool_date.isoformat(),
                     pool.total_amount, pool.distribution_method.value,
                     1 if pool.is_distributed else 0,
                     pool.distributed_at.isoformat() if pool.distributed_at else None,
                     pool.distributed_by))
                for e in pool.entries:
                    conn.execute(
                        "INSERT OR REPLACE INTO tip_pool_entries VALUES (?,?)",
                        (pool.pool_id, e.entry_id))
        except Exception:
            pass

    def save_allocations(self, allocations: List[TipAllocation]):
        with self._lock:
            self._allocations.extend(allocations)
            try:
                from rosteriq import persistence as _p
                if not _p.is_persistence_enabled():
                    return
                with _p.write_txn() as conn:
                    for a in allocations:
                        conn.execute(
                            "INSERT OR REPLACE INTO tip_allocations VALUES (?,?,?,?,?,?,?,?)",
                            (a.allocation_id, a.pool_id, a.employee_id, a.employee_name,
                             a.hours_worked, a.points, a.share_amount, a.share_pct))
            except Exception:
                pass

    def get_pool(self, pool_id: str) -> Optional[TipPool]:
        with self._lock:
            for p in self._pools:
                if p.pool_id == pool_id:
                    return p
            return None

    def update_pool(self, pool: TipPool):
        with self._lock:
            for i, p in enumerate(self._pools):
                if p.pool_id == pool.pool_id:
                    self._pools[i] = pool
                    break
            self._persist_pool(pool)

    def remove_allocations(self, pool_id: str):
        with self._lock:
            self._allocations = [a for a in self._allocations if a.pool_id != pool_id]
            try:
                from rosteriq import persistence as _p
                if not _p.is_persistence_enabled():
                    return
                with _p.write_txn() as conn:
                    conn.execute("DELETE FROM tip_allocations WHERE pool_id=?", (pool_id,))
            except Exception:
                pass

    def list_entries(self, venue_id: str, date_from: Optional[date] = None,
                     date_to: Optional[date] = None) -> List[TipEntry]:
        with self._lock:
            results = [e for e in self._entries if e.venue_id == venue_id]
            if date_from:
                results = [e for e in results if e.shift_date >= date_from]
            if date_to:
                results = [e for e in results if e.shift_date <= date_to]
            return results

    def list_pools(self, venue_id: str, date_from: Optional[date] = None,
                   date_to: Optional[date] = None,
                   distributed: Optional[bool] = None) -> List[TipPool]:
        with self._lock:
            results = [p for p in self._pools if p.venue_id == venue_id]
            if date_from:
                results = [p for p in results if p.pool_date >= date_from]
            if date_to:
                results = [p for p in results if p.pool_date <= date_to]
            if distributed is not None:
                results = [p for p in results if p.is_distributed == distributed]
            return results

    def get_employee_allocations(self, employee_id: str, venue_id: str,
                                  date_from: Optional[date] = None,
                                  date_to: Optional[date] = None) -> List[TipAllocation]:
        with self._lock:
            pool_ids = {p.pool_id for p in self._pools if p.venue_id == venue_id}
            if date_from:
                pool_ids = {p.pool_id for p in self._pools
                           if p.venue_id == venue_id and p.pool_date >= date_from}
            if date_to:
                pool_ids = {p.pool_id for p in self._pools
                           if p.venue_id == venue_id and p.pool_date <= date_to}
            return [a for a in self._allocations
                    if a.employee_id == employee_id and a.pool_id in pool_ids]

    def list_all_pools(self) -> List[TipPool]:
        with self._lock:
            return list(self._pools)


_store: Optional[TipPoolStore] = None
_store_lock = threading.Lock()


def get_tip_pool_store() -> TipPoolStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = TipPoolStore()
    return _store


def _reset_for_tests():
    global _store
    with _store_lock:
        s = TipPoolStore.__new__(TipPoolStore)
        s._lock = threading.Lock()
        s._entries = []
        s._pools = []
        s._allocations = []
        _store = s


# --- Public API ---

def add_tip_entry(venue_id: str, shift_date: date, amount: float,
                  source: str, entered_by: str, notes: str = "") -> TipEntry:
    entry = TipEntry(
        entry_id=str(uuid.uuid4())[:8],
        venue_id=venue_id,
        shift_date=shift_date,
        amount=amount,
        source=source,
        entered_by=entered_by,
        notes=notes,
    )
    return get_tip_pool_store().add_entry(entry)


def create_pool(venue_id: str, pool_date: date, entries: List[TipEntry],
                method: DistributionMethod = DistributionMethod.HOURS_BASED) -> TipPool:
    total = sum(e.amount for e in entries)
    pool = TipPool(
        pool_id=str(uuid.uuid4())[:8],
        venue_id=venue_id,
        pool_date=pool_date,
        total_amount=total,
        entries=list(entries),
        distribution_method=method,
    )
    return get_tip_pool_store().add_pool(pool)


def distribute_pool(pool_id: str, staff_on_shift: List[dict],
                    method: Optional[DistributionMethod] = None,
                    point_weights: Optional[Dict[str, float]] = None) -> List[TipAllocation]:
    """Distribute a tip pool to staff.

    staff_on_shift: [{"employee_id", "employee_name", "hours_worked", "role"}, ...]
    """
    store = get_tip_pool_store()
    pool = store.get_pool(pool_id)
    if pool is None:
        raise ValueError(f"Pool {pool_id} not found")
    if pool.is_distributed:
        raise ValueError(f"Pool {pool_id} already distributed")
    if not staff_on_shift:
        raise ValueError("No staff to distribute to")

    use_method = method or pool.distribution_method
    weights = point_weights or DEFAULT_POINT_WEIGHTS
    allocations = []

    if use_method == DistributionMethod.HOURS_BASED:
        total_hours = sum(s["hours_worked"] for s in staff_on_shift)
        if total_hours <= 0:
            raise ValueError("Total hours must be > 0 for hours-based distribution")
        for s in staff_on_shift:
            pct = (s["hours_worked"] / total_hours) * 100
            share = (s["hours_worked"] / total_hours) * pool.total_amount
            allocations.append(TipAllocation(
                allocation_id=str(uuid.uuid4())[:8],
                pool_id=pool_id,
                employee_id=s["employee_id"],
                employee_name=s["employee_name"],
                hours_worked=s["hours_worked"],
                points=0,
                share_amount=round(share, 2),
                share_pct=round(pct, 2),
            ))

    elif use_method == DistributionMethod.EQUAL_SPLIT:
        n = len(staff_on_shift)
        share = pool.total_amount / n
        pct = 100.0 / n
        for s in staff_on_shift:
            allocations.append(TipAllocation(
                allocation_id=str(uuid.uuid4())[:8],
                pool_id=pool_id,
                employee_id=s["employee_id"],
                employee_name=s["employee_name"],
                hours_worked=s.get("hours_worked", 0),
                points=0,
                share_amount=round(share, 2),
                share_pct=round(pct, 2),
            ))

    elif use_method == DistributionMethod.POINTS_BASED:
        for s in staff_on_shift:
            role = s.get("role", "floor")
            w = weights.get(role, 1.0)
            s["_points"] = s["hours_worked"] * w
        total_points = sum(s["_points"] for s in staff_on_shift)
        if total_points <= 0:
            raise ValueError("Total points must be > 0")
        for s in staff_on_shift:
            pct = (s["_points"] / total_points) * 100
            share = (s["_points"] / total_points) * pool.total_amount
            allocations.append(TipAllocation(
                allocation_id=str(uuid.uuid4())[:8],
                pool_id=pool_id,
                employee_id=s["employee_id"],
                employee_name=s["employee_name"],
                hours_worked=s["hours_worked"],
                points=s["_points"],
                share_amount=round(share, 2),
                share_pct=round(pct, 2),
            ))

    pool.is_distributed = True
    pool.distributed_at = datetime.now(timezone.utc)
    pool.allocations = allocations
    store.update_pool(pool)
    store.save_allocations(allocations)
    return allocations


def undo_distribution(pool_id: str) -> TipPool:
    store = get_tip_pool_store()
    pool = store.get_pool(pool_id)
    if pool is None:
        raise ValueError(f"Pool {pool_id} not found")
    if not pool.is_distributed:
        raise ValueError(f"Pool {pool_id} is not distributed")
    pool.is_distributed = False
    pool.distributed_at = None
    pool.distributed_by = None
    pool.allocations = []
    store.update_pool(pool)
    store.remove_allocations(pool_id)
    return pool


def get_employee_tips(employee_id: str, venue_id: str,
                      date_from: Optional[date] = None,
                      date_to: Optional[date] = None) -> List[TipAllocation]:
    return get_tip_pool_store().get_employee_allocations(
        employee_id, venue_id, date_from, date_to)


def build_tip_summary(venue_id: str, date_from: date,
                      date_to: date) -> TipSummary:
    store = get_tip_pool_store()
    entries = store.list_entries(venue_id, date_from, date_to)
    pools = store.list_pools(venue_id, date_from, date_to)

    total_tips = sum(e.amount for e in entries)
    distributed_pools = [p for p in pools if p.is_distributed]
    total_distributed = sum(p.total_amount for p in distributed_pools)

    by_source: Dict[str, float] = {}
    for e in entries:
        by_source[e.source] = by_source.get(e.source, 0) + e.amount

    by_method: Dict[str, int] = {}
    for p in distributed_pools:
        m = p.distribution_method.value
        by_method[m] = by_method.get(m, 0) + 1

    all_allocations = []
    for p in distributed_pools:
        all_allocations.extend(p.allocations)
    unique_employees = len({a.employee_id for a in all_allocations}) if all_allocations else 0

    return TipSummary(
        venue_id=venue_id,
        period_start=date_from,
        period_end=date_to,
        total_tips=total_tips,
        total_distributed=total_distributed,
        pools_count=len(pools),
        avg_per_pool=total_tips / len(pools) if pools else 0,
        avg_per_employee=total_distributed / unique_employees if unique_employees else 0,
        by_source=by_source,
        by_method=by_method,
    )
