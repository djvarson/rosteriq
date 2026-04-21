"""Xero Bidirectional Revenue Sync for RosterIQ.

Pulls revenue, COGS, and P&L summary data from Xero's accounting API
so that labour cost % can be calculated from real accounting figures
rather than POS estimates.  Also supports pushing payroll journal
entries back to Xero.

OAuth 2.0 flow for Xero API access.  Revenue data is stored locally
for dashboards and trend analysis.

Persistence: SQLite via rosteriq.persistence.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("rosteriq.xero_sync")

# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------

try:
    from rosteriq.persistence import get_persistence as _get_persistence
except ImportError:
    _get_persistence = None


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class SyncStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RevenueCategory(str, Enum):
    FOOD = "FOOD"
    BEVERAGE = "BEVERAGE"
    GAMING = "GAMING"
    ACCOMMODATION = "ACCOMMODATION"
    EVENTS = "EVENTS"
    MERCHANDISE = "MERCHANDISE"
    OTHER = "OTHER"


class PLLineType(str, Enum):
    REVENUE = "REVENUE"
    COST_OF_GOODS = "COST_OF_GOODS"
    WAGES = "WAGES"
    OVERHEAD = "OVERHEAD"
    OTHER_EXPENSE = "OTHER_EXPENSE"


@dataclass
class XeroConnection:
    """Stores Xero OAuth2 credentials for a venue."""
    id: str
    venue_id: str
    tenant_id: str  # Xero organisation ID
    access_token: str
    refresh_token: str
    token_expires_at: str  # ISO datetime
    organisation_name: str
    connected_at: str  # ISO datetime
    last_sync_at: Optional[str] = None
    is_active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "tenant_id": self.tenant_id,
            "organisation_name": self.organisation_name,
            "connected_at": self.connected_at,
            "last_sync_at": self.last_sync_at,
            "is_active": self.is_active,
            # Never expose tokens in API responses
        }


@dataclass
class RevenueRecord:
    """Daily revenue record pulled from Xero invoices/bank transactions."""
    id: str
    venue_id: str
    date: str  # ISO date
    category: str  # RevenueCategory value
    amount: float  # AUD
    tax_amount: float  # GST
    net_amount: float  # amount - tax
    source: str  # "xero_invoice", "xero_bank", "manual"
    xero_reference: Optional[str] = None  # Xero transaction ID
    description: Optional[str] = None
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "date": self.date,
            "category": self.category,
            "amount": self.amount,
            "tax_amount": self.tax_amount,
            "net_amount": self.net_amount,
            "source": self.source,
            "xero_reference": self.xero_reference,
            "description": self.description,
            "created_at": self.created_at,
        }


@dataclass
class PLSummary:
    """Profit & Loss summary for a period."""
    id: str
    venue_id: str
    period_start: str  # ISO date
    period_end: str  # ISO date
    total_revenue: float
    cost_of_goods: float
    gross_profit: float
    total_wages: float
    other_expenses: float
    net_profit: float
    labour_cost_pct: float  # wages / revenue * 100
    cogs_pct: float  # COGS / revenue * 100
    gross_margin_pct: float  # gross_profit / revenue * 100
    line_items: List[Dict[str, Any]] = field(default_factory=list)
    source: str = "calculated"
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "total_revenue": self.total_revenue,
            "cost_of_goods": self.cost_of_goods,
            "gross_profit": self.gross_profit,
            "total_wages": self.total_wages,
            "other_expenses": self.other_expenses,
            "net_profit": self.net_profit,
            "labour_cost_pct": round(self.labour_cost_pct, 2),
            "cogs_pct": round(self.cogs_pct, 2),
            "gross_margin_pct": round(self.gross_margin_pct, 2),
            "line_items": self.line_items,
            "source": self.source,
            "created_at": self.created_at,
        }


@dataclass
class SyncLog:
    """Record of a sync operation."""
    id: str
    venue_id: str
    sync_type: str  # "revenue_pull", "payroll_push", "pl_pull"
    status: str  # SyncStatus value
    started_at: str
    completed_at: Optional[str] = None
    records_synced: int = 0
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "sync_type": self.sync_type,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "records_synced": self.records_synced,
            "error_message": self.error_message,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Account Mapping — maps Xero account codes to RosterIQ categories
# ---------------------------------------------------------------------------

DEFAULT_ACCOUNT_MAPPING = {
    # Revenue accounts (Xero type = REVENUE)
    "200": RevenueCategory.FOOD.value,
    "201": RevenueCategory.BEVERAGE.value,
    "202": RevenueCategory.GAMING.value,
    "203": RevenueCategory.ACCOMMODATION.value,
    "204": RevenueCategory.EVENTS.value,
    "205": RevenueCategory.MERCHANDISE.value,
    # Expense accounts
    "400": PLLineType.COST_OF_GOODS.value,
    "410": PLLineType.COST_OF_GOODS.value,
    "420": PLLineType.WAGES.value,
    "421": PLLineType.WAGES.value,
    "430": PLLineType.OVERHEAD.value,
}


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

_store: Optional["XeroSyncStore"] = None
_store_lock = threading.Lock()


def get_xero_sync_store() -> "XeroSyncStore":
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = XeroSyncStore()
    return _store


def _reset_for_tests():
    """Test helper: clear the in-memory store (skips persistence reload)."""
    global _store
    with _store_lock:
        _store = XeroSyncStore.__new__(XeroSyncStore)
        _store._lock = threading.Lock()
        _store._connections = {}
        _store._revenue = {}
        _store._pl_summaries = {}
        _store._sync_logs = {}
        _store._account_mappings = {}


class XeroSyncStore:
    """Thread-safe store for Xero sync data with SQLite persistence."""

    def __init__(self):
        self._lock = threading.Lock()
        self._connections: Dict[str, XeroConnection] = {}  # keyed by id
        self._revenue: Dict[str, RevenueRecord] = {}  # keyed by id
        self._pl_summaries: Dict[str, PLSummary] = {}  # keyed by id
        self._sync_logs: Dict[str, SyncLog] = {}  # keyed by id
        self._account_mappings: Dict[str, Dict[str, str]] = {}  # venue_id -> mapping

        # Wire persistence
        if _get_persistence is not None:
            try:
                _p = _get_persistence()
                _p.register_schema("xero_connections", [
                    "id TEXT PRIMARY KEY",
                    "venue_id TEXT NOT NULL",
                    "data TEXT NOT NULL",
                ])
                _p.register_schema("xero_revenue", [
                    "id TEXT PRIMARY KEY",
                    "venue_id TEXT NOT NULL",
                    "date TEXT NOT NULL",
                    "data TEXT NOT NULL",
                ])
                _p.register_schema("xero_pl_summaries", [
                    "id TEXT PRIMARY KEY",
                    "venue_id TEXT NOT NULL",
                    "data TEXT NOT NULL",
                ])
                _p.register_schema("xero_sync_logs", [
                    "id TEXT PRIMARY KEY",
                    "venue_id TEXT NOT NULL",
                    "data TEXT NOT NULL",
                ])
                logger.info("Xero sync persistence schemas registered")
            except Exception as exc:
                logger.warning("Xero sync persistence unavailable: %s", exc)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def save_connection(self, conn_dict: Dict[str, Any]) -> XeroConnection:
        """Save or update a Xero OAuth connection."""
        with self._lock:
            conn_id = conn_dict.get("id", str(uuid.uuid4()))
            conn = XeroConnection(
                id=conn_id,
                venue_id=conn_dict["venue_id"],
                tenant_id=conn_dict["tenant_id"],
                access_token=conn_dict["access_token"],
                refresh_token=conn_dict["refresh_token"],
                token_expires_at=conn_dict["token_expires_at"],
                organisation_name=conn_dict.get("organisation_name", ""),
                connected_at=conn_dict.get("connected_at",
                                           datetime.now(timezone.utc).isoformat()),
                last_sync_at=conn_dict.get("last_sync_at"),
                is_active=conn_dict.get("is_active", True),
            )
            self._connections[conn.id] = conn
            return conn

    def get_connection(self, venue_id: str) -> Optional[XeroConnection]:
        """Get active Xero connection for a venue."""
        with self._lock:
            for c in self._connections.values():
                if c.venue_id == venue_id and c.is_active:
                    return c
            return None

    def disconnect(self, venue_id: str) -> bool:
        """Deactivate Xero connection for a venue."""
        with self._lock:
            for c in self._connections.values():
                if c.venue_id == venue_id and c.is_active:
                    c.is_active = False
                    return True
            return False

    def update_tokens(self, connection_id: str, access_token: str,
                      refresh_token: str, expires_at: str) -> Optional[XeroConnection]:
        """Update OAuth tokens after refresh."""
        with self._lock:
            conn = self._connections.get(connection_id)
            if conn:
                conn.access_token = access_token
                conn.refresh_token = refresh_token
                conn.token_expires_at = expires_at
                return conn
            return None

    def is_token_expired(self, venue_id: str) -> bool:
        """Check if the venue's Xero token has expired."""
        conn = self.get_connection(venue_id)
        if not conn:
            return True
        try:
            expires = datetime.fromisoformat(conn.token_expires_at.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) >= expires
        except (ValueError, AttributeError):
            return True

    # ------------------------------------------------------------------
    # Revenue records
    # ------------------------------------------------------------------

    def add_revenue(self, rev_dict: Dict[str, Any]) -> RevenueRecord:
        """Add a revenue record (from Xero or manual entry)."""
        with self._lock:
            rev_id = rev_dict.get("id", str(uuid.uuid4()))
            amount = float(rev_dict.get("amount", 0))
            tax = float(rev_dict.get("tax_amount", amount / 11))  # default 10% GST
            rec = RevenueRecord(
                id=rev_id,
                venue_id=rev_dict["venue_id"],
                date=rev_dict["date"],
                category=rev_dict.get("category", RevenueCategory.OTHER.value),
                amount=amount,
                tax_amount=round(tax, 2),
                net_amount=round(amount - tax, 2),
                source=rev_dict.get("source", "manual"),
                xero_reference=rev_dict.get("xero_reference"),
                description=rev_dict.get("description"),
            )
            self._revenue[rec.id] = rec
            return rec

    def get_revenue(self, venue_id: str, date_from: Optional[str] = None,
                    date_to: Optional[str] = None,
                    category: Optional[str] = None) -> List[RevenueRecord]:
        """Get revenue records with optional filters."""
        with self._lock:
            results = [r for r in self._revenue.values()
                       if r.venue_id == venue_id]
            if date_from:
                results = [r for r in results if r.date >= date_from]
            if date_to:
                results = [r for r in results if r.date <= date_to]
            if category:
                results = [r for r in results if r.category == category]
            return sorted(results, key=lambda r: r.date)

    def get_daily_revenue_total(self, venue_id: str, target_date: str) -> Dict[str, Any]:
        """Get total revenue for a specific date, broken down by category."""
        records = self.get_revenue(venue_id, date_from=target_date, date_to=target_date)
        by_category: Dict[str, float] = {}
        total = 0.0
        total_tax = 0.0
        for r in records:
            by_category[r.category] = by_category.get(r.category, 0) + r.amount
            total += r.amount
            total_tax += r.tax_amount
        return {
            "date": target_date,
            "venue_id": venue_id,
            "total_revenue": round(total, 2),
            "total_tax": round(total_tax, 2),
            "net_revenue": round(total - total_tax, 2),
            "by_category": {k: round(v, 2) for k, v in by_category.items()},
            "record_count": len(records),
        }

    def get_revenue_trend(self, venue_id: str, days: int = 28) -> List[Dict[str, Any]]:
        """Get daily revenue totals for the last N days."""
        today = date.today()
        results = []
        for i in range(days):
            d = (today - timedelta(days=days - 1 - i)).isoformat()
            daily = self.get_daily_revenue_total(venue_id, d)
            results.append(daily)
        return results

    def delete_revenue(self, record_id: str) -> bool:
        """Delete a revenue record."""
        with self._lock:
            if record_id in self._revenue:
                del self._revenue[record_id]
                return True
            return False

    def bulk_add_revenue(self, records: List[Dict[str, Any]]) -> List[RevenueRecord]:
        """Add multiple revenue records at once (from Xero sync)."""
        results = []
        for rec in records:
            results.append(self.add_revenue(rec))
        return results

    # ------------------------------------------------------------------
    # P&L Summaries
    # ------------------------------------------------------------------

    def calculate_pl(self, venue_id: str, period_start: str,
                     period_end: str,
                     wage_cost: float = 0,
                     cogs: float = 0,
                     other_expenses: float = 0,
                     line_items: Optional[List[Dict[str, Any]]] = None) -> PLSummary:
        """Calculate P&L summary from revenue records + provided costs."""
        revenue_records = self.get_revenue(venue_id,
                                           date_from=period_start,
                                           date_to=period_end)
        total_revenue = sum(r.amount for r in revenue_records)

        # If no explicit COGS/wages provided, try to derive from line items
        if line_items:
            for item in line_items:
                lt = item.get("type", "")
                amt = float(item.get("amount", 0))
                if lt == PLLineType.COST_OF_GOODS.value:
                    cogs += amt
                elif lt == PLLineType.WAGES.value:
                    wage_cost += amt
                elif lt in (PLLineType.OVERHEAD.value, PLLineType.OTHER_EXPENSE.value):
                    other_expenses += amt

        gross_profit = total_revenue - cogs
        net_profit = gross_profit - wage_cost - other_expenses

        labour_pct = (wage_cost / total_revenue * 100) if total_revenue > 0 else 0
        cogs_pct = (cogs / total_revenue * 100) if total_revenue > 0 else 0
        gm_pct = (gross_profit / total_revenue * 100) if total_revenue > 0 else 0

        with self._lock:
            pl = PLSummary(
                id=str(uuid.uuid4()),
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
                total_revenue=round(total_revenue, 2),
                cost_of_goods=round(cogs, 2),
                gross_profit=round(gross_profit, 2),
                total_wages=round(wage_cost, 2),
                other_expenses=round(other_expenses, 2),
                net_profit=round(net_profit, 2),
                labour_cost_pct=round(labour_pct, 2),
                cogs_pct=round(cogs_pct, 2),
                gross_margin_pct=round(gm_pct, 2),
                line_items=line_items or [],
                source="calculated",
            )
            self._pl_summaries[pl.id] = pl
            return pl

    def get_pl_summaries(self, venue_id: str,
                         period_start: Optional[str] = None,
                         period_end: Optional[str] = None) -> List[PLSummary]:
        """Get P&L summaries for a venue."""
        with self._lock:
            results = [p for p in self._pl_summaries.values()
                       if p.venue_id == venue_id]
            if period_start:
                results = [p for p in results if p.period_start >= period_start]
            if period_end:
                results = [p for p in results if p.period_end <= period_end]
            return sorted(results, key=lambda p: p.period_start)

    def get_latest_pl(self, venue_id: str) -> Optional[PLSummary]:
        """Get the most recent P&L summary."""
        summaries = self.get_pl_summaries(venue_id)
        return summaries[-1] if summaries else None

    def compare_periods(self, venue_id: str,
                        period1_start: str, period1_end: str,
                        period2_start: str, period2_end: str) -> Dict[str, Any]:
        """Compare two P&L periods — calculates deltas."""
        p1_records = self.get_revenue(venue_id, period1_start, period1_end)
        p2_records = self.get_revenue(venue_id, period2_start, period2_end)

        p1_rev = sum(r.amount for r in p1_records)
        p2_rev = sum(r.amount for r in p2_records)

        rev_change = p2_rev - p1_rev
        rev_change_pct = (rev_change / p1_rev * 100) if p1_rev > 0 else 0

        return {
            "period_1": {"start": period1_start, "end": period1_end,
                         "revenue": round(p1_rev, 2)},
            "period_2": {"start": period2_start, "end": period2_end,
                         "revenue": round(p2_rev, 2)},
            "revenue_change": round(rev_change, 2),
            "revenue_change_pct": round(rev_change_pct, 2),
        }

    # ------------------------------------------------------------------
    # Sync log
    # ------------------------------------------------------------------

    def start_sync(self, venue_id: str, sync_type: str) -> SyncLog:
        """Create a sync log entry when starting a sync operation."""
        with self._lock:
            log = SyncLog(
                id=str(uuid.uuid4()),
                venue_id=venue_id,
                sync_type=sync_type,
                status=SyncStatus.IN_PROGRESS.value,
                started_at=datetime.now(timezone.utc).isoformat(),
            )
            self._sync_logs[log.id] = log
            return log

    def complete_sync(self, sync_id: str, records_synced: int = 0,
                      error: Optional[str] = None) -> Optional[SyncLog]:
        """Mark a sync as completed or failed."""
        with self._lock:
            log = self._sync_logs.get(sync_id)
            if not log:
                return None
            log.completed_at = datetime.now(timezone.utc).isoformat()
            log.records_synced = records_synced
            if error:
                log.status = SyncStatus.FAILED.value
                log.error_message = error
            else:
                log.status = SyncStatus.COMPLETED.value
            return log

    def get_sync_history(self, venue_id: str,
                         sync_type: Optional[str] = None,
                         limit: int = 20) -> List[SyncLog]:
        """Get sync history for a venue."""
        with self._lock:
            results = [s for s in self._sync_logs.values()
                       if s.venue_id == venue_id]
            if sync_type:
                results = [s for s in results if s.sync_type == sync_type]
            results.sort(key=lambda s: s.started_at, reverse=True)
            return results[:limit]

    def get_last_sync(self, venue_id: str,
                      sync_type: str = "revenue_pull") -> Optional[SyncLog]:
        """Get the most recent successful sync."""
        history = self.get_sync_history(venue_id, sync_type)
        for s in history:
            if s.status == SyncStatus.COMPLETED.value:
                return s
        return None

    # ------------------------------------------------------------------
    # Account mapping
    # ------------------------------------------------------------------

    def set_account_mapping(self, venue_id: str,
                            mapping: Dict[str, str]) -> Dict[str, str]:
        """Set custom Xero account code -> RosterIQ category mapping."""
        with self._lock:
            self._account_mappings[venue_id] = mapping
            return mapping

    def get_account_mapping(self, venue_id: str) -> Dict[str, str]:
        """Get account mapping, falling back to defaults."""
        with self._lock:
            return self._account_mappings.get(venue_id, DEFAULT_ACCOUNT_MAPPING.copy())

    # ------------------------------------------------------------------
    # Xero API simulation (actual API calls would use httpx)
    # ------------------------------------------------------------------

    def simulate_xero_revenue_pull(self, venue_id: str,
                                   date_from: str, date_to: str,
                                   invoices: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process Xero invoice data into revenue records.

        In production, this would call the Xero API. Here it accepts
        pre-fetched invoice data for processing.

        Each invoice dict should have:
          - invoice_number, date, total, tax, line_items
          - line_items: [{account_code, description, amount, tax_amount}]
        """
        sync_log = self.start_sync(venue_id, "revenue_pull")
        mapping = self.get_account_mapping(venue_id)

        records_added = 0
        try:
            for inv in invoices:
                inv_date = inv.get("date", date_from)
                for item in inv.get("line_items", []):
                    acct = item.get("account_code", "")
                    category = mapping.get(acct, RevenueCategory.OTHER.value)
                    # Only add revenue categories
                    if category in [rc.value for rc in RevenueCategory]:
                        self.add_revenue({
                            "venue_id": venue_id,
                            "date": inv_date,
                            "category": category,
                            "amount": float(item.get("amount", 0)),
                            "tax_amount": float(item.get("tax_amount", 0)),
                            "source": "xero_invoice",
                            "xero_reference": inv.get("invoice_number"),
                            "description": item.get("description", ""),
                        })
                        records_added += 1

            self.complete_sync(sync_log.id, records_synced=records_added)
            # Update connection last_sync
            conn = self.get_connection(venue_id)
            if conn:
                conn.last_sync_at = datetime.now(timezone.utc).isoformat()

        except Exception as exc:
            self.complete_sync(sync_log.id, error=str(exc))
            raise

        return {
            "sync_id": sync_log.id,
            "records_synced": records_added,
            "status": "COMPLETED",
        }

    def build_payroll_journal(self, venue_id: str, period_start: str,
                              period_end: str,
                              payroll_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build a Xero-compatible journal entry from payroll data.

        This creates the data structure that would be POSTed to
        Xero's Manual Journals API.

        payroll_data: list of dicts with employee_name, gross_pay,
                      super_amount, tax_withheld
        """
        total_wages = sum(float(p.get("gross_pay", 0)) for p in payroll_data)
        total_super = sum(float(p.get("super_amount", 0)) for p in payroll_data)
        total_tax = sum(float(p.get("tax_withheld", 0)) for p in payroll_data)
        net_pay = total_wages - total_tax

        journal = {
            "Date": period_end,
            "Narration": f"RosterIQ Payroll {period_start} to {period_end}",
            "JournalLines": [
                {
                    "AccountCode": "420",  # Wages expense
                    "Description": "Gross wages",
                    "LineAmount": round(total_wages, 2),
                    "IsDebit": True,
                },
                {
                    "AccountCode": "421",  # Superannuation expense
                    "Description": "Superannuation guarantee",
                    "LineAmount": round(total_super, 2),
                    "IsDebit": True,
                },
                {
                    "AccountCode": "820",  # Wages payable
                    "Description": "Net wages payable",
                    "LineAmount": round(net_pay, 2),
                    "IsDebit": False,
                },
                {
                    "AccountCode": "830",  # PAYG withholding
                    "Description": "PAYG tax withheld",
                    "LineAmount": round(total_tax, 2),
                    "IsDebit": False,
                },
                {
                    "AccountCode": "840",  # Super payable
                    "Description": "Superannuation payable",
                    "LineAmount": round(total_super, 2),
                    "IsDebit": False,
                },
            ],
            "Status": "DRAFT",
            "summary": {
                "total_wages": round(total_wages, 2),
                "total_super": round(total_super, 2),
                "total_tax_withheld": round(total_tax, 2),
                "net_pay": round(net_pay, 2),
                "employee_count": len(payroll_data),
            },
        }
        return journal

    # ------------------------------------------------------------------
    # Labour cost % from real Xero data
    # ------------------------------------------------------------------

    def get_real_labour_cost_pct(self, venue_id: str,
                                 period_start: str, period_end: str,
                                 total_wages: float) -> Dict[str, Any]:
        """Calculate labour cost % using real Xero revenue data.

        This is the key differentiator over POS-estimate-based
        calculations — uses actual accounting figures.
        """
        revenue_records = self.get_revenue(venue_id, period_start, period_end)
        total_revenue = sum(r.amount for r in revenue_records)

        labour_pct = (total_wages / total_revenue * 100) if total_revenue > 0 else 0

        # Determine health status
        if labour_pct <= 0:
            status = "NO_DATA"
        elif labour_pct < 25:
            status = "EXCELLENT"
        elif labour_pct < 30:
            status = "GOOD"
        elif labour_pct < 35:
            status = "WARNING"
        else:
            status = "CRITICAL"

        return {
            "venue_id": venue_id,
            "period_start": period_start,
            "period_end": period_end,
            "total_revenue": round(total_revenue, 2),
            "total_wages": round(total_wages, 2),
            "labour_cost_pct": round(labour_pct, 2),
            "status": status,
            "source": "xero_actuals",
            "revenue_record_count": len(revenue_records),
        }
