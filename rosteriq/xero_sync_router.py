"""FastAPI router for Xero Bidirectional Revenue Sync.

Endpoints for managing Xero connections, pulling revenue data,
viewing P&L summaries, and pushing payroll journals.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.xero_sync_router")

# Lazy imports for sandbox compatibility
try:
    from fastapi import APIRouter, HTTPException, Request, Query
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    Query = None
    BaseModel = object
    Field = lambda *a, **kw: None  # noqa: E731

router = None

if APIRouter is not None:
    router = APIRouter()

    # ------------------------------------------------------------------
    # Auth gate
    # ------------------------------------------------------------------

    def _gate(request, level_name):
        try:
            from rosteriq.auth import require_access
            require_access(request, level_name)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Request / Response models
    # ------------------------------------------------------------------

    class XeroConnectRequest(BaseModel):
        venue_id: str
        tenant_id: str
        access_token: str
        refresh_token: str
        token_expires_at: str
        organisation_name: str = ""

    class RevenueEntryRequest(BaseModel):
        venue_id: str
        date: str
        category: str = "OTHER"
        amount: float
        tax_amount: float = 0
        description: str = ""
        source: str = "manual"

    class XeroSyncRequest(BaseModel):
        date_from: str
        date_to: str
        invoices: List[Dict[str, Any]] = []

    class PLCalculateRequest(BaseModel):
        period_start: str
        period_end: str
        wage_cost: float = 0
        cogs: float = 0
        other_expenses: float = 0
        line_items: List[Dict[str, Any]] = []

    class PayrollJournalRequest(BaseModel):
        period_start: str
        period_end: str
        payroll_data: List[Dict[str, Any]]

    class AccountMappingRequest(BaseModel):
        mapping: Dict[str, str]

    class LabourCostRequest(BaseModel):
        period_start: str
        period_end: str
        total_wages: float

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    @router.post("/connect")
    async def connect_xero(request: Request, body: XeroConnectRequest):
        """Save Xero OAuth2 connection for a venue (OWNER)."""
        _gate(request, "OWNER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        conn = store.save_connection(body.dict())
        return {"status": "connected", "connection": conn.to_dict()}

    @router.get("/connection/{venue_id}")
    async def get_connection(request: Request, venue_id: str):
        """Check Xero connection status (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        conn = store.get_connection(venue_id)
        if not conn:
            return {"connected": False, "venue_id": venue_id}
        return {
            "connected": True,
            "connection": conn.to_dict(),
            "token_expired": store.is_token_expired(venue_id),
        }

    @router.post("/disconnect/{venue_id}")
    async def disconnect_xero(request: Request, venue_id: str):
        """Disconnect Xero for a venue (OWNER)."""
        _gate(request, "OWNER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        result = store.disconnect(venue_id)
        return {"disconnected": result, "venue_id": venue_id}

    @router.post("/sync/{venue_id}")
    async def sync_revenue(request: Request, venue_id: str,
                           body: XeroSyncRequest):
        """Pull revenue data from Xero invoices (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        result = store.simulate_xero_revenue_pull(
            venue_id, body.date_from, body.date_to, body.invoices)
        return result

    @router.post("/revenue")
    async def add_revenue(request: Request, body: RevenueEntryRequest):
        """Manually add a revenue record (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        rec = store.add_revenue(body.dict())
        return rec.to_dict()

    @router.get("/revenue/{venue_id}")
    async def get_revenue(request: Request, venue_id: str,
                          date_from: Optional[str] = None,
                          date_to: Optional[str] = None,
                          category: Optional[str] = None):
        """Get revenue records for a venue (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        records = store.get_revenue(venue_id, date_from, date_to, category)
        return {"records": [r.to_dict() for r in records],
                "count": len(records)}

    @router.get("/revenue/{venue_id}/daily/{target_date}")
    async def get_daily_revenue(request: Request, venue_id: str,
                                target_date: str):
        """Get daily revenue breakdown (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        return store.get_daily_revenue_total(venue_id, target_date)

    @router.get("/revenue/{venue_id}/trend")
    async def get_revenue_trend(request: Request, venue_id: str,
                                days: int = 28):
        """Get revenue trend for last N days (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        return {"trend": store.get_revenue_trend(venue_id, days)}

    @router.post("/pl/{venue_id}")
    async def calculate_pl(request: Request, venue_id: str,
                           body: PLCalculateRequest):
        """Calculate P&L summary for a period (L2+)."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        pl = store.calculate_pl(
            venue_id, body.period_start, body.period_end,
            body.wage_cost, body.cogs, body.other_expenses, body.line_items)
        return pl.to_dict()

    @router.get("/pl/{venue_id}")
    async def get_pl_summaries(request: Request, venue_id: str,
                               period_start: Optional[str] = None,
                               period_end: Optional[str] = None):
        """Get P&L summaries for a venue (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        summaries = store.get_pl_summaries(venue_id, period_start, period_end)
        return {"summaries": [s.to_dict() for s in summaries],
                "count": len(summaries)}

    @router.get("/pl/{venue_id}/latest")
    async def get_latest_pl(request: Request, venue_id: str):
        """Get latest P&L summary (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        pl = store.get_latest_pl(venue_id)
        if not pl:
            raise HTTPException(404, "No P&L summaries found")
        return pl.to_dict()

    @router.post("/journal/{venue_id}")
    async def build_payroll_journal(request: Request, venue_id: str,
                                    body: PayrollJournalRequest):
        """Build Xero payroll journal entry (OWNER)."""
        _gate(request, "OWNER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        journal = store.build_payroll_journal(
            venue_id, body.period_start, body.period_end, body.payroll_data)
        return journal

    @router.post("/mapping/{venue_id}")
    async def set_account_mapping(request: Request, venue_id: str,
                                  body: AccountMappingRequest):
        """Set Xero account code mapping (OWNER)."""
        _gate(request, "OWNER")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        mapping = store.set_account_mapping(venue_id, body.mapping)
        return {"venue_id": venue_id, "mapping": mapping}

    @router.get("/mapping/{venue_id}")
    async def get_account_mapping(request: Request, venue_id: str):
        """Get account code mapping (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        return {"venue_id": venue_id,
                "mapping": store.get_account_mapping(venue_id)}

    @router.post("/labour-cost/{venue_id}")
    async def get_labour_cost(request: Request, venue_id: str,
                              body: LabourCostRequest):
        """Get real labour cost % from Xero actuals (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        return store.get_real_labour_cost_pct(
            venue_id, body.period_start, body.period_end, body.total_wages)

    @router.get("/sync-history/{venue_id}")
    async def get_sync_history(request: Request, venue_id: str,
                               sync_type: Optional[str] = None,
                               limit: int = 20):
        """Get sync history (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        history = store.get_sync_history(venue_id, sync_type, limit)
        return {"history": [s.to_dict() for s in history],
                "count": len(history)}

    @router.get("/compare/{venue_id}")
    async def compare_periods(request: Request, venue_id: str,
                              p1_start: str = "", p1_end: str = "",
                              p2_start: str = "", p2_end: str = ""):
        """Compare revenue between two periods (L1+)."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.xero_sync import get_xero_sync_store
        store = get_xero_sync_store()
        return store.compare_periods(venue_id, p1_start, p1_end,
                                     p2_start, p2_end)
