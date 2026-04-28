"""
Xero integration API routes for RosterIQ.

Endpoints:
- GET /api/xero/connect - Initiate OAuth2 authorization
- GET /api/xero/callback - OAuth2 callback (redirected from Xero)
- GET /api/xero/status - Check Xero connection status
- POST /api/xero/disconnect - Revoke credentials
- POST /api/xero/sync/revenue - Manually trigger revenue sync
- POST /api/xero/sync/labour-costs - Export labour costs journal
- GET /api/xero/pnl/{venue_id} - Get P&L with labour % metrics

Usage:
    from rosteriq.xero_routes import setup_xero_routes
    setup_xero_routes(app, db)
"""

import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from rosteriq.xero_integration import (
    XeroOAuth,
    XeroClient,
    XeroCredentials,
    LabourCostJournal,
    save_xero_credentials,
    get_xero_credentials,
    delete_xero_credentials,
)

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

XERO_CLIENT_ID = os.environ.get("XERO_CLIENT_ID", "")
XERO_CLIENT_SECRET = os.environ.get("XERO_CLIENT_SECRET", "")
XERO_REDIRECT_URI = os.environ.get("XERO_REDIRECT_URI", "http://localhost:8000/api/xero/callback")

# ============================================================================
# Request/Response Models
# ============================================================================


class ConnectRequest(BaseModel):
    """Initiate OAuth2 flow."""

    venue_id: str
    state: Optional[str] = None  # For session management


class ConnectResponse(BaseModel):
    """OAuth2 authorization URL."""

    authorize_url: str
    state: str


class CallbackRequest(BaseModel):
    """Xero OAuth2 callback."""

    code: str
    state: str
    code_verifier: str


class StatusResponse(BaseModel):
    """Xero connection status."""

    venue_id: str
    connected: bool
    client_id: Optional[str] = None
    tenant_id: Optional[str] = None
    token_expires: Optional[str] = None
    last_synced: Optional[str] = None


class SyncRevenueRequest(BaseModel):
    """Manual revenue sync."""

    venue_id: str
    start_date: date
    end_date: Optional[date] = None


class SyncRevenueResponse(BaseModel):
    """Revenue sync result."""

    venue_id: str
    snapshots_count: int
    total_revenue: Decimal
    period_start: date
    period_end: date


class LabourCostRequest(BaseModel):
    """Labour cost export."""

    venue_id: str
    journal_date: date


class PnLResponse(BaseModel):
    """P&L report."""

    venue_id: str
    period_start: date
    period_end: date
    total_revenue: Decimal
    cogs: Decimal
    gross_profit: Decimal
    labour_costs: Decimal
    labour_percentage: Decimal
    other_expenses: Decimal
    net_profit: Decimal


# ============================================================================
# API Routes
# ============================================================================


def setup_xero_routes(app, db):
    """Register Xero integration routes on the FastAPI app."""

    router = APIRouter(prefix="/api/xero", tags=["xero"])

    # ========================================================================
    # OAuth2 Flow
    # ========================================================================

    @router.post("/connect", response_model=ConnectResponse)
    async def initiate_oauth(req: ConnectRequest):
        """
        POST /api/xero/connect

        Initiate OAuth2 PKCE flow. Returns authorize URL to redirect user to.

        Client should:
        1. GET this endpoint
        2. Redirect user to authorize_url
        3. Xero redirects back to /api/xero/callback with code
        """
        if not XERO_CLIENT_ID or not XERO_CLIENT_SECRET:
            raise HTTPException(
                500,
                "Xero OAuth not configured (missing XERO_CLIENT_ID/SECRET)",
            )

        oauth = XeroOAuth(
            XERO_CLIENT_ID,
            XERO_CLIENT_SECRET,
            XERO_REDIRECT_URI,
        )

        authorize_url, state, code_verifier = oauth.generate_authorize_url()

        # Store state + code_verifier in session (production: use session store)
        if not hasattr(db, "_oauth_states"):
            db._oauth_states = {}
        db._oauth_states[state] = {
            "venue_id": req.venue_id,
            "code_verifier": code_verifier,
            "created_at": date.today().isoformat(),
        }

        return ConnectResponse(authorize_url=authorize_url, state=state)

    @router.get("/callback")
    async def oauth_callback(
        code: str = Query(...),
        state: str = Query(...),
    ):
        """
        GET /api/xero/callback

        OAuth2 callback from Xero. Exchanges code for tokens.

        Xero redirects here after user authorizes. Validates state,
        exchanges code for tokens, and saves credentials.
        """
        # Validate state
        if not hasattr(db, "_oauth_states") or state not in db._oauth_states:
            raise HTTPException(400, "Invalid state — session expired")

        state_data = db._oauth_states.pop(state)
        venue_id = state_data["venue_id"]
        code_verifier = state_data.get("code_verifier", "")

        # Exchange code for tokens
        oauth = XeroOAuth(
            XERO_CLIENT_ID,
            XERO_CLIENT_SECRET,
            XERO_REDIRECT_URI,
        )

        try:
            credentials = await oauth.exchange_code(code, state, code_verifier)
            credentials.venue_id = venue_id
            await save_xero_credentials(db, credentials)

            return {
                "status": "success",
                "message": f"Xero connected for venue {venue_id}",
                "venue_id": venue_id,
                "tenant_id": credentials.tenant_id,
            }
        except Exception as e:
            logger.error(f"OAuth callback failed: {e}")
            raise HTTPException(400, f"OAuth failed: {str(e)}")

    # ========================================================================
    # Status & Management
    # ========================================================================

    @router.get("/status/{venue_id}", response_model=StatusResponse)
    async def get_xero_status(venue_id: str):
        """
        GET /api/xero/status/{venue_id}

        Check Xero connection status for venue.
        """
        credentials = await get_xero_credentials(db, venue_id)

        if not credentials:
            return StatusResponse(
                venue_id=venue_id,
                connected=False,
            )

        return StatusResponse(
            venue_id=venue_id,
            connected=True,
            client_id=credentials.client_id,
            tenant_id=credentials.tenant_id,
            token_expires=credentials.token_expires.isoformat(),
            last_synced=None,  # Would pull from audit log in production
        )

    @router.post("/disconnect/{venue_id}")
    async def disconnect_xero(venue_id: str):
        """
        POST /api/xero/disconnect/{venue_id}

        Revoke Xero credentials for venue.
        """
        await delete_xero_credentials(db, venue_id)

        return {
            "status": "success",
            "message": f"Xero disconnected for venue {venue_id}",
        }

    # ========================================================================
    # Revenue Sync
    # ========================================================================

    @router.post("/sync/revenue", response_model=SyncRevenueResponse)
    async def sync_revenue(req: SyncRevenueRequest):
        """
        POST /api/xero/sync/revenue

        Manually trigger revenue sync from Xero.

        Body:
        {
            "venue_id": "v1",
            "start_date": "2026-04-15",
            "end_date": "2026-04-23"
        }

        If end_date is omitted, syncs just the start_date.
        """
        credentials = await get_xero_credentials(db, req.venue_id)
        if not credentials:
            raise HTTPException(
                401,
                f"Xero not connected for venue {req.venue_id}",
            )

        end_date = req.end_date or req.start_date

        try:
            async with XeroClient(credentials, db) as xero:
                snapshots = await xero.pull_revenue_range(
                    req.start_date,
                    end_date,
                    req.venue_id,
                )

            # Save snapshots to database
            # In production: insert into revenue_snapshots table
            total_revenue = sum(s.total_revenue for s in snapshots)

            logger.info(
                f"Revenue synced for {req.venue_id}: "
                f"{len(snapshots)} days, ${total_revenue:,.2f}"
            )

            return SyncRevenueResponse(
                venue_id=req.venue_id,
                snapshots_count=len(snapshots),
                total_revenue=total_revenue,
                period_start=req.start_date,
                period_end=end_date,
            )
        except Exception as e:
            logger.error(f"Revenue sync failed: {e}")
            raise HTTPException(500, f"Revenue sync failed: {str(e)}")

    # ========================================================================
    # Labour Cost Export
    # ========================================================================

    @router.post("/sync/labour-costs")
    async def sync_labour_costs(req: LabourCostRequest):
        """
        POST /api/xero/sync/labour-costs

        Export labour costs to Xero as manual journal entry.

        Body:
        {
            "venue_id": "v1",
            "journal_date": "2026-04-23"
        }

        Fetches labour cost breakdown from RosterIQ (rosters + employee rates),
        builds journal entry, and pushes to Xero.

        In production:
        - Pulls labour costs from payroll/roster for the date
        - Includes penalty rates and superannuation
        - Pushes as draft journal for review
        """
        credentials = await get_xero_credentials(db, req.venue_id)
        if not credentials:
            raise HTTPException(
                401,
                f"Xero not connected for venue {req.venue_id}",
            )

        # TODO: Fetch labour costs from RosterIQ rosters/payroll
        # For now, return placeholder
        return {
            "status": "success",
            "message": f"Labour costs scheduled for export on {req.journal_date}",
            "venue_id": req.venue_id,
            "journal_date": req.journal_date.isoformat(),
            "note": "Integration pending payroll data hookup",
        }

    # ========================================================================
    # P&L Reporting
    # ========================================================================

    @router.get("/pnl/{venue_id}", response_model=PnLResponse)
    async def get_pnl_report(
        venue_id: str,
        period_days: int = Query(30, ge=1, le=365),
    ):
        """
        GET /api/xero/pnl/{venue_id}?period_days=30

        Pull P&L report from Xero and calculate labour % of revenue.

        Query params:
        - period_days: Number of days to report on (default 30)

        Returns P&L with labour_percentage = labour_costs / total_revenue * 100
        """
        credentials = await get_xero_credentials(db, venue_id)
        if not credentials:
            raise HTTPException(
                401,
                f"Xero not connected for venue {venue_id}",
            )

        period_end = date.today()
        period_start = period_end - timedelta(days=period_days)

        try:
            async with XeroClient(credentials, db) as xero:
                pnl = await xero.get_pnl_report(period_start, period_end)

            return PnLResponse(
                venue_id=venue_id,
                period_start=period_start,
                period_end=period_end,
                total_revenue=pnl.total_revenue,
                cogs=pnl.cogs,
                gross_profit=pnl.gross_profit,
                labour_costs=pnl.labour_costs,
                labour_percentage=pnl.labour_percentage,
                other_expenses=pnl.other_expenses,
                net_profit=pnl.net_profit,
            )
        except Exception as e:
            logger.error(f"P&L report failed: {e}")
            raise HTTPException(500, f"P&L report failed: {str(e)}")

    app.include_router(router)
