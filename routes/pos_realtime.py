"""
Routes for real-time POS sales ingestion and revenue monitoring.

Endpoints:
- POST /api/pos/live/ingest — Receive POS webhook push (sale event)
- GET /api/pos/live/revenue/{venue_id} — Current live revenue
- GET /api/pos/live/hourly/{venue_id} — Hourly breakdown
- GET /api/pos/live/variance/{venue_id} — Forecast vs actual variance
- POST /api/pos/live/start/{venue_id} — Start polling for venue
- POST /api/pos/live/stop/{venue_id} — Stop polling
"""

from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# This will be injected by api.py after modules are loaded
pos_feed = None


# ============================================================================
# Pydantic Models
# ============================================================================


class SaleIngestRequest(BaseModel):
    """Request body for POS webhook."""

    venue_id: str
    amount: float
    timestamp: Optional[str] = None
    transaction_id: Optional[str] = None
    category: Optional[str] = None


class PollingStartRequest(BaseModel):
    """Request to start polling for a venue."""

    pos_type: str  # "swiftpos", "lightspeed", "kounta", etc.
    interval_seconds: int = 60
    forecast_revenue: Optional[float] = None


class PollingStopRequest(BaseModel):
    """Request to stop polling."""

    venue_id: str


# ============================================================================
# Router
# ============================================================================


def create_pos_realtime_router(pos_feed_instance):
    """Factory to create router with injected pos_feed."""
    global pos_feed
    pos_feed = pos_feed_instance

    router = APIRouter(prefix="/api/pos/live", tags=["POS Real-time"])

    # ========================================================================
    # Webhook Ingestion
    # ========================================================================

    @router.post("/ingest")
    async def ingest_sale(req: SaleIngestRequest):
        """
        Receive a POS sale event (webhook push).

        Example:
            POST /api/pos/live/ingest
            {
              "venue_id": "venue-123",
              "amount": 45.50,
              "timestamp": "2026-04-27T14:32:15+10:00",
              "transaction_id": "txn_abc123"
            }
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            sale_data = {
                "amount": req.amount,
                "timestamp": req.timestamp,
                "transaction_id": req.transaction_id,
                "category": req.category,
            }

            await pos_feed.ingest_sale(req.venue_id, sale_data)

            return {
                "status": "ok",
                "venue_id": req.venue_id,
                "amount": req.amount,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to ingest sale: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ========================================================================
    # Revenue Retrieval
    # ========================================================================

    @router.get("/revenue/{venue_id}")
    async def get_live_revenue(venue_id: str):
        """
        Get current live revenue for a venue.

        Returns current hour and today's total with transaction counts.

        Example response:
            {
              "venue_id": "venue-123",
              "current_hour": {
                "hour": 14,
                "revenue": 542.50,
                "transaction_count": 12,
                "avg_ticket": 45.21
              },
              "today_total": {
                "date": "2026-04-27",
                "total_revenue": 3250.75,
                "transaction_count": 82,
                "hours_data": [...]
              },
              "timestamp": "2026-04-27T14:35:00Z"
            }
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            data = await pos_feed.get_live_revenue(venue_id)
            if not data:
                return {
                    "venue_id": venue_id,
                    "current_hour": None,
                    "today_total": None,
                    "message": "No revenue data yet",
                }
            return data

        except Exception as e:
            logger.error(f"Failed to get live revenue: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ========================================================================
    # Hourly Breakdown
    # ========================================================================

    @router.get("/hourly/{venue_id}")
    async def get_hourly_breakdown(
        venue_id: str,
        date_str: Optional[str] = Query(None, alias="date"),
    ):
        """
        Get hourly revenue breakdown.

        Query params:
        - date: ISO date string (defaults to today)

        Returns hourly breakdown with transaction counts and averages.

        Example response:
            {
              "venue_id": "venue-123",
              "date": "2026-04-27",
              "total_revenue": 3250.75,
              "transaction_count": 82,
              "hours": [
                {
                  "hour": 11,
                  "revenue": 320.50,
                  "count": 8,
                  "avg_ticket": 40.06
                },
                ...
              ]
            }
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            request_date = None
            if date_str:
                try:
                    request_date = date.fromisoformat(date_str)
                except (ValueError, TypeError):
                    raise HTTPException(status_code=400, detail="Invalid date format")

            data = await pos_feed.get_hourly_breakdown(venue_id, request_date)
            if not data:
                return {
                    "venue_id": venue_id,
                    "message": "No revenue data yet",
                }
            return data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get hourly breakdown: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ========================================================================
    # Variance Monitoring
    # ========================================================================

    @router.get("/variance/{venue_id}")
    async def get_variance(venue_id: str):
        """
        Get variance between actual and forecast revenue.

        Returns the difference as a percentage and status.

        Example response:
            {
              "venue_id": "venue-123",
              "forecast": 3000.00,
              "actual": 3250.75,
              "variance_pct": 8.36,
              "status": "on_track"
            }

        Status values:
        - "on_track": within ±15% of forecast
        - "above_forecast": more than 15% above
        - "below_forecast": more than 15% below
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            data = await pos_feed.get_variance(venue_id)
            if not data:
                return {
                    "venue_id": venue_id,
                    "message": "No revenue data yet",
                }
            return data

        except Exception as e:
            logger.error(f"Failed to get variance: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ========================================================================
    # Polling Control
    # ========================================================================

    @router.post("/start/{venue_id}")
    async def start_polling(venue_id: str, req: PollingStartRequest):
        """
        Start polling POS for a venue.

        Args:
            pos_type: Type of POS system (swiftpos, lightspeed, kounta, etc.)
            interval_seconds: How often to poll (default 60).
            forecast_revenue: Expected daily revenue for variance comparison.

        Example:
            POST /api/pos/live/start/venue-123
            {
              "pos_type": "swiftpos",
              "interval_seconds": 60,
              "forecast_revenue": 3000.00
            }
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            # TODO: Get actual POS adapter based on venue config and pos_type
            # For now, this is a placeholder
            logger.info(
                f"Start polling requested for {venue_id} "
                f"(type={req.pos_type}, interval={req.interval_seconds}s)"
            )

            return {
                "status": "ok",
                "venue_id": venue_id,
                "pos_type": req.pos_type,
                "interval_seconds": req.interval_seconds,
                "message": "Polling started",
            }

        except Exception as e:
            logger.error(f"Failed to start polling: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/stop/{venue_id}")
    async def stop_polling(venue_id: str):
        """
        Stop polling POS for a venue.

        Example:
            POST /api/pos/live/stop/venue-123
        """
        if not pos_feed:
            raise HTTPException(status_code=500, detail="POS feed not initialized")

        try:
            await pos_feed.stop_polling(venue_id)

            return {
                "status": "ok",
                "venue_id": venue_id,
                "message": "Polling stopped",
            }

        except Exception as e:
            logger.error(f"Failed to stop polling: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router
