"""
RosterIQ FastAPI Application

Main API entry point providing REST endpoints for:
- Health checks and system status
- Roster generation and management
- Shift swap requests
- Award interpretation and cost calculations
- Tanda integration webhooks
- Dashboard data feeds
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger("rosteriq")

# --- App Setup ---

app = FastAPI(
    title="RosterIQ API",
    description="AI-powered rostering for Australian hospitality venues",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Health & Status ---

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway/Docker monitoring."""
    return {
        "status": "healthy",
        "service": "rosteriq",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "RosterIQ API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }


# --- Roster Endpoints ---

class RosterRequest(BaseModel):
    venue_id: str
    week_start: str  # ISO date string
    demand_override: dict | None = None


@app.post("/api/v1/rosters/generate")
async def generate_roster(request: RosterRequest):
    """Generate an optimised roster for a venue and week."""
    try:
        from rosteriq.roster_engine import RosterEngine
        # Placeholder - in production this pulls from Tanda/database
        return {
            "status": "success",
            "venue_id": request.venue_id,
            "week_start": request.week_start,
            "message": "Roster generation endpoint active. Connect Tanda integration for live data.",
        }
    except Exception as e:
        logger.error(f"Roster generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Award Engine Endpoints ---

@app.post("/api/v1/awards/calculate")
async def calculate_award_costs(request: Request):
    """Calculate award costs for a given roster or shift set."""
    body = await request.json()
    return {
        "status": "success",
        "message": "Award calculation endpoint active.",
        "data": body,
    }


# --- Shift Swap Endpoints ---

@app.get("/api/v1/swaps")
async def list_swap_requests():
    """List pending shift swap requests."""
    return {"status": "success", "swaps": [], "message": "No pending swaps."}


@app.post("/api/v1/swaps/request")
async def create_swap_request(request: Request):
    """Create a new shift swap request."""
    body = await request.json()
    return {"status": "success", "message": "Swap request created.", "data": body}


# --- Tanda Integration ---

@app.post("/api/v1/webhooks/tanda")
async def tanda_webhook(request: Request):
    """Receive webhook events from Tanda."""
    body = await request.json()
    logger.info(f"Tanda webhook received: {body.get('type', 'unknown')}")
    return {"status": "received"}


@app.get("/api/v1/tanda/status")
async def tanda_status():
    """Check Tanda integration status."""
    tanda_configured = bool(os.getenv("TANDA_CLIENT_ID")) and bool(os.getenv("TANDA_CLIENT_SECRET"))
    return {
        "status": "configured" if tanda_configured else "not_configured",
        "message": "Set TANDA_CLIENT_ID and TANDA_CLIENT_SECRET to enable." if not tanda_configured else "Tanda integration ready.",
    }


# --- Reports ---

@app.get("/api/v1/reports/labour-cost")
async def labour_cost_report(venue_id: str = "demo", period: str = "week"):
    """Get labour cost report for a venue."""
    return {
        "status": "success",
        "venue_id": venue_id,
        "period": period,
        "message": "Report endpoint active. Connect data sources for live reports.",
    }


# --- Dashboard ---

@app.get("/api/v1/dashboard/summary")
async def dashboard_summary(venue_id: str = "demo"):
    """Get dashboard summary data."""
    return {
        "status": "success",
        "venue_id": venue_id,
        "roster_count": 0,
        "active_employees": 0,
        "pending_swaps": 0,
        "message": "Dashboard ready. Connect Tanda for live data.",
    }
