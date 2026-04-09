"""
RosterIQ FastAPI Application v2

Clean pipeline-based architecture for:
- Roster generation and management
- Dashboard data feeds (roster-maker and on-shift)
- Staff call-in and shift management
- Signal aggregation and forecasting
- Award calculations and labour costs

Integrates with RosterIQPipeline for all business logic.
Serves static dashboard at root and provides /api/v1/* endpoints.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field

logger = logging.getLogger("rosteriq.api_v2")

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
STATIC_DIR = PROJECT_ROOT / "static"

# Application version and startup time
APP_VERSION = "2.0.0"
STARTUP_TIME = datetime.now(timezone.utc)


# ============================================================================
# Request/Response Models
# ============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    version: str
    uptime_seconds: float
    timestamp: str
    connected_services: Dict[str, bool]


class RosterGenerateRequest(BaseModel):
    """Request to generate a new roster."""
    venue_id: str = Field(..., description="Venue identifier")
    week_start: str = Field(..., description="ISO date string YYYY-MM-DD")
    demand_override: Optional[Dict[str, Any]] = Field(None, description="Optional demand forecast override")


class RosterGenerateResponse(BaseModel):
    """Response after roster generation."""
    venue_id: str
    week_start: str
    shifts: List[Dict[str, Any]]
    total_labour_cost: float
    total_hours: float
    coverage_score: float
    fairness_score: float
    cost_efficiency_score: float
    warnings: List[str]


class DashboardRosterMakerResponse(BaseModel):
    """Roster-maker dashboard data."""
    venue_id: str
    week_start: str
    roster: List[Dict[str, Any]]
    demand_forecast: List[Dict[str, Any]]
    constraints: Dict[str, Any]
    scores: Dict[str, float]
    timestamp: str


class DashboardOnShiftResponse(BaseModel):
    """On-shift monitoring dashboard data."""
    venue_id: str
    current_revenue: float
    expected_revenue: float
    variance_pct: float
    staff_on_shift: int
    hourly_demand_curve: List[float]
    current_hour: int
    recommended_actions: List[str]
    active_signals: List[Dict[str, Any]]
    timestamp: str


class CallInRequest(BaseModel):
    """Request to handle staff call-in."""
    venue_id: str
    shift_id: str
    reason: str


class CallInResponse(BaseModel):
    """Response after call-in processing."""
    shift_id: str
    venue_id: str
    status: str
    message: str
    timestamp: str


class AwardCalculateRequest(BaseModel):
    """Request to calculate award costs."""
    employee_type: str = Field(..., description="'casual', 'part_time', 'full_time'")
    level: int = Field(..., description="Award level/classification")
    shift_start: str = Field(..., description="ISO datetime")
    shift_end: str = Field(..., description="ISO datetime")
    date: str = Field(..., description="ISO date string YYYY-MM-DD")


class AwardCalculateResponse(BaseModel):
    """Award calculation result."""
    base_pay: float
    penalties: float
    overtime: float
    super_contribution: float
    total: float
    breakdown: Dict[str, float]


class SignalsResponse(BaseModel):
    """Aggregated signals for a date."""
    venue_id: str
    date: str
    weather: Dict[str, Any]
    events: List[Dict[str, Any]]
    bookings: Dict[str, float]
    foot_traffic: Dict[str, float]
    forecast_demand: float
    timestamp: str


class StaffListResponse(BaseModel):
    """Staff list for a venue."""
    venue_id: str
    staff_count: int
    staff: List[Dict[str, Any]]


class ForecastResponse(BaseModel):
    """Weekly demand forecast."""
    venue_id: str
    week_start: str
    daily_forecasts: List[Dict[str, Any]]
    confidence_level: float
    timestamp: str


# ============================================================================
# Lifespan / Startup/Shutdown
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application startup and shutdown.

    Startup: Initialize pipeline and database connections.
    Shutdown: Clean up resources.
    """
    logger.info(f"RosterIQ API v2 starting up (version {APP_VERSION})")

    # Import pipeline here to handle lazy initialization
    try:
        from rosteriq.pipeline import get_pipeline
        pipeline = get_pipeline()
        app.state.pipeline = pipeline
        logger.info("Pipeline initialized successfully")
    except Exception as e:
        logger.warning(f"Pipeline initialization failed (non-critical): {e}")
        app.state.pipeline = None

    yield

    logger.info("RosterIQ API v2 shutting down")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="RosterIQ API",
    description="AI rostering system for Australian hospitality venues",
    version=APP_VERSION,
    lifespan=lifespan,
)

# CORS middleware - allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files directory
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ============================================================================
# Helper Functions
# ============================================================================

def get_pipeline():
    """Get or create pipeline instance from app state."""
    if not hasattr(app.state, "pipeline") or app.state.pipeline is None:
        from rosteriq.pipeline import get_pipeline as factory_get_pipeline
        app.state.pipeline = factory_get_pipeline()
    return app.state.pipeline


def get_uptime_seconds() -> float:
    """Get application uptime in seconds."""
    delta = datetime.now(timezone.utc) - STARTUP_TIME
    return delta.total_seconds()


# ============================================================================
# Static Routes
# ============================================================================

@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    """
    Serve the static dashboard HTML at root.

    Returns the main dashboard interface for roster management.
    """
    dashboard_path = STATIC_DIR / "dashboard.html"
    if not dashboard_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return dashboard_path


# ============================================================================
# Health & Status
# ============================================================================

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint with version and uptime info.

    Returns application status and connected services status.
    """
    try:
        pipeline = get_pipeline()
        pipeline_ok = pipeline is not None
    except Exception:
        pipeline_ok = False

    return HealthResponse(
        status="healthy",
        service="RosterIQ",
        version=APP_VERSION,
        uptime_seconds=get_uptime_seconds(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        connected_services={
            "pipeline": pipeline_ok,
        },
    )


# ============================================================================
# Roster Management
# ============================================================================

@app.post("/api/v1/rosters/generate", response_model=RosterGenerateResponse)
async def generate_roster(request: RosterGenerateRequest) -> RosterGenerateResponse:
    """
    Generate an optimal roster for a venue and week.

    Uses RosterIQPipeline.generate_roster() to create shift assignments
    with cost optimization and fairness constraints.

    Args:
        request: Venue ID and week start date (YYYY-MM-DD)

    Returns:
        Complete roster with shifts, costs, and quality scores
    """
    try:
        pipeline = get_pipeline()
        result = await pipeline.generate_roster(
            venue_id=request.venue_id,
            week_start=request.week_start,
            demand_override=request.demand_override,
        )
        return RosterGenerateResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Roster generation failed for {request.venue_id}")
        raise HTTPException(status_code=500, detail="Roster generation failed")


# ============================================================================
# Dashboards
# ============================================================================

@app.get("/api/v1/dashboard/roster-maker/{venue_id}", response_model=DashboardRosterMakerResponse)
async def get_roster_maker_dashboard(
    venue_id: str,
    week_start: Optional[str] = None,
) -> DashboardRosterMakerResponse:
    """
    Get roster-maker dashboard data.

    Provides current roster, demand forecasts, constraints, and quality scores
    for manual roster optimization by managers.

    Args:
        venue_id: Target venue identifier
        week_start: Optional week start date (defaults to current week)

    Returns:
        Roster, forecasts, constraints, and scoring metrics
    """
    try:
        pipeline = get_pipeline()
        if week_start is None:
            from datetime import date
            week_start = date.today().isoformat()

        result = await pipeline.get_roster_maker_dashboard(
            venue_id=venue_id,
            week_start=week_start,
        )
        return DashboardRosterMakerResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Roster-maker dashboard failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Dashboard fetch failed")


@app.get("/api/v1/dashboard/on-shift/{venue_id}", response_model=DashboardOnShiftResponse)
async def get_on_shift_dashboard(venue_id: str) -> DashboardOnShiftResponse:
    """
    Get on-shift monitoring dashboard data.

    Provides real-time metrics for the current shift including revenue tracking,
    staff counts, demand curve, and recommended actions.

    Args:
        venue_id: Target venue identifier

    Returns:
        Real-time shift data with recommendations
    """
    try:
        pipeline = get_pipeline()
        result = await pipeline.get_on_shift_dashboard(venue_id=venue_id)
        return DashboardOnShiftResponse(**result)
    except Exception as e:
        logger.exception(f"On-shift dashboard failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Dashboard fetch failed")


# ============================================================================
# Staff Management
# ============================================================================

@app.post("/api/v1/staff/call-in", response_model=CallInResponse)
async def handle_call_in(request: CallInRequest) -> CallInResponse:
    """
    Handle staff call-in for a shift.

    Processes shift coverage requests and recommends replacement staff
    or actions based on availability and seniority.

    Args:
        request: Venue, shift ID, and reason for call-in

    Returns:
        Call-in status and recommended action
    """
    try:
        pipeline = get_pipeline()
        result = await pipeline.handle_call_in(
            venue_id=request.venue_id,
            shift_id=request.shift_id,
            reason=request.reason,
        )
        return CallInResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Call-in handling failed for shift {request.shift_id}")
        raise HTTPException(status_code=500, detail="Call-in processing failed")


@app.get("/api/v1/staff/{venue_id}", response_model=StaffListResponse)
async def get_staff(venue_id: str) -> StaffListResponse:
    """
    Get staff list for a venue.

    Returns all staff members with roles, availability, and employment details.

    Args:
        venue_id: Target venue identifier

    Returns:
        Staff list with availability and employment data
    """
    try:
        pipeline = get_pipeline()
        result = await pipeline.get_staff(venue_id=venue_id)
        return StaffListResponse(**result)
    except Exception as e:
        logger.exception(f"Staff fetch failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Staff fetch failed")


# ============================================================================
# Signals & Forecasting
# ============================================================================

@app.get("/api/v1/signals/{venue_id}/{date}", response_model=SignalsResponse)
async def get_signals(venue_id: str, date: str) -> SignalsResponse:
    """
    Get aggregated signals for a venue and date.

    Combines weather, events, bookings, and foot traffic data
    to inform demand forecasting and staffing decisions.

    Args:
        venue_id: Target venue identifier
        date: ISO date string (YYYY-MM-DD)

    Returns:
        Aggregated signals with demand forecast
    """
    try:
        # Validate date format
        from datetime import datetime as dt
        dt.fromisoformat(date)

        pipeline = get_pipeline()
        result = await pipeline.get_signals(venue_id=venue_id, date=date)
        return SignalsResponse(**result)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    except Exception as e:
        logger.exception(f"Signal fetch failed for {venue_id} on {date}")
        raise HTTPException(status_code=500, detail="Signal fetch failed")


@app.get("/api/v1/forecast/{venue_id}/{week_start}", response_model=ForecastResponse)
async def get_forecast(venue_id: str, week_start: str) -> ForecastResponse:
    """
    Get weekly demand forecast for a venue.

    Provides day-by-hour demand predictions based on historical patterns,
    signals, and bookings data.

    Args:
        venue_id: Target venue identifier
        week_start: ISO date string (YYYY-MM-DD)

    Returns:
        Weekly forecast with confidence level
    """
    try:
        # Validate date format
        from datetime import datetime as dt
        dt.fromisoformat(week_start)

        pipeline = get_pipeline()
        result = await pipeline.get_forecast(venue_id=venue_id, week_start=week_start)
        return ForecastResponse(**result)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    except Exception as e:
        logger.exception(f"Forecast fetch failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Forecast fetch failed")


# ============================================================================
# Awards & Costs
# ============================================================================

@app.post("/api/v1/awards/calculate", response_model=AwardCalculateResponse)
async def calculate_award(request: AwardCalculateRequest) -> AwardCalculateResponse:
    """
    Calculate award costs for a shift.

    Computes base pay, penalties, overtime, superannuation, and total cost
    based on Australian award classifications and shift details.

    Args:
        request: Employee type, level, shift times, and date

    Returns:
        Cost breakdown with total compensation
    """
    try:
        pipeline = get_pipeline()
        result = await pipeline.calculate_award(
            employee_type=request.employee_type,
            level=request.level,
            shift_start=request.shift_start,
            shift_end=request.shift_end,
            date=request.date,
        )
        return AwardCalculateResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Award calculation failed")
        raise HTTPException(status_code=500, detail="Award calculation failed")


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Standard HTTP exception handler with timestamp."""
    return {
        "detail": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler for unhandled errors."""
    logger.exception("Unhandled exception in request handler")
    return {
        "detail": "Internal server error",
        "status_code": 500,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
