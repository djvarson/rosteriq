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
    """On-shift monitoring dashboard data.

    Shape matches the contract the frontend consumes at
    static/dashboard.html::populateOnShiftData(). Keep in sync if you
    change either side.
    """
    venue_id: str
    date: str
    current_time: str
    staff_on_deck: List[Dict[str, Any]]       # [{name, role, hours, break}]
    active_signals: List[Dict[str, Any]]      # [{text, alert}]
    recommended_actions: List[str]
    hourly_demand: List[Dict[str, Any]]       # [{hour, expected, actual}]
    revenue_actual: float
    revenue_forecast: float
    revenue_variance_pct: float
    current_demand_multiplier: float
    generated_at: str


class LiveWagePulseResponse(BaseModel):
    """Live wage ticker pulse — real-time wage burn vs forecast.

    Consumed by the on-shift dashboard's wage ticker widget.
    """
    venue_id: str
    timestamp: str
    current_hour: str
    wages_burned_so_far: float       # $ spent on wages today up to now
    wages_forecast_today: float      # $ forecast for the full day
    wages_pct_of_forecast: float     # how much of today's budget is already spent
    revenue_so_far: float
    revenue_forecast_today: float
    current_wage_pct_of_revenue: float
    projected_wage_pct_of_revenue: float
    hourly_burn_rate: float          # $/hour right now
    trend: str                       # "on_track", "overspending", "underspending"
    minutes_remaining: int           # minutes left in trading hours today


class ScenarioSolveRequest(BaseModel):
    """Scenario solver request — wraps three modes in one endpoint.

    Mode is inferred from which fields are populated:
      - SOLVE_SALES:     wage_cost + target_wage_cost_pct, optional forecast_sales
      - SOLVE_WAGE_BUDGET: forecast_sales + target_wage_cost_pct, optional blended_hourly_rate, on_cost_multiplier, planned_wage_cost
      - DIAGNOSE:        wage_cost + forecast_sales, optional target_wage_cost_pct
    """
    mode: str                                              # "solve_sales" | "solve_wage_budget" | "diagnose"
    target_wage_cost_pct: Optional[float] = None           # 0.18 or 18 — solver normalises
    wage_cost: Optional[float] = None                      # loaded $ of labour for the shift/week
    forecast_sales: Optional[float] = None                 # $ forecast
    blended_hourly_rate: Optional[float] = None            # avg $/hr across the roster
    on_cost_multiplier: Optional[float] = None             # 1.165 default, solver supplies if None
    planned_wage_cost: Optional[float] = None              # $ of labour in the current planned roster


class ScenarioSolveResponse(BaseModel):
    """Scenario solver response — mirrors ScenarioResult.to_dict() from
    rosteriq.scenario_solver, flattened for the dashboard to consume.
    """
    mode: str
    target_wage_cost_pct: float
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    assumptions: List[str]
    warnings: List[str]
    suggestions: List[str]


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

    # P0 FIX: Pipelines are now lazily initialized per-venue via the local
    # get_pipeline(venue_id) wrapper below. Don't eagerly construct one here
    # since the factory requires a real venue_id and we don't have one at
    # process startup.
    app.state.pipelines = {}
    logger.info("Pipeline cache initialized (lazy, per-venue)")

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

def get_pipeline(venue_id: Optional[str] = None):
    """Get or create a pipeline instance for a venue from app state.

    P0 FIX: The factory in rosteriq/pipeline.py requires venue_id as a
    mandatory positional argument. The previous no-arg wrapper crashed every
    request with TypeError. We now cache pipelines per-venue in a dict so that
    different venues can share the process without stepping on each other.

    venue_id=None is accepted for compat with award-calc and health checks,
    and falls back to a sentinel "_default" key.
    """
    key = venue_id or "_default"
    if not hasattr(app.state, "pipelines") or app.state.pipelines is None:
        app.state.pipelines = {}
    if key not in app.state.pipelines:
        from rosteriq.pipeline import get_pipeline as factory_get_pipeline
        app.state.pipelines[key] = factory_get_pipeline(venue_id=key)
    return app.state.pipelines[key]


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
        # Health check just verifies the factory can be imported — don't
        # construct a pipeline here since we don't have a venue_id.
        from rosteriq.pipeline import get_pipeline as _factory  # noqa: F401
        pipeline_ok = True
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
        # P0 FIX: factory requires venue_id.
        pipeline = get_pipeline(venue_id=request.venue_id)
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
        # P0 FIX: factory requires venue_id; method kwarg is week_start_date.
        pipeline = get_pipeline(venue_id=venue_id)
        if week_start is None:
            from datetime import date
            week_start = date.today().isoformat()

        result = await pipeline.get_roster_maker_dashboard(
            week_start_date=week_start,
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
        # P0 FIX: factory requires venue_id; method does not take venue_id kwarg.
        pipeline = get_pipeline(venue_id=venue_id)
        result = await pipeline.get_on_shift_dashboard()

        # Adapt the pipeline's internal shape to the frontend's consumer contract.
        rev = result.get("revenue_metrics", {}) or {}
        staff = [
            {
                "name": s.get("name", ""),
                "role": s.get("role", ""),
                "hours": 0.0,      # TODO: surface from shift record once available
                "break": False,    # TODO: surface from break_started_at once available
            }
            for s in (result.get("staff_on_deck", []) or [])
        ]
        signals = [
            {
                "text": s.get("description", ""),
                "alert": (s.get("impact_score", 0) or 0) >= 0.6,
            }
            for s in (result.get("signals_active", []) or [])
        ]
        # staffing_recommendations may be list of dicts OR list of strings
        raw_recs = result.get("staffing_recommendations", []) or []
        actions: List[str] = []
        for r in raw_recs:
            if isinstance(r, str):
                actions.append(r)
            elif isinstance(r, dict):
                actions.append(r.get("text") or r.get("description") or str(r))
        # hourly_curve may be a list of floats OR a list of dicts
        raw_curve = result.get("hourly_curve", []) or []
        hourly: List[Dict[str, Any]] = []
        for i, c in enumerate(raw_curve):
            if isinstance(c, dict):
                hourly.append({
                    "hour": c.get("hour", f"{i:02d}:00"),
                    "expected": c.get("expected", 0),
                    "actual": c.get("actual", 0),
                })
            else:
                hourly.append({
                    "hour": f"{i:02d}:00",
                    "expected": float(c or 0),
                    "actual": 0,
                })

        return DashboardOnShiftResponse(
            venue_id=result.get("venue_id", venue_id),
            date=result.get("date", ""),
            current_time=result.get("current_time", ""),
            staff_on_deck=staff,
            active_signals=signals,
            recommended_actions=actions,
            hourly_demand=hourly,
            revenue_actual=float(rev.get("actual", 0) or 0),
            revenue_forecast=float(rev.get("forecast", 0) or 0),
            revenue_variance_pct=float(rev.get("variance_pct", 0) or 0),
            current_demand_multiplier=float(result.get("current_demand_multiplier", 1.0) or 1.0),
            generated_at=result.get("generated_at", ""),
        )
    except Exception as e:
        logger.exception(f"On-shift dashboard failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Dashboard fetch failed")


@app.get("/api/v1/dashboard/wage-pulse/{venue_id}", response_model=LiveWagePulseResponse)
async def get_live_wage_pulse(venue_id: str) -> LiveWagePulseResponse:
    """
    Live wage ticker — real-time wage burn vs forecast for today.

    Powers the demo's wage ticker widget. Computes wages spent so far vs
    forecast, current burn rate, and projected end-of-day wage %.
    Ported from the top-level api.py /live-pulse endpoint.

    Args:
        venue_id: Target venue identifier

    Returns:
        Live wage metrics for the current trading day.
    """
    try:
        from datetime import datetime as _dt, date as _date
        pipeline = get_pipeline(venue_id=venue_id)

        # Pull today's on-shift snapshot — wages, revenue, hourly curve.
        snapshot = await pipeline.get_on_shift_dashboard()

        now = _dt.now()
        hour = now.hour
        mins = now.minute
        # Assume trading hours 10:00–23:00 (13h). Most AU pubs/restaurants match this.
        TRADING_START = 10
        TRADING_END = 23
        total_trading_mins = (TRADING_END - TRADING_START) * 60
        elapsed_mins = max(0, (hour - TRADING_START) * 60 + mins)
        elapsed_mins = min(elapsed_mins, total_trading_mins)
        elapsed_frac = elapsed_mins / total_trading_mins if total_trading_mins else 0.0
        remaining_mins = max(0, total_trading_mins - elapsed_mins)

        rev = snapshot.get("revenue_metrics", {}) or {}
        revenue_so_far = float(rev.get("actual", 0) or 0)
        revenue_forecast_today = float(rev.get("forecast", 0) or 0)

        # Estimate wage burn from hourly curve (load units → rough $ via assumed rate).
        # If the pipeline doesn't surface wage numbers directly, fall back to a
        # proportional estimate tied to elapsed trading time.
        wages_forecast_today = float(snapshot.get("wages_forecast_today", 0) or 0)
        if wages_forecast_today <= 0:
            # Fallback: hospitality industry median is ~30% of revenue.
            wages_forecast_today = revenue_forecast_today * 0.30

        wages_burned = float(snapshot.get("wages_burned_so_far", 0) or 0)
        if wages_burned <= 0:
            wages_burned = wages_forecast_today * elapsed_frac

        hourly_burn = (wages_burned / (elapsed_mins / 60)) if elapsed_mins > 0 else 0.0

        wages_pct_of_forecast = (wages_burned / wages_forecast_today) if wages_forecast_today > 0 else 0.0
        current_wage_pct = (wages_burned / revenue_so_far) if revenue_so_far > 0 else 0.0
        projected_wage_pct = (wages_forecast_today / revenue_forecast_today) if revenue_forecast_today > 0 else 0.0

        # Trend logic: if burning faster than revenue pace, we're overspending.
        if elapsed_frac == 0:
            trend = "on_track"
        elif wages_pct_of_forecast > elapsed_frac + 0.03:
            trend = "overspending"
        elif wages_pct_of_forecast < elapsed_frac - 0.03:
            trend = "underspending"
        else:
            trend = "on_track"

        return LiveWagePulseResponse(
            venue_id=venue_id,
            timestamp=now.isoformat(),
            current_hour=now.strftime("%H:%M"),
            wages_burned_so_far=round(wages_burned, 2),
            wages_forecast_today=round(wages_forecast_today, 2),
            wages_pct_of_forecast=round(wages_pct_of_forecast, 4),
            revenue_so_far=round(revenue_so_far, 2),
            revenue_forecast_today=round(revenue_forecast_today, 2),
            current_wage_pct_of_revenue=round(current_wage_pct, 4),
            projected_wage_pct_of_revenue=round(projected_wage_pct, 4),
            hourly_burn_rate=round(hourly_burn, 2),
            trend=trend,
            minutes_remaining=int(remaining_mins),
        )
    except Exception as e:
        logger.exception(f"Live wage pulse failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Wage pulse fetch failed")


# ============================================================================
# Scenario solver — "what sales do I need to hit X% wage?" et al.
# ============================================================================

@app.post("/api/v1/scenarios/wage-cost", response_model=ScenarioSolveResponse)
async def solve_wage_scenario(request: ScenarioSolveRequest) -> ScenarioSolveResponse:
    """
    Bidirectional wage-cost scenario solver. Three modes in one endpoint:

    1. solve_sales — "Given this wage cost and target %, what sales do I need?"
       Requires: wage_cost, target_wage_cost_pct
       Optional: forecast_sales (for a gap-to-forecast suggestion)

    2. solve_wage_budget — "Given forecast sales and target %, what's my wage budget?"
       Requires: forecast_sales, target_wage_cost_pct
       Optional: blended_hourly_rate, on_cost_multiplier, planned_wage_cost

    3. diagnose — "Given wage_cost and forecast_sales, what's my current wage%?"
       Requires: wage_cost, forecast_sales
       Optional: target_wage_cost_pct (for green/amber/red colouring)

    The underlying math lives in rosteriq.scenario_solver — zero DB, zero HTTP,
    pure Decimal math so it's unit-testable in isolation.
    """
    try:
        from rosteriq.scenario_solver import (
            ScenarioMode,
            diagnose,
            solve_required_sales,
            solve_wage_budget,
        )
        from decimal import Decimal

        mode = (request.mode or "").strip().lower()

        def _dec(x):
            return Decimal(str(x)) if x is not None else None

        if mode == ScenarioMode.SOLVE_SALES.value:
            if request.wage_cost is None or request.target_wage_cost_pct is None:
                raise ValueError("solve_sales requires wage_cost and target_wage_cost_pct")
            result = solve_required_sales(
                wage_cost=_dec(request.wage_cost),
                target_wage_cost_pct=request.target_wage_cost_pct,
                forecast_sales=_dec(request.forecast_sales),
            )
        elif mode == ScenarioMode.SOLVE_WAGE_BUDGET.value:
            if request.forecast_sales is None or request.target_wage_cost_pct is None:
                raise ValueError("solve_wage_budget requires forecast_sales and target_wage_cost_pct")
            kwargs = {
                "forecast_sales": _dec(request.forecast_sales),
                "target_wage_cost_pct": request.target_wage_cost_pct,
                "blended_hourly_rate": _dec(request.blended_hourly_rate),
                "planned_wage_cost": _dec(request.planned_wage_cost),
            }
            if request.on_cost_multiplier is not None:
                kwargs["on_cost_multiplier"] = _dec(request.on_cost_multiplier)
            result = solve_wage_budget(**kwargs)
        elif mode == ScenarioMode.DIAGNOSE.value:
            if request.wage_cost is None or request.forecast_sales is None:
                raise ValueError("diagnose requires wage_cost and forecast_sales")
            result = diagnose(
                wage_cost=_dec(request.wage_cost),
                forecast_sales=_dec(request.forecast_sales),
                target_wage_cost_pct=request.target_wage_cost_pct,
            )
        else:
            raise ValueError(
                f"Unknown scenario mode '{request.mode}' — use solve_sales, solve_wage_budget, or diagnose"
            )

        return ScenarioSolveResponse(**result.to_dict())

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Scenario solver failed for mode {request.mode}")
        raise HTTPException(status_code=500, detail="Scenario solver failed")


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
        # P0 FIX: factory requires venue_id.
        pipeline = get_pipeline(venue_id=request.venue_id)
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
        # P0 FIX: factory requires venue_id.
        pipeline = get_pipeline(venue_id=venue_id)
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

        # P0 FIX: factory requires venue_id.
        pipeline = get_pipeline(venue_id=venue_id)
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

        # P0 FIX: factory requires venue_id.
        pipeline = get_pipeline(venue_id=venue_id)
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
        # Award calculation is not venue-scoped; wrapper accepts None and
        # uses a cached "_default" pipeline for federal-award math.
        pipeline = get_pipeline(venue_id=None)
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
