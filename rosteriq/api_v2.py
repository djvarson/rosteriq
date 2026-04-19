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
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field

# Access control
try:
    from rosteriq.auth import AccessLevel, require_access, User
except ImportError:
    AccessLevel = None
    require_access = None
    User = None

logger = logging.getLogger("rosteriq.api_v2")

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
STATIC_DIR = PROJECT_ROOT / "static"

# Application version and startup time
APP_VERSION = "2.0.0"
STARTUP_TIME = datetime.now(timezone.utc)

# ── Auth configuration ─────────────────────────────────────────────────────
# Set ROSTERIQ_AUTH_ENABLED=1 to enforce JWT auth on all data endpoints.
# Default: off (demo mode — endpoints are open, no login required).
AUTH_ENABLED = os.getenv("ROSTERIQ_AUTH_ENABLED", "").lower() in ("1", "true", "yes")


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


class DataModeResponse(BaseModel):
    """Data mode status response."""
    mode: str = Field(..., description="Current data mode: 'demo' or 'live'")
    tanda_connected: bool = Field(..., description="Whether real Tanda connection is active")


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
    # Round 8 Track A: surface enriched signals so the dashboard can show
    # *why* the roster looks the way it does (weather/events/POS/bookings/patterns).
    # Always present; empty list when the pipeline didn't enrich.
    signals: List[Dict[str, Any]] = Field(default_factory=list)


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
    recommended_actions: List[Dict[str, Any]]  # [{action, reason, priority}]
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


class AskRequest(BaseModel):
    """Natural-language question for the /ask endpoint."""
    venue_id: str
    question: str
    # Optional override for "today" — primarily for tests / replay.
    today: Optional[str] = None


class AskResponse(BaseModel):
    """Structured answer to a natural-language question.

    When `matched` is True, `query_result` mirrors QueryResult.to_dict()
    from rosteriq.query_library. When False, `reason` explains why the
    router couldn't handle the question and `suggestions` lists example
    phrasings that would work.
    """
    matched: bool
    question: str
    query_result: Optional[Dict[str, Any]] = None
    reason: Optional[str] = None
    suggestions: List[str] = Field(default_factory=list)


class HeadCountLogRequest(BaseModel):
    """Log a head-count change at a venue.

    `delta` is the change: +1 / -1 from the tap buttons, or a larger
    positive/negative integer for a group walking in or a bus leaving.
    `note` is an optional free-text label (e.g. "bus group", "function
    ends") that shows in the timeline and feeds the learning loop.
    """
    venue_id: str
    delta: int = Field(..., description="Signed change in head count")
    note: Optional[str] = Field(None, description="Optional free-text label")
    source: str = Field("button", description="'button' | 'group' | 'reset'")


class HeadCountResetRequest(BaseModel):
    """Hard reset the venue's head count (e.g. at start of shift)."""
    venue_id: str
    count: int = Field(..., ge=0, description="New absolute head count")
    note: Optional[str] = None


class HeadCountEntry(BaseModel):
    """One immutable log entry in the head-count history."""
    timestamp: str
    delta: int
    count_after: int
    note: Optional[str] = None
    source: str


class HeadCountStateResponse(BaseModel):
    """Current head count plus recent timeline for a venue."""
    venue_id: str
    current: int
    updated_at: str
    recent: List[HeadCountEntry]
    total_logged_today: int


class ShiftRecapRevenue(BaseModel):
    """Revenue block inside a shift recap."""
    actual: float
    forecast: float
    delta: float
    delta_pct: float


class ShiftRecapWages(BaseModel):
    """Wages block inside a shift recap."""
    actual: float
    forecast: float
    delta: float
    pct_of_revenue_actual: float
    pct_of_revenue_target: float
    pct_delta: float


class ShiftRecapHeadcount(BaseModel):
    """Head-count roll-up inside a shift recap."""
    peak: int
    peak_time: Optional[str] = None
    last_count: int
    total_taps: int
    reset_count: int


class ShiftRecapAccountability(BaseModel):
    """Accountability roll-up inside a shift recap (Moment 8)."""
    total: int
    pending: int
    accepted: int
    dismissed: int
    estimated_impact_missed_aud: float
    estimated_impact_pending_aud: float
    acceptance_rate: float
    top_missed: List[Dict[str, Any]] = Field(default_factory=list)


class ShiftRecapResponse(BaseModel):
    """End-of-shift recap — the Moment 7/8 'what just happened' card.

    Produced by rosteriq.shift_recap.compose_recap and consumed by
    static/dashboard.html::renderShiftRecap(). Keep shapes in sync.
    """
    venue_id: str
    shift_date: str
    generated_at: str
    revenue: ShiftRecapRevenue
    wages: ShiftRecapWages
    headcount: ShiftRecapHeadcount
    accountability: Optional[ShiftRecapAccountability] = None
    traffic_light: str  # "green" | "amber" | "red"
    summary: str


class AccountabilityRecordRequest(BaseModel):
    """Record a new recommendation in the accountability ledger."""
    venue_id: str
    text: str
    source: str = "manual"
    impact_estimate_aud: Optional[float] = None
    priority: str = "med"  # "low" | "med" | "high"
    rec_id: Optional[str] = None  # if passed, idempotent re-record


class AccountabilityRespondRequest(BaseModel):
    """Mark a pending recommendation as accepted or dismissed."""
    venue_id: str
    rec_id: str
    status: str  # "accepted" | "dismissed"
    note: Optional[str] = None


class AccountabilityEvent(BaseModel):
    """One recommendation event in the accountability ledger."""
    id: str
    venue_id: str
    recorded_at: str
    source: str
    text: str
    impact_estimate_aud: Optional[float] = None
    priority: str
    status: str
    responded_at: Optional[str] = None
    response_note: Optional[str] = None


class AccountabilitySummary(BaseModel):
    """Summary roll-up for the accountability ledger."""
    total: int
    pending: int
    accepted: int
    dismissed: int
    estimated_impact_missed_aud: float
    estimated_impact_pending_aud: float
    acceptance_rate: float


class AccountabilityStateResponse(BaseModel):
    """Full state response for the accountability ledger."""
    venue_id: str
    summary: AccountabilitySummary
    recent: List[AccountabilityEvent]
    generated_at: str


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

    # Round 11: open SQLite (if ROSTERIQ_DB_PATH set), apply schemas,
    # rehydrate in-memory stores. No-op if the env var is unset, so
    # demo/sandbox mode is unaffected.
    try:
        from rosteriq import persistence as _persistence
        _persistence.init_db()
        if _persistence.is_persistence_enabled():
            logger.info("SQLite persistence ENABLED at %s", _persistence.db_path())
        else:
            logger.info("SQLite persistence disabled (in-memory only)")
    except Exception:
        logger.exception("Persistence init failed (non-fatal)")

    # Demo seed: populate realistic pilot data if empty
    try:
        from rosteriq.demo_seed import seed_if_empty
        seed_if_empty("demo-venue-001")
    except Exception:
        logger.exception("Demo seed failed (non-fatal)")

    # P0 FIX: Pipelines are now lazily initialized per-venue via the local
    # get_pipeline(venue_id) wrapper below. Don't eagerly construct one here
    # since the factory requires a real venue_id and we don't have one at
    # process startup.
    app.state.pipelines = {}
    logger.info("Pipeline cache initialized (lazy, per-venue)")

    # Moment 14-follow-on 4: in-process scheduler for weekly digest +
    # Tanda writeback retry sweep. Runs in a daemon thread; jobs are
    # registered further down in the module via _ensure_scheduled_jobs.
    try:
        _ensure_scheduled_jobs()
        _start_scheduler_thread_if_enabled()
    except Exception:
        logger.exception("Failed to start scheduled jobs")

    # Auth: mount routes + create demo user if auth is enabled
    try:
        from rosteriq.auth import setup_auth
        setup_auth(app)
        if AUTH_ENABLED:
            logger.info("Authentication ENABLED — JWT required on data endpoints")
        else:
            logger.info("Authentication routes mounted (demo mode — endpoints open)")
    except Exception:
        logger.exception("Failed to setup auth module (non-fatal)")

    # Scheduled briefs: start the asyncio background scheduler that delivers
    # morning/weekly/portfolio briefs to subscribed recipients via SMS + email.
    try:
        from rosteriq.brief_lifecycle import start_briefs_on_startup
        await start_briefs_on_startup()
        logger.info("Scheduled brief dispatcher started")
    except Exception:
        logger.exception("Failed to start scheduled brief dispatcher (non-fatal)")

    yield

    logger.info("RosterIQ API v2 shutting down")
    try:
        from rosteriq import scheduled_jobs as _sj
        _sj.get_global_scheduler().stop(join_timeout_s=2.0)
    except Exception:
        logger.exception("Scheduler shutdown failed")
    try:
        from rosteriq.brief_lifecycle import stop_briefs_on_shutdown
        await stop_briefs_on_shutdown()
    except Exception:
        logger.exception("Brief dispatcher shutdown failed")


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


# ── Auth middleware (path-based) ───────────────────────────────────────────
# When ROSTERIQ_AUTH_ENABLED=1, all /api/v1/* endpoints except health,
# auth routes, and the static dashboard require a valid JWT Bearer token.
_AUTH_OPEN_PREFIXES = (
    "/api/v1/health",
    "/api/v1/auth/",     # login, register, refresh
    "/docs",
    "/openapi.json",
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Enforce JWT auth on /api/v1/* paths when auth is enabled."""
    path = request.url.path

    # Skip auth check entirely if disabled or path is open
    if (
        not AUTH_ENABLED
        or not path.startswith("/api/")
        or any(path.startswith(p) for p in _AUTH_OPEN_PREFIXES)
    ):
        return await call_next(request)

    # Validate Bearer token. Lazy import so the module still loads when
    # pyjwt/passlib aren't installed (demo/sandbox); if the import fails
    # while AUTH_ENABLED is true we fail closed with a 503.
    try:
        from rosteriq.auth import decode_token, get_user_by_id
    except Exception as _auth_import_err:  # pragma: no cover - infra
        logger.error("Auth stack unavailable: %s", _auth_import_err)
        return PlainTextResponse(
            "Auth subsystem unavailable", status_code=503,
        )
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return PlainTextResponse("Missing Bearer token", status_code=401)
    token = auth_header.split(" ", 1)[1]
    try:
        payload = decode_token(token)
    except Exception as e:
        return PlainTextResponse(f"Invalid token: {e}", status_code=401)

    user = get_user_by_id(payload.get("sub", ""))
    if not user:
        return PlainTextResponse("User not found", status_code=401)

    # Attach user to request state for downstream use
    request.state.user = user
    return await call_next(request)


# ============================================================================
# Auth Dependencies
# ============================================================================

async def _get_current_user_if_auth():
    """Return the current User when auth is enabled, None otherwise.

    This is a FastAPI dependency — endpoints can declare it but never
    block callers in demo mode."""
    if not AUTH_ENABLED:
        return None
    # Lazy import so the module loads even when pyjwt / passlib aren't installed
    from rosteriq.auth import get_current_user
    from fastapi.security import HTTPBearer, HTTPAuthenticationCredentials
    bearer = HTTPBearer(auto_error=True)
    # get_current_user is itself an async dependency; we can't call it
    # directly because it expects an HTTPAuthenticationCredentials.  Return
    # it as a sub-dependency instead.
    return get_current_user


async def require_auth(request: Request):
    """FastAPI dependency that enforces JWT auth when AUTH_ENABLED.

    In demo mode this is a no-op.  In auth mode it extracts and validates
    the Bearer token, returning the User or raising 401.
    """
    if not AUTH_ENABLED:
        return None
    from rosteriq.auth import decode_token, get_user_by_id
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth_header.split(" ", 1)[1]
    try:
        payload = decode_token(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))
    user = get_user_by_id(payload.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


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


# ---------------------------------------------------------------------------
# Head-count clicker — backed by rosteriq.headcount_store
# ---------------------------------------------------------------------------
# The store lives in a pure-stdlib sibling module so tests don't need FastAPI
# in the environment. The endpoints below delegate all logic there; this
# module just handles HTTP plumbing and error responses.

from rosteriq import headcount_store as _hc_store


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


@app.get("/api/v1/data-mode", response_model=DataModeResponse)
async def get_data_mode() -> DataModeResponse:
    """
    Get current data mode status and Tanda connection health.

    Returns:
        mode: 'demo' or 'live' - the currently active data mode
        tanda_connected: True if real Tanda connection is active, False otherwise
    """
    try:
        pipeline = get_pipeline()
        status = await pipeline.get_data_mode_status()
        return DataModeResponse(
            mode=status["mode"],
            tanda_connected=status["tanda_connected"],
        )
    except Exception as e:
        logger.warning(f"Error getting data mode status: {e}")
        return DataModeResponse(
            mode="demo",
            tanda_connected=False,
        )


# ============================================================================
# Roster Management
# ============================================================================

@app.post("/api/v1/rosters/generate", response_model=RosterGenerateResponse)
async def generate_roster(
    request: RosterGenerateRequest,
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)) if require_access else None,
) -> RosterGenerateResponse:
    """
    Generate an optimal roster for a venue and week.

    Requires: L2 Roster Maker or higher

    Uses RosterIQPipeline.generate_roster() to create shift assignments
    with cost optimization and fairness constraints.

    Args:
        request: Venue ID and week start date (YYYY-MM-DD)
        user: Current authenticated user (must be L2 or OWNER)

    Returns:
        Complete roster with shifts, costs, and quality scores
    """
    try:
        # P0 FIX: factory requires venue_id; method only takes week_start_date.
        # demand_override is currently not consumed by the pipeline — logged as
        # a TODO so callers can see when their override is ignored.
        pipeline = get_pipeline(venue_id=request.venue_id)
        if request.demand_override:
            logger.info(
                "demand_override provided for venue %s but pipeline does not "
                "currently consume it; ignoring", request.venue_id,
            )
        result = await pipeline.generate_roster(week_start_date=request.week_start)
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
        # staffing_recommendations may be list of dicts OR list of strings.
        # Normalise to rich dict shape so the dashboard can render Accept /
        # Dismiss buttons that hit the accountability endpoints directly.
        raw_recs = result.get("staffing_recommendations", []) or []
        actions: List[Dict[str, Any]] = []
        for r in raw_recs:
            if isinstance(r, str):
                actions.append({
                    "action": r,
                    "reason": "",
                    "priority": "med",
                })
            elif isinstance(r, dict):
                actions.append({
                    "action": r.get("text") or r.get("description") or str(r),
                    "reason": r.get("reason") or "",
                    "priority": (r.get("priority") or "med"),
                    "impact_estimate_aud": r.get("impact_estimate_aud"),
                })

        # Moment 10 — merge any pending wage-pulse recs from the
        # accountability store into the recommended-actions list so the
        # dashboard's Recommended Actions panel surfaces them alongside
        # the pipeline's own staffing recommendations. rec_id is
        # supplied so the dashboard skips its client-side hash and
        # wires Accept / Dismiss straight to the server-owned id.
        try:
            pulse_recs = [
                ev for ev in _acct_store.history(venue_id)
                if ev.get("source") == "wage_pulse" and ev.get("status") == "pending"
            ]
            # Newest first so the most recent alert lands at the top of the panel.
            pulse_recs.sort(key=lambda e: e.get("recorded_at") or "", reverse=True)
            for ev in pulse_recs:
                actions.insert(0, {
                    "rec_id": ev.get("id"),
                    "action": ev.get("text") or "",
                    "reason": "Live wage pulse",
                    "priority": ev.get("priority") or "med",
                    "impact_estimate_aud": ev.get("impact_estimate_aud"),
                    "source": "wage_pulse",
                })
        except Exception:
            logger.exception(
                "Failed to merge pulse recs into on-shift response for %s", venue_id,
            )
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

        response = LiveWagePulseResponse(
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

        # Moment 10 — hand the pulse snapshot to the rec bridge. The
        # bridge is idempotent per (venue, date, severity-bucket) so
        # calling it on every poll is safe. Recs flow into the
        # accountability store and surface in the on-shift dashboard
        # alongside existing staffing recommendations.
        try:
            try:
                pulse_dict = response.model_dump()  # pydantic v2
            except AttributeError:
                pulse_dict = response.dict()        # pydantic v1
            _pulse_rec_bridge.record_pulse_recs(pulse_dict)
        except Exception:
            # Never let the bridge break the pulse widget — log and swallow.
            logger.exception("pulse_rec_bridge failed for %s", venue_id)

        return response
    except Exception as e:
        logger.exception(f"Live wage pulse failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Wage pulse fetch failed")


# ============================================================================
# Scenario solver — "what sales do I need to hit X% wage?" et al.
# ============================================================================

@app.post("/api/v1/scenarios/wage-cost", response_model=ScenarioSolveResponse)
async def solve_wage_scenario(
    request: ScenarioSolveRequest,
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)) if require_access else None,
) -> ScenarioSolveResponse:
    """
    Bidirectional wage-cost scenario solver. Three modes in one endpoint:

    Requires: L2 Roster Maker or higher

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
# Natural-language Ask endpoint — Moment 3
# ============================================================================

@app.post("/api/v1/ask", response_model=AskResponse)
async def ask_question(
    request: AskRequest,
    user: User = Depends(require_access(AccessLevel.L2_ROSTER_MAKER)) if require_access else None,
) -> AskResponse:
    """
    Natural-language question answering. The router lives in
    rosteriq.query_library and is purely deterministic: same question +
    same context = byte-identical answer, every time. No LLM, no
    temperature, no prompt drift.

    Requires: L2 Roster Maker or higher

    Supported phrasings include:
      "sales last week"
      "total wage cost last month"
      "wage % last saturday"
      "last 4 saturdays"
      "busiest day this month"
      "peak head count yesterday"
      "which days over 30% last month"
      "overtime hours last week"
      "hours by employee last week"

    The context is currently built from synthetic demo data via
    rosteriq.ask_context.build_demo_query_context so the feature works
    end-to-end on Railway without needing the pilot venue's POS
    connection live. When a real venue is connected, swap the context
    builder for a DB-backed one and the query_library itself stays
    unchanged.
    """
    try:
        from datetime import date as _date
        from rosteriq.query_library import route_question, list_supported_queries
        from rosteriq.ask_context import build_demo_query_context

        question = (request.question or "").strip()
        if not question:
            raise HTTPException(status_code=400, detail="question cannot be empty")

        # Resolve "today" from the request or fall back to real today
        if request.today:
            try:
                today = _date.fromisoformat(request.today)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"today must be YYYY-MM-DD (got {request.today!r})",
                )
        else:
            today = _date.today()

        ctx = build_demo_query_context(
            venue_id=request.venue_id,
            today=today,
        )
        router_result = route_question(question, ctx)

        if router_result.matched and router_result.query_result is not None:
            return AskResponse(
                matched=True,
                question=question,
                query_result=router_result.query_result.to_dict(),
                reason=None,
                suggestions=[],
            )

        # Unmatched — surface some example phrasings to help the user
        example_phrasings = [
            "sales last week",
            "wage % last saturday",
            "busiest day this month",
            "last 4 saturdays",
            "peak head count yesterday",
            "which days over 30% last month",
        ]
        return AskResponse(
            matched=False,
            question=question,
            query_result=None,
            reason=router_result.reason or "Couldn't match the question to a known query.",
            suggestions=example_phrasings,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Ask endpoint failed for question {request.question!r}")
        raise HTTPException(status_code=500, detail="Ask endpoint failed")


# ============================================================================
# Head-count clicker (Moment 6: on-shift tools)
# ============================================================================

@app.get("/api/v1/headcount/{venue_id}", response_model=HeadCountStateResponse)
async def get_head_count(venue_id: str) -> HeadCountStateResponse:
    """
    Return the current head count, last-updated timestamp, and the most
    recent entries (newest first) for the given venue.

    First call for a venue seeds the history with a single 'start of
    shift' entry at count 0.
    """
    try:
        return HeadCountStateResponse(**_hc_store.state(venue_id))
    except Exception:
        logger.exception(f"Head-count GET failed for venue {venue_id!r}")
        raise HTTPException(status_code=500, detail="Head-count state unavailable")


@app.post("/api/v1/headcount/log", response_model=HeadCountStateResponse)
async def log_head_count(request: HeadCountLogRequest) -> HeadCountStateResponse:
    """
    Append a delta to a venue's head count and return the updated state.

    `delta` can be positive or negative; the store clamps the result at 0
    so a duty manager cannot roll the count below empty. `note` and
    `source` are both preserved verbatim on the entry and shown in the
    dashboard timeline.
    """
    try:
        if request.delta == 0:
            raise HTTPException(
                status_code=400,
                detail="delta must be non-zero (use /headcount/reset to set an absolute value)",
            )
        _hc_store.apply_delta(
            venue_id=request.venue_id,
            delta=int(request.delta),
            note=(request.note or None),
            source=request.source or "button",
        )
        return HeadCountStateResponse(**_hc_store.state(request.venue_id))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Head-count LOG failed")
        raise HTTPException(status_code=500, detail="Head-count log failed")


@app.post("/api/v1/headcount/reset", response_model=HeadCountStateResponse)
async def reset_head_count(request: HeadCountResetRequest) -> HeadCountStateResponse:
    """
    Hard-reset a venue's head count to an absolute value. Appends a
    'reset' entry to history (rather than wiping it) so accountability
    stays intact.
    """
    try:
        _hc_store.reset(
            venue_id=request.venue_id,
            count=int(request.count),
            note=(request.note or None),
        )
        return HeadCountStateResponse(**_hc_store.state(request.venue_id))
    except Exception:
        logger.exception("Head-count RESET failed")
        raise HTTPException(status_code=500, detail="Head-count reset failed")


# ============================================================================
# Shift Recap — Moment 7 (end-of-shift "what just happened" card)
# ============================================================================

from rosteriq import shift_recap as _shift_recap
from rosteriq import accountability_store as _acct_store
from rosteriq import pulse_rec_bridge as _pulse_rec_bridge
from rosteriq import portfolio_recap as _portfolio_recap
from rosteriq import morning_brief as _morning_brief
from rosteriq import brief_dispatcher as _brief_dispatcher
from rosteriq import trends as _trends
from rosteriq import tanda_writeback as _tanda_writeback
from rosteriq import weekly_digest as _weekly_digest


async def _build_venue_shift_recap(venue_id: str) -> Dict[str, Any]:
    """Build a single venue's shift-recap dict (pre-pydantic).

    Pulled out so both the per-venue shift-recap endpoint and the
    portfolio endpoint can reuse the same plumbing.
    """
    pipeline = get_pipeline(venue_id=venue_id)

    revenue_actual = 0.0
    revenue_forecast = 0.0
    wages_actual: Optional[float] = None
    wages_forecast: Optional[float] = None
    shift_date = datetime.now().date().isoformat()

    try:
        snapshot = await pipeline.get_on_shift_dashboard()
        rev = snapshot.get("revenue_metrics", {}) or {}
        revenue_actual = float(rev.get("actual", 0) or 0)
        revenue_forecast = float(rev.get("forecast", 0) or 0)
        if snapshot.get("wages_burned_so_far") is not None:
            wages_actual = float(snapshot.get("wages_burned_so_far") or 0)
        if snapshot.get("wages_forecast_today") is not None:
            wages_forecast = float(snapshot.get("wages_forecast_today") or 0)
        shift_date = snapshot.get("date", shift_date) or shift_date
    except Exception:
        logger.exception(
            "Shift recap: on-shift snapshot failed for %s, recap will use zeros",
            venue_id,
        )

    hc_history = _hc_store.history(venue_id)
    acct_history = _acct_store.history(venue_id)

    wage_target_pct = _shift_recap.DEFAULT_WAGE_TARGET_PCT
    try:
        constraints = getattr(pipeline, "constraints", None)
        if constraints is not None:
            target = getattr(constraints, "target_wage_cost_pct", None)
            if target is not None:
                wage_target_pct = float(target)
    except Exception:
        pass

    return _shift_recap.compose_recap(
        venue_id=venue_id,
        shift_date=shift_date,
        revenue_actual=revenue_actual,
        revenue_forecast=revenue_forecast,
        wages_actual=wages_actual,
        wages_forecast=wages_forecast,
        wage_target_pct=wage_target_pct,
        headcount_history=hc_history,
        recommendations=acct_history,
    )


@app.get("/api/v1/shift-recap/{venue_id}", response_model=ShiftRecapResponse)
async def get_shift_recap(venue_id: str) -> ShiftRecapResponse:
    """
    Composite end-of-shift recap for a venue.

    Pulls today's revenue/wages from the on-shift snapshot, reads the
    head-count timeline from the in-process head-count store, reads the
    accountability ledger from the in-process accountability store, and
    delegates to rosteriq.shift_recap.compose_recap for all the maths
    and the one-line natural-language summary.

    Safe to call mid-shift — the numbers just reflect 'so far'.
    """
    try:
        recap = await _build_venue_shift_recap(venue_id)
        return ShiftRecapResponse(**recap)
    except Exception:
        logger.exception(f"Shift recap failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Shift recap failed")


# ============================================================================
# Portfolio Recap — Moment 9 (multi-venue roll-up for group operators)
# ============================================================================

class PortfolioRecapResponse(BaseModel):
    """Multi-venue portfolio roll-up — shape matches portfolio_recap.compose_portfolio."""
    portfolio_id: str
    shift_date: str
    generated_at: str
    traffic_light: str  # "green" | "amber" | "red" | "unknown"
    summary: str
    totals: Dict[str, Any]
    accountability: Dict[str, Any]
    venues: List[Dict[str, Any]]


# ============================================================================
# Venue list — lightweight discovery endpoint
# ============================================================================
# Consolidates venue IDs + labels from:
# 1. An env-var JSON string (ROSTERIQ_VENUES), e.g.
#    [{"id":"venue_demo_001","label":"Mojo's Bar"}, ...]
# 2. The brief_dispatcher venue registry (runtime-registered venues).
# The result is deduplicated by venue_id (env-var wins).

_STATIC_VENUES: List[Dict[str, str]] = []
_raw_venues_json = os.environ.get("ROSTERIQ_VENUES", "").strip()
if _raw_venues_json:
    try:
        _STATIC_VENUES = json.loads(_raw_venues_json)
    except Exception:
        logger.warning("Could not parse ROSTERIQ_VENUES JSON, falling back to empty")

# If no env-var, fall back to the demo set so the dashboard works
# out of the box. Production deployments should set ROSTERIQ_VENUES
# or register venues at runtime via /api/v1/brief-dispatch/register.
if not _STATIC_VENUES:
    _STATIC_VENUES = [
        {"id": "venue_demo_001", "label": "Mojo's Bar"},
        {"id": "venue_demo_002", "label": "Earl's Kitchen"},
        {"id": "venue_demo_003", "label": "Francine's"},
    ]


@app.get("/api/v1/venues", response_model=Dict[str, Any])
async def list_venues() -> Dict[str, Any]:
    """
    Return the known venue list.

    Sources: the ROSTERIQ_VENUES env var (or demo fallback) plus any
    venues registered at runtime through the brief-dispatch registry.
    Deduplicated by venue_id (static config wins over registry).

    Response::

        {
            "venues": [
                {"id": "venue_demo_001", "label": "Mojo's Bar"},
                ...
            ],
            "default_venue_id": "venue_demo_001"
        }
    """
    seen: Dict[str, Dict[str, str]] = {}
    # Static / env-var venues first — they win on conflict.
    for v in _STATIC_VENUES:
        vid = str(v.get("id") or "").strip()
        if vid:
            seen[vid] = {"id": vid, "label": str(v.get("label") or vid)}
    # Runtime-registered venues fill in any gaps.
    for vid, entry in _brief_dispatcher.get_registry().items():
        vid = str(vid).strip()
        if vid and vid not in seen:
            seen[vid] = {"id": vid, "label": str(entry.get("label") or vid)}
    venues = sorted(seen.values(), key=lambda v: v["label"])
    default_id = _STATIC_VENUES[0]["id"] if _STATIC_VENUES else (venues[0]["id"] if venues else "")
    return {
        "venues": venues,
        "default_venue_id": default_id,
    }


@app.get("/api/v1/portfolio/recap", response_model=PortfolioRecapResponse)
async def get_portfolio_recap(
    request: Request,
    portfolio_id: str = "",
    include_trends: bool = False,
    trend_window_days: int = 7,
    user: User = Depends(require_access(AccessLevel.OWNER)) if require_access else None,
) -> PortfolioRecapResponse:
    """
    Portfolio recap for 2+ venues — the Tier-3 group-operator view.

    Query params:
        venue_ids: repeat the param for each venue — e.g.
            ``?venue_ids=venue_a&venue_ids=venue_b&venue_ids=venue_c``.
            Also accepts a single comma-separated value for convenience.
        labels: optional ``{venue_id}={human_name}`` pairs, repeated:
            ``?labels=venue_a=Mojo's&labels=venue_b=Earl's``.
        portfolio_id: optional free-form group identifier.
        include_trends: when True, each mini-card includes a compact
            trends overlay (7/14/28-day sparkline + headline) pulled
            from the accountability store. Defaults False.
        trend_window_days: window passed to the trends composer when
            ``include_trends`` is True. 7, 14, or 28.

    Returns:
        A rolled-up recap across the requested venues: worst-of traffic
        light, aggregated revenue/wages/headcount, aggregated
        accountability block (counts + missed $), and a per-venue mini
        summary array sorted red-first for the dashboard's sub-cards.
    """
    try:
        # Parse venue_ids from query params — supports both
        # ?venue_ids=a&venue_ids=b AND ?venue_ids=a,b,c forms.
        raw_venue_ids = request.query_params.getlist("venue_ids") if hasattr(
            request.query_params, "getlist"
        ) else request.query_params.get("venue_ids", "").split(",")
        venue_ids: List[str] = []
        for v in raw_venue_ids:
            for part in (v or "").split(","):
                part = part.strip()
                if part:
                    venue_ids.append(part)
        if not venue_ids:
            raise HTTPException(
                status_code=400,
                detail="venue_ids query param is required (at least one venue)",
            )

        # Parse optional labels — ?labels=venue_a=Mojo's&labels=venue_b=Earl's
        venue_labels: Dict[str, str] = {}
        raw_labels = request.query_params.getlist("labels") if hasattr(
            request.query_params, "getlist"
        ) else []
        for lab in raw_labels:
            if "=" in lab:
                k, _, v = lab.partition("=")
                if k:
                    venue_labels[k.strip()] = v.strip()

        # Build each venue's recap, tolerating per-venue failures —
        # one broken venue should not black out the whole portfolio.
        venue_recaps: List[Dict[str, Any]] = []
        for vid in venue_ids:
            try:
                r = await _build_venue_shift_recap(vid)
                venue_recaps.append(r)
            except Exception:
                logger.exception("Portfolio: venue recap failed for %s", vid)
                # Emit a placeholder zero-recap so the venue still
                # appears in the UI (marked unknown) rather than
                # vanishing silently.
                venue_recaps.append({
                    "venue_id": vid,
                    "shift_date": datetime.now().date().isoformat(),
                    "traffic_light": "unknown",
                    "summary": "Recap unavailable for this venue.",
                    "revenue": {"actual": 0, "forecast": 0, "delta": 0, "delta_pct": 0},
                    "wages": {
                        "actual": 0, "forecast": 0,
                        "pct_of_revenue_actual": 0, "pct_of_revenue_target": 0,
                        "pct_delta": 0,
                    },
                    "headcount": {"peak": 0, "peak_time": None, "total_taps": 0},
                    "accountability": {
                        "total": 0, "pending": 0, "accepted": 0, "dismissed": 0,
                        "estimated_impact_missed_aud": 0, "estimated_impact_pending_aud": 0,
                        "acceptance_rate": 0, "top_missed": [],
                    },
                })

        result = _portfolio_recap.compose_portfolio(
            venue_recaps,
            portfolio_id=portfolio_id or None,
            venue_labels=venue_labels or None,
            include_trends=bool(include_trends),
            trend_window_days=int(trend_window_days or 7),
        )
        return PortfolioRecapResponse(**result)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Portfolio recap failed")
        raise HTTPException(status_code=500, detail="Portfolio recap failed")


# ============================================================================
# Morning Brief — Moment 11 (next-day accountability digest)
# ============================================================================

class MorningBriefResponse(BaseModel):
    """Morning brief payload — shape matches morning_brief.compose_brief."""

    venue_id: str
    venue_label: str
    date: str
    generated_at: str
    traffic_light: str
    headline: str
    one_thing: str
    summary: str
    rollup: Dict[str, Any]
    top_dismissed: List[Dict[str, Any]]
    recap_context: Dict[str, Any]


@app.get("/api/v1/morning-brief/{venue_id}", response_model=MorningBriefResponse)
async def get_morning_brief(
    venue_id: str,
    date: str = "",
    venue_label: str = "",
) -> MorningBriefResponse:
    """
    Morning accountability brief — "yesterday cost you $X in dismissed
    recs, here's the one thing to do differently today."

    Query params:
        date: YYYY-MM-DD string to review. Defaults to yesterday (UTC).
        venue_label: Optional human-friendly venue name for the header.

    Pulls events straight from the in-memory accountability store and
    optionally incorporates the prior-day shift recap if the venue has
    recap data available. The brief is deterministic — same events +
    same date always produce the same brief.
    """
    try:
        target_date = date.strip() or None
        label = venue_label.strip() or None

        # Try to pull yesterday's shift recap for full context. If the
        # venue has no recap data (or today's compose_recap throws), we
        # still return a brief — just without the recap_context block.
        yesterday_recap: Optional[Dict[str, Any]] = None
        try:
            recap = await _build_venue_shift_recap(venue_id)
            if isinstance(recap, dict):
                yesterday_recap = recap
        except Exception:
            logger.debug("Morning brief: no recap available for %s", venue_id)

        brief = _morning_brief.compose_brief_from_store(
            venue_id,
            target_date=target_date,
            yesterday_recap=yesterday_recap,
            venue_label=label,
        )
        return MorningBriefResponse(**brief)
    except Exception:
        logger.exception("Morning brief failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Morning brief failed")


@app.get("/api/v1/morning-brief/{venue_id}/text", response_class=PlainTextResponse)
async def get_morning_brief_text(
    venue_id: str,
    date: str = "",
    venue_label: str = "",
) -> str:
    """
    Plain-text version of the morning brief — suitable for piping to an
    email job, a Slack post, or a cron-driven ``curl`` in the meantime.

    Same parameters as the JSON variant.
    """
    try:
        target_date = date.strip() or None
        label = venue_label.strip() or None
        yesterday_recap: Optional[Dict[str, Any]] = None
        try:
            yesterday_recap = await _build_venue_shift_recap(venue_id)
        except Exception:
            yesterday_recap = None
        brief = _morning_brief.compose_brief_from_store(
            venue_id,
            target_date=target_date,
            yesterday_recap=yesterday_recap,
            venue_label=label,
        )
        return _morning_brief.render_text(brief)
    except Exception:
        logger.exception("Morning brief (text) failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Morning brief failed")


# ============================================================================
# Brief Dispatcher — Moment 12 (route the daily digest to its audience)
# ============================================================================

# Default sinks: stdout for the logs, and a file sink writing to
# /tmp/rosteriq_briefs so Railway deploys have a tail-able artifact.
# Real deployments can POST to /api/v1/brief-dispatch/sinks to add a
# webhook without a redeploy.
_DEFAULT_BRIEF_DIR = os.environ.get("ROSTERIQ_BRIEF_DIR", "/tmp/rosteriq_briefs")
try:
    _brief_dispatcher.register_sink(_brief_dispatcher.StdoutSink())
    _brief_dispatcher.register_sink(_brief_dispatcher.FileSink(_DEFAULT_BRIEF_DIR))
    logger.info("Brief dispatcher: default sinks registered (stdout, file=%s)", _DEFAULT_BRIEF_DIR)
except Exception:
    logger.exception("Brief dispatcher: default sink registration failed")


class VenueRegistrationRequest(BaseModel):
    """Register a venue for the morning brief cron."""

    venue_id: str
    label: Optional[str] = None
    sinks: Optional[List[str]] = None


class DispatchResultResponse(BaseModel):
    """Result of a single-venue or fan-out dispatch."""

    results: List[Dict[str, Any]]
    summary: Dict[str, Any]


async def _brief_recap_fetcher(venue_id: str) -> Optional[Dict[str, Any]]:
    """Adapter so dispatch_all can enrich briefs with the venue's
    most recent shift recap. Tolerates failures — the dispatcher
    will fall back to a recap-less brief if this returns None."""
    try:
        return await _build_venue_shift_recap(venue_id)
    except Exception:
        logger.debug("brief dispatcher: recap fetch failed for %s", venue_id)
        return None


@app.get("/api/v1/brief-dispatch/registry", response_model=Dict[str, Any])
async def list_brief_registry() -> Dict[str, Any]:
    """Return the current venue registry + registered sink names."""
    registry = _brief_dispatcher.get_registry()
    sinks = sorted(list(_brief_dispatcher.get_sinks().keys()))
    return {
        "venues": list(registry.values()),
        "sinks": sinks,
    }


@app.post("/api/v1/brief-dispatch/register", response_model=Dict[str, Any])
async def register_brief_venue(request: VenueRegistrationRequest) -> Dict[str, Any]:
    """
    Register (or update) a venue for the daily brief.

    Body:
        venue_id: required
        label:    optional, defaults to venue_id
        sinks:    optional list of sink names — must match
                  names returned from GET /brief-dispatch/registry
    """
    try:
        entry = _brief_dispatcher.register_venue(
            request.venue_id,
            label=request.label,
            sinks=request.sinks,
        )
        return {"status": "ok", "venue": entry}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Brief dispatcher: register_venue failed")
        raise HTTPException(status_code=500, detail="Registration failed")


@app.post("/api/v1/brief-dispatch/unregister/{venue_id}", response_model=Dict[str, Any])
async def unregister_brief_venue(venue_id: str) -> Dict[str, Any]:
    """Remove a venue from the dispatch registry."""
    _brief_dispatcher.unregister_venue(venue_id)
    return {"status": "ok", "venue_id": venue_id}


@app.post("/api/v1/brief-dispatch/run", response_model=DispatchResultResponse)
async def run_brief_dispatch(date: str = "") -> DispatchResultResponse:
    """
    Trigger a dispatch cycle right now — walks every registered venue,
    composes that venue's brief for ``date`` (defaults to yesterday),
    and fans out to each venue's configured sinks.

    This is the endpoint a cron or scheduled-tasks worker hits at 7am:

        POST /api/v1/brief-dispatch/run

    Returns the full result dict with per-venue brief bodies and
    delivery statuses.
    """
    target_date = date.strip() or None

    # We need an async-capable recap fetcher, but dispatch_all is
    # synchronous. Pre-fetch recaps here and hand the dispatcher a
    # simple sync lookup. The recap fetch is best-effort and tolerates
    # per-venue failures.
    registry = _brief_dispatcher.get_registry()
    recap_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    for vid in registry.keys():
        try:
            recap_cache[vid] = await _build_venue_shift_recap(vid)
        except Exception:
            recap_cache[vid] = None

    def fetcher(vid: str) -> Optional[Dict[str, Any]]:
        return recap_cache.get(vid)

    try:
        result = _brief_dispatcher.dispatch_all(
            target_date=target_date,
            recap_fetcher=fetcher,
        )
        return DispatchResultResponse(**result)
    except Exception:
        logger.exception("Brief dispatch run failed")
        raise HTTPException(status_code=500, detail="Dispatch failed")


@app.post("/api/v1/weekly-digest/dispatch/{venue_id}", response_model=Dict[str, Any])
async def dispatch_weekly_digest_one(
    venue_id: str,
    week_ending: str = "",
    window_days: int = 7,
    only_when_should_send: bool = False,
) -> Dict[str, Any]:
    """
    Dispatch a single venue's weekly digest through the same sink
    fan-out the morning brief uses. Returns the delivered digest plus
    per-sink status.

    The digest flows through whichever sinks the venue has registered
    via ``/api/v1/brief-dispatch/register``. ``FileSink`` differentiates
    the weekly file from the daily file using the ``_kind`` marker.
    """
    try:
        result = _brief_dispatcher.dispatch_weekly_digest(
            venue_id,
            week_ending=(week_ending.strip() or None),
            window_days=int(window_days or 7),
            only_when_should_send=bool(only_when_should_send),
        )
        return result
    except Exception:
        logger.exception("Weekly digest dispatch failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Weekly digest dispatch failed")


@app.post("/api/v1/weekly-digest/dispatch", response_model=Dict[str, Any])
async def dispatch_weekly_digest_all(
    week_ending: str = "",
    window_days: int = 7,
    only_when_should_send: bool = False,
) -> Dict[str, Any]:
    """
    Walk every registered venue and dispatch a weekly digest. This is
    the endpoint a Monday-morning scheduled task hits:

        POST /api/v1/weekly-digest/dispatch

    When ``only_when_should_send`` is true, venues with zero events
    over the window are skipped — no 'nothing to report' emails on
    quiet weeks.
    """
    try:
        result = _brief_dispatcher.dispatch_all_weekly_digests(
            week_ending=(week_ending.strip() or None),
            window_days=int(window_days or 7),
            only_when_should_send=bool(only_when_should_send),
        )
        return result
    except Exception:
        logger.exception("Weekly digest dispatch_all failed")
        raise HTTPException(status_code=500, detail="Weekly digest dispatch failed")


# ============================================================================
# Trends — Moment 13 (accountability over time, sparkline-ready)
# ============================================================================

class TrendResponse(BaseModel):
    """Trend payload — shape matches trends.compose_trend."""

    venue_id: str
    window_days: int
    generated_at: str
    traffic_light: str
    headline: str
    daily: List[Dict[str, Any]]
    series: Dict[str, List[Any]]
    slopes: Dict[str, Any]
    totals: Dict[str, Any]


# Register the default writeback sink at import time — a journal file
# under /tmp/rosteriq_writebacks.jsonl (or $ROSTERIQ_WRITEBACK_JOURNAL
# if set). This gives Dale a durable audit trail even when no real
# Tanda adapter is in the loop.
_DEFAULT_WRITEBACK_JOURNAL = os.environ.get(
    "ROSTERIQ_WRITEBACK_JOURNAL", "/tmp/rosteriq_writebacks.jsonl"
)
try:
    _tanda_writeback.register_sink(
        _tanda_writeback.JournalSink(_DEFAULT_WRITEBACK_JOURNAL)
    )
except Exception:
    logger.exception("Failed to register default writeback journal sink")

# Moment 14-follow-on 2: live Tanda plugin surface. When the env vars
# TANDA_WRITEBACK_URL is set, we wire a real TandaApiSink behind the
# journal sink so every accepted rec is both logged locally AND pushed
# to Tanda. If TANDA_WRITEBACK_URL is not set, the sink is skipped —
# existing deployments keep their journal-only behavior.
_TANDA_WRITEBACK_URL = os.environ.get("TANDA_WRITEBACK_URL", "").strip()
_TANDA_WRITEBACK_TOKEN = os.environ.get("TANDA_WRITEBACK_TOKEN", "").strip() or None
_TANDA_WRITEBACK_DEAD_LETTER = os.environ.get(
    "TANDA_WRITEBACK_DEAD_LETTER", "/tmp/rosteriq_tanda_dead_letter.jsonl"
).strip() or None
if _TANDA_WRITEBACK_URL:
    try:
        _tanda_writeback.register_sink(
            _tanda_writeback.TandaApiSink(
                _TANDA_WRITEBACK_URL,
                api_token=_TANDA_WRITEBACK_TOKEN,
                dead_letter_path=_TANDA_WRITEBACK_DEAD_LETTER,
                max_attempts=3,
                backoff_base_s=0.5,
                backoff_cap_s=8.0,
                timeout_s=float(os.environ.get("TANDA_WRITEBACK_TIMEOUT_S", "5.0")),
            )
        )
        logger.info("TandaApiSink registered against %s", _TANDA_WRITEBACK_URL)
    except Exception:
        logger.exception("Failed to register TandaApiSink")


# ---------------------------------------------------------------------------
# Moment 14-follow-on 4: scheduled jobs
#
# Two in-process jobs:
#   - weekly digest: walks the venue registry on Monday mornings and
#     fires brief_dispatcher.dispatch_all_weekly_digests for venues that
#     have had events in the last window. only_when_should_send=True so
#     quiet weeks stay quiet.
#   - tanda retry sweep: every N minutes, replays entries in the Tanda
#     writeback dead-letter file through the registered sinks. Entries
#     that still fail get written back to the file; resolved entries
#     drop out. The TandaApiSink Idempotency-Key header protects against
#     double-apply if upstream already processed an earlier attempt.
#
# Controlled by env vars — scheduled jobs are opt-in so existing
# deployments don't suddenly start firing Monday-morning emails or
# sweeping dead-letters they weren't expecting.
# ---------------------------------------------------------------------------

_SCHEDULED_JOBS_READY = False

_SCHEDULER_ENABLED = (
    os.environ.get("ROSTERIQ_SCHEDULER_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)
_WEEKLY_DIGEST_JOB_ENABLED = (
    os.environ.get("ROSTERIQ_WEEKLY_DIGEST_JOB_ENABLED", "true").strip().lower()
    in ("1", "true", "yes", "on")
)
_TANDA_SWEEP_JOB_ENABLED = (
    os.environ.get("ROSTERIQ_TANDA_SWEEP_JOB_ENABLED", "true").strip().lower()
    in ("1", "true", "yes", "on")
)
# Round 14: daily Tanda history ingest — opt-in.
_TANDA_HISTORY_INGEST_ENABLED = (
    os.environ.get("ROSTERIQ_TANDA_HISTORY_INGEST_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)
# Comma-separated "venue:org" pairs, e.g. "venue_1:org_1,venue_2:org_1"
_TANDA_HISTORY_VENUES = [
    p.strip() for p in os.environ.get("ROSTERIQ_TANDA_HISTORY_VENUES", "").split(",")
    if p.strip()
]


def _tanda_history_venue_pairs():
    """Parse ROSTERIQ_TANDA_HISTORY_VENUES into (venue_id, org_id) tuples."""
    out = []
    for pair in _TANDA_HISTORY_VENUES:
        if ":" in pair:
            v, o = pair.split(":", 1)
            out.append((v.strip(), o.strip()))
        else:
            # If no org specified, assume venue_id doubles as org_id.
            out.append((pair.strip(), pair.strip()))
    return out


def _ensure_scheduled_jobs() -> None:
    """Register the weekly-digest and Tanda retry-sweep jobs exactly once.

    Idempotent so the lifespan handler can call it on every startup
    without duplicating jobs when a hot-reload reuses the module.
    """
    global _SCHEDULED_JOBS_READY
    if _SCHEDULED_JOBS_READY:
        return

    from rosteriq import scheduled_jobs as _sj
    scheduler = _sj.get_global_scheduler()

    if _WEEKLY_DIGEST_JOB_ENABLED and scheduler.get("weekly_digest") is None:
        try:
            job = _sj.make_weekly_digest_job(
                # Check every hour; the Monday-morning gate decides
                # whether to actually fire.
                interval_s=float(
                    os.environ.get("ROSTERIQ_WEEKLY_DIGEST_INTERVAL_S", "3600")
                ),
            )
            scheduler.add(job)
            logger.info("Scheduled weekly_digest job registered")
        except Exception:
            logger.exception("Failed to register weekly_digest job")

    if (
        _TANDA_SWEEP_JOB_ENABLED
        and _TANDA_WRITEBACK_DEAD_LETTER
        and scheduler.get("tanda_retry_sweep") is None
    ):
        try:
            job = _sj.make_tanda_retry_sweep_job(
                dead_letter_path=_TANDA_WRITEBACK_DEAD_LETTER,
                interval_s=float(
                    os.environ.get("ROSTERIQ_TANDA_SWEEP_INTERVAL_S", "300")
                ),
                max_entries=int(
                    os.environ.get("ROSTERIQ_TANDA_SWEEP_MAX_ENTRIES", "100")
                ),
            )
            scheduler.add(job)
            logger.info(
                "Scheduled tanda_retry_sweep job registered (path=%s)",
                _TANDA_WRITEBACK_DEAD_LETTER,
            )
        except Exception:
            logger.exception("Failed to register tanda_retry_sweep job")

    if (
        _TANDA_HISTORY_INGEST_ENABLED
        and _tanda_history_venue_pairs()
        and scheduler.get("tanda_history_ingest") is None
    ):
        try:
            job = _sj.make_tanda_history_ingest_job(
                interval_s=float(
                    os.environ.get("ROSTERIQ_TANDA_HISTORY_INTERVAL_S", str(24 * 3600))
                ),
                lookback_days=int(
                    os.environ.get("ROSTERIQ_TANDA_HISTORY_LOOKBACK_DAYS", "2")
                ),
                venue_map_fn=_tanda_history_venue_pairs,
            )
            scheduler.add(job)
            logger.info(
                "Scheduled tanda_history_ingest job registered (%d venues)",
                len(_tanda_history_venue_pairs()),
            )
        except Exception:
            logger.exception("Failed to register tanda_history_ingest job")

    _SCHEDULED_JOBS_READY = True


def _start_scheduler_thread_if_enabled() -> None:
    """Spin up the background thread when ROSTERIQ_SCHEDULER_ENABLED is set.

    The scheduler ticks every minute by default — jobs use their own
    ``interval_s`` to decide whether to fire.
    """
    if not _SCHEDULER_ENABLED:
        logger.info(
            "Scheduler loop disabled (set ROSTERIQ_SCHEDULER_ENABLED=true "
            "to turn on in-process jobs)"
        )
        return
    try:
        from rosteriq import scheduled_jobs as _sj
        poll = float(os.environ.get("ROSTERIQ_SCHEDULER_POLL_S", "60"))
        _sj.get_global_scheduler().run_forever(poll_interval_s=poll)
        logger.info("Scheduler loop started (poll=%ss)", poll)
    except Exception:
        logger.exception("Failed to start scheduler loop")


class TandaWritebackRequest(BaseModel):
    """Input to POST /api/v1/tanda/writeback."""

    venue_id: str
    rec_id: str


class TandaWritebackResponse(BaseModel):
    """Writeback result — shape matches tanda_writeback.writeback_accepted_rec."""

    venue_id: str
    rec_id: str
    status: str
    reason: str
    delta: Optional[Dict[str, Any]] = None
    results: List[Dict[str, Any]] = []


@app.get("/api/v1/trends/{venue_id}", response_model=TrendResponse)
async def get_trends(
    venue_id: str,
    window: int = 7,
) -> TrendResponse:
    """
    Accountability trend for a venue over the last N days.

    Query params:
        window: 7, 14, or 28 days. Other values are clamped to [1, 90].

    Returns daily zero-filled rollups plus sparkline-friendly series
    arrays (acceptance_rate, missed_aud, total_events), first-half vs
    second-half slope deltas, and a single worst-axis-first headline
    like "Your acceptance rate is down 18 pts over the last 7 days."

    The trend is fully deterministic — same events + same window always
    produce the same response.
    """
    try:
        trend = _trends.compose_trend_from_store(
            venue_id,
            window_days=int(window),
        )
        return TrendResponse(**trend)
    except Exception:
        logger.exception("Trend fetch failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Trend fetch failed")


# ============================================================================
# Weekly Digest — Moment 14b (roll 7 days of accountability into one summary)
# ============================================================================

class WeeklyDigestResponse(BaseModel):
    """Weekly digest payload — shape matches weekly_digest.compose_weekly_digest."""

    venue_id: str
    venue_label: str
    date: str
    week_start: str
    week_end: str
    window_days: int
    generated_at: str
    traffic_light: str
    headline: str
    one_pattern: str
    summary: str
    rollup: Dict[str, Any]
    patterns: List[Dict[str, Any]]
    should_send: bool


@app.get("/api/v1/weekly-digest/{venue_id}", response_model=WeeklyDigestResponse)
async def get_weekly_digest(
    venue_id: str,
    week_ending: str = "",
    window_days: int = 7,
    venue_label: str = "",
) -> WeeklyDigestResponse:
    """
    Weekly accountability digest — "last week cost you $X across
    these 3 patterns, here's the one thing to fix next week."

    Query params:
        week_ending: YYYY-MM-DD anchor (last day of the window,
            inclusive). Defaults to yesterday (UTC).
        window_days: how many days back from week_ending to include.
            Clamped to [1, 90]. Defaults to 7.
        venue_label: optional human-friendly name for the header.

    Deterministic — same events + same week_ending + same window
    always produce the same digest. Rolls up the current
    accountability store directly.
    """
    try:
        week_arg = week_ending.strip() or None
        label = venue_label.strip() or None
        digest = _weekly_digest.compose_weekly_digest_from_store(
            venue_id,
            week_ending=week_arg,
            window_days=int(window_days),
            venue_label=label,
        )
        return WeeklyDigestResponse(**digest)
    except Exception:
        logger.exception("Weekly digest failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Weekly digest failed")


@app.get(
    "/api/v1/weekly-digest/{venue_id}/text",
    response_class=PlainTextResponse,
)
async def get_weekly_digest_text(
    venue_id: str,
    week_ending: str = "",
    window_days: int = 7,
    venue_label: str = "",
) -> str:
    """
    Plain-text version of the weekly digest — suitable for piping
    into an email job or a Monday-morning Slack post.
    """
    try:
        week_arg = week_ending.strip() or None
        label = venue_label.strip() or None
        digest = _weekly_digest.compose_weekly_digest_from_store(
            venue_id,
            week_ending=week_arg,
            window_days=int(window_days),
            venue_label=label,
        )
        return _weekly_digest.render_text(digest)
    except Exception:
        logger.exception("Weekly digest (text) failed for %s", venue_id)
        raise HTTPException(status_code=500, detail="Weekly digest failed")


# ============================================================================
# Tanda Writeback — Moment 14a (plugin surface: act on accepted recs)
# ============================================================================

@app.post("/api/v1/tanda/writeback", response_model=TandaWritebackResponse)
async def tanda_writeback(
    request: TandaWritebackRequest,
) -> TandaWritebackResponse:
    """
    Push an accepted recommendation out to registered writeback sinks.

    Only acts on recs with ``status == "accepted"`` — pending and
    dismissed recs are explicitly skipped (if you dismissed it, you
    meant it). The composer maps the rec's action suffix to a
    structured ShiftDelta (cut_staff / send_home / call_in /
    trim_shift), then fans it out to every registered sink.

    The default sink is a journal file (``/tmp/rosteriq_writebacks.jsonl``
    or ``$ROSTERIQ_WRITEBACK_JOURNAL``) so every accepted rec leaves an
    audit trail, even before a real Tanda adapter is wired in.

    Return shape is stable — callers can rely on status being one of:
    ``ok`` (all sinks succeeded), ``partial`` (some sinks failed),
    ``skipped`` (rec not found, or not accepted), or ``no_delta``
    (rec has no mapping — usually a manual rec).
    """
    try:
        result = _tanda_writeback.writeback_accepted_rec(
            request.venue_id,
            request.rec_id,
        )
        return TandaWritebackResponse(**result)
    except Exception:
        logger.exception("Tanda writeback failed")
        raise HTTPException(status_code=500, detail="Writeback failed")


@app.get("/api/v1/tanda/writeback/journal/{venue_id}", response_model=List[Dict[str, Any]])
async def tanda_writeback_journal(
    venue_id: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Read back the writeback journal for a venue.

    Returns most-recent-first. Filters on the default journal file;
    if you've pointed the sink elsewhere (via ``$ROSTERIQ_WRITEBACK_JOURNAL``)
    this endpoint reads whatever the current default path points to.
    """
    try:
        entries = _tanda_writeback.read_journal(
            _DEFAULT_WRITEBACK_JOURNAL,
            venue_id=venue_id,
            limit=int(limit),
        )
        return entries
    except Exception:
        logger.exception("Writeback journal read failed")
        raise HTTPException(status_code=500, detail="Journal read failed")


@app.get("/api/v1/tanda/writeback/sinks", response_model=Dict[str, Any])
async def tanda_writeback_sinks() -> Dict[str, Any]:
    """List currently registered writeback sink names."""
    try:
        return {
            "sinks": _tanda_writeback.registered_sinks(),
            "default_journal": _DEFAULT_WRITEBACK_JOURNAL,
            "tanda_api_url": _TANDA_WRITEBACK_URL or None,
            "tanda_api_dead_letter": _TANDA_WRITEBACK_DEAD_LETTER or None,
        }
    except Exception:
        logger.exception("Writeback sinks list failed")
        raise HTTPException(status_code=500, detail="Sinks list failed")


@app.get("/api/v1/tanda/writeback/dead-letter/{venue_id}", response_model=List[Dict[str, Any]])
async def tanda_writeback_dead_letter(
    venue_id: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Read back the Tanda API writeback dead-letter file for a venue.

    Most-recent-first. Returns empty list when no dead-letter file is
    configured or when the file doesn't exist yet. Use this to inspect
    writebacks that exhausted retries against a real Tanda endpoint.
    """
    if not _TANDA_WRITEBACK_DEAD_LETTER:
        return []
    try:
        return _tanda_writeback.read_dead_letter(
            _TANDA_WRITEBACK_DEAD_LETTER,
            venue_id=venue_id,
            limit=int(limit),
        )
    except Exception:
        logger.exception("Writeback dead-letter read failed")
        raise HTTPException(status_code=500, detail="Dead-letter read failed")


# ============================================================================
# Moment 14-follow-on 4: scheduled jobs status + manual triggers
# ============================================================================

@app.get("/api/v1/scheduler/status", response_model=Dict[str, Any])
async def scheduler_status() -> Dict[str, Any]:
    """
    Return the current state of the in-process scheduler.

    Includes per-job run counts, last run timestamps, and whether the
    background loop is enabled. Useful for monitoring — hook this up
    to a Railway health check or a uptime ping if desired.
    """
    try:
        from rosteriq import scheduled_jobs as _sj
        _ensure_scheduled_jobs()
        scheduler = _sj.get_global_scheduler()
        return {
            "loop_enabled": _SCHEDULER_ENABLED,
            "jobs": scheduler.status(),
            "weekly_digest_enabled": _WEEKLY_DIGEST_JOB_ENABLED,
            "tanda_sweep_enabled": _TANDA_SWEEP_JOB_ENABLED,
            "tanda_dead_letter_path": _TANDA_WRITEBACK_DEAD_LETTER,
        }
    except Exception:
        logger.exception("Scheduler status read failed")
        raise HTTPException(status_code=500, detail="Scheduler status failed")


@app.post("/api/v1/scheduler/run/{job_name}", response_model=Dict[str, Any])
async def scheduler_run_job(job_name: str) -> Dict[str, Any]:
    """
    Fire a named scheduled job immediately, bypassing its interval gate.

    Intended for manual runs from the Railway shell or for wiring to
    an external cron (Railway cron → POST here) instead of relying on
    the in-process loop. The job's gate (e.g. "Monday morning only") is
    also bypassed, since a manual trigger is an explicit ask.

    Returns the job result (shape depends on the job — weekly digest
    returns a dispatch summary, Tanda sweep returns a sweep summary).
    """
    from rosteriq import scheduled_jobs as _sj
    _ensure_scheduled_jobs()
    scheduler = _sj.get_global_scheduler()
    job = scheduler.get(job_name)
    if job is None:
        raise HTTPException(status_code=404, detail=f"No such job: {job_name}")
    try:
        result = job.fn(datetime.now(timezone.utc))
        if not isinstance(result, dict):
            result = {"raw": result}
        job.last_result = result
        job.last_error = None
        job.runs += 1
        # Stamp last_run_ts using monotonic clock so the interval
        # gate respects this manual run on the next tick.
        import time as _time
        job.last_run_ts = _time.monotonic()
        return {"ok": True, "job": job_name, "result": result}
    except Exception as exc:
        logger.exception("Manual scheduler run failed for %s", job_name)
        job.errors += 1
        job.last_error = str(exc)
        raise HTTPException(status_code=500, detail=f"Job {job_name} failed: {exc}")


# ============================================================================
# Accountability Ledger — Moment 8 (decisions-taken-or-not)
# ============================================================================

@app.get("/api/v1/accountability/{venue_id}", response_model=AccountabilityStateResponse)
async def get_accountability_state(venue_id: str) -> AccountabilityStateResponse:
    """
    Current accountability ledger for a venue — pending/accepted/dismissed
    recommendations plus a summary roll-up.
    """
    try:
        return AccountabilityStateResponse(**_acct_store.state(venue_id))
    except Exception:
        logger.exception(f"Accountability fetch failed for {venue_id}")
        raise HTTPException(status_code=500, detail="Accountability fetch failed")


@app.post("/api/v1/accountability/record", response_model=AccountabilityStateResponse)
async def record_accountability_event(
    request: AccountabilityRecordRequest,
) -> AccountabilityStateResponse:
    """
    Record a new recommendation in the accountability ledger. Idempotent
    when `rec_id` is supplied — re-posting the same rec_id is a no-op.
    """
    try:
        text = (request.text or "").strip()
        if not text:
            raise HTTPException(status_code=400, detail="text must not be empty")
        _acct_store.record(
            venue_id=request.venue_id,
            text=text,
            source=request.source or "manual",
            impact_estimate_aud=request.impact_estimate_aud,
            priority=request.priority or "med",
            rec_id=request.rec_id,
        )
        return AccountabilityStateResponse(**_acct_store.state(request.venue_id))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Accountability record failed")
        raise HTTPException(status_code=500, detail="Accountability record failed")


@app.post("/api/v1/accountability/respond", response_model=AccountabilityStateResponse)
async def respond_to_accountability_event(
    request: AccountabilityRespondRequest,
) -> AccountabilityStateResponse:
    """
    Mark a pending recommendation as accepted or dismissed. A response
    note is optional but strongly encouraged for dismissed items — it's
    exactly the "you had all this data and kept people on — why?" answer.
    """
    try:
        _acct_store.respond(
            venue_id=request.venue_id,
            rec_id=request.rec_id,
            status=request.status,
            note=(request.note or None),
        )
        return AccountabilityStateResponse(**_acct_store.state(request.venue_id))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Accountability respond failed")
        raise HTTPException(status_code=500, detail="Accountability respond failed")


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
# Tanda Forecast Revenue — benchmark our forecast against Tanda's own
# ============================================================================
#
# The Roster Maker uses this to show a side-by-side: "Tanda thinks
# $11,500, we think $13,200 — here's why." Over time the delta tells
# operators which signals RosterIQ is picking up that Tanda's forecast
# misses (weather, stadium games, event spillover).


class TandaForecastDay(BaseModel):
    """One day of Tanda revenue forecast."""
    date: str = Field(..., description="ISO date string YYYY-MM-DD")
    tanda_forecast: float = Field(..., description="Tanda's forecast revenue for the day")
    department_breakdown: Dict[str, float] = Field(
        default_factory=dict,
        description="Per-department forecast split (friendly dept names → $)",
    )


class TandaForecastRevenueResponse(BaseModel):
    """Response for GET /api/v1/tanda/forecast-revenue/{venue_id}."""
    venue_id: str
    date_from: str
    date_to: str
    source: str = Field(..., description="'tanda' (live) or 'tanda_demo' (fallback)")
    days: List[TandaForecastDay]


@app.get(
    "/api/v1/tanda/forecast-revenue/{venue_id}",
    response_model=TandaForecastRevenueResponse,
)
async def get_tanda_forecast_revenue(
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> TandaForecastRevenueResponse:
    """
    Return Tanda's own revenue forecast for the venue over a date range.

    Defaults to the next 7 days if date_from/date_to are omitted. Used by
    the Roster Maker dashboard to benchmark RosterIQ's forecast against
    Tanda's — the delta is the value the forecast engine is adding.
    """
    from rosteriq.tanda_adapter import get_tanda_adapter

    try:
        if date_from:
            start = date.fromisoformat(date_from)
        else:
            start = date.today()
        if date_to:
            end = date.fromisoformat(date_to)
        else:
            end = start + timedelta(days=6)
        if end < start:
            raise HTTPException(
                status_code=400,
                detail="date_to must be on or after date_from",
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date: {e}")

    try:
        adapter = get_tanda_adapter()
        forecasts = await adapter.get_forecast_revenue(venue_id, (start, end))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch Tanda forecast revenue")
        raise HTTPException(
            status_code=502,
            detail=f"Tanda forecast revenue unavailable: {e}",
        )

    # Infer source from first forecast (adapter stamps "tanda" vs "tanda_demo")
    source = forecasts[0].source if forecasts else "tanda"
    return TandaForecastRevenueResponse(
        venue_id=venue_id,
        date_from=start.isoformat(),
        date_to=end.isoformat(),
        source=source,
        days=[
            TandaForecastDay(
                date=f.date.isoformat(),
                tanda_forecast=f.forecast,
                department_breakdown=f.department_breakdown,
            )
            for f in forecasts
        ],
    )


# ============================================================================
# Feature routers (availability, weather, events, call-in)
# ============================================================================
# These live in sibling modules so the feature code is easy to find and test
# in isolation. They're included here after all direct @app.* endpoints so
# their paths don't shadow anything registered above.

from rosteriq.availability_router import router as _availability_router
from rosteriq.weather_router import router as _weather_router
from rosteriq.events_router import router as _events_router
from rosteriq.call_in_router import router as _call_in_router
from rosteriq.ask_router import ask_router as _ask_router
from rosteriq.roi_router import router as _roi_router
from rosteriq.shift_events_router import router as _shift_events_router
from rosteriq.access_router import access_router as _access_router
from rosteriq.award_router import router as _award_router
from rosteriq.tanda_webhook_router import router as _tanda_webhook_router
from rosteriq.accountability_router import router as _accountability_router
from rosteriq.brief_subscriptions_router import router as _brief_subscriptions_router
from rosteriq.data_feeds_router import router as _data_feeds_router
from rosteriq.tanda_marketplace_router import router as _tanda_marketplace_router
from rosteriq.tenants_router import tenants_router as _tenants_router
from rosteriq.billing_router import router as _billing_router
from rosteriq.tanda_history_router import router as _tanda_history_router
from rosteriq.onboarding_router import router as _onboarding_router
from rosteriq.concierge_router import router as _concierge_router
from rosteriq.forecast_accuracy_router import router as _forecast_accuracy_router

try:
    from rosteriq.ws_router import router as _ws_router
    _ws_router_available = True
except Exception:
    logger.warning("ws_router unavailable")
    _ws_router_available = False

app.include_router(_availability_router)
app.include_router(_weather_router)
app.include_router(_events_router)
app.include_router(_call_in_router)
app.include_router(_ask_router)
app.include_router(_roi_router)
app.include_router(_shift_events_router)
app.include_router(_access_router)
app.include_router(_award_router)
app.include_router(_tanda_webhook_router)
app.include_router(_tanda_marketplace_router)
app.include_router(_accountability_router)
app.include_router(_brief_subscriptions_router)
app.include_router(_data_feeds_router)
app.include_router(_tenants_router)
app.include_router(_billing_router)
app.include_router(_tanda_history_router)
app.include_router(_onboarding_router)
app.include_router(_concierge_router)
app.include_router(_forecast_accuracy_router)

if _ws_router_available:
    app.include_router(_ws_router)


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
