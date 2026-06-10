"""
RosterIQ FastAPI server.

Thin API layer over the existing engine modules. Provides REST endpoints
for roster generation, analysis, forecasting, and real-time decisions.

Run:
    uvicorn rosteriq.api:app --reload
    # or
    python -m rosteriq.api
"""

import os
import sys
import logging
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from rosteriq import __version__
from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
    CostBreakdown, VarianceSignal, StaffingRecommendation,
)
from rosteriq.middleware.api_version import APIVersionMiddleware
from rosteriq.middleware.tenant import TenantMiddleware
from rosteriq.roster_optimiser import (
    generate_weekly_roster, generate_daily_roster,
    analyse_roster, suggest_improvements,
    calculate_required_staff, identify_peak_periods,
    DEFAULT_COVERS_PER_STAFF,
)
from rosteriq.cost_calculator import (
    calculate_shift_cost_breakdown, calculate_roster_cost,
    compare_rosters, calculate_labour_percentage,
    find_cost_savings_opportunities,
)
from rosteriq.variance_engine import (
    calculate_weighted_variance, detect_threshold_breach,
    create_signal, combine_forecasts, get_signal_summary,
)
from rosteriq.decision_engine import make_decision
from rosteriq.award_rules import (
    get_day_type, get_penalty_multiplier,
    validate_shift_compliance, get_public_holidays,
)
from rosteriq.ensemble import EnsembleForecaster
from rosteriq.database import get_db, DATABASE_URL
from rosteriq.services.config import get_config as get_app_config
from rosteriq.services.error_reporting import init_sentry
from rosteriq.services.i18n import init_i18n
from rosteriq.services.ws_events import get_dispatcher
from rosteriq.services.db_pool import ConnectionPool
from rosteriq.routes.db_health import create_db_health_router

# Rate limiting and caching middleware
try:
    from rosteriq.middleware.rate_limiter import RateLimiterMiddleware
except ImportError:
    RateLimiterMiddleware = None

try:
    from rosteriq.middleware.cache import get_cache_manager, cache_response
except ImportError:
    get_cache_manager = None
    cache_response = None

try:
    from rosteriq.middleware.logging import StructuredLoggingMiddleware, get_structured_logger
except ImportError:
    StructuredLoggingMiddleware = None
    get_structured_logger = None

# Input validation and sanitisation middleware
try:
    from rosteriq.middleware.input_validation import InputValidationMiddleware, ValidationConfig
except ImportError:
    InputValidationMiddleware = None
    ValidationConfig = None

# Security headers middleware
try:
    from rosteriq.middleware.security_headers import SecurityHeadersMiddleware, SecurityConfig, Environment
except ImportError:
    SecurityHeadersMiddleware = None
    SecurityConfig = None
    Environment = None

# Abuse detection middleware
try:
    from rosteriq.middleware.abuse_detector import AbuseDetectionMiddleware
except ImportError:
    AbuseDetectionMiddleware = None

# Metrics collection middleware
try:
    from rosteriq.middleware.metrics import MetricsMiddleware
except ImportError:
    MetricsMiddleware = None

# Monitoring routes (metrics, alerts, health)
try:
    from rosteriq.routes.monitoring import create_monitoring_router
except ImportError:
    create_monitoring_router = None


# ============================================================================
# Logging setup
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# Metrics collector
# ============================================================================

class MetricsCollector:
    """Simple in-memory metrics collector for production monitoring."""

    def __init__(self):
        self.total_requests = 0
        self.total_errors = 0
        self.total_response_time_ms = 0.0
        self.startup_time = datetime.now()
        self.request_count_by_method = defaultdict(int)
        self.error_count_by_status = defaultdict(int)

    def record_request(self, method: str, status_code: int, duration_ms: float):
        """Record a request's metrics."""
        self.total_requests += 1
        self.request_count_by_method[method] += 1
        self.total_response_time_ms += duration_ms

        if 400 <= status_code < 600:
            self.total_errors += 1
            self.error_count_by_status[status_code] += 1

    def get_metrics(self) -> dict:
        """Return current metrics snapshot."""
        uptime_seconds = (datetime.now() - self.startup_time).total_seconds()
        avg_response_time = (
            self.total_response_time_ms / self.total_requests
            if self.total_requests > 0
            else 0
        )
        return {
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "error_rate": (
                self.total_errors / self.total_requests
                if self.total_requests > 0
                else 0
            ),
            "average_response_time_ms": round(avg_response_time, 2),
            "uptime_seconds": round(uptime_seconds, 2),
            "request_count_by_method": dict(self.request_count_by_method),
            "error_count_by_status": dict(self.error_count_by_status),
        }


_metrics = MetricsCollector()


# ============================================================================
# Request logging middleware
# ============================================================================

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log structured details for every request (skip /health)."""

    async def dispatch(self, request: Request, call_next):
        # Skip logging for health checks to reduce noise
        if request.url.path == "/health":
            response = await call_next(request)
            return response

        start_time = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000

        # Record metrics
        _metrics.record_request(request.method, response.status_code, duration_ms)

        # Log with structured format
        logger.info(
            "request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round(duration_ms, 2),
            },
        )

        return response


# ============================================================================
# App setup
# ============================================================================

tags_metadata = [
    {
        "name": "venues",
        "description": "Venue configuration, locations, and settings",
    },
    {
        "name": "employees",
        "description": "Employee data, employment types, availability",
    },
    {
        "name": "rosters",
        "description": "Roster generation, scheduling, and management",
    },
    {
        "name": "forecasts",
        "description": "Demand forecasting and staffing predictions",
    },
    {
        "name": "auth",
        "description": "User authentication, tokens, and permissions",
    },
    {
        "name": "billing",
        "description": "Subscription management and billing",
    },
    {
        "name": "webhooks",
        "description": "Webhook receivers for Tanda and external services",
    },
    {
        "name": "feeds",
        "description": "Data feed configuration (weather, events, POS, etc)",
    },
    {
        "name": "admin",
        "description": "Admin panel and system management",
    },
]

app = FastAPI(
    title="RosterIQ",
    description="AI-powered predictive rostering for Australian hospitality venues. Integrates with Tanda workforce management, POS systems, and external data feeds for intelligent shift planning.",
    version=__version__,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=tags_metadata,
)

# ============================================================================
# Middleware stack (order matters — added in reverse: last added = outermost)
# Correct execution order (outermost to innermost):
# 1. CORSMiddleware
# 2. SecurityHeadersMiddleware
# 3. InputValidationMiddleware
# 4. RateLimiterMiddleware
# 5. CacheMiddleware
# 6. TenantMiddleware
# 7. APIVersionMiddleware
# ============================================================================

# Add API versioning middleware (innermost — runs last before app logic)
app.add_middleware(APIVersionMiddleware)
logger.info("API versioning middleware enabled")

# Add tenant context middleware (multi-tenancy isolation)
app.add_middleware(TenantMiddleware)
logger.info("Tenant context middleware enabled")

# Add cache middleware if available
if get_cache_manager:
    try:
        from rosteriq.middleware.cache import CacheMiddleware as CacheMiddlewareClass
        app.add_middleware(CacheMiddlewareClass, cache_manager=get_cache_manager())
        logger.info("Cache middleware enabled")
    except ImportError:
        logger.debug("Cache middleware not available")

# Add rate limiting middleware
if RateLimiterMiddleware:
    app.add_middleware(RateLimiterMiddleware)
    logger.info("Rate limiting middleware enabled")

# Add input validation and sanitisation middleware
if InputValidationMiddleware:
    input_config = ValidationConfig(
        max_body_size_bytes=1_000_000,  # 1MB
        max_string_length=10_000,
        max_array_length=1_000,
        max_json_depth=10,
        block_suspicious=False,  # Log-only mode for now (can be set to True in prod)
    )
    app.add_middleware(InputValidationMiddleware, config=input_config)
    logger.info("Input validation middleware enabled")

# Add security headers middleware
_security_middleware = None
if SecurityHeadersMiddleware:
    security_config = SecurityConfig(
        environment=Environment.PRODUCTION if os.getenv("ENV") == "production" else Environment.DEVELOPMENT,
        enable_hsts=True,
        allowed_origins=os.getenv("ALLOWED_ORIGINS", "https://app.rosteriq.com").split(","),
        csp_report_uri="/api/v1/admin/security/csp-report",
    )
    _security_middleware = SecurityHeadersMiddleware(app, config=security_config)
    app.add_middleware(SecurityHeadersMiddleware, config=security_config)
    logger.info("Security headers middleware enabled")

# Add CORS middleware (outermost — runs first)
_cors_origins = os.environ.get("CORS_ORIGINS", "").split(",")
_cors_origins = [o.strip() for o in _cors_origins if o.strip()]
if not _cors_origins:
    # Default: allow the Railway deployment URL and localhost for dev
    _cors_origins = [
        "https://rosteriq-production-6aaf.up.railway.app",
        "https://app.rosteriq.com",
        "http://localhost:8000",
        "http://localhost:3000",
    ]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-API-Version", "X-Tenant-ID", "X-Request-ID"],
)
logger.info(f"CORS middleware enabled for origins: {_cors_origins}")

# Request logging (optional, can be added to any level depending on needs)
app.add_middleware(RequestLoggingMiddleware)
logger.info("Request logging middleware enabled")

# Add theming middleware (white-label CSS injection)
try:
    from rosteriq.middleware.theming import ThemeInjectorMiddleware
    from rosteriq.services.theming import ThemeService
    app.add_middleware(ThemeInjectorMiddleware, theme_service=ThemeService())
    logger.info("Theme injector middleware enabled")
except ImportError:
    logger.warning("Theme injector middleware unavailable")
except Exception as e:
    logger.error(f"Failed to register theme injector middleware: {e}")

# Serve static files (dashboard, demo)
try:
    from fastapi.staticfiles import StaticFiles
    import pathlib
    _static_dir = pathlib.Path(__file__).parent / "static"
    if _static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")
except Exception:
    pass

# ============================================================================
# Optional modules — loaded when dependencies are available
# ============================================================================

# Xero integration routes
try:
    from rosteriq.xero_routes import setup_xero_routes
    setup_xero_routes(app, get_db())
    logger.info("Xero integration routes registered")
except ImportError:
    logger.warning("Xero integration routes unavailable")
except Exception as e:
    logger.error(f"Failed to register Xero integration routes: {e}")

# Tanda webhook receiver routes
try:
    from rosteriq.routes.webhook_routes import router as webhook_router
    app.include_router(webhook_router)
    logger.info("Tanda webhook receiver routes registered")
except ImportError:
    logger.warning("Tanda webhook receiver routes unavailable")
except Exception as e:
    logger.error(f"Failed to register Tanda webhook receiver routes: {e}")

# Auth routes (requires SQLAlchemy — optional until DB migration)
try:
    from rosteriq.routes.auth import router as auth_router
    app.include_router(auth_router)
    logger.info("Auth routes registered")
except ImportError:
    logger.warning("Auth routes unavailable")
except Exception as e:
    logger.error(f"Failed to register auth routes: {e}")

# Credential management routes (API keys, webhook secrets)
try:
    from rosteriq.routes.credentials import router as credentials_router
    app.include_router(credentials_router)
except ImportError:
    logger.warning("Credential management routes unavailable")
except Exception as e:
    logger.error(f"Failed to register credential management routes: {e}")

# Onboarding routes for venue setup workflow
try:
    from rosteriq.routes.onboarding import router as onboarding_router
    app.include_router(onboarding_router)
    logger.info("Onboarding routes registered")
except ImportError:
    logger.warning("Onboarding routes unavailable")
except Exception as e:
    logger.error(f"Failed to register onboarding routes: {e}")

# Billing routes
try:
    from rosteriq.routes.billing import router as billing_router
    app.include_router(billing_router)
    logger.info("Billing routes registered")
except ImportError:
    logger.warning("Billing routes unavailable")
except Exception as e:
    logger.error(f"Failed to register billing routes: {e}")

# Tanda Marketplace plugin routes
try:
    from rosteriq.routes.tanda_plugin import router as tanda_plugin_router
    app.include_router(tanda_plugin_router)
except ImportError:
    logger.warning("Tanda plugin routes unavailable")
except Exception as e:
    logger.error(f"Failed to register Tanda plugin routes: {e}")

# Deputy integration routes
try:
    from rosteriq.routes.deputy import router as deputy_router
    app.include_router(deputy_router)
    logger.info("Deputy integration routes registered")
except ImportError:
    logger.info("Deputy routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register Deputy routes: {e}")

# HumanForce integration routes
try:
    from rosteriq.routes.humanforce import router as humanforce_router
    app.include_router(humanforce_router)
    logger.info("HumanForce integration routes registered")
except ImportError:
    logger.info("HumanForce routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register HumanForce routes: {e}")

# MYOB accounting/payroll integration routes
try:
    from rosteriq.routes.myob import router as myob_router
    app.include_router(myob_router)
    logger.info("MYOB integration routes registered")
except ImportError:
    logger.info("MYOB routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register MYOB routes: {e}")

# POS system integration routes (SwiftPOS, Lightspeed, Kounta)
try:
    from rosteriq.routes.pos import router as pos_router
    app.include_router(pos_router)
    logger.info("POS integration routes registered")
except ImportError:
    logger.info("POS integration routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register POS integration routes: {e}")

# Reservation/booking system integration routes (NowBookIt, ResDiary, OpenTable, BookitLive)
try:
    from rosteriq.routes.reservations import router as reservations_router
    app.include_router(reservations_router)
    logger.info("Reservation integration routes registered")
except ImportError:
    logger.info("Reservation routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register reservation routes: {e}")

# Function Tracker event/function management routes
try:
    from rosteriq.routes.function_tracker import router as function_tracker_router
    app.include_router(function_tracker_router)
    logger.info("Function Tracker routes registered")
except ImportError:
    logger.info("Function Tracker routes not available — skipping")
except Exception as e:
    logger.error(f"Failed to register Function Tracker routes: {e}")

# Data feed configuration routes
try:
    from rosteriq.routes.feed_config import router as feed_config_router
    app.include_router(feed_config_router)
    logger.info("Feed config routes registered")
except ImportError:
    logger.warning("Feed config routes unavailable")
except Exception as e:
    logger.error(f"Failed to register feed config routes: {e}")

# Outbound webhook routes
try:
    from rosteriq.routes.outbound_webhooks import router as outbound_webhooks_router
    app.include_router(outbound_webhooks_router)
except ImportError:
    logger.warning("Outbound webhook routes unavailable")

# Webhook queue management routes (retry, dead-letter, circuit breaker)
try:
    from rosteriq.routes.webhook_queue import router as webhook_queue_router
    app.include_router(webhook_queue_router)
except ImportError:
    logger.warning("Webhook queue routes unavailable")
except Exception as e:
    logger.error(f"Failed to register webhook queue routes: {e}")

# Roster template routes
try:
    from rosteriq.routes.roster_templates import router as roster_templates_router
    app.include_router(roster_templates_router)
except ImportError:
    logger.warning("Roster template routes unavailable")
except Exception as e:
    logger.error(f"Failed to register roster template routes: {e}")

# Bulk roster generation routes
try:
    from rosteriq.routes.bulk import router as bulk_router
    app.include_router(bulk_router)
except ImportError:
    logger.warning("Bulk roster routes unavailable")
except Exception as e:
    logger.error(f"Failed to register bulk roster routes: {e}")

# Compliance reporting routes
try:
    from rosteriq.routes.reports import router as reports_router
    app.include_router(reports_router)
    logger.info("Compliance reporting routes registered")
except ImportError:
    logger.warning("Compliance reporting routes unavailable")
except Exception as e:
    logger.error(f"Failed to register compliance reporting routes: {e}")

# Employee costing and labour cost projection routes
try:
    from rosteriq.routes.costing import router as costing_router
    app.include_router(costing_router)
    logger.info("Employee costing routes registered")
except ImportError:
    logger.warning("Employee costing routes unavailable")
except Exception as e:
    logger.error(f"Failed to register employee costing routes: {e}")

# Penalty rate calculator for real-time UI feedback
try:
    from rosteriq.routes.penalty import router as penalty_router
    app.include_router(penalty_router)
    logger.info("Penalty rate calculator routes registered")
except ImportError:
    logger.warning("Penalty rate calculator routes unavailable")
except Exception as e:
    logger.error(f"Failed to register penalty rate calculator routes: {e}")

# Staff self-service portal routes
try:
    from rosteriq.routes.staff import router as staff_router
    app.include_router(staff_router)
    logger.info("Staff portal routes registered")
except ImportError:
    logger.warning("Staff portal routes unavailable")
except Exception as e:
    logger.error(f"Failed to register staff portal routes: {e}")

# Notification preferences and SMS routes
try:
    from rosteriq.routes.notification_prefs import router as notification_prefs_router
    app.include_router(notification_prefs_router)
    logger.info("Notification preferences routes registered")
except ImportError:
    logger.warning("Notification preferences routes unavailable")
except Exception as e:
    logger.error(f"Failed to register notification preferences routes: {e}")

# Unified notification hub dispatch routes
try:
    from rosteriq.routes.notification_hub import router as notification_hub_router
    app.include_router(notification_hub_router)
    logger.info("Notification hub routes registered")
except ImportError:
    logger.warning("Notification hub routes unavailable")
except Exception as e:
    logger.error(f"Failed to register notification hub routes: {e}")

# Shift handover notes system for staff communication
try:
    from rosteriq.routes.handover import router as handover_router
    app.include_router(handover_router)
    logger.info("Shift handover routes registered")
except ImportError:
    logger.warning("Shift handover routes unavailable")
except Exception as e:
    logger.error(f"Failed to register shift handover routes: {e}")

# Privacy compliance and data retention routes (Australian Privacy Act 1988)
try:
    from rosteriq.routes.privacy import router as privacy_router
    app.include_router(privacy_router, prefix="/api/privacy", tags=["privacy"])
    logger.info("Privacy compliance routes registered")
except ImportError:
    logger.warning("Privacy compliance routes unavailable")
except Exception as e:
    logger.error(f"Failed to register privacy compliance routes: {e}")

# A/B testing and experimentation routes
try:
    from rosteriq.routes.ab_testing import router as ab_testing_router
    app.include_router(ab_testing_router)
    logger.info("A/B testing routes registered")
except ImportError:
    logger.warning("A/B testing routes unavailable")
except Exception as e:
    logger.error(f"Failed to register A/B testing routes: {e}")

# Labour cost analytics and intelligence routes
try:
    from rosteriq.routes.analytics import router as analytics_router
    app.include_router(analytics_router)
    logger.info("Labour cost analytics routes registered")
except ImportError:
    logger.warning("Labour cost analytics routes unavailable")
except Exception as e:
    logger.error(f"Failed to register analytics routes: {e}")

# Venue benchmarking and multi-venue comparison routes
try:
    from rosteriq.routes.benchmarks import router as benchmarks_router
    app.include_router(benchmarks_router)
    logger.info("Venue benchmarking routes registered")
except ImportError:
    logger.warning("Venue benchmarking routes unavailable")
except Exception as e:
    logger.error(f"Failed to register benchmarking routes: {e}")

# Industry benchmarking routes (AU hospitality standards)
try:
    from rosteriq.routes.industry_benchmarks import router as industry_benchmarks_router
    app.include_router(industry_benchmarks_router)
    logger.info("Industry benchmarking routes registered")
except ImportError:
    logger.warning("Industry benchmarking routes unavailable")
except Exception as e:
    logger.error(f"Failed to register industry benchmarking routes: {e}")

# Role-based dashboard configuration and routing
try:
    from rosteriq.routes.dashboard_config import router as dashboard_config_router
    app.include_router(dashboard_config_router)
    logger.info("Dashboard configuration routes registered")
except ImportError:
    logger.warning("Dashboard configuration routes unavailable")
except Exception as e:
    logger.error(f"Failed to register dashboard configuration routes: {e}")

# Forecast accuracy tracking and analysis routes
try:
    from rosteriq.routes.forecast_accuracy import router as forecast_accuracy_router
    app.include_router(forecast_accuracy_router)
    logger.info("Forecast accuracy analysis routes registered")
except ImportError:
    logger.warning("Forecast accuracy routes unavailable")
except Exception as e:
    logger.error(f"Failed to register forecast accuracy routes: {e}")

# Labour cost trends analytics routes
try:
    from rosteriq.routes.cost_trends import router as cost_trends_router
    app.include_router(cost_trends_router)
    logger.info("Cost trends analytics routes registered")
except ImportError:
    logger.warning("Cost trends analytics routes unavailable")
except Exception as e:
    logger.error(f"Failed to register cost trends analytics routes: {e}")

# White-label theming and branding routes
try:
    from rosteriq.routes.theming import router as theming_router
    app.include_router(theming_router)
    logger.info("White-label theming routes registered")
except ImportError:
    logger.warning("White-label theming routes unavailable")
except Exception as e:
    logger.error(f"Failed to register theming routes: {e}")

# Staff scheduling preference learning routes
try:
    from rosteriq.routes.preferences import router as preferences_router
    app.include_router(preferences_router)
    logger.info("Preference learning routes registered")
except ImportError:
    logger.warning("Preference learning routes unavailable")
except Exception as e:
    logger.error(f"Failed to register preference learning routes: {e}")

# Real-time POS sales ingestion and revenue monitoring routes
try:
    from rosteriq.services.pos_realtime import (
        RevenueAccumulator,
        StaffingTrigger,
        RealtimePOSFeed,
    )
    from rosteriq.routes.pos_realtime import create_pos_realtime_router

    # Initialize POS realtime components
    _revenue_accumulator = RevenueAccumulator()
    _staffing_trigger = StaffingTrigger(
        decision_engine=make_decision,
        websocket_hub=get_dispatcher(),
    )
    _pos_feed = RealtimePOSFeed(
        accumulator=_revenue_accumulator,
        trigger=_staffing_trigger,
    )

    # Register routes with injected dependencies
    pos_realtime_router = create_pos_realtime_router(_pos_feed)
    app.include_router(pos_realtime_router)
    logger.info("Real-time POS ingestion routes registered")
except ImportError as e:
    logger.warning(f"Real-time POS ingestion routes unavailable: {e}")
except Exception as e:
    logger.error(f"Failed to register real-time POS routes: {e}")

# Real-time labour tracking with colour-coded alerts
try:
    from rosteriq.services.labour_tracker import LabourTracker
    from rosteriq.routes.labour import create_labour_router

    # Initialize labour tracker
    _labour_tracker = LabourTracker()

    # Register routes with injected dependencies
    labour_router = create_labour_router(_labour_tracker)
    app.include_router(labour_router)
    logger.info("Real-time labour tracking routes registered")
except ImportError as e:
    logger.warning(f"Real-time labour tracking routes unavailable: {e}")
except Exception as e:
    logger.error(f"Failed to register labour tracking routes: {e}")

# Payroll export routes (Xero Payroll, KeyPay)
try:
    from rosteriq.routes.payroll import router as payroll_router
    app.include_router(payroll_router)
    logger.info("Payroll export routes registered")
except ImportError:
    logger.warning("Payroll export routes unavailable")
except Exception as e:
    logger.error(f"Failed to register payroll export routes: {e}")

# Database backup, restore, and data export routes
try:
    from rosteriq.routes.backup import router as backup_router
    app.include_router(backup_router)
    logger.info("Database backup and data export routes registered")
except ImportError:
    logger.warning("Database backup routes unavailable")
except Exception as e:
    logger.error(f"Failed to register backup routes: {e}")

# MILP roster optimiser v2 routes
try:
    from rosteriq.routes.optimiser import router as optimiser_router
    app.include_router(optimiser_router)
    logger.info("MILP roster optimiser routes registered")
except ImportError:
    logger.warning("MILP roster optimiser routes unavailable")
except Exception as e:
    logger.error(f"Failed to register optimiser routes: {e}")

# Smart auto-scheduler routes (AI-powered week generation)
try:
    from rosteriq.routes.auto_schedule import router as auto_schedule_router
    app.include_router(auto_schedule_router)
    logger.info("Auto-scheduler routes registered")
except ImportError:
    logger.warning("Auto-scheduler routes unavailable")
except Exception as e:
    logger.error(f"Failed to register auto-scheduler routes: {e}")

# Enhanced demand forecasting v2 routes
try:
    from rosteriq.routes.forecast_v2 import router as forecast_v2_router
    app.include_router(forecast_v2_router)
    logger.info("Enhanced forecast v2 routes registered")
except ImportError:
    logger.warning("Enhanced forecast v2 routes unavailable")
except Exception as e:
    logger.error(f"Failed to register forecast v2 routes: {e}")

# Break scheduler routes (MA000009 compliance)
try:
    from rosteriq.routes.breaks import router as breaks_router
    app.include_router(breaks_router)
    logger.info("Break scheduler routes registered")
except ImportError:
    logger.warning("Break scheduler routes unavailable")
except Exception as e:
    logger.error(f"Failed to register break scheduler routes: {e}")

# Shift splitter routes (intelligent compliance-based shift splitting)
try:
    from rosteriq.routes.shift_split import router as shift_split_router
    app.include_router(shift_split_router)
    logger.info("Shift splitter routes registered")
except ImportError:
    logger.warning("Shift splitter routes unavailable")
except Exception as e:
    logger.error(f"Failed to register shift splitter routes: {e}")

# Revenue forecasting routes
try:
    from rosteriq.routes.revenue import router as revenue_router
    app.include_router(revenue_router)
    logger.info("Revenue forecasting routes registered")
except ImportError:
    logger.warning("Revenue forecasting routes unavailable")
except Exception as e:
    logger.error(f"Failed to register revenue forecasting routes: {e}")

# Real-time demand surge detection routes
try:
    from rosteriq.routes.surge import router as surge_router
    app.include_router(surge_router)
    logger.info("Real-time surge detection routes registered")
except ImportError:
    logger.warning("Real-time surge detection routes unavailable")
except Exception as e:
    logger.error(f"Failed to register surge detection routes: {e}")

# Push notification routes (Web Push API)
try:
    from rosteriq.routes.push import router as push_router
    app.include_router(push_router, tags=["push"])
    logger.info("Push notification routes registered")
except ImportError:
    logger.warning("Push notification routes unavailable")
except Exception as e:
    logger.error(f"Failed to register push notification routes: {e}")

# Approval workflow routes (roster approval engine with rules)
try:
    from rosteriq.routes.approvals import router as approvals_router
    app.include_router(approvals_router)
    logger.info("Approval workflow routes registered")
except ImportError:
    logger.warning("Approval workflow routes unavailable")
except Exception as e:
    logger.error(f"Failed to register approval workflow routes: {e}")


# Conflict detection routes (compliance and operational conflict detection)
try:
    from rosteriq.routes.conflicts import router as conflicts_router
    app.include_router(conflicts_router)
    logger.info("Conflict detection routes registered")
except ImportError:
    logger.warning("Conflict detection routes unavailable")
except Exception as e:
    logger.error(f"Failed to register conflict detection routes: {e}")


# Cross-venue synchronisation routes (prevent multi-venue double-booking)
try:
    from rosteriq.routes.cross_venue import router as cross_venue_router
    app.include_router(cross_venue_router)
    logger.info("Cross-venue synchronisation routes registered")
except ImportError:
    logger.warning("Cross-venue synchronisation routes unavailable")
except Exception as e:
    logger.error(f"Failed to register cross-venue routes: {e}")


# Roster comparison and diff routes
try:
    from rosteriq.routes.roster_diff import router as roster_diff_router
    app.include_router(roster_diff_router)
    logger.info("Roster diff and comparison routes registered")
except ImportError:
    logger.warning("Roster diff routes unavailable")
except Exception as e:
    logger.error(f"Failed to register roster diff routes: {e}")


# Admin logging routes (log querying and request tracing)
try:
    from rosteriq.routes.admin_logs import admin_logs_router, init_log_buffering
    app.include_router(admin_logs_router)
    logger.info("Admin logging routes registered at /api/v1/admin/logs")
    # Initialize log buffering
    init_log_buffering()
except ImportError:
    logger.warning("Admin logging routes unavailable")
except Exception as e:
    logger.error(f"Failed to register admin logging routes: {e}")


# Task scheduler for background jobs
try:
    from rosteriq.services.task_scheduler import setup_scheduler
    setup_scheduler(app)
except ImportError:
    pass


# Staff skill matrix and training gap analysis routes
try:
    from rosteriq.routes.skill_matrix import router as skill_matrix_router
    app.include_router(skill_matrix_router)
    logger.info("Skill matrix and training gap analysis routes registered")
except ImportError:
    logger.warning("Skill matrix routes unavailable")
except Exception as e:
    logger.error(f"Failed to register skill matrix routes: {e}")


# Test reporting and coverage analysis routes (admin endpoints)
try:
    from rosteriq.routes.test_report import router as test_report_router
    app.include_router(test_report_router)
    logger.info("Test reporting routes registered at /api/v1/admin/test-*")
except ImportError:
    logger.warning("Test reporting routes unavailable")
except Exception as e:
    logger.error(f"Failed to register test reporting routes: {e}")


# Weekly digest routes (performance summaries)
try:
    from rosteriq.routes.digest import router as digest_router
    app.include_router(digest_router)
    logger.info("Weekly digest routes registered at /api/v1/venues/{venue_id}/digest/*")
except ImportError:
    logger.warning("Weekly digest routes unavailable")
except Exception as e:
    logger.error(f"Failed to register digest routes: {e}")


# GraphQL API with Strawberry
try:
    from strawberry.fastapi import GraphQLRouter
    from graphql_schema.schema import schema

    graphql_app = GraphQLRouter(schema)
    app.include_router(graphql_app, prefix="/graphql")
    logger.info("GraphQL API registered at /graphql")
except ImportError:
    logger.warning("Strawberry GraphQL not installed; GraphQL API unavailable")
except Exception as e:
    logger.error(f"Failed to register GraphQL API: {e}")


# ============================================================================
# Data store — uses PostgreSQL when DATABASE_URL is set, otherwise in-memory
# ============================================================================

# The _store dict is kept for backward compatibility with all endpoint code.
# On startup, get_db() decides the backend (Postgres or memory).
# The _store proxy delegates to get_db() so we don't rewrite every endpoint.
_db = get_db()

# Connection pool for database health monitoring and circuit breaker
_pool: Optional[ConnectionPool] = None


class _StoreProxy(dict):
    """Dict-like proxy that delegates to the database layer."""

    def __getitem__(self, key):
        if key == "venues":
            return _VenueProxy(_db)
        if key == "employees":
            return _EmployeeProxy(_db)
        if key == "rosters":
            return _RosterProxy(_db)
        if key == "forecasts":
            return _ForecastProxy(_db)
        if key == "forecasters":
            return {}
        raise KeyError(key)


class _VenueProxy:
    def __init__(self, db): self._db = db
    def __setitem__(self, k, v): self._db.save_venue(v)
    def __getitem__(self, k):
        v = self._db.get_venue(k)
        if v is None: raise KeyError(k)
        return v
    def __contains__(self, k): return self._db.get_venue(k) is not None
    def get(self, k, default=None): return self._db.get_venue(k) or default
    def values(self): return self._db.list_venues()
    def __len__(self): return len(self._db.list_venues())


class _EmployeeProxy:
    def __init__(self, db): self._db = db
    def __setitem__(self, k, v): self._db.save_employee(v)
    def __getitem__(self, k):
        e = self._db.get_employee(k)
        if e is None: raise KeyError(k)
        return e
    def __contains__(self, k): return self._db.get_employee(k) is not None
    def get(self, k, default=None): return self._db.get_employee(k) or default
    def values(self): return self._db.list_employees()
    def __len__(self): return len(self._db.list_employees())


class _RosterProxy:
    def __init__(self, db): self._db = db
    def __setitem__(self, k, v): self._db.save_roster(v)
    def __getitem__(self, k):
        r = self._db.get_roster(k)
        if r is None: raise KeyError(k)
        return r
    def __contains__(self, k): return self._db.get_roster(k) is not None
    def get(self, k, default=None): return self._db.get_roster(k) or default
    def values(self): return self._db.list_rosters()
    def __len__(self): return len(self._db.list_rosters())


class _ForecastProxy(list):
    def __init__(self, db):
        super().__init__()
        self._db = db
    def extend(self, items):
        self._db.add_forecasts(list(items))
    def append(self, item):
        self._db.add_forecasts([item])
    def __iter__(self):
        return iter(self._db.get_forecasts())
    def __len__(self):
        return len(self._db.get_forecasts())


_store = _StoreProxy()


# ============================================================================
# JSON helpers — Decimal/date serialisation
# ============================================================================

# ============================================================================
# Error response standard format
# ============================================================================

class ErrorResponse(BaseModel):
    """Standardised error response format."""
    error: dict

    class Config:
        json_schema_extra = {
            "example": {
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Venue not found",
                    "status": 404,
                }
            }
        }


class RIQJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json

        def default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            if isinstance(obj, set):
                return list(obj)
            if hasattr(obj, "model_dump"):
                return obj.model_dump()
            if hasattr(obj, "value"):
                return obj.value
            raise TypeError(f"Not serialisable: {type(obj)}")

        return json.dumps(content, default=default, ensure_ascii=False).encode("utf-8")


app.default_response_class = RIQJSONResponse


# ============================================================================
# Custom exception handlers for standardised error responses
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTPException with standardised format."""
    # Extract error code from detail if it's a dict, otherwise use status code
    if isinstance(exc.detail, dict):
        message = exc.detail.get("message", str(exc.detail))
        code = exc.detail.get("code", "HTTP_ERROR")
    else:
        message = str(exc.detail)
        # Try to infer code from status code
        code_map = {
            400: "BAD_REQUEST",
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            409: "CONFLICT",
            422: "UNPROCESSABLE_ENTITY",
            429: "TOO_MANY_REQUESTS",
            500: "INTERNAL_SERVER_ERROR",
            503: "SERVICE_UNAVAILABLE",
        }
        code = code_map.get(exc.status_code, "HTTP_ERROR")

    return RIQJSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "status": exc.status_code,
            }
        },
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Handle ValueError with 400 Bad Request."""
    logger.warning(f"ValueError: {str(exc)}", exc_info=exc)
    return RIQJSONResponse(
        status_code=400,
        content={
            "error": {
                "code": "BAD_REQUEST",
                "message": str(exc),
                "status": 400,
            }
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions with generic message (don't leak internals)."""
    logger.exception(f"Unhandled exception: {exc}")
    return RIQJSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An internal error occurred. Please try again later.",
                "status": 500,
            }
        },
    )


# ============================================================================
# Request/response schemas
# ============================================================================

class GenerateRosterRequest(BaseModel):
    venue_id: str
    week_start: date
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF


class DailyRosterRequest(BaseModel):
    venue_id: str
    target_date: date
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF


class VarianceRequest(BaseModel):
    signals: list[dict]  # list of {signal_type, value, weight, confidence, source}


class DecisionRequest(BaseModel):
    venue_id: str
    variance: float
    threshold: float = 0.15


class HealthResponse(BaseModel):
    status: str
    version: str
    modules: list[str]
    venues_loaded: int
    employees_loaded: int


# ============================================================================
# Startup event — log banner, initialize monitoring, and log routes
# ============================================================================

@app.on_event("startup")
async def log_routes():
    """Log registered routes on startup for debugging."""
    routes = [r for r in app.routes if hasattr(r, 'methods')]
    logger.info(f"RosterIQ started: {len(routes)} routes registered")


def _validate_environment():
    """Validate required environment variables at startup.

    In production, missing critical vars are fatal (RuntimeError).
    In development, missing vars produce warnings only.
    """
    env = os.environ.get("ENVIRONMENT", "development")
    is_production = env == "production"

    if is_production:
        # DATABASE_URL is required in production
        if not os.environ.get("DATABASE_URL"):
            raise RuntimeError(
                "FATAL: DATABASE_URL must be set in production. "
                "Cannot start without a database connection."
            )

        # JWT_SECRET must be set and must not be a dev placeholder
        jwt_secret = os.environ.get("JWT_SECRET", "")
        if not jwt_secret:
            raise RuntimeError(
                "FATAL: JWT_SECRET must be set in production."
            )
        if jwt_secret.startswith("dev"):
            raise RuntimeError(
                "FATAL: JWT_SECRET must not start with 'dev' in production. "
                "Use a strong, randomly generated secret."
            )

        # Warn about optional but recommended vars
        if not os.environ.get("SENTRY_DSN"):
            logger.warning(
                "SENTRY_DSN is not set — error reporting will not be available"
            )
        if not os.environ.get("STRIPE_SECRET_KEY"):
            logger.warning(
                "STRIPE_SECRET_KEY is not set — billing features will be unavailable"
            )

        # Log integration configuration status
        integrations = {
            "Tanda": bool(os.environ.get("TANDA_API_KEY") or os.environ.get("TANDA_CLIENT_ID")),
            "Deputy": bool(os.environ.get("DEPUTY_API_KEY") or os.environ.get("DEPUTY_CLIENT_ID")),
            "Xero": bool(os.environ.get("XERO_CLIENT_ID")),
        }
        configured = [name for name, enabled in integrations.items() if enabled]
        not_configured = [name for name, enabled in integrations.items() if not enabled]
        if configured:
            logger.info("Integrations configured: %s", ", ".join(configured))
        if not_configured:
            logger.info("Integrations not configured: %s", ", ".join(not_configured))
    else:
        # Development mode — warn but don't crash
        if not os.environ.get("DATABASE_URL"):
            logger.warning(
                "Running in development mode with in-memory store"
            )
        if not os.environ.get("JWT_SECRET"):
            logger.warning("JWT_SECRET is not set — using insecure default")
        elif os.environ.get("JWT_SECRET", "").startswith("dev"):
            logger.warning("JWT_SECRET starts with 'dev' — not safe for production")
        if not os.environ.get("SENTRY_DSN"):
            logger.warning("SENTRY_DSN is not set")
        if not os.environ.get("STRIPE_SECRET_KEY"):
            logger.warning("STRIPE_SECRET_KEY is not set")


@app.on_event("startup")
async def startup_event():
    """Log startup banner with version, database type, port, and environment."""
    # Validate environment variables
    _validate_environment()

    # Validate configuration on startup
    try:
        config = get_app_config()
        errors = config.validate()

        if errors:
            logger.error(f"Configuration validation failed with {len(errors)} error(s):")
            for error in errors:
                logger.error(f"  - {error}")

            if config.is_production():
                logger.critical(
                    "Application is in PRODUCTION mode with configuration errors. Exiting."
                )
                sys.exit(1)
            else:
                logger.warning(
                    "Configuration errors detected in non-production environment. "
                    "Starting anyway, but fix these before deploying to production."
                )

        # Log configuration summary
        config.log_summary()
    except Exception as e:
        logger.error(f"Failed to initialize configuration: {e}")
        sys.exit(1)

    db_type = "postgres" if os.environ.get("DATABASE_URL") else "memory"
    port = os.environ.get("PORT", "8000")
    env = os.environ.get("ENVIRONMENT", "development")

    banner = f"""
    ╔════════════════════════════════════════════════════════════╗
    ║                    RosterIQ API Starting                   ║
    ╠════════════════════════════════════════════════════════════╣
    ║ Version:        {__version__:<42} ║
    ║ Database:       {db_type:<42} ║
    ║ Port:           {port:<42} ║
    ║ Environment:    {env:<42} ║
    ║ Timestamp:      {datetime.now().isoformat():<28} ║
    ╚════════════════════════════════════════════════════════════╝
    """
    logger.info(banner)

    # Initialize i18n (internationalization)
    try:
        i18n = init_i18n()
        available_locales = i18n.available_locales()
        logger.info(f"i18n initialized with locales: {', '.join(available_locales)}")
    except Exception as e:
        logger.warning(f"Failed to initialize i18n: {e}")

    # Initialize Sentry error reporting if configured
    try:
        if init_sentry(app):
            logger.info("Sentry error reporting initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Sentry: {e}")

    # Initialize cache manager and start cleanup task
    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.start_cleanup_task()
            logger.info("Cache manager initialized with background cleanup task")
        except Exception as e:
            logger.warning(f"Failed to initialize cache manager: {e}")

    # Start webhook queue processor
    try:
        from rosteriq.services.webhook_queue import get_webhook_queue
        queue = get_webhook_queue()
        await queue.start()
        logger.info("Webhook queue processor started")
    except Exception as e:
        logger.warning(f"Failed to start webhook queue processor: {e}")

    # Initialize POS real-time polling for venues with POS configured
    try:
        if '_pos_feed' in globals():
            # In a real implementation, you'd iterate over configured venues
            # and start polling for each one that has POS integration enabled.
            # For now, this is a placeholder for future venue-specific setup.
            logger.info("POS real-time feed ready for polling (awaiting venue configuration)")
    except Exception as e:
        logger.warning(f"Failed to initialize POS polling: {e}")

    # Initialize connection pool for database health monitoring
    global _pool
    if DATABASE_URL:
        try:
            _pool = ConnectionPool(
                dsn=DATABASE_URL,
                min_size=int(os.environ.get("DB_POOL_MIN_SIZE", "5")),
                max_size=int(os.environ.get("DB_POOL_MAX_SIZE", "20")),
                max_idle_time=int(os.environ.get("DB_POOL_IDLE_TIMEOUT", "300")),
                max_lifetime=int(os.environ.get("DB_POOL_LIFETIME", "3600")),
                health_check_interval=int(os.environ.get("DB_HEALTH_CHECK_INTERVAL", "30")),
                query_timeout=int(os.environ.get("DB_QUERY_TIMEOUT", "30")),
            )
            await _pool.initialize()
            logger.info("Connection pool initialized and health monitoring started")

            # Register database health monitoring routes
            health_router = create_db_health_router(_pool)
            app.include_router(health_router)
            logger.info("Database health monitoring routes registered")
        except ImportError as e:
            logger.warning(f"Connection pool unavailable (asyncpg not installed): {e}")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")

    # Register security administration routes
    try:
        from rosteriq.routes.security import create_security_router
        # _security_middleware should be set during middleware setup
        if '_security_middleware' in globals():
            security_router = create_security_router(_security_middleware)
            app.include_router(security_router)
            logger.info("Security administration routes registered")
        else:
            logger.warning("Security middleware not available; security routes not registered")
    except ImportError as e:
        logger.warning(f"Security routes unavailable: {e}")
    except Exception as e:
        logger.error(f"Failed to register security routes: {e}")


@app.on_event("startup")
async def cache_vendor_assets():
    """Download and cache vendor JS libraries (Chart.js) for self-hosting."""
    import pathlib
    vendor_dir = pathlib.Path(__file__).parent / "static" / "vendor"
    vendor_dir.mkdir(parents=True, exist_ok=True)
    chart_path = vendor_dir / "chart.umd.js"
    if chart_path.exists() and chart_path.stat().st_size > 100000:
        logger.info("Chart.js already cached locally")
        return
    cdns = [
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js",
        "https://unpkg.com/chart.js@4.4.1/dist/chart.umd.js",
        "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js",
    ]
    import urllib.request
    for url in cdns:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "RosterIQ/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = resp.read()
                if len(data) > 100000:
                    chart_path.write_bytes(data)
                    logger.info(f"Chart.js cached from {url} ({len(data)} bytes)")
                    return
        except Exception as e:
            logger.warning(f"Failed to download Chart.js from {url}: {e}")
    logger.error("Could not cache Chart.js from any CDN — charts may not render")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    # Stop webhook queue processor
    try:
        from rosteriq.services.webhook_queue import get_webhook_queue
        queue = get_webhook_queue()
        await queue.stop()
        logger.info("Webhook queue processor stopped")
    except Exception as e:
        logger.warning(f"Error stopping webhook queue processor: {e}")

    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.stop_cleanup_task()
            logger.info("Cache cleanup task stopped")
        except Exception as e:
            logger.warning(f"Error stopping cache cleanup task: {e}")

    # Close database store connection
    try:
        from rosteriq.database import PostgresStore
        if isinstance(_db, PostgresStore):
            _db.close()
            logger.info("Database store connection closed")
    except Exception as e:
        logger.warning(f"Error closing database store: {e}")

    # Close connection pool
    global _pool
    if _pool:
        try:
            await _pool.close_all()
            logger.info("Connection pool closed")
        except Exception as e:
            logger.warning(f"Error closing connection pool: {e}")


# ============================================================================
# API Documentation
# ============================================================================

@app.get("/docs/api", include_in_schema=False)
async def api_docs():
    """Serve comprehensive API documentation page."""
    try:
        from fastapi.responses import FileResponse
        import pathlib
        docs_file = pathlib.Path(__file__).parent / "static" / "docs.html"
        if docs_file.exists():
            return FileResponse(docs_file, media_type="text/html")
    except Exception as e:
        logger.error(f"Error serving API docs: {e}")

    raise HTTPException(404, "API documentation not found")


# ============================================================================
# Health & info
# ============================================================================

@app.get("/")
async def root():
    """Redirect root to login page."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/login")


@app.get("/login", tags=["auth"])
async def login_page():
    """Login page — entry point for all users."""
    try:
        import pathlib
        login_file = pathlib.Path(__file__).parent / "static" / "login.html"
        if login_file.exists():
            from fastapi.responses import FileResponse
            return FileResponse(login_file, media_type="text/html")
    except Exception:
        pass
    from fastapi.responses import HTMLResponse
    return HTMLResponse("<h1>RosterIQ Login</h1><p>Login page not found.</p>")


@app.get("/api/status", response_model=HealthResponse)
async def api_status():
    """API status endpoint — returns system info as JSON."""
    return HealthResponse(
        status="ok",
        version=__version__,
        modules=[
            "models", "award_rules", "cost_calculator", "variance_engine",
            "decision_engine", "ensemble", "tanda_adapter", "pos_import",
            "roster_optimiser",
        ],
        venues_loaded=len(_store["venues"]),
        employees_loaded=len(_store["employees"]),
    )


@app.get("/health")
async def health():
    """Liveness probe — returns 200 if the process is alive (no auth required)."""
    response = {
        "status": "ok",
        "version": __version__,
        "timestamp": datetime.now().isoformat(),
    }

    # Include pool utilization if available
    if _pool:
        try:
            pool_stats = await _pool.get_stats()
            response["pool"] = pool_stats
        except Exception as e:
            logger.warning(f"Error fetching pool stats for /health: {e}")

    return response


@app.get("/ready")
async def ready():
    """
    Readiness probe — checks critical dependencies (database connectivity).
    Returns 503 if any critical check fails.
    """
    checks = {}

    # Check database connectivity
    try:
        venues = _db.list_venues()
        checks["database"] = True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        checks["database"] = False

    # Check scheduler if available
    scheduler_available = False
    try:
        from rosteriq.services.task_scheduler import get_scheduler
        sched = get_scheduler()
        scheduler_available = sched is not None
    except Exception:
        pass
    checks["scheduler"] = scheduler_available

    # Determine status
    is_ready = checks.get("database", False)  # Database is critical
    status_code = 200 if is_ready else 503

    response = {
        "status": "ready" if is_ready else "not_ready",
        "checks": checks,
    }

    if status_code == 503:
        return JSONResponse(response, status_code=503)
    return response


# ============================================================================
# Progressive Web App Routes
# ============================================================================

@app.get("/sw.js", include_in_schema=False)
async def service_worker():
    """Serve Service Worker from root for correct scope."""
    from fastapi.responses import FileResponse
    import pathlib
    try:
        _static_dir = pathlib.Path(__file__).parent / "static"
        sw_path = _static_dir / "sw.js"
        if sw_path.exists():
            return FileResponse(sw_path, media_type="application/javascript")
    except Exception:
        pass
    return JSONResponse({"error": "Service Worker not found"}, status_code=404)


@app.get("/static/manifest.json", include_in_schema=False)
async def manifest():
    """Serve manifest.json for PWA."""
    from fastapi.responses import FileResponse
    import pathlib
    try:
        _static_dir = pathlib.Path(__file__).parent / "static"
        manifest_path = _static_dir / "manifest.json"
        if manifest_path.exists():
            return FileResponse(manifest_path, media_type="application/manifest+json")
    except Exception:
        pass
    return JSONResponse({"error": "Manifest not found"}, status_code=404)


@app.get("/metrics")
async def metrics():
    """
    Metrics endpoint for monitoring (no auth required).
    Reports: total requests, errors, response time, uptime, resource counts.
    """
    venue_count = len(_store["venues"])
    employee_count = len(_store["employees"])

    return {
        "version": __version__,
        **_metrics.get_metrics(),
        "data": {
            "venues": venue_count,
            "employees": employee_count,
            "rosters": len(_store["rosters"]),
            "forecasts": len(_store["forecasts"]),
        },
    }


@app.get("/api/cache/stats")
async def cache_stats():
    """
    Cache statistics endpoint for monitoring (no auth required).
    Reports: cache sizes, hit rates, evictions per cache.
    """
    if not get_cache_manager:
        return {"error": "Cache manager not available"}

    try:
        cache_manager = get_cache_manager()
        return {
            "status": "ok",
            "caches": cache_manager.get_stats(),
        }
    except Exception as e:
        logger.error(f"Error retrieving cache stats: {e}")
        return {"error": str(e)}, 500


@app.get("/admin", tags=["admin"])
async def admin_panel():
    """
    Admin panel dashboard — static HTML page for system administration.

    Features:
    - User management (list, create, toggle active, change roles)
    - Venue overview (subscription status, employee count, last roster)
    - System health (database, scheduler, cache stats)
    - Billing summary (subscription tiers, revenue)
    - Plugin installations

    Returns static HTML that loads data via fetch() to API endpoints.
    """
    try:
        import pathlib
        admin_file = pathlib.Path(__file__).parent / "static" / "admin.html"
        if admin_file.exists():
            from fastapi.responses import FileResponse
            return FileResponse(admin_file, media_type="text/html")
        else:
            return {"error": "Admin panel not found"}, 404
    except Exception as e:
        logger.error(f"Error serving admin panel: {e}")
        return {"error": str(e)}, 500


@app.get("/api/v1/admin/cache-stats", tags=["admin"])
async def get_cache_stats():
    """
    Get cache statistics for monitoring and debugging.

    Returns cache hit/miss rates, entry counts, and Redis/in-memory mode status.
    Requires admin authentication in production.

    Returns:
        dict: Cache statistics including:
            - All named caches (venue_configs, employee_lists, etc.)
            - Redis mode info if enabled
            - Hit rates and entry counts
    """
    if not get_cache_manager:
        return {
            "error": "Cache manager not available",
            "mode": "disabled"
        }, 503

    try:
        cache_manager = get_cache_manager()
        stats = cache_manager.get_stats()
        return {
            "status": "ok",
            "timestamp": datetime.utcnow().isoformat(),
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        return {
            "error": str(e),
            "status": "error"
        }, 500


@app.get("/api/v1/admin/http-stats", tags=["admin"])
async def get_http_stats():
    """
    Get HTTP client statistics for external API calls.

    Returns per-host metrics for monitoring resilience, retries, circuit breaker states,
    and latency. Useful for debugging integration issues with Tanda, Stripe, BOM, etc.

    Returns:
        dict: HTTP client statistics including per-host:
            - request_count: Total requests made
            - error_count: Failed requests
            - success_count: Successful requests
            - avg_latency_ms: Average request latency
            - min/max_latency_ms: Latency bounds
            - retry_count: Total retries (across all requests)
            - circuit_state: Current circuit breaker state (closed/open/half_open)
            - last_error_at: Timestamp of last error
            - last_success_at: Timestamp of last success
    """
    try:
        from rosteriq.services.http_client import get_http_client

        client = await get_http_client()
        stats = client.stats()

        return {
            "status": "ok",
            "timestamp": datetime.utcnow().isoformat(),
            "hosts": stats
        }
    except Exception as e:
        logger.error(f"Error getting HTTP client stats: {e}")
        return {
            "error": str(e),
            "status": "error"
        }, 500


@app.get("/staff", tags=["staff"])
async def staff_portal():
    """
    Staff self-service portal — single-page app for shift and availability management.

    Features:
    - View upcoming shifts with timeline visualization
    - Set availability preferences by day and time block
    - Request shift swaps with other staff members
    - View pay estimates with penalty rate breakdowns
    - Manage profile information

    Returns static HTML that loads data via authenticated API endpoints.
    """
    try:
        import pathlib
        staff_file = pathlib.Path(__file__).parent / "static" / "staff.html"
        if staff_file.exists():
            from fastapi.responses import FileResponse
            return FileResponse(staff_file, media_type="text/html")
        else:
            return {"error": "Staff portal not found"}, 404
    except Exception as e:
        logger.error(f"Error serving staff portal: {e}")
        return {"error": str(e)}, 500


# ============================================================================
# Venue management
# ============================================================================

@app.post("/venues")
async def create_venue(venue: VenueConfig):
    _store["venues"][venue.id] = venue

    # Invalidate venue cache
    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.invalidate_all("venue_configs")
            logger.debug("Invalidated venue_configs cache after create")
        except Exception as e:
            logger.debug(f"Cache invalidation failed: {e}")

    return {"id": venue.id, "status": "created"}


@app.get("/venues")
async def list_venues(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    request: Request = None,
):
    # Use cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"venues_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            cached = await cache_manager.get("venue_configs", cache_key)
            if cached is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached
        except Exception as e:
            logger.debug(f"Cache lookup failed: {e}")

    all_venues = list(_store["venues"].values())
    total = len(all_venues)
    items = all_venues[offset:offset + limit]
    result = {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }

    # Store in cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"venues_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            await cache_manager.set("venue_configs", cache_key, result, ttl=300)
            logger.debug(f"Cached: {cache_key}")
        except Exception as e:
            logger.debug(f"Cache store failed: {e}")

    return result


@app.get("/venues/{venue_id}")
async def get_venue(venue_id: str):
    if venue_id not in _store["venues"]:
        raise HTTPException(404, f"Venue {venue_id} not found")
    return _store["venues"][venue_id]


# ============================================================================
# Employee management
# ============================================================================

@app.post("/employees")
async def create_employee(employee: Employee):
    _store["employees"][employee.id] = employee

    # Invalidate employee cache
    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.invalidate_all("employee_lists")
            logger.debug("Invalidated employee_lists cache after create")
        except Exception as e:
            logger.debug(f"Cache invalidation failed: {e}")

    return {"id": employee.id, "status": "created"}


@app.post("/employees/bulk")
async def bulk_create_employees(employees: list[Employee]):
    for emp in employees:
        _store["employees"][emp.id] = emp

    # Invalidate employee cache
    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.invalidate_all("employee_lists")
            logger.debug("Invalidated employee_lists cache after bulk create")
        except Exception as e:
            logger.debug(f"Cache invalidation failed: {e}")

    return {"count": len(employees), "status": "created"}


@app.get("/employees")
async def list_employees(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    venue_id: Optional[str] = Query(None),
    request: Request = None,
):
    # Use cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"employees_venue={venue_id}_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            cached = await cache_manager.get("employee_lists", cache_key)
            if cached is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached
        except Exception as e:
            logger.debug(f"Cache lookup failed: {e}")

    all_employees = list(_store["employees"].values())

    if venue_id:
        all_employees = [e for e in all_employees if e.venue_id == venue_id]

    total = len(all_employees)
    items = all_employees[offset:offset + limit]
    result = {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }

    # Store in cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"employees_venue={venue_id}_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            await cache_manager.set("employee_lists", cache_key, result, ttl=120)
            logger.debug(f"Cached: {cache_key}")
        except Exception as e:
            logger.debug(f"Cache store failed: {e}")

    return result


@app.get("/employees/{employee_id}")
async def get_employee(employee_id: str):
    if employee_id not in _store["employees"]:
        raise HTTPException(404, f"Employee {employee_id} not found")
    return _store["employees"][employee_id]


# ============================================================================
# Forecasts
# ============================================================================

@app.post("/forecasts")
async def add_forecasts(forecasts: list[DemandForecast]):
    _store["forecasts"].extend(forecasts)

    # Invalidate forecast cache
    if get_cache_manager:
        try:
            cache_manager = get_cache_manager()
            await cache_manager.invalidate_all("forecast_data")
            logger.debug("Invalidated forecast_data cache after add")
        except Exception as e:
            logger.debug(f"Cache invalidation failed: {e}")

    return {"count": len(forecasts), "status": "added"}


@app.get("/forecasts")
async def get_forecasts(
    venue_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    request: Request = None,
):
    # Use cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"forecasts_venue={venue_id}_start={start_date}_end={end_date}_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            cached = await cache_manager.get("forecast_data", cache_key)
            if cached is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached
        except Exception as e:
            logger.debug(f"Cache lookup failed: {e}")

    results = _store["forecasts"]
    if venue_id:
        results = [f for f in results if f.venue_id == venue_id]
    if start_date:
        results = [f for f in results if f.date >= start_date]
    if end_date:
        results = [f for f in results if f.date <= end_date]

    total = len(results)
    items = results[offset:offset + limit]
    result = {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }

    # Store in cache if available
    if get_cache_manager and request:
        try:
            cache_key = f"forecasts_venue={venue_id}_start={start_date}_end={end_date}_limit={limit}_offset={offset}"
            cache_manager = get_cache_manager()
            await cache_manager.set("forecast_data", cache_key, result, ttl=600)
            logger.debug(f"Cached: {cache_key}")
        except Exception as e:
            logger.debug(f"Cache store failed: {e}")

    return result


@app.get("/forecasts/required-staff")
async def get_required_staff(
    venue_id: str,
    target_date: date,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
):
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    day_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == target_date
    ]
    if not day_forecasts:
        raise HTTPException(404, f"No forecasts for {venue_id} on {target_date}")

    min_staff = venue.min_staff if venue else None
    required = calculate_required_staff(day_forecasts, covers_per_staff, min_staff)
    periods = identify_peak_periods(required)

    return {"date": target_date, "required_by_hour": required, "peak_periods": periods}


# ============================================================================
# Roster generation
# ============================================================================

@app.post("/rosters/generate")
async def generate_roster(req: GenerateRosterRequest):
    venue = _store["venues"].get(req.venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {req.venue_id} not found")

    employees = list(_store["employees"].values())
    if not employees:
        raise HTTPException(400, "No employees loaded")

    week_end = req.week_start + timedelta(days=6)
    forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == req.venue_id
        and req.week_start <= f.date <= week_end
    ]
    if not forecasts:
        raise HTTPException(400, f"No forecasts for {req.venue_id} in week of {req.week_start}")

    roster = generate_weekly_roster(
        req.week_start, forecasts, employees, venue, req.covers_per_staff
    )

    _store["rosters"][roster.id] = roster

    # Broadcast roster update to connected clients (non-fatal)
    try:
        dispatcher = get_dispatcher()
        summary = f"Generated roster for week of {req.week_start} with {len(roster.shifts)} shifts"
        await dispatcher.roster_updated(
            venue_id=req.venue_id,
            roster_id=roster.id,
            summary=summary,
        )
    except Exception as e:
        logger.warning(f"Failed to broadcast roster update: {e}")

    return roster


@app.post("/rosters/generate-daily")
async def generate_daily(req: DailyRosterRequest):
    venue = _store["venues"].get(req.venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {req.venue_id} not found")

    employees = list(_store["employees"].values())
    forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == req.venue_id and f.date == req.target_date
    ]

    shifts = generate_daily_roster(
        req.target_date, forecasts, employees, venue.state,
        venue_config=venue, covers_per_staff=req.covers_per_staff,
    )
    return {"date": req.target_date, "shifts": shifts, "count": len(shifts)}


# ============================================================================
# Roster analysis
# ============================================================================

@app.get("/rosters")
async def list_rosters(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    all_rosters = list(_store["rosters"].values())
    total = len(all_rosters)
    items = all_rosters[offset:offset + limit]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@app.get("/rosters/{roster_id}")
async def get_roster(roster_id: str):
    if roster_id not in _store["rosters"]:
        raise HTTPException(404, f"Roster {roster_id} not found")
    return _store["rosters"][roster_id]


@app.get("/rosters/{roster_id}/analyse")
async def analyse(roster_id: str):
    roster = _store["rosters"].get(roster_id)
    if not roster:
        raise HTTPException(404, f"Roster {roster_id} not found")

    venue = _store["venues"].get(roster.venue_id)
    state = venue.state if venue else State.vic

    result = analyse_roster(roster, _store["employees"], state)
    return result


@app.get("/rosters/{roster_id}/suggestions")
async def get_suggestions(
    roster_id: str,
    covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
):
    roster = _store["rosters"].get(roster_id)
    if not roster:
        raise HTTPException(404, f"Roster {roster_id} not found")

    venue = _store["venues"].get(roster.venue_id)
    state = venue.state if venue else State.vic

    week_end = roster.week_start + timedelta(days=6)
    forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == roster.venue_id
        and roster.week_start <= f.date <= week_end
    ]

    suggestions = suggest_improvements(
        roster, forecasts, _store["employees"], state, covers_per_staff
    )
    return {"roster_id": roster_id, "suggestions": suggestions, "count": len(suggestions)}


# ============================================================================
# Cost endpoints
# ============================================================================

@app.get("/costs/shift/{employee_id}")
async def shift_cost(
    employee_id: str,
    shift_date: date = Query(...),
    start_hour: int = Query(...),
    end_hour: int = Query(...),
    break_minutes: int = Query(0),
    state: State = Query(State.vic),
):
    emp = _store["employees"].get(employee_id)
    if not emp:
        raise HTTPException(404, f"Employee {employee_id} not found")

    from rosteriq.models import Shift, ShiftStatus
    from datetime import time
    shift = Shift(
        id="cost-calc", employee_id=employee_id, date=shift_date,
        start_time=time(start_hour, 0),
        end_time=time(end_hour if end_hour < 24 else 0, 0),
        break_minutes=break_minutes, status=ShiftStatus.scheduled, role="general",
    )
    breakdown = calculate_shift_cost_breakdown(emp, shift, state)
    return breakdown


@app.get("/costs/labour-percentage")
async def labour_pct(
    roster_id: str,
    revenue: float,
):
    roster = _store["rosters"].get(roster_id)
    if not roster:
        raise HTTPException(404, f"Roster {roster_id} not found")

    venue = _store["venues"].get(roster.venue_id)
    state = venue.state if venue else State.vic

    total_cost = calculate_roster_cost(roster, _store["employees"], state)
    pct = calculate_labour_percentage(total_cost, Decimal(str(revenue)))
    return {"roster_id": roster_id, "labour_cost": total_cost, "revenue": revenue, "percentage": pct}


# ============================================================================
# Variance & decisions (real-time)
# ============================================================================

@app.post("/variance/calculate")
async def calc_variance(req: VarianceRequest):
    signals = []
    for s in req.signals:
        signals.append(create_signal(
            signal_type=s["signal_type"],
            value=s["value"],
            confidence=s.get("confidence", 0.8),
            source=s.get("source", "api"),
        ))
    variance = calculate_weighted_variance(signals)
    breach = detect_threshold_breach(variance)
    summary = get_signal_summary(signals)
    return {
        "variance": variance,
        "breach": breach,
        "signals": summary,
    }


@app.post("/decisions/recommend")
async def recommend_decision(req: DecisionRequest):
    venue = _store["venues"].get(req.venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {req.venue_id} not found")

    # Get today's active shifts
    today = date.today()
    active_shifts = []
    for roster in _store["rosters"].values():
        for shift in roster.shifts:
            if shift.date == today and shift.status in (
                ShiftStatus.scheduled, ShiftStatus.confirmed, ShiftStatus.in_progress
            ):
                active_shifts.append(shift)

    employees = list(_store["employees"].values())
    available = [e for e in employees if e.id not in {s.employee_id for s in active_shifts}]

    result = make_decision(
        variance=req.variance,
        active_shifts=active_shifts,
        available_employees=available,
        employee_lookup=_store["employees"],
        state=venue.state,
        threshold=req.threshold,
    )
    return result


# ============================================================================
# Award rules helpers
# ============================================================================

@app.get("/awards/day-type")
async def day_type_check(check_date: date, state: State = State.vic):
    dt = get_day_type(check_date, state)
    return {"date": check_date, "state": state, "day_type": dt}


@app.get("/awards/penalty-rate")
async def penalty_rate(
    employment_type: EmploymentType,
    check_date: date,
    state: State = State.vic,
):
    dt = get_day_type(check_date, state)
    mult = get_penalty_multiplier(employment_type, dt)
    return {
        "employment_type": employment_type,
        "date": check_date,
        "day_type": dt,
        "multiplier": float(mult),
    }


@app.get("/awards/public-holidays/{state}")
async def public_holidays(state: State, year: int = 2026):
    holidays = get_public_holidays(state, year)
    return {"state": state, "year": year, "holidays": holidays}


# ============================================================================
# POS data import
# ============================================================================


class POSImportRequest(BaseModel):
    venue_id: str
    csv_data: str  # Raw CSV content
    system: Optional[str] = None  # 'lightspeed', 'square', 'hl' — auto-detected if omitted


@app.post("/pos/import")
async def import_pos(req: POSImportRequest):
    """
    Import POS transaction data from a CSV string.

    Accepts raw CSV content (from Lightspeed, Square, or H&L),
    normalises it, and stores the aggregated hourly records.
    The data feeds the ensemble forecaster for demand prediction.
    """
    venue = _store["venues"].get(req.venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {req.venue_id} not found")

    from rosteriq.pos_import import import_pos_string
    try:
        records = import_pos_string(req.csv_data, system=req.system)
    except ValueError as e:
        raise HTTPException(400, str(e))

    # Convert to DemandForecasts and store
    new_forecasts = []
    for rec in records:
        fc = DemandForecast(
            id=f"pos-{rec['date']}-{rec['hour']}",
            venue_id=req.venue_id,
            date=rec["date"],
            hour=rec["hour"],
            predicted_covers=float(rec["covers"]),
            confidence=0.9,
            signals_used=["historical"],
            model_version="pos-import-v1",
        )
        new_forecasts.append(fc)

    _store["forecasts"].extend(new_forecasts)

    return {
        "status": "imported",
        "records": len(records),
        "forecasts_created": len(new_forecasts),
        "date_range": {
            "from": str(records[0]["date"]) if records else None,
            "to": str(records[-1]["date"]) if records else None,
        },
    }


@app.post("/pos/import-file")
async def import_pos_file(venue_id: str = Query(...), system: Optional[str] = None):
    """
    Import POS data from an uploaded file.

    Use with multipart/form-data upload. The file is parsed,
    normalised, and stored as historical demand data.
    """
    # This endpoint would use FastAPI's UploadFile in production
    # For now, direct users to /pos/import with CSV string
    return {
        "status": "use_csv_endpoint",
        "message": "Upload your CSV to /pos/import as csv_data field",
        "supported_systems": ["lightspeed", "square", "hl"],
    }


# ============================================================================
# Demo data loader
# ============================================================================

@app.post("/demo/load")
async def load_demo():
    """Load demo data (same as CLI demo) for quick testing."""
    from rosteriq.cli import _build_demo_employees, _build_demo_forecasts, _build_demo_venue

    venue = _build_demo_venue()
    employees = _build_demo_employees()

    today = date.today()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    week_start = today + timedelta(days=days_until_monday)

    forecasts = _build_demo_forecasts(week_start)

    _store["venues"][venue.id] = venue
    for emp in employees:
        _store["employees"][emp.id] = emp
    _store["forecasts"].extend(forecasts)

    return {
        "status": "loaded",
        "venue": venue.name,
        "employees": len(employees),
        "forecasts": len(forecasts),
        "week_start": week_start,
    }


# ============================================================================
# Tanda OAuth & webhook endpoints
# ============================================================================

TANDA_CLIENT_ID = os.environ.get("TANDA_CLIENT_ID", "")
TANDA_CLIENT_SECRET = os.environ.get("TANDA_CLIENT_SECRET", "")
TANDA_REDIRECT_URI = os.environ.get("TANDA_REDIRECT_URI", "http://localhost:8000/tanda/callback")
TANDA_WEBHOOK_SECRET = os.environ.get("TANDA_WEBHOOK_SECRET", "")


@app.get("/tanda/connect")
async def tanda_connect(venue_id: str):
    """Redirect the venue owner to Tanda's OAuth consent screen."""
    if not TANDA_CLIENT_ID:
        raise HTTPException(400, "TANDA_CLIENT_ID not configured")

    from rosteriq.tanda_adapter import TandaOAuth
    oauth = TandaOAuth(TANDA_CLIENT_ID, TANDA_CLIENT_SECRET, TANDA_REDIRECT_URI)
    url = oauth.get_authorize_url()

    # Store venue_id in a state param so we know which venue to link
    return {
        "authorize_url": url + f"&state={venue_id}",
        "instructions": "Redirect the venue owner to authorize_url",
    }


@app.get("/tanda/callback")
async def tanda_callback(code: str, state: str = ""):
    """
    OAuth callback — Tanda redirects here after the venue owner authorizes.

    Exchanges the authorization code for tokens and stores them.
    The 'state' param carries the venue_id.
    """
    if not TANDA_CLIENT_ID:
        raise HTTPException(400, "TANDA_CLIENT_ID not configured")

    from rosteriq.tanda_adapter import TandaOAuth
    oauth = TandaOAuth(TANDA_CLIENT_ID, TANDA_CLIENT_SECRET, TANDA_REDIRECT_URI)

    credentials = await oauth.exchange_code(code)
    venue_id = state

    # Store credentials (in production, encrypt and save to tanda_credentials table)
    if venue_id:
        # Attach to venue
        _store.get("tanda_creds", {})[venue_id] = credentials

    return {
        "status": "connected",
        "venue_id": venue_id,
        "token_expires": credentials.token_expires_at.isoformat() if credentials.token_expires_at else None,
    }


@app.post("/tanda/sync/{venue_id}")
async def tanda_sync(venue_id: str):
    """
    Sync employees and shifts from Tanda for a connected venue.

    Pulls all employees and upcoming shifts, maps them to RosterIQ models,
    and stores them in the database.
    """
    from rosteriq.tanda_adapter import TandaAdapter

    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    # Get stored credentials
    creds = _store.get("tanda_creds", {}).get(venue_id)
    if not creds:
        raise HTTPException(400, f"Venue {venue_id} not connected to Tanda. Use /tanda/connect first.")

    async with TandaAdapter(creds, state=venue.state) as tanda:
        # Health check
        healthy = await tanda.health_check()
        if not healthy:
            raise HTTPException(502, "Cannot reach Tanda API")

        # Sync employees
        employees = await tanda.get_employees()
        for emp in employees:
            _store["employees"][emp.id] = emp

        # Sync upcoming shifts (next 2 weeks)
        today = date.today()
        shifts = await tanda.get_shifts(today, today + timedelta(days=14))

    return {
        "status": "synced",
        "venue_id": venue_id,
        "employees_synced": len(employees),
        "shifts_synced": len(shifts),
    }


@app.post("/tanda/push-roster")
async def tanda_push_roster(
    roster_id: str = Query(...),
    venue_id: str = Query(...),
    dry_run: bool = Query(True),
):
    """
    Push a RosterIQ roster back to Tanda.

    Args:
        roster_id: The RosterIQ roster ID to push.
        venue_id: The Tanda venue ID.
        dry_run: If True, validate without pushing. Defaults to True.

    Returns:
        PushResult with success/failure counts and details.
    """
    try:
        from rosteriq.tanda_adapter import TandaAdapter, TandaAPIError
        from rosteriq.services.tanda_roster_push import TandaRosterPush
    except ImportError as e:
        raise HTTPException(500, f"Tanda push service not available: {e}")

    # Get the roster
    try:
        roster = _store["rosters"].get(roster_id)
        if not roster:
            raise HTTPException(404, f"Roster {roster_id} not found")
    except Exception as e:
        raise HTTPException(400, f"Could not load roster: {e}")

    # Get stored credentials
    creds = _store.get("tanda_creds", {}).get(venue_id)
    if not creds:
        raise HTTPException(400, f"Venue {venue_id} not connected to Tanda. Use /tanda/connect first.")

    try:
        async with TandaAdapter(creds, state=State.vic) as tanda:
            # Health check
            healthy = await tanda.health_check()
            if not healthy:
                raise HTTPException(502, "Cannot reach Tanda API")

            # Create pusher and push
            pusher = TandaRosterPush(tanda)
            result = await pusher.push_roster(roster, venue_id, dry_run=dry_run)

        return result.to_dict()

    except TandaAPIError as e:
        raise HTTPException(502, f"Tanda API error: {e.api_error.message}")
    except Exception as e:
        raise HTTPException(500, f"Error pushing roster: {str(e)}")


@app.post("/tanda/diff-roster")
async def tanda_diff_roster(
    roster_id: str = Query(...),
    venue_id: str = Query(...),
):
    """
    Compare a RosterIQ roster against the current Tanda roster.

    Args:
        roster_id: The RosterIQ roster ID to compare.
        venue_id: The Tanda venue ID.

    Returns:
        RosterDiff with new, removed, and changed shifts.
    """
    try:
        from rosteriq.tanda_adapter import TandaAdapter, TandaAPIError
        from rosteriq.services.tanda_roster_push import TandaRosterPush
    except ImportError as e:
        raise HTTPException(500, f"Tanda push service not available: {e}")

    # Get the roster
    try:
        roster = _store["rosters"].get(roster_id)
        if not roster:
            raise HTTPException(404, f"Roster {roster_id} not found")
    except Exception as e:
        raise HTTPException(400, f"Could not load roster: {e}")

    # Get stored credentials
    creds = _store.get("tanda_creds", {}).get(venue_id)
    if not creds:
        raise HTTPException(400, f"Venue {venue_id} not connected to Tanda. Use /tanda/connect first.")

    try:
        async with TandaAdapter(creds, state=State.vic) as tanda:
            # Health check
            healthy = await tanda.health_check()
            if not healthy:
                raise HTTPException(502, "Cannot reach Tanda API")

            # Create pusher and diff
            pusher = TandaRosterPush(tanda)
            diff = await pusher.diff_roster(roster, venue_id)

        return diff.to_dict()

    except TandaAPIError as e:
        raise HTTPException(502, f"Tanda API error: {e.api_error.message}")
    except Exception as e:
        raise HTTPException(500, f"Error diffing roster: {str(e)}")


@app.post("/tanda/webhook")
async def tanda_webhook(request_obj: dict, request: Request = None):
    """
    Receive webhook events from Tanda.

    Supported events:
    - user.created / user.updated: Re-sync the affected employee
    - shift.updated: Update the shift in our store
    - clockin.updated: Log for real-time variance engine
    - roster.published: Trigger roster comparison
    """
    # Verify webhook signature if secret is configured
    if TANDA_WEBHOOK_SECRET and request:
        import hmac, hashlib, json
        signature = request.headers.get("x-tanda-signature", "")
        body = json.dumps(request_obj, separators=(",", ":")).encode()
        expected = hmac.new(
            TANDA_WEBHOOK_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(401, "Invalid webhook signature")

    event_type = request_obj.get("event") or request_obj.get("type", "unknown")
    payload = request_obj.get("payload") or request_obj.get("data", {})

    # Log the webhook for audit
    import logging
    logging.getLogger(__name__).info("Tanda webhook: %s", event_type)

    # Process based on event type
    if event_type in ("user.created", "user.updated"):
        # Could trigger employee re-sync here
        return {"status": "received", "action": "employee_sync_queued", "event": event_type}

    elif event_type == "shift.updated":
        return {"status": "received", "action": "shift_updated", "event": event_type}

    elif event_type == "clockin.updated":
        return {"status": "received", "action": "clockin_logged", "event": event_type}

    elif event_type == "roster.published":
        return {"status": "received", "action": "comparison_queued", "event": event_type}

    return {"status": "received", "action": "no_handler", "event": event_type}


# ============================================================================
# Dashboard endpoints — real-time visibility & signal aggregation
# ============================================================================

from rosteriq.data_feeds.base import FeedSignal, FeedCategory, Location, SignalStrength, STRENGTH_MULTIPLIERS
from rosteriq.data_feeds.aggregator import SignalAggregator
import random
import hashlib


# Request/response schemas for dashboard
class DemandOutlookRequest(BaseModel):
    venue_id: str
    target_date: date


class SignalDashboardRequest(BaseModel):
    venue_id: str
    start_date: date
    end_date: Optional[date] = None


class LivePulseRequest(BaseModel):
    venue_id: str


class DataFeedConfig(BaseModel):
    venue_id: str
    feeds_enabled: dict  # {feed_name: True/False}
    api_keys: Optional[dict] = None  # {feed_name: api_key}


def _generate_demo_signals(venue_id: str, target_date: date) -> list[dict]:
    """
    Generate deterministic, realistic-looking demo signals for an Australian pub scenario.
    Same date = same signals (deterministic based on venue_id + date hash).
    """
    # Seed based on venue_id and date so it's deterministic
    seed = int(hashlib.md5(f"{venue_id}{target_date}".encode()).hexdigest(), 16) % (2**31)
    rng = random.Random(seed)

    signals = []
    now = datetime.now()

    # Weather signals (sunny, rainy, cold affects foot traffic)
    weather_types = ["sunny", "rainy", "overcast", "hot", "cool"]
    weather_impact = rng.choice(weather_types)
    signals.append({
        "category": "weather",
        "source": "bom",
        "strength": rng.choice([s.value for s in SignalStrength]),
        "value": weather_impact,
        "confidence": rng.uniform(0.7, 0.95),
        "description": f"Bureau of Meteorology: {weather_impact.title()} conditions",
        "signal_hour": now.hour,
        "fetched_at": now.isoformat(),
    })

    # Local events (footy, concert, market day)
    event_types = ["AFL round", "local market", "school holidays", "concert", "sports match"]
    has_event = rng.random() > 0.6
    if has_event:
        event = rng.choice(event_types)
        signals.append({
            "category": "local_events",
            "source": "eventbrite",
            "strength": rng.choice([s.value for s in SignalStrength]),
            "value": event,
            "confidence": rng.uniform(0.8, 1.0),
            "description": f"Detected upcoming {event} in area",
            "signal_hour": now.hour,
            "fetched_at": now.isoformat(),
        })

    # Social media buzz (mentions of the venue, trending hashtags)
    buzz_levels = ["low", "medium", "high", "viral"]
    socmed_signal = {
        "category": "social_media",
        "source": "twitter_api",
        "strength": rng.choice([s.value for s in SignalStrength]),
        "value": rng.choice(buzz_levels),
        "confidence": rng.uniform(0.65, 0.9),
        "description": f"Social media mentions: {rng.choice(buzz_levels)} buzz detected",
        "signal_hour": now.hour,
        "fetched_at": now.isoformat(),
    }
    signals.append(socmed_signal)

    # Competitor activity (nearby pubs have events, promos)
    competitor_signal = {
        "category": "competitor_activity",
        "source": "web_scraper",
        "strength": rng.choice([s.value for s in SignalStrength]),
        "value": f"nearby_promo_{'active' if rng.random() > 0.5 else 'none'}",
        "confidence": rng.uniform(0.7, 0.85),
        "description": "Competitor promotions detected in 2km radius",
        "signal_hour": now.hour,
        "fetched_at": now.isoformat(),
    }
    signals.append(competitor_signal)

    # Staffing readiness (roster signal — internal)
    roster_signal = {
        "category": "staff_availability",
        "source": "roster_system",
        "strength": rng.choice([s.value for s in SignalStrength]),
        "value": rng.randint(3, 12),
        "confidence": 1.0,
        "description": f"Current roster: {rng.randint(3, 12)} staff scheduled today",
        "signal_hour": now.hour,
        "fetched_at": now.isoformat(),
    }
    signals.append(roster_signal)

    # Booking/reservation signals (if available from booking system)
    if rng.random() > 0.4:
        booking_signal = {
            "category": "reservations",
            "source": "booking_system",
            "strength": rng.choice([s.value for s in SignalStrength]),
            "value": rng.randint(2, 8),
            "confidence": rng.uniform(0.8, 1.0),
            "description": f"{rng.randint(2, 8)} reservations logged for today",
            "signal_hour": now.hour,
            "fetched_at": now.isoformat(),
        }
        signals.append(booking_signal)

    # Payroll/award period (impacts cost, may affect demand)
    day_type = get_day_type(target_date, State.vic)
    if str(day_type).lower() in ["public_holiday", "weekend"]:
        payroll_signal = {
            "category": "payroll_cycles",
            "source": "calendar",
            "strength": "high",
            "value": str(day_type),
            "confidence": 1.0,
            "description": f"{str(day_type).replace('_', ' ').title()} — penalty rates apply",
            "signal_hour": now.hour,
            "fetched_at": now.isoformat(),
        }
        signals.append(payroll_signal)

    return signals


def _demand_classification(predicted_covers: float, venue_avg: float = 45) -> str:
    """Classify demand level based on covers."""
    if predicted_covers < venue_avg * 0.4:
        return "quiet"
    elif predicted_covers < venue_avg * 0.7:
        return "normal"
    elif predicted_covers < venue_avg * 1.2:
        return "busy"
    elif predicted_covers < venue_avg * 1.5:
        return "very_busy"
    else:
        return "extreme"


def _get_today_costs(roster: Optional[Roster], employees_lookup: dict) -> float:
    """Calculate total labour cost for today's active shifts."""
    if not roster:
        return 0.0
    today = date.today()
    today_shifts = [s for s in roster.shifts if s.date == today]
    total = 0.0
    for shift in today_shifts:
        emp = employees_lookup.get(shift.employee_id)
        if emp:
            bd = calculate_shift_cost_breakdown(emp, shift, State.vic)
            total += float(bd.get("total_cost", 0))
    return total


@app.get("/dashboard/{venue_id}/overview")
async def dashboard_overview(venue_id: str):
    """
    GET /dashboard/{venue_id}/overview

    Returns the full dashboard overview for a venue: venue info, demand outlook,
    active signals, roster summary, week-ahead forecast, top signals, and costs.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    today = date.today()

    # Today's signals
    demo_signals = _generate_demo_signals(venue_id, today)

    # Demand outlook for today
    day_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == today
    ]
    if day_forecasts:
        avg_covers = sum(f.predicted_covers for f in day_forecasts) / len(day_forecasts)
    else:
        avg_covers = 45  # fallback

    # Week ahead (next 7 days)
    week_forecast = []
    for i in range(7):
        d = today + timedelta(days=i)
        d_forecasts = [
            f for f in _store["forecasts"]
            if f.venue_id == venue_id and f.date == d
        ]
        if d_forecasts:
            pred_covers = sum(f.predicted_covers for f in d_forecasts) / len(d_forecasts)
        else:
            pred_covers = avg_covers  # No forecast data; use today's average as default
        week_forecast.append({
            "date": d.isoformat(),
            "demand_level": _demand_classification(pred_covers, avg_covers),
            "predicted_covers": round(pred_covers, 1),
            "confidence": "low" if not d_forecasts else "normal",
            "data_source": "default_estimate" if not d_forecasts else "forecast",
        })

    # Roster summary for today
    today_roster = None
    for r in _store["rosters"].values():
        if r.venue_id == venue_id:
            today_roster = r
            break

    today_shifts = []
    if today_roster:
        today_shifts = [s for s in today_roster.shifts if s.date == today]

    staff_cost_today = _get_today_costs(today_roster, _store["employees"])

    # Demand outlook summary
    demand_level = _demand_classification(avg_covers, avg_covers)
    # Top 3 signals by strength
    top_signals = sorted(
        demo_signals,
        key=lambda s: STRENGTH_MULTIPLIERS.get(s["strength"], 1.0),
        reverse=True
    )[:3]

    return {
        "venue": {
            "id": venue.id,
            "name": venue.name,
            "state": venue.state.value,
            "timezone": "Australia/Melbourne",
        },
        "demand_outlook_today": {
            "classification": demand_level,
            "bullish_percentage": None,
            "trend": None,
            "sentiment_available": False,
        },
        "active_signals": {
            "total": len(demo_signals),
            "by_category": defaultdict(int),
        },
        "roster_summary_today": {
            "shifts_scheduled": len(today_shifts),
            "staff_on_duty": len(set(s.employee_id for s in today_shifts if s.status in [ShiftStatus.in_progress, ShiftStatus.confirmed])),
            "total_scheduled": len(set(s.employee_id for s in today_shifts)),
            "labour_cost": round(staff_cost_today, 2),
        },
        "week_ahead_forecast": week_forecast,
        "top_impactful_signals": top_signals,
        "labour_cost_vs_budget": {
            "today_cost": round(staff_cost_today, 2),
            "budget": round(float(venue.budget) if venue.budget else 1000, 2),
            "remaining": round(float(venue.budget) - staff_cost_today if venue.budget else 1000 - staff_cost_today, 2),
        },
    }


@app.get("/dashboard/{venue_id}/signals")
async def dashboard_signals(
    venue_id: str,
    start_date: date = Query(None),
    end_date: Optional[date] = Query(None),
):
    """
    GET /dashboard/{venue_id}/signals?start_date=2026-04-04&end_date=2026-04-05

    Returns all active signals for the venue grouped by category.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    if start_date is None:
        start_date = date.today()
    if end_date is None:
        end_date = start_date

    # Collect signals across the date range
    all_signals = []
    for d in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
        all_signals.extend(_generate_demo_signals(venue_id, d))

    # Group by category
    by_category = defaultdict(list)
    for sig in all_signals:
        by_category[sig["category"]].append(sig)

    # Summary stats
    return {
        "venue_id": venue_id,
        "period": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "summary": {
            "total_signals": len(all_signals),
            "dominant_category": max(by_category.keys(), key=lambda k: len(by_category[k])) if by_category else None,
            "overall_variance": 0.0,  # Unknown — no historical variance data available
        },
        "signals_by_category": dict(by_category),
    }


@app.get("/dashboard/{venue_id}/demand-outlook")
async def dashboard_demand_outlook(
    venue_id: str,
    target_date: date = Query(None),
):
    """
    GET /dashboard/{venue_id}/demand-outlook?target_date=2026-04-04

    Returns hour-by-hour demand prediction for the day.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    if target_date is None:
        target_date = date.today()

    # Get forecasts for this day
    day_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == target_date
    ]

    # Build hour-by-hour array
    hourly = []
    for hour in range(24):
        hour_forecasts = [f for f in day_forecasts if f.hour == hour]
        if hour_forecasts:
            pred_covers = sum(f.predicted_covers for f in hour_forecasts) / len(hour_forecasts)
            confidence = sum(f.confidence for f in hour_forecasts) / len(hour_forecasts)
        else:
            # No forecast data for this hour — use deterministic default
            pred_covers = 30.0  # Static default estimate
            confidence = 0.0  # No real forecast confidence

        recommended_staff = max(1, int(pred_covers / DEFAULT_COVERS_PER_STAFF))

        has_forecast = len(hour_forecasts) > 0
        hourly.append({
            "hour": hour,
            "predicted_covers": round(pred_covers, 1),
            "confidence": round(confidence, 2) if has_forecast else "low",
            "data_source": "forecast" if has_forecast else "default_estimate",
            "signals_contributing": len(hour_forecasts) if has_forecast else 0,
            "recommended_staff": recommended_staff,
        })

    # Day classification
    avg_covers = sum(h["predicted_covers"] for h in hourly) / 24
    day_classification = _demand_classification(avg_covers)

    # Comparison to last week
    last_week = target_date - timedelta(days=7)
    last_week_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == last_week
    ]
    if last_week_forecasts:
        last_week_avg = sum(f.predicted_covers for f in last_week_forecasts) / len(last_week_forecasts)
        week_on_week_change = round((avg_covers - last_week_avg) / last_week_avg * 100, 1) if last_week_avg > 0 else 0
    else:
        last_week_avg = None
        week_on_week_change = None

    # Top 3 signals
    demo_signals = _generate_demo_signals(venue_id, target_date)
    top_signals = sorted(
        demo_signals,
        key=lambda s: STRENGTH_MULTIPLIERS.get(s["strength"], 1.0),
        reverse=True
    )[:3]

    return {
        "date": target_date.isoformat(),
        "hourly": hourly,
        "day_classification": day_classification,
        "average_covers_predicted": round(avg_covers, 1),
        "comparison_to_last_week": {
            "last_week_date": last_week.isoformat(),
            "last_week_average_covers": round(last_week_avg, 1) if last_week_avg else None,
            "week_on_week_change_percent": week_on_week_change,
        },
        "key_factors": top_signals,
    }


@app.get("/dashboard/{venue_id}/live-pulse")
async def dashboard_live_pulse(venue_id: str):
    """
    GET /dashboard/{venue_id}/live-pulse

    Real-time endpoint — designed to be polled every 60 seconds.
    Returns current hour metrics, variance, threshold breaches, and recommendations.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    now = datetime.now()
    today = now.date()
    current_hour = now.hour

    # Current hour forecast
    hour_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == today and f.hour == current_hour
    ]
    has_forecast = len(hour_forecasts) > 0
    if has_forecast:
        predicted_covers = sum(f.predicted_covers for f in hour_forecasts) / len(hour_forecasts)
    else:
        predicted_covers = 30.0  # Static default when no forecast available

    # Actual covers must come from real POS/reservation data — never fake
    actual_covers = None
    actual_available = False

    # Current roster
    today_roster = None
    for r in _store["rosters"].values():
        if r.venue_id == venue_id:
            today_roster = r
            break

    on_duty = 0
    if today_roster:
        for shift in today_roster.shifts:
            if shift.date == today and shift.status in [ShiftStatus.in_progress, ShiftStatus.confirmed]:
                # Handle midnight crossing: if start_time > end_time, shift spans midnight
                if shift.start_time.hour <= shift.end_time.hour:
                    # Normal shift (no midnight crossing)
                    if shift.start_time.hour <= current_hour < shift.end_time.hour:
                        on_duty += 1
                else:
                    # Shift crosses midnight: active if current_hour >= start OR current_hour < end
                    if current_hour >= shift.start_time.hour or current_hour < shift.end_time.hour:
                        on_duty += 1

    recommended_staff = max(1, int(predicted_covers / DEFAULT_COVERS_PER_STAFF))

    # Variance and breach — only computable with real actual data
    if actual_available and actual_covers is not None:
        variance = abs(actual_covers - predicted_covers) / max(predicted_covers, 1) if predicted_covers > 0 else 0
        breach = variance > 0.15
    else:
        variance = None
        breach = False

    # Next hour prediction
    next_hour = (current_hour + 1) % 24
    next_hour_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == today and f.hour == next_hour
    ]
    has_next_forecast = len(next_hour_forecasts) > 0
    if has_next_forecast:
        next_predicted = sum(f.predicted_covers for f in next_hour_forecasts) / len(next_hour_forecasts)
    else:
        next_predicted = predicted_covers  # Carry forward current prediction as best estimate

    # Alerts — only generate from real data
    alerts = []
    if actual_available and breach:
        if actual_covers < recommended_staff * DEFAULT_COVERS_PER_STAFF * 0.5:
            alerts.append("understaffed_warning")
        elif actual_covers > recommended_staff * DEFAULT_COVERS_PER_STAFF * 1.2:
            alerts.append("overstaffed_warning")

    return {
        "timestamp": now.isoformat(),
        "current_hour": current_hour,
        "current_metrics": {
            "actual_covers": round(actual_covers, 1) if actual_available else None,
            "actual_available": actual_available,
            "predicted_covers": round(predicted_covers, 1),
            "data_source": "forecast" if has_forecast else "default_estimate",
            "variance_score": round(variance, 3) if variance is not None else None,
            "on_duty_staff": on_duty,
            "recommended_staff": recommended_staff,
        },
        "threshold_breach": breach,
        "threshold_breach_reason": ("understaffed" if actual_available and breach and actual_covers > predicted_covers else ("overstaffed" if breach else None)) if actual_available else None,
        "active_recommendations": [
            {
                "type": "call_in" if on_duty < recommended_staff else "send_home",
                "description": f"{'Call in' if on_duty < recommended_staff else 'Send'} {abs(on_duty - recommended_staff)} staff",
                "confidence": 0.85,
            }
        ] if breach else [],
        "next_hour_prediction": {
            "hour": next_hour,
            "predicted_covers": round(next_predicted, 1),
            "data_source": "forecast" if has_next_forecast else "carry_forward",
            "recommended_staff": max(1, int(next_predicted / DEFAULT_COVERS_PER_STAFF)),
        },
        "active_alerts": alerts,
    }


@app.get("/dashboard/{venue_id}/week-ahead")
async def dashboard_week_ahead(venue_id: str):
    """
    GET /dashboard/{venue_id}/week-ahead

    Returns 7-day forward view with demand predictions and staffing recommendations.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    today = date.today()
    week_data = []

    for i in range(7):
        d = today + timedelta(days=i)
        d_forecasts = [
            f for f in _store["forecasts"]
            if f.venue_id == venue_id and f.date == d
        ]

        has_day_forecast = len(d_forecasts) > 0
        if has_day_forecast:
            pred_covers = sum(f.predicted_covers for f in d_forecasts) / len(d_forecasts)
        else:
            pred_covers = 30.0  # Static default estimate when no forecast data

        demand_level = _demand_classification(pred_covers)
        day_type = get_day_type(d, State.vic)
        # Peak covers: use actual forecast max if available, otherwise estimate at 1.2x average
        if has_day_forecast:
            peak_covers = max(f.predicted_covers for f in d_forecasts)
        else:
            peak_covers = pred_covers * 1.2  # Deterministic 20% above average estimate

        # Estimate labour cost
        recommended_shifts = max(2, int(pred_covers / DEFAULT_COVERS_PER_STAFF / 4))  # 4-hour shifts avg
        estimated_cost = recommended_shifts * 25 * 4  # rough estimate

        # Top signals for the day
        demo_signals = _generate_demo_signals(venue_id, d)
        top_signals = sorted(
            demo_signals,
            key=lambda s: STRENGTH_MULTIPLIERS.get(s["strength"], 1.0),
            reverse=True
        )[:3]

        # Flag special days
        special = None
        if str(day_type).lower() in ["public_holiday", "weekend"]:
            special = f"weekend" if "weekend" in str(day_type).lower() else "public_holiday"

        week_data.append({
            "date": d.isoformat(),
            "day_type": str(day_type),
            "demand_level": demand_level,
            "predicted_peak_covers": round(peak_covers, 1),
            "predicted_average_covers": round(pred_covers, 1),
            "confidence": "normal" if has_day_forecast else "low",
            "data_source": "forecast" if has_day_forecast else "default_estimate",
            "key_signals": top_signals,
            "recommended_total_shifts": recommended_shifts,
            "estimated_labour_cost": round(estimated_cost, 2),
            "special_flag": special,
        })

    # Week totals
    week_cost = sum(d["estimated_labour_cost"] for d in week_data)
    # Last week cost comparison — requires real historical data
    # Check if we have last week's roster with actual cost
    last_week_start = today - timedelta(days=7)
    last_week_roster = None
    for r in _store["rosters"].values():
        if r.venue_id == venue_id and r.week_start == last_week_start:
            last_week_roster = r
            break

    if last_week_roster and last_week_roster.total_cost:
        last_week_cost = float(last_week_roster.total_cost)
        comparison_available = True
        week_on_week_change = round((week_cost - last_week_cost) / last_week_cost * 100, 1) if last_week_cost > 0 else 0
    else:
        last_week_cost = None
        comparison_available = False
        week_on_week_change = None

    return {
        "week_start": today.isoformat(),
        "week_end": (today + timedelta(days=6)).isoformat(),
        "forecast_days": week_data,
        "week_summary": {
            "total_estimated_labour_cost": round(week_cost, 2),
            "last_week_cost": round(last_week_cost, 2) if last_week_cost is not None else None,
            "comparison_available": comparison_available,
            "week_on_week_change_percent": week_on_week_change,
        },
    }


@app.get("/dashboard/{venue_id}/signals/history")
async def dashboard_signals_history(
    venue_id: str,
    days: int = Query(30, ge=1, le=365),
    category: Optional[str] = Query(None),
):
    """
    GET /dashboard/{venue_id}/signals/history?days=30&category=weather

    Returns historical signal data for charts and trend analysis.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    today = date.today()
    start_date = today - timedelta(days=days)

    daily_aggregates = {}
    for i in range(days):
        d = start_date + timedelta(days=i)
        signals = _generate_demo_signals(venue_id, d)

        if category:
            signals = [s for s in signals if s["category"] == category]

        by_cat = defaultdict(list)
        for sig in signals:
            by_cat[sig["category"]].append(sig)

        daily_aggregates[d.isoformat()] = dict(by_cat)

    return {
        "venue_id": venue_id,
        "period_days": days,
        "filter_category": category,
        "daily_aggregates": daily_aggregates,
        "accuracy_tracking": {
            "predicted_vs_actual": "Not yet available — real POS data integration in progress",
        },
    }


@app.post("/dashboard/{venue_id}/configure-feeds")
async def configure_feeds(venue_id: str, config: DataFeedConfig):
    """
    POST /dashboard/{venue_id}/configure-feeds

    Saves which data feeds are enabled for this venue and their API keys.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    # In production, store encrypted API keys in a separate feeds_config table
    # For now, just acknowledge
    enabled_feeds = [k for k, v in config.feeds_enabled.items() if v]

    return {
        "venue_id": venue_id,
        "status": "configured",
        "enabled_feeds": enabled_feeds,
        "message": "Feed configuration saved. Real data will progressively replace demo data as feeds activate.",
    }


# ============================================================================
# Notification endpoints
# ============================================================================

from rosteriq.services.notifications import get_notification_service


class NotificationTestRequest(BaseModel):
    """Request to send a test notification."""

    email: str
    notification_type: str = "test"  # test, digest, roster_published, etc


@app.post("/api/notifications/test")
async def send_test_notification(req: NotificationTestRequest):
    """
    POST /api/notifications/test

    Send a test email to verify SMTP configuration.
    """
    service = get_notification_service()

    html = service._wrap_template(
        title="RosterIQ Test Email",
        date_str=date.today().isoformat(),
        body="""
        <h2 style="color: #3366FF; margin-bottom: 20px;">Test Email</h2>
        <p style="color: #666; margin: 20px 0;">
            This is a test email from RosterIQ. If you received this, your SMTP configuration is working correctly.
        </p>

        <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; text-align: center; margin: 20px 0;">
            <div style="font-weight: bold; color: #2e7d32; margin-bottom: 10px;">Configuration Status</div>
            <p style="margin: 5px 0;">SMTP Host: {service.smtp_host}:{service.smtp_port}</p>
            <p style="margin: 5px 0;">From: {service.from_email}</p>
        </div>
        """,
    )

    success = await service.send_email(req.email, "RosterIQ Test Email", html)
    return {
        "status": "sent" if success else "failed",
        "email": req.email,
        "type": req.notification_type,
    }


@app.post("/api/notifications/digest/{venue_id}")
async def trigger_daily_digest(venue_id: str, manager_email: str = Query(...)):
    """
    POST /api/notifications/digest/{venue_id}?manager_email=manager@example.com

    Trigger a daily digest email for the specified venue.
    """
    venue = _store["venues"].get(venue_id)
    if not venue:
        raise HTTPException(404, f"Venue {venue_id} not found")

    service = get_notification_service()

    # Get today's forecast
    today = date.today()
    day_forecasts = [
        f for f in _store["forecasts"]
        if f.venue_id == venue_id and f.date == today
    ]

    expected_covers = {}
    for hour in range(24):
        hour_forecasts = [f for f in day_forecasts if f.hour == hour]
        if hour_forecasts:
            expected_covers[hour] = sum(f.predicted_covers for f in hour_forecasts) / len(
                hour_forecasts
            )
        else:
            expected_covers[hour] = 0

    # Get today's roster
    today_roster = None
    for r in _store["rosters"].values():
        if r.venue_id == venue_id:
            today_roster = r
            break

    today_shifts = []
    if today_roster:
        today_shifts = [s for s in today_roster.shifts if s.date == today]

    success = await service.send_daily_digest(
        venue_id=venue_id,
        venue=venue,
        manager_email=manager_email,
        roster_shifts=today_shifts,
        expected_covers=expected_covers,
        weather_forecast="Sunny, 24°C",
        events=["Local market day", "AFL Round 10"],
    )

    return {
        "status": "sent" if success else "failed",
        "venue_id": venue_id,
        "manager_email": manager_email,
        "shifts_included": len(today_shifts),
    }


# Employee onboarding checklist routes (with document tracking)
try:
    from rosteriq.routes.onboarding_checklist import router as onboarding_checklist_router
    app.include_router(onboarding_checklist_router)
    logger.info("Employee onboarding checklist routes registered at /api/v1/employees/{id}/onboarding and /api/v1/venues/{id}/onboarding-*")
except ImportError:
    logger.warning("Employee onboarding checklist routes unavailable")
except Exception as e:
    logger.error(f"Failed to register employee onboarding checklist routes: {e}")


# Shift bidding marketplace routes
try:
    from rosteriq.routes.bidding import router as bidding_router
    app.include_router(bidding_router)
    logger.info("Shift bidding marketplace routes registered")
except ImportError:
    logger.warning("Shift bidding marketplace routes unavailable")
except Exception as e:
    logger.error(f"Failed to register shift bidding routes: {e}")

# Roster publishing and lifecycle management routes
try:
    from rosteriq.routes.publishing import router as publishing_router
    app.include_router(publishing_router)
    logger.info("Roster publishing routes registered")
except ImportError:
    logger.warning("Roster publishing routes unavailable")
except Exception as e:
    logger.error(f"Failed to register roster publishing routes: {e}")

# Staff fatigue prediction and burnout risk assessment routes
try:
    from rosteriq.routes.fatigue import router as fatigue_router
    app.include_router(fatigue_router)
    logger.info("Fatigue prediction routes registered")
except ImportError:
    logger.warning("Fatigue prediction routes unavailable")
except Exception as e:
    logger.error(f"Failed to register fatigue prediction routes: {e}")

# No-show risk prediction and mitigation routes
try:
    from rosteriq.routes.noshow import router as noshow_router
    app.include_router(noshow_router)
    logger.info("No-show prediction routes registered")
except ImportError:
    logger.warning("No-show prediction routes unavailable")
except Exception as e:
    logger.error(f"Failed to register no-show prediction routes: {e}")

# Monitoring and metrics routes (Prometheus, JSON metrics, alerts, health)
if create_monitoring_router:
    try:
        from rosteriq.services.monitoring import MetricsCollector
        monitoring_router = create_monitoring_router(db_pool=_pool)
        app.include_router(monitoring_router)

        # Load monitoring config if available
        collector = MetricsCollector()
        import os
        config_path = os.path.join(os.path.dirname(__file__), "monitoring_config.json")
        if os.path.exists(config_path):
            collector.load_config(config_path)

        logger.info("Monitoring routes registered (Prometheus metrics, alerts, health checks)")
    except Exception as e:
        logger.error(f"Failed to register monitoring routes: {e}")
else:
    logger.warning("Monitoring routes unavailable")

# Roster cost simulator routes (what-if scenario modelling)
try:
    from rosteriq.routes.simulator import router as simulator_router
    app.include_router(simulator_router)
    logger.info("Roster cost simulator routes registered")
except ImportError:
    logger.warning("Roster cost simulator routes unavailable")
except Exception as e:
    logger.error(f"Failed to register simulator routes: {e}")

# Message template routes (staff communication templates)
try:
    from rosteriq.routes.message_templates import router as message_templates_router
    app.include_router(message_templates_router)
    logger.info("Message template routes registered")
except ImportError:
    logger.warning("Message template routes unavailable")
except Exception as e:
    logger.error(f"Failed to register message template routes: {e}")

# Roster changelog routes (immutable audit trail)
try:
    from rosteriq.routes.changelog import router as changelog_router
    app.include_router(changelog_router)
    logger.info("Roster changelog routes registered")
except ImportError:
    logger.warning("Roster changelog routes unavailable")
except Exception as e:
    logger.error(f"Failed to register changelog routes: {e}")


# ============================================================================
# Run directly
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("rosteriq.api:app", host="0.0.0.0", port=8000, reload=True)
