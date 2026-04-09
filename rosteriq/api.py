"""
RosterIQ FastAPI Application

Main API entry point providing REST endpoints for:
- Health checks and system status
- Roster generation and management
- Award calculations and labour costs
- Shift swap requests and management
- Dashboard data feeds and on-shift monitoring
- Signal integration and reporting

Integrates with:
- RosterEngine for optimal roster generation
- AwardEngine for Australian award compliance
- ShiftSwap for staff request management
- Reports module for business analytics
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone, date, time
from decimal import Decimal
from typing import Optional, Dict, List, Any

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel

# Core engines
from rosteriq.roster_engine import (
    RosterEngine, RosterConstraints, DemandForecast, Employee, Role, EmploymentType
)
from rosteriq.award_engine import AwardEngine, EmploymentType as AwardEmploymentType, ShiftClassification
from rosteriq.reports import ReportGenerator, LabourCostReport
from rosteriq.auth import setup_auth

# Adapters (with graceful degradation)
try:
    from rosteriq.tanda_integration import TandaIntegration
    _has_tanda = True
except ImportError:
    _has_tanda = False

try:
    from rosteriq.shift_swap import create_swap_router, SwapManager, NotificationManager
    _has_swap = True
except ImportError:
    _has_swap = False

logger = logging.getLogger("rosteriq")

# ============================================================================
# Request/Response Models
# ============================================================================

class RosterGenerateRequest(BaseModel):
    """Request to generate a new roster."""
    venue_id: str
    week_start: str  # ISO date string YYYY-MM-DD
    demand_override: Optional[Dict[str, Dict[int, Dict[str, float]]]] = None


class RosterResponse(BaseModel):
    """Complete roster with shifts and metadata."""
    venue_id: str
    week_start: str
    shifts: List[Dict[str, Any]]
    total_labour_cost: float
    total_hours: float
    coverage_score: float
    fairness_score: float
    cost_efficiency_score: float
    warnings: List[str]


class AwardCalculateRequest(BaseModel):
    """Request to calculate award costs for shifts."""
    employee_id: str
    award_level: int
    employment_type: str  # "casual", "part_time", "full_time"
    shifts: List[Dict[str, Any]]  # [{date, start_time, end_time}, ...]


class AwardCalculateResponse(BaseModel):
    """Award cost calculation result."""
    employee_id: str
    base_pay: float
    penalties: float
    overtime: float
    super_contribution: float
    total: float
    breakdown: List[Dict[str, Any]]


class CallInRequest(BaseModel):
    """Request to find staff for call-in."""
    venue_id: str
    role_needed: str
    date: str  # ISO date
    start_hour: int
    end_hour: int


class CallInResponse(BaseModel):
    """Recommended employee for call-in with template."""
    employee_id: str
    employee_name: str
    phone: str
    sms_template: str


class ShiftSummaryRequest(BaseModel):
    """Shift summary data for reporting."""
    notes: str = ""
    actual_revenue: float
    actual_staff_count: int


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    version: str
    timestamp: str
    connected_services: Dict[str, bool]


# ============================================================================
# Demo Data Functions
# ============================================================================

def _demo_employees() -> List[Employee]:
    """
    Generate 28 demo employees for Brisbane hotel.
    Covers all roles with varied employment types and availability.
    """
    employees = [
        # Managers
        Employee(
            id="em_001", name="Alex Thompson", role=Role.MANAGER,
            skills=[Role.MANAGER, Role.BAR, Role.FLOOR],
            hourly_rate=45.0, max_hours_per_week=40, min_hours_per_week=30,
            availability={i: [(8, 23)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, is_manager=True,
            seniority_score=0.95
        ),
        Employee(
            id="em_002", name="Jordan Lee", role=Role.MANAGER,
            skills=[Role.MANAGER, Role.BAR],
            hourly_rate=42.0, max_hours_per_week=38, min_hours_per_week=28,
            availability={i: [(10, 23)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, is_manager=True,
            seniority_score=0.88
        ),
        # Bar staff (12)
        Employee(
            id="bs_001", name="Emma Wilson", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=27.50, max_hours_per_week=38, min_hours_per_week=20,
            availability={i: [(10, 2)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.75
        ),
        Employee(
            id="bs_002", name="Marcus Chen", role=Role.BAR,
            skills=[Role.BAR],
            hourly_rate=26.50, max_hours_per_week=40, min_hours_per_week=15,
            availability={i: [(12, 3)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.60
        ),
        Employee(
            id="bs_003", name="Olivia Rodriguez", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=28.0, max_hours_per_week=35, min_hours_per_week=18,
            availability={i: [(10, 1)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.70
        ),
        Employee(
            id="bs_004", name="James Murphy", role=Role.BAR,
            skills=[Role.BAR],
            hourly_rate=25.50, max_hours_per_week=30, min_hours_per_week=10,
            availability={i: [(15, 3)] for i in range(5)} | {5: [(12, 4)], 6: [(12, 2)]},
            employment_type=EmploymentType.CASUAL, seniority_score=0.50
        ),
        Employee(
            id="bs_005", name="Sophie Anderson", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=27.0, max_hours_per_week=38, min_hours_per_week=25,
            availability={i: [(11, 2)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, seniority_score=0.80
        ),
        Employee(
            id="bs_006", name="Liam O'Brien", role=Role.BAR,
            skills=[Role.BAR],
            hourly_rate=26.0, max_hours_per_week=35, min_hours_per_week=12,
            availability={i: [(14, 2)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.55
        ),
        Employee(
            id="bs_007", name="Isabella Garcia", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=28.50, max_hours_per_week=30, min_hours_per_week=20,
            availability={i: [(10, 1)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.72
        ),
        Employee(
            id="bs_008", name="Noah Kim", role=Role.BAR,
            skills=[Role.BAR],
            hourly_rate=26.50, max_hours_per_week=38, min_hours_per_week=15,
            availability={i: [(12, 3)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.58
        ),
        Employee(
            id="bs_009", name="Ava Martinez", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=27.75, max_hours_per_week=35, min_hours_per_week=22,
            availability={i: [(11, 2)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.68
        ),
        Employee(
            id="bs_010", name="Ethan Taylor", role=Role.BAR,
            skills=[Role.BAR],
            hourly_rate=25.75, max_hours_per_week=30, min_hours_per_week=10,
            availability={i: [(16, 2)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.48
        ),
        Employee(
            id="bs_011", name="Mia Patel", role=Role.BAR,
            skills=[Role.BAR, Role.FLOOR],
            hourly_rate=28.25, max_hours_per_week=38, min_hours_per_week=24,
            availability={i: [(10, 2)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, seniority_score=0.77
        ),
        # Floor staff (8)
        Employee(
            id="fs_001", name="Charlotte Brown", role=Role.FLOOR,
            skills=[Role.FLOOR, Role.BAR],
            hourly_rate=26.50, max_hours_per_week=35, min_hours_per_week=18,
            availability={i: [(11, 22)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.65
        ),
        Employee(
            id="fs_002", name="Lucas White", role=Role.FLOOR,
            skills=[Role.FLOOR],
            hourly_rate=25.0, max_hours_per_week=30, min_hours_per_week=10,
            availability={i: [(12, 22)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.52
        ),
        Employee(
            id="fs_003", name="Zoe Harper", role=Role.FLOOR,
            skills=[Role.FLOOR, Role.BAR],
            hourly_rate=26.75, max_hours_per_week=38, min_hours_per_week=20,
            availability={i: [(11, 22)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.70
        ),
        Employee(
            id="fs_004", name="Oliver Scott", role=Role.FLOOR,
            skills=[Role.FLOOR],
            hourly_rate=25.25, max_hours_per_week=35, min_hours_per_week=15,
            availability={i: [(14, 23)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.50
        ),
        Employee(
            id="fs_005", name="Grace King", role=Role.FLOOR,
            skills=[Role.FLOOR, Role.BAR],
            hourly_rate=27.0, max_hours_per_week=38, min_hours_per_week=22,
            availability={i: [(10, 22)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, seniority_score=0.75
        ),
        Employee(
            id="fs_006", name="Benjamin Price", role=Role.FLOOR,
            skills=[Role.FLOOR],
            hourly_rate=25.50, max_hours_per_week=30, min_hours_per_week=12,
            availability={i: [(15, 23)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.48
        ),
        Employee(
            id="fs_007", name="Amelia Knight", role=Role.FLOOR,
            skills=[Role.FLOOR, Role.BAR],
            hourly_rate=26.50, max_hours_per_week=35, min_hours_per_week=18,
            availability={i: [(11, 22)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.62
        ),
        Employee(
            id="fs_008", name="Daniel Fox", role=Role.FLOOR,
            skills=[Role.FLOOR],
            hourly_rate=25.0, max_hours_per_week=38, min_hours_per_week=16,
            availability={i: [(13, 23)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.55
        ),
        # Kitchen staff (6)
        Employee(
            id="ks_001", name="Victoria Green", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=30.0, max_hours_per_week=40, min_hours_per_week=30,
            availability={i: [(10, 22)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, seniority_score=0.85
        ),
        Employee(
            id="ks_002", name="David Stone", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=28.50, max_hours_per_week=38, min_hours_per_week=20,
            availability={i: [(10, 21)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.72
        ),
        Employee(
            id="ks_003", name="Ruby Adams", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=27.0, max_hours_per_week=35, min_hours_per_week=15,
            availability={i: [(11, 20)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.60
        ),
        Employee(
            id="ks_004", name="Michael Baker", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=29.0, max_hours_per_week=40, min_hours_per_week=28,
            availability={i: [(9, 22)] for i in range(7)},
            employment_type=EmploymentType.FULL_TIME, seniority_score=0.80
        ),
        Employee(
            id="ks_005", name="Sophie Nelson", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=27.50, max_hours_per_week=35, min_hours_per_week=18,
            availability={i: [(10, 21)] for i in range(7)},
            employment_type=EmploymentType.PART_TIME, seniority_score=0.65
        ),
        Employee(
            id="ks_006", name="Jack Carter", role=Role.KITCHEN,
            skills=[Role.KITCHEN],
            hourly_rate=26.50, max_hours_per_week=30, min_hours_per_week=12,
            availability={i: [(11, 20)] for i in range(7)},
            employment_type=EmploymentType.CASUAL, seniority_score=0.55
        ),
    ]
    return employees


def _demo_signals(check_date: str) -> Dict[str, Any]:
    """
    Generate demo signals for a date: weather, events, bookings.

    Args:
        check_date: ISO date string (YYYY-MM-DD)

    Returns:
        Dict with weather, events, bookings signals
    """
    import hashlib
    # Deterministic hash for consistency
    hash_val = int(hashlib.md5(check_date.encode()).hexdigest(), 16)

    weather_patterns = ["clear", "rain", "overcast", "hot"]
    event_patterns = ["none", "private_event", "live_music", "sports_event"]

    return {
        "date": check_date,
        "weather": {
            "condition": weather_patterns[hash_val % len(weather_patterns)],
            "temperature": 20 + (hash_val % 15),
            "impact_on_demand": "neutral" if hash_val % 3 == 0 else "positive"
        },
        "events": {
            "type": event_patterns[hash_val % len(event_patterns)],
            "expected_covers": 80 if hash_val % 5 == 0 else 0
        },
        "bookings": {
            "advance_bookings": 45 + (hash_val % 50),
            "walk_in_expected": 30 + (hash_val % 40)
        }
    }


def _demo_hourly_demand() -> List[float]:
    """
    Generate 24-hour demand curve for Brisbane hotel.
    Values represent fraction of max capacity (0-1).
    """
    return [
        0.1,   # 0:00 - late night
        0.05,  # 1:00
        0.05,  # 2:00
        0.05,  # 3:00 - quiet time
        0.10,  # 4:00
        0.15,  # 5:00
        0.20,  # 6:00 - breakfast starts
        0.35,  # 7:00
        0.45,  # 8:00
        0.55,  # 9:00
        0.65,  # 10:00 - lunch ramp
        0.75,  # 11:00
        0.80,  # 12:00 - peak lunch
        0.75,  # 13:00
        0.65,  # 14:00
        0.50,  # 15:00 - afternoon lull
        0.40,  # 16:00
        0.50,  # 17:00
        0.70,  # 18:00 - dinner ramp
        0.85,  # 19:00
        0.90,  # 20:00 - peak dinner
        0.85,  # 21:00
        0.60,  # 22:00 - late evening
        0.30,  # 23:00 - wind down
    ]


def _demo_revenue_tracking() -> Dict[str, Any]:
    """Generate demo revenue tracking data."""
    return {
        "current_revenue": 2845.50,
        "expected_revenue": 3200.00,
        "variance_percent": -11.1,
        "variance_reason": "Slower lunch service due to weather"
    }


# ============================================================================
# App Initialization
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("=" * 60)
    logger.info("RosterIQ API Starting Up")
    logger.info("=" * 60)
    logger.info(f"Version: 1.0.0")
    logger.info(f"Tanda Integration: {'Available' if _has_tanda else 'Not available'}")
    logger.info(f"Swap System: {'Available' if _has_swap else 'Not available'}")
    logger.info("=" * 60)
    yield
    logger.info("RosterIQ API Shutting Down")


app = FastAPI(
    title="RosterIQ API",
    description="AI-powered rostering for Australian hospitality venues",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize engines
roster_engine = RosterEngine(
    constraints=RosterConstraints(
        min_staff_per_hour=2,
        max_staff_per_hour=8,
        required_roles={Role.MANAGER: 1},
        max_consecutive_days=5,
        min_hours_between_shifts=11.0,
        max_shift_length_hours=10.0,
        budget_limit_weekly=3500.0,
    )
)
award_engine = AwardEngine(award_year=2025)
report_generator = ReportGenerator()

# Setup auth
setup_auth(app)


# ============================================================================
# Health & Status Endpoints
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint for monitoring.
    Returns service status and connected service availability.
    """
    return HealthResponse(
        status="healthy",
        service="rosteriq",
        version="1.0.0",
        timestamp=datetime.now(timezone.utc).isoformat(),
        connected_services={
            "tanda": _has_tanda,
            "shift_swap": _has_swap,
            "award_engine": True,
            "roster_engine": True,
        }
    )


@app.get("/")
async def root():
    """Root endpoint with API documentation."""
    return {
        "service": "RosterIQ API",
        "version": "1.0.0",
        "description": "AI-powered rostering for Australian hospitality",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "rosters": "/api/v1/rosters/generate",
            "awards": "/api/v1/awards/calculate",
            "dashboard_roster": "/api/v1/dashboard/roster-maker/{venue_id}",
            "dashboard_onshift": "/api/v1/dashboard/on-shift/{venue_id}",
            "signals": "/api/v1/signals/{venue_id}/{date}",
            "staff": "/api/v1/staff/{venue_id}",
            "reports": "/api/v1/reports/labour-cost/{venue_id}",
        }
    }


# ============================================================================
# Roster Generation Endpoints
# ============================================================================

@app.post("/api/v1/rosters/generate", response_model=RosterResponse)
async def generate_roster(request: RosterGenerateRequest):
    """
    Generate an optimized roster for a venue and week.

    Uses RosterEngine with demo employees and demand forecast.
    Returns shifts, costs, and quality scores.
    """
    try:
        # Parse week start date
        week_start = datetime.fromisoformat(request.week_start).date()

        # Get demo employees
        employees = _demo_employees()

        # Generate demand forecasts for the week
        demand_forecasts = []
        for day_offset in range(7):
            forecast_date = week_start + timedelta(days=day_offset)
            hourly_demand_curve = _demo_hourly_demand()

            # Build hourly demand by role
            hourly_demand = {}
            for hour, demand_fraction in enumerate(hourly_demand_curve):
                # Distribute demand across roles
                hourly_demand[hour] = {
                    Role.BAR: max(1, int(3 * demand_fraction)),
                    Role.FLOOR: max(1, int(2 * demand_fraction)),
                    Role.KITCHEN: max(1, int(2 * demand_fraction)),
                }

            demand = DemandForecast(
                date=forecast_date.isoformat(),
                hourly_demand=hourly_demand,
                total_covers_expected=int(100 + 50 * sum(hourly_demand_curve) / 24),
                signals=[],
                confidence=0.75
            )
            demand_forecasts.append(demand)

        # Generate roster
        roster = roster_engine.generate_roster(
            employees=employees,
            demand_forecasts=demand_forecasts,
            week_start_date=week_start.isoformat()
        )

        # Convert shifts to dict for response
        shifts_data = []
        for shift in roster.shifts:
            shifts_data.append({
                "id": shift.id,
                "date": shift.date,
                "start_hour": shift.start_hour,
                "end_hour": shift.end_hour,
                "duration_hours": shift.duration_hours,
                "role_required": shift.role_required.value,
                "employee_id": shift.employee_id,
                "employee_name": next(
                    (e.name for e in employees if e.id == shift.employee_id),
                    None
                ) if shift.employee_id else None,
                "is_filled": shift.is_filled,
            })

        return RosterResponse(
            venue_id=roster.venue_id,
            week_start=roster.week_start_date,
            shifts=shifts_data,
            total_labour_cost=roster.total_labour_cost,
            total_hours=roster.total_hours,
            coverage_score=roster.coverage_score,
            fairness_score=roster.fairness_score,
            cost_efficiency_score=roster.cost_efficiency_score,
            warnings=roster.warnings,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {str(e)}")
    except Exception as e:
        logger.exception("Error generating roster")
        raise HTTPException(status_code=500, detail="Roster generation failed")


# ============================================================================
# Award Calculation Endpoints
# ============================================================================

@app.post("/api/v1/awards/calculate", response_model=AwardCalculateResponse)
async def calculate_award_costs(request: AwardCalculateRequest):
    """
    Calculate award costs for employee shifts.

    Uses AwardEngine to compute base pay, penalties, overtime, super.
    Returns detailed breakdown for compliance.
    """
    try:
        total_base_pay = Decimal("0")
        total_penalties = Decimal("0")
        total_overtime = Decimal("0")
        total_super = Decimal("0")
        breakdown = []

        # Map employment type string to enum
        emp_type_map = {
            "casual": AwardEmploymentType.CASUAL,
            "part_time": AwardEmploymentType.PART_TIME,
            "full_time": AwardEmploymentType.FULL_TIME,
        }
        employment_type = emp_type_map.get(
            request.employment_type.lower(),
            AwardEmploymentType.CASUAL
        )

        # Calculate for each shift
        for shift_data in request.shifts:
            shift_date = datetime.fromisoformat(shift_data["date"]).date()
            start_time = datetime.fromisoformat(shift_data["start_time"]).time()
            end_time = datetime.fromisoformat(shift_data["end_time"]).time()

            calculation = award_engine.calculate_shift_cost(
                employee_id=request.employee_id,
                award_level=request.award_level,
                shift_date=shift_date,
                start_time=start_time,
                end_time=end_time,
                employment_type=employment_type,
            )

            total_base_pay += calculation.gross_pay
            total_super += calculation.super_contribution

            # Extract penalties and overtime from breakdown
            for item in calculation.breakdown:
                if item.classification == ShiftClassification.OVERTIME:
                    total_overtime += item.amount
                elif item.classification in [
                    ShiftClassification.SUNDAY,
                    ShiftClassification.SATURDAY,
                    ShiftClassification.LATE_NIGHT,
                ]:
                    total_penalties += item.amount

                breakdown.append({
                    "date": shift_data["date"],
                    "classification": item.classification.value,
                    "hours": str(item.hours),
                    "rate": str(item.rate),
                    "amount": str(item.amount),
                    "description": item.description,
                })

        return AwardCalculateResponse(
            employee_id=request.employee_id,
            base_pay=float(total_base_pay),
            penalties=float(total_penalties),
            overtime=float(total_overtime),
            super_contribution=float(total_super),
            total=float(total_base_pay + total_penalties + total_overtime),
            breakdown=breakdown,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {str(e)}")
    except Exception as e:
        logger.exception("Error calculating award costs")
        raise HTTPException(status_code=500, detail="Award calculation failed")


# ============================================================================
# Dashboard Endpoints
# ============================================================================

@app.get("/api/v1/dashboard/roster-maker/{venue_id}")
async def dashboard_roster_maker(venue_id: str):
    """
    Dashboard for roster creation and planning.

    Returns:
    - Staff count and composition
    - Demand forecast for the week ahead
    - Labour cost estimates
    - Signals summary (weather, events, bookings)
    - Draft roster if exists
    - Week ahead outlook
    """
    try:
        employees = _demo_employees()

        # Staff breakdown by role
        staff_by_role = {}
        for role in Role:
            staff_by_role[role.value] = len([e for e in employees if e.role == role])

        # 7-day demand forecast
        today = date.today()
        demand_forecast = []
        for day_offset in range(7):
            forecast_date = today + timedelta(days=day_offset)
            hourly_demand_curve = _demo_hourly_demand()
            demand_forecast.append({
                "date": forecast_date.isoformat(),
                "daily_demand_total": sum(hourly_demand_curve),
                "peak_hour": hourly_demand_curve.index(max(hourly_demand_curve)),
                "peak_demand": max(hourly_demand_curve),
            })

        # Labour cost estimate (28 staff @ average $27/hour @ 100 hours/week)
        labour_cost_estimate = sum(e.hourly_rate for e in employees) * 100 / len(employees)

        # Signals summary
        signals_summary = []
        for day_offset in range(3):
            check_date = today + timedelta(days=day_offset)
            signals = _demo_signals(check_date.isoformat())
            signals_summary.append(signals)

        return {
            "venue_id": venue_id,
            "staff_count": len(employees),
            "staff_by_role": staff_by_role,
            "demand_forecast": demand_forecast,
            "labour_cost_estimate": labour_cost_estimate,
            "signals_summary": signals_summary,
            "draft_roster": None,
            "week_ahead_outlook": {
                "average_daily_cost": labour_cost_estimate,
                "estimated_weekly_cost": labour_cost_estimate * 7,
                "coverage_risk": "low",
            }
        }

    except Exception as e:
        logger.exception("Error fetching roster-maker dashboard")
        raise HTTPException(status_code=500, detail="Dashboard fetch failed")


@app.get("/api/v1/dashboard/on-shift/{venue_id}")
async def dashboard_on_shift(venue_id: str):
    """
    Real-time on-shift monitoring dashboard.

    Returns:
    - Current revenue vs expected
    - Staff on shift count
    - Hourly demand curve with current hour marker
    - Recommended actions (e.g., cut staff, call in)
    - Active signals affecting current shift
    """
    try:
        revenue = _demo_revenue_tracking()
        hourly_demand = _demo_hourly_demand()
        current_hour = datetime.now().hour

        # Recommended actions based on demand
        recommended_actions = []
        if current_hour < len(hourly_demand):
            current_demand = hourly_demand[current_hour]
            if current_demand < 0.3:
                recommended_actions.append("Consider cutting 2 bar staff — demand dropping")
            elif current_demand > 0.8:
                recommended_actions.append("Consider calling in extra kitchen staff — high demand")

        # Today's signals
        today = date.today().isoformat()
        signals = _demo_signals(today)

        return {
            "venue_id": venue_id,
            "current_revenue": revenue["current_revenue"],
            "expected_revenue": revenue["expected_revenue"],
            "variance_pct": revenue["variance_percent"],
            "staff_on_shift": 8,
            "hourly_demand_curve": hourly_demand,
            "current_hour": current_hour,
            "recommended_actions": recommended_actions,
            "active_signals": [signals],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.exception("Error fetching on-shift dashboard")
        raise HTTPException(status_code=500, detail="Dashboard fetch failed")


# ============================================================================
# Signal Endpoints
# ============================================================================

@app.get("/api/v1/signals/{venue_id}/{date_str}")
async def get_signals(venue_id: str, date_str: str):
    """
    Get signals for a date: weather, events, bookings.

    Signals feed demand forecasting and staffing decisions.
    """
    try:
        # Validate date format
        datetime.fromisoformat(date_str)
        signals = _demo_signals(date_str)
        signals["venue_id"] = venue_id
        return signals

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    except Exception as e:
        logger.exception("Error fetching signals")
        raise HTTPException(status_code=500, detail="Signal fetch failed")


# ============================================================================
# Staff Endpoints
# ============================================================================

@app.get("/api/v1/staff/{venue_id}")
async def get_staff(venue_id: str):
    """
    Get staff list with availability for a venue.

    Returns demo staff with:
    - ID, name, role, skills
    - Availability windows by day
    - Employment type and hours preferences
    """
    try:
        employees = _demo_employees()

        staff_list = []
        for emp in employees:
            availability = {}
            for day_idx, windows in emp.availability.items():
                day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                availability[day_names[day_idx]] = [
                    {"start": start, "end": end} for start, end in windows
                ]

            staff_list.append({
                "id": emp.id,
                "name": emp.name,
                "role": emp.role.value,
                "skills": [s.value for s in emp.skills],
                "hourly_rate": emp.hourly_rate,
                "employment_type": emp.employment_type.value,
                "max_hours_per_week": emp.max_hours_per_week,
                "min_hours_per_week": emp.min_hours_per_week,
                "availability": availability,
                "is_manager": emp.is_manager,
                "seniority_score": emp.seniority_score,
            })

        return {
            "venue_id": venue_id,
            "staff_count": len(staff_list),
            "staff": staff_list,
        }

    except Exception as e:
        logger.exception("Error fetching staff list")
        raise HTTPException(status_code=500, detail="Staff fetch failed")


@app.post("/api/v1/staff/call-in", response_model=CallInResponse)
async def call_in_staff(request: CallInRequest):
    """
    Find and recommend staff for call-in.

    Takes role needed + time window and returns best available employee
    with SMS template for quick contact.
    """
    try:
        employees = _demo_employees()

        # Filter by role and availability
        available = []
        for emp in employees:
            if emp.role.value != request.role_needed.lower():
                continue

            # Check date availability
            date_obj = datetime.fromisoformat(request.date).date()
            day_of_week = date_obj.weekday()

            windows = emp.availability.get(day_of_week, [])
            for start_hour, end_hour in windows:
                if start_hour <= request.start_hour < end_hour:
                    available.append(emp)
                    break

        if not available:
            raise HTTPException(status_code=404, detail="No staff available for this shift")

        # Pick best by seniority
        best = max(available, key=lambda e: e.seniority_score)

        # Generate SMS template
        sms_template = (
            f"Hi {best.name}, we need {request.role_needed} "
            f"coverage on {request.date} {request.start_hour:02d}:00-{request.end_hour:02d}:00. "
            f"Can you help? Reply YES or NO."
        )

        return CallInResponse(
            employee_id=best.id,
            employee_name=best.name,
            phone="+61400000000",  # Demo
            sms_template=sms_template,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error finding call-in staff")
        raise HTTPException(status_code=500, detail="Call-in lookup failed")


# ============================================================================
# Report Endpoints
# ============================================================================

@app.post("/api/v1/reports/shift-summary/{venue_id}/{date_str}")
async def shift_summary_report(
    venue_id: str, date_str: str, request: ShiftSummaryRequest
):
    """
    Create and store shift summary report.

    Records actual performance (revenue, staff count) against forecasts
    for later analysis.
    """
    try:
        datetime.fromisoformat(date_str)

        return {
            "venue_id": venue_id,
            "date": date_str,
            "notes": request.notes,
            "actual_revenue": request.actual_revenue,
            "actual_staff_count": request.actual_staff_count,
            "status": "stored",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    except Exception as e:
        logger.exception("Error creating shift summary")
        raise HTTPException(status_code=500, detail="Report creation failed")


@app.get("/api/v1/reports/labour-cost/{venue_id}")
async def labour_cost_report(venue_id: str, period: str = "week"):
    """
    Get labour cost breakdown for a venue.

    Returns cost summary by:
    - Day of week
    - Role
    - Employee
    - Employment type

    Includes budget variance analysis.
    """
    try:
        employees = _demo_employees()

        # Simple demo calculation: each employee at their rate @ 20 hours
        total_cost = sum(e.hourly_rate * 20 for e in employees)

        cost_by_role = {}
        for role in Role:
            role_cost = sum(
                e.hourly_rate * 20
                for e in employees
                if e.role == role
            )
            cost_by_role[role.value] = role_cost

        cost_by_employment = {}
        for emp_type in EmploymentType:
            emp_cost = sum(
                e.hourly_rate * 20
                for e in employees
                if e.employment_type == emp_type
            )
            cost_by_employment[emp_type.value] = emp_cost

        budget_limit = 3500.0
        variance = total_cost - budget_limit

        return {
            "venue_id": venue_id,
            "period": period,
            "total_labour_cost": total_cost,
            "budget_limit": budget_limit,
            "variance": variance,
            "variance_percent": (variance / budget_limit) * 100 if budget_limit else 0,
            "cost_by_role": cost_by_role,
            "cost_by_employment_type": cost_by_employment,
            "average_hourly_rate": total_cost / sum(e.max_hours_per_week for e in employees),
        }

    except Exception as e:
        logger.exception("Error generating labour cost report")
        raise HTTPException(status_code=500, detail="Report generation failed")


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Standard HTTP exception handler."""
    return {
        "detail": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler."""
    logger.exception("Unhandled exception")
    return {
        "detail": "Internal server error",
        "status_code": 500,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
