"""
RosterIQ Data Models - Pydantic v2

Comprehensive data models for all entities in the RosterIQ system:
- Core entities: Venue, Employee, Shift, Roster, etc.
- Request/response models for API endpoints
- Enums for type safety
- Dashboard data structures

All models use strict validation and complete type hints.
"""

from __future__ import annotations

from datetime import datetime, date, time
from enum import Enum
from typing import Optional, List, Dict, Any
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator, ConfigDict


# ============================================================================
# Enums - Type Safety for Categorical Fields
# ============================================================================

class EmploymentType(str, Enum):
    """Employment classification for employees."""
    CASUAL = "casual"
    PART_TIME = "part_time"
    FULL_TIME = "full_time"


class ShiftStatus(str, Enum):
    """Lifecycle status of a shift."""
    DRAFT = "draft"
    PUBLISHED = "published"
    CONFIRMED = "confirmed"
    COMPLETED = "completed"


class ShiftSource(str, Enum):
    """Origin of shift creation."""
    AI_GENERATED = "ai_generated"
    MANUAL = "manual"
    TANDA_SYNC = "tanda_sync"


class RosterStatus(str, Enum):
    """Lifecycle status of a roster."""
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class SignalType(str, Enum):
    """Type of external signal affecting demand."""
    WEATHER = "weather"
    EVENT = "event"
    BOOKING = "booking"
    POS_SALES = "pos_sales"
    FOOT_TRAFFIC = "foot_traffic"
    DELIVERY = "delivery"


class ShiftEventType(str, Enum):
    """Type of shift event requiring accountability tracking."""
    STAFF_CUT = "staff_cut"
    STAFF_CALLED_IN = "staff_called_in"
    DEMAND_SPIKE = "demand_spike"
    DEMAND_DROP = "demand_drop"


class AvailabilityPreference(str, Enum):
    """Employee preference level for availability."""
    AVAILABLE = "available"
    PREFERRED = "preferred"
    UNAVAILABLE = "unavailable"


class DayOfWeek(int, Enum):
    """ISO 8601 day of week (0=Monday, 6=Sunday)."""
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class AustralianState(str, Enum):
    """Australian states and territories."""
    NSW = "NSW"
    VIC = "VIC"
    QLD = "QLD"
    WA = "WA"
    SA = "SA"
    TAS = "TAS"
    ACT = "ACT"
    NT = "NT"


# ============================================================================
# Core Entity Models
# ============================================================================

class VenueBase(BaseModel):
    """Base venue information."""
    name: str = Field(..., min_length=1, max_length=255)
    address: Optional[str] = Field(None, max_length=500)
    state: Optional[AustralianState] = None
    timezone: str = Field(default="Australia/Sydney")


class VenueCreate(VenueBase):
    """Create a new venue."""
    tanda_org_id: Optional[str] = None
    swiftpos_site_id: Optional[str] = None
    nowbookit_venue_id: Optional[str] = None


class Venue(VenueBase):
    """Complete venue entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    tanda_org_id: Optional[str] = None
    swiftpos_site_id: Optional[str] = None
    nowbookit_venue_id: Optional[str] = None
    created_at: str


class EmployeeBase(BaseModel):
    """Base employee information."""
    name: str = Field(..., min_length=1, max_length=255)
    email: Optional[str] = Field(None, pattern=r"^[^@]+@[^@]+\.[^@]+$")
    phone: Optional[str] = None
    role: Optional[str] = None
    employment_type: EmploymentType
    hourly_rate: Optional[float] = Field(None, gt=0)
    skills: Optional[List[str]] = Field(default_factory=list)
    max_hours_week: Optional[float] = Field(None, gt=0)
    is_active: bool = True


class EmployeeCreate(EmployeeBase):
    """Create a new employee."""
    venue_id: str
    tanda_id: Optional[str] = None


class Employee(EmployeeBase):
    """Complete employee entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    tanda_id: Optional[str] = None
    created_at: str


class AvailabilityBase(BaseModel):
    """Base availability entry."""
    day_of_week: DayOfWeek
    start_time: str = Field(..., pattern=r"^\d{2}:\d{2}$")  # HH:MM
    end_time: str = Field(..., pattern=r"^\d{2}:\d{2}$")    # HH:MM
    preference: AvailabilityPreference

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        """Validate HH:MM format and range."""
        parts = v.split(":")
        if len(parts) != 2:
            raise ValueError("Time must be HH:MM format")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise ValueError("Invalid time values")
        return v


class AvailabilityCreate(AvailabilityBase):
    """Create a new availability entry."""
    employee_id: str


class Availability(AvailabilityBase):
    """Complete availability entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    employee_id: str


class LeaveRecordBase(BaseModel):
    """Base leave record."""
    start_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    end_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")    # YYYY-MM-DD
    leave_type: str
    status: str


class LeaveRecordCreate(LeaveRecordBase):
    """Create a new leave record."""
    employee_id: str
    tanda_leave_id: Optional[str] = None


class LeaveRecord(LeaveRecordBase):
    """Complete leave record entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    employee_id: str
    tanda_leave_id: Optional[str] = None
    created_at: str


class ShiftBase(BaseModel):
    """Base shift information."""
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    start_time: str = Field(..., pattern=r"^\d{2}:\d{2}$")  # HH:MM
    end_time: str = Field(..., pattern=r"^\d{2}:\d{2}$")    # HH:MM
    role: Optional[str] = None
    break_minutes: int = Field(default=0, ge=0)
    status: ShiftStatus
    source: ShiftSource
    cost_estimate: Optional[float] = Field(None, ge=0)


class ShiftCreate(ShiftBase):
    """Create a new shift."""
    venue_id: str
    employee_id: Optional[str] = None
    roster_id: Optional[str] = None


class Shift(ShiftBase):
    """Complete shift entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    employee_id: Optional[str] = None
    roster_id: Optional[str] = None
    created_at: str


class RosterBase(BaseModel):
    """Base roster information."""
    week_start: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    status: RosterStatus
    total_cost: Optional[float] = Field(None, ge=0)
    coverage_score: Optional[float] = Field(None, ge=0, le=1)
    fairness_score: Optional[float] = Field(None, ge=0, le=1)


class RosterCreate(RosterBase):
    """Create a new roster."""
    venue_id: str
    created_by: Optional[str] = None


class Roster(RosterBase):
    """Complete roster entity with shifts."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    created_by: Optional[str] = None
    created_at: str


class DemandForecastBase(BaseModel):
    """Base demand forecast."""
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    hour: int = Field(..., ge=0, le=23)
    predicted_demand: float = Field(..., ge=0)
    confidence: Optional[float] = Field(None, ge=0, le=1)
    model_version: Optional[str] = None


class DemandForecastCreate(DemandForecastBase):
    """Create a new demand forecast."""
    venue_id: str


class DemandForecast(DemandForecastBase):
    """Complete demand forecast entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    created_at: str


class SignalBase(BaseModel):
    """Base external signal."""
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    signal_type: SignalType
    source: str
    data: Dict[str, Any]


class SignalCreate(SignalBase):
    """Create a new signal."""
    venue_id: str


class Signal(SignalBase):
    """Complete signal entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    fetched_at: str


class ShiftEventBase(BaseModel):
    """Base shift event for accountability."""
    shift_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    event_type: ShiftEventType
    details: Optional[Dict[str, Any]] = None
    decided_by: Optional[str] = None
    ai_recommendation: Optional[str] = None
    action_taken: Optional[str] = None


class ShiftEventCreate(ShiftEventBase):
    """Create a new shift event."""
    venue_id: str


class ShiftEvent(ShiftEventBase):
    """Complete shift event entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    created_at: str


class ShiftSummaryBase(BaseModel):
    """Base shift summary."""
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
    actual_revenue: Optional[float] = Field(None, ge=0)
    expected_revenue: Optional[float] = Field(None, ge=0)
    staff_count: Optional[int] = Field(None, ge=0)
    notes: Optional[str] = None


class ShiftSummaryCreate(ShiftSummaryBase):
    """Create a new shift summary."""
    venue_id: str
    created_by: Optional[str] = None


class ShiftSummarySubmission(ShiftSummaryBase):
    """Submit shift summary for a date."""
    venue_id: str
    created_by: Optional[str] = None


class ShiftSummary(ShiftSummaryBase):
    """Complete shift summary entity."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    venue_id: str
    created_by: Optional[str] = None
    created_at: str


class VenueSetting(BaseModel):
    """Key-value configuration for a venue."""
    model_config = ConfigDict(from_attributes=True)

    venue_id: str
    key: str
    value: Optional[str] = None


# ============================================================================
# Request/Response Models for API
# ============================================================================

class CreateRosterRequest(BaseModel):
    """Request to generate a new roster."""
    venue_id: str
    week_start: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    demand_override: Optional[Dict[str, Any]] = None
    created_by: Optional[str] = None


class RosterResponse(BaseModel):
    """Response with complete roster and shifts."""
    model_config = ConfigDict(from_attributes=True)

    roster: Roster
    shifts: List[Shift]
    employee_details: List[Employee]
    total_cost: float
    coverage_score: float
    fairness_score: float


class DashboardSummary(BaseModel):
    """High-level dashboard summary for venue managers."""
    venue_id: str
    venue_name: str
    week_start: str
    active_rosters: int
    active_employees: int
    pending_swaps: int
    labour_cost_ytd: float
    coverage_score_avg: float


class HourlyDemandPoint(BaseModel):
    """Single hour of demand forecast."""
    hour: int = Field(..., ge=0, le=23)
    predicted_staff: float
    actual_staff: Optional[int] = None
    variance: Optional[float] = None


class OnShiftDashboard(BaseModel):
    """Real-time dashboard for duty managers during shift."""
    venue_id: str
    shift_date: str
    shift_start: str
    shift_end: str
    current_revenue: float
    expected_revenue: float
    revenue_variance_pct: float
    staff_on_shift: int
    staff_roster: List[Dict[str, Any]]
    recommended_actions: List[Dict[str, Any]]
    active_signals: List[Signal]
    hourly_demand_curve: List[HourlyDemandPoint]

    @field_validator("revenue_variance_pct")
    @classmethod
    def validate_variance_percentage(cls, v: float) -> float:
        """Variance should be realistic percentage."""
        if not (-100 <= v <= 100):
            raise ValueError("Variance percentage should be between -100 and 100")
        return v


class StaffSummary(BaseModel):
    """Summary of staffing for a period."""
    total_staff: int
    casual_count: int
    part_time_count: int
    full_time_count: int
    average_hours_per_employee: float
    scheduled_hours_total: float


class RosterMakerDashboard(BaseModel):
    """Dashboard for roster makers planning schedules."""
    venue_id: str
    week_start: str
    demand_forecast: List[DemandForecast]
    staff_summary: StaffSummary
    draft_roster: Optional[Roster] = None
    shifts_draft: List[Shift] = []
    labour_cost_projection: float
    labour_cost_variance_pct: float
    signals_summary: List[Dict[str, Any]]
    week_ahead: Dict[str, Any] = Field(
        default_factory=dict,
        description="Summary of scheduling constraints and opportunities for the week"
    )
    coverage_by_hour: List[HourlyDemandPoint]
    fairness_analysis: Dict[str, Any] = Field(
        default_factory=dict,
        description="Analysis of shift distribution fairness across employees"
    )


class VenueMetrics(BaseModel):
    """Performance metrics for a venue."""
    venue_id: str
    week_start: str
    labour_cost: float
    labour_cost_as_pct_revenue: float
    coverage_score: float
    fairness_score: float
    revenue_actual: Optional[float] = None
    revenue_expected: Optional[float] = None
    staff_utilization: float


# ============================================================================
# Helper Models for Complex Types
# ============================================================================

class EmployeeAvailability(BaseModel):
    """Employee with their full availability schedule."""
    employee: Employee
    availability: List[Availability]
    leave: List[LeaveRecord]


class VenueWithEmployees(BaseModel):
    """Venue with all its employees and their data."""
    venue: Venue
    employees: List[Employee]
    settings: List[VenueSetting] = []


class WeeklyShiftSummary(BaseModel):
    """Summary of all shifts for a week."""
    week_start: str
    total_shifts: int
    total_hours: float
    total_cost: float
    shifts_by_status: Dict[ShiftStatus, int]
    average_shift_length_hours: float


class DemandCurve(BaseModel):
    """Demand forecast for a full day by hour."""
    date: str
    forecasts: List[DemandForecast]

    @property
    def peak_hour(self) -> int:
        """Return hour with highest predicted demand."""
        if not self.forecasts:
            return 0
        return max(self.forecasts, key=lambda f: f.predicted_demand).hour

    @property
    def peak_demand(self) -> float:
        """Return maximum predicted demand for the day."""
        if not self.forecasts:
            return 0.0
        return max(f.predicted_demand for f in self.forecasts)
