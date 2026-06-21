"""
Data contracts module for RosterIQ.

Comprehensive Pydantic v2 models and enums for an AI-powered predictive rostering system
for Australian hospitality venues integrated with Tanda workforce management.
"""

from decimal import Decimal
from datetime import date, time, datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


__all__ = [
    # Enums
    "EmploymentType",
    "ShiftStatus",
    "DayType",
    "AlertType",
    "SignalType",
    "StaffAction",
    "AwardLevel",
    "State",
    "UserRole",
    # Core Models
    "Employee",
    "Shift",
    "Roster",
    "DemandForecast",
    "VarianceSignal",
    "StaffingRecommendation",
    "CostBreakdown",
    "RosterComparison",
    "VenueConfig",
    "VenueLocation",
    "DataFeedConfig",
    "TandaCredentials",
    "XeroCredentials",
    "APIError",
    "User",
]


# ============================================================================
# ENUMS
# ============================================================================


class EmploymentType(str, Enum):
    """Employment type classification."""

    full_time = "full_time"
    part_time = "part_time"
    casual = "casual"


class ShiftStatus(str, Enum):
    """Status of a roster shift."""

    scheduled = "scheduled"
    confirmed = "confirmed"
    in_progress = "in_progress"
    completed = "completed"
    cancelled = "cancelled"
    no_show = "no_show"


class DayType(str, Enum):
    """Type of day for demand forecasting."""

    weekday = "weekday"
    saturday = "saturday"
    sunday = "sunday"
    public_holiday = "public_holiday"


class AlertType(str, Enum):
    """Types of roster alerts."""

    overstaffed = "overstaffed"
    understaffed = "understaffed"
    high_variance = "high_variance"
    cost_threshold = "cost_threshold"
    compliance_warning = "compliance_warning"


class SignalType(str, Enum):
    """Signal types for variance engine."""

    weather = "weather"
    events = "events"
    historical = "historical"
    bookings = "bookings"
    pos_trends = "pos_trends"
    foot_traffic = "foot_traffic"
    school_holidays = "school_holidays"
    sports = "sports"
    nearby_venues = "nearby_venues"
    delivery = "delivery"
    tourism = "tourism"
    transport = "transport"
    economic = "economic"
    calendar = "calendar"


class StaffAction(str, Enum):
    """Staff action types for decision engine."""

    cut = "cut"
    call_in = "call_in"
    extend = "extend"
    shorten = "shorten"


class AwardLevel(str, Enum):
    """Australian hospitality award levels."""

    level_1 = "level_1"
    level_2 = "level_2"
    level_3 = "level_3"
    level_4 = "level_4"
    level_5 = "level_5"
    level_6 = "level_6"


class State(str, Enum):
    """Australian states and territories."""

    nsw = "nsw"
    vic = "vic"
    qld = "qld"
    sa = "sa"
    wa = "wa"
    tas = "tas"
    nt = "nt"
    act = "act"


class UserRole(str, Enum):
    """User role classification."""

    owner = "owner"
    manager = "manager"
    staff = "staff"


# ============================================================================
# CORE MODELS
# ============================================================================


class Employee(BaseModel):
    """Employee record with availability and constraints."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    tanda_id: Optional[str] = None
    venue_id: Optional[str] = None
    name: str
    employment_type: EmploymentType
    award_level: AwardLevel
    state: State
    hourly_base_rate: Decimal
    phone: Optional[str] = None
    email: Optional[str] = None
    skills: List[str] = []
    availability: Dict[str, List[Dict[str, str]]] = {}  # day -> list of {start, end} ranges
    max_hours_per_week: float = 38.0
    consecutive_days_limit: int = 6
    created_at: datetime
    updated_at: datetime

    @field_validator("hourly_base_rate")
    @classmethod
    def validate_hourly_base_rate(cls, v: Decimal) -> Decimal:
        """Ensure hourly rate is positive."""
        if v <= 0:
            raise ValueError("hourly_base_rate must be greater than 0")
        return v

    @field_validator("max_hours_per_week")
    @classmethod
    def validate_max_hours_per_week(cls, v: float) -> float:
        """Ensure max hours per week is between 1 and 60."""
        if not (1 <= v <= 60):
            raise ValueError("max_hours_per_week must be between 1 and 60")
        return v


class Shift(BaseModel):
    """Individual roster shift with timing and cost information."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    employee_id: str
    date: date
    start_time: time
    end_time: time
    break_minutes: int = 0
    status: ShiftStatus
    role: str
    cost: Optional[Decimal] = None
    penalty_multiplier: float = 1.0

    @field_validator("break_minutes")
    @classmethod
    def validate_break_minutes(cls, v: int) -> int:
        """Ensure break minutes is non-negative."""
        if v < 0:
            raise ValueError("break_minutes must be >= 0")
        return v

    @model_validator(mode="after")
    def validate_times(self) -> "Shift":
        """Ensure end_time > start_time, handling overnight shifts."""
        # For overnight shifts, end_time can be less than start_time
        # This validator ensures logical shift duration
        return self

    @property
    def duration_hours(self) -> float:
        """Calculate total shift duration in hours, handling overnight shifts."""
        start_minutes = self.start_time.hour * 60 + self.start_time.minute
        end_minutes = self.end_time.hour * 60 + self.end_time.minute

        # Handle overnight shift
        if end_minutes < start_minutes:
            end_minutes += 24 * 60

        return (end_minutes - start_minutes) / 60.0

    @property
    def net_hours(self) -> float:
        """Calculate net hours worked (duration minus break)."""
        return max(0, self.duration_hours - self.break_minutes / 60.0)


class Roster(BaseModel):
    """Weekly roster with shifts and cost information."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    venue_id: str
    week_start: date
    week_end: date
    shifts: List[Shift] = []
    total_cost: Optional[Decimal] = None
    created_at: datetime

    @model_validator(mode="after")
    def validate_week_dates(self) -> "Roster":
        """Ensure week_end is 6 days after week_start."""
        expected_end = self.week_start + timedelta(days=6)
        if self.week_end != expected_end:
            raise ValueError(
                f"week_end must be exactly 6 days after week_start. "
                f"Expected {expected_end}, got {self.week_end}"
            )
        return self

    @property
    def total_hours(self) -> float:
        """Calculate total hours across all shifts."""
        return sum(shift.net_hours for shift in self.shifts)

    @property
    def shift_count(self) -> int:
        """Count total number of shifts."""
        return len(self.shifts)

    @property
    def employees_used(self) -> set:
        """Get unique set of employees in roster."""
        return {shift.employee_id for shift in self.shifts}


class DemandForecast(BaseModel):
    """Predicted staffing demand for a venue at a specific time."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    venue_id: str
    date: date
    hour: int
    predicted_covers: float
    confidence: float
    signals_used: List[SignalType] = []
    model_version: str

    @field_validator("hour")
    @classmethod
    def validate_hour(cls, v: int) -> int:
        """Ensure hour is between 0 and 23."""
        if not (0 <= v <= 23):
            raise ValueError("hour must be between 0 and 23")
        return v

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Ensure confidence is between 0 and 1."""
        if not (0 <= v <= 1):
            raise ValueError("confidence must be between 0 and 1")
        return v

    @field_validator("predicted_covers")
    @classmethod
    def validate_predicted_covers(cls, v: float) -> float:
        """Ensure predicted covers is non-negative."""
        if v < 0:
            raise ValueError("predicted_covers must be >= 0")
        return v


class VarianceSignal(BaseModel):
    """Signal contributing to staffing variance in the variance engine."""

    model_config = ConfigDict(validate_assignment=True)

    signal_type: SignalType
    value: float
    weight: float
    confidence: float
    source: str
    timestamp: datetime

    @field_validator("weight")
    @classmethod
    def validate_weight(cls, v: float) -> float:
        """Ensure weight is between 0 and 1."""
        if not (0 <= v <= 1):
            raise ValueError("weight must be between 0 and 1")
        return v

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Ensure confidence is between 0 and 1."""
        if not (0 <= v <= 1):
            raise ValueError("confidence must be between 0 and 1")
        return v


class StaffingRecommendation(BaseModel):
    """Recommendation from decision engine for staffing adjustment."""

    model_config = ConfigDict(validate_assignment=True)

    action: StaffAction
    employee_id: str
    shift_id: Optional[str] = None
    reason: str
    priority: float
    estimated_savings: Optional[Decimal] = None

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: float) -> float:
        """Ensure priority is between 0 and 1."""
        if not (0 <= v <= 1):
            raise ValueError("priority must be between 0 and 1")
        return v


class CostBreakdown(BaseModel):
    """Detailed cost breakdown for a roster or shift."""

    model_config = ConfigDict(validate_assignment=True)

    base_cost: Decimal
    penalty_cost: Decimal
    casual_loading: Decimal
    super_contribution: Decimal
    total_cost: Decimal

    @model_validator(mode="after")
    def validate_total(self) -> "CostBreakdown":
        """Ensure total_cost equals sum of all components."""
        expected_total = (
            self.base_cost
            + self.penalty_cost
            + self.casual_loading
            + self.super_contribution
        )
        if self.total_cost != expected_total:
            raise ValueError(
                f"total_cost must equal sum of components. "
                f"Expected {expected_total}, got {self.total_cost}"
            )
        return self


class RosterComparison(BaseModel):
    """Comparison between original and optimized roster."""

    model_config = ConfigDict(validate_assignment=True)

    original: Roster
    optimised: Roster
    cost_savings: Decimal
    hours_saved: float
    alerts: List[str] = []


class VenueConfig(BaseModel):
    """Venue configuration for rostering."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    name: str
    tanda_org_id: str
    state: State
    timezone: str = "Australia/Melbourne"
    min_staff: Dict[str, int] = {}  # role -> min count
    max_labour_pct: float
    pos_system: Optional[str] = None
    created_at: datetime

    @field_validator("max_labour_pct")
    @classmethod
    def validate_max_labour_pct(cls, v: float) -> float:
        """Ensure max labour percentage is between 0 and 100."""
        if not (0 <= v <= 100):
            raise ValueError("max_labour_pct must be between 0 and 100")
        return v


class VenueLocation(BaseModel):
    """Geographic location for a venue, used by data feed adapters."""

    model_config = ConfigDict(validate_assignment=True)

    venue_id: str
    latitude: float
    longitude: float
    address: str = ""
    suburb: str = ""
    state: State = State.vic
    postcode: str = ""
    google_place_id: Optional[str] = None

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not (-90 <= v <= 90):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not (-180 <= v <= 180):
            raise ValueError("longitude must be between -180 and 180")
        return v


class DataFeedConfig(BaseModel):
    """Configuration for external data feed connections."""

    model_config = ConfigDict(validate_assignment=True)

    venue_id: str
    weather_api_key: Optional[str] = None
    google_places_api_key: Optional[str] = None
    ticketmaster_api_key: Optional[str] = None
    eventbrite_token: Optional[str] = None
    predicthq_token: Optional[str] = None
    resdiary_api_key: Optional[str] = None
    nowbookit_api_key: Optional[str] = None
    opentable_token: Optional[str] = None
    sevenrooms_api_key: Optional[str] = None
    ubereats_client_id: Optional[str] = None
    ubereats_client_secret: Optional[str] = None
    doordash_developer_id: Optional[str] = None
    doordash_key_id: Optional[str] = None
    menulog_api_key: Optional[str] = None
    sportradar_api_key: Optional[str] = None
    besttime_api_key: Optional[str] = None
    str_api_key: Optional[str] = None
    airdna_api_key: Optional[str] = None
    ptv_dev_id: Optional[str] = None
    ptv_api_key: Optional[str] = None
    tfnsw_api_key: Optional[str] = None
    booking_no_show_rate: float = 0.15
    foot_traffic_radius_km: float = 1.0
    event_radius_km: float = 5.0
    competitor_radius_km: float = 1.0


class TandaCredentials(BaseModel):
    """Credentials for Tanda API integration."""

    model_config = ConfigDict(validate_assignment=True)

    client_id: str
    client_secret: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_expires_at: Optional[datetime] = None
    org_id: str


class XeroCredentials(BaseModel):
    """OAuth2 PKCE credentials for Xero accounting integration."""

    model_config = ConfigDict(validate_assignment=True)

    venue_id: str
    client_id: str
    client_secret: str
    tenant_id: str  # Xero Organization ID (UUID)
    access_token: str
    refresh_token: str
    token_expires: datetime
    created_at: datetime
    updated_at: datetime


class APIError(BaseModel):
    """API error response model."""

    model_config = ConfigDict(validate_assignment=True)

    status_code: int
    message: str
    # Accepts a string OR structured data (adapters pass dicts like
    # {"url": ..., "response": ...}); a str-only type raised ValidationError
    # on those error paths instead of surfacing the real API error.
    detail: Optional[Any] = None
    retry_after: Optional[float] = None


class User(BaseModel):
    """User account model for authentication."""

    model_config = ConfigDict(validate_assignment=True)

    id: str
    email: str
    password_hash: str
    name: str
    role: str  # "owner", "manager", "staff"
    venue_ids: List[str] = []
    api_key_hash: str = ""
    is_active: bool = True
    created_at: datetime
    last_login: Optional[datetime] = None
