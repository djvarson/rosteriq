"""
Strawberry GraphQL type definitions for RosterIQ.

Maps Pydantic models to GraphQL types with field resolution.
"""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional, List
import strawberry

from rosteriq.models import (
    Employee, Shift, Roster, VenueConfig, DemandForecast,
    EmploymentType, ShiftStatus, AwardLevel, State, AlertType, SignalType,
)


# ============================================================================
# Scalar Type Mappings
# ============================================================================

# Decimal is mapped to Float in GraphQL
# date, datetime, time are mapped to String in GraphQL
# Enums are automatically mapped


# ============================================================================
# Enum Types
# ============================================================================

@strawberry.enum
class EmploymentTypeEnum(str):
    """Employment type classification."""
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CASUAL = "casual"


@strawberry.enum
class ShiftStatusEnum(str):
    """Status of a roster shift."""
    SCHEDULED = "scheduled"
    CONFIRMED = "confirmed"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    NO_SHOW = "no_show"


@strawberry.enum
class AwardLevelEnum(str):
    """Australian hospitality award levels."""
    LEVEL_1 = "level_1"
    LEVEL_2 = "level_2"
    LEVEL_3 = "level_3"
    LEVEL_4 = "level_4"
    LEVEL_5 = "level_5"
    LEVEL_6 = "level_6"


@strawberry.enum
class StateEnum(str):
    """Australian states and territories."""
    NSW = "nsw"
    VIC = "vic"
    QLD = "qld"
    SA = "sa"
    WA = "wa"
    TAS = "tas"
    NT = "nt"
    ACT = "act"


@strawberry.enum
class AlertTypeEnum(str):
    """Types of roster alerts."""
    OVERSTAFFED = "overstaffed"
    UNDERSTAFFED = "understaffed"
    HIGH_VARIANCE = "high_variance"
    COST_THRESHOLD = "cost_threshold"
    COMPLIANCE_WARNING = "compliance_warning"


@strawberry.enum
class SignalTypeEnum(str):
    """Signal types for variance engine."""
    WEATHER = "weather"
    EVENTS = "events"
    HISTORICAL = "historical"
    BOOKINGS = "bookings"
    POS_TRENDS = "pos_trends"
    FOOT_TRAFFIC = "foot_traffic"
    SCHOOL_HOLIDAYS = "school_holidays"
    SPORTS = "sports"
    NEARBY_VENUES = "nearby_venues"
    DELIVERY = "delivery"
    TOURISM = "tourism"
    TRANSPORT = "transport"
    ECONOMIC = "economic"
    CALENDAR = "calendar"


# ============================================================================
# Object Types
# ============================================================================

@strawberry.type
class VenueType:
    """Venue configuration type."""
    id: str
    name: str
    tanda_org_id: str
    state: StateEnum
    timezone: str
    min_staff: strawberry.field(description="Min staff by role")
    max_labour_pct: float
    pos_system: Optional[str]
    created_at: str  # ISO format datetime


@strawberry.type
class EmployeeType:
    """Employee type with skills and availability."""
    id: str
    tanda_id: Optional[str]
    name: str
    employment_type: EmploymentTypeEnum
    award_level: AwardLevelEnum
    state: StateEnum
    hourly_base_rate: float
    phone: Optional[str]
    email: Optional[str]
    skills: List[str]
    max_hours_per_week: float
    consecutive_days_limit: int
    created_at: str
    updated_at: str


@strawberry.type
class ShiftType:
    """Individual shift type with employee resolution."""
    id: str
    employee_id: str
    date: str  # ISO format date
    start_time: str  # HH:MM format
    end_time: str  # HH:MM format
    break_minutes: int
    status: ShiftStatusEnum
    role: str
    cost: Optional[float]
    penalty_multiplier: float
    duration_hours: float = strawberry.field(
        description="Total shift duration in hours"
    )
    net_hours: float = strawberry.field(
        description="Net hours worked (duration - break)"
    )


@strawberry.type
class RosterType:
    """Weekly roster type with shifts and costs."""
    id: str
    venue_id: str
    week_start: str  # ISO format date
    week_end: str  # ISO format date
    shifts: List[ShiftType]
    total_cost: Optional[float]
    created_at: str
    total_hours: float = strawberry.field(
        description="Total hours across all shifts"
    )
    shift_count: int = strawberry.field(
        description="Total number of shifts"
    )
    employees_used: List[str] = strawberry.field(
        description="Unique employee IDs in roster"
    )


@strawberry.type
class ForecastType:
    """Demand forecast type."""
    id: str
    venue_id: str
    date: str  # ISO format date
    hour: int
    predicted_covers: float
    confidence: float
    signals_used: List[SignalTypeEnum]
    model_version: str


@strawberry.type
class AlertType:
    """Alert type for roster notifications."""
    id: str
    venue_id: str
    alert_type: AlertTypeEnum
    severity: str  # "low", "medium", "high"
    message: str
    created_at: str
    resolved: bool


@strawberry.type
class CostBreakdownType:
    """Cost breakdown type for detailed costing."""
    base_cost: float
    penalty_cost: float
    casual_loading: float
    super_contribution: float
    total_cost: float


@strawberry.type
class AnalyticsSummaryType:
    """Analytics summary for a venue."""
    venue_id: str
    labour_percentage: float
    forecast_accuracy: float
    headcount: int
    revenue_estimate: Optional[float]
    period_start: str
    period_end: str


@strawberry.type
class SwapResultType:
    """Result of a shift swap operation."""
    success: bool
    shift_id: str
    old_employee_id: str
    new_employee_id: str
    message: str


@strawberry.type
class AvailabilityType:
    """Employee availability type."""
    employee_id: str
    day: str  # e.g. "monday", "tuesday"
    blocks: List[str]  # e.g. ["09:00-17:00", "18:00-22:00"]
    updated_at: str


@strawberry.type
class RevenueUpdateType:
    """Real-time revenue update from POS."""
    venue_id: str
    timestamp: str
    revenue: float
    transaction_count: int
    average_transaction_value: float


@strawberry.type
class OptimisationProgressType:
    """Progress update during roster optimisation."""
    roster_id: str
    percentage_complete: int
    current_phase: str
    message: str
    timestamp: str


@strawberry.type
class RosterConflictType:
    """A conflict detected in a roster."""
    conflict_type: str  # e.g. "overlapping_shifts", "excessive_hours", "rest_breach"
    severity: str  # e.g. "low", "medium", "high"
    description: str
    affected_employee_ids: List[str]


@strawberry.type
class ConflictEventType:
    """Real-time conflict detection event."""
    roster_id: str
    conflict_type: str
    severity: str
    message: str
    employee_ids: List[str]
    timestamp: str


@strawberry.type
class RosterStateEventType:
    """Roster state change event."""
    roster_id: str
    venue_id: str
    old_state: str
    new_state: str
    actor_id: str
    timestamp: str


@strawberry.type
class BidEventType:
    """Shift bid activity event."""
    bid_id: str
    shift_id: str
    employee_id: str
    employee_name: str
    action: str  # placed, withdrawn, accepted, rejected
    timestamp: str


@strawberry.type
class ForecastEventType:
    """Demand forecast update event."""
    venue_id: str
    date: str
    hour: int
    predicted_covers: float
    confidence: float
    model_version: str
    timestamp: str
