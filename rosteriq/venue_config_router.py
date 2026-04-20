"""FastAPI router for venue configuration endpoints (Round 39).

REST API for venue configuration management:
- GET /api/v1/config/{venue_id} (L1+) — get current config
- PUT /api/v1/config/{venue_id} (L2+) — update config
- POST /api/v1/config/{venue_id}/reset (OWNER) — reset to defaults
- GET /api/v1/config/{venue_id}/history (L2+) — config version history
- POST /api/v1/config/{venue_id}/rollback/{version_id} (L2+) — restore version
- GET /api/v1/config/{venue_id}/staffing/{day}/{hour} (L1+) — staffing requirement
- POST /api/v1/config/{venue_id}/validate (L2+) — validate config
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.venue_config import (
    DayOfWeek,
    VenueConfig,
    VenueArea,
    OperatingHours,
    RoleConfig,
    StaffingLevel,
    PenaltyOverride,
    IntegrationConfig,
    create_default_config,
    get_config,
    update_config,
    get_config_history,
    rollback_config,
    validate_config,
    get_staffing_requirement,
    is_open,
)

logger = logging.getLogger("rosteriq.venue_config_router")

# Auth gating — fall back to no-op in demo/sandbox
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic Request/Response Models
# ---------------------------------------------------------------------------


class VenueAreaRequest(BaseModel):
    """Request model for venue area."""
    area_id: str = Field(..., description="Unique area identifier")
    name: str = Field(..., description="Display name")
    capacity: int = Field(..., description="Max staff capacity")
    requires_certs: List[str] = Field(default_factory=list)


class OperatingHoursRequest(BaseModel):
    """Request model for operating hours."""
    day: int = Field(..., description="DayOfWeek value (0-6)")
    open_time: str = Field(..., description="HH:MM format")
    close_time: str = Field(..., description="HH:MM format")
    is_closed: bool = Field(default=False, description="Closed this day?")


class RoleConfigRequest(BaseModel):
    """Request model for role."""
    role_name: str = Field(..., description="Role name")
    min_staff_per_shift: int = Field(...)
    ideal_staff_per_shift: int = Field(...)
    hourly_rate_override: Optional[float] = Field(None)
    requires_certs: List[str] = Field(default_factory=list)


class StaffingLevelRequest(BaseModel):
    """Request model for staffing level."""
    day: int = Field(..., description="DayOfWeek value (0-6)")
    hour: int = Field(..., description="0-23")
    area_id: str = Field(...)
    min_staff: int = Field(...)
    ideal_staff: int = Field(...)


class PenaltyOverrideRequest(BaseModel):
    """Request model for penalty override."""
    name: str = Field(...)
    multiplier: float = Field(...)
    applies_to: str = Field(...)


class IntegrationConfigRequest(BaseModel):
    """Request model for integration."""
    provider: str = Field(..., description="tanda, deputy, humanforce")
    api_key_ref: str = Field(..., description="Reference only, not actual key")
    org_id: str = Field(...)
    enabled: bool = Field(default=True)


class VenueConfigRequest(BaseModel):
    """Request model for updating venue config."""
    venue_name: Optional[str] = Field(None)
    timezone: Optional[str] = Field(None)
    currency: Optional[str] = Field(None)
    areas: Optional[List[VenueAreaRequest]] = Field(None)
    operating_hours: Optional[List[OperatingHoursRequest]] = Field(None)
    roles: Optional[List[RoleConfigRequest]] = Field(None)
    staffing_levels: Optional[List[StaffingLevelRequest]] = Field(None)
    penalty_overrides: Optional[List[PenaltyOverrideRequest]] = Field(None)
    integrations: Optional[List[IntegrationConfigRequest]] = Field(None)
    budget_target_labour_pct: Optional[float] = Field(None)
    break_compliance_enabled: Optional[bool] = Field(None)
    fatigue_management_enabled: Optional[bool] = Field(None)
    max_shift_hours: Optional[float] = Field(None)
    min_gap_hours: Optional[float] = Field(None)


class VenueAreaResponse(BaseModel):
    """Response model for venue area."""
    area_id: str
    name: str
    capacity: int
    requires_certs: List[str]


class OperatingHoursResponse(BaseModel):
    """Response model for operating hours."""
    day: int
    open_time: str
    close_time: str
    is_closed: bool


class RoleConfigResponse(BaseModel):
    """Response model for role."""
    role_name: str
    min_staff_per_shift: int
    ideal_staff_per_shift: int
    hourly_rate_override: Optional[float]
    requires_certs: List[str]


class StaffingLevelResponse(BaseModel):
    """Response model for staffing level."""
    day: int
    hour: int
    area_id: str
    min_staff: int
    ideal_staff: int


class PenaltyOverrideResponse(BaseModel):
    """Response model for penalty override."""
    name: str
    multiplier: float
    applies_to: str


class IntegrationConfigResponse(BaseModel):
    """Response model for integration."""
    provider: str
    api_key_ref: str
    org_id: str
    enabled: bool
    last_sync: Optional[str]


class VenueConfigResponse(BaseModel):
    """Response model for venue config."""
    config_id: str
    venue_id: str
    venue_name: str
    timezone: str
    currency: str
    areas: List[VenueAreaResponse]
    operating_hours: List[OperatingHoursResponse]
    roles: List[RoleConfigResponse]
    staffing_levels: List[StaffingLevelResponse]
    penalty_overrides: List[PenaltyOverrideResponse]
    integrations: List[IntegrationConfigResponse]
    budget_target_labour_pct: float
    break_compliance_enabled: bool
    fatigue_management_enabled: bool
    max_shift_hours: float
    min_gap_hours: float
    created_at: str
    updated_at: str

    @classmethod
    def from_config(cls, config: VenueConfig) -> VenueConfigResponse:
        return cls(
            config_id=config.config_id,
            venue_id=config.venue_id,
            venue_name=config.venue_name,
            timezone=config.timezone,
            currency=config.currency,
            areas=[
                VenueAreaResponse(
                    area_id=a.area_id,
                    name=a.name,
                    capacity=a.capacity,
                    requires_certs=a.requires_certs,
                )
                for a in config.areas
            ],
            operating_hours=[
                OperatingHoursResponse(
                    day=oh.day.value,
                    open_time=oh.open_time,
                    close_time=oh.close_time,
                    is_closed=oh.is_closed,
                )
                for oh in config.operating_hours
            ],
            roles=[
                RoleConfigResponse(
                    role_name=r.role_name,
                    min_staff_per_shift=r.min_staff_per_shift,
                    ideal_staff_per_shift=r.ideal_staff_per_shift,
                    hourly_rate_override=r.hourly_rate_override,
                    requires_certs=r.requires_certs,
                )
                for r in config.roles
            ],
            staffing_levels=[
                StaffingLevelResponse(
                    day=sl.day.value,
                    hour=sl.hour,
                    area_id=sl.area_id,
                    min_staff=sl.min_staff,
                    ideal_staff=sl.ideal_staff,
                )
                for sl in config.staffing_levels
            ],
            penalty_overrides=[
                PenaltyOverrideResponse(
                    name=po.name,
                    multiplier=po.multiplier,
                    applies_to=po.applies_to,
                )
                for po in config.penalty_overrides
            ],
            integrations=[
                IntegrationConfigResponse(
                    provider=i.provider,
                    api_key_ref=i.api_key_ref,
                    org_id=i.org_id,
                    enabled=i.enabled,
                    last_sync=i.last_sync.isoformat() if i.last_sync else None,
                )
                for i in config.integrations
            ],
            budget_target_labour_pct=config.budget_target_labour_pct,
            break_compliance_enabled=config.break_compliance_enabled,
            fatigue_management_enabled=config.fatigue_management_enabled,
            max_shift_hours=config.max_shift_hours,
            min_gap_hours=config.min_gap_hours,
            created_at=config.created_at.isoformat(),
            updated_at=config.updated_at.isoformat(),
        )


class ValidationResponse(BaseModel):
    """Response model for config validation."""
    is_valid: bool
    errors: List[str]


class StaffingRequirementResponse(BaseModel):
    """Response model for staffing requirement."""
    day: str
    hour: int
    by_area: Dict[str, Dict[str, int]]


class ConfigHistoryResponse(BaseModel):
    """Response model for config history."""
    configs: List[VenueConfigResponse]


# ---------------------------------------------------------------------------
# Router Definition
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/{venue_id}", response_model=VenueConfigResponse, tags=["configuration"])
async def get_venue_config(venue_id: str, request: Request) -> VenueConfigResponse:
    """Get current venue configuration.

    Requires: L1_SUPERVISOR or higher
    """
    await _gate(request, "L1_SUPERVISOR")
    config = get_config(venue_id)
    return VenueConfigResponse.from_config(config)


@router.put("/{venue_id}", response_model=VenueConfigResponse, tags=["configuration"])
async def update_venue_config(
    venue_id: str, req: VenueConfigRequest, request: Request
) -> VenueConfigResponse:
    """Update venue configuration (partial update).

    Requires: L2_ROSTER_MAKER or higher
    """
    await _gate(request, "L2_ROSTER_MAKER")

    # Build update dict from request (only set fields)
    updates = {}
    if req.venue_name is not None:
        updates["venue_name"] = req.venue_name
    if req.timezone is not None:
        updates["timezone"] = req.timezone
    if req.currency is not None:
        updates["currency"] = req.currency
    if req.budget_target_labour_pct is not None:
        updates["budget_target_labour_pct"] = req.budget_target_labour_pct
    if req.break_compliance_enabled is not None:
        updates["break_compliance_enabled"] = req.break_compliance_enabled
    if req.fatigue_management_enabled is not None:
        updates["fatigue_management_enabled"] = req.fatigue_management_enabled
    if req.max_shift_hours is not None:
        updates["max_shift_hours"] = req.max_shift_hours
    if req.min_gap_hours is not None:
        updates["min_gap_hours"] = req.min_gap_hours

    # Handle complex nested types
    if req.areas is not None:
        updates["areas"] = [
            VenueArea(
                area_id=a.area_id,
                name=a.name,
                capacity=a.capacity,
                requires_certs=a.requires_certs,
            )
            for a in req.areas
        ]
    if req.operating_hours is not None:
        updates["operating_hours"] = [
            OperatingHours(
                day=DayOfWeek(oh.day),
                open_time=oh.open_time,
                close_time=oh.close_time,
                is_closed=oh.is_closed,
            )
            for oh in req.operating_hours
        ]
    if req.roles is not None:
        updates["roles"] = [
            RoleConfig(
                role_name=r.role_name,
                min_staff_per_shift=r.min_staff_per_shift,
                ideal_staff_per_shift=r.ideal_staff_per_shift,
                hourly_rate_override=r.hourly_rate_override,
                requires_certs=r.requires_certs,
            )
            for r in req.roles
        ]
    if req.staffing_levels is not None:
        updates["staffing_levels"] = [
            StaffingLevel(
                day=DayOfWeek(sl.day),
                hour=sl.hour,
                area_id=sl.area_id,
                min_staff=sl.min_staff,
                ideal_staff=sl.ideal_staff,
            )
            for sl in req.staffing_levels
        ]
    if req.penalty_overrides is not None:
        updates["penalty_overrides"] = [
            PenaltyOverride(
                name=po.name,
                multiplier=po.multiplier,
                applies_to=po.applies_to,
            )
            for po in req.penalty_overrides
        ]
    if req.integrations is not None:
        updates["integrations"] = [
            IntegrationConfig(
                provider=i.provider,
                api_key_ref=i.api_key_ref,
                org_id=i.org_id,
                enabled=i.enabled,
            )
            for i in req.integrations
        ]

    config = update_config(venue_id, **updates)
    return VenueConfigResponse.from_config(config)


@router.post("/{venue_id}/reset", response_model=VenueConfigResponse, tags=["configuration"])
async def reset_venue_config(venue_id: str, request: Request) -> VenueConfigResponse:
    """Reset venue configuration to defaults.

    Requires: OWNER
    """
    await _gate(request, "OWNER")
    config = create_default_config(venue_id, f"Venue {venue_id}")
    # Save it by doing an update
    current = get_config(venue_id)
    for attr in [
        "timezone",
        "currency",
        "areas",
        "operating_hours",
        "roles",
        "staffing_levels",
        "penalty_overrides",
        "integrations",
    ]:
        setattr(config, attr, getattr(config, attr))
    from rosteriq.venue_config import get_store
    store = get_store()
    store.update_config(config)
    return VenueConfigResponse.from_config(config)


@router.get(
    "/{venue_id}/history",
    response_model=ConfigHistoryResponse,
    tags=["configuration"],
)
async def get_venue_config_history(
    venue_id: str, limit: int = 5, request: Request = None
) -> ConfigHistoryResponse:
    """Get configuration version history.

    Requires: L2_ROSTER_MAKER or higher
    """
    await _gate(request, "L2_ROSTER_MAKER")
    histories = get_config_history(venue_id, limit)
    return ConfigHistoryResponse(
        configs=[VenueConfigResponse.from_config(h) for h in histories]
    )


@router.post(
    "/{venue_id}/rollback/{version_index}",
    response_model=VenueConfigResponse,
    tags=["configuration"],
)
async def rollback_venue_config(
    venue_id: str, version_index: int, request: Request
) -> VenueConfigResponse:
    """Rollback to a previous configuration version.

    version_index: 0 = current, 1 = previous, etc.

    Requires: L2_ROSTER_MAKER or higher
    """
    await _gate(request, "L2_ROSTER_MAKER")
    config = rollback_config(venue_id, version_index)
    return VenueConfigResponse.from_config(config)


@router.get(
    "/{venue_id}/staffing/{day}/{hour}",
    response_model=StaffingRequirementResponse,
    tags=["configuration"],
)
async def get_venue_staffing_requirement(
    venue_id: str, day: int, hour: int, request: Request
) -> StaffingRequirementResponse:
    """Get staffing requirement for a specific time slot.

    day: DayOfWeek value (0-6, 0=Monday)
    hour: 0-23

    Requires: L1_SUPERVISOR or higher
    """
    await _gate(request, "L1_SUPERVISOR")
    day_enum = DayOfWeek(day)
    req = get_staffing_requirement(venue_id, day_enum, hour)
    return StaffingRequirementResponse(
        day=req["day"],
        hour=req["hour"],
        by_area=req["by_area"],
    )


@router.post(
    "/{venue_id}/validate",
    response_model=ValidationResponse,
    tags=["configuration"],
)
async def validate_venue_config(
    venue_id: str, request: Request
) -> ValidationResponse:
    """Validate venue configuration.

    Requires: L2_ROSTER_MAKER or higher
    """
    await _gate(request, "L2_ROSTER_MAKER")
    config = get_config(venue_id)
    is_valid, errors = validate_config(config)
    return ValidationResponse(is_valid=is_valid, errors=errors)
