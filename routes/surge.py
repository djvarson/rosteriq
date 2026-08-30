"""
Real-time demand surge detection API routes for RosterIQ.

Endpoints:
- GET /api/v1/venues/{venue_id}/surge-status — current surge status
- GET /api/v1/venues/{venue_id}/surge-history — recent surge events
- POST /api/v1/venues/{venue_id}/call-in-staff — trigger call-in for surge
- GET /api/v1/venues/{venue_id}/quiet-status — check if overstaffed
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, List, Any

from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.services.surge_detector import (
    get_detector,
    SurgeStatus,
    QuietStatus,
    SurgeEvent,
    OnCallEmployee,
)
from rosteriq.services.ws_events import get_dispatcher
from rosteriq.middleware.tenant import enforce_venue_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/venues", tags=["surge"])


# ============================================================================
# Request/Response Models
# ============================================================================


class OnCallEmployeeResponse(BaseModel):
    """Available on-call employee."""

    employee_id: str
    name: str
    phone: Optional[str] = None
    skills: List[str] = Field(default_factory=list)
    estimated_arrival_minutes: int
    hourly_cost: str  # Decimal as string
    response_time_score: float


class SurgeStatusResponse(BaseModel):
    """Current surge status response."""

    venue_id: str
    timestamp: str
    is_surging: bool
    surge_level: str  # "none", "mild", "moderate", "critical"
    deviation_pct: float
    predicted_covers: float
    estimated_actual_covers: float
    additional_staff_needed: Dict[str, int]
    available_oncall: List[OnCallEmployeeResponse]
    suggested_action: str
    estimated_extra_cost: str  # Decimal as string


class SurgeHistoryEventResponse(BaseModel):
    """Historical surge event."""

    venue_id: str
    timestamp: str
    surge_level: str
    deviation_pct: float
    actual_covers: float
    staff_called_in: int
    staff_released: int = 0


class CallInStaffRequest(BaseModel):
    """Request to call in staff for a surge."""

    employee_ids: List[str] = Field(..., description="IDs of employees to call in")
    reason: str = Field(
        default="demand_surge", description="Reason for call-in (surge, event, etc)"
    )
    estimated_duration_hours: float = Field(
        default=2.0, description="Estimated duration of shift"
    )


class CallInStaffResponse(BaseModel):
    """Response after calling in staff."""

    venue_id: str
    success: bool
    called_in_count: int
    total_estimated_cost: str  # Decimal as string
    message: str


class QuietStatusResponse(BaseModel):
    """Quiet period (overstaffed) detection response."""

    venue_id: str
    timestamp: str
    is_quiet: bool
    deviation_pct: float
    staff_releasable: int
    estimated_savings: str  # Decimal as string
    suggested_action: str


# ============================================================================
# Endpoints
# ============================================================================


@router.get(
    "/{venue_id}/surge-status",
    response_model=SurgeStatusResponse,
    summary="Get current surge status",
    description="Check current demand surge status for a venue",
)
async def get_surge_status(
    venue_id: str = Path(..., description="Venue identifier"),
) -> SurgeStatusResponse:
    """
    Get current surge status for a venue.

    Compares actual POS data against forecasted demand and returns
    surge level, staff needed, and recommended action.
    """
    try:
        db = get_db()
        detector = get_detector(database=db)

        status = await detector.check_surge(venue_id)

        return SurgeStatusResponse(
            venue_id=status.venue_id,
            timestamp=status.timestamp,
            is_surging=status.is_surging,
            surge_level=status.surge_level,
            deviation_pct=status.deviation_pct,
            predicted_covers=status.predicted_covers,
            estimated_actual_covers=status.estimated_actual_covers,
            additional_staff_needed=status.additional_staff_needed,
            available_oncall=[
                OnCallEmployeeResponse(
                    employee_id=emp.employee_id,
                    name=emp.name,
                    phone=emp.phone,
                    skills=emp.skills,
                    estimated_arrival_minutes=emp.estimated_arrival_minutes,
                    hourly_cost=str(emp.hourly_cost),
                    response_time_score=emp.response_time_score,
                )
                for emp in status.available_oncall
            ],
            suggested_action=status.suggested_action,
            estimated_extra_cost=str(status.estimated_extra_cost),
        )
    except Exception as e:
        logger.error(f"Failed to get surge status for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get surge status: {str(e)}",
        )


@router.get(
    "/{venue_id}/surge-history",
    response_model=List[SurgeHistoryEventResponse],
    summary="Get recent surge events",
    description="Retrieve recent surge events for trend analysis",
)
async def get_surge_history(
    venue_id: str = Path(..., description="Venue identifier"),
    days: int = Query(7, ge=1, le=90, description="Days to look back"),
) -> List[SurgeHistoryEventResponse]:
    """
    Get recent surge events for a venue.

    Returns surge events over the past N days, useful for trend analysis
    and identifying recurring surge patterns.
    """
    try:
        db = get_db()
        detector = get_detector(database=db)

        events = detector.get_surge_history(venue_id, days=days)

        return [
            SurgeHistoryEventResponse(
                venue_id=event.venue_id,
                timestamp=event.timestamp,
                surge_level=event.surge_level,
                deviation_pct=event.deviation_pct,
                actual_covers=event.actual_covers,
                staff_called_in=event.staff_called_in,
                staff_released=event.staff_released,
            )
            for event in events
        ]
    except Exception as e:
        logger.error(f"Failed to get surge history for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get surge history: {str(e)}",
        )


@router.post(
    "/{venue_id}/call-in-staff",
    response_model=CallInStaffResponse,
    summary="Call in staff for surge",
    description="Trigger immediate call-in for available staff during demand surge",
)
async def call_in_staff(
    venue_id: str = Path(..., description="Venue identifier"),
    request: CallInStaffRequest = None,
) -> CallInStaffResponse:
    """
    Call in staff to handle demand surge.

    Triggers immediate call-in for specified employees and logs the action.
    Broadcasts notification via WebSocket to on-duty managers.
    """
    enforce_venue_manager(venue_id)
    if not request:
        request = CallInStaffRequest(employee_ids=[])

    try:
        db = get_db()
        dispatcher = get_dispatcher()

        if not request.employee_ids:
            raise HTTPException(
                status_code=400,
                detail="At least one employee ID required",
            )

        # Verify employees exist and belong to venue
        called_in = []
        total_cost = Decimal("0.00")

        for emp_id in request.employee_ids:
            try:
                emp = db.get_employee(emp_id)
                if not emp or emp.id not in [
                    e.id for e in db.list_employees(venue_id)
                ]:
                    logger.warning(
                        f"Employee {emp_id} not found or not in {venue_id}"
                    )
                    continue

                called_in.append(emp_id)

                # Estimate cost
                estimated_shift_cost = (
                    emp.hourly_base_rate * Decimal(str(request.estimated_duration_hours))
                )
                total_cost += estimated_shift_cost

                # Log the call-in action
                db.log_surge_action(
                    venue_id=venue_id,
                    action="call_in",
                    employee_id=emp_id,
                    reason=request.reason,
                    timestamp=datetime.now(),
                )
            except Exception as e:
                logger.error(f"Failed to process call-in for {emp_id}: {e}")
                continue

        # Broadcast notification
        try:
            await dispatcher.send_alert(
                venue_id=venue_id,
                alert_type="staff_called_in",
                severity="info",
                message=(
                    f"{len(called_in)} staff called in for {request.reason}. "
                    f"Estimated cost: ${total_cost}"
                ),
            )
        except Exception as e:
            logger.error(f"Failed to broadcast call-in notification: {e}")

        return CallInStaffResponse(
            venue_id=venue_id,
            success=len(called_in) > 0,
            called_in_count=len(called_in),
            total_estimated_cost=str(total_cost),
            message=f"Called in {len(called_in)} of {len(request.employee_ids)} staff",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to call in staff for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to call in staff: {str(e)}",
        )


@router.get(
    "/{venue_id}/quiet-status",
    response_model=QuietStatusResponse,
    summary="Detect quiet periods",
    description="Check if venue is overstaffed and staff can be released early",
)
async def get_quiet_status(
    venue_id: str = Path(..., description="Venue identifier"),
) -> QuietStatusResponse:
    """
    Detect quiet periods (opposite of surge).

    Identifies when actual demand is significantly below forecast,
    suggesting opportunity to release staff early and save costs.
    """
    try:
        db = get_db()
        detector = get_detector(database=db)

        status = await detector.detect_quiet_period(venue_id)

        return QuietStatusResponse(
            venue_id=status.venue_id,
            timestamp=status.timestamp,
            is_quiet=status.is_quiet,
            deviation_pct=status.deviation_pct,
            staff_releasable=status.staff_releasable,
            estimated_savings=str(status.estimated_savings),
            suggested_action=status.suggested_action,
        )
    except Exception as e:
        logger.error(f"Failed to get quiet status for {venue_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get quiet status: {str(e)}",
        )
