"""
Break scheduling API routes for RosterIQ.

Endpoints:
- POST /api/breaks/schedule/{roster_id} — auto-insert breaks into roster
- GET /api/breaks/validate/{roster_id} — validate break compliance
- POST /api/breaks/optimise — optimise break timing given shifts and demand
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, date
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import load_roster_in_scope, enforce_venue_manager
from rosteriq.services.break_scheduler import (
    BreakScheduler, BreakConfig, Break, BreakComplianceReport
)
from rosteriq.models import Shift

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/breaks", tags=["breaks"])


# ============================================================================
# Request/Response Models
# ============================================================================


class BreakResponse(BaseModel):
    """Response model for a scheduled break."""
    employee_id: str
    shift_id: str
    break_type: str
    start_time: datetime
    end_time: datetime
    is_paid: bool
    duration_minutes: int
    position_in_shift: str


class BreakViolationResponse(BaseModel):
    """Response model for a break violation."""
    shift_id: str
    employee_id: str
    violation_type: str
    details: str
    severity: str


class BreakComplianceReportResponse(BaseModel):
    """Response model for break compliance report."""
    total_shifts: int
    compliant_shifts: int
    compliance_score: float
    violations: List[BreakViolationResponse]
    violations_by_type: Dict[str, int]


class ScheduleBreaksRequest(BaseModel):
    """Request to auto-schedule breaks for a roster."""
    roster_id: str


class ValidateBreaksRequest(BaseModel):
    """Request to validate breaks in a roster."""
    roster_id: str


class OptimiseBreaksRequest(BaseModel):
    """Request to optimise break timing."""
    shifts: List[dict]  # shift dictionaries
    demand_curve: Dict[int, float]  # hour -> demand (0.0-1.0)


class OptimiseBreaksResponse(BaseModel):
    """Response with optimised break times."""
    break_times: Dict[str, datetime]  # shift_id -> start_time


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/schedule/{roster_id}")
async def schedule_breaks(roster_id: str) -> Dict:
    """
    Auto-insert breaks into roster shifts.

    For each shift, calculates required breaks based on MA000009 rules,
    places them at optimal times (low-demand periods), and staggers
    across staff to maintain minimum coverage.

    Args:
        roster_id: ID of the roster to process

    Returns:
        Updated roster with breaks inserted and list of scheduled breaks
    """
    db = get_db()
    # 404 if missing or another tenant's; then require manager/owner of the venue.
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager(getattr(roster, "venue_id", None))

    try:
        scheduler = BreakScheduler(config=BreakConfig())
        updated_roster, breaks_scheduled = scheduler.schedule_breaks(roster)

        # Save updated roster
        db.save_roster(updated_roster)

        return {
            "roster_id": roster_id,
            "total_shifts": len(roster.shifts),
            "total_breaks_scheduled": len(breaks_scheduled),
            "breaks": [
                {
                    "employee_id": b.employee_id,
                    "shift_id": b.shift_id,
                    "break_type": b.break_type,
                    "start_time": b.start_time.isoformat(),
                    "end_time": b.end_time.isoformat(),
                    "is_paid": b.is_paid,
                    "duration_minutes": b.duration_minutes,
                    "position_in_shift": b.position_in_shift,
                }
                for b in breaks_scheduled
            ],
            "success": True,
        }
    except Exception as e:
        logger.error(f"Error scheduling breaks for roster {roster_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to schedule breaks: {str(e)}",
        )


@router.get("/validate/{roster_id}")
async def validate_breaks(roster_id: str) -> Dict:
    """
    Validate break compliance for a roster.

    Checks every shift for MA000009 compliance:
    - Required breaks present
    - Breaks taken at correct times
    - Sufficient gaps between breaks

    Args:
        roster_id: ID of the roster to validate

    Returns:
        BreakComplianceReport with violations and compliance score
    """
    db = get_db()
    # Read: 404 if missing or another tenant's (membership scope; no role gate).
    roster = load_roster_in_scope(db, roster_id)

    try:
        scheduler = BreakScheduler(config=BreakConfig())
        report = scheduler.validate_breaks(roster)

        return {
            "roster_id": roster_id,
            "total_shifts": report.total_shifts,
            "compliant_shifts": report.compliant_shifts,
            "compliance_score": report.compliance_score,
            "violations_by_type": report.violations_by_type,
            "violations": [
                {
                    "shift_id": v.shift_id,
                    "employee_id": v.employee_id,
                    "violation_type": v.violation_type,
                    "details": v.details,
                    "severity": v.severity,
                }
                for v in report.violations
            ],
            "compliant": report.compliance_score == 1.0,
        }
    except Exception as e:
        logger.error(f"Error validating breaks for roster {roster_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to validate breaks: {str(e)}",
        )


@router.post("/optimise")
async def optimise_break_timing(request: OptimiseBreaksRequest) -> Dict:
    """
    Optimise break timing given concurrent shifts and hourly demand forecast.

    Uses greedy algorithm to assign breaks to lowest-demand hours,
    minimising coverage gaps and maintaining minimum staffing.

    Args:
        request: OptimiseBreaksRequest with shifts and demand_curve

    Returns:
        Dict mapping shift_id to optimal break start time
    """
    try:
        # Reconstruct Shift objects from request data
        shifts = []
        for shift_data in request.shifts:
            # Minimal reconstruction for timing purposes
            shift = Shift(
                id=shift_data.get("id"),
                employee_id=shift_data.get("employee_id"),
                start_time=datetime.fromisoformat(shift_data["start_time"])
                if "start_time" in shift_data
                else None,
                end_time=datetime.fromisoformat(shift_data["end_time"])
                if "end_time" in shift_data
                else None,
            )
            shifts.append(shift)

        scheduler = BreakScheduler(config=BreakConfig())
        break_times = scheduler.optimise_break_timing(shifts, request.demand_curve)

        return {
            "success": True,
            "total_shifts": len(shifts),
            "break_times": {
                shift_id: time.isoformat() for shift_id, time in break_times.items()
            },
        }
    except Exception as e:
        logger.error(f"Error optimising break timing: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to optimise break timing: {str(e)}",
        )
