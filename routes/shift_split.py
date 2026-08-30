"""
Shift Splitting API routes for RosterIQ.

Endpoints for intelligent auto-splitting of long or non-compliant shifts
to meet Fair Work Australia requirements and optimise penalty costs.

Endpoints:
- POST /api/v1/rosters/{id}/auto-split — preview all suggested splits
- POST /api/v1/rosters/{id}/apply-splits — apply selected splits
- POST /api/v1/shifts/{id}/split — split a single shift
- GET /api/v1/rosters/{id}/compliance-check — check for splittable shifts
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, date, time
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import (
    load_roster_in_scope, load_shift_in_scope, enforce_venue_manager,
)
from rosteriq.services.shift_splitter import (
    ShiftSplitter, SplitResult, ShiftSplit, ShiftSegment
)
from rosteriq.models import Shift, Roster

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["shift_splitting"])


# ============================================================================
# Request/Response Models
# ============================================================================


class ShiftSegmentResponse(BaseModel):
    """Response model for a shift segment."""
    start_time: str  # ISO format time
    end_time: str
    break_minutes: int
    employee_id: str
    estimated_cost: Optional[str] = None  # Decimal as string


class ShiftSplitResponse(BaseModel):
    """Response model for a shift split suggestion."""
    original_shift_id: str
    reason: str  # compliance/breaks/cost_optimisation
    original_start: str  # ISO format time
    original_end: str
    new_segments: List[ShiftSegmentResponse] = []
    compliance_violations_fixed: List[str] = []

    # Flatten nested structure for response
    @staticmethod
    def from_split(split: ShiftSplit) -> "ShiftSplitResponse":
        """Convert ShiftSplit to response model."""
        return ShiftSplitResponse(
            original_shift_id=split.original_shift_id,
            reason=split.reason,
            original_start=split.original_start.isoformat(),
            original_end=split.original_end.isoformat(),
            new_segments=[
                ShiftSegmentResponse(
                    start_time=seg.start_time.isoformat(),
                    end_time=seg.end_time.isoformat(),
                    break_minutes=seg.break_minutes,
                    employee_id=seg.employee_id,
                    estimated_cost=str(seg.estimated_cost) if seg.estimated_cost else None,
                )
                for seg in split.new_segments
            ],
            compliance_violations_fixed=split.compliance_violations_fixed,
        )


class SplitResultResponse(BaseModel):
    """Response model for split operation results."""
    roster_id: str
    original_shift_count: int
    new_shift_count: int
    shifts_split: int
    splits: List[Dict] = []
    cost_before: str  # Decimal as string
    cost_after: str
    cost_delta: str
    compliance_issues_fixed: int
    warnings: List[str] = []


class AutoSplitRequest(BaseModel):
    """Request to auto-split a roster."""
    roster_id: str


class ApplySplitsRequest(BaseModel):
    """Request to apply selected splits."""
    roster_id: str
    split_shift_ids: List[str]  # Original shift IDs to split


class SplitSingleShiftRequest(BaseModel):
    """Request to split a single shift."""
    shift_id: str
    roster_id: str
    strategy: str = "compliance"  # compliance/breaks/cost_optimisation


class ComplianceCheckResponse(BaseModel):
    """Response for compliance check."""
    roster_id: str
    total_shifts: int
    compliant_shifts: int
    non_compliant_shifts: int
    compliance_percentage: float
    violations_by_type: Dict[str, int] = {}
    warnings: List[str] = []


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/rosters/{roster_id}/auto-split")
async def preview_roster_splits(roster_id: str) -> Dict:
    """
    Preview all suggested shift splits for a roster.

    Analyses the roster without modifying it and returns all shifts that
    need splitting, grouped by reason (compliance, breaks, cost optimisation).
    This is a dry-run operation.

    Args:
        roster_id: ID of the roster to analyse

    Returns:
        SplitResultResponse with all suggested splits and estimated cost impact
    """
    db = get_db()
    # 404 if missing or another tenant's (membership scope), before any work.
    roster = load_roster_in_scope(db, roster_id)

    try:
        splitter = ShiftSplitter()
        result = splitter.preview_splits(roster_id)

        return {
            "roster_id": result.roster_id,
            "original_shift_count": result.original_shift_count,
            "new_shift_count": result.new_shift_count,
            "shifts_split": result.shifts_split,
            "splits": [
                {
                    "original_shift_id": split.original_shift_id,
                    "reason": split.reason,
                    "original_start": split.original_start.isoformat(),
                    "original_end": split.original_end.isoformat(),
                    "new_segments": [
                        {
                            "start_time": seg.start_time.isoformat(),
                            "end_time": seg.end_time.isoformat(),
                            "break_minutes": seg.break_minutes,
                            "employee_id": seg.employee_id,
                            "estimated_cost": str(seg.estimated_cost) if seg.estimated_cost else None,
                        }
                        for seg in split.new_segments
                    ],
                    "compliance_violations_fixed": split.compliance_violations_fixed,
                }
                for split in result.splits
            ],
            "cost_before": str(result.cost_before),
            "cost_after": str(result.cost_after),
            "cost_delta": str(result.cost_delta),
            "compliance_issues_fixed": result.compliance_issues_fixed,
            "warnings": result.warnings,
            "success": True,
        }
    except ValueError as e:
        logger.warning(f"Validation error in auto-split for roster {roster_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error previewing splits for roster {roster_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to preview splits: {str(e)}",
        )


@router.post("/rosters/{roster_id}/apply-splits")
async def apply_roster_splits(roster_id: str, request: ApplySplitsRequest) -> Dict:
    """
    Apply selected shift splits to a roster.

    Persists the split shifts to the database, replacing original shifts
    with their split segments. Only splits the shifts specified in the request.

    Args:
        roster_id: ID of the roster
        request: ApplySplitsRequest with list of shift IDs to split

    Returns:
        Updated Roster with splits applied
    """
    db = get_db()
    # 404 if missing or another tenant's; apply-splits persists -> manager.
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager(getattr(roster, "venue_id", None))

    if request.roster_id != roster_id:
        raise HTTPException(
            status_code=400,
            detail="Roster ID in URL and request body do not match",
        )

    try:
        splitter = ShiftSplitter()
        updated_roster = splitter.apply_splits(roster_id, request.split_shift_ids)

        return {
            "roster_id": updated_roster.id,
            "total_shifts": len(updated_roster.shifts),
            "shifts_created": sum(
                1 for s in updated_roster.shifts if "_split_" in s.id
            ),
            "success": True,
            "message": f"Applied splits to {len(request.split_shift_ids)} shifts",
        }
    except ValueError as e:
        logger.warning(f"Validation error in apply-splits for roster {roster_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error applying splits to roster {roster_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to apply splits: {str(e)}",
        )


@router.post("/shifts/{shift_id}/split")
async def split_single_shift(
    shift_id: str,
    request: SplitSingleShiftRequest,
) -> Dict:
    """
    Split a single shift and return preview of resulting segments.

    This endpoint is useful for splitting a single problematic shift
    without needing to analyse the whole roster.

    Args:
        shift_id: ID of the shift to split
        request: SplitSingleShiftRequest with strategy

    Returns:
        Dict with original shift details and suggested segments
    """
    db = get_db()
    # 404 if missing or another tenant's (membership scope via the shift's venue).
    shift = load_shift_in_scope(db, shift_id)

    roster = db.get_roster(request.roster_id)
    if not roster:
        raise HTTPException(status_code=404, detail=f"Roster {request.roster_id} not found")

    if shift not in roster.shifts:
        raise HTTPException(
            status_code=400,
            detail=f"Shift {shift_id} not found in roster {request.roster_id}",
        )

    try:
        employee = db.get_employee(shift.employee_id)
        if not employee:
            raise ValueError(f"Employee {shift.employee_id} not found")

        splitter = ShiftSplitter()

        # Split based on strategy
        if request.strategy == "compliance":
            segments = splitter._split_for_compliance(shift, employee)
        elif request.strategy == "breaks":
            segments = splitter._split_for_breaks(shift, employee)
        elif request.strategy == "cost_optimisation":
            segments = splitter._split_for_cost_optimisation(shift, employee)
        else:
            raise ValueError(f"Unknown strategy: {request.strategy}")

        venue = db.get_venue(roster.venue_id)
        estimated_cost = splitter._estimate_segments_cost(
            segments, employee, shift.date, venue.state
        )

        return {
            "shift_id": shift_id,
            "original": {
                "date": shift.date.isoformat(),
                "start_time": shift.start_time.isoformat(),
                "end_time": shift.end_time.isoformat(),
                "break_minutes": shift.break_minutes,
                "duration_hours": shift.duration_hours,
                "net_hours": shift.net_hours,
                "cost": str(shift.cost) if shift.cost else None,
            },
            "strategy": request.strategy,
            "segments": [
                {
                    "start_time": seg.start_time.isoformat(),
                    "end_time": seg.end_time.isoformat(),
                    "break_minutes": seg.break_minutes,
                    "employee_id": seg.employee_id,
                    "duration_hours": seg.duration_hours(),
                    "net_hours": seg.net_hours(),
                    "estimated_cost": str(seg.estimated_cost) if seg.estimated_cost else None,
                }
                for seg in segments
            ],
            "total_cost_estimate": str(estimated_cost),
            "cost_delta": str(estimated_cost - (shift.cost or Decimal("0.00"))),
            "success": True,
        }
    except ValueError as e:
        logger.warning(f"Validation error in split-single-shift for {shift_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error splitting shift {shift_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to split shift: {str(e)}",
        )


@router.get("/rosters/{roster_id}/compliance-check")
async def check_roster_compliance(roster_id: str) -> Dict:
    """
    Check roster for compliance issues and identify splittable shifts.

    Scans the roster for shifts that violate Fair Work Australia MA000009
    requirements and need splitting. Returns a detailed compliance report.

    Args:
        roster_id: ID of the roster to check

    Returns:
        ComplianceCheckResponse with violations and suggested fixes
    """
    db = get_db()
    # 404 if missing or another tenant's (membership scope), before any work.
    roster = load_roster_in_scope(db, roster_id)

    try:
        violations_by_type = {
            "max_shift_length": 0,
            "missing_breaks": 0,
            "min_engagement": 0,
            "other": 0,
        }

        non_compliant_shifts = []
        warnings = []

        for shift in roster.shifts:
            employee = db.get_employee(shift.employee_id)
            if not employee:
                continue

            # Check compliance
            splitter = ShiftSplitter()
            split_reasons = splitter._check_split_requirements(shift, employee)

            if split_reasons:
                for reason in split_reasons:
                    if reason == "compliance":
                        violations_by_type["max_shift_length"] += 1
                    elif reason == "breaks":
                        violations_by_type["missing_breaks"] += 1
                    elif reason == "engagement":
                        violations_by_type["min_engagement"] += 1
                    else:
                        violations_by_type["other"] += 1

                non_compliant_shifts.append(shift.id)

        compliance_percentage = (
            ((len(roster.shifts) - len(non_compliant_shifts)) / len(roster.shifts) * 100)
            if roster.shifts
            else 100.0
        )

        # Generate warnings
        if violations_by_type["max_shift_length"] > 0:
            warnings.append(
                f"{violations_by_type['max_shift_length']} shifts exceed maximum "
                f"10-hour length"
            )
        if violations_by_type["missing_breaks"] > 0:
            warnings.append(
                f"{violations_by_type['missing_breaks']} shifts missing required breaks"
            )

        return {
            "roster_id": roster_id,
            "total_shifts": len(roster.shifts),
            "compliant_shifts": len(roster.shifts) - len(non_compliant_shifts),
            "non_compliant_shifts": len(non_compliant_shifts),
            "compliance_percentage": round(compliance_percentage, 2),
            "violations_by_type": violations_by_type,
            "non_compliant_shift_ids": non_compliant_shifts,
            "warnings": warnings,
            "success": True,
        }
    except Exception as e:
        logger.error(f"Error checking compliance for roster {roster_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check compliance: {str(e)}",
        )
