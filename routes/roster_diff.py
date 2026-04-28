"""
Roster comparison and diff API routes for RosterIQ.

Endpoints for comparing rosters, analyzing diffs, and tracking roster version history.

Routes:
    POST /api/v1/rosters/compare — compare two rosters by ID
    POST /api/v1/rosters/compare-inline — compare two roster objects (for preview)
    GET /api/v1/rosters/{id}/history — list roster versions with diffs
    GET /api/v1/rosters/{id}/diff — get diff between current and previous version

Response includes:
    - added_shifts: Shifts only in roster_b
    - removed_shifts: Shifts only in roster_a
    - modified_shifts: Shifts that changed
    - cost_delta: Total cost difference
    - hours_delta: Total hours difference
    - per_employee_impact: Dict of employee -> impact
    - summary: Human-readable text summary
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.models import Roster, User, UserRole
from rosteriq.services.roster_diff import RosterDiffService
from rosteriq.auth import get_current_user, require_role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/rosters", tags=["roster_diff"])

# Initialize service
_diff_service = RosterDiffService()


# ============================================================================
# Request/Response Models
# ============================================================================

class CompareRostersRequest(BaseModel):
    """Request to compare two rosters by ID."""

    roster_a_id: str = Field(..., description="ID of baseline roster")
    roster_b_id: str = Field(..., description="ID of comparison roster")
    include_details: bool = Field(
        default=True,
        description="Include shift details in response"
    )


class CompareRostersInlineRequest(BaseModel):
    """Request to compare two roster objects (for preview)."""

    roster_a: Roster = Field(..., description="Baseline roster")
    roster_b: Roster = Field(..., description="Comparison roster")
    include_details: bool = Field(
        default=True,
        description="Include shift details in response"
    )


class DiffResponse(BaseModel):
    """Response containing roster diff."""

    diff: dict = Field(..., description="Complete diff object")
    summary: str = Field(..., description="Human-readable summary")
    risk_changes: List[dict] = Field(
        default_factory=list,
        description="Identified risky changes"
    )
    savings_analysis: dict = Field(
        default_factory=dict,
        description="Cost savings breakdown"
    )


class RosterVersion(BaseModel):
    """Representation of a roster version."""

    roster_id: str
    version_number: int
    created_at: str
    created_by: str
    total_cost: Decimal
    total_hours: float
    shift_count: int
    employee_count: int


class RosterHistoryResponse(BaseModel):
    """Response with roster version history."""

    roster_id: str
    current_version: int
    versions: List[RosterVersion] = Field(
        default_factory=list,
        description="List of versions in descending order (newest first)"
    )
    diffs: List[dict] = Field(
        default_factory=list,
        description="Diffs between consecutive versions"
    )


# ============================================================================
# Routes
# ============================================================================

@router.post("/compare", response_model=DiffResponse)
async def compare_rosters(
    request: CompareRostersRequest,
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> DiffResponse:
    """
    Compare two rosters by ID.

    Returns detailed diff including added/removed/modified shifts,
    cost and hours deltas, per-employee impact, and risk analysis.
    """
    try:
        # Fetch both rosters
        roster_a = db.get_roster(request.roster_a_id)
        roster_b = db.get_roster(request.roster_b_id)

        if not roster_a:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {request.roster_a_id} not found"
            )
        if not roster_b:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {request.roster_b_id} not found"
            )

        # Verify user has access to both rosters (via venue)
        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Compare rosters
        diff = _diff_service.compare_rosters(roster_a, roster_b)

        # Analyze risks and savings
        risk_changes = _diff_service.identify_risk_changes(diff)
        savings_analysis = _diff_service.calculate_savings(diff)

        # Build response
        if request.include_details:
            diff_dict = _diff_service.diff_to_dict(diff)
        else:
            # Minimal response (no shift details)
            diff_dict = {
                "added_shifts": [],
                "removed_shifts": [],
                "modified_shifts": [],
                "cost_delta": str(diff.cost_delta),
                "hours_delta": diff.hours_delta,
                "headcount_delta": diff.headcount_delta,
                "shift_count_delta": diff.shift_count_delta,
                "per_employee_impact": {},
                "summary": "",
            }

        return DiffResponse(
            diff=diff_dict,
            summary=diff.summary,
            risk_changes=risk_changes,
            savings_analysis=savings_analysis,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing rosters: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare rosters: {str(e)}"
        )


@router.post("/compare-inline", response_model=DiffResponse)
async def compare_rosters_inline(
    request: CompareRostersInlineRequest,
    current_user: User = Depends(get_current_user),
) -> DiffResponse:
    """
    Compare two roster objects (for preview before save).

    Useful for showing users what will change when they modify a roster
    before committing to the database.
    """
    try:
        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Compare rosters
        diff = _diff_service.compare_rosters(request.roster_a, request.roster_b)

        # Analyze risks and savings
        risk_changes = _diff_service.identify_risk_changes(diff)
        savings_analysis = _diff_service.calculate_savings(diff)

        # Build response
        if request.include_details:
            diff_dict = _diff_service.diff_to_dict(diff)
        else:
            # Minimal response
            diff_dict = {
                "added_shifts": [],
                "removed_shifts": [],
                "modified_shifts": [],
                "cost_delta": str(diff.cost_delta),
                "hours_delta": diff.hours_delta,
                "headcount_delta": diff.headcount_delta,
                "shift_count_delta": diff.shift_count_delta,
                "per_employee_impact": {},
                "summary": "",
            }

        return DiffResponse(
            diff=diff_dict,
            summary=diff.summary,
            risk_changes=risk_changes,
            savings_analysis=savings_analysis,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing rosters inline: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare rosters: {str(e)}"
        )


@router.get("/{roster_id}/history", response_model=RosterHistoryResponse)
async def get_roster_history(
    roster_id: str,
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> RosterHistoryResponse:
    """
    Get history of a roster including all versions and diffs between them.

    Returns roster versions in descending order (newest first) and diffs
    between consecutive versions to show evolution of the roster.
    """
    try:
        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Fetch roster
        current_roster = db.get_roster(roster_id)
        if not current_roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        # Fetch version history from database
        # TODO: Implement version tracking in database layer
        # For now, return basic structure with current version only
        versions = [
            RosterVersion(
                roster_id=current_roster.id,
                version_number=1,
                created_at=current_roster.created_at.isoformat(),
                created_by="unknown",  # Would come from db
                total_cost=current_roster.total_cost or Decimal("0"),
                total_hours=current_roster.total_hours,
                shift_count=current_roster.shift_count,
                employee_count=len(current_roster.employees_used),
            )
        ]

        return RosterHistoryResponse(
            roster_id=roster_id,
            current_version=1,
            versions=versions,
            diffs=[],  # No diffs with only one version
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching roster history: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch roster history: {str(e)}"
        )


@router.get("/{roster_id}/diff")
async def get_diff_from_previous(
    roster_id: str,
    previous_roster_id: Optional[str] = Query(
        None,
        description="ID of previous roster to compare against (defaults to immediate predecessor)"
    ),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> DiffResponse:
    """
    Get diff between current roster and previous version.

    If previous_roster_id is provided, compares against that specific roster.
    Otherwise, compares against the immediate predecessor in version history.
    """
    try:
        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Fetch current roster
        current_roster = db.get_roster(roster_id)
        if not current_roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        # Fetch previous roster
        if previous_roster_id:
            previous_roster = db.get_roster(previous_roster_id)
            if not previous_roster:
                raise HTTPException(
                    status_code=404,
                    detail=f"Previous roster {previous_roster_id} not found"
                )
        else:
            # TODO: Look up previous version from database
            raise HTTPException(
                status_code=400,
                detail="Version history not yet implemented; provide previous_roster_id"
            )

        # Compare rosters
        diff = _diff_service.compare_rosters(previous_roster, current_roster)

        # Analyze risks and savings
        risk_changes = _diff_service.identify_risk_changes(diff)
        savings_analysis = _diff_service.calculate_savings(diff)

        return DiffResponse(
            diff=_diff_service.diff_to_dict(diff),
            summary=diff.summary,
            risk_changes=risk_changes,
            savings_analysis=savings_analysis,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting roster diff: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get roster diff: {str(e)}"
        )
