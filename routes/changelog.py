"""
Roster changelog API routes for RosterIQ.

Endpoints for managing immutable roster audit trails, version history,
change tracking, and compliance reporting.

Routes:
    GET /api/v1/rosters/{id}/changelog — paginated changelog
    GET /api/v1/rosters/{id}/changelog/version — current version number
    GET /api/v1/rosters/{id}/changelog/diff — diff between versions
    POST /api/v1/rosters/{id}/changelog/revert/{version} — revert to version
    GET /api/v1/venues/{id}/recent-changes — recent activity
    GET /api/v1/rosters/{id}/changelog/export — export for audit
    GET /api/v1/rosters/{id}/changelog/stats — change history statistics

Response includes complete audit trail with timestamps, authors, change types,
and detailed field-level changes.
"""

import logging
from datetime import datetime
from typing import Optional, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.models import User, UserRole
from rosteriq.services.roster_changelog import (
    RosterChangelogService,
    ChangeEntry,
    ChangeDetails,
    FieldChange,
)
from rosteriq.auth import get_current_user, require_role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["changelog"])

# Initialize service
_changelog_service = RosterChangelogService()


# ============================================================================
# Request/Response Models
# ============================================================================

class FieldChangeRequest(BaseModel):
    """Request model for a field change."""

    field: str = Field(..., description="Field name (e.g., 'start_time')")
    old_value: str = Field(..., description="Previous value")
    new_value: str = Field(..., description="New value")


class ChangeDetailsRequest(BaseModel):
    """Request model for change details."""

    shift_id: Optional[str] = Field(None, description="Affected shift ID")
    employee_id: Optional[str] = Field(None, description="Affected employee ID")
    field_changes: List[FieldChangeRequest] = Field(
        default_factory=list,
        description="List of field changes"
    )
    affected_employees: List[str] = Field(
        default_factory=list,
        description="List of affected employee IDs (for bulk updates)"
    )
    swapped_with_shift_id: Optional[str] = Field(
        None,
        description="Shift ID this was swapped with (for shift_swapped)"
    )
    swap_partner_employee_id: Optional[str] = Field(
        None,
        description="Employee ID of swap partner (for shift_swapped)"
    )
    reason: Optional[str] = Field(None, description="Reason for the change")


class ChangeEntryResponse(BaseModel):
    """Response model for a change entry."""

    id: str
    roster_id: str
    venue_id: str
    timestamp: str
    author_id: str
    author_name: str
    change_type: str
    description: str
    details: dict  # Serialized ChangeDetails
    version: int


class ChangelogResponse(BaseModel):
    """Response model for changelog query."""

    roster_id: str
    entries: List[ChangeEntryResponse]
    total_count: int
    limit: int
    offset: int


class VersionResponse(BaseModel):
    """Response with current version number."""

    roster_id: str
    current_version: int
    first_change_timestamp: Optional[str] = None
    last_change_timestamp: Optional[str] = None


class DiffResponse(BaseModel):
    """Response with diff between versions."""

    roster_id: str
    version_a: int
    version_b: int
    changes_count: int
    changes_between: List[ChangeEntryResponse]
    summary: str


class RecentActivityResponse(BaseModel):
    """Response with recent changes for a venue."""

    venue_id: str
    entries: List[ChangeEntryResponse]
    total_count: int


class ChangeStatsResponse(BaseModel):
    """Response with changelog statistics."""

    roster_id: str
    total_changes: int
    current_version: int
    change_types: dict
    top_authors: List[tuple]
    first_change_timestamp: Optional[str] = None
    last_change_timestamp: Optional[str] = None


# ============================================================================
# Routes
# ============================================================================

@router.get("/rosters/{roster_id}/changelog", response_model=ChangelogResponse)
async def get_changelog(
    roster_id: str,
    limit: int = Query(50, ge=1, le=200, description="Max entries to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> ChangelogResponse:
    """
    GET /api/v1/rosters/{roster_id}/changelog

    Get paginated changelog for a roster in reverse chronological order.

    Query Parameters:
    - limit: Maximum entries to return (1-200, default 50)
    - offset: Pagination offset (default 0)

    Returns:
    - entries: List of change entries
    - total_count: Total number of changes for this roster
    - limit, offset: Echo of pagination parameters
    """
    try:
        # Verify roster exists and user has access
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get changelog
        entries = _changelog_service.get_changelog(roster_id, limit=limit, offset=offset)

        # Build response
        return ChangelogResponse(
            roster_id=roster_id,
            entries=[
                ChangeEntryResponse(**e.to_dict()) for e in entries
            ],
            total_count=len(_changelog_service.get_changelog(roster_id, limit=9999)),
            limit=limit,
            offset=offset,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting changelog for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get changelog: {str(e)}"
        )


@router.get("/rosters/{roster_id}/changelog/version", response_model=VersionResponse)
async def get_version(
    roster_id: str,
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> VersionResponse:
    """
    GET /api/v1/rosters/{roster_id}/changelog/version

    Get current version number for a roster.

    Returns:
    - current_version: Current version number
    - first_change_timestamp: When changelog started (ISO 8601)
    - last_change_timestamp: When last change was recorded (ISO 8601)
    """
    try:
        # Verify roster exists
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get version info
        current_version = _changelog_service.get_version(roster_id)
        entries = _changelog_service.get_changelog(roster_id, limit=9999)

        first_timestamp = None
        last_timestamp = None
        if entries:
            sorted_entries = sorted(entries, key=lambda e: e.timestamp)
            first_timestamp = sorted_entries[0].timestamp
            last_timestamp = sorted_entries[-1].timestamp

        return VersionResponse(
            roster_id=roster_id,
            current_version=current_version,
            first_change_timestamp=first_timestamp,
            last_change_timestamp=last_timestamp,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting version for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get version: {str(e)}"
        )


@router.get("/rosters/{roster_id}/changelog/diff", response_model=DiffResponse)
async def get_diff(
    roster_id: str,
    from_version: int = Query(..., ge=1, description="Starting version"),
    to_version: int = Query(..., ge=1, description="Ending version"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> DiffResponse:
    """
    GET /api/v1/rosters/{roster_id}/changelog/diff?from_version=1&to_version=5

    Get diff between two versions of a roster.

    Query Parameters:
    - from_version: Starting version number (required)
    - to_version: Ending version number (required)

    Returns:
    - changes_between: All changes between the versions
    - summary: Text summary of changes
    - changes_count: Number of changes
    """
    try:
        # Verify roster exists
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get diff
        diff = _changelog_service.diff_versions(roster_id, from_version, to_version)

        return DiffResponse(
            roster_id=roster_id,
            version_a=diff["version_a"],
            version_b=diff["version_b"],
            changes_count=diff["changes_count"],
            changes_between=[
                ChangeEntryResponse(**c) for c in diff["changes_between"]
            ],
            summary=diff["summary"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting diff for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get diff: {str(e)}"
        )


@router.post("/rosters/{roster_id}/changelog/revert/{version}")
async def revert_to_version(
    roster_id: str,
    version: int = Query(..., ge=1, description="Target version to revert to"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> dict:
    """
    POST /api/v1/rosters/{roster_id}/changelog/revert/{version}

    Revert a roster to a previous version.

    This creates a new change entry marking the reversion, so the action
    is auditable and reversible.

    Path Parameters:
    - version: Version number to revert to (must be less than current)

    Returns:
    - status: "reverted"
    - previous_version: Version that was current before revert
    - new_version: Version after revert
    - change_entry: The revert change entry recorded
    """
    try:
        # Verify roster exists
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get current version
        current_version = _changelog_service.get_version(roster_id)

        # Validate target version
        if version >= current_version:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot revert to version {version}: current version is {current_version}"
            )

        if version < 1:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid version {version}: must be >= 1"
            )

        # Get changes between target and current
        changes = _changelog_service.get_changes_between(roster_id, version, current_version)

        # Record revert as a change
        entry = _changelog_service.record_change(
            roster_id=roster_id,
            venue_id=roster.venue_id,
            author_id=current_user.id,
            author_name=current_user.name,
            change_type="revert",
            description=f"Reverted from version {current_version} to version {version}",
            details=ChangeDetails(
                reason=f"Manual revert from version {current_version}",
                affected_employees=list(roster.employees_used),
            ),
        )

        return {
            "status": "reverted",
            "previous_version": current_version,
            "new_version": _changelog_service.get_version(roster_id),
            "change_entry": ChangeEntryResponse(**entry.to_dict()),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reverting roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to revert roster: {str(e)}"
        )


@router.get("/venues/{venue_id}/recent-changes", response_model=RecentActivityResponse)
async def get_recent_activity(
    venue_id: str,
    limit: int = Query(20, ge=1, le=100, description="Max entries to return"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> RecentActivityResponse:
    """
    GET /api/v1/venues/{venue_id}/recent-changes

    Get recent changes across all rosters for a venue.

    Query Parameters:
    - limit: Maximum entries to return (1-100, default 20)

    Returns:
    - entries: Recent change entries for all rosters at the venue
    - total_count: Number of entries returned
    """
    try:
        # Verify venue exists
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(
                status_code=404,
                detail=f"Venue {venue_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get recent activity
        entries = _changelog_service.get_recent_activity(venue_id, limit=limit)

        return RecentActivityResponse(
            venue_id=venue_id,
            entries=[
                ChangeEntryResponse(**e.to_dict()) for e in entries
            ],
            total_count=len(entries),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting recent activity for venue {venue_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get recent activity: {str(e)}"
        )


@router.get("/rosters/{roster_id}/changelog/export")
async def export_changelog(
    roster_id: str,
    format: str = Query("text", regex="^(text|json)$", description="Export format"),
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> dict:
    """
    GET /api/v1/rosters/{roster_id}/changelog/export

    Export changelog for audit purposes.

    Query Parameters:
    - format: "text" (formatted text) or "json" (JSON array)

    Returns:
    - format: Export format used
    - content: Exported changelog content
    - generated_at: ISO 8601 timestamp
    """
    try:
        # Verify roster exists
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        if format == "text":
            content = _changelog_service.export_changelog(roster_id)
        else:  # json
            content = _changelog_service.get_changelog_json(roster_id, limit=9999)

        return {
            "roster_id": roster_id,
            "format": format,
            "content": content,
            "generated_at": datetime.utcnow().isoformat(),
            "generated_by": current_user.name,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting changelog for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export changelog: {str(e)}"
        )


@router.get("/rosters/{roster_id}/changelog/stats", response_model=ChangeStatsResponse)
async def get_changelog_stats(
    roster_id: str,
    current_user: User = Depends(get_current_user),
    db=Depends(get_db),
) -> ChangeStatsResponse:
    """
    GET /api/v1/rosters/{roster_id}/changelog/stats

    Get statistics about a roster's change history.

    Returns:
    - total_changes: Total number of change entries
    - current_version: Current version number
    - change_types: Count of each change type
    - top_authors: Most active authors (top 5)
    - first_change_timestamp: When changes started
    - last_change_timestamp: When last change occurred
    """
    try:
        # Verify roster exists
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(
                status_code=404,
                detail=f"Roster {roster_id} not found"
            )

        require_role(current_user, [UserRole.owner, UserRole.manager])

        # Get stats
        stats = _changelog_service.get_roster_stats(roster_id)

        return ChangeStatsResponse(
            roster_id=stats["roster_id"],
            total_changes=stats["total_changes"],
            current_version=stats["current_version"],
            change_types=stats.get("change_types", {}),
            top_authors=stats.get("top_authors", []),
            first_change_timestamp=stats.get("first_change_timestamp"),
            last_change_timestamp=stats.get("last_change_timestamp"),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting changelog stats for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get changelog stats: {str(e)}"
        )
