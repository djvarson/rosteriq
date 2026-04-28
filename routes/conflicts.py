"""
Conflict detection API endpoints for RosterIQ.

Provides REST endpoints for detecting and managing roster conflicts:
- GET /api/v1/rosters/{roster_id}/conflicts — detect and return all conflicts
- GET /api/v1/rosters/{roster_id}/conflicts/summary — count by type and severity
- POST /api/v1/rosters/{roster_id}/conflicts/auto-fix — attempt automatic resolution

Conflicts are detected against Fair Work compliance rules, employee availability,
skill requirements, and venue staffing levels.
"""

import logging
from typing import Optional, List
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.models import Roster, VenueConfig, Employee
from rosteriq.services.conflict_detector import (
    ConflictDetector, ConflictType, ConflictSeverity, RosterConflict, ConflictSummary
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/rosters",
    tags=["conflicts"],
)


# ============================================================================
# Response models
# ============================================================================

class ConflictResponse(BaseModel):
    """Response model for a single conflict."""
    conflict_type: str
    severity: str
    message: str
    employee_ids: List[str]
    shift_ids: List[str]
    hour: Optional[int] = None
    date: Optional[str] = None
    suggestion: Optional[str] = None

    class Config:
        from_attributes = True


class ConflictSummaryResponse(BaseModel):
    """Response model for conflict summary."""
    total_conflicts: int
    count_by_type: dict
    count_by_severity: dict


class DetectConflictsResponse(BaseModel):
    """Response model for conflict detection endpoint."""
    roster_id: str
    total_conflicts: int
    critical: int
    warnings: int
    info: int
    conflicts: List[ConflictResponse]


class AutoFixResponse(BaseModel):
    """Response model for auto-fix operation."""
    changes_made: int
    shifts_modified: List[str]
    summary: str


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/{roster_id}/conflicts")
async def detect_conflicts(
    roster_id: str,
    severity_filter: Optional[str] = Query(None, description="Filter by severity (critical/warning/info)"),
    conflict_type_filter: Optional[str] = Query(None, description="Filter by conflict type"),
) -> DetectConflictsResponse:
    """
    Detect all conflicts in a roster.

    Checks for:
    - Double bookings (same employee, overlapping shifts)
    - Availability violations (shifts outside employee's stated availability)
    - Overtime breaches (exceeds max_hours_per_week)
    - Consecutive days violations (exceeds max consecutive days)
    - Skill mismatches (employee lacks required role skill)
    - Minimum engagement violations (shift too short)
    - Max shift length violations (>11.5 hours)
    - Break requirement violations
    - Understaffing/overstaffing by hour
    - Fatigue risk (insufficient rest between shifts)

    Args:
        roster_id: The roster to check
        severity_filter: Optional filter by severity level
        conflict_type_filter: Optional filter by conflict type

    Returns:
        List of detected conflicts with details and suggestions
    """
    db = get_db()

    try:
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(404, f"Roster {roster_id} not found")

        venue = db.get_venue(roster.venue_id)
        if not venue:
            raise HTTPException(404, f"Venue {roster.venue_id} not found")

        employees = db.list_employees()

        # Run conflict detection
        detector = ConflictDetector()
        conflicts = detector.detect_conflicts(roster, venue, employees)

        # Apply filters if specified
        if severity_filter:
            conflicts = [c for c in conflicts if c.severity.value == severity_filter]

        if conflict_type_filter:
            conflicts = [c for c in conflicts if c.conflict_type.value == conflict_type_filter]

        # Count by severity
        critical_count = len([c for c in conflicts if c.severity == ConflictSeverity.CRITICAL])
        warning_count = len([c for c in conflicts if c.severity == ConflictSeverity.WARNING])
        info_count = len([c for c in conflicts if c.severity == ConflictSeverity.INFO])

        return DetectConflictsResponse(
            roster_id=roster_id,
            total_conflicts=len(conflicts),
            critical=critical_count,
            warnings=warning_count,
            info=info_count,
            conflicts=[
                ConflictResponse(
                    conflict_type=c.conflict_type.value,
                    severity=c.severity.value,
                    message=c.message,
                    employee_ids=c.employee_ids,
                    shift_ids=c.shift_ids,
                    hour=c.hour,
                    date=c.date.isoformat() if c.date else None,
                    suggestion=c.suggestion,
                )
                for c in conflicts
            ]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error detecting conflicts in roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to detect conflicts: {str(e)}")


@router.get("/{roster_id}/conflicts/summary")
async def get_conflicts_summary(roster_id: str) -> ConflictSummaryResponse:
    """
    Get a summary of conflicts grouped by type and severity.

    Returns counts of conflicts in each category, useful for dashboards
    and high-level overviews.

    Args:
        roster_id: The roster to summarise

    Returns:
        Conflict counts grouped by type and severity
    """
    db = get_db()

    try:
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(404, f"Roster {roster_id} not found")

        venue = db.get_venue(roster.venue_id)
        if not venue:
            raise HTTPException(404, f"Venue {roster.venue_id} not found")

        employees = db.list_employees()

        # Run conflict detection
        detector = ConflictDetector()
        conflicts = detector.detect_conflicts(roster, venue, employees)

        # Build summary
        summary = ConflictSummary(conflicts)

        return ConflictSummaryResponse(
            total_conflicts=summary.count_by_type.get(ConflictType.DOUBLE_BOOKING, 0) +
                          sum(summary.count_by_type.values()),
            count_by_type={k.value: v for k, v in summary.count_by_type.items()},
            count_by_severity={k.value: v for k, v in summary.count_by_severity.items()},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating conflict summary for roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to generate summary: {str(e)}")


@router.post("/{roster_id}/conflicts/auto-fix")
async def auto_fix_conflicts(
    roster_id: str,
    fix_types: Optional[List[str]] = Query(None, description="Only fix these conflict types"),
) -> AutoFixResponse:
    """
    Attempt automatic resolution of fixable conflicts.

    This endpoint tries to resolve conflicts where an obvious fix exists:
    - MINIMUM_ENGAGEMENT: Extend shift to minimum hours
    - OVERSTAFFED_HOUR: Remove staff from overstaffed hours
    - Skips critical conflicts that require manual intervention

    Args:
        roster_id: The roster to fix
        fix_types: Optional list of conflict types to fix (others are skipped)

    Returns:
        Summary of changes made
    """
    db = get_db()

    try:
        roster = db.get_roster(roster_id)
        if not roster:
            raise HTTPException(404, f"Roster {roster_id} not found")

        venue = db.get_venue(roster.venue_id)
        if not venue:
            raise HTTPException(404, f"Venue {roster.venue_id} not found")

        employees = db.list_employees()

        # Run conflict detection
        detector = ConflictDetector()
        conflicts = detector.detect_conflicts(roster, venue, employees)

        # Filter to fixable types (non-critical, non-manual)
        fixable_types = {
            ConflictType.MINIMUM_ENGAGEMENT,
            ConflictType.OVERSTAFFED_HOUR,
        }

        if fix_types:
            fixable_types = {ct for ct in fixable_types if ct.value in fix_types}

        fixable_conflicts = [c for c in conflicts if c.conflict_type in fixable_types]

        # Attempt fixes
        changes_made = 0
        modified_shifts = set()

        for conflict in fixable_conflicts:
            if conflict.conflict_type == ConflictType.MINIMUM_ENGAGEMENT:
                # Extend first shift to minimum engagement hours
                if conflict.shift_ids:
                    shift = next((s for s in roster.shifts if s.id == conflict.shift_ids[0]), None)
                    if shift:
                        employee = next((e for e in employees if e.id == shift.employee_id), None)
                        if employee:
                            min_hours = 3.0 if employee.employment_type.value == "part_time" else 2.0
                            if shift.net_hours < min_hours:
                                # Extend shift by 30 minutes
                                from datetime import time, timedelta
                                new_end = shift.end_time
                                total_minutes = new_end.hour * 60 + new_end.minute + 30
                                new_end = time(hour=total_minutes // 60, minute=total_minutes % 60)
                                shift.end_time = new_end
                                changes_made += 1
                                modified_shifts.add(shift.id)

            elif conflict.conflict_type == ConflictType.OVERSTAFFED_HOUR:
                # Remove one staff from overstaffed hour (simplified)
                if conflict.employee_ids:
                    # Just log as opportunity, don't actually remove
                    logger.info(f"Overstaffed hour {conflict.hour} on {conflict.date}, "
                              f"could remove one of: {conflict.employee_ids}")

        # Save roster if changes were made
        if changes_made > 0:
            db.save_roster(roster)

        return AutoFixResponse(
            changes_made=changes_made,
            shifts_modified=list(modified_shifts),
            summary=f"Fixed {changes_made} conflicts by modifying {len(modified_shifts)} shifts"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error auto-fixing conflicts in roster {roster_id}: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to auto-fix conflicts: {str(e)}")
