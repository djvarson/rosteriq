"""
Bulk roster generation routes for RosterIQ.

REST API endpoints for generating rosters across multiple venues.

Endpoints:
- POST /api/bulk/generate-rosters — generate rosters for multiple venues, one
  after another through POST /rosters/generate
- POST /api/bulk/apply-templates — apply different templates to multiple venues
"""

import asyncio
import logging
from datetime import date
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.roster_templates import RosterTemplateService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["bulk"])


# ============================================================================
# Request/Response Models
# ============================================================================


class BulkGenerateRostersRequest(BaseModel):
    """Request to generate rosters for multiple venues."""

    venue_ids: List[str]
    week_start: str  # ISO date format "YYYY-MM-DD"
    options: Optional[Dict[str, Any]] = None  # Future: covers_per_staff, etc.


class GenerateRosterResult(BaseModel):
    """Result for a single venue's roster generation."""

    venue_id: str
    roster_id: Optional[str] = None
    status: str  # "success", "error", "skipped"
    error: Optional[str] = None
    shift_count: Optional[int] = None
    # What POST /rosters/generate reports out loud: recorded visa caps the week
    # goes over, lapsed or lapsing recorded visas, other-venue bookings.
    warnings: List[str] = []


class BulkGenerateRostersResponse(BaseModel):
    """Response from bulk roster generation."""

    request_timestamp: str
    results: List[GenerateRosterResult]
    total_venues: int
    successful: int
    failed: int
    skipped: int


class TemplateApplication(BaseModel):
    """Application of a template to a venue and week."""

    template_id: str
    venue_id: str
    week_start: str  # ISO date format "YYYY-MM-DD"


class BulkApplyTemplatesRequest(BaseModel):
    """Request to apply templates to multiple venues."""

    template_applications: List[TemplateApplication]


class ApplyTemplateResult(BaseModel):
    """Result for applying a template."""

    template_id: str
    venue_id: str
    week_start: str
    roster_id: Optional[str] = None
    status: str  # "success", "error", "skipped"
    error: Optional[str] = None
    shift_count: Optional[int] = None


class BulkApplyTemplatesResponse(BaseModel):
    """Response from bulk template application."""

    request_timestamp: str
    results: List[ApplyTemplateResult]
    total_applications: int
    successful: int
    failed: int
    skipped: int


# ============================================================================
# Routes
# ============================================================================


@router.post("/api/bulk/generate-rosters", response_model=BulkGenerateRostersResponse)
async def bulk_generate_rosters(req: BulkGenerateRostersRequest):
    """
    Generate rosters for multiple venues.

    Each venue goes through POST /rosters/generate (approved leave, shared
    staff's bookings at the other venues, last week, older drafts
    superseded), one after another so a later venue sees the drafts made
    for the earlier ones.

    Caps at 10 venues per request to avoid resource exhaustion.
    Returns results per venue including status and any errors.

    Args:
        venue_ids: List of venue IDs to generate rosters for
        week_start: ISO date string for the week start
        options: Optional configuration (reserved for future use)

    Returns:
        BulkGenerateRostersResponse with per-venue results
    """
    for v in req.venue_ids:
        enforce_venue_manager(v)
    try:
        # Validate request
        if len(req.venue_ids) > 10:
            raise HTTPException(
                status_code=400,
                detail="Maximum 10 venues per request",
            )

        if not req.venue_ids:
            raise HTTPException(
                status_code=400,
                detail="Must specify at least one venue_id",
            )

        # Parse week start date
        try:
            week_start = date.fromisoformat(req.week_start)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid week_start format. Use ISO date: YYYY-MM-DD",
            )

        results = []
        for venue_id in req.venue_ids:
            try:
                results.append(await _generate_roster_for_venue(venue_id, week_start))
            except Exception as e:
                results.append(e)

        # Process results
        final_results = []
        successful = 0
        failed = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append(
                    GenerateRosterResult(
                        venue_id=req.venue_ids[i],
                        status="error",
                        error=str(result),
                    )
                )
                failed += 1
            else:
                final_results.append(result)
                if result.status == "success":
                    successful += 1
                elif result.status == "error":
                    failed += 1

        return BulkGenerateRostersResponse(
            request_timestamp=date.today().isoformat(),
            results=final_results,
            total_venues=len(req.venue_ids),
            successful=successful,
            failed=failed,
            skipped=len(req.venue_ids) - successful - failed,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in bulk roster generation")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/bulk/apply-templates", response_model=BulkApplyTemplatesResponse)
async def bulk_apply_templates(req: BulkApplyTemplatesRequest):
    """
    Apply different templates to different venues in parallel.

    Allows applying multiple templates across multiple venues in a single request.
    Caps at 20 template applications per request.

    Args:
        template_applications: List of template-venue-week combinations to apply

    Returns:
        BulkApplyTemplatesResponse with per-application results
    """
    for app in req.template_applications:
        enforce_venue_manager(app.venue_id)
    try:
        # Validate request
        if len(req.template_applications) > 20:
            raise HTTPException(
                status_code=400,
                detail="Maximum 20 template applications per request",
            )

        if not req.template_applications:
            raise HTTPException(
                status_code=400,
                detail="Must specify at least one template application",
            )

        # Prepare tasks
        db = get_db()
        tasks = [
            _apply_template_for_venue(app.template_id, app.venue_id, app.week_start, db)
            for app in req.template_applications
        ]

        # Execute in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        final_results = []
        successful = 0
        failed = 0

        for i, result in enumerate(results):
            app = req.template_applications[i]
            if isinstance(result, Exception):
                final_results.append(
                    ApplyTemplateResult(
                        template_id=app.template_id,
                        venue_id=app.venue_id,
                        week_start=app.week_start,
                        status="error",
                        error=str(result),
                    )
                )
                failed += 1
            else:
                final_results.append(result)
                if result.status == "success":
                    successful += 1
                elif result.status == "error":
                    failed += 1

        return BulkApplyTemplatesResponse(
            request_timestamp=date.today().isoformat(),
            results=final_results,
            total_applications=len(req.template_applications),
            successful=successful,
            failed=failed,
            skipped=len(req.template_applications) - successful - failed,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in bulk template application")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Helper functions
# ============================================================================


async def _generate_roster_for_venue(venue_id: str, week_start: date) -> GenerateRosterResult:
    """Generate one venue's roster through POST /rosters/generate."""
    from rosteriq.api import GenerateRosterRequest, generate_roster
    try:
        out = await generate_roster(GenerateRosterRequest(venue_id=venue_id, week_start=week_start))
        if not isinstance(out, dict):
            out = {"id": out.id, "shifts": out.shifts}
        warnings = [f["message"] for f in out.get("visa_flags") or []]
        for a in out.get("visa_expiry_alerts") or []:
            if a.get("kind") == "expired":
                warnings.append(f"{a['name']}'s recorded visa expired {a['expiry']} "
                                "— confirm their work rights before publishing")
            else:
                warnings.append(f"{a['name']}'s recorded visa expires {a['expiry']} — re-check VEVO")
        warnings += [f"Kept off while {m}" for m in out.get("booked_elsewhere") or []]
        return GenerateRosterResult(
            venue_id=venue_id,
            roster_id=out["id"],
            status="success",
            shift_count=len(out["shifts"]),
            warnings=warnings,
        )

    except HTTPException as e:
        return GenerateRosterResult(venue_id=venue_id, status="error", error=str(e.detail))
    except Exception as e:
        logger.exception(f"Error generating roster for venue {venue_id}")
        return GenerateRosterResult(
            venue_id=venue_id,
            status="error",
            error=str(e),
        )


async def _apply_template_for_venue(
    template_id: str, venue_id: str, week_start_str: str, db
) -> ApplyTemplateResult:
    """Apply a template for a single venue in an async context."""
    try:
        # Parse week start
        try:
            week_start = date.fromisoformat(week_start_str)
        except ValueError:
            return ApplyTemplateResult(
                template_id=template_id,
                venue_id=venue_id,
                week_start=week_start_str,
                status="error",
                error="Invalid week_start format. Use ISO date: YYYY-MM-DD",
            )

        # Get template
        service = RosterTemplateService(db)
        template = service.get_template(template_id)

        if not template:
            return ApplyTemplateResult(
                template_id=template_id,
                venue_id=venue_id,
                week_start=week_start_str,
                status="error",
                error=f"Template {template_id} not found",
            )

        if template.venue_id != venue_id:
            return ApplyTemplateResult(
                template_id=template_id,
                venue_id=venue_id,
                week_start=week_start_str,
                status="error",
                error=f"Template venue {template.venue_id} does not match {venue_id}",
            )

        # Apply template
        roster = service.apply_template(
            template_id=template_id,
            week_start_date=week_start,
            venue_id=venue_id,
        )

        if not roster:
            return ApplyTemplateResult(
                template_id=template_id,
                venue_id=venue_id,
                week_start=week_start_str,
                status="error",
                error="Failed to apply template",
            )

        return ApplyTemplateResult(
            template_id=template_id,
            venue_id=venue_id,
            week_start=week_start_str,
            roster_id=roster.id,
            status="success",
            shift_count=len(roster.shifts),
        )

    except Exception as e:
        logger.exception(f"Error applying template {template_id} to venue {venue_id}")
        return ApplyTemplateResult(
            template_id=template_id,
            venue_id=venue_id,
            week_start=week_start_str,
            status="error",
            error=str(e),
        )
