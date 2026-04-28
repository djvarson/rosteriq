"""
Roster template routes for RosterIQ.

REST API endpoints for creating, managing, and applying roster templates.

Endpoints:
- POST /api/templates — create template
- POST /api/templates/from-roster — create template from existing roster
- GET /api/templates?venue_id=... — list templates for venue
- GET /api/templates/{id} — get single template
- POST /api/templates/{id}/apply — apply template to generate roster
- DELETE /api/templates/{id} — delete template
- POST /api/templates/{id}/duplicate — duplicate template
"""

import logging
from datetime import date, datetime, time
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from rosteriq.database import get_db
from rosteriq.services.roster_templates import (
    RosterTemplateService, RosterTemplate, ShiftPattern,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["templates"])


# ============================================================================
# Pydantic request/response models
# ============================================================================


class ShiftPatternRequest(BaseModel):
    """Request model for creating a shift pattern."""

    role: str
    day_of_week: int  # 0=Monday, 6=Sunday
    start_time: str  # ISO format "HH:MM:SS"
    end_time: str    # ISO format "HH:MM:SS"
    employee_count: int
    skills_required: List[str] = []

    def to_shift_pattern(self) -> ShiftPattern:
        """Convert to ShiftPattern."""
        return ShiftPattern(
            role=self.role,
            day_of_week=self.day_of_week,
            start_time=time.fromisoformat(self.start_time),
            end_time=time.fromisoformat(self.end_time),
            employee_count=self.employee_count,
            skills_required=self.skills_required,
        )


class ShiftPatternResponse(BaseModel):
    """Response model for shift pattern."""

    role: str
    day_of_week: int
    start_time: str
    end_time: str
    employee_count: int
    skills_required: List[str]


class RosterTemplateRequest(BaseModel):
    """Request model for creating a roster template."""

    name: str
    venue_id: str
    description: str = ""
    shift_patterns: List[ShiftPatternRequest]


class RosterTemplateResponse(BaseModel):
    """Response model for roster template."""

    id: str
    name: str
    venue_id: str
    description: str
    created_by: str
    created_at: str
    updated_at: Optional[str]
    shift_patterns: List[ShiftPatternResponse]


class CreateFromRosterRequest(BaseModel):
    """Request to create template from existing roster."""

    roster_id: str
    name: str
    description: str = ""


class ApplyTemplateRequest(BaseModel):
    """Request to apply template and generate roster."""

    week_start: str  # ISO date format "YYYY-MM-DD"


class ApplyTemplateResponse(BaseModel):
    """Response from applying template."""

    roster_id: str
    venue_id: str
    week_start: str
    week_end: str
    shift_count: int
    status: str


class DuplicateTemplateRequest(BaseModel):
    """Request to duplicate a template."""

    new_name: str


# ============================================================================
# Routes
# ============================================================================


@router.post("/api/templates", response_model=RosterTemplateResponse, status_code=201)
async def create_template(req: RosterTemplateRequest):
    """
    Create a new roster template.

    Returns the created template with all details.
    """
    try:
        db = get_db()
        service = RosterTemplateService(db)

        # Convert shift patterns
        patterns = [p.to_shift_pattern() for p in req.shift_patterns]

        # Create template
        template = service.create_template(
            name=req.name,
            venue_id=req.venue_id,
            shift_patterns=patterns,
            description=req.description,
            created_by="api-user",
        )

        return _template_to_response(template)
    except Exception as e:
        logger.exception("Error creating template")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/api/templates/from-roster",
    response_model=RosterTemplateResponse,
    status_code=201,
)
async def create_template_from_roster(req: CreateFromRosterRequest):
    """
    Create a template by extracting patterns from an existing roster.

    Analyzes the roster and creates a reusable template based on its shift patterns.
    """
    try:
        db = get_db()
        service = RosterTemplateService(db)

        template = service.create_from_roster(
            roster_id=req.roster_id,
            name=req.name,
            description=req.description,
            created_by="api-user",
        )

        if not template:
            raise HTTPException(status_code=404, detail=f"Roster {req.roster_id} not found")

        return _template_to_response(template)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error creating template from roster")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/templates", response_model=List[RosterTemplateResponse])
async def list_templates(venue_id: str = Query(...)):
    """List all templates for a specific venue."""
    try:
        db = get_db()
        service = RosterTemplateService(db)
        templates = service.list_templates(venue_id)
        return [_template_to_response(t) for t in templates]
    except Exception as e:
        logger.exception("Error listing templates")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/templates/{template_id}", response_model=RosterTemplateResponse)
async def get_template(template_id: str):
    """Get a single template by ID."""
    try:
        db = get_db()
        service = RosterTemplateService(db)
        template = service.get_template(template_id)

        if not template:
            raise HTTPException(status_code=404, detail=f"Template {template_id} not found")

        return _template_to_response(template)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting template")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/templates/{template_id}/apply", response_model=ApplyTemplateResponse)
async def apply_template(template_id: str, req: ApplyTemplateRequest):
    """
    Apply a template to generate a roster for a specific week.

    The template defines the shift patterns, which are scheduled for the
    corresponding day in the requested week.
    """
    try:
        db = get_db()
        service = RosterTemplateService(db)

        # Get template to extract venue_id
        template = service.get_template(template_id)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template {template_id} not found")

        # Parse week start date
        week_start = date.fromisoformat(req.week_start)

        # Apply template
        roster = service.apply_template(
            template_id=template_id,
            week_start_date=week_start,
            venue_id=template.venue_id,
        )

        if not roster:
            raise HTTPException(
                status_code=400,
                detail="Failed to apply template to generate roster",
            )

        return ApplyTemplateResponse(
            roster_id=roster.id,
            venue_id=roster.venue_id,
            week_start=roster.week_start.isoformat(),
            week_end=roster.week_end.isoformat(),
            shift_count=len(roster.shifts),
            status="generated",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error applying template")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/templates/{template_id}", status_code=204)
async def delete_template(template_id: str):
    """Delete a template by ID."""
    try:
        db = get_db()
        service = RosterTemplateService(db)
        service.delete_template(template_id)
        return None
    except Exception as e:
        logger.exception("Error deleting template")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/templates/{template_id}/duplicate", response_model=RosterTemplateResponse)
async def duplicate_template(template_id: str, req: DuplicateTemplateRequest):
    """Create a copy of an existing template with a new name."""
    try:
        db = get_db()
        service = RosterTemplateService(db)

        new_template = service.duplicate_template(
            template_id=template_id,
            new_name=req.new_name,
        )

        if not new_template:
            raise HTTPException(
                status_code=404,
                detail=f"Template {template_id} not found",
            )

        return _template_to_response(new_template)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error duplicating template")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Helper functions
# ============================================================================


def _template_to_response(template: RosterTemplate) -> RosterTemplateResponse:
    """Convert RosterTemplate to response model."""
    patterns = [
        ShiftPatternResponse(
            role=p.role,
            day_of_week=p.day_of_week,
            start_time=p.start_time.isoformat(),
            end_time=p.end_time.isoformat(),
            employee_count=p.employee_count,
            skills_required=p.skills_required,
        )
        for p in template.shift_patterns
    ]

    return RosterTemplateResponse(
        id=template.id,
        name=template.name,
        venue_id=template.venue_id,
        description=template.description,
        created_by=template.created_by,
        created_at=template.created_at.isoformat(),
        updated_at=template.updated_at.isoformat() if template.updated_at else None,
        shift_patterns=patterns,
    )
