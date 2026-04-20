"""FastAPI router for incident/safety log endpoints (Round 35).

Provides REST API for WHS incident management:
- POST /api/v1/incidents - Report an incident (L1+)
- GET /api/v1/incidents/{venue_id} - List incidents (L1+)
- GET /api/v1/incidents/{venue_id}/{incident_id} - Incident detail (L1+)
- PUT /api/v1/incidents/{incident_id} - Update incident (L2+)
- POST /api/v1/incidents/{incident_id}/actions - Add corrective action (L2+)
- PUT /api/v1/incidents/actions/{action_id}/complete - Complete action (L1+)
- GET /api/v1/incidents/{venue_id}/summary - Incident summary/report (L2+)
- GET /api/v1/incidents/{venue_id}/overdue - Overdue corrective actions (L1+)
"""

from __future__ import annotations

import logging
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.incident_log import (
    get_incident_store,
    Incident,
    IncidentSeverity,
    IncidentCategory,
    IncidentStatus,
    CorrectiveAction,
    IncidentSummary,
    report_incident,
    update_incident,
    add_corrective_action,
    complete_corrective_action,
    check_overdue_actions,
    get_incident_timeline,
    build_incident_summary,
)

logger = logging.getLogger("rosteriq.incident_log_router")

# Auth gating - fall back to no-op in demo/sandbox when auth stack unavailable
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


class ReportIncidentRequest(BaseModel):
    """Request to report a new incident."""
    venue_id: str = Field(..., description="Venue ID")
    reported_by: str = Field(..., description="Employee ID of reporter")
    reported_by_name: str = Field(..., description="Name of reporter")
    date_occurred: str = Field(..., description="ISO datetime when incident occurred")
    location: str = Field(..., description="Location in venue (e.g. kitchen, bar)")
    category: str = Field(..., description="IncidentCategory value")
    severity: str = Field(..., description="IncidentSeverity value")
    description: str = Field(..., description="What happened")
    injured_person: Optional[str] = Field(None, description="Name/ID of injured person")
    injury_description: Optional[str] = Field(None, description="Nature of injury")
    witnesses: Optional[List[str]] = Field(None, description="Witness names/IDs")
    immediate_action: Optional[str] = Field("", description="Immediate response taken")


class UpdateIncidentRequest(BaseModel):
    """Request to update an incident."""
    status: Optional[str] = Field(None, description="IncidentStatus value")
    severity: Optional[str] = Field(None, description="IncidentSeverity value")
    description: Optional[str] = Field(None, description="Updated description")
    immediate_action: Optional[str] = Field(None, description="Updated immediate action")
    injured_person: Optional[str] = Field(None, description="Update injured person")
    injury_description: Optional[str] = Field(None, description="Update injury description")


class AddCorrectiveActionRequest(BaseModel):
    """Request to add a corrective action."""
    description: str = Field(..., description="Action description")
    assigned_to: str = Field(..., description="Responsible person")
    due_date: str = Field(..., description="ISO date YYYY-MM-DD")


class CompleteActionRequest(BaseModel):
    """Request to complete a corrective action."""
    completed_date: Optional[str] = Field(None, description="ISO date (default: today)")


class IncidentResponse(BaseModel):
    """Response model for incident."""
    incident_id: str
    venue_id: str
    reported_by: str
    reported_by_name: str
    date_occurred: str
    date_reported: str
    location: str
    category: str
    severity: str
    description: str
    injured_person: Optional[str]
    injury_description: Optional[str]
    witnesses: List[str]
    immediate_action: str
    status: str
    is_notifiable: bool

    @classmethod
    def from_incident(cls, inc: Incident) -> IncidentResponse:
        return cls(
            incident_id=inc.incident_id,
            venue_id=inc.venue_id,
            reported_by=inc.reported_by,
            reported_by_name=inc.reported_by_name,
            date_occurred=inc.date_occurred.isoformat(),
            date_reported=inc.date_reported.isoformat(),
            location=inc.location,
            category=inc.category.value,
            severity=inc.severity.value,
            description=inc.description,
            injured_person=inc.injured_person,
            injury_description=inc.injury_description,
            witnesses=inc.witnesses,
            immediate_action=inc.immediate_action,
            status=inc.status.value,
            is_notifiable=inc.is_notifiable,
        )


class CorrectiveActionResponse(BaseModel):
    """Response model for corrective action."""
    action_id: str
    incident_id: str
    description: str
    assigned_to: str
    due_date: str
    completed_date: Optional[str]
    status: str

    @classmethod
    def from_action(cls, act: CorrectiveAction) -> CorrectiveActionResponse:
        return cls(
            action_id=act.action_id,
            incident_id=act.incident_id,
            description=act.description,
            assigned_to=act.assigned_to,
            due_date=act.due_date.isoformat(),
            completed_date=act.completed_date.isoformat() if act.completed_date else None,
            status=act.status.value,
        )


class IncidentSummaryResponse(BaseModel):
    """Response model for incident summary."""
    venue_id: str
    period_start: str
    period_end: str
    total_incidents: int
    by_severity: Dict[str, int]
    by_category: Dict[str, int]
    by_location: Dict[str, int]
    by_status: Dict[str, int]
    notifiable_count: int
    open_actions: int
    overdue_actions: int
    incident_rate: Optional[float]

    @classmethod
    def from_summary(cls, summary: IncidentSummary) -> IncidentSummaryResponse:
        return cls(
            venue_id=summary.venue_id,
            period_start=summary.period_start.isoformat(),
            period_end=summary.period_end.isoformat(),
            total_incidents=summary.total_incidents,
            by_severity=summary.by_severity,
            by_category=summary.by_category,
            by_location=summary.by_location,
            by_status=summary.by_status,
            notifiable_count=summary.notifiable_count,
            open_actions=summary.open_actions,
            overdue_actions=summary.overdue_actions,
            incident_rate=summary.incident_rate,
        )


class IncidentTimelineResponse(BaseModel):
    """Response model for incident timeline."""
    incident_id: str
    timeline: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter()


@router.post("/")
async def report_new_incident(
    req: ReportIncidentRequest, request: Request
) -> IncidentResponse:
    """Report a new workplace incident (L1+).

    Validates and stores the incident. Auto-flags as notifiable if severity
    is CRITICAL/NOTIFIABLE or if serious with injury.

    Returns:
        IncidentResponse with auto-assigned incident_id.
    """
    await _gate(request, "L1_SUPERVISOR")

    try:
        category = IncidentCategory(req.category)
        severity = IncidentSeverity(req.severity)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid category/severity: {e}")

    try:
        date_occurred = datetime.fromisoformat(req.date_occurred)
        if date_occurred.tzinfo is None:
            date_occurred = date_occurred.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="date_occurred must be ISO datetime")

    incident = report_incident(
        venue_id=req.venue_id,
        reported_by=req.reported_by,
        reported_by_name=req.reported_by_name,
        date_occurred=date_occurred,
        location=req.location,
        category=category,
        severity=severity,
        description=req.description,
        injured_person=req.injured_person,
        injury_description=req.injury_description,
        witnesses=req.witnesses,
        immediate_action=req.immediate_action or "",
    )

    logger.info("incident reported: %s", incident.incident_id)
    return IncidentResponse.from_incident(incident)


@router.get("/{venue_id}")
async def list_venue_incidents(
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    request: Request = None,
) -> List[IncidentResponse]:
    """List incidents for a venue with optional filters (L1+).

    Query params:
    - date_from, date_to: ISO datetimes
    - severity, category, status: enum values
    """
    await _gate(request, "L1_SUPERVISOR")

    # Parse filters
    date_from_dt = None
    date_to_dt = None
    severity_enum = None
    category_enum = None
    status_enum = None

    if date_from:
        try:
            date_from_dt = datetime.fromisoformat(date_from)
            if date_from_dt.tzinfo is None:
                date_from_dt = date_from_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_from must be ISO datetime")

    if date_to:
        try:
            date_to_dt = datetime.fromisoformat(date_to)
            if date_to_dt.tzinfo is None:
                date_to_dt = date_to_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(status_code=400, detail="date_to must be ISO datetime")

    if severity:
        try:
            severity_enum = IncidentSeverity(severity)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid severity: {severity}")

    if category:
        try:
            category_enum = IncidentCategory(category)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid category: {category}")

    if status:
        try:
            status_enum = IncidentStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    store = get_incident_store()
    incidents = store.list_incidents(
        venue_id,
        date_from=date_from_dt,
        date_to=date_to_dt,
        severity=severity_enum,
        category=category_enum,
        status=status_enum,
    )

    return [IncidentResponse.from_incident(inc) for inc in incidents]


@router.get("/{venue_id}/{incident_id}")
async def get_incident_detail(
    venue_id: str, incident_id: str, request: Request
) -> Dict[str, Any]:
    """Get full incident detail with timeline (L1+)."""
    await _gate(request, "L1_SUPERVISOR")

    store = get_incident_store()
    incident = store.get_incident(incident_id)

    if not incident or incident.venue_id != venue_id:
        raise HTTPException(status_code=404, detail="Incident not found")

    timeline = get_incident_timeline(incident_id)

    return {
        "incident": IncidentResponse.from_incident(incident),
        "timeline": timeline,
    }


@router.put("/{incident_id}")
async def update_incident_endpoint(
    incident_id: str, req: UpdateIncidentRequest, request: Request
) -> IncidentResponse:
    """Update an incident's details (L2+)."""
    await _gate(request, "L2_ROSTER_MAKER")

    # Build updates dict, skipping None values
    updates = {k: v for k, v in req.dict().items() if v is not None}

    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    incident = update_incident(incident_id, **updates)

    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    logger.info("incident updated: %s", incident_id)
    return IncidentResponse.from_incident(incident)


@router.post("/{incident_id}/actions")
async def add_action_to_incident(
    incident_id: str, req: AddCorrectiveActionRequest, request: Request
) -> CorrectiveActionResponse:
    """Add a corrective action to an incident (L2+)."""
    await _gate(request, "L2_ROSTER_MAKER")

    # Verify incident exists
    store = get_incident_store()
    incident = store.get_incident(incident_id)
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    try:
        due_date = date.fromisoformat(req.due_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="due_date must be ISO date YYYY-MM-DD")

    action = add_corrective_action(
        incident_id=incident_id,
        description=req.description,
        assigned_to=req.assigned_to,
        due_date=due_date,
    )

    logger.info("corrective action added: %s", action.action_id)
    return CorrectiveActionResponse.from_action(action)


@router.put("/actions/{action_id}/complete")
async def complete_action_endpoint(
    action_id: str, req: CompleteActionRequest, request: Request
) -> CorrectiveActionResponse:
    """Complete a corrective action (L1+)."""
    await _gate(request, "L1_SUPERVISOR")

    completed_date = None
    if req.completed_date:
        try:
            completed_date = date.fromisoformat(req.completed_date)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="completed_date must be ISO date YYYY-MM-DD"
            )

    action = complete_corrective_action(action_id, completed_date)

    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    logger.info("corrective action completed: %s", action_id)
    return CorrectiveActionResponse.from_action(action)


@router.get("/{venue_id}/summary")
async def get_venue_summary(
    venue_id: str,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    hours_worked: Optional[float] = None,
    request: Request = None,
) -> IncidentSummaryResponse:
    """Get aggregated incident summary for a venue (L2+).

    Query params:
    - period_start, period_end: ISO dates (default: last 30 days)
    - hours_worked: Total hours for incident_rate calc (optional)
    """
    await _gate(request, "L2_ROSTER_MAKER")

    period_start_d = None
    period_end_d = None

    if period_start:
        try:
            period_start_d = date.fromisoformat(period_start)
        except ValueError:
            raise HTTPException(status_code=400, detail="period_start must be ISO date")

    if period_end:
        try:
            period_end_d = date.fromisoformat(period_end)
        except ValueError:
            raise HTTPException(status_code=400, detail="period_end must be ISO date")

    summary = build_incident_summary(
        venue_id,
        period_start=period_start_d,
        period_end=period_end_d,
        hours_worked=hours_worked,
    )

    return IncidentSummaryResponse.from_summary(summary)


@router.get("/{venue_id}/overdue")
async def get_overdue_actions(venue_id: str, request: Request) -> List[CorrectiveActionResponse]:
    """Get all overdue corrective actions for a venue (L1+)."""
    await _gate(request, "L1_SUPERVISOR")

    actions = check_overdue_actions(venue_id)

    return [CorrectiveActionResponse.from_action(act) for act in actions]
