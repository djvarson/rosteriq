"""
API routes for employee onboarding checklists with document tracking.

Endpoints:
- POST /api/v1/employees/{id}/onboarding — create onboarding checklist
- GET /api/v1/employees/{id}/onboarding — get checklist status
- PUT /api/v1/employees/{id}/onboarding/{item_id} — update item
- GET /api/v1/employees/{id}/onboarding/ready — check if rostering-ready
- POST /api/v1/employees/{id}/onboarding/remind — send reminders
- GET /api/v1/venues/{id}/onboarding-template — get venue template
- PUT /api/v1/venues/{id}/onboarding-template — update venue template
- GET /api/v1/venues/{id}/onboarding-status — venue-wide overview
"""

import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from rosteriq.services.employee_onboarding import (
    get_onboarding_service,
    OnboardingItemStatus,
    OnboardingItemCategory,
)
from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager, get_tenant_context_optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["employee-onboarding"])


# ============================================================================
# Tenant scoping. Every route here takes an employee_id / venue_id straight
# from the URL, so each one must prove the caller may act on that venue
# BEFORE touching the service (whose checklists span all venues). Denials
# are 404 — the same answer as "no such employee" — so ids are not an oracle.
# NOTE: these checks sit OUTSIDE the handlers' try/except so the broad
# ``except Exception -> 500`` cannot swallow the HTTPException.
# ============================================================================

def _deny_as_404(what: str):
    return HTTPException(status_code=404, detail=f"{what} not found")


def _scope_venue(venue_id: str) -> None:
    """403→404 for a venue outside the caller's scope."""
    try:
        enforce_venue_access(venue_id)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise _deny_as_404("Venue")
        raise


def _scope_employee(employee_id: str, venue_id: Optional[str] = None) -> Optional[str]:
    """
    Resolve the venue an employee belongs to and enforce access to it.
    Order: the employee record's venue (authoritative) → an existing checklist's
    venue → the caller-supplied venue_id. Returns the venue id that was proven,
    or None when nothing is known yet (a non-owner then can only proceed for
    a venue they hold — checked by the caller). Denials are 404.
    """
    emp = None
    try:
        emp = get_db().get_employee(employee_id)
    except Exception:
        emp = None
    emp_venue = getattr(emp, "venue_id", None) if emp is not None else None
    if emp_venue:
        if venue_id and venue_id != emp_venue:
            # Asking about the employee under a venue they don't belong to
            raise _deny_as_404("Employee")
        _scope_employee_venue(emp_venue)
        return emp_venue
    checklist = get_onboarding_service().get_onboarding(employee_id, venue_id)
    if checklist is not None:
        _scope_employee_venue(checklist.venue_id)
        return checklist.venue_id
    if venue_id:
        _scope_employee_venue(venue_id)
        return venue_id
    tenant = get_tenant_context_optional()
    if tenant is not None and not tenant.is_owner:
        # Unknown employee, no venue given: nothing a non-owner may see
        raise _deny_as_404("Employee")
    return None


def _scope_employee_venue(venue_id: str) -> None:
    try:
        enforce_venue_access(venue_id)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise _deny_as_404("Employee")
        raise


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateOnboardingRequest(BaseModel):
    """Request to create an onboarding checklist."""
    employee_id: str
    venue_id: str
    role: str


class OnboardingItemResponse(BaseModel):
    """Response model for a single checklist item."""
    id: str
    name: str
    description: str
    category: str
    is_mandatory: bool
    status: str
    completed_at: Optional[str] = None
    document_url: Optional[str] = None
    notes: Optional[str] = None
    reminded_at: Optional[str] = None


class OnboardingChecklistResponse(BaseModel):
    """Response model for complete checklist."""
    employee_id: str
    venue_id: str
    role: str
    started_at: str
    completed_at: Optional[str] = None
    items: List[OnboardingItemResponse]
    completion_pct: float
    is_rostering_ready: bool


class UpdateItemRequest(BaseModel):
    """Request to update a checklist item."""
    status: str  # pending, in_progress, completed, waived
    notes: Optional[str] = None
    document_url: Optional[str] = None


class RosteringReadyResponse(BaseModel):
    """Response for rostering readiness check."""
    is_ready: bool
    missing_items: List[str]
    completion_pct: float


class SendReminderRequest(BaseModel):
    """Request to send reminders."""
    item_ids: Optional[List[str]] = None  # None = all incomplete


class SendReminderResponse(BaseModel):
    """Response after sending reminders."""
    reminders_sent: int


class OnboardingItemTemplateResponse(BaseModel):
    """Response model for template item."""
    id: str
    name: str
    description: str
    category: str
    is_mandatory: bool
    required_for_roles: List[str]


class VenueTemplateResponse(BaseModel):
    """Response model for venue template."""
    venue_id: str
    items: List[OnboardingItemTemplateResponse]


class UpdateVenueTemplateRequest(BaseModel):
    """Request to update venue template."""
    items: List[dict]  # {name, description, category, is_mandatory, required_for_roles}


class VenueOnboardingStatusItemResponse(BaseModel):
    """Single item in most_common_pending_items list."""
    name: str
    count: int


class VenueOnboardingStatusResponse(BaseModel):
    """Response model for venue-wide onboarding status."""
    venue_id: str
    total_employees_onboarding: int
    completed: int
    in_progress: int
    pending: int
    completion_rate: float
    average_days_to_complete: float
    most_common_pending_items: List[VenueOnboardingStatusItemResponse]


class AutoRemindResponse(BaseModel):
    """Response from auto-remind operation."""
    reminders_sent: int
    employees_reminded: int


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/employees/{employee_id}/onboarding", response_model=OnboardingChecklistResponse)
async def create_onboarding(employee_id: str, req: CreateOnboardingRequest):
    """
    Create a new onboarding checklist for an employee.

    Auto-selects mandatory items based on their role:
    - All: TAX_FILE, SUPER_CHOICE, BANK_DETAILS, WORKING_RIGHTS,
      EMERGENCY_CONTACT, FIRE_SAFETY, WHS_INDUCTION, POLICY_ACKNOWLEDGEMENT
    - Bar/Floor/Manager: RSA_CERTIFICATE
    - Kitchen/Chef/Cook: FOOD_SAFETY
    - Optional for all: UNIFORM_ISSUED, VENUE_TOUR, POS_TRAINING

    Args:
        employee_id: Path parameter (must match req.employee_id)
        req: CreateOnboardingRequest with venue_id and role

    Returns:
        OnboardingChecklistResponse with items and 0% completion
    """
    if employee_id != req.employee_id:
        raise HTTPException(
            status_code=400,
            detail="employee_id in path does not match request body",
        )
    # Caller must hold the target venue, and if the employee already exists
    # they must belong to that venue (no cross-venue checklist injection).
    _scope_venue(req.venue_id)
    _scope_employee(employee_id, req.venue_id)
    enforce_venue_manager(req.venue_id)  # HR onboarding record = manager

    try:
        service = get_onboarding_service()
        checklist = service.create_onboarding(req.employee_id, req.venue_id, req.role)

        return OnboardingChecklistResponse(
            employee_id=checklist.employee_id,
            venue_id=checklist.venue_id,
            role=checklist.role,
            started_at=checklist.started_at,
            completed_at=checklist.completed_at,
            items=[
                OnboardingItemResponse(
                    id=item.id,
                    name=item.name,
                    description=item.description,
                    category=item.category,
                    is_mandatory=item.is_mandatory,
                    status=item.status,
                    completed_at=item.completed_at,
                    document_url=item.document_url,
                    notes=item.notes,
                    reminded_at=item.reminded_at,
                )
                for item in checklist.items
            ],
            completion_pct=checklist.completion_pct,
            is_rostering_ready=checklist.is_rostering_ready,
        )
    except Exception as e:
        logger.error(f"Error creating onboarding for {employee_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/employees/{employee_id}/onboarding", response_model=OnboardingChecklistResponse)
async def get_onboarding(employee_id: str, venue_id: Optional[str] = None):
    """
    Get the current onboarding checklist for an employee.

    Args:
        employee_id: Employee ID
        venue_id: Venue ID (optional, required if employee has multiple onboardings)

    Returns:
        OnboardingChecklistResponse with current status
    """
    venue_id = _scope_employee(employee_id, venue_id) or venue_id
    try:
        service = get_onboarding_service()
        checklist = service.get_onboarding(employee_id, venue_id)

        if not checklist:
            raise HTTPException(
                status_code=404,
                detail=f"Onboarding checklist not found for employee {employee_id}",
            )

        return OnboardingChecklistResponse(
            employee_id=checklist.employee_id,
            venue_id=checklist.venue_id,
            role=checklist.role,
            started_at=checklist.started_at,
            completed_at=checklist.completed_at,
            items=[
                OnboardingItemResponse(
                    id=item.id,
                    name=item.name,
                    description=item.description,
                    category=item.category,
                    is_mandatory=item.is_mandatory,
                    status=item.status,
                    completed_at=item.completed_at,
                    document_url=item.document_url,
                    notes=item.notes,
                    reminded_at=item.reminded_at,
                )
                for item in checklist.items
            ],
            completion_pct=checklist.completion_pct,
            is_rostering_ready=checklist.is_rostering_ready,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving onboarding for {employee_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/employees/{employee_id}/onboarding/{item_id}", response_model=OnboardingItemResponse)
async def update_item(
    employee_id: str,
    item_id: str,
    req: UpdateItemRequest,
    venue_id: Optional[str] = None,
):
    """
    Update the status of a checklist item.

    Supports:
    - status: pending, in_progress, completed, waived
    - document_url: URL to uploaded proof/certificate
    - notes: Any notes about completion

    Args:
        employee_id: Employee ID
        item_id: Item ID within checklist
        req: UpdateItemRequest with status and optional document_url/notes
        venue_id: Venue ID (optional)

    Returns:
        Updated OnboardingItemResponse
    """
    venue_id = _scope_employee(employee_id, venue_id) or venue_id
    # Completing/waiving a mandatory compliance item (RSA/WHS/food-safety) is a
    # manager sign-off, not a self-serve action.
    enforce_venue_manager(venue_id)
    try:
        service = get_onboarding_service()

        item = service.update_item(
            employee_id=employee_id,
            item_id=item_id,
            status=req.status,
            venue_id=venue_id,
            notes=req.notes,
            document_url=req.document_url,
        )

        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Item {item_id} not found for employee {employee_id}",
            )

        return OnboardingItemResponse(
            id=item.id,
            name=item.name,
            description=item.description,
            category=item.category,
            is_mandatory=item.is_mandatory,
            status=item.status,
            completed_at=item.completed_at,
            document_url=item.document_url,
            notes=item.notes,
            reminded_at=item.reminded_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating item {item_id} for {employee_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/employees/{employee_id}/onboarding/ready", response_model=RosteringReadyResponse)
async def check_rostering_ready(employee_id: str, venue_id: Optional[str] = None):
    """
    Check if an employee is ready for rostering.

    "Rostering ready" means all mandatory items are completed or waived.

    Args:
        employee_id: Employee ID
        venue_id: Venue ID (optional)

    Returns:
        RosteringReadyResponse with is_ready flag and list of missing mandatory items
    """
    venue_id = _scope_employee(employee_id, venue_id) or venue_id
    try:
        service = get_onboarding_service()
        is_ready, missing_items = service.is_rostering_ready(employee_id, venue_id)

        # Get completion percentage for reference
        checklist = service.get_onboarding(employee_id, venue_id)
        completion_pct = checklist.completion_pct if checklist else 0.0

        return RosteringReadyResponse(
            is_ready=is_ready,
            missing_items=missing_items,
            completion_pct=completion_pct,
        )
    except Exception as e:
        logger.error(f"Error checking rostering readiness for {employee_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/employees/{employee_id}/onboarding/remind", response_model=SendReminderResponse)
async def send_reminder(
    employee_id: str,
    req: SendReminderRequest,
    venue_id: Optional[str] = None,
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """
    Send reminders to an employee about incomplete onboarding items.

    If item_ids is not provided, reminders are sent for all incomplete items.

    Args:
        employee_id: Employee ID
        req: SendReminderRequest with optional item_ids
        venue_id: Venue ID (optional)
        background_tasks: Background task queue

    Returns:
        SendReminderResponse with count of reminders sent
    """
    venue_id = _scope_employee(employee_id, venue_id) or venue_id
    try:
        service = get_onboarding_service()

        # Run in background
        async def send():
            count = await service.send_reminder(
                employee_id=employee_id,
                item_ids=req.item_ids,
                venue_id=venue_id,
            )
            return count

        # For sync endpoints, we send immediately
        count = await service.send_reminder(
            employee_id=employee_id,
            item_ids=req.item_ids,
            venue_id=venue_id,
        )

        return SendReminderResponse(reminders_sent=count)
    except Exception as e:
        logger.error(f"Error sending reminders to {employee_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/venues/{venue_id}/onboarding-template", response_model=VenueTemplateResponse)
async def get_venue_template(venue_id: str):
    """
    Get the customized onboarding template for a venue.

    Returns the default template if venue hasn't customized.

    Args:
        venue_id: Venue ID

    Returns:
        VenueTemplateResponse with list of template items
    """
    _scope_venue(venue_id)
    try:
        service = get_onboarding_service()
        templates = service.get_venue_template(venue_id)

        return VenueTemplateResponse(
            venue_id=venue_id,
            items=[
                OnboardingItemTemplateResponse(
                    id=template.id,
                    name=template.name,
                    description=template.description,
                    category=template.category,
                    is_mandatory=template.is_mandatory,
                    required_for_roles=template.required_for_roles,
                )
                for template in templates
            ],
        )
    except Exception as e:
        logger.error(f"Error retrieving template for venue {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/venues/{venue_id}/onboarding-template", response_model=VenueTemplateResponse)
async def update_venue_template(venue_id: str, req: UpdateVenueTemplateRequest):
    """
    Customize the onboarding template for a venue.

    Allows venues to add, remove, or modify which items apply to which roles.

    Args:
        venue_id: Venue ID
        req: UpdateVenueTemplateRequest with items list

    Returns:
        Updated VenueTemplateResponse
    """
    _scope_venue(venue_id)
    enforce_venue_manager(venue_id)  # venue-wide compliance config = manager
    try:
        service = get_onboarding_service()
        templates = service.update_venue_template(venue_id, req.items)

        return VenueTemplateResponse(
            venue_id=venue_id,
            items=[
                OnboardingItemTemplateResponse(
                    id=template.id,
                    name=template.name,
                    description=template.description,
                    category=template.category,
                    is_mandatory=template.is_mandatory,
                    required_for_roles=template.required_for_roles,
                )
                for template in templates
            ],
        )
    except Exception as e:
        logger.error(f"Error updating template for venue {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/venues/{venue_id}/onboarding-status", response_model=VenueOnboardingStatusResponse)
async def get_venue_onboarding_status(venue_id: str):
    """
    Get venue-wide onboarding overview.

    Returns aggregated metrics:
    - Total employees in onboarding
    - Completion/in-progress/pending counts
    - Overall completion rate
    - Average days to complete
    - Most common pending items (top 5)

    Args:
        venue_id: Venue ID

    Returns:
        VenueOnboardingStatusResponse with aggregate data
    """
    _scope_venue(venue_id)
    try:
        service = get_onboarding_service()
        status = service.get_venue_onboarding_status(venue_id)

        return VenueOnboardingStatusResponse(
            venue_id=status.venue_id,
            total_employees_onboarding=status.total_employees_onboarding,
            completed=status.completed,
            in_progress=status.in_progress,
            pending=status.pending,
            completion_rate=status.completion_rate,
            average_days_to_complete=status.average_days_to_complete,
            most_common_pending_items=[
                VenueOnboardingStatusItemResponse(name=item["name"], count=item["count"])
                for item in status.most_common_pending_items
            ],
        )
    except Exception as e:
        logger.error(f"Error retrieving status for venue {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/venues/{venue_id}/onboarding/auto-remind", response_model=AutoRemindResponse)
async def auto_remind_incomplete(
    venue_id: str,
    days_since_start: int = 7,
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """
    Bulk remind employees with incomplete onboarding after N days.

    Useful for automated reminders triggered by cron/scheduler.
    Only reminds about incomplete mandatory items.

    Args:
        venue_id: Venue ID
        days_since_start: Only remind if onboarding started >= N days ago (default: 7)
        background_tasks: Background task queue

    Returns:
        AutoRemindResponse with counts
    """
    _scope_venue(venue_id)
    try:
        service = get_onboarding_service()

        # Run in background for large venues
        total_reminded = await service.auto_remind_incomplete(venue_id, days_since_start)

        # Estimate employees reminded (rough)
        status = service.get_venue_onboarding_status(venue_id)
        employees_with_incomplete = status.in_progress + status.pending

        return AutoRemindResponse(
            reminders_sent=total_reminded,
            employees_reminded=min(total_reminded, employees_with_incomplete),
        )
    except Exception as e:
        logger.error(f"Error with auto-remind for venue {venue_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
