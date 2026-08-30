"""
API routes for venue onboarding workflow.

Endpoints:
- POST /api/onboarding/start — begin setup for a venue
- GET /api/onboarding/status/{venue_id} — get current progress
- POST /api/onboarding/step/{venue_id}/{step} — run a specific step
- POST /api/onboarding/retry/{venue_id} — retry failed step
- GET /api/onboarding/summary/{venue_id} — full import summary
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.onboarding import OnboardingService, OnboardingStep
from rosteriq.models import TandaCredentials

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])

# Service instance
_onboarding_service = None


def get_onboarding_service() -> OnboardingService:
    """Get or create the onboarding service."""
    global _onboarding_service
    if _onboarding_service is None:
        _onboarding_service = OnboardingService()
    return _onboarding_service


# ============================================================================
# Request/Response models
# ============================================================================

class OnboardingStartRequest(BaseModel):
    """Request to start onboarding."""
    venue_id: str
    tanda_client_id: str
    tanda_client_secret: str
    tanda_access_token: str
    tanda_refresh_token: Optional[str] = None
    tanda_org_id: str


class OnboardingStepRequest(BaseModel):
    """Request to run a step."""
    tanda_client_id: Optional[str] = None
    tanda_client_secret: Optional[str] = None
    tanda_access_token: Optional[str] = None
    tanda_refresh_token: Optional[str] = None
    tanda_org_id: Optional[str] = None


class OnboardingStatusResponse(BaseModel):
    """Response with onboarding status."""
    venue_id: str
    current_step: str
    progress_pct: int
    completed_steps: list
    errors: list
    last_error: Optional[str] = None
    imported_counts: dict
    venue_name: Optional[str] = None
    started_at: str


class OnboardingSummaryResponse(BaseModel):
    """Response with onboarding summary."""
    venue_id: str
    venue_name: Optional[str]
    status: str
    imported_counts: dict
    total_errors: int
    started_at: str
    completed_steps: list


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/start")
async def start_onboarding(req: OnboardingStartRequest, background_tasks: BackgroundTasks):
    """
    Start onboarding for a venue.

    Body:
    - venue_id: Venue identifier
    - tanda_client_id, tanda_client_secret: OAuth credentials
    - tanda_access_token: Current access token
    - tanda_org_id: Tanda organization ID

    Response: OnboardingStatusResponse
    """
    enforce_venue_manager(req.venue_id)
    try:
        service = get_onboarding_service()

        # Create Tanda credentials
        credentials = TandaCredentials(
            client_id=req.tanda_client_id,
            client_secret=req.tanda_client_secret,
            access_token=req.tanda_access_token,
            refresh_token=req.tanda_refresh_token,
            org_id=req.tanda_org_id,
        )

        # Start onboarding
        state = await service.start_onboarding(req.venue_id, req.tanda_access_token)

        # Run all steps in background
        background_tasks.add_task(service.run_all, req.venue_id, credentials)

        return {
            "venue_id": req.venue_id,
            "message": "Onboarding started. Processing in background.",
            "status": "started",
        }

    except Exception as e:
        logger.error(f"Error starting onboarding: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/status/{venue_id}", response_model=Optional[OnboardingStatusResponse])
async def get_status(venue_id: str):
    """
    Get current onboarding status for a venue.

    Response: OnboardingStatusResponse with current step, progress, errors
    """
    try:
        service = get_onboarding_service()
        status = service.get_status(venue_id)

        if not status:
            raise HTTPException(status_code=404, detail=f"No onboarding found for venue {venue_id}")

        return status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting onboarding status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/step/{venue_id}/{step}")
async def run_step(venue_id: str, step: str, req: OnboardingStepRequest):
    """
    Run a specific onboarding step.

    Path params:
    - venue_id: Venue identifier
    - step: Step name (connect_tanda, import_employees, etc.)

    Body:
    - tanda_* fields: Credentials (required for some steps)

    Response: {success: bool, message: str}
    """
    enforce_venue_manager(venue_id)
    try:
        service = get_onboarding_service()

        # Parse step
        try:
            step_enum = OnboardingStep(step)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Unknown step: {step}")

        # Build credentials if provided
        credentials = None
        if req.tanda_access_token and req.tanda_org_id:
            credentials = TandaCredentials(
                client_id=req.tanda_client_id or "",
                client_secret=req.tanda_client_secret or "",
                access_token=req.tanda_access_token,
                refresh_token=req.tanda_refresh_token,
                org_id=req.tanda_org_id,
            )

        success = await service.run_step(venue_id, step_enum, credentials)

        if not success:
            status = service.get_status(venue_id)
            return {
                "success": False,
                "message": f"Step {step} failed",
                "last_error": status.get("last_error") if status else None,
            }

        return {
            "success": True,
            "message": f"Step {step} completed",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running step: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/retry/{venue_id}")
async def retry_step(venue_id: str, req: OnboardingStepRequest):
    """
    Retry the current failed step.

    Body:
    - tanda_* fields: Credentials (required for most steps)

    Response: {success: bool, message: str}
    """
    enforce_venue_manager(venue_id)
    try:
        service = get_onboarding_service()

        # Build credentials if provided
        credentials = None
        if req.tanda_access_token and req.tanda_org_id:
            credentials = TandaCredentials(
                client_id=req.tanda_client_id or "",
                client_secret=req.tanda_client_secret or "",
                access_token=req.tanda_access_token,
                refresh_token=req.tanda_refresh_token,
                org_id=req.tanda_org_id,
            )

        success = await service.retry_step(venue_id, credentials)

        if not success:
            status = service.get_status(venue_id)
            return {
                "success": False,
                "message": "Retry failed",
                "last_error": status.get("last_error") if status else None,
            }

        return {
            "success": True,
            "message": "Retry succeeded",
        }

    except Exception as e:
        logger.error(f"Error retrying step: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary/{venue_id}", response_model=Optional[OnboardingSummaryResponse])
async def get_summary(venue_id: str):
    """
    Get full import summary with counts.

    Response: OnboardingSummaryResponse with import statistics
    """
    try:
        service = get_onboarding_service()
        summary = service.get_summary(venue_id)

        if not summary:
            raise HTTPException(status_code=404, detail=f"No onboarding found for venue {venue_id}")

        return summary

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
