"""
Roster optimisation API routes for RosterIQ.

Endpoints for running and monitoring roster optimisation jobs via MILP and hybrid solvers.

Routes:
    POST /api/v1/rosters/optimise — run roster optimisation (async job)
    GET /api/v1/rosters/optimise/status/{job_id} — check optimisation job status
    POST /api/v1/rosters/optimise/cancel/{job_id} — cancel a running job
    GET /api/v1/rosters/{roster_id} — retrieve optimised roster

The optimisation endpoint accepts:
    - venue_id: Venue to optimise for
    - week_start: Start date of the week
    - strategy: "cost_optimized", "coverage_first", or "balanced"
    - solver_preference: "milp", "hybrid", or "greedy"
    - covers_per_staff: Optional override for covers-to-staff ratio
    - timeout_seconds: Optional solver timeout (max 60s)

Status endpoint returns job state:
    - pending: Job queued
    - running: In progress
    - completed: Success with roster_id
    - failed: Error with error_message
    - timeout: Exceeded time limit

Uses background task queue (Celery or similar) for async execution.
All monetary values use Decimal for precision.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Optional, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.models import (
    VenueConfig, Employee, DemandForecast, Roster, State,
    User, UserRole,
)
from rosteriq.services.roster_optimiser_v2 import (
    MILPRosterOptimiser, HybridOptimiser, OptimisationStrategy,
)
from rosteriq.auth import get_current_user, require_role

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/rosters", tags=["optimiser"])


# ============================================================================
# Job State Tracking (Simple In-Memory; Use DB in Production)
# ============================================================================

class OptimisationJobStatus(str):
    """Status of an optimisation job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class OptimisationJob:
    """In-memory job tracker."""

    def __init__(self, job_id: str, venue_id: str, week_start: date):
        self.job_id = job_id
        self.venue_id = venue_id
        self.week_start = week_start
        self.status = OptimisationJobStatus.PENDING
        self.roster_id: Optional[str] = None
        self.error_message: Optional[str] = None
        self.solver_used: Optional[str] = None
        self.solver_message: Optional[str] = None
        self.created_at = date.today()

    def to_dict(self) -> dict:
        """Serialize to dict."""
        return {
            "job_id": self.job_id,
            "venue_id": self.venue_id,
            "week_start": self.week_start.isoformat(),
            "status": self.status,
            "roster_id": self.roster_id,
            "error_message": self.error_message,
            "solver_used": self.solver_used,
            "solver_message": self.solver_message,
            "created_at": self.created_at.isoformat(),
        }


# Global job tracker (use Redis/database in production)
_job_store: dict[str, OptimisationJob] = {}


# ============================================================================
# Request/Response Models
# ============================================================================

class OptimiseRosterRequest(BaseModel):
    """Request to optimise roster for a venue."""

    venue_id: str = Field(..., description="Venue ID to optimise")
    week_start: date = Field(..., description="Start date of week (Monday)")
    strategy: str = Field(
        default="cost_optimized",
        description="Strategy: cost_optimized, coverage_first, or balanced"
    )
    solver_preference: str = Field(
        default="hybrid",
        description="Solver: milp, hybrid, or greedy"
    )
    covers_per_staff: float = Field(
        default=15.0,
        description="Covers per staff member for demand calculation"
    )
    timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=60,
        description="Maximum solver time in seconds"
    )


class OptimisationJobResponse(BaseModel):
    """Response with job ID."""

    job_id: str = Field(..., description="Job ID for tracking")
    status: str = Field(..., description="Current job status")
    venue_id: str
    week_start: str
    message: str = "Optimisation job queued"


class OptimisationStatusResponse(BaseModel):
    """Response with job status."""

    job_id: str
    venue_id: str
    week_start: str
    status: str
    roster_id: Optional[str] = None
    error_message: Optional[str] = None
    solver_used: Optional[str] = None
    solver_message: Optional[str] = None


class RosterResponse(BaseModel):
    """Response with roster details."""

    id: str
    venue_id: str
    week_start: str
    week_end: str
    shift_count: int
    total_hours: float
    total_cost: str  # Decimal as string
    employees_used: int


# ============================================================================
# Helper Functions
# ============================================================================

async def get_venue(
    db,
    venue_id: str,
    current_user: User,
) -> VenueConfig:
    """
    Fetch venue and check authorization.

    Args:
        db: Database session
        venue_id: Venue ID
        current_user: Authenticated user

    Returns:
        VenueConfig

    Raises:
        HTTPException if not found or not authorized
    """
    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")

    # Check authorization (simplified; implement proper auth in production)
    # For now, assume user can access any venue they have permissions for
    return venue


# ============================================================================
# Routes
# ============================================================================

@router.post("/optimise", response_model=OptimisationJobResponse)
async def optimise_roster(
    request: OptimiseRosterRequest,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Queue a roster optimisation job.

    The endpoint validates the request, fetches venue/employee/forecast data,
    and queues a background job to run the optimiser. Returns immediately
    with a job_id for tracking progress.

    Args:
        request: Optimisation parameters
        current_user: Authenticated user
        db: Database session

    Returns:
        OptimisationJobResponse with job_id

    Raises:
        HTTPException if venue not found or invalid parameters
    """
    # Validate strategy
    if request.strategy not in ["cost_optimized", "coverage_first", "balanced"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid strategy; must be cost_optimized, coverage_first, or balanced"
        )

    # Validate solver preference
    if request.solver_preference not in ["milp", "hybrid", "greedy"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid solver_preference; must be milp, hybrid, or greedy"
        )

    # Fetch venue
    try:
        venue = await get_venue(db, request.venue_id, current_user)
    except HTTPException:
        raise

    logger.info(
        f"Optimisation requested for venue {request.venue_id} "
        f"week {request.week_start} by user {current_user.id}"
    )

    # Create job
    job_id = str(uuid4())
    job = OptimisationJob(job_id, request.venue_id, request.week_start)
    _job_store[job_id] = job

    # Queue background task (simplified; use Celery in production)
    # For now, run synchronously
    try:
        job.status = OptimisationJobStatus.RUNNING

        # Fetch employees for venue
        employees: List[Employee] = [
            e for e in db.list_employees() if e.venue_id == request.venue_id
        ]

        if not employees:
            raise ValueError("No employees found for venue")

        # Fetch forecasts for week
        week_end = request.week_start + __import__('datetime').timedelta(days=6)
        forecasts: List[DemandForecast] = db.get_forecasts(
            request.venue_id, request.week_start, week_end
        )

        if not forecasts:
            raise ValueError("No demand forecasts found for week")

        # Run optimiser
        strategy = OptimisationStrategy[request.strategy]

        if request.solver_preference == "milp":
            optimiser = MILPRosterOptimiser(
                venue_config=venue,
                employees=employees,
                forecasts=forecasts,
                week_start=request.week_start,
                week_end=week_end,
                strategy=strategy,
                covers_per_staff=request.covers_per_staff,
            )
            roster = optimiser.solve(request.timeout_seconds)
            if roster is None:
                raise ValueError("MILP solver returned no feasible solution")

        elif request.solver_preference == "hybrid":
            optimiser = HybridOptimiser(
                venue_config=venue,
                employees=employees,
                forecasts=forecasts,
                week_start=request.week_start,
                week_end=week_end,
                strategy=strategy,
                covers_per_staff=request.covers_per_staff,
            )
            roster = optimiser.solve(request.timeout_seconds)
            job.solver_used = optimiser.solver_used.value
            job.solver_message = optimiser.solver_message

        else:  # greedy
            # Use greedy fallback directly
            optimiser = HybridOptimiser(
                venue_config=venue,
                employees=employees,
                forecasts=forecasts,
                week_start=request.week_start,
                week_end=week_end,
                strategy=strategy,
                covers_per_staff=request.covers_per_staff,
            )
            roster = optimiser._greedy_solve()
            job.solver_used = "greedy"
            job.solver_message = "Greedy solver executed"

        # Save roster
        db.add(roster)
        db.commit()

        job.roster_id = roster.id
        job.status = OptimisationJobStatus.COMPLETED

        logger.info(f"Optimisation job {job_id} completed with roster {roster.id}")

    except Exception as e:
        job.status = OptimisationJobStatus.FAILED
        job.error_message = str(e)
        logger.error(f"Optimisation job {job_id} failed: {e}", exc_info=True)

    return OptimisationJobResponse(
        job_id=job_id,
        status=job.status,
        venue_id=request.venue_id,
        week_start=request.week_start.isoformat(),
        message="Optimisation complete" if job.status == OptimisationJobStatus.COMPLETED
                else f"Optimisation {job.status}"
    )


@router.get("/optimise/status/{job_id}", response_model=OptimisationStatusResponse)
async def get_optimisation_status(
    job_id: str,
    current_user: User = Depends(get_current_user),
):
    """
    Get the status of an optimisation job.

    Args:
        job_id: Job ID from the optimise endpoint
        current_user: Authenticated user

    Returns:
        OptimisationStatusResponse with current state

    Raises:
        HTTPException if job not found
    """
    if job_id not in _job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _job_store[job_id]

    return OptimisationStatusResponse(
        job_id=job.job_id,
        venue_id=job.venue_id,
        week_start=job.week_start.isoformat(),
        status=job.status,
        roster_id=job.roster_id,
        error_message=job.error_message,
        solver_used=job.solver_used,
        solver_message=job.solver_message,
    )


@router.post("/optimise/cancel/{job_id}")
async def cancel_optimisation_job(
    job_id: str,
    current_user: User = Depends(get_current_user),
):
    """
    Cancel a pending or running optimisation job.

    Args:
        job_id: Job ID to cancel
        current_user: Authenticated user

    Returns:
        {"message": "Job cancelled"}

    Raises:
        HTTPException if job not found or already completed
    """
    if job_id not in _job_store:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _job_store[job_id]

    if job.status in [OptimisationJobStatus.COMPLETED, OptimisationJobStatus.FAILED]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job in {job.status} state"
        )

    job.status = OptimisationJobStatus.CANCELLED
    logger.info(f"Optimisation job {job_id} cancelled by user {current_user.id}")

    return {"message": f"Job {job_id} cancelled"}


@router.get("/{roster_id}", response_model=RosterResponse)
async def get_roster(
    roster_id: str,
    current_user: User = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Retrieve an optimised roster.

    Args:
        roster_id: Roster ID
        current_user: Authenticated user
        db: Database session

    Returns:
        RosterResponse with roster details

    Raises:
        HTTPException if roster not found
    """
    roster = db.get_roster(roster_id)
    if not roster:
        raise HTTPException(status_code=404, detail="Roster not found")

    return RosterResponse(
        id=roster.id,
        venue_id=roster.venue_id,
        week_start=roster.week_start.isoformat(),
        week_end=roster.week_end.isoformat(),
        shift_count=len(roster.shifts),
        total_hours=roster.total_hours,
        total_cost=str(roster.total_cost),
        employees_used=len(roster.employees_used),
    )


@router.post("/compare")
async def compare_rosters(
    original_roster_id: str = Query(...),
    optimised_roster_id: str = Query(...),
    current_user: User = Depends(get_current_user),
    db = Depends(get_db),
):
    """
    Compare an original roster with an optimised one.

    Returns cost savings, hours saved, and alerts.

    Args:
        original_roster_id: ID of original roster
        optimised_roster_id: ID of optimised roster
        current_user: Authenticated user
        db: Database session

    Returns:
        {"cost_savings": Decimal, "hours_saved": float, "alerts": List[str]}

    Raises:
        HTTPException if rosters not found
    """
    original = db.get_roster(original_roster_id)
    optimised = db.get_roster(optimised_roster_id)

    if not original or not optimised:
        raise HTTPException(status_code=404, detail="Roster not found")

    cost_savings = (original.total_cost or Decimal("0")) - \
                   (optimised.total_cost or Decimal("0"))
    hours_saved = original.total_hours - optimised.total_hours

    alerts = []
    if cost_savings < 0:
        alerts.append("Optimised roster is more expensive; review constraints")
    if hours_saved < 0:
        alerts.append("Optimised roster has more hours; may indicate over-staffing in original")

    return {
        "cost_savings": str(cost_savings),
        "hours_saved": hours_saved,
        "alerts": alerts,
    }
