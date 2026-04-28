"""
API routes for staff scheduling preferences.

Endpoints for preference learning, profile access, and preference-based recommendations.
"""

import logging
from datetime import datetime, date
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from rosteriq.services.preference_learner import (
    PreferenceLearner, PreferenceProfile
)
from rosteriq.models import Shift, Roster, ShiftStatus
from rosteriq.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/preferences", tags=["preferences"])


# ============================================================================
# Request/Response models
# ============================================================================


class TrainPreferencesRequest(BaseModel):
    """Request to train preference model."""

    venue_id: str = Field(..., description="ID of venue to train on")
    lookback_days: int = Field(default=90, ge=7, le=365, description="Days of history to analyze")


class TrainPreferencesResponse(BaseModel):
    """Response from training."""

    venue_id: str
    profiles_trained: int
    started_at: datetime
    completed_at: datetime
    status: str = "completed"


class PreferencesProfileResponse(BaseModel):
    """Response with preference profile."""

    employee_id: str
    venue_id: str
    day_scores: dict = Field(description="Day name -> preference score (0.0-1.0)")
    time_scores: dict = Field(description="Time slot -> preference score (0.0-1.0)")
    preferred_shift_length: float = Field(description="Ideal shift duration in hours")
    coworker_affinities: dict = Field(description="Employee ID -> affinity score (0.0-1.0)")
    consistency_score: float = Field(ge=0.0, le=1.0, description="0=loves variety, 1=loves routine")
    venue_preferences: dict = Field(description="Venue ID -> preference score (0.0-1.0)")
    role_preferences: dict = Field(description="Role -> preference score (0.0-1.0)")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in profile (0.0-1.0)")
    samples_used: int = Field(description="Number of shifts analyzed")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PredictHappinessRequest(BaseModel):
    """Request to predict happiness for a shift."""

    employee_id: str = Field(..., description="Employee ID")
    shift_date: date = Field(..., description="Shift date")
    shift_start_hour: int = Field(..., ge=0, le=23, description="Shift start hour")
    shift_end_hour: int = Field(..., ge=0, le=23, description="Shift end hour")
    shift_break_minutes: int = Field(default=0, ge=0, description="Break duration in minutes")
    role: str = Field(default="general", description="Shift role")


class HappinessResponse(BaseModel):
    """Response with happiness prediction."""

    employee_id: str
    happiness_score: float = Field(ge=0.0, le=1.0, description="Predicted happiness 0.0-1.0")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in prediction")
    day_contribution: float = Field(description="Contribution from day preference")
    time_contribution: float = Field(description="Contribution from time preference")
    length_contribution: float = Field(description="Contribution from shift length")


class RankEmployeesRequest(BaseModel):
    """Request to rank employees for a shift."""

    candidates: List[str] = Field(..., description="List of employee IDs to rank")
    shift_date: date = Field(..., description="Shift date")
    shift_start_hour: int = Field(..., ge=0, le=23, description="Shift start hour")
    shift_end_hour: int = Field(..., ge=0, le=23, description="Shift end hour")
    role: str = Field(default="general", description="Shift role")


class RankedEmployeeResponse(BaseModel):
    """Single ranked employee."""

    employee_id: str
    happiness_score: float = Field(ge=0.0, le=1.0)


class RankingResponse(BaseModel):
    """Response with ranked employees."""

    rankings: List[RankedEmployeeResponse] = Field(
        description="Employees ranked by happiness (descending)"
    )


class SuggestedShiftResponse(BaseModel):
    """Suggested shift for an employee."""

    date: date
    day_name: str
    time_slot: str = Field(description="morning/afternoon/evening/night")
    estimated_happiness: float = Field(ge=0.0, le=1.0)


class SuggestionsResponse(BaseModel):
    """Response with shift suggestions."""

    employee_id: str
    suggestions: List[SuggestedShiftResponse]


class RosterHappinessResponse(BaseModel):
    """Response with roster happiness score."""

    roster_id: str
    average_happiness: float = Field(ge=0.0, le=1.0)
    shift_count: int
    employee_count: int
    shift_details: List[dict] = Field(
        description="List of shift happiness scores"
    )


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/train/{venue_id}", response_model=TrainPreferencesResponse)
async def train_preferences(
    venue_id: str,
    lookback_days: int = Query(90, ge=7, le=365),
    background_tasks: BackgroundTasks = None,
) -> TrainPreferencesResponse:
    """
    Train preference model from historical rosters.

    This endpoint triggers the learning process that analyzes past shifts
    and builds preference profiles for all employees.

    Can be run in background (preferred for production).

    Args:
        venue_id: ID of venue to train on
        lookback_days: How many days of history to analyze (default: 90)
        background_tasks: FastAPI background tasks

    Returns:
        Training status and number of profiles created
    """
    try:
        started_at = datetime.utcnow()
        learner = PreferenceLearner()

        # Run training
        profiles = learner.train(venue_id, lookback_days)

        completed_at = datetime.utcnow()

        logger.info(
            f"Trained {len(profiles)} preference profiles for venue {venue_id} "
            f"using {lookback_days} days of history"
        )

        return TrainPreferencesResponse(
            venue_id=venue_id,
            profiles_trained=len(profiles),
            started_at=started_at,
            completed_at=completed_at,
            status="completed",
        )

    except Exception as e:
        logger.error(f"Preference training failed for venue {venue_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Training failed: {str(e)}")


@router.get(
    "/profile/{employee_id}",
    response_model=PreferencesProfileResponse
)
async def get_preference_profile(
    employee_id: str,
) -> PreferencesProfileResponse:
    """
    Get learned preference profile for an employee.

    Args:
        employee_id: ID of employee

    Returns:
        Preference profile with all learned preferences
    """
    try:
        learner = PreferenceLearner()
        profile = learner.get_preference_profile(employee_id)

        if not profile:
            raise HTTPException(
                status_code=404,
                detail=f"No preference profile found for employee {employee_id}"
            )

        return PreferencesProfileResponse(**profile.to_dict())

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get preference profile for {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict", response_model=HappinessResponse)
async def predict_happiness(
    request: PredictHappinessRequest,
) -> HappinessResponse:
    """
    Predict employee happiness for a proposed shift.

    Analyzes the shift against learned preferences and returns a happiness
    score (0.0-1.0) indicating how well it aligns with the employee's
    scheduling preferences.

    Args:
        request: Shift details and employee ID

    Returns:
        Happiness prediction with contribution breakdown
    """
    try:
        learner = PreferenceLearner()

        # Build shift object for prediction
        shift = Shift(
            id="temp_shift",
            employee_id=request.employee_id,
            date=request.shift_date,
            start_time=__import__("datetime").time(request.shift_start_hour, 0),
            end_time=__import__("datetime").time(request.shift_end_hour, 0),
            break_minutes=request.shift_break_minutes,
            status=ShiftStatus.scheduled,
            role=request.role,
        )

        happiness = learner.predict_happiness(request.employee_id, shift)
        profile = learner.get_preference_profile(request.employee_id)

        if not profile:
            confidence = 0.0
            day_contrib = 0.0
            time_contrib = 0.0
            length_contrib = 0.0
        else:
            day_name = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][
                request.shift_date.weekday()
            ]
            day_contrib = profile.day_scores.get(day_name, 0.5) * 0.3

            slot_name = learner._get_time_slot(request.shift_start_hour)
            time_contrib = profile.time_scores.get(slot_name, 0.5) * 0.25

            length_diff = abs(shift.net_hours - profile.preferred_shift_length)
            length_contrib = max(0.0, 1.0 - (length_diff / 8.0)) * 0.15

            confidence = profile.confidence

        return HappinessResponse(
            employee_id=request.employee_id,
            happiness_score=happiness,
            confidence=confidence,
            day_contribution=day_contrib,
            time_contribution=time_contrib,
            length_contribution=length_contrib,
        )

    except Exception as e:
        logger.error(f"Happiness prediction failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rank-employees", response_model=RankingResponse)
async def rank_employees_for_shift(
    request: RankEmployeesRequest,
) -> RankingResponse:
    """
    Rank employees by predicted happiness for a specific shift.

    Used by roster optimiser to break ties between equally-available employees.

    Args:
        request: Shift details and list of candidate employee IDs

    Returns:
        Sorted list of employees with happiness scores (best first)
    """
    try:
        learner = PreferenceLearner()

        # Build shift object
        shift = Shift(
            id="temp_shift",
            employee_id="",  # Will be set per candidate
            date=request.shift_date,
            start_time=__import__("datetime").time(request.shift_start_hour, 0),
            end_time=__import__("datetime").time(request.shift_end_hour, 0),
            break_minutes=0,
            status=ShiftStatus.scheduled,
            role=request.role,
        )

        # Rank employees
        rankings = learner.rank_employees_for_shift(shift, request.candidates)

        return RankingResponse(
            rankings=[
                RankedEmployeeResponse(
                    employee_id=emp_id,
                    happiness_score=score,
                )
                for emp_id, score in rankings
            ]
        )

    except Exception as e:
        logger.error(f"Employee ranking failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions/{employee_id}", response_model=SuggestionsResponse)
async def suggest_ideal_shifts(
    employee_id: str,
    num_days: int = Query(7, ge=1, le=30),
) -> SuggestionsResponse:
    """
    Suggest ideal shifts for an employee over the next N days.

    Returns shifts that align well with their learned preferences.

    Args:
        employee_id: ID of employee
        num_days: Number of days to suggest shifts for (default: 7)

    Returns:
        List of suggested shifts with estimated happiness scores
    """
    try:
        learner = PreferenceLearner()

        # Generate dates starting from tomorrow
        today = date.today()
        dates = [today + __import__("datetime").timedelta(days=i) for i in range(1, num_days + 1)]

        suggestions = learner.suggest_ideal_shifts(employee_id, dates)

        if not suggestions:
            raise HTTPException(
                status_code=404,
                detail=f"No preference profile found for employee {employee_id}"
            )

        return SuggestionsResponse(
            employee_id=employee_id,
            suggestions=[
                SuggestedShiftResponse(**s) for s in suggestions
            ]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Shift suggestions failed for {employee_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/roster-score/{roster_id}", response_model=RosterHappinessResponse)
async def get_roster_happiness_score(
    roster_id: str,
) -> RosterHappinessResponse:
    """
    Calculate average happiness score for all shifts in a roster.

    Useful for A/B testing roster quality and comparing optimization approaches.

    Args:
        roster_id: ID of roster

    Returns:
        Average happiness with per-shift breakdown
    """
    try:
        db = get_db()
        roster = db.get_roster(roster_id)

        if not roster:
            raise HTTPException(status_code=404, detail=f"Roster {roster_id} not found")

        learner = PreferenceLearner()
        avg_happiness = learner.roster_happiness_score(roster)

        # Calculate per-shift happiness
        shift_details = []
        for shift in roster.shifts:
            shift_happiness = learner.predict_happiness(shift.employee_id, shift)
            shift_details.append({
                "shift_id": shift.id,
                "employee_id": shift.employee_id,
                "date": str(shift.date),
                "happiness": shift_happiness,
            })

        return RosterHappinessResponse(
            roster_id=roster_id,
            average_happiness=avg_happiness,
            shift_count=len(roster.shifts),
            employee_count=len(roster.employees_used),
            shift_details=shift_details,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Roster happiness calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/venue/{venue_id}/profiles", response_model=List[PreferencesProfileResponse])
async def list_venue_profiles(
    venue_id: str,
) -> List[PreferencesProfileResponse]:
    """
    Get all preference profiles for a venue.

    Args:
        venue_id: ID of venue

    Returns:
        List of all preference profiles for the venue
    """
    try:
        learner = PreferenceLearner()
        profiles = learner.list_preference_profiles(venue_id)

        return [
            PreferencesProfileResponse(**p.to_dict()) for p in profiles
        ]

    except Exception as e:
        logger.error(f"Failed to list profiles for venue {venue_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
