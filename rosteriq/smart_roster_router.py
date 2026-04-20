"""REST API router for Smart Roster Suggestions Engine.

Endpoints:
- POST /api/v1/smart-roster/score-candidate (L1+) — score single candidate
- POST /api/v1/smart-roster/suggest-shift (L2+) — suggest candidates for shift
- POST /api/v1/smart-roster/plan (L2+) — generate full roster plan
- GET /api/v1/smart-roster/{venue_id}/coverage (L2+) — coverage analysis

Integrates with FastAPI; requires auth layer from api_v2.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, date, timezone

logger = logging.getLogger("rosteriq.smart_roster_router")

# Lazy imports to handle missing FastAPI/Pydantic in sandboxed environment
try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    BaseModel = None
    Field = None


# ─────────────────────────────────────────────────────────────────────────────
# Request/Response Models (Pydantic)
# ─────────────────────────────────────────────────────────────────────────────


if BaseModel is not None:

    class SuitabilityFactorResponse(BaseModel):
        """Single suitability factor."""
        name: str
        score: float
        weight: float
        reason: str

    class StaffSuggestionResponse(BaseModel):
        """Suggestion for a single employee."""
        employee_id: str
        employee_name: str
        suitability_score: float
        factors: List[SuitabilityFactorResponse]
        warnings: List[str]
        estimated_cost: float
        is_overtime: bool
        explanation: str

    class ShiftRequirementRequest(BaseModel):
        """Request body for shift requirement."""
        venue_id: str
        date: str  # ISO date YYYY-MM-DD
        start_time: str  # HH:MM
        end_time: str  # HH:MM
        role: str
        area: Optional[str] = None
        min_staff: int = 1
        required_certs: List[str] = Field(default_factory=list)

    class ScoreCandidateRequest(BaseModel):
        """Request to score a single candidate."""
        employee: Dict[str, Any]  # Employee dict with id, name, role, skills, hourly_rate, etc.
        requirement: ShiftRequirementRequest
        context: Optional[Dict[str, Any]] = Field(default_factory=dict)  # Pre-fetched context

    class SuggestShiftRequest(BaseModel):
        """Request to suggest candidates for a shift."""
        requirement: ShiftRequirementRequest
        candidates: List[Dict[str, Any]]  # Employee dicts
        context: Optional[Dict[str, Any]] = Field(default_factory=dict)

    class RosterSuggestionResponse(BaseModel):
        """Suggestions for a single shift."""
        requirement: ShiftRequirementRequest
        suggestions: List[StaffSuggestionResponse]
        unfilled: bool
        unfilled_reason: Optional[str]

    class BuildRosterPlanRequest(BaseModel):
        """Request to build a full roster plan."""
        venue_id: str
        period_start: str  # ISO date YYYY-MM-DD
        period_end: str  # ISO date YYYY-MM-DD
        candidates: List[Dict[str, Any]]
        context: Optional[Dict[str, Any]] = Field(default_factory=dict)

    class RosterPlanResponse(BaseModel):
        """Complete roster plan."""
        venue_id: str
        plan_date: str
        period_start: str
        period_end: str
        shift_requirements_count: int
        suggestions: List[RosterSuggestionResponse]
        total_estimated_cost: float
        budget_status: str
        coverage_pct: float
        warnings: List[str]

    class CoverageAnalysisResponse(BaseModel):
        """Coverage analysis for existing roster."""
        venue_id: str
        period_start: str
        period_end: str
        total_shifts: int
        filled_shifts: int
        coverage_pct: float
        warnings: List[str]


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────


def _get_auth_helper():
    """Lazy import of auth module."""
    try:
        from rosteriq import auth

        return auth
    except ImportError:
        return None


def _get_smart_roster_module():
    """Lazy import of smart_roster module."""
    try:
        from rosteriq import smart_roster

        return smart_roster
    except ImportError:
        return None


def _gate(request: Request, level_name: str) -> Optional[Dict[str, Any]]:
    """Lazy auth gating (same pattern as api_v2).

    Returns user dict if auth passes, None if disabled, raises HTTPException if denied.
    """
    auth = _get_auth_helper()
    if auth is None:
        return None

    try:
        return auth.require_access(request, level_name)
    except Exception as e:
        if APIRouter is not None:
            raise HTTPException(status_code=403, detail=str(e))
        raise


def _date_from_iso(date_str: str) -> date:
    """Parse ISO date string to date object."""
    return datetime.fromisoformat(date_str).date()


# ─────────────────────────────────────────────────────────────────────────────
# Router Setup
# ─────────────────────────────────────────────────────────────────────────────


def create_smart_roster_router() -> Optional[Any]:
    """Create and return the smart roster router."""
    if APIRouter is None:
        logger.warning("FastAPI not available; smart_roster_router cannot be created")
        return None

    router = APIRouter()

    # ─────────────────────────────────────────────────────────────────────
    # Endpoint: POST /api/v1/smart-roster/score-candidate (L1+)
    # ─────────────────────────────────────────────────────────────────────

    @router.post(
        "/score-candidate",
        response_model=StaffSuggestionResponse,
        summary="Score a single candidate for a shift",
        tags=["smart-roster"],
    )
    async def score_candidate(
        request: Request,
        body: ScoreCandidateRequest,
    ) -> Dict[str, Any]:
        """Score a single candidate against a shift requirement.

        **Auth:** L1+ (manager level or higher)

        **Request:**
        - employee: Employee dict (id, name, role, skills, hourly_rate, availability, etc.)
        - requirement: Shift requirement (venue_id, date, start_time, end_time, role, required_certs)
        - context: Optional pre-fetched context (staff_scores, fatigue_assessments, etc.)

        **Response:**
        - StaffSuggestionResponse with suitability_score (0-100), factors, warnings, explanation
        """
        user = _gate(request, "L1")

        smart_roster = _get_smart_roster_module()
        if smart_roster is None:
            raise HTTPException(
                status_code=503,
                detail="Smart roster module not available",
            )

        try:
            # Parse shift requirement date
            shift_date = _date_from_iso(body.requirement.date)

            # Create ShiftRequirement object
            from rosteriq.smart_roster import ShiftRequirement

            requirement = ShiftRequirement(
                venue_id=body.requirement.venue_id,
                date=shift_date,
                start_time=body.requirement.start_time,
                end_time=body.requirement.end_time,
                role=body.requirement.role,
                area=body.requirement.area,
                min_staff=body.requirement.min_staff,
                required_certs=body.requirement.required_certs,
            )

            # Score the candidate
            suggestion = smart_roster.score_candidate(
                body.employee,
                requirement,
                body.context or {},
            )

            # Return as dict (Pydantic will validate)
            return suggestion.to_dict()

        except Exception as e:
            logger.exception(f"Error scoring candidate: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    # ─────────────────────────────────────────────────────────────────────
    # Endpoint: POST /api/v1/smart-roster/suggest-shift (L2+)
    # ─────────────────────────────────────────────────────────────────────

    @router.post(
        "/suggest-shift",
        response_model=RosterSuggestionResponse,
        summary="Suggest candidates for a single shift",
        tags=["smart-roster"],
    )
    async def suggest_shift(
        request: Request,
        body: SuggestShiftRequest,
    ) -> Dict[str, Any]:
        """Rank candidates for a single shift requirement.

        **Auth:** L2+ (manager level or higher)

        **Request:**
        - requirement: Shift requirement
        - candidates: List of employee dicts
        - context: Optional pre-fetched context

        **Response:**
        - RosterSuggestionResponse with ranked suggestions (best to worst)
        """
        user = _gate(request, "L2")

        smart_roster = _get_smart_roster_module()
        if smart_roster is None:
            raise HTTPException(
                status_code=503,
                detail="Smart roster module not available",
            )

        try:
            # Parse shift requirement
            from rosteriq.smart_roster import ShiftRequirement

            shift_date = _date_from_iso(body.requirement.date)
            requirement = ShiftRequirement(
                venue_id=body.requirement.venue_id,
                date=shift_date,
                start_time=body.requirement.start_time,
                end_time=body.requirement.end_time,
                role=body.requirement.role,
                area=body.requirement.area,
                min_staff=body.requirement.min_staff,
                required_certs=body.requirement.required_certs,
            )

            # Suggest candidates
            roster_suggestion = smart_roster.suggest_for_shift(
                requirement,
                body.candidates,
                body.context or {},
            )

            # Convert to dict response
            response_dict = {
                "requirement": roster_suggestion.requirement.to_dict(),
                "suggestions": [s.to_dict() for s in roster_suggestion.suggestions],
                "unfilled": roster_suggestion.unfilled,
                "unfilled_reason": roster_suggestion.unfilled_reason,
            }
            return response_dict

        except Exception as e:
            logger.exception(f"Error suggesting shift: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    # ─────────────────────────────────────────────────────────────────────
    # Endpoint: POST /api/v1/smart-roster/plan (L2+)
    # ─────────────────────────────────────────────────────────────────────

    @router.post(
        "/plan",
        response_model=RosterPlanResponse,
        summary="Generate full roster plan for a period",
        tags=["smart-roster"],
    )
    async def build_roster_plan(
        request: Request,
        body: BuildRosterPlanRequest,
    ) -> Dict[str, Any]:
        """Generate a complete roster plan for a period.

        **Auth:** L2+ (manager level or higher)

        **Request:**
        - venue_id: Venue identifier
        - period_start: Start date (ISO format YYYY-MM-DD)
        - period_end: End date (ISO format YYYY-MM-DD)
        - candidates: List of employee dicts
        - context: Optional pre-fetched context

        **Response:**
        - RosterPlanResponse with shift requirements, suggestions, cost, coverage
        """
        user = _gate(request, "L2")

        smart_roster = _get_smart_roster_module()
        if smart_roster is None:
            raise HTTPException(
                status_code=503,
                detail="Smart roster module not available",
            )

        try:
            period_start = _date_from_iso(body.period_start)
            period_end = _date_from_iso(body.period_end)

            # Build plan
            plan = smart_roster.build_roster_plan(
                body.venue_id,
                period_start,
                period_end,
                body.candidates,
                body.context or {},
            )

            # Convert to dict response
            response_dict = plan.to_dict()
            return response_dict

        except Exception as e:
            logger.exception(f"Error building roster plan: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    # ─────────────────────────────────────────────────────────────────────
    # Endpoint: GET /api/v1/smart-roster/{venue_id}/coverage (L2+)
    # ─────────────────────────────────────────────────────────────────────

    @router.get(
        "/{venue_id}/coverage",
        response_model=CoverageAnalysisResponse,
        summary="Coverage analysis for existing roster",
        tags=["smart-roster"],
    )
    async def coverage_analysis(
        request: Request,
        venue_id: str,
        date_from: Optional[str] = None,  # ISO date YYYY-MM-DD
        date_to: Optional[str] = None,  # ISO date YYYY-MM-DD
    ) -> Dict[str, Any]:
        """Analyze coverage for existing roster over a period.

        **Auth:** L2+ (manager level or higher)

        **Query parameters:**
        - date_from: Start date (ISO format)
        - date_to: End date (ISO format)

        **Response:**
        - CoverageAnalysisResponse with filled/unfilled shift counts, coverage percentage, warnings
        """
        user = _gate(request, "L2")

        # Stub: actual implementation would fetch roster and analyze coverage
        # For now, return a basic response
        if date_from:
            period_start = _date_from_iso(date_from)
        else:
            period_start = date.today()

        if date_to:
            period_end = _date_from_iso(date_to)
        else:
            period_end = date.today()

        return {
            "venue_id": venue_id,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "total_shifts": 0,
            "filled_shifts": 0,
            "coverage_pct": 0.0,
            "warnings": ["Coverage analysis not yet implemented"],
        }

    return router


# ─────────────────────────────────────────────────────────────────────────────
# Factory function
# ─────────────────────────────────────────────────────────────────────────────


smart_roster_router = create_smart_roster_router()
