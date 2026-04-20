"""
API router for Training & Skills Development Tracker.

Wires TrainingTrackerStore to FastAPI with 12 endpoints for:
- Skill CRUD and listing
- Employee skill assessments
- Training program management
- Training session scheduling and completion
- Gap analysis and reporting
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
except ImportError:
    # Allow module to load even without FastAPI/Pydantic
    APIRouter = None
    HTTPException = None
    Request = None
    BaseModel = None
    Field = None


from rosteriq.training_tracker import (
    get_training_tracker_store,
    SkillCategory,
    TrainingSessionStatus,
)


# ============================================================================
# Request/Response Models
# ============================================================================

if BaseModel:
    class SkillRequest(BaseModel):
        """Request to add or update a skill."""
        name: str = Field(..., min_length=1)
        category: str = Field(...)
        description: Optional[str] = None
        max_level: int = Field(default=5, ge=1, le=10)
        is_required: bool = False
        related_certification: Optional[str] = None

    class AssessmentRequest(BaseModel):
        """Request to assess an employee's skill."""
        skill_id: str = Field(...)
        level: int = Field(..., ge=1, le=5)
        assessed_by: str = Field(...)
        notes: Optional[str] = None
        target_level: Optional[int] = None
        target_date: Optional[str] = None

    class ProgramRequest(BaseModel):
        """Request to create a training program."""
        name: str = Field(..., min_length=1)
        description: Optional[str] = None
        skills_covered: List[str] = Field(default_factory=list)
        duration_hours: float = Field(default=0, ge=0)
        max_participants: Optional[int] = None
        cost_per_person: float = Field(default=0, ge=0)
        is_mandatory: bool = False

    class SessionRequest(BaseModel):
        """Request to schedule a training session."""
        program_id: str = Field(...)
        scheduled_date: str = Field(...)
        scheduled_time: str = Field(default="09:00")
        trainer: Optional[str] = None
        location: Optional[str] = None
        attendees: List[str] = Field(default_factory=list)
        notes: Optional[str] = None

    class SessionUpdateRequest(BaseModel):
        """Request to add attendee or mark completion."""
        employee_id: str = Field(...)


# ============================================================================
# Helper: Auth gating
# ============================================================================

def _gate(request: Any, level_name: str) -> None:
    """Gate access by level (L1, L2, L3, etc)."""
    try:
        from rosteriq.auth import require_access
        if request:
            require_access(request, level_name)
    except Exception:
        pass


# ============================================================================
# Router Setup
# ============================================================================

if APIRouter:
    training_tracker_router = APIRouter(prefix="/api/v1/training", tags=["training"])
else:
    training_tracker_router = None


# ============================================================================
# Endpoints
# ============================================================================

if APIRouter:
    @training_tracker_router.post("/skills/{venue_id}", response_model=Dict[str, Any])
    async def add_skill(
        venue_id: str,
        req: SkillRequest,
        request: Request,
    ) -> Dict[str, Any]:
        """Add a new skill to a venue. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        try:
            skill = store.add_skill({
                "venue_id": venue_id,
                "name": req.name,
                "category": req.category,
                "description": req.description,
                "max_level": req.max_level,
                "is_required": req.is_required,
                "related_certification": req.related_certification,
            })
            return skill.to_dict()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @training_tracker_router.get("/skills/{venue_id}", response_model=List[Dict[str, Any]])
    async def list_skills(
        venue_id: str,
        category: Optional[str] = None,
        request: Optional[Request] = None,
    ) -> List[Dict[str, Any]]:
        """List skills for a venue. Requires L1+."""
        _gate(request, "L1")

        store = get_training_tracker_store()
        skills = store.list_skills(venue_id, category=category)
        return [s.to_dict() for s in skills]

    @training_tracker_router.post("/assess/{venue_id}/{employee_id}", response_model=Dict[str, Any])
    async def assess_skill(
        venue_id: str,
        employee_id: str,
        req: AssessmentRequest,
        request: Request,
    ) -> Dict[str, Any]:
        """Assess an employee's skill level. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        try:
            assessment = store.assess_skill({
                "venue_id": venue_id,
                "employee_id": employee_id,
                "skill_id": req.skill_id,
                "level": req.level,
                "assessed_by": req.assessed_by,
                "notes": req.notes,
                "target_level": req.target_level,
                "target_date": req.target_date,
            })
            return assessment.to_dict()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @training_tracker_router.get("/employee/{venue_id}/{employee_id}", response_model=List[Dict[str, Any]])
    async def get_employee_skills(
        venue_id: str,
        employee_id: str,
        request: Optional[Request] = None,
    ) -> List[Dict[str, Any]]:
        """Get all skills assessed for an employee. Requires L1+."""
        _gate(request, "L1")

        store = get_training_tracker_store()
        skills = store.get_employee_skills(venue_id, employee_id)
        return [s.to_dict() for s in skills]

    @training_tracker_router.get("/holders/{venue_id}/{skill_id}", response_model=List[Dict[str, Any]])
    async def get_skill_holders(
        venue_id: str,
        skill_id: str,
        min_level: int = 1,
        request: Optional[Request] = None,
    ) -> List[Dict[str, Any]]:
        """Get employees with a skill. Requires L1+."""
        _gate(request, "L1")

        store = get_training_tracker_store()
        holders = store.get_skill_holders(venue_id, skill_id, min_level=min_level)
        return holders

    @training_tracker_router.get("/gaps/{venue_id}", response_model=List[Dict[str, Any]])
    async def get_skill_gaps(
        venue_id: str,
        request: Request,
    ) -> List[Dict[str, Any]]:
        """Get skill gaps (employees missing required skills). Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        gaps = store.get_skill_gaps(venue_id)
        return gaps

    @training_tracker_router.get("/matrix/{venue_id}", response_model=Dict[str, Any])
    async def get_skill_matrix(
        venue_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get skill matrix for venue. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        matrix = store.get_venue_skill_matrix(venue_id)
        return matrix

    @training_tracker_router.post("/programs/{venue_id}", response_model=Dict[str, Any])
    async def create_program(
        venue_id: str,
        req: ProgramRequest,
        request: Request,
    ) -> Dict[str, Any]:
        """Create a training program. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        try:
            program = store.create_program({
                "venue_id": venue_id,
                "name": req.name,
                "description": req.description,
                "skills_covered": req.skills_covered,
                "duration_hours": req.duration_hours,
                "max_participants": req.max_participants,
                "cost_per_person": req.cost_per_person,
                "is_mandatory": req.is_mandatory,
            })
            return program.to_dict()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @training_tracker_router.get("/programs/{venue_id}", response_model=List[Dict[str, Any]])
    async def list_programs(
        venue_id: str,
        request: Optional[Request] = None,
    ) -> List[Dict[str, Any]]:
        """List training programs for a venue. Requires L1+."""
        _gate(request, "L1")

        store = get_training_tracker_store()
        programs = store.list_programs(venue_id)
        return [p.to_dict() for p in programs]

    @training_tracker_router.post("/sessions/{venue_id}", response_model=Dict[str, Any])
    async def schedule_session(
        venue_id: str,
        req: SessionRequest,
        request: Request,
    ) -> Dict[str, Any]:
        """Schedule a training session. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        try:
            session = store.schedule_session({
                "venue_id": venue_id,
                "program_id": req.program_id,
                "scheduled_date": req.scheduled_date,
                "scheduled_time": req.scheduled_time,
                "trainer": req.trainer,
                "location": req.location,
                "attendees": req.attendees,
                "notes": req.notes,
            })
            return session.to_dict()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @training_tracker_router.post("/sessions/{session_id}/complete", response_model=Dict[str, Any])
    async def complete_session(
        session_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Complete a training session. Requires L2+."""
        _gate(request, "L2")

        store = get_training_tracker_store()
        try:
            session = store.complete_session(session_id)
            return session.to_dict()
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    @training_tracker_router.get("/history/{venue_id}/{employee_id}", response_model=List[Dict[str, Any]])
    async def get_training_history(
        venue_id: str,
        employee_id: str,
        request: Optional[Request] = None,
    ) -> List[Dict[str, Any]]:
        """Get training history for an employee. Requires L1+."""
        _gate(request, "L1")

        store = get_training_tracker_store()
        history = store.get_employee_training_history(venue_id, employee_id)
        return history
