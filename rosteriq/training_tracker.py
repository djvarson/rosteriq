"""Training & Skills Development Tracker for Australian hospitality venues.

Manages training programs, individual skill competency levels, training session
completion, and skill gap analysis. Links to certifications module for formal
qualifications.

Data persisted to SQLite for durability and compliance reporting.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.training_tracker")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class SkillCategory(str, Enum):
    """Categories of skills for hospitality venues."""
    BAR = "bar"
    KITCHEN = "kitchen"
    FLOOR = "floor"
    MANAGEMENT = "management"
    SAFETY = "safety"
    CUSTOMER_SERVICE = "customer_service"


class TrainingSessionStatus(str, Enum):
    """Status of a training session."""
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class Skill:
    """Represents a trainable skill at a venue."""
    id: str
    venue_id: str
    name: str
    category: SkillCategory
    description: Optional[str] = None
    max_level: int = 5
    is_required: bool = False
    related_certification: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "name": self.name,
            "category": self.category.value,
            "description": self.description,
            "max_level": self.max_level,
            "is_required": self.is_required,
            "related_certification": self.related_certification,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class EmployeeSkill:
    """Individual competency level in a skill."""
    id: str
    venue_id: str
    employee_id: str
    skill_id: str
    level: int  # 1-5
    assessed_by: str
    assessed_at: datetime
    notes: Optional[str] = None
    target_level: Optional[int] = None
    target_date: Optional[date] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": self.skill_id,
            "level": self.level,
            "assessed_by": self.assessed_by,
            "assessed_at": self.assessed_at.isoformat(),
            "notes": self.notes,
            "target_level": self.target_level,
            "target_date": self.target_date.isoformat() if self.target_date else None,
        }


@dataclass
class TrainingProgram:
    """A training program covering one or more skills."""
    id: str
    venue_id: str
    name: str
    description: Optional[str] = None
    skills_covered: List[str] = field(default_factory=list)  # Skill IDs
    duration_hours: float = 0.0
    max_participants: Optional[int] = None
    cost_per_person: float = 0.0
    is_mandatory: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "name": self.name,
            "description": self.description,
            "skills_covered": self.skills_covered,
            "duration_hours": self.duration_hours,
            "max_participants": self.max_participants,
            "cost_per_person": self.cost_per_person,
            "is_mandatory": self.is_mandatory,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class TrainingSession:
    """A scheduled instance of a training program."""
    id: str
    venue_id: str
    program_id: str
    scheduled_date: date
    scheduled_time: str  # HH:MM format
    status: TrainingSessionStatus = TrainingSessionStatus.SCHEDULED
    trainer: Optional[str] = None
    location: Optional[str] = None
    attendees: List[str] = field(default_factory=list)  # Employee IDs
    completions: List[str] = field(default_factory=list)  # Employee IDs
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "program_id": self.program_id,
            "scheduled_date": self.scheduled_date.isoformat(),
            "scheduled_time": self.scheduled_time,
            "status": self.status.value,
            "trainer": self.trainer,
            "location": self.location,
            "attendees": self.attendees,
            "completions": self.completions,
            "notes": self.notes,
            "created_at": self.created_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_TRAINING_TRACKER_SCHEMA = """
CREATE TABLE IF NOT EXISTS skills (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    description TEXT,
    max_level INTEGER DEFAULT 5,
    is_required BOOLEAN DEFAULT 0,
    related_certification TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_skill_venue ON skills(venue_id);
CREATE INDEX IF NOT EXISTS ix_skill_category ON skills(category);

CREATE TABLE IF NOT EXISTS employee_skills (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    level INTEGER NOT NULL,
    assessed_by TEXT NOT NULL,
    assessed_at TEXT NOT NULL,
    notes TEXT,
    target_level INTEGER,
    target_date TEXT
);
CREATE INDEX IF NOT EXISTS ix_emp_skill_venue ON employee_skills(venue_id);
CREATE INDEX IF NOT EXISTS ix_emp_skill_employee ON employee_skills(employee_id);
CREATE INDEX IF NOT EXISTS ix_emp_skill_skill ON employee_skills(skill_id);

CREATE TABLE IF NOT EXISTS training_programs (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    skills_covered TEXT,
    duration_hours REAL DEFAULT 0,
    max_participants INTEGER,
    cost_per_person REAL DEFAULT 0,
    is_mandatory BOOLEAN DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_program_venue ON training_programs(venue_id);

CREATE TABLE IF NOT EXISTS training_sessions (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    program_id TEXT NOT NULL,
    scheduled_date TEXT NOT NULL,
    scheduled_time TEXT NOT NULL,
    status TEXT DEFAULT 'scheduled',
    trainer TEXT,
    location TEXT,
    attendees TEXT,
    completions TEXT,
    notes TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_session_venue ON training_sessions(venue_id);
CREATE INDEX IF NOT EXISTS ix_session_program ON training_sessions(program_id);
CREATE INDEX IF NOT EXISTS ix_session_status ON training_sessions(status);
CREATE INDEX IF NOT EXISTS ix_session_date ON training_sessions(scheduled_date);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("training_tracker", _TRAINING_TRACKER_SCHEMA)
            def _rehydrate_on_init():
                store = get_training_tracker_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Training Tracker Store
# ---------------------------------------------------------------------------


class TrainingTrackerStore:
    """Thread-safe in-memory store for training data with persistence.

    Manages skills, employee assessments, training programs, and sessions.
    Persists to SQLite when persistence is enabled.
    """

    def __init__(self):
        self._skills: Dict[str, Skill] = {}
        self._employee_skills: Dict[str, EmployeeSkill] = {}
        self._programs: Dict[str, TrainingProgram] = {}
        self._sessions: Dict[str, TrainingSession] = {}
        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Skill Methods
    # -------------------------------------------------------------------------

    def _persist_skill(self, skill: Skill) -> None:
        """Persist a skill to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": skill.id,
            "venue_id": skill.venue_id,
            "name": skill.name,
            "category": skill.category.value,
            "description": skill.description,
            "max_level": skill.max_level,
            "is_required": skill.is_required,
            "related_certification": skill.related_certification,
            "created_at": skill.created_at.isoformat(),
        }
        try:
            _p.upsert("skills", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist skill %s: %s", skill.id, e)

    def add_skill(self, skill_dict: Dict[str, Any]) -> Skill:
        """Add a new skill."""
        skill = Skill(
            id=skill_dict.get("id") or f"skill_{uuid.uuid4().hex[:12]}",
            venue_id=skill_dict["venue_id"],
            name=skill_dict["name"],
            category=SkillCategory(skill_dict.get("category", "bar")),
            description=skill_dict.get("description"),
            max_level=skill_dict.get("max_level", 5),
            is_required=skill_dict.get("is_required", False),
            related_certification=skill_dict.get("related_certification"),
        )
        with self._lock:
            self._skills[skill.id] = skill
        self._persist_skill(skill)
        return skill

    def get_skill(self, skill_id: str) -> Optional[Skill]:
        """Get a skill by ID."""
        with self._lock:
            return self._skills.get(skill_id)

    def list_skills(self, venue_id: str, category: Optional[str] = None) -> List[Skill]:
        """List skills for a venue, optionally filtered by category."""
        with self._lock:
            skills = [s for s in self._skills.values() if s.venue_id == venue_id]
            if category:
                try:
                    cat = SkillCategory(category)
                    skills = [s for s in skills if s.category == cat]
                except ValueError:
                    pass
            return sorted(skills, key=lambda s: s.name)

    def update_skill(self, skill_id: str, updates: Dict[str, Any]) -> Skill:
        """Update a skill."""
        with self._lock:
            skill = self._skills.get(skill_id)
            if not skill:
                raise ValueError(f"Skill {skill_id} not found")
            for key, value in updates.items():
                if key == "category" and isinstance(value, str):
                    setattr(skill, key, SkillCategory(value))
                elif hasattr(skill, key):
                    setattr(skill, key, value)
        self._persist_skill(skill)
        return skill

    def delete_skill(self, skill_id: str) -> bool:
        """Delete a skill."""
        with self._lock:
            if skill_id not in self._skills:
                return False
            del self._skills[skill_id]

        _p = _get_persistence()
        if _p and _p.is_persistence_enabled():
            try:
                _p.execute("DELETE FROM skills WHERE id = ?", (skill_id,))
            except Exception as e:
                logger.warning("Failed to delete skill %s: %s", skill_id, e)
        return True

    # -------------------------------------------------------------------------
    # Employee Skill Methods
    # -------------------------------------------------------------------------

    def _persist_employee_skill(self, emp_skill: EmployeeSkill) -> None:
        """Persist employee skill to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": emp_skill.id,
            "venue_id": emp_skill.venue_id,
            "employee_id": emp_skill.employee_id,
            "skill_id": emp_skill.skill_id,
            "level": emp_skill.level,
            "assessed_by": emp_skill.assessed_by,
            "assessed_at": emp_skill.assessed_at.isoformat(),
            "notes": emp_skill.notes,
            "target_level": emp_skill.target_level,
            "target_date": emp_skill.target_date.isoformat() if emp_skill.target_date else None,
        }
        try:
            _p.upsert("employee_skills", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist employee skill %s: %s", emp_skill.id, e)

    def assess_skill(self, assessment_dict: Dict[str, Any]) -> EmployeeSkill:
        """Record or update an employee's skill competency."""
        emp_skill = EmployeeSkill(
            id=assessment_dict.get("id") or f"empskill_{uuid.uuid4().hex[:12]}",
            venue_id=assessment_dict["venue_id"],
            employee_id=assessment_dict["employee_id"],
            skill_id=assessment_dict["skill_id"],
            level=assessment_dict.get("level", 1),
            assessed_by=assessment_dict["assessed_by"],
            assessed_at=assessment_dict.get("assessed_at") or datetime.now(timezone.utc),
            notes=assessment_dict.get("notes"),
            target_level=assessment_dict.get("target_level"),
            target_date=assessment_dict.get("target_date"),
        )
        with self._lock:
            self._employee_skills[emp_skill.id] = emp_skill
        self._persist_employee_skill(emp_skill)
        return emp_skill

    def get_employee_skills(self, venue_id: str, employee_id: str) -> List[EmployeeSkill]:
        """Get all skills assessed for an employee."""
        with self._lock:
            skills = [
                es for es in self._employee_skills.values()
                if es.venue_id == venue_id and es.employee_id == employee_id
            ]
            return sorted(skills, key=lambda s: s.skill_id)

    def get_skill_holders(
        self, venue_id: str, skill_id: str, min_level: int = 1
    ) -> List[Dict[str, Any]]:
        """Get employees with a skill at or above min_level."""
        with self._lock:
            holders = [
                es for es in self._employee_skills.values()
                if (es.venue_id == venue_id and es.skill_id == skill_id
                    and es.level >= min_level)
            ]
            return [
                {
                    "employee_id": es.employee_id,
                    "level": es.level,
                    "assessed_at": es.assessed_at.isoformat(),
                }
                for es in sorted(holders, key=lambda x: -x.level)
            ]

    def get_skill_gaps(self, venue_id: str) -> List[Dict[str, Any]]:
        """Find employees missing required skills (level < 1)."""
        with self._lock:
            # Get required skills
            required_skills = [
                s for s in self._skills.values()
                if s.venue_id == venue_id and s.is_required
            ]

            # Get all employees at venue
            all_employees = set(
                es.employee_id for es in self._employee_skills.values()
                if es.venue_id == venue_id
            )

            gaps = []
            for skill in required_skills:
                skill_holders = {
                    es.employee_id for es in self._employee_skills.values()
                    if (es.venue_id == venue_id and es.skill_id == skill.id
                        and es.level >= 1)
                }
                missing_employees = all_employees - skill_holders
                for emp_id in missing_employees:
                    gaps.append({
                        "employee_id": emp_id,
                        "skill_id": skill.id,
                        "skill_name": skill.name,
                        "category": skill.category.value,
                    })
            return gaps

    def get_venue_skill_matrix(self, venue_id: str) -> Dict[str, Any]:
        """Get employees × skills competency grid."""
        with self._lock:
            skills = {
                s.id: s for s in self._skills.values()
                if s.venue_id == venue_id
            }
            emp_skills = [
                es for es in self._employee_skills.values()
                if es.venue_id == venue_id
            ]

            # Build matrix
            matrix: Dict[str, Dict[str, int]] = {}
            for emp_skill in emp_skills:
                emp_id = emp_skill.employee_id
                if emp_id not in matrix:
                    matrix[emp_id] = {}
                matrix[emp_id][emp_skill.skill_id] = emp_skill.level

            return {
                "venue_id": venue_id,
                "skills": {k: v.to_dict() for k, v in skills.items()},
                "matrix": matrix,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

    # -------------------------------------------------------------------------
    # Training Program Methods
    # -------------------------------------------------------------------------

    def _persist_program(self, program: TrainingProgram) -> None:
        """Persist training program to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": program.id,
            "venue_id": program.venue_id,
            "name": program.name,
            "description": program.description,
            "skills_covered": ",".join(program.skills_covered) if program.skills_covered else "",
            "duration_hours": program.duration_hours,
            "max_participants": program.max_participants,
            "cost_per_person": program.cost_per_person,
            "is_mandatory": program.is_mandatory,
            "created_at": program.created_at.isoformat(),
        }
        try:
            _p.upsert("training_programs", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist program %s: %s", program.id, e)

    def create_program(self, program_dict: Dict[str, Any]) -> TrainingProgram:
        """Create a new training program."""
        program = TrainingProgram(
            id=program_dict.get("id") or f"prog_{uuid.uuid4().hex[:12]}",
            venue_id=program_dict["venue_id"],
            name=program_dict["name"],
            description=program_dict.get("description"),
            skills_covered=program_dict.get("skills_covered", []),
            duration_hours=program_dict.get("duration_hours", 0.0),
            max_participants=program_dict.get("max_participants"),
            cost_per_person=program_dict.get("cost_per_person", 0.0),
            is_mandatory=program_dict.get("is_mandatory", False),
        )
        with self._lock:
            self._programs[program.id] = program
        self._persist_program(program)
        return program

    def get_program(self, program_id: str) -> Optional[TrainingProgram]:
        """Get a training program by ID."""
        with self._lock:
            return self._programs.get(program_id)

    def list_programs(self, venue_id: str) -> List[TrainingProgram]:
        """List all training programs for a venue."""
        with self._lock:
            programs = [
                p for p in self._programs.values()
                if p.venue_id == venue_id
            ]
            return sorted(programs, key=lambda p: p.name)

    def update_program(self, program_id: str, updates: Dict[str, Any]) -> TrainingProgram:
        """Update a training program."""
        with self._lock:
            program = self._programs.get(program_id)
            if not program:
                raise ValueError(f"Program {program_id} not found")
            for key, value in updates.items():
                if hasattr(program, key):
                    setattr(program, key, value)
        self._persist_program(program)
        return program

    # -------------------------------------------------------------------------
    # Training Session Methods
    # -------------------------------------------------------------------------

    def _persist_session(self, session: TrainingSession) -> None:
        """Persist training session to SQLite."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": session.id,
            "venue_id": session.venue_id,
            "program_id": session.program_id,
            "scheduled_date": session.scheduled_date.isoformat(),
            "scheduled_time": session.scheduled_time,
            "status": session.status.value,
            "trainer": session.trainer,
            "location": session.location,
            "attendees": ",".join(session.attendees) if session.attendees else "",
            "completions": ",".join(session.completions) if session.completions else "",
            "notes": session.notes,
            "created_at": session.created_at.isoformat(),
        }
        try:
            _p.upsert("training_sessions", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist session %s: %s", session.id, e)

    def schedule_session(self, session_dict: Dict[str, Any]) -> TrainingSession:
        """Schedule a new training session."""
        scheduled_date = session_dict.get("scheduled_date")
        if isinstance(scheduled_date, str):
            scheduled_date = date.fromisoformat(scheduled_date)

        session = TrainingSession(
            id=session_dict.get("id") or f"sess_{uuid.uuid4().hex[:12]}",
            venue_id=session_dict["venue_id"],
            program_id=session_dict["program_id"],
            scheduled_date=scheduled_date,
            scheduled_time=session_dict.get("scheduled_time", "09:00"),
            trainer=session_dict.get("trainer"),
            location=session_dict.get("location"),
            attendees=session_dict.get("attendees", []),
            completions=[],
            notes=session_dict.get("notes"),
        )
        with self._lock:
            self._sessions[session.id] = session
        self._persist_session(session)
        return session

    def get_session(self, session_id: str) -> Optional[TrainingSession]:
        """Get a training session by ID."""
        with self._lock:
            return self._sessions.get(session_id)

    def list_sessions(
        self,
        venue_id: str,
        status: Optional[str] = None,
        date_from: Optional[date] = None,
    ) -> List[TrainingSession]:
        """List training sessions for a venue."""
        with self._lock:
            sessions = [
                s for s in self._sessions.values()
                if s.venue_id == venue_id
            ]
            if status:
                try:
                    st = TrainingSessionStatus(status)
                    sessions = [s for s in sessions if s.status == st]
                except ValueError:
                    pass
            if date_from:
                sessions = [s for s in sessions if s.scheduled_date >= date_from]
            return sorted(sessions, key=lambda s: s.scheduled_date)

    def add_attendee(self, session_id: str, employee_id: str) -> TrainingSession:
        """Add an attendee to a training session."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")
            if employee_id not in session.attendees:
                session.attendees.append(employee_id)
        self._persist_session(session)
        return session

    def record_completion(self, session_id: str, employee_id: str) -> TrainingSession:
        """Mark an employee as completed, auto-bumping skill levels."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")

            if employee_id not in session.completions:
                session.completions.append(employee_id)

            # Auto-bump skill levels for all skills in the program
            program = self._programs.get(session.program_id)
            if program:
                for skill_id in program.skills_covered:
                    # Find existing assessment
                    existing = None
                    for es in self._employee_skills.values():
                        if (es.employee_id == employee_id and
                            es.skill_id == skill_id and
                            es.venue_id == session.venue_id):
                            existing = es
                            break

                    if existing:
                        # Bump level (max to skill's max_level)
                        skill = self._skills.get(skill_id)
                        if skill:
                            max_level = skill.max_level
                            new_level = min(existing.level + 1, max_level)
                            if new_level != existing.level:
                                existing.level = new_level
                                existing.assessed_at = datetime.now(timezone.utc)
                                self._persist_employee_skill(existing)

        self._persist_session(session)
        return session

    def complete_session(self, session_id: str) -> TrainingSession:
        """Mark a training session as completed."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")
            session.status = TrainingSessionStatus.COMPLETED
        self._persist_session(session)
        return session

    def cancel_session(self, session_id: str) -> TrainingSession:
        """Cancel a training session."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")
            session.status = TrainingSessionStatus.CANCELLED
        self._persist_session(session)
        return session

    def get_employee_training_history(
        self, venue_id: str, employee_id: str
    ) -> List[Dict[str, Any]]:
        """Get training session history for an employee."""
        with self._lock:
            history = []
            for session in self._sessions.values():
                if session.venue_id == venue_id and employee_id in session.attendees:
                    program = self._programs.get(session.program_id)
                    history.append({
                        "session_id": session.id,
                        "program_id": session.program_id,
                        "program_name": program.name if program else None,
                        "scheduled_date": session.scheduled_date.isoformat(),
                        "scheduled_time": session.scheduled_time,
                        "status": session.status.value,
                        "completed": employee_id in session.completions,
                        "trainer": session.trainer,
                        "location": session.location,
                    })
            return sorted(history, key=lambda h: h["scheduled_date"], reverse=True)

    def get_training_costs(
        self, venue_id: str, date_from: Optional[date] = None, date_to: Optional[date] = None
    ) -> Dict[str, Any]:
        """Calculate training costs for a venue."""
        with self._lock:
            total_cost = 0.0
            session_count = 0
            participant_count = 0

            for session in self._sessions.values():
                if session.venue_id != venue_id:
                    continue
                if session.status == TrainingSessionStatus.CANCELLED:
                    continue

                if date_from and session.scheduled_date < date_from:
                    continue
                if date_to and session.scheduled_date > date_to:
                    continue

                program = self._programs.get(session.program_id)
                if program:
                    session_cost = program.cost_per_person * len(session.attendees)
                    total_cost += session_cost
                    session_count += 1
                    participant_count += len(session.attendees)

            return {
                "venue_id": venue_id,
                "total_cost": round(total_cost, 2),
                "session_count": session_count,
                "participant_count": participant_count,
                "avg_cost_per_participant": round(total_cost / participant_count, 2) if participant_count > 0 else 0.0,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
            }

    # -------------------------------------------------------------------------
    # Rehydration from persistence
    # -------------------------------------------------------------------------

    def _rehydrate(self) -> None:
        """Load all data from SQLite. Called on startup."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            # Load skills
            for row in _p.fetchall("SELECT * FROM skills"):
                row_dict = dict(row)
                skill = Skill(
                    id=row_dict["id"],
                    venue_id=row_dict["venue_id"],
                    name=row_dict["name"],
                    category=SkillCategory(row_dict.get("category", "bar")),
                    description=row_dict.get("description"),
                    max_level=row_dict.get("max_level", 5),
                    is_required=row_dict.get("is_required", False),
                    related_certification=row_dict.get("related_certification"),
                )
                self._skills[skill.id] = skill

            # Load employee skills
            for row in _p.fetchall("SELECT * FROM employee_skills"):
                row_dict = dict(row)
                target_date = row_dict.get("target_date")
                if isinstance(target_date, str):
                    target_date = date.fromisoformat(target_date)

                emp_skill = EmployeeSkill(
                    id=row_dict["id"],
                    venue_id=row_dict["venue_id"],
                    employee_id=row_dict["employee_id"],
                    skill_id=row_dict["skill_id"],
                    level=row_dict.get("level", 1),
                    assessed_by=row_dict["assessed_by"],
                    assessed_at=datetime.fromisoformat(row_dict["assessed_at"]),
                    notes=row_dict.get("notes"),
                    target_level=row_dict.get("target_level"),
                    target_date=target_date,
                )
                self._employee_skills[emp_skill.id] = emp_skill

            # Load programs
            for row in _p.fetchall("SELECT * FROM training_programs"):
                row_dict = dict(row)
                skills_str = row_dict.get("skills_covered", "")
                skills_covered = [s.strip() for s in skills_str.split(",") if s.strip()]

                program = TrainingProgram(
                    id=row_dict["id"],
                    venue_id=row_dict["venue_id"],
                    name=row_dict["name"],
                    description=row_dict.get("description"),
                    skills_covered=skills_covered,
                    duration_hours=row_dict.get("duration_hours", 0.0),
                    max_participants=row_dict.get("max_participants"),
                    cost_per_person=row_dict.get("cost_per_person", 0.0),
                    is_mandatory=row_dict.get("is_mandatory", False),
                )
                self._programs[program.id] = program

            # Load sessions
            for row in _p.fetchall("SELECT * FROM training_sessions"):
                row_dict = dict(row)
                attendees_str = row_dict.get("attendees", "")
                attendees = [a.strip() for a in attendees_str.split(",") if a.strip()]
                completions_str = row_dict.get("completions", "")
                completions = [c.strip() for c in completions_str.split(",") if c.strip()]

                session = TrainingSession(
                    id=row_dict["id"],
                    venue_id=row_dict["venue_id"],
                    program_id=row_dict["program_id"],
                    scheduled_date=date.fromisoformat(row_dict["scheduled_date"]),
                    scheduled_time=row_dict.get("scheduled_time", "09:00"),
                    status=TrainingSessionStatus(row_dict.get("status", "scheduled")),
                    trainer=row_dict.get("trainer"),
                    location=row_dict.get("location"),
                    attendees=attendees,
                    completions=completions,
                    notes=row_dict.get("notes"),
                )
                self._sessions[session.id] = session

            logger.info(
                "Rehydrated %d skills, %d employee skills, %d programs, %d sessions",
                len(self._skills), len(self._employee_skills), len(self._programs),
                len(self._sessions)
            )
        except Exception as e:
            logger.warning("Failed to rehydrate training tracker: %s", e)


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[TrainingTrackerStore] = None
_store_lock = threading.Lock()


def get_training_tracker_store() -> TrainingTrackerStore:
    """Get the module-level training tracker store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = TrainingTrackerStore()
    return _store


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton for testing."""
    global _store
    with _store_lock:
        _store = TrainingTrackerStore.__new__(TrainingTrackerStore)
        _store._lock = threading.Lock()
        _store._skills = {}
        _store._employee_skills = {}
        _store._programs = {}
        _store._sessions = {}
