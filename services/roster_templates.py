"""
Roster template service for RosterIQ.

Provides functionality to create, manage, and apply roster templates to generate
consistent rosters across multiple weeks and venues. Templates extract repeating
shift patterns from existing rosters or are created manually.

A RosterTemplate consists of:
- Metadata (id, name, venue, description, creator, timestamps)
- ShiftPatterns: recurring shifts defined by role, day-of-week, time, employee count, skills
"""

from dataclasses import dataclass, field
from datetime import date, time, datetime, timedelta
from typing import Optional, List, Dict, Any
from decimal import Decimal
import uuid
import logging

from rosteriq.models import (
    Employee, Shift, Roster, ShiftStatus, EmploymentType, AwardLevel, State,
)
from rosteriq.roster_optimiser import generate_weekly_roster, SHIFT_TEMPLATES
from rosteriq.database import BaseStore

logger = logging.getLogger(__name__)


# ============================================================================
# Data models
# ============================================================================


@dataclass
class ShiftPattern:
    """A recurring shift pattern within a template."""

    role: str
    day_of_week: int  # 0=Monday, 6=Sunday
    start_time: time
    end_time: time
    employee_count: int
    skills_required: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "role": self.role,
            "day_of_week": self.day_of_week,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "employee_count": self.employee_count,
            "skills_required": self.skills_required,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ShiftPattern":
        """Create from dictionary storage format."""
        start_time = data["start_time"]
        if isinstance(start_time, str):
            start_time = time.fromisoformat(start_time)
        end_time = data["end_time"]
        if isinstance(end_time, str):
            end_time = time.fromisoformat(end_time)

        return cls(
            role=data["role"],
            day_of_week=data["day_of_week"],
            start_time=start_time,
            end_time=end_time,
            employee_count=data["employee_count"],
            skills_required=data.get("skills_required", []),
        )


@dataclass
class RosterTemplate:
    """A reusable roster template for a venue."""

    id: str
    name: str
    venue_id: str
    description: str
    created_by: str
    created_at: datetime
    shift_patterns: List[ShiftPattern]
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "name": self.name,
            "venue_id": self.venue_id,
            "description": self.description,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "shift_patterns": [p.to_dict() for p in self.shift_patterns],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RosterTemplate":
        """Create from dictionary storage format."""
        created_at = data["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        return cls(
            id=data["id"],
            name=data["name"],
            venue_id=data["venue_id"],
            description=data["description"],
            created_by=data["created_by"],
            created_at=created_at,
            updated_at=updated_at,
            shift_patterns=[ShiftPattern.from_dict(p) for p in data.get("shift_patterns", [])],
        )


# ============================================================================
# Template service
# ============================================================================


class RosterTemplateService:
    """Service for managing roster templates."""

    def __init__(self, db: BaseStore):
        """Initialize with a database store."""
        self.db = db

    def create_template(
        self,
        name: str,
        venue_id: str,
        shift_patterns: List[ShiftPattern],
        description: str = "",
        created_by: str = "system",
    ) -> RosterTemplate:
        """
        Create a new roster template.

        Args:
            name: Template name
            venue_id: Venue ID this template is for
            shift_patterns: List of recurring shift patterns
            description: Optional description
            created_by: User ID or system identifier

        Returns:
            Created RosterTemplate
        """
        template = RosterTemplate(
            id=str(uuid.uuid4()),
            name=name,
            venue_id=venue_id,
            description=description,
            created_by=created_by,
            created_at=datetime.utcnow(),
            shift_patterns=shift_patterns,
        )

        self.db.save_roster_template(template.to_dict())
        logger.info(f"Created template {template.id}: {name} for venue {venue_id}")
        return template

    def create_from_roster(
        self,
        roster_id: str,
        name: str,
        description: str = "",
        created_by: str = "system",
    ) -> Optional[RosterTemplate]:
        """
        Extract shift patterns from an existing roster and create a template.

        Analyzes the roster to identify repeating patterns by day-of-week,
        role, and time.

        Args:
            roster_id: ID of roster to extract patterns from
            name: Name for the new template
            description: Optional description
            created_by: User ID or system identifier

        Returns:
            Created RosterTemplate, or None if roster not found
        """
        roster = self.db.get_roster(roster_id)
        if not roster:
            logger.warning(f"Roster {roster_id} not found")
            return None

        # Group shifts by day-of-week and role
        patterns_by_day_role: Dict[tuple, List[Shift]] = {}
        for shift in roster.shifts:
            shift_date = shift.date
            # Calculate day_of_week from shift date and week_start
            days_since_week_start = (shift_date - roster.week_start).days
            day_of_week = days_since_week_start % 7
            key = (day_of_week, shift.role)

            if key not in patterns_by_day_role:
                patterns_by_day_role[key] = []
            patterns_by_day_role[key].append(shift)

        # Convert grouped shifts to patterns
        shift_patterns = []
        for (day_of_week, role), shifts in patterns_by_day_role.items():
            # Use the most common time window for this role on this day
            start_times = [s.start_time for s in shifts]
            end_times = [s.end_time for s in shifts]

            # Simple approach: use median/mode time
            start_times.sort()
            end_times.sort()
            start_time = start_times[len(start_times) // 2]
            end_time = end_times[len(end_times) // 2]

            pattern = ShiftPattern(
                role=role,
                day_of_week=day_of_week,
                start_time=start_time,
                end_time=end_time,
                employee_count=len(shifts),
                skills_required=[],
            )
            shift_patterns.append(pattern)

        # Create and save template
        template = RosterTemplate(
            id=str(uuid.uuid4()),
            name=name,
            venue_id=roster.venue_id,
            description=description or f"Created from roster {roster_id}",
            created_by=created_by,
            created_at=datetime.utcnow(),
            shift_patterns=shift_patterns,
        )

        self.db.save_roster_template(template.to_dict())
        logger.info(
            f"Created template {template.id} from roster {roster_id}: {name}"
        )
        return template

    def apply_template(
        self,
        template_id: str,
        week_start_date: date,
        venue_id: str,
        employees: Optional[List[Employee]] = None,
    ) -> Optional[Roster]:
        """
        Apply a template to generate a roster for a specific week.

        Creates shifts for each pattern in the template, scheduled for the
        corresponding day in the week starting week_start_date.

        Args:
            template_id: ID of template to apply
            week_start_date: Start date of week to generate roster for
            venue_id: Venue ID (must match template venue)
            employees: Optional list of employees. If not provided, fetches from DB.

        Returns:
            Generated Roster, or None if template not found
        """
        template = self.get_template(template_id)
        if not template:
            logger.warning(f"Template {template_id} not found")
            return None

        if template.venue_id != venue_id:
            logger.warning(
                f"Template venue {template.venue_id} does not match request venue {venue_id}"
            )
            return None

        # Get employees if not provided
        if employees is None:
            employees = self.db.list_employees()
            employees = [e for e in employees if hasattr(e, 'venue_id') is False or e.venue_id == venue_id]

        # Create roster with shifts from template
        roster = Roster(
            id=str(uuid.uuid4()),
            venue_id=venue_id,
            week_start=week_start_date,
            week_end=week_start_date + timedelta(days=6),
            shifts=[],
            created_at=datetime.utcnow(),
        )

        # For each pattern, create shifts
        for pattern in template.shift_patterns:
            shift_date = week_start_date + timedelta(days=pattern.day_of_week)

            # Create employee_count shifts for this pattern
            for i in range(pattern.employee_count):
                # Simple round-robin assignment
                suitable_employees = [
                    e for e in employees
                    if (not pattern.skills_required or
                        any(skill in e.skills for skill in pattern.skills_required))
                ]

                if suitable_employees:
                    employee = suitable_employees[i % len(suitable_employees)]
                    shift = Shift(
                        id=str(uuid.uuid4()),
                        employee_id=employee.id,
                        date=shift_date,
                        start_time=pattern.start_time,
                        end_time=pattern.end_time,
                        break_minutes=30,  # Default
                        status=ShiftStatus.scheduled,
                        role=pattern.role,
                        cost=None,  # Will be calculated
                    )
                    roster.shifts.append(shift)

        self.db.save_roster(roster)
        logger.info(
            f"Applied template {template_id} to generate roster {roster.id}"
        )
        return roster

    def list_templates(self, venue_id: str) -> List[RosterTemplate]:
        """List all templates for a venue."""
        templates_data = self.db.list_roster_templates(venue_id)
        return [RosterTemplate.from_dict(t) for t in templates_data]

    def get_template(self, template_id: str) -> Optional[RosterTemplate]:
        """Get a single template by ID."""
        template_data = self.db.get_roster_template(template_id)
        if template_data:
            return RosterTemplate.from_dict(template_data)
        return None

    def delete_template(self, template_id: str) -> bool:
        """Delete a template by ID. Returns True if deleted, False if not found."""
        self.db.delete_roster_template(template_id)
        logger.info(f"Deleted template {template_id}")
        return True

    def duplicate_template(self, template_id: str, new_name: str) -> Optional[RosterTemplate]:
        """
        Create a copy of a template with a new name.

        Args:
            template_id: ID of template to duplicate
            new_name: Name for the new copy

        Returns:
            New RosterTemplate, or None if original not found
        """
        original = self.get_template(template_id)
        if not original:
            logger.warning(f"Template {template_id} not found for duplication")
            return None

        # Create copy with new ID and name
        new_template = RosterTemplate(
            id=str(uuid.uuid4()),
            name=new_name,
            venue_id=original.venue_id,
            description=original.description,
            created_by=original.created_by,
            created_at=datetime.utcnow(),
            shift_patterns=original.shift_patterns,
        )

        self.db.save_roster_template(new_template.to_dict())
        logger.info(
            f"Duplicated template {template_id} to {new_template.id} with name: {new_name}"
        )
        return new_template
