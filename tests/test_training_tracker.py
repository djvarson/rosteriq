"""
Tests for rosteriq.training_tracker — Skills, assessments, programs, sessions, and reporting.

Runs with: PYTHONPATH=. python3 -m unittest tests/test_training_tracker.py -v
Pure-stdlib unittest — no pytest required.
"""

from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import training_tracker as tt


# ---------------------------------------------------------------------------
# Test Fixtures
# ---------------------------------------------------------------------------

class TestTrainingTracker(unittest.TestCase):
    """Test suite for training tracker module."""

    def setUp(self):
        """Reset store before each test."""
        tt._reset_for_tests()
        self.store = tt.get_training_tracker_store()
        self.venue_id = "venue_001"
        self.employee_id = "emp_001"
        self.manager_id = "mgr_001"

    # -------------------------------------------------------------------------
    # Skill CRUD Tests
    # -------------------------------------------------------------------------

    def test_add_skill(self):
        """Test adding a skill."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        self.assertEqual(skill.name, "Cocktail Making")
        self.assertEqual(skill.venue_id, self.venue_id)
        self.assertTrue(skill.id)

    def test_add_skill_with_all_fields(self):
        """Test adding a skill with all optional fields."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Barista",
            "category": "bar",
            "description": "Coffee machine operation",
            "max_level": 5,
            "is_required": True,
            "related_certification": "cert_barista_001",
        })
        self.assertEqual(skill.description, "Coffee machine operation")
        self.assertTrue(skill.is_required)
        self.assertEqual(skill.related_certification, "cert_barista_001")

    def test_get_skill(self):
        """Test retrieving a skill."""
        added = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Glass Collection",
            "category": "bar",
        })
        retrieved = self.store.get_skill(added.id)
        self.assertEqual(retrieved.id, added.id)
        self.assertEqual(retrieved.name, "Glass Collection")

    def test_get_nonexistent_skill(self):
        """Test retrieving a nonexistent skill."""
        result = self.store.get_skill("nonexistent_id")
        self.assertIsNone(result)

    def test_list_skills(self):
        """Test listing skills for a venue."""
        self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Food Running",
            "category": "floor",
        })
        self.store.add_skill({
            "venue_id": "other_venue",
            "name": "Till Operation",
            "category": "bar",
        })

        skills = self.store.list_skills(self.venue_id)
        self.assertEqual(len(skills), 2)
        names = {s.name for s in skills}
        self.assertEqual(names, {"Cocktail Making", "Food Running"})

    def test_list_skills_filtered_by_category(self):
        """Test listing skills filtered by category."""
        self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Food Running",
            "category": "floor",
        })

        bar_skills = self.store.list_skills(self.venue_id, category="bar")
        self.assertEqual(len(bar_skills), 1)
        self.assertEqual(bar_skills[0].name, "Cocktail Making")

    def test_update_skill(self):
        """Test updating a skill."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Old Name",
            "category": "bar",
        })
        updated = self.store.update_skill(skill.id, {"name": "New Name", "max_level": 3})
        self.assertEqual(updated.name, "New Name")
        self.assertEqual(updated.max_level, 3)

    def test_delete_skill(self):
        """Test deleting a skill."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "To Delete",
            "category": "bar",
        })
        result = self.store.delete_skill(skill.id)
        self.assertTrue(result)
        self.assertIsNone(self.store.get_skill(skill.id))

    def test_delete_nonexistent_skill(self):
        """Test deleting a nonexistent skill."""
        result = self.store.delete_skill("nonexistent_id")
        self.assertFalse(result)

    # -------------------------------------------------------------------------
    # Employee Skill Assessment Tests
    # -------------------------------------------------------------------------

    def test_assess_skill(self):
        """Test assessing an employee's skill."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        assessment = self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill.id,
            "level": 3,
            "assessed_by": self.manager_id,
        })
        self.assertEqual(assessment.level, 3)
        self.assertEqual(assessment.employee_id, self.employee_id)

    def test_assess_skill_with_targets(self):
        """Test assessing with target level and date."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Barista",
            "category": "bar",
        })
        target_date = date.today() + timedelta(days=30)
        assessment = self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill.id,
            "level": 2,
            "assessed_by": self.manager_id,
            "target_level": 4,
            "target_date": target_date,
        })
        self.assertEqual(assessment.target_level, 4)
        self.assertEqual(assessment.target_date, target_date)

    def test_get_employee_skills(self):
        """Test retrieving all skills for an employee."""
        skill1 = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        skill2 = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Glass Collection",
            "category": "bar",
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill1.id,
            "level": 3,
            "assessed_by": self.manager_id,
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill2.id,
            "level": 2,
            "assessed_by": self.manager_id,
        })

        skills = self.store.get_employee_skills(self.venue_id, self.employee_id)
        self.assertEqual(len(skills), 2)

    def test_get_skill_holders(self):
        """Test getting employees with a skill."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_001",
            "skill_id": skill.id,
            "level": 4,
            "assessed_by": self.manager_id,
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_002",
            "skill_id": skill.id,
            "level": 2,
            "assessed_by": self.manager_id,
        })

        holders = self.store.get_skill_holders(self.venue_id, skill.id, min_level=3)
        self.assertEqual(len(holders), 1)
        self.assertEqual(holders[0]["employee_id"], "emp_001")

    # -------------------------------------------------------------------------
    # Skill Gap Analysis Tests
    # -------------------------------------------------------------------------

    def test_get_skill_gaps(self):
        """Test finding skill gaps (missing required skills)."""
        required_skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Food Safety",
            "category": "safety",
            "is_required": True,
        })
        optional_skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })

        # emp_001 has the optional skill but not the required skill
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_001",
            "skill_id": optional_skill.id,
            "level": 2,
            "assessed_by": self.manager_id,
        })
        # emp_002 has the required skill
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_002",
            "skill_id": required_skill.id,
            "level": 1,
            "assessed_by": self.manager_id,
        })

        gaps = self.store.get_skill_gaps(self.venue_id)
        # Should have gap for emp_001 (missing required skill)
        gap_employees = {g["employee_id"] for g in gaps}
        self.assertIn("emp_001", gap_employees)

    def test_no_gaps_when_all_required_present(self):
        """Test no gaps when all employees have required skills."""
        required_skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Food Safety",
            "category": "safety",
            "is_required": True,
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": required_skill.id,
            "level": 1,
            "assessed_by": self.manager_id,
        })

        gaps = self.store.get_skill_gaps(self.venue_id)
        gap_for_emp = [g for g in gaps if g["employee_id"] == self.employee_id]
        self.assertEqual(len(gap_for_emp), 0)

    # -------------------------------------------------------------------------
    # Skill Matrix Tests
    # -------------------------------------------------------------------------

    def test_get_venue_skill_matrix(self):
        """Test retrieving skill matrix."""
        skill1 = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        skill2 = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Barista",
            "category": "bar",
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_001",
            "skill_id": skill1.id,
            "level": 4,
            "assessed_by": self.manager_id,
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": "emp_001",
            "skill_id": skill2.id,
            "level": 2,
            "assessed_by": self.manager_id,
        })

        matrix = self.store.get_venue_skill_matrix(self.venue_id)
        self.assertEqual(matrix["venue_id"], self.venue_id)
        self.assertIn(skill1.id, matrix["skills"])
        self.assertIn(skill2.id, matrix["skills"])
        self.assertIn("emp_001", matrix["matrix"])

    # -------------------------------------------------------------------------
    # Training Program Tests
    # -------------------------------------------------------------------------

    def test_create_program(self):
        """Test creating a training program."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "description": "Introduction to cocktails",
            "skills_covered": [skill.id],
            "duration_hours": 4.0,
            "cost_per_person": 50.0,
        })
        self.assertEqual(program.name, "Mixology 101")
        self.assertIn(skill.id, program.skills_covered)

    def test_get_program(self):
        """Test retrieving a program."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Barista Training",
            "duration_hours": 8.0,
        })
        retrieved = self.store.get_program(program.id)
        self.assertEqual(retrieved.id, program.id)

    def test_list_programs(self):
        """Test listing programs for a venue."""
        self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Program 1",
        })
        self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Program 2",
        })
        self.store.create_program({
            "venue_id": "other_venue",
            "name": "Program 3",
        })

        programs = self.store.list_programs(self.venue_id)
        self.assertEqual(len(programs), 2)

    def test_update_program(self):
        """Test updating a program."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Old Name",
        })
        updated = self.store.update_program(program.id, {"name": "New Name", "duration_hours": 6.0})
        self.assertEqual(updated.name, "New Name")
        self.assertEqual(updated.duration_hours, 6.0)

    # -------------------------------------------------------------------------
    # Training Session Tests
    # -------------------------------------------------------------------------

    def test_schedule_session(self):
        """Test scheduling a training session."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
            "scheduled_time": "14:00",
            "trainer": "John Doe",
        })
        self.assertEqual(session.program_id, program.id)
        self.assertEqual(session.scheduled_time, "14:00")
        self.assertEqual(session.status, tt.TrainingSessionStatus.SCHEDULED)

    def test_add_attendee(self):
        """Test adding an attendee to a session."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        updated = self.store.add_attendee(session.id, self.employee_id)
        self.assertIn(self.employee_id, updated.attendees)

    def test_add_attendee_idempotent(self):
        """Test that adding same attendee twice doesn't duplicate."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)
        self.store.add_attendee(session.id, self.employee_id)
        session = self.store.get_session(session.id)
        count = sum(1 for a in session.attendees if a == self.employee_id)
        self.assertEqual(count, 1)

    def test_record_completion(self):
        """Test marking completion."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
        })
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "skills_covered": [skill.id],
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)

        updated = self.store.record_completion(session.id, self.employee_id)
        self.assertIn(self.employee_id, updated.completions)

    def test_record_completion_auto_bumps_skill_level(self):
        """Test that completion auto-bumps skill levels."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
            "max_level": 5,
        })
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "skills_covered": [skill.id],
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill.id,
            "level": 2,
            "assessed_by": self.manager_id,
        })

        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)
        self.store.record_completion(session.id, self.employee_id)

        emp_skills = self.store.get_employee_skills(self.venue_id, self.employee_id)
        cocktail_skill = [s for s in emp_skills if s.skill_id == skill.id][0]
        self.assertEqual(cocktail_skill.level, 3)

    def test_auto_level_bump_respects_max(self):
        """Test that auto-bump doesn't exceed max_level."""
        skill = self.store.add_skill({
            "venue_id": self.venue_id,
            "name": "Cocktail Making",
            "category": "bar",
            "max_level": 3,
        })
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "skills_covered": [skill.id],
        })
        self.store.assess_skill({
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "skill_id": skill.id,
            "level": 3,
            "assessed_by": self.manager_id,
        })

        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)
        self.store.record_completion(session.id, self.employee_id)

        emp_skills = self.store.get_employee_skills(self.venue_id, self.employee_id)
        cocktail_skill = [s for s in emp_skills if s.skill_id == skill.id][0]
        self.assertEqual(cocktail_skill.level, 3)  # Should stay at max

    def test_complete_session(self):
        """Test completing a session."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        completed = self.store.complete_session(session.id)
        self.assertEqual(completed.status, tt.TrainingSessionStatus.COMPLETED)

    def test_cancel_session(self):
        """Test cancelling a session."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        cancelled = self.store.cancel_session(session.id)
        self.assertEqual(cancelled.status, tt.TrainingSessionStatus.CANCELLED)

    def test_list_sessions(self):
        """Test listing sessions."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.schedule_session({
            "venue_id": "other_venue",
            "program_id": program.id,
            "scheduled_date": date.today(),
        })

        sessions = self.store.list_sessions(self.venue_id)
        self.assertEqual(len(sessions), 2)

    def test_list_sessions_filtered_by_status(self):
        """Test listing sessions filtered by status."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session1 = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        session2 = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.complete_session(session1.id)

        completed = self.store.list_sessions(self.venue_id, status="completed")
        self.assertEqual(len(completed), 1)

    # -------------------------------------------------------------------------
    # Training History & Costs Tests
    # -------------------------------------------------------------------------

    def test_get_employee_training_history(self):
        """Test retrieving employee training history."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)

        history = self.store.get_employee_training_history(self.venue_id, self.employee_id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["program_id"], program.id)

    def test_get_training_costs(self):
        """Test calculating training costs."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "cost_per_person": 50.0,
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, "emp_001")
        self.store.add_attendee(session.id, "emp_002")

        costs = self.store.get_training_costs(self.venue_id)
        self.assertEqual(costs["total_cost"], 100.0)
        self.assertEqual(costs["participant_count"], 2)

    def test_get_training_costs_with_date_range(self):
        """Test calculating costs within a date range."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "cost_per_person": 50.0,
        })
        today = date.today()
        self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": today,
        })
        self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": today + timedelta(days=10),
        })

        date_from = today
        date_to = today + timedelta(days=5)
        costs = self.store.get_training_costs(self.venue_id, date_from=date_from, date_to=date_to)
        self.assertEqual(costs["session_count"], 1)

    def test_cancelled_sessions_not_counted_in_costs(self):
        """Test that cancelled sessions don't count toward costs."""
        program = self.store.create_program({
            "venue_id": self.venue_id,
            "name": "Mixology 101",
            "cost_per_person": 50.0,
        })
        session = self.store.schedule_session({
            "venue_id": self.venue_id,
            "program_id": program.id,
            "scheduled_date": date.today(),
        })
        self.store.add_attendee(session.id, self.employee_id)
        self.store.cancel_session(session.id)

        costs = self.store.get_training_costs(self.venue_id)
        self.assertEqual(costs["total_cost"], 0.0)


# ---------------------------------------------------------------------------
# Run Tests
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
