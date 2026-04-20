"""Test suite for incident_log.py module (Round 35).

26 comprehensive test cases covering:
- Incident reporting and validation
- Notifiable flagging (CRITICAL, CRITICAL with injury, NOTIFIABLE)
- Status transitions
- Corrective actions lifecycle (add, complete, overdue)
- Overdue detection and marking
- Summary aggregation (by severity, category, location, status)
- Store persistence (SQLite roundtrip)
- Timeline generation (chronological events)
- Edge cases (no incidents, no actions, empty filters)
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, date, timezone, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.incident_log import (
    get_incident_store,
    _reset_for_tests,
    Incident,
    IncidentSeverity,
    IncidentCategory,
    IncidentStatus,
    CorrectiveAction,
    CorrectiveActionStatus,
    report_incident,
    update_incident,
    add_corrective_action,
    complete_corrective_action,
    check_overdue_actions,
    get_incident_timeline,
    build_incident_summary,
)
from rosteriq import persistence as _p


class TestIncidentReporting(unittest.TestCase):
    """Test incident reporting and basic operations."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store and persistence before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_report_incident_basic(self):
        """Test reporting a basic incident with all fields."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime(2026, 4, 20, 14, 30, tzinfo=timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Minor burn on arm",
            injured_person="Alice",
            injury_description="Burn mark on forearm",
            witnesses=["Bob", "Charlie"],
            immediate_action="Applied cold water, bandaged",
        )

        self.assertTrue(incident.incident_id.startswith("inc_"))
        self.assertEqual(incident.venue_id, "venue_001")
        self.assertEqual(incident.reported_by_name, "Alice")
        self.assertEqual(incident.category, IncidentCategory.BURN)
        self.assertEqual(incident.severity, IncidentSeverity.MINOR)
        self.assertFalse(incident.is_notifiable)
        self.assertEqual(incident.status, IncidentStatus.REPORTED)

    def test_report_incident_minimal(self):
        """Test reporting incident with minimal fields."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.NEAR_MISS,
            description="Customer slipped on wet floor",
        )

        self.assertEqual(incident.venue_id, "venue_002")
        self.assertEqual(incident.location, "bar")
        self.assertIsNone(incident.injured_person)
        self.assertEqual(incident.witnesses, [])
        self.assertEqual(incident.immediate_action, "")

    def test_report_incident_retrieval(self):
        """Test retrieving a reported incident."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="loading dock",
            category=IncidentCategory.MANUAL_HANDLING,
            severity=IncidentSeverity.MODERATE,
            description="Back strain lifting boxes",
        )

        store = get_incident_store()
        retrieved = store.get_incident(incident.incident_id)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.incident_id, incident.incident_id)
        self.assertEqual(retrieved.venue_id, "venue_003")
        self.assertEqual(retrieved.severity, IncidentSeverity.MODERATE)

    def test_report_incident_not_found(self):
        """Test retrieving a non-existent incident."""
        store = get_incident_store()
        retrieved = store.get_incident("inc_nonexistent")
        self.assertIsNone(retrieved)


class TestNotifiableFlagging(unittest.TestCase):
    """Test notifiable incident detection per AU WHS."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_critical_severity_notifiable(self):
        """Test that CRITICAL severity is marked notifiable."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.ELECTRICAL,
            severity=IncidentSeverity.CRITICAL,
            description="Electrocution",
            injured_person="Alice",
        )

        self.assertTrue(incident.is_notifiable)

    def test_notifiable_severity_notifiable(self):
        """Test that NOTIFIABLE severity is marked notifiable."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.ASSAULT,
            severity=IncidentSeverity.NOTIFIABLE,
            description="Physical assault",
            injured_person="Bob",
        )

        self.assertTrue(incident.is_notifiable)

    def test_serious_with_injury_notifiable(self):
        """Test that SERIOUS + injury is marked notifiable."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="loading dock",
            category=IncidentCategory.MANUAL_HANDLING,
            severity=IncidentSeverity.SERIOUS,
            description="Severe back injury",
            injured_person="Carol",
            injury_description="Hospitalization required",
        )

        self.assertTrue(incident.is_notifiable)

    def test_serious_without_injury_not_notifiable(self):
        """Test that SERIOUS without injury is not notifiable."""
        incident = report_incident(
            venue_id="venue_004",
            reported_by="emp_004",
            reported_by_name="Dave",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.EQUIPMENT_FAILURE,
            severity=IncidentSeverity.SERIOUS,
            description="Equipment malfunction",
        )

        self.assertFalse(incident.is_notifiable)

    def test_minor_not_notifiable(self):
        """Test that MINOR is not notifiable."""
        incident = report_incident(
            venue_id="venue_005",
            reported_by="emp_005",
            reported_by_name="Eve",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CUT_LACERATION,
            severity=IncidentSeverity.MINOR,
            description="Minor cut",
        )

        self.assertFalse(incident.is_notifiable)


class TestIncidentStatusTransitions(unittest.TestCase):
    """Test incident status updates."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_update_incident_status(self):
        """Test updating incident status."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        self.assertEqual(incident.status, IncidentStatus.REPORTED)

        updated = update_incident(
            incident.incident_id, status=IncidentStatus.UNDER_INVESTIGATION.value
        )

        self.assertIsNotNone(updated)
        self.assertEqual(updated.status, IncidentStatus.UNDER_INVESTIGATION)

    def test_update_incident_severity(self):
        """Test updating incident severity."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        updated = update_incident(
            incident.incident_id, severity=IncidentSeverity.SERIOUS.value
        )

        self.assertIsNotNone(updated)
        self.assertEqual(updated.severity, IncidentSeverity.SERIOUS)
        # SERIOUS + no injury should still not be notifiable
        self.assertFalse(updated.is_notifiable)

    def test_update_incident_multiple_fields(self):
        """Test updating multiple fields at once."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CUT_LACERATION,
            severity=IncidentSeverity.MINOR,
            description="Initial description",
            immediate_action="Initial action",
        )

        updated = update_incident(
            incident.incident_id,
            description="Updated description",
            immediate_action="Updated action",
            status=IncidentStatus.CORRECTIVE_ACTION.value,
        )

        self.assertEqual(updated.description, "Updated description")
        self.assertEqual(updated.immediate_action, "Updated action")
        self.assertEqual(updated.status, IncidentStatus.CORRECTIVE_ACTION)

    def test_update_incident_not_found(self):
        """Test updating non-existent incident."""
        updated = update_incident("inc_nonexistent", status="reported")
        self.assertIsNone(updated)


class TestCorrectiveActionsLifecycle(unittest.TestCase):
    """Test corrective action add, complete, and lifecycle."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_add_corrective_action(self):
        """Test adding a corrective action to an incident."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Install heat guard on oven",
            assigned_to="Bob",
            due_date=date.today() + timedelta(days=7),
        )

        self.assertTrue(action.action_id.startswith("act_"))
        self.assertEqual(action.incident_id, incident.incident_id)
        self.assertEqual(action.assigned_to, "Bob")
        self.assertEqual(action.status, CorrectiveActionStatus.PENDING)

    def test_complete_corrective_action(self):
        """Test completing a corrective action."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Place wet floor sign",
            assigned_to="Charlie",
            due_date=date.today() + timedelta(days=1),
        )

        self.assertEqual(action.status, CorrectiveActionStatus.PENDING)

        completed = complete_corrective_action(action.action_id, date.today())

        self.assertIsNotNone(completed)
        self.assertEqual(completed.status, CorrectiveActionStatus.COMPLETED)
        self.assertEqual(completed.completed_date, date.today())

    def test_complete_action_default_date(self):
        """Test completing action with default (today) date."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CUT_LACERATION,
            severity=IncidentSeverity.MINOR,
            description="Cut",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Replace dull knife",
            assigned_to="Dave",
            due_date=date.today(),
        )

        completed = complete_corrective_action(action.action_id)

        self.assertEqual(completed.status, CorrectiveActionStatus.COMPLETED)
        self.assertEqual(completed.completed_date, date.today())

    def test_complete_action_not_found(self):
        """Test completing non-existent action."""
        completed = complete_corrective_action("act_nonexistent")
        self.assertIsNone(completed)

    def test_multiple_actions_per_incident(self):
        """Test adding multiple actions to one incident."""
        incident = report_incident(
            venue_id="venue_004",
            reported_by="emp_004",
            reported_by_name="Eve",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CHEMICAL_EXPOSURE,
            severity=IncidentSeverity.MODERATE,
            description="Chemical spill",
        )

        action1 = add_corrective_action(
            incident_id=incident.incident_id,
            description="Clean and neutralize spill",
            assigned_to="Frank",
            due_date=date.today(),
        )

        action2 = add_corrective_action(
            incident_id=incident.incident_id,
            description="Review storage procedures",
            assigned_to="Grace",
            due_date=date.today() + timedelta(days=7),
        )

        store = get_incident_store()
        self.assertEqual(len(store.actions), 2)
        self.assertEqual(action1.incident_id, action2.incident_id)


class TestOverdueDetection(unittest.TestCase):
    """Test overdue action detection and marking."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_check_overdue_actions_empty(self):
        """Test checking overdue actions with none present."""
        overdue = check_overdue_actions("venue_001")
        self.assertEqual(len(overdue), 0)

    def test_check_overdue_actions_past_due(self):
        """Test detecting action past due date."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        # Action due 5 days ago
        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Fix oven thermostat",
            assigned_to="Bob",
            due_date=date.today() - timedelta(days=5),
        )

        overdue = check_overdue_actions("venue_001")

        self.assertEqual(len(overdue), 1)
        self.assertEqual(overdue[0].action_id, action.action_id)
        self.assertEqual(overdue[0].status, CorrectiveActionStatus.OVERDUE)

    def test_check_overdue_actions_not_yet_due(self):
        """Test that future-due actions are not marked overdue."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Install non-slip mat",
            assigned_to="Charlie",
            due_date=date.today() + timedelta(days=7),
        )

        overdue = check_overdue_actions("venue_002")

        self.assertEqual(len(overdue), 0)
        self.assertEqual(action.status, CorrectiveActionStatus.PENDING)

    def test_check_overdue_completed_not_included(self):
        """Test that completed actions are not marked overdue even if past due."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CUT_LACERATION,
            severity=IncidentSeverity.MINOR,
            description="Cut",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Sharpen knife",
            assigned_to="Dave",
            due_date=date.today() - timedelta(days=3),
        )

        # Mark as completed yesterday
        complete_corrective_action(action.action_id, date.today() - timedelta(days=1))

        overdue = check_overdue_actions("venue_003")

        self.assertEqual(len(overdue), 0)

    def test_check_overdue_filters_by_venue(self):
        """Test that overdue check only includes venue's actions."""
        # Incident at venue_001
        inc1 = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        # Incident at venue_002
        inc2 = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        # Add overdue action to venue_001
        add_corrective_action(
            incident_id=inc1.incident_id,
            description="Action 1",
            assigned_to="Carol",
            due_date=date.today() - timedelta(days=1),
        )

        # Add overdue action to venue_002
        add_corrective_action(
            incident_id=inc2.incident_id,
            description="Action 2",
            assigned_to="Dave",
            due_date=date.today() - timedelta(days=1),
        )

        overdue1 = check_overdue_actions("venue_001")
        overdue2 = check_overdue_actions("venue_002")

        self.assertEqual(len(overdue1), 1)
        self.assertEqual(len(overdue2), 1)
        self.assertNotEqual(overdue1[0].action_id, overdue2[0].action_id)


class TestIncidentSummary(unittest.TestCase):
    """Test incident summary aggregation and reporting."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_summary_empty_venue(self):
        """Test summary for venue with no incidents."""
        summary = build_incident_summary("venue_empty")

        self.assertEqual(summary.venue_id, "venue_empty")
        self.assertEqual(summary.total_incidents, 0)
        self.assertEqual(summary.by_severity, {})
        self.assertEqual(summary.by_category, {})
        self.assertEqual(summary.notifiable_count, 0)

    def test_summary_aggregates_by_severity(self):
        """Test summary counts incidents by severity."""
        # Add incidents with different severities
        for sev in [IncidentSeverity.MINOR, IncidentSeverity.MINOR, IncidentSeverity.MODERATE]:
            report_incident(
                venue_id="venue_001",
                reported_by="emp_001",
                reported_by_name="Alice",
                date_occurred=datetime.now(timezone.utc),
                location="kitchen",
                category=IncidentCategory.BURN,
                severity=sev,
                description=f"Incident with {sev.value}",
            )

        summary = build_incident_summary("venue_001")

        self.assertEqual(summary.total_incidents, 3)
        self.assertEqual(summary.by_severity["minor"], 2)
        self.assertEqual(summary.by_severity["moderate"], 1)

    def test_summary_aggregates_by_category(self):
        """Test summary counts incidents by category."""
        for cat in [IncidentCategory.BURN, IncidentCategory.BURN, IncidentCategory.SLIP_TRIP_FALL]:
            report_incident(
                venue_id="venue_002",
                reported_by="emp_002",
                reported_by_name="Bob",
                date_occurred=datetime.now(timezone.utc),
                location="kitchen",
                category=cat,
                severity=IncidentSeverity.MINOR,
                description=f"Incident: {cat.value}",
            )

        summary = build_incident_summary("venue_002")

        self.assertEqual(summary.total_incidents, 3)
        self.assertEqual(summary.by_category["burn"], 2)
        self.assertEqual(summary.by_category["slip_trip_fall"], 1)

    def test_summary_counts_notifiable(self):
        """Test summary counts notifiable incidents."""
        # Non-notifiable
        report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Not notifiable",
        )

        # Notifiable
        report_incident(
            venue_id="venue_003",
            reported_by="emp_004",
            reported_by_name="Dave",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.ELECTRICAL,
            severity=IncidentSeverity.CRITICAL,
            description="Critical incident",
            injured_person="Dave",
        )

        summary = build_incident_summary("venue_003")

        self.assertEqual(summary.total_incidents, 2)
        self.assertEqual(summary.notifiable_count, 1)

    def test_summary_counts_open_actions(self):
        """Test summary counts open (non-completed) corrective actions."""
        incident = report_incident(
            venue_id="venue_004",
            reported_by="emp_005",
            reported_by_name="Eve",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        # Add 2 actions, complete 1
        action1 = add_corrective_action(
            incident_id=incident.incident_id,
            description="Fix oven",
            assigned_to="Frank",
            due_date=date.today() + timedelta(days=7),
        )

        action2 = add_corrective_action(
            incident_id=incident.incident_id,
            description="Training",
            assigned_to="Grace",
            due_date=date.today() + timedelta(days=14),
        )

        complete_corrective_action(action1.action_id)

        summary = build_incident_summary("venue_004")

        self.assertEqual(summary.open_actions, 1)

    def test_summary_incident_rate(self):
        """Test summary calculates incident rate per 1000 hours."""
        # Add 5 incidents
        for i in range(5):
            report_incident(
                venue_id="venue_005",
                reported_by=f"emp_{i}",
                reported_by_name=f"Employee {i}",
                date_occurred=datetime.now(timezone.utc),
                location="kitchen",
                category=IncidentCategory.BURN,
                severity=IncidentSeverity.MINOR,
                description=f"Incident {i}",
            )

        # With 1000 hours worked, rate should be (5 / 1000) * 1000 = 5
        summary = build_incident_summary("venue_005", hours_worked=1000)

        self.assertEqual(summary.incident_rate, 5.0)

    def test_summary_incident_rate_zero_hours(self):
        """Test summary with zero hours (no rate calculated)."""
        report_incident(
            venue_id="venue_006",
            reported_by="emp_006",
            reported_by_name="Helen",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        summary = build_incident_summary("venue_006", hours_worked=0)

        self.assertIsNone(summary.incident_rate)


class TestTimeline(unittest.TestCase):
    """Test incident timeline generation."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_timeline_empty_for_nonexistent_incident(self):
        """Test timeline for non-existent incident."""
        timeline = get_incident_timeline("inc_nonexistent")
        self.assertEqual(len(timeline), 0)

    def test_timeline_incident_reported(self):
        """Test timeline includes incident reported event."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        timeline = get_incident_timeline(incident.incident_id)

        self.assertGreater(len(timeline), 0)
        self.assertEqual(timeline[0]["event_type"], "reported")
        self.assertIn("Alice", timeline[0]["description"])

    def test_timeline_corrective_actions(self):
        """Test timeline includes corrective actions."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Install non-slip mat",
            assigned_to="Charlie",
            due_date=date.today() + timedelta(days=7),
        )

        timeline = get_incident_timeline(incident.incident_id)

        action_events = [e for e in timeline if e["event_type"] == "corrective_action_added"]
        self.assertEqual(len(action_events), 1)
        self.assertIn("non-slip mat", action_events[0]["description"])

    def test_timeline_chronological(self):
        """Test timeline is chronologically sorted."""
        incident = report_incident(
            venue_id="venue_003",
            reported_by="emp_003",
            reported_by_name="Carol",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.CUT_LACERATION,
            severity=IncidentSeverity.MINOR,
            description="Cut",
        )

        # Add actions with different dates
        for i in range(3):
            add_corrective_action(
                incident_id=incident.incident_id,
                description=f"Action {i}",
                assigned_to=f"Person{i}",
                due_date=date.today() + timedelta(days=i),
            )

        timeline = get_incident_timeline(incident.incident_id)

        # Check chronological order
        for i in range(len(timeline) - 1):
            ts1 = timeline[i]["timestamp"]
            ts2 = timeline[i + 1]["timestamp"]
            self.assertLessEqual(ts1, ts2)


class TestPersistence(unittest.TestCase):
    """Test persistence to SQLite and rehydration."""

    @classmethod
    def setUpClass(cls):
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        _p.reset_for_tests()

    def test_persistence_incident_survives_reset(self):
        """Test incident persists to SQLite and survives store reset."""
        incident = report_incident(
            venue_id="venue_001",
            reported_by="emp_001",
            reported_by_name="Alice",
            date_occurred=datetime.now(timezone.utc),
            location="kitchen",
            category=IncidentCategory.BURN,
            severity=IncidentSeverity.MINOR,
            description="Burn",
        )

        incident_id = incident.incident_id

        # Reset store (simulates server restart)
        _reset_for_tests()
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        # Retrieve after rehydration
        store = get_incident_store()
        retrieved = store.get_incident(incident_id)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.incident_id, incident_id)
        self.assertEqual(retrieved.venue_id, "venue_001")

    def test_persistence_corrective_action_survives_reset(self):
        """Test corrective action persists and rehydrates."""
        incident = report_incident(
            venue_id="venue_002",
            reported_by="emp_002",
            reported_by_name="Bob",
            date_occurred=datetime.now(timezone.utc),
            location="bar",
            category=IncidentCategory.SLIP_TRIP_FALL,
            severity=IncidentSeverity.MINOR,
            description="Slip",
        )

        action = add_corrective_action(
            incident_id=incident.incident_id,
            description="Install mat",
            assigned_to="Charlie",
            due_date=date.today() + timedelta(days=7),
        )

        action_id = action.action_id

        # Reset and rehydrate
        _reset_for_tests()
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store = get_incident_store()
        retrieved = store.get_corrective_action(action_id)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.action_id, action_id)
        self.assertEqual(retrieved.assigned_to, "Charlie")


if __name__ == "__main__":
    unittest.main()
