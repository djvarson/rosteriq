"""
Comprehensive test suite for RosterIQ Roster Generation Engine.

30+ test methods covering:
- Employee assignment and scoring
- Constraint validation
- Demand forecast conversion
- Fairness and coverage calculation
- Optimization algorithms
- Budget limits and cost tracking
- Shift gap enforcement
- Full roster generation workflows
"""

import unittest
from datetime import datetime, timedelta
from typing import List

from rosteriq.roster_engine import (
    Employee, Shift, DemandForecast, RosterConstraints, Roster, RosterScore,
    RosterEngine, Role, EmploymentType
)


class TestEmployeeDataModel(unittest.TestCase):
    """Test Employee dataclass creation and validation."""

    def test_employee_creation_basic(self):
        """Test basic employee creation with required fields."""
        emp = Employee(
            id="emp001",
            name="John Smith",
            role=Role.BAR,
            hourly_rate=25.0
        )
        self.assertEqual(emp.id, "emp001")
        self.assertEqual(emp.name, "John Smith")
        self.assertEqual(emp.role, Role.BAR)
        self.assertEqual(emp.hourly_rate, 25.0)
        self.assertIn(Role.BAR, emp.skills)

    def test_employee_creation_with_skills(self):
        """Test employee with multiple skills."""
        emp = Employee(
            id="emp002",
            name="Jane Doe",
            role=Role.MANAGER,
            skills=[Role.MANAGER, Role.BAR, Role.FLOOR],
            is_manager=True
        )
        self.assertEqual(len(emp.skills), 3)
        self.assertIn(Role.MANAGER, emp.skills)
        self.assertTrue(emp.is_manager)

    def test_employee_invalid_hourly_rate(self):
        """Test that invalid hourly rates are rejected."""
        with self.assertRaises(ValueError):
            Employee(id="emp003", name="Bad Pay", role=Role.FLOOR, hourly_rate=-10)

    def test_employee_availability_dict(self):
        """Test employee availability scheduling."""
        availability = {
            0: [(9, 17)],  # Monday 9am-5pm
            1: [(9, 17)],  # Tuesday 9am-5pm
            5: [(18, 23)], # Saturday 6pm-11pm
        }
        emp = Employee(
            id="emp004",
            name="Part Time",
            role=Role.KITCHEN,
            availability=availability
        )
        self.assertEqual(len(emp.availability), 3)
        self.assertIn(0, emp.availability)

    def test_employee_employment_types(self):
        """Test different employment type classifications."""
        full_time = Employee(
            id="ft001", name="Full Time", role=Role.MANAGER,
            employment_type=EmploymentType.FULL_TIME,
            max_hours_per_week=38
        )
        part_time = Employee(
            id="pt001", name="Part Time", role=Role.FLOOR,
            employment_type=EmploymentType.PART_TIME,
            max_hours_per_week=20
        )
        casual = Employee(
            id="cas001", name="Casual", role=Role.BAR,
            employment_type=EmploymentType.CASUAL,
            max_hours_per_week=0
        )
        self.assertEqual(full_time.employment_type, EmploymentType.FULL_TIME)
        self.assertEqual(part_time.employment_type, EmploymentType.PART_TIME)
        self.assertEqual(casual.employment_type, EmploymentType.CASUAL)


class TestShiftDataModel(unittest.TestCase):
    """Test Shift dataclass and properties."""

    def test_shift_creation(self):
        """Test basic shift creation."""
        shift = Shift(
            id="shift001",
            date="2026-04-06",
            start_hour=9,
            end_hour=17,
            role_required=Role.BAR
        )
        self.assertEqual(shift.id, "shift001")
        self.assertEqual(shift.start_hour, 9)
        self.assertEqual(shift.end_hour, 17)
        self.assertFalse(shift.is_filled)

    def test_shift_filled_property(self):
        """Test is_filled property."""
        shift = Shift(
            id="shift002",
            date="2026-04-06",
            start_hour=10,
            end_hour=18,
            role_required=Role.FLOOR
        )
        self.assertFalse(shift.is_filled)

        shift.employee_id = "emp001"
        self.assertTrue(shift.is_filled)

    def test_shift_duration_calculation(self):
        """Test shift duration including break deduction."""
        shift = Shift(
            id="shift003",
            date="2026-04-06",
            start_hour=9,
            end_hour=17,
            role_required=Role.KITCHEN,
            break_minutes=30
        )
        # 8 hours - 0.5 hour break = 7.5 hours
        self.assertEqual(shift.duration_hours, 7.5)

    def test_shift_minimum_duration(self):
        """Test minimum shift duration of 0.5 hours."""
        shift = Shift(
            id="shift004",
            date="2026-04-06",
            start_hour=9,
            end_hour=9,
            role_required=Role.BAR
        )
        self.assertEqual(shift.duration_hours, 0.5)

    def test_shift_hash(self):
        """Test that shifts are hashable."""
        shift1 = Shift(id="s1", date="2026-04-06", start_hour=9, end_hour=17, role_required=Role.BAR)
        shift2 = Shift(id="s1", date="2026-04-06", start_hour=9, end_hour=17, role_required=Role.BAR)
        shift_set = {shift1}
        self.assertTrue(shift1 in shift_set)


class TestDemandForecast(unittest.TestCase):
    """Test DemandForecast dataclass."""

    def test_demand_forecast_creation(self):
        """Test basic demand forecast."""
        forecast = DemandForecast(
            date="2026-04-06",
            hourly_demand={
                10: {Role.BAR: 2, Role.FLOOR: 3},
                11: {Role.BAR: 3, Role.FLOOR: 4},
            },
            total_covers_expected=45,
            confidence=0.85
        )
        self.assertEqual(forecast.date, "2026-04-06")
        self.assertEqual(forecast.confidence, 0.85)
        self.assertEqual(forecast.hourly_demand[10][Role.BAR], 2)

    def test_demand_forecast_with_signals(self):
        """Test demand forecast with event signals."""
        forecast = DemandForecast(
            date="2026-04-06",
            hourly_demand={12: {Role.BAR: 5}},
            total_covers_expected=100,
            signals=["promotion", "special_event"],
            confidence=0.95
        )
        self.assertIn("promotion", forecast.signals)
        self.assertEqual(len(forecast.signals), 2)


class TestRosterConstraints(unittest.TestCase):
    """Test RosterConstraints dataclass."""

    def test_constraints_defaults(self):
        """Test default constraints."""
        constraints = RosterConstraints()
        self.assertEqual(constraints.min_staff_per_hour, 2)
        self.assertEqual(constraints.max_staff_per_hour, 10)
        self.assertEqual(constraints.max_consecutive_days, 5)
        self.assertEqual(constraints.min_hours_between_shifts, 11.0)
        self.assertEqual(constraints.max_shift_length_hours, 12.0)

    def test_constraints_custom(self):
        """Test custom constraints."""
        constraints = RosterConstraints(
            min_staff_per_hour=3,
            max_staff_per_hour=8,
            budget_limit_weekly=2500.0,
            max_consecutive_days=4
        )
        self.assertEqual(constraints.min_staff_per_hour, 3)
        self.assertEqual(constraints.budget_limit_weekly, 2500.0)

    def test_constraints_required_roles(self):
        """Test required roles per shift."""
        constraints = RosterConstraints(
            required_roles={Role.MANAGER: 1, Role.BAR: 2}
        )
        self.assertEqual(constraints.required_roles[Role.MANAGER], 1)
        self.assertEqual(constraints.required_roles[Role.BAR], 2)


class TestRosterEngine(unittest.TestCase):
    """Core roster engine functionality tests."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints(
            min_staff_per_hour=2,
            max_staff_per_hour=8,
            budget_limit_weekly=3000.0
        )
        self.engine = RosterEngine(self.constraints)

    def test_engine_initialization(self):
        """Test engine creation with constraints."""
        self.assertEqual(self.engine.constraints.min_staff_per_hour, 2)
        self.assertEqual(self.engine.constraints.budget_limit_weekly, 3000.0)

    def test_engine_with_award_rules(self):
        """Test engine with award-specific rules."""
        award_rules = {
            'penalty_rate_after_hour': 20,
            'weekend_multiplier': 1.5
        }
        engine = RosterEngine(self.constraints, award_rules)
        self.assertIn('penalty_rate_after_hour', engine.award_rules)


class TestDemandConversion(unittest.TestCase):
    """Test demand forecast to shift slot conversion."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)

    def test_calculate_demand_slots_basic(self):
        """Test conversion of hourly demand to shift slots."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={
                    10: {Role.BAR: 2},
                    11: {Role.BAR: 3},
                    12: {Role.BAR: 2},
                },
                total_covers_expected=50,
                confidence=0.8
            )
        ]
        slots = self.engine._calculate_demand_slots(forecasts, "2026-04-06")
        self.assertEqual(len(slots), 3)
        self.assertTrue(all(s['role'] == Role.BAR for s in slots))

    def test_calculate_demand_slots_multiple_roles(self):
        """Test demand with multiple roles per hour."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={
                    18: {Role.BAR: 2, Role.FLOOR: 3, Role.KITCHEN: 2},
                },
                total_covers_expected=80,
                confidence=0.9
            )
        ]
        slots = self.engine._calculate_demand_slots(forecasts, "2026-04-06")
        self.assertEqual(len(slots), 3)
        roles = {s['role'] for s in slots}
        self.assertEqual(roles, {Role.BAR, Role.FLOOR, Role.KITCHEN})

    def test_demand_slots_prioritization(self):
        """Test that peak hours are prioritized."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={
                    8: {Role.BAR: 1},   # Early, non-peak
                    15: {Role.BAR: 2},  # Peak hour
                    22: {Role.BAR: 1},  # Late, non-peak
                },
                total_covers_expected=50,
                confidence=0.8
            )
        ]
        slots = self.engine._calculate_demand_slots(forecasts, "2026-04-06")
        # Peak hour (15) should have higher priority
        self.assertEqual(slots[0]['start_hour'], 15)


class TestEmployeeAssignment(unittest.TestCase):
    """Test employee assignment logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)

        self.employees = [
            Employee(
                id="emp001", name="Alice", role=Role.BAR,
                hourly_rate=25.0, max_hours_per_week=38
            ),
            Employee(
                id="emp002", name="Bob", role=Role.KITCHEN,
                hourly_rate=26.0, max_hours_per_week=38
            ),
            Employee(
                id="emp003", name="Charlie", role=Role.FLOOR,
                hourly_rate=24.0, max_hours_per_week=20,
                availability={0: [(9, 17)]}  # Monday 9am-5pm only
            ),
        ]

    def test_score_assignment_skill_match(self):
        """Test scoring with skill match as primary factor."""
        slot = {
            'date': '2026-04-06',
            'role': Role.BAR,
            'start_hour': 10,
            'end_hour': 18,
            'demand_units': 1
        }
        score_match = self.engine._score_assignment(self.employees[0], slot, {})
        score_no_match = self.engine._score_assignment(self.employees[1], slot, {})

        self.assertGreater(score_match, score_no_match)

    def test_score_assignment_fairness(self):
        """Test that fairness prefers underloaded employees."""
        slot = {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 10, 'end_hour': 18}

        # Empty hours dict - fresh employee
        score_fresh = self.engine._score_assignment(self.employees[0], slot, {'emp001': 0})

        # Already has 30 hours
        score_loaded = self.engine._score_assignment(self.employees[0], slot, {'emp001': 30})

        self.assertGreater(score_fresh, score_loaded)

    def test_is_candidate_available_role_check(self):
        """Test that role matching is enforced."""
        slot = {'date': '2026-04-06', 'role': Role.MANAGER, 'start_hour': 10, 'end_hour': 18}

        # None of our test employees are managers
        for emp in self.employees:
            is_avail = self.engine._is_candidate_available(emp, slot, {})
            self.assertFalse(is_avail)

    def test_is_candidate_available_availability_window(self):
        """Test that availability windows are checked."""
        # Tuesday (weekday 1) - Charlie only available Monday
        slot = {'date': '2026-04-07', 'role': Role.FLOOR, 'start_hour': 10, 'end_hour': 18}

        # Charlie has limited availability
        is_avail = self.engine._is_candidate_available(self.employees[2], slot, {})
        self.assertFalse(is_avail)

    def test_is_candidate_available_max_shift_length(self):
        """Test that max shift length is enforced."""
        slot = {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 8, 'end_hour': 23}  # 15 hours

        # Default max is 12 hours
        is_avail = self.engine._is_candidate_available(self.employees[0], slot, {})
        self.assertFalse(is_avail)

    def test_assign_employees_basic(self):
        """Test basic assignment of employees to slots."""
        slots = [
            {
                'date': '2026-04-06',
                'role': Role.BAR,
                'start_hour': 10,
                'end_hour': 18,
                'demand_units': 1,
                'priority': (True, 0.8),
                'confidence': 0.8
            }
        ]
        shifts = self.engine._assign_employees(slots, self.employees)

        # Should have assigned the BAR employee
        filled = [s for s in shifts if s.is_filled]
        self.assertEqual(len(filled), 1)
        self.assertEqual(filled[0].role_required, Role.BAR)

    def test_assign_employees_unfilled_slot(self):
        """Test that unavailable slots are marked unfilled."""
        slots = [
            {
                'date': '2026-04-06',
                'role': Role.MANAGER,  # No managers in our list
                'start_hour': 10,
                'end_hour': 18,
                'demand_units': 1,
                'priority': (True, 0.8),
                'confidence': 0.8
            }
        ]
        shifts = self.engine._assign_employees(slots, self.employees)

        unfilled = [s for s in shifts if not s.is_filled]
        self.assertEqual(len(unfilled), 1)

    def test_merge_consecutive_slots(self):
        """Test merging of consecutive hourly slots."""
        slots = [
            {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 10, 'end_hour': 11, 'demand_units': 1},
            {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 11, 'end_hour': 12, 'demand_units': 1},
            {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 12, 'end_hour': 13, 'demand_units': 1},
        ]
        merged = self.engine._merge_consecutive_slots(slots)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['start_hour'], 10)
        self.assertEqual(merged[0]['end_hour'], 13)

    def test_merge_slots_different_roles(self):
        """Test that different roles don't merge."""
        slots = [
            {'date': '2026-04-06', 'role': Role.BAR, 'start_hour': 10, 'end_hour': 11, 'demand_units': 1},
            {'date': '2026-04-06', 'role': Role.FLOOR, 'start_hour': 11, 'end_hour': 12, 'demand_units': 1},
        ]
        merged = self.engine._merge_consecutive_slots(slots)

        self.assertEqual(len(merged), 2)


class TestConstraintChecking(unittest.TestCase):
    """Test constraint validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)
        self.employee = Employee(
            id="emp001", name="Test", role=Role.BAR,
            hourly_rate=25.0, max_hours_per_week=38
        )

    def test_check_constraints_valid_assignment(self):
        """Test that valid assignments pass constraints."""
        shift = Shift(
            id="shift001",
            date="2026-04-06",
            start_hour=10,
            end_hour=17,
            role_required=Role.BAR,
            employee_id="emp001"
        )
        violations = self.engine._check_constraints(shift, self.employee, [shift])

        self.assertEqual(len(violations), 0)

    def test_check_constraints_role_mismatch(self):
        """Test that role mismatch is caught."""
        shift = Shift(
            id="shift001",
            date="2026-04-06",
            start_hour=10,
            end_hour=17,
            role_required=Role.MANAGER,  # Employee is BAR, not MANAGER
            employee_id="emp001"
        )
        violations = self.engine._check_constraints(shift, self.employee, [shift])

        self.assertTrue(any("Role" in v for v in violations))

    def test_check_constraints_max_shift_length(self):
        """Test that overly long shifts are caught."""
        shift = Shift(
            id="shift001",
            date="2026-04-06",
            start_hour=8,
            end_hour=23,  # 15 hours, exceeds 12-hour max
            role_required=Role.BAR,
            employee_id="emp001"
        )
        violations = self.engine._check_constraints(shift, self.employee, [shift])

        self.assertTrue(any("Shift" in v for v in violations))

    def test_check_constraints_availability_violation(self):
        """Test that availability windows are enforced."""
        # Create employee with strict availability window
        emp_limited = Employee(
            id="emp002", name="Limited", role=Role.FLOOR,
            availability={3: [(9, 17)]}  # Thursday 9am-5pm only (day 3)
        )

        # Try to assign shift outside availability window (6pm-11pm on Thursday)
        # 2026-04-09 is Thursday
        shift = Shift(
            id="shift001",
            date="2026-04-09",  # Thursday (weekday 3)
            start_hour=18,      # 6pm, outside 9-5 window
            end_hour=23,        # 11pm
            role_required=Role.FLOOR,
            employee_id="emp002"
        )
        violations = self.engine._check_constraints(shift, emp_limited, [shift])

        # Should have availability violation
        self.assertTrue(any("available" in v.lower() for v in violations))


class TestFairnessCalculation(unittest.TestCase):
    """Test fairness scoring algorithm."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)
        self.employees = [
            Employee(id="emp001", name="Alice", role=Role.BAR, max_hours_per_week=38),
            Employee(id="emp002", name="Bob", role=Role.BAR, max_hours_per_week=38),
        ]

    def test_fairness_perfect_equality(self):
        """Test fairness score is 1.0 for equal distribution."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 15, Role.BAR, "emp001"),  # 5 hours
            Shift("s2", "2026-04-07", 10, 15, Role.BAR, "emp002"),  # 5 hours
        ]
        fairness = self.engine._calculate_fairness(shifts, self.employees)

        # Perfect equality should score high
        self.assertGreater(fairness, 0.9)

    def test_fairness_unequal_distribution(self):
        """Test fairness score is lower for unequal hours."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),  # 8 hours
            Shift("s2", "2026-04-06", 10, 12, Role.BAR, "emp002"),  # 2 hours
            Shift("s3", "2026-04-07", 10, 18, Role.BAR, "emp001"),  # 8 hours
        ]
        fairness = self.engine._calculate_fairness(shifts, self.employees)

        # Unequal distribution should score lower
        self.assertLess(fairness, 0.9)

    def test_fairness_no_shifts(self):
        """Test fairness score when no shifts are filled."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 18, Role.BAR),  # Unfilled
        ]
        fairness = self.engine._calculate_fairness(shifts, self.employees)

        # No filled shifts = perfect fairness
        self.assertEqual(fairness, 1.0)


class TestCoverageCalculation(unittest.TestCase):
    """Test coverage scoring algorithm."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)
        self.employees = [
            Employee(id="emp001", name="Alice", role=Role.BAR, max_hours_per_week=38),
        ]

    def test_coverage_perfect_match(self):
        """Test coverage score when demand is perfectly met."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 1}, 11: {Role.BAR: 1}},
                total_covers_expected=50
            )
        ]
        shifts = [
            Shift("s1", "2026-04-06", 10, 12, Role.BAR, "emp001"),
        ]
        coverage = self.engine._calculate_coverage(shifts, forecasts)

        # Perfect coverage should be 1.0
        self.assertEqual(coverage, 1.0)

    def test_coverage_partial_fulfillment(self):
        """Test coverage score when demand is partially met."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 2}, 11: {Role.BAR: 2}},
                total_covers_expected=50
            )
        ]
        shifts = [
            Shift("s1", "2026-04-06", 10, 12, Role.BAR, "emp001"),  # Only 1 person
        ]
        coverage = self.engine._calculate_coverage(shifts, forecasts)

        # Only meeting half the demand
        self.assertEqual(coverage, 0.5)

    def test_coverage_no_demand(self):
        """Test coverage when there's no demand."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={},
                total_covers_expected=0
            )
        ]
        shifts = []
        coverage = self.engine._calculate_coverage(shifts, forecasts)

        # No demand = perfect coverage
        self.assertEqual(coverage, 1.0)


class TestCostEfficiency(unittest.TestCase):
    """Test cost efficiency scoring."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints(budget_limit_weekly=1000.0)
        self.engine = RosterEngine(self.constraints)

    def test_cost_efficiency_perfect_budget(self):
        """Test score at 100% budget utilization."""
        score = self.engine._calculate_cost_efficiency(1000.0, 1000.0)
        self.assertEqual(score, 1.0)

    def test_cost_efficiency_under_budget(self):
        """Test score when under budget."""
        score = self.engine._calculate_cost_efficiency(800.0, 1000.0)
        self.assertEqual(score, 0.8)

    def test_cost_efficiency_over_budget(self):
        """Test score when over budget."""
        score = self.engine._calculate_cost_efficiency(1200.0, 1000.0)
        self.assertLess(score, 1.0)

    def test_cost_efficiency_zero_budget(self):
        """Test score with zero budget."""
        score = self.engine._calculate_cost_efficiency(100.0, 0)
        self.assertEqual(score, 0.5)


class TestOptimization(unittest.TestCase):
    """Test roster optimization algorithms."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)
        self.employees = [
            Employee(id="emp001", name="Alice", role=Role.BAR, max_hours_per_week=38),
            Employee(id="emp002", name="Bob", role=Role.BAR, max_hours_per_week=38),
        ]

    def test_optimise_roster_returns_shifts(self):
        """Test that optimization returns a roster."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),
        ]
        optimized = self.engine._optimise_roster(shifts, self.employees)

        self.assertIsInstance(optimized, list)
        self.assertEqual(len(optimized), 1)

    def test_is_valid_swap_respects_constraints(self):
        """Test that swaps respect constraints."""
        emp_a = Employee(id="emp001", name="Alice", role=Role.BAR, availability={})
        emp_b = Employee(id="emp002", name="Bob", role=Role.KITCHEN)  # Different role

        shift_a = Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001")
        shift_b = Shift("s2", "2026-04-06", 10, 18, Role.KITCHEN, "emp002")

        is_valid = self.engine._is_valid_swap(shift_a, shift_b, emp_a, emp_b, [shift_a, shift_b], [emp_a, emp_b])

        # Should be invalid due to role mismatch
        self.assertFalse(is_valid)


class TestFullRosterGeneration(unittest.TestCase):
    """Test end-to-end roster generation."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints(
            min_staff_per_hour=2,
            max_staff_per_hour=8,
            budget_limit_weekly=3000.0
        )
        self.engine = RosterEngine(self.constraints)

        self.employees = [
            Employee(
                id="bar001", name="Alice Bar", role=Role.BAR,
                hourly_rate=26.0, max_hours_per_week=38,
                employment_type=EmploymentType.FULL_TIME
            ),
            Employee(
                id="bar002", name="Bob Bar", role=Role.BAR,
                hourly_rate=24.0, max_hours_per_week=20,
                employment_type=EmploymentType.PART_TIME
            ),
            Employee(
                id="kitchen001", name="Charlie Kitchen", role=Role.KITCHEN,
                hourly_rate=27.0, max_hours_per_week=38,
                employment_type=EmploymentType.FULL_TIME
            ),
            Employee(
                id="mgr001", name="Diana Manager", role=Role.MANAGER,
                hourly_rate=35.0, max_hours_per_week=38,
                is_manager=True,
                employment_type=EmploymentType.FULL_TIME
            ),
        ]

    def test_generate_roster_basic(self):
        """Test basic roster generation for a week."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={
                    10: {Role.BAR: 2, Role.KITCHEN: 1, Role.MANAGER: 1},
                    18: {Role.BAR: 3, Role.KITCHEN: 2, Role.MANAGER: 1},
                },
                total_covers_expected=80,
                confidence=0.85
            ),
            DemandForecast(
                date="2026-04-07",
                hourly_demand={
                    10: {Role.BAR: 2, Role.KITCHEN: 1, Role.MANAGER: 1},
                    18: {Role.BAR: 2, Role.KITCHEN: 1, Role.MANAGER: 1},
                },
                total_covers_expected=60,
                confidence=0.80
            ),
        ]

        roster = self.engine.generate_roster(self.employees, forecasts, "2026-04-06")

        self.assertIsInstance(roster, Roster)
        self.assertEqual(roster.week_start_date, "2026-04-06")
        self.assertGreater(len(roster.shifts), 0)

    def test_generate_roster_has_metrics(self):
        """Test that generated roster has quality metrics."""
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 1, Role.MANAGER: 1}},
                total_covers_expected=40,
                confidence=0.8
            )
        ]

        roster = self.engine.generate_roster(self.employees, forecasts, "2026-04-06")

        self.assertGreaterEqual(roster.coverage_score, 0)
        self.assertLessEqual(roster.coverage_score, 1.0)
        self.assertGreaterEqual(roster.fairness_score, 0)
        self.assertLessEqual(roster.fairness_score, 1.0)
        self.assertGreater(roster.total_labour_cost, 0)

    def test_generate_roster_respects_budget(self):
        """Test that roster respects budget limits."""
        # Normal budget should generate costs
        normal_budget = RosterConstraints(budget_limit_weekly=3000.0)
        normal_engine = RosterEngine(normal_budget)

        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 2, Role.MANAGER: 1}},
                total_covers_expected=50,
                confidence=0.9
            )
        ]

        roster = normal_engine.generate_roster(self.employees, forecasts, "2026-04-06")

        # Roster should have reasonable cost and not exceed budget
        self.assertGreater(roster.total_labour_cost, 0)
        self.assertLessEqual(roster.total_labour_cost, normal_budget.budget_limit_weekly)

    def test_generate_roster_full_week(self):
        """Test roster generation for full week."""
        forecasts = []
        for day_offset in range(7):
            date = (datetime(2026, 4, 6) + timedelta(days=day_offset)).strftime('%Y-%m-%d')
            forecasts.append(
                DemandForecast(
                    date=date,
                    hourly_demand={
                        10: {Role.BAR: 2, Role.KITCHEN: 1, Role.MANAGER: 1},
                        18: {Role.BAR: 3, Role.KITCHEN: 2, Role.MANAGER: 1},
                    },
                    total_covers_expected=70,
                    confidence=0.85
                )
            )

        roster = self.engine.generate_roster(self.employees, forecasts, "2026-04-06")

        # Should have shifts for multiple days
        dates = {s.date for s in roster.shifts}
        self.assertGreaterEqual(len(dates), 2)


class TestRosterScoring(unittest.TestCase):
    """Test comprehensive roster scoring."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)
        self.employees = [
            Employee(id="emp001", name="Alice", role=Role.BAR, hourly_rate=25.0),
            Employee(id="emp002", name="Bob", role=Role.BAR, hourly_rate=26.0),
        ]

    def test_score_roster_returns_rosterscore(self):
        """Test that scoring returns RosterScore object."""
        roster = Roster(
            venue_id="venue001",
            week_start_date="2026-04-06",
            shifts=[
                Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),
            ],
            total_labour_cost=200.0,
            coverage_score=0.85,
            fairness_score=0.80,
            cost_efficiency_score=0.90
        )

        score = self.engine.score_roster(roster, self.employees)

        self.assertIsInstance(score, RosterScore)
        self.assertGreaterEqual(score.overall, 0)
        self.assertLessEqual(score.overall, 100)

    def test_score_roster_overall_aggregate(self):
        """Test that overall score is weighted average of components."""
        roster = Roster(
            venue_id="venue001",
            week_start_date="2026-04-06",
            shifts=[
                Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),
            ],
            total_labour_cost=200.0,
            coverage_score=0.80,
            fairness_score=0.60,
            cost_efficiency_score=1.0
        )

        score = self.engine.score_roster(roster, self.employees)

        # Overall should be between min and max components
        min_score = min(score.coverage, score.fairness, score.cost_efficiency)
        max_score = max(score.coverage, score.fairness, score.cost_efficiency)

        self.assertGreaterEqual(score.overall, min_score * 0.8)
        self.assertLessEqual(score.overall, max_score)

    def test_score_roster_breakdown(self):
        """Test that scoring includes detailed breakdown."""
        roster = Roster(
            venue_id="venue001",
            week_start_date="2026-04-06",
            shifts=[
                Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),
                Shift("s2", "2026-04-07", 10, 18, Role.BAR),  # Unfilled
            ],
            total_labour_cost=200.0,
            coverage_score=0.8,
            fairness_score=0.8,
            cost_efficiency_score=0.9
        )

        score = self.engine.score_roster(roster, self.employees)

        self.assertIn('unfilled_shifts', score.breakdown)
        self.assertEqual(score.breakdown['unfilled_shifts'], 1)
        self.assertIn('total_shifts', score.breakdown)


class TestWarningGeneration(unittest.TestCase):
    """Test warning and alert generation."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints(budget_limit_weekly=500.0)
        self.engine = RosterEngine(self.constraints)
        self.employees = [
            Employee(id="emp001", name="Alice", role=Role.BAR, hourly_rate=30.0),
        ]

    def test_warnings_unfilled_shifts(self):
        """Test that unfilled shifts generate warnings."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),
            Shift("s2", "2026-04-07", 10, 18, Role.BAR),  # Unfilled
        ]
        warnings = self.engine._generate_warnings(shifts, [], self.employees)

        self.assertTrue(any("unfilled" in w.lower() for w in warnings))

    def test_warnings_budget_exceeded(self):
        """Test that budget overages generate warnings."""
        shifts = [
            Shift("s1", "2026-04-06", 10, 18, Role.BAR, "emp001"),  # 8 hours * $30 = $240
            Shift("s2", "2026-04-07", 10, 18, Role.BAR, "emp001"),  # 8 hours * $30 = $240
            Shift("s3", "2026-04-08", 10, 18, Role.BAR, "emp001"),  # 8 hours * $30 = $240 (total $720)
        ]
        warnings = self.engine._generate_warnings(shifts, [], self.employees)

        self.assertTrue(any("Budget" in w for w in warnings))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        """Set up test fixtures."""
        self.constraints = RosterConstraints()
        self.engine = RosterEngine(self.constraints)

    def test_empty_demand_list(self):
        """Test handling of empty demand."""
        employees = [Employee(id="e1", name="Test", role=Role.BAR)]
        forecasts = []

        roster = self.engine.generate_roster(employees, forecasts, "2026-04-06")

        self.assertEqual(len(roster.shifts), 0)

    def test_no_suitable_employees(self):
        """Test when no employees match required roles."""
        employees = [Employee(id="e1", name="Test", role=Role.FLOOR)]
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.MANAGER: 1}},  # Need manager, have floor staff
                total_covers_expected=50
            )
        ]

        roster = self.engine.generate_roster(employees, forecasts, "2026-04-06")

        # Should create unfilled shifts
        unfilled = [s for s in roster.shifts if not s.is_filled]
        self.assertGreater(len(unfilled), 0)

    def test_single_employee_scenario(self):
        """Test roster with single employee."""
        employees = [
            Employee(id="e1", name="Solo", role=Role.BAR, max_hours_per_week=38)
        ]
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 1}, 18: {Role.BAR: 1}},
                total_covers_expected=50
            )
        ]

        roster = self.engine.generate_roster(employees, forecasts, "2026-04-06")

        self.assertGreater(len(roster.shifts), 0)

    def test_zero_demand_units(self):
        """Test handling of zero demand."""
        employees = [Employee(id="e1", name="Test", role=Role.BAR)]
        forecasts = [
            DemandForecast(
                date="2026-04-06",
                hourly_demand={10: {Role.BAR: 0}},  # Zero demand
                total_covers_expected=0
            )
        ]

        roster = self.engine.generate_roster(employees, forecasts, "2026-04-06")

        # No shifts should be created for zero demand
        shifts = [s for s in roster.shifts if s.is_filled]
        self.assertEqual(len(shifts), 0)


if __name__ == '__main__':
    unittest.main()
