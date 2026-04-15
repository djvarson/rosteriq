"""
Test suite for award_router.py endpoints.

Tests the /api/v1/award/evaluate and /api/v1/award/rules endpoints
with realistic roster scenarios including Saturday evening shifts and
overtime conditions.
"""

import unittest
from datetime import datetime, date, time, timedelta
from decimal import Decimal
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Try to use FastAPI TestClient if available, else fall back
try:
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# Always try to import the router (it will work if we skip FastAPI tests)
if FASTAPI_AVAILABLE:
    from rosteriq.award_router import router as award_router
else:
    # If FastAPI is not available, create a mock router
    award_router = None


class TestAwardRouterEvaluateEndpoint(unittest.TestCase):
    """Test POST /api/v1/award/evaluate"""

    @classmethod
    def setUpClass(cls):
        """Set up FastAPI test client if available."""
        if FASTAPI_AVAILABLE:
            app = FastAPI()
            app.include_router(award_router)
            cls.client = TestClient(app)
        else:
            cls.client = None

    def test_evaluate_simple_monday_shift(self):
        """Test evaluation of a simple Monday 8-hour shift."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        # Monday 8am-4pm shift
        monday = date(2026, 4, 13)  # A Monday
        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_001",
                    "role": 1,  # Level 1: Food & Beverage Attendant
                    "shift_start": datetime.combine(monday, time(8, 0)).isoformat(),
                    "shift_end": datetime.combine(monday, time(16, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("total_base_cost", data)
        self.assertIn("total_loading_cost", data)
        self.assertIn("total_cost", data)
        self.assertIn("penalty_breakdown", data)
        self.assertIn("compliance_issues", data)

        # Monday is ordinary time, so base cost should equal total cost
        # (no loading)
        self.assertEqual(data["total_loading_cost"], 0.0)
        self.assertGreater(data["total_base_cost"], 0.0)

    def test_evaluate_saturday_evening_shift(self):
        """Test Saturday evening shift produces Saturday penalty."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        # Saturday 6pm-10pm (evening + Saturday)
        saturday = date(2026, 4, 18)  # A Saturday
        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_001",
                    "role": 1,
                    "shift_start": datetime.combine(saturday, time(18, 0)).isoformat(),
                    "shift_end": datetime.combine(saturday, time(22, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Saturday should have penalty, so loading cost should be > 0
        self.assertGreater(data["total_loading_cost"], 0.0)
        self.assertGreater(data["total_cost"], data["total_base_cost"])

        # Penalty breakdown should show Saturday loading
        self.assertEqual(len(data["penalty_breakdown"]), 1)
        self.assertEqual(data["penalty_breakdown"][0]["loading_type"], "saturday")
        self.assertGreater(data["penalty_breakdown"][0]["loading_percent"], 0.0)

    def test_evaluate_sunday_shift(self):
        """Test Sunday shift produces Sunday penalty."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        # Sunday 10am-6pm
        sunday = date(2026, 4, 19)  # A Sunday
        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_002",
                    "role": 2,
                    "shift_start": datetime.combine(sunday, time(10, 0)).isoformat(),
                    "shift_end": datetime.combine(sunday, time(18, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Sunday should have higher penalty than Saturday
        self.assertGreater(data["total_loading_cost"], 0.0)
        self.assertEqual(data["penalty_breakdown"][0]["loading_type"], "sunday")

    def test_evaluate_long_shift_warning(self):
        """Test that shifts over 12 hours produce a warning."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        monday = date(2026, 4, 13)
        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_003",
                    "role": 1,
                    "shift_start": datetime.combine(monday, time(8, 0)).isoformat(),
                    "shift_end": datetime.combine(monday, time(21, 0)).isoformat(),  # 13 hours
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Should have a warning about shift exceeding 12 hours
        warning_found = any("exceeds 12 hours" in w.lower() for w in data["warnings"])
        self.assertTrue(warning_found, "Expected warning about shift length")

    def test_evaluate_casual_with_loading(self):
        """Test casual worker produces higher loading than full-time."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        saturday = date(2026, 4, 18)

        # Full-time Saturday
        request_ft = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_ft",
                    "role": 1,
                    "shift_start": datetime.combine(saturday, time(10, 0)).isoformat(),
                    "shift_end": datetime.combine(saturday, time(18, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response_ft = self.client.post("/evaluate", json=request_ft)
        data_ft = response_ft.json()

        # Casual Saturday
        request_casual = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_casual",
                    "role": 1,
                    "shift_start": datetime.combine(saturday, time(10, 0)).isoformat(),
                    "shift_end": datetime.combine(saturday, time(18, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "casual",
                    "age": None
                }
            ]
        }

        response_casual = self.client.post("/evaluate", json=request_casual)
        data_casual = response_casual.json()

        # Casual should have higher total cost due to casual loading + penalty
        self.assertGreater(data_casual["total_cost"], data_ft["total_cost"])

    def test_evaluate_public_holiday_shift(self):
        """Test public holiday shift produces public holiday penalty."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        # Christmas Day 2026
        christmas = date(2026, 12, 25)
        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_004",
                    "role": 1,
                    "shift_start": datetime.combine(christmas, time(12, 0)).isoformat(),
                    "shift_end": datetime.combine(christmas, time(20, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                }
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Public holiday should have significant loading
        self.assertGreater(data["total_loading_cost"], 0.0)
        self.assertEqual(data["penalty_breakdown"][0]["loading_type"], "public_holiday")

    def test_evaluate_multiple_shifts(self):
        """Test evaluation with multiple employees and shifts."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        monday = date(2026, 4, 13)
        tuesday = date(2026, 4, 14)

        request_body = {
            "venue_id": "venue_demo_001",
            "roster": [
                {
                    "employee_id": "emp_001",
                    "role": 1,
                    "shift_start": datetime.combine(monday, time(8, 0)).isoformat(),
                    "shift_end": datetime.combine(monday, time(16, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                },
                {
                    "employee_id": "emp_001",
                    "role": 1,
                    "shift_start": datetime.combine(tuesday, time(16, 0)).isoformat(),
                    "shift_end": datetime.combine(tuesday, time(22, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                },
                {
                    "employee_id": "emp_002",
                    "role": 2,
                    "shift_start": datetime.combine(monday, time(16, 0)).isoformat(),
                    "shift_end": datetime.combine(monday, time(22, 0)).isoformat(),
                    "hourly_rate": None,
                    "employment_type": "full_time",
                    "age": None
                },
            ]
        }

        response = self.client.post("/evaluate", json=request_body)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Should have 3 penalty breakdown entries
        self.assertEqual(len(data["penalty_breakdown"]), 3)

        # Total cost should be sum of all shifts
        self.assertGreater(data["total_cost"], 0.0)
        self.assertEqual(
            data["total_cost"],
            data["total_base_cost"] + data["total_loading_cost"]
        )


class TestAwardRouterRulesEndpoint(unittest.TestCase):
    """Test GET /api/v1/award/rules"""

    @classmethod
    def setUpClass(cls):
        """Set up FastAPI test client if available."""
        if FASTAPI_AVAILABLE:
            app = FastAPI()
            app.include_router(award_router)
            cls.client = TestClient(app)
        else:
            cls.client = None

    def test_get_rules(self):
        """Test that rules endpoint returns rule list."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        response = self.client.get("/rules")
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("rules", data)
        self.assertIn("description", data)
        self.assertGreater(len(data["rules"]), 0)

    def test_rules_contain_expected_fields(self):
        """Test that each rule has required fields."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        response = self.client.get("/rules")
        data = response.json()

        for rule in data["rules"]:
            self.assertIn("name", rule)
            self.assertIn("description", rule)
            self.assertIn("applies_to", rule)

    def test_rules_include_award_2020(self):
        """Test that description mentions correct award."""
        if not self.client:
            self.skipTest("FastAPI TestClient not available")

        response = self.client.get("/rules")
        data = response.json()

        self.assertIn("2020", data["description"])
        self.assertIn("MA000009", data["description"])


class TestAwardEngineIntegration(unittest.TestCase):
    """Direct integration tests with AwardEngine (no FastAPI required)."""

    def setUp(self):
        """Import award engine for direct testing."""
        from rosteriq.award_engine import AwardEngine, EmploymentType
        self.engine = AwardEngine(award_year=2025)
        self.EmploymentType = EmploymentType

    def test_saturday_penalty_produces_extra_cost(self):
        """Test that Saturday shift costs more than Monday shift."""
        # Monday shift (April 20 is a regular Monday)
        monday = date(2026, 4, 20)
        calc_monday = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=monday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        # Saturday shift (same duration) - April 25 is a Saturday
        saturday = date(2026, 4, 25)
        calc_saturday = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=saturday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        # Saturday should cost more
        self.assertGreater(calc_saturday.gross_pay, calc_monday.gross_pay)
        self.assertGreater(calc_saturday.penalty_multiplier, Decimal("1.0"))

    def test_sunday_penalty_exceeds_saturday_penalty(self):
        """Test that Sunday penalty is higher than Saturday."""
        sunday = date(2026, 4, 26)  # April 26 is a Sunday
        calc_sunday = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=sunday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        saturday = date(2026, 4, 25)  # April 25 is a Saturday
        calc_saturday = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=saturday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        # Sunday penalty should be > Saturday penalty
        self.assertGreater(calc_sunday.penalty_multiplier, calc_saturday.penalty_multiplier)

    def test_public_holiday_produces_highest_penalty(self):
        """Test that public holidays produce the highest penalty."""
        christmas = date(2026, 12, 25)
        calc_ph = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=christmas,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=True
        )

        # Should have 225% multiplier
        self.assertEqual(calc_ph.penalty_multiplier, Decimal("2.25"))

    def test_casual_loading_increases_cost(self):
        """Test that casual workers cost more due to 25% loading."""
        monday = date(2026, 4, 20)  # Regular Monday

        calc_ft = self.engine.calculate_shift_cost(
            employee_id="emp_ft",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=monday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        calc_casual = self.engine.calculate_shift_cost(
            employee_id="emp_casual",
            award_level=1,
            employment_type=self.EmploymentType.CASUAL,
            shift_date=monday,
            start_time=time(10, 0),
            end_time=time(18, 0),
            is_public_holiday=False
        )

        # Casual should cost 25% more
        self.assertGreater(calc_casual.gross_pay, calc_ft.gross_pay)

    def test_long_shift_produces_warning(self):
        """Test that shifts over 12 hours produce a warning."""
        monday = date(2026, 4, 20)  # Regular Monday (not a holiday)
        calc = self.engine.calculate_shift_cost(
            employee_id="emp_001",
            award_level=1,
            employment_type=self.EmploymentType.FULL_TIME,
            shift_date=monday,
            start_time=time(8, 0),
            end_time=time(21, 0),  # 13 hours
            is_public_holiday=False
        )

        # Should have warning
        self.assertTrue(any("exceeds 12 hours" in w for w in calc.warnings))

    def test_compliance_check_11_hour_break(self):
        """Test compliance check for 11-hour break between shifts."""
        monday = date(2026, 4, 20)  # Regular Monday
        tuesday = date(2026, 4, 21)  # Regular Tuesday

        shifts = [
            (monday, time(8, 0), time(16, 0)),
            (tuesday, time(1, 0), time(9, 0))  # Only 9 hours after previous shift
        ]

        warnings = self.engine.check_compliance(
            employee_id="emp_001",
            shifts=shifts,
            employment_type=self.EmploymentType.FULL_TIME,
            age=None
        )

        # Should have error about insufficient break
        self.assertTrue(
            any(w.rule == "min_break_between_shifts" for w in warnings),
            "Expected compliance warning for insufficient break"
        )

    def test_public_holiday_detection(self):
        """Test that known public holidays are detected."""
        christmas = date(2026, 12, 25)
        self.assertTrue(self.engine.is_public_holiday(christmas))

        holiday_name = self.engine.get_public_holiday_name(christmas)
        self.assertEqual(holiday_name, "Christmas Day")

    def test_non_public_holiday_not_detected(self):
        """Test that regular days are not public holidays."""
        regular_day = date(2026, 4, 20)  # Regular Monday (not a holiday)
        self.assertFalse(self.engine.is_public_holiday(regular_day))


if __name__ == "__main__":
    unittest.main()
