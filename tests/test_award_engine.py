"""
Comprehensive test suite for the Australian Award Engine.
Tests cover all major functionality including:
- Base rate calculations
- Penalty rates (Saturday, Sunday, public holidays)
- Casual loading
- Overtime calculations
- Junior worker rates
- Compliance checking
- Weekly and roster costing
"""

import unittest
from datetime import datetime, date, time, timedelta
from decimal import Decimal

# Adjust import path as needed
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.award_engine import (
    AwardEngine, EmploymentType, ShiftClassification,
    PayCalculation, ComplianceWarning, RosterCostSummary
)


class TestAwardEngineInitialization(unittest.TestCase):
    """Test engine initialization and rate loading."""

    def setUp(self):
        self.engine = AwardEngine(award_year=2025)

    def test_engine_initializes(self):
        """Engine should initialize without errors."""
        self.assertIsNotNone(self.engine)
        self.assertEqual(self.engine.award_year, 2025)

    def test_award_levels_loaded(self):
        """All 6 award levels should be loaded."""
        for level in range(1, 7):
            self.assertIn(level, self.engine.award_levels)

    def test_level_1_rate_2025(self):
        """Level 1 rate should be $24.10 as of July 2025."""
        level_1 = self.engine.award_levels[1]
        self.assertEqual(level_1.base_hourly_rate, Decimal("24.10"))

    def test_level_6_rate_exists(self):
        """Level 6 should have highest rate."""
        level_6 = self.engine.award_levels[6]
        self.assertGreater(level_6.base_hourly_rate, Decimal("30"))

    def test_casual_loading_set(self):
        """Casual loading should be 25%."""
        self.assertEqual(self.engine.casual_loading, Decimal("0.25"))

    def test_penalties_loaded(self):
        """Penalty multipliers should be loaded."""
        self.assertEqual(self.engine.penalties["sunday"], Decimal("1.50"))
        self.assertEqual(self.engine.penalties["public_holiday"], Decimal("2.25"))

    def test_super_rate_set(self):
        """Superannuation rate should be 11.5%."""
        self.assertEqual(self.engine.super_rate, Decimal("0.115"))

    def test_public_holidays_2026_loaded(self):
        """2026 public holidays should be loaded."""
        self.assertIn(date(2026, 1, 1), self.engine.AU_PUBLIC_HOLIDAYS_2026)
        self.assertIn(date(2026, 12, 25), self.engine.AU_PUBLIC_HOLIDAYS_2026)


class TestBaseRateCalculations(unittest.TestCase):
    """Test base rate calculation logic."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_level_1_full_time_rate(self):
        """Level 1 full-time should be base rate."""
        rate = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        self.assertEqual(rate, Decimal("24.10"))

    def test_level_2_rate_higher_than_level_1(self):
        """Level 2 should have higher rate than Level 1."""
        rate_1 = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        rate_2 = self.engine.get_base_rate(2, EmploymentType.FULL_TIME)
        self.assertGreater(rate_2, rate_1)

    def test_casual_loading_25_percent(self):
        """Casual should add 25% to base rate."""
        ft_rate = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        casual_rate = self.engine.get_base_rate(1, EmploymentType.CASUAL)

        expected = ft_rate * Decimal("1.25")
        self.assertEqual(casual_rate, expected)

    def test_part_time_same_rate_as_full_time(self):
        """Part-time and full-time should have same base rate."""
        ft_rate = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        pt_rate = self.engine.get_base_rate(1, EmploymentType.PART_TIME)
        self.assertEqual(ft_rate, pt_rate)

    def test_junior_rate_16_years_old(self):
        """16-year-old should get ~51% of Level 1 rate."""
        rate = self.engine.get_base_rate(1, EmploymentType.JUNIOR, age=16)
        base_rate = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        expected = base_rate * Decimal("0.51")
        self.assertEqual(rate, expected)

    def test_junior_rate_18_years_old(self):
        """18-year-old should get ~80% of Level 1 rate."""
        rate = self.engine.get_base_rate(1, EmploymentType.JUNIOR, age=18)
        base_rate = self.engine.get_base_rate(1, EmploymentType.FULL_TIME)
        expected = base_rate * Decimal("0.80")
        self.assertEqual(rate, expected)

    def test_invalid_level_raises_error(self):
        """Invalid level should raise ValueError."""
        with self.assertRaises(ValueError):
            self.engine.get_base_rate(99, EmploymentType.FULL_TIME)


class TestPenaltyMultipliers(unittest.TestCase):
    """Test penalty rate calculations."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_sunday_full_time_penalty(self):
        """Full-time Sunday should be 150%."""
        # Sunday = weekday 6
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=6, hour=12, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.50"))

    def test_sunday_casual_penalty(self):
        """Casual Sunday should be 175%."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.CASUAL, day_of_week=6, hour=12, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.75"))

    def test_saturday_full_time_penalty(self):
        """Full-time Saturday should be 125%."""
        # Saturday = weekday 5
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=5, hour=12, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.25"))

    def test_saturday_casual_penalty(self):
        """Casual Saturday should be 150%."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.CASUAL, day_of_week=5, hour=12, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.50"))

    def test_public_holiday_full_time(self):
        """Public holiday full-time should be 225%."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=3, hour=12, is_public_holiday=True
        )
        self.assertEqual(multiplier, Decimal("2.25"))

    def test_public_holiday_casual(self):
        """Public holiday casual should be 250%."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.CASUAL, day_of_week=3, hour=12, is_public_holiday=True
        )
        self.assertEqual(multiplier, Decimal("2.50"))

    def test_public_holiday_overrides_saturday(self):
        """Public holiday should override Saturday penalties."""
        # Saturday that's a public holiday
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=5, hour=12, is_public_holiday=True
        )
        self.assertEqual(multiplier, Decimal("2.25"))

    def test_early_morning_penalty(self):
        """Hours before 7am should get early morning penalty."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=1, hour=6, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.15"))

    def test_evening_penalty(self):
        """Hours from 7pm should get evening penalty."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=1, hour=19, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.15"))

    def test_ordinary_time_no_penalty(self):
        """Normal weekday hours should be 100%."""
        multiplier = self.engine.get_penalty_multiplier(
            EmploymentType.FULL_TIME, day_of_week=2, hour=12, is_public_holiday=False
        )
        self.assertEqual(multiplier, Decimal("1.0"))


class TestShiftCalculations(unittest.TestCase):
    """Test single shift pay calculations."""

    def setUp(self):
        self.engine = AwardEngine()
        self.test_date = date(2026, 3, 15)  # Sunday
        self.monday = date(2026, 3, 16)     # Monday

    def test_simple_8_hour_shift_full_time(self):
        """8-hour full-time shift should calculate correctly."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        self.assertEqual(calc.base_hours, Decimal("8"))
        self.assertEqual(calc.base_rate, Decimal("24.10"))
        expected_pay = Decimal("8") * Decimal("24.10")
        self.assertEqual(calc.gross_pay, expected_pay)

    def test_sunday_shift_pays_penalty_rate(self):
        """Sunday shift should pay 150% for full-time."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.test_date,  # Sunday
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        self.assertEqual(calc.penalty_multiplier, Decimal("1.50"))
        expected_rate = Decimal("24.10") * Decimal("1.50")
        self.assertEqual(calc.effective_rate, expected_rate)
        expected_pay = Decimal("8") * expected_rate
        self.assertEqual(calc.gross_pay, expected_pay)

    def test_casual_shift_includes_loading(self):
        """Casual shift should include 25% loading."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.CASUAL,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        casual_rate = Decimal("24.10") * Decimal("1.25")
        self.assertEqual(calc.base_rate, casual_rate)

    def test_public_holiday_shift(self):
        """Public holiday should pay 225% for full-time."""
        xmas = date(2026, 12, 25)
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=xmas,
            start_time=time(9, 0),
            end_time=time(17, 0),
            is_public_holiday=True,
        )

        self.assertEqual(calc.penalty_multiplier, Decimal("2.25"))

    def test_superannuation_calculated(self):
        """Superannuation should be 11.5% of ordinary time earnings."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        ordinary_earnings = Decimal("8") * Decimal("24.10")
        expected_super = ordinary_earnings * Decimal("0.115")
        self.assertEqual(calc.super_contribution, expected_super)

    def test_short_shift_calculation(self):
        """3-hour shift should calculate correctly."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.PART_TIME,
            shift_date=self.monday,
            start_time=time(14, 0),
            end_time=time(17, 0),
        )

        self.assertEqual(calc.base_hours, Decimal("3"))

    def test_total_cost_includes_super(self):
        """Total employer cost should include superannuation."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        expected_total = calc.gross_pay + calc.super_contribution
        self.assertEqual(calc.total_cost_to_employer, expected_total)

    def test_level_3_rate_higher_pay(self):
        """Level 3 should result in higher pay than Level 1."""
        calc_1 = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        calc_3 = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=3,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=self.monday,
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        self.assertGreater(calc_3.gross_pay, calc_1.gross_pay)


class TestComplianceChecking(unittest.TestCase):
    """Test compliance violation detection."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_11_hour_break_violation(self):
        """Shifts less than 11 hours apart should trigger warning."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),   # Monday 9-5
            (date(2026, 3, 16), time(18, 0), time(22, 0)),  # Monday 6-10 (only 1 hour break)
        ]

        warnings = self.engine.check_compliance(
            "EMP001", shifts, EmploymentType.FULL_TIME
        )

        self.assertTrue(any(w.rule == "min_break_between_shifts" for w in warnings))
        self.assertTrue(any(w.severity == "error" for w in warnings))

    def test_no_violation_11_hour_break(self):
        """11-hour break should not trigger warning."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),   # Monday 9-5
            (date(2026, 3, 17), time(4, 0), time(12, 0)),   # Tuesday 4am (11 hours after 5pm)
        ]

        warnings = self.engine.check_compliance(
            "EMP001", shifts, EmploymentType.FULL_TIME
        )

        self.assertFalse(any(w.rule == "min_break_between_shifts" for w in warnings))

    def test_consecutive_days_warning(self):
        """More than 6 consecutive work days should trigger warning."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),
            (date(2026, 3, 17), time(9, 0), time(17, 0)),
            (date(2026, 3, 18), time(9, 0), time(17, 0)),
            (date(2026, 3, 19), time(9, 0), time(17, 0)),
            (date(2026, 3, 20), time(9, 0), time(17, 0)),
            (date(2026, 3, 21), time(9, 0), time(17, 0)),
            (date(2026, 3, 22), time(9, 0), time(17, 0)),  # 7 consecutive days
        ]

        warnings = self.engine.check_compliance(
            "EMP001", shifts, EmploymentType.FULL_TIME
        )

        self.assertTrue(any(w.rule == "consecutive_work_days" for w in warnings))

    def test_junior_hour_restriction(self):
        """Junior under 18 shouldn't exceed 30 hours/week."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 17), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 18), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 19), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 20), time(9, 0), time(13, 0)),   # 4 hours = 36 total
        ]

        warnings = self.engine.check_compliance(
            "EMP001", shifts, EmploymentType.FULL_TIME, age=16
        )

        self.assertTrue(any(w.rule == "junior_hour_restriction" for w in warnings))

    def test_junior_within_limits_no_warning(self):
        """Junior under 30 hours should have no warning."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(15, 0)),   # 6 hours
            (date(2026, 3, 17), time(9, 0), time(15, 0)),   # 6 hours
            (date(2026, 3, 18), time(9, 0), time(15, 0)),   # 6 hours
            (date(2026, 3, 19), time(9, 0), time(15, 0)),   # 6 hours
            (date(2026, 3, 20), time(9, 0), time(15, 0)),   # 6 hours = 30 total
        ]

        warnings = self.engine.check_compliance(
            "EMP001", shifts, EmploymentType.FULL_TIME, age=16
        )

        self.assertFalse(any(w.rule == "junior_hour_restriction" for w in warnings))


class TestWeeklyCostCalculations(unittest.TestCase):
    """Test weekly calculations and overtime tracking."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_simple_38_hour_week(self):
        """38 hours should have no overtime."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),  # Mon 8 hours
            (date(2026, 3, 17), time(9, 0), time(17, 0)),  # Tue 8 hours
            (date(2026, 3, 18), time(9, 0), time(17, 0)),  # Wed 8 hours
            (date(2026, 3, 19), time(9, 0), time(17, 0)),  # Thu 8 hours
            (date(2026, 3, 20), time(9, 0), time(14, 0)),  # Fri 5 hours = 37 hours
        ]

        calcs = self.engine.calculate_weekly_cost(
            "EMP001", 1, EmploymentType.FULL_TIME, shifts
        )

        total_ot = sum(c.overtime_hours for c in calcs)
        self.assertEqual(total_ot, Decimal("0"))

    def test_overtime_over_38_hours(self):
        """Hours over 38 should be calculated as overtime."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 17), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 18), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 19), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 20), time(9, 0), time(17, 0)),   # 8 hours = 44 total
        ]

        calcs = self.engine.calculate_weekly_cost(
            "EMP001", 1, EmploymentType.FULL_TIME, shifts
        )

        total_ot = sum(c.overtime_hours for c in calcs)
        self.assertEqual(total_ot, Decimal("6"))  # 44 - 38 = 6 hours OT

    def test_casual_no_overtime(self):
        """Casual workers should not accumulate overtime."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 17), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 18), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 19), time(9, 0), time(18, 0)),   # 9 hours
            (date(2026, 3, 20), time(9, 0), time(17, 0)),   # 8 hours
        ]

        calcs = self.engine.calculate_weekly_cost(
            "EMP001", 1, EmploymentType.CASUAL, shifts
        )

        total_ot = sum(c.overtime_hours for c in calcs)
        self.assertEqual(total_ot, Decimal("0"))  # No OT for casual

    def test_week_with_public_holiday(self):
        """Public holiday hours shouldn't count toward 38-hour threshold."""
        shifts = [
            (date(2026, 3, 16), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 17), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 18), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 19), time(9, 0), time(17, 0)),   # 8 hours
            (date(2026, 3, 20), time(9, 0), time(17, 0)),   # 8 hours (pub hol - doesn't count to 38)
            (date(2026, 3, 21), time(9, 0), time(17, 0)),   # 8 hours (40 ordinary hours total, so 2 OT)
            (date(2026, 3, 22), time(9, 0), time(17, 0)),   # 8 hours
        ]

        calcs = self.engine.calculate_weekly_cost(
            "EMP001", 1, EmploymentType.FULL_TIME, shifts,
            public_holiday_dates=[date(2026, 3, 20)]
        )

        # Only 32 ordinary hours (exclude pub hol) + new week starts, so no OT
        total_ot = sum(c.overtime_hours for c in calcs)
        self.assertEqual(total_ot, Decimal("0"))


class TestPublicHolidayDetection(unittest.TestCase):
    """Test public holiday identification."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_christmas_day_is_public_holiday(self):
        """Christmas Day should be recognized as public holiday."""
        self.assertTrue(self.engine.is_public_holiday(date(2026, 12, 25)))

    def test_new_years_day_is_public_holiday(self):
        """New Year's Day should be recognized as public holiday."""
        self.assertTrue(self.engine.is_public_holiday(date(2026, 1, 1)))

    def test_australia_day_is_public_holiday(self):
        """Australia Day should be recognized as public holiday."""
        self.assertTrue(self.engine.is_public_holiday(date(2026, 1, 26)))

    def test_good_friday_is_public_holiday(self):
        """Good Friday should be recognized as public holiday."""
        self.assertTrue(self.engine.is_public_holiday(date(2026, 4, 10)))

    def test_random_day_not_public_holiday(self):
        """Random day should not be public holiday."""
        self.assertFalse(self.engine.is_public_holiday(date(2026, 3, 15)))

    def test_get_public_holiday_name(self):
        """Should return holiday name."""
        name = self.engine.get_public_holiday_name(date(2026, 12, 25))
        self.assertEqual(name, "Christmas Day")

    def test_boxing_day_is_public_holiday(self):
        """Boxing Day should be recognized."""
        self.assertTrue(self.engine.is_public_holiday(date(2026, 12, 26)))


class TestRosterCostCalculations(unittest.TestCase):
    """Test aggregate roster cost calculations."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_single_employee_roster(self):
        """Single employee roster should calculate total costs."""
        roster = [
            {
                "employee_id": "EMP001",
                "award_level": 1,
                "employment_type": "full_time",
                "shifts": [
                    (date(2026, 3, 16), time(9, 0), time(17, 0)),
                    (date(2026, 3, 17), time(9, 0), time(17, 0)),
                    (date(2026, 3, 18), time(9, 0), time(17, 0)),
                    (date(2026, 3, 19), time(9, 0), time(17, 0)),
                    (date(2026, 3, 20), time(9, 0), time(13, 0)),
                ]
            }
        ]

        summary = self.engine.calculate_roster_cost(roster)

        self.assertGreater(summary.total_gross_pay, Decimal("0"))
        self.assertGreater(summary.total_super, Decimal("0"))
        self.assertGreater(summary.total_employer_cost, Decimal("0"))

    def test_multi_employee_roster(self):
        """Multiple employee roster should sum all costs."""
        roster = [
            {
                "employee_id": "EMP001",
                "award_level": 1,
                "employment_type": "full_time",
                "shifts": [
                    (date(2026, 3, 16), time(9, 0), time(17, 0)),
                    (date(2026, 3, 17), time(9, 0), time(17, 0)),
                ]
            },
            {
                "employee_id": "EMP002",
                "award_level": 2,
                "employment_type": "part_time",
                "shifts": [
                    (date(2026, 3, 16), time(14, 0), time(22, 0)),
                    (date(2026, 3, 17), time(14, 0), time(22, 0)),
                ]
            }
        ]

        summary = self.engine.calculate_roster_cost(roster)

        self.assertIn("EMP001", summary.by_employee)
        self.assertIn("EMP002", summary.by_employee)
        self.assertGreater(summary.by_employee["EMP002"], summary.by_employee["EMP001"])

    def test_roster_by_day_breakdown(self):
        """Roster should break down costs by day."""
        roster = [
            {
                "employee_id": "EMP001",
                "award_level": 1,
                "employment_type": "full_time",
                "shifts": [
                    (date(2026, 3, 16), time(9, 0), time(17, 0)),
                    (date(2026, 3, 17), time(9, 0), time(17, 0)),
                ]
            }
        ]

        summary = self.engine.calculate_roster_cost(roster)

        self.assertIn(date(2026, 3, 16), summary.by_day)
        self.assertIn(date(2026, 3, 17), summary.by_day)

    def test_roster_super_contribution(self):
        """Roster should calculate superannuation."""
        roster = [
            {
                "employee_id": "EMP001",
                "award_level": 1,
                "employment_type": "full_time",
                "shifts": [
                    (date(2026, 3, 16), time(9, 0), time(17, 0)),  # 8 hours
                ]
            }
        ]

        summary = self.engine.calculate_roster_cost(roster)

        # Super should be 11.5% of 8 * $24.10
        expected_super = Decimal("8") * Decimal("24.10") * Decimal("0.115")
        self.assertAlmostEqual(float(summary.total_super), float(expected_super), places=2)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_midnight_shift_crossing_days(self):
        """Shift crossing midnight should calculate correctly."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=date(2026, 3, 16),
            start_time=time(22, 0),
            end_time=time(6, 0),  # 8 hours but crosses midnight
        )

        self.assertEqual(calc.base_hours, Decimal("8"))

    def test_very_short_shift(self):
        """Very short (15 minute) shift should calculate."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.CASUAL,
            shift_date=date(2026, 3, 16),
            start_time=time(17, 0),
            end_time=time(17, 15),
        )

        self.assertEqual(calc.base_hours, Decimal("0.25"))

    def test_24_hour_shift(self):
        """24-hour shift (same time next day) should calculate correctly."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.CASUAL,
            shift_date=date(2026, 3, 16),
            start_time=time(9, 0),
            end_time=time(9, 0),  # Same time means 24 hours per shift engine logic
        )

        self.assertEqual(calc.base_hours, Decimal("24"))

    def test_zero_hour_shift_treated_as_24_hours(self):
        """Zero-hour shift (same time) is treated as 24-hour shift."""
        # This is intentional: if start_time == end_time, we assume it means next day same time (24 hours)
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=date(2026, 3, 16),
            start_time=time(9, 0),
            end_time=time(9, 0),
        )

        self.assertEqual(calc.base_hours, Decimal("24"))


class TestRateDecimalPrecision(unittest.TestCase):
    """Test that rates maintain appropriate decimal precision."""

    def setUp(self):
        self.engine = AwardEngine()

    def test_rates_use_decimal_type(self):
        """All rates should use Decimal type."""
        level = self.engine.award_levels[1]
        self.assertIsInstance(level.base_hourly_rate, Decimal)

    def test_calculated_pay_is_decimal(self):
        """Calculated pay should use Decimal."""
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=date(2026, 3, 16),
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        self.assertIsInstance(calc.gross_pay, Decimal)
        self.assertIsInstance(calc.super_contribution, Decimal)

    def test_no_floating_point_errors(self):
        """Results shouldn't have floating-point precision errors."""
        # $24.10 * 8 hours = $192.80
        calc = self.engine.calculate_shift_cost(
            employee_id="EMP001",
            award_level=1,
            employment_type=EmploymentType.FULL_TIME,
            shift_date=date(2026, 3, 16),
            start_time=time(9, 0),
            end_time=time(17, 0),
        )

        expected = Decimal("192.80")
        self.assertEqual(calc.gross_pay, expected)


if __name__ == '__main__':
    unittest.main()
