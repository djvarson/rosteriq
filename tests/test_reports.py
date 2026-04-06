"""
Comprehensive test suite for RosterIQ Reporting and Analytics Module.

Tests cover:
- Labour cost calculations and breakdowns
- Forecast accuracy measurements
- Roster efficiency scoring
- Employee performance reporting
- Period comparisons
- CSV/JSON export functionality
- Formatting functions
- Analytics and turnover risk calculation
"""

import unittest
import tempfile
import os
import json
import csv
from datetime import datetime, date, timedelta
from decimal import Decimal

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.reports import (
    ReportPeriod, LabourCostReport, ForecastAccuracyReport,
    RosterEfficiencyReport, EmployeeReport, WeeklyDigest,
    ReportGenerator, export_to_csv, export_to_json,
    export_roster_to_csv, format_currency, format_percentage,
    format_hours, calculate_labour_percentage, calculate_staff_utilisation,
    identify_overstaffed_periods, identify_understaffed_periods,
    calculate_turnover_risk
)


# ============================================================================
# TEST DATA FIXTURES
# ============================================================================

def create_sample_roster():
    """Create sample roster data for testing."""
    return [
        {
            'id': 'shift_001',
            'date': '2025-04-01',
            'start_hour': 9,
            'end_hour': 17,
            'role_required': 'bar',
            'employee_id': 'emp_001',
            'break_minutes': 30
        },
        {
            'id': 'shift_002',
            'date': '2025-04-01',
            'start_hour': 17,
            'end_hour': 22,
            'role_required': 'bar',
            'employee_id': 'emp_002',
            'break_minutes': 30
        },
        {
            'id': 'shift_003',
            'date': '2025-04-02',
            'start_hour': 10,
            'end_hour': 18,
            'role_required': 'kitchen',
            'employee_id': 'emp_003',
            'break_minutes': 30
        },
        {
            'id': 'shift_004',
            'date': '2025-04-05',
            'start_hour': 8,
            'end_hour': 16,
            'role_required': 'floor',
            'employee_id': 'emp_001',
            'break_minutes': 30
        },
        {
            'id': 'shift_005',
            'date': '2025-04-06',
            'start_hour': 18,
            'end_hour': 23,
            'role_required': 'bar',
            'employee_id': 'emp_002',
            'break_minutes': 30
        }
    ]


def create_sample_pay_calculations():
    """Create sample pay calculation data."""
    return [
        {
            'employee_id': 'emp_001',
            'total': 300.00,
            'overtime_hours': 2.0,
            'overtime_cost': 45.00,
            'penalty_cost': 20.00,
            'super_cost': 28.50
        },
        {
            'employee_id': 'emp_002',
            'total': 275.00,
            'overtime_hours': 0.0,
            'overtime_cost': 0.00,
            'penalty_cost': 30.00,
            'super_cost': 26.13
        },
        {
            'employee_id': 'emp_003',
            'total': 250.00,
            'overtime_hours': 1.0,
            'overtime_cost': 25.00,
            'penalty_cost': 0.00,
            'super_cost': 23.75
        }
    ]


def create_sample_forecasts():
    """Create sample forecast data."""
    return [
        {'date': '2025-04-01', 'hour': 12, 'predicted_covers': 30, 'source': 'historical'},
        {'date': '2025-04-01', 'hour': 13, 'predicted_covers': 40, 'source': 'historical'},
        {'date': '2025-04-01', 'hour': 18, 'predicted_covers': 50, 'source': 'ml_model'},
        {'date': '2025-04-01', 'hour': 19, 'predicted_covers': 55, 'source': 'ml_model'},
        {'date': '2025-04-02', 'hour': 12, 'predicted_covers': 28, 'source': 'historical'},
        {'date': '2025-04-02', 'hour': 18, 'predicted_covers': 45, 'source': 'ml_model'},
    ]


def create_sample_actuals():
    """Create sample actual covers data."""
    return [
        {'date': '2025-04-01', 'hour': 12, 'actual_covers': 28},
        {'date': '2025-04-01', 'hour': 13, 'actual_covers': 42},
        {'date': '2025-04-01', 'hour': 18, 'actual_covers': 48},
        {'date': '2025-04-01', 'hour': 19, 'actual_covers': 57},
        {'date': '2025-04-02', 'hour': 12, 'actual_covers': 30},
        {'date': '2025-04-02', 'hour': 18, 'actual_covers': 43},
    ]


def create_sample_demand():
    """Create sample demand forecast data."""
    return [
        {'date': '2025-04-01', 'hour': 9, 'required_staff': 2},
        {'date': '2025-04-01', 'hour': 10, 'required_staff': 2},
        {'date': '2025-04-01', 'hour': 17, 'required_staff': 3},
        {'date': '2025-04-01', 'hour': 18, 'required_staff': 3},
        {'date': '2025-04-02', 'hour': 10, 'required_staff': 2},
        {'date': '2025-04-02', 'hour': 18, 'required_staff': 2},
    ]


def create_sample_employees():
    """Create sample employee data."""
    return [
        {'id': 'emp_001', 'name': 'Alice Smith'},
        {'id': 'emp_002', 'name': 'Bob Jones'},
        {'id': 'emp_003', 'name': 'Charlie Brown'},
    ]


# ============================================================================
# TEST CLASSES
# ============================================================================

class TestReportPeriod(unittest.TestCase):
    """Test ReportPeriod data model."""

    def test_period_creation(self):
        """ReportPeriod should be created with all fields."""
        period = ReportPeriod('2025-04-01', '2025-04-07', 'venue_001', 'Test Venue')
        self.assertEqual(period.start_date, '2025-04-01')
        self.assertEqual(period.end_date, '2025-04-07')
        self.assertEqual(period.venue_id, 'venue_001')
        self.assertEqual(period.venue_name, 'Test Venue')


class TestLabourCostReport(unittest.TestCase):
    """Test labour cost report generation."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.roster = create_sample_roster()
        self.pay_calcs = create_sample_pay_calculations()

    def test_labour_report_generation(self):
        """Should generate valid labour cost report."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertIsInstance(report, LabourCostReport)
        self.assertEqual(report.period.venue_id, 'venue_001')
        self.assertGreater(report.total_cost, 0)
        self.assertGreater(report.total_hours, 0)

    def test_labour_cost_aggregation(self):
        """Should aggregate costs correctly."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        expected_cost = 300.00 + 275.00 + 250.00
        self.assertEqual(report.total_cost, expected_cost)

    def test_cost_by_day_breakdown(self):
        """Should break down costs by day."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertIn('2025-04-01', report.cost_by_day)
        self.assertGreater(report.cost_by_day['2025-04-01'], 0)

    def test_cost_by_role_breakdown(self):
        """Should break down costs by role."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertIn('bar', report.cost_by_role)
        self.assertGreater(report.cost_by_role['bar'], 0)

    def test_cost_by_employee_breakdown(self):
        """Should break down costs by employee."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertIn('emp_001', report.cost_by_employee)
        self.assertEqual(report.cost_by_employee['emp_001'], 300.00)

    def test_overtime_aggregation(self):
        """Should aggregate overtime hours and costs."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertEqual(report.overtime_hours, 3.0)
        self.assertEqual(report.overtime_cost, 70.00)

    def test_penalty_cost_aggregation(self):
        """Should aggregate penalty costs."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertEqual(report.penalty_cost, 50.00)

    def test_super_cost_aggregation(self):
        """Should aggregate superannuation costs."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        self.assertEqual(report.super_cost, 78.38)

    def test_budget_variance_calculation(self):
        """Should calculate budget variance correctly."""
        budget = 750.00
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs, budget)
        self.assertEqual(report.budget, 750.00)
        self.assertEqual(report.budget_variance, 75.00)
        self.assertEqual(report.budget_variance_pct, 10.0)

    def test_average_hourly_cost(self):
        """Should calculate average hourly cost."""
        report = self.generator.generate_labour_cost_report(self.roster, self.pay_calcs)
        expected_avg = report.total_cost / report.total_hours
        self.assertAlmostEqual(report.avg_hourly_cost, expected_avg, places=1)

    def test_empty_roster_handling(self):
        """Should handle empty roster gracefully."""
        report = self.generator.generate_labour_cost_report([], self.pay_calcs)
        self.assertEqual(report.total_cost, 0)
        self.assertEqual(report.total_hours, 0)


class TestForecastAccuracyReport(unittest.TestCase):
    """Test forecast accuracy report generation."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.forecasts = create_sample_forecasts()
        self.actuals = create_sample_actuals()

    def test_forecast_report_generation(self):
        """Should generate valid forecast accuracy report."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertIsInstance(report, ForecastAccuracyReport)
        self.assertGreaterEqual(report.overall_accuracy_pct, 0)
        self.assertLessEqual(report.overall_accuracy_pct, 100)

    def test_forecast_accuracy_range(self):
        """Forecast accuracy should be 0-100%."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreaterEqual(report.overall_accuracy_pct, 0)
        self.assertLessEqual(report.overall_accuracy_pct, 100)

    def test_forecast_by_day_breakdown(self):
        """Should breakdown accuracy by day."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertIn('2025-04-01', report.by_day)

    def test_forecast_by_hour_breakdown(self):
        """Should breakdown accuracy by hour."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreater(len(report.by_hour), 0)

    def test_forecast_by_source_breakdown(self):
        """Should breakdown accuracy by signal source."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertIn('historical', report.by_signal_source)
        self.assertIn('ml_model', report.by_signal_source)

    def test_mean_absolute_error_calculation(self):
        """Should calculate MAE correctly."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreaterEqual(report.mean_absolute_error, 0)

    def test_total_covers_tracking(self):
        """Should track total forecast and actual covers."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreater(report.total_forecast_covers, 0)
        self.assertGreater(report.total_actual_covers, 0)

    def test_overforecast_detection(self):
        """Should identify overforecasted hours."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreaterEqual(report.overforecast_hours, 0)

    def test_underforecast_detection(self):
        """Should identify underforecasted hours."""
        report = self.generator.generate_forecast_accuracy(self.forecasts, self.actuals)
        self.assertGreaterEqual(report.underforecast_hours, 0)


class TestRosterEfficiencyReport(unittest.TestCase):
    """Test roster efficiency report generation."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.roster = create_sample_roster()
        self.demand = create_sample_demand()

    def test_efficiency_report_generation(self):
        """Should generate valid efficiency report."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertIsInstance(report, RosterEfficiencyReport)

    def test_coverage_score_range(self):
        """Coverage score should be 0-100."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.coverage_score, 0)
        self.assertLessEqual(report.coverage_score, 100)

    def test_fairness_score_range(self):
        """Fairness score should be 0-100."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.fairness_score, 0)
        self.assertLessEqual(report.fairness_score, 100)

    def test_cost_efficiency_score_range(self):
        """Cost efficiency score should be 0-100."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.cost_efficiency_score, 0)
        self.assertLessEqual(report.cost_efficiency_score, 100)

    def test_overall_score_calculation(self):
        """Overall score should be weighted average of component scores."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.overall_score, 0)
        self.assertLessEqual(report.overall_score, 100)

    def test_staff_utilisation_calculation(self):
        """Should calculate staff utilisation percentage."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.staff_utilisation_pct, 0)
        self.assertLessEqual(report.staff_utilisation_pct, 100)

    def test_overstaffed_hours_detection(self):
        """Should identify overstaffed hours."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.overstaffed_hours, 0)

    def test_understaffed_hours_detection(self):
        """Should identify understaffed hours."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertGreaterEqual(report.understaffed_hours, 0)

    def test_recommendations_generation(self):
        """Should generate recommendations list."""
        report = self.generator.generate_roster_efficiency(self.roster, self.demand)
        self.assertIsInstance(report.recommendations, list)
        self.assertGreater(len(report.recommendations), 0)


class TestEmployeeReport(unittest.TestCase):
    """Test employee report generation."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.employees = create_sample_employees()
        self.roster = create_sample_roster()
        self.pay_calcs = create_sample_pay_calculations()

    def test_employee_report_generation(self):
        """Should generate valid employee report."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertIsInstance(report, EmployeeReport)
        self.assertEqual(report.employee_id, 'emp_001')
        self.assertEqual(report.name, 'Alice Smith')

    def test_total_hours_calculation(self):
        """Should calculate total hours correctly."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertGreater(report.total_hours, 0)

    def test_shift_count(self):
        """Should count shifts correctly."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertEqual(report.total_shifts, 2)

    def test_average_shift_length(self):
        """Should calculate average shift length."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        if report.total_shifts > 0:
            self.assertAlmostEqual(
                report.avg_shift_length,
                report.total_hours / report.total_shifts,
                places=1
            )

    def test_weekend_shift_detection(self):
        """Should detect weekend shifts (Saturday/Sunday)."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertGreaterEqual(report.weekend_shifts, 0)

    def test_evening_shift_detection(self):
        """Should detect evening shifts (18:00+)."""
        emp = self.employees[1]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertGreater(report.evening_shifts, 0)

    def test_overtime_hours_tracking(self):
        """Should track overtime hours from pay calculations."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertEqual(report.overtime_hours, 2.0)

    def test_total_earnings(self):
        """Should show total earnings from pay calculations."""
        emp = self.employees[0]
        emp_shifts = [s for s in self.roster if s['employee_id'] == emp['id']]
        report = self.generator.generate_employee_report(emp, emp_shifts, self.pay_calcs)
        self.assertEqual(report.total_earnings, 300.00)


class TestWeeklyDigest(unittest.TestCase):
    """Test weekly digest generation."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.roster = create_sample_roster()
        self.forecasts = create_sample_forecasts()
        self.actuals = create_sample_actuals()
        self.pay_calcs = create_sample_pay_calculations()

    def test_weekly_digest_generation(self):
        """Should generate valid weekly digest."""
        digest = self.generator.generate_weekly_digest(
            self.roster, self.forecasts, self.actuals, self.pay_calcs
        )
        self.assertIsInstance(digest, WeeklyDigest)
        self.assertGreater(len(digest.labour_summary), 0)

    def test_digest_includes_highlights(self):
        """Digest should include highlights list."""
        digest = self.generator.generate_weekly_digest(
            self.roster, self.forecasts, self.actuals, self.pay_calcs
        )
        self.assertIsInstance(digest.highlights, list)

    def test_digest_includes_warnings(self):
        """Digest should include warnings list."""
        digest = self.generator.generate_weekly_digest(
            self.roster, self.forecasts, self.actuals, self.pay_calcs
        )
        self.assertIsInstance(digest.warnings, list)

    def test_digest_includes_recommendations(self):
        """Digest should include recommendations."""
        digest = self.generator.generate_weekly_digest(
            self.roster, self.forecasts, self.actuals, self.pay_calcs
        )
        self.assertGreater(len(digest.recommendations), 0)

    def test_digest_includes_labour_summary(self):
        """Digest should include labour summary metrics."""
        digest = self.generator.generate_weekly_digest(
            self.roster, self.forecasts, self.actuals, self.pay_calcs
        )
        self.assertIn('total_cost', digest.labour_summary)
        self.assertIn('total_hours', digest.labour_summary)


class TestPeriodComparison(unittest.TestCase):
    """Test period-over-period comparison."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')
        self.roster_a = create_sample_roster()
        self.pay_calcs_a = create_sample_pay_calculations()
        self.roster_b = [
            {**r, 'date': '2025-04-08'} if r['date'].startswith('2025-04-0') else r
            for r in create_sample_roster()
        ]
        self.pay_calcs_b = [
            {**p, 'total': p['total'] * 1.1} for p in create_sample_pay_calculations()
        ]

    def test_period_comparison(self):
        """Should compare two labour cost reports."""
        report_a = self.generator.generate_labour_cost_report(self.roster_a, self.pay_calcs_a)
        report_b = self.generator.generate_labour_cost_report(self.roster_b, self.pay_calcs_b)

        comparison = self.generator.compare_periods(report_a, report_b)

        self.assertIn('cost_change', comparison)
        self.assertIn('cost_change_pct', comparison)
        self.assertIn('hours_change', comparison)
        self.assertIn('overtime_change', comparison)


class TestFormatting(unittest.TestCase):
    """Test formatting functions."""

    def test_format_currency(self):
        """Should format currency correctly."""
        self.assertEqual(format_currency(1234.56), '$1,234.56')
        self.assertEqual(format_currency(0), '$0.00')
        self.assertEqual(format_currency(1000000.00), '$1,000,000.00')

    def test_format_percentage(self):
        """Should format percentage correctly."""
        self.assertEqual(format_percentage(85.2), '85.2%')
        self.assertEqual(format_percentage(100), '100.0%')
        self.assertEqual(format_percentage(0), '0.0%')

    def test_format_hours(self):
        """Should format hours correctly."""
        self.assertEqual(format_hours(38.5), '38.5 hrs')
        self.assertEqual(format_hours(8.0), '8.0 hrs')
        self.assertEqual(format_hours(0), '0.0 hrs')


class TestAnalyticsCalculations(unittest.TestCase):
    """Test analytics calculation functions."""

    def test_labour_percentage_calculation(self):
        """Should calculate labour cost as % of revenue."""
        # AU hospitality target: 25-35%
        result = calculate_labour_percentage(25000, 100000)
        self.assertEqual(result, 25.0)

    def test_labour_percentage_zero_revenue(self):
        """Should handle zero revenue gracefully."""
        result = calculate_labour_percentage(1000, 0)
        self.assertEqual(result, 0)

    def test_staff_utilisation_calculation(self):
        """Should calculate staff utilisation percentage."""
        # 80 productive hours from 100 rostered = 80%
        result = calculate_staff_utilisation(100, 80)
        self.assertEqual(result, 80.0)

    def test_staff_utilisation_capped_at_100(self):
        """Utilisation should not exceed 100%."""
        result = calculate_staff_utilisation(100, 150)
        self.assertEqual(result, 100.0)

    def test_overstaffed_periods_identification(self):
        """Should identify overstaffed time periods."""
        roster = create_sample_roster()
        demand = [
            {'date': '2025-04-01', 'hour': 9, 'required_staff': 1},
            {'date': '2025-04-01', 'hour': 17, 'required_staff': 1},
        ]
        overstaffed = identify_overstaffed_periods(roster, demand)
        self.assertIsInstance(overstaffed, list)

    def test_understaffed_periods_identification(self):
        """Should identify understaffed time periods."""
        roster = create_sample_roster()
        demand = [
            {'date': '2025-04-01', 'hour': 9, 'required_staff': 5},
            {'date': '2025-04-01', 'hour': 17, 'required_staff': 5},
        ]
        understaffed = identify_understaffed_periods(roster, demand)
        self.assertIsInstance(understaffed, list)
        self.assertGreater(len(understaffed), 0)

    def test_turnover_risk_calculation(self):
        """Should calculate turnover risk for employees."""
        employees = create_sample_employees()
        emp_reports = [
            EmployeeReport(
                employee_id=e['id'],
                name=e['name'],
                total_hours=30.0,
                total_shifts=5,
                avg_shift_length=6.0,
                overtime_hours=0,
                weekend_shifts=1,
                evening_shifts=2,
                total_earnings=750.00
            )
            for e in employees
        ]
        risk_map = calculate_turnover_risk(emp_reports)
        self.assertIsInstance(risk_map, dict)


class TestCSVExport(unittest.TestCase):
    """Test CSV export functionality."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_labour_report_csv_export(self):
        """Should export labour report to CSV."""
        generator = ReportGenerator('venue_001', 'Test Venue')
        roster = create_sample_roster()
        pay_calcs = create_sample_pay_calculations()

        report = generator.generate_labour_cost_report(roster, pay_calcs)
        filepath = os.path.join(self.temp_dir, 'labour_report.csv')

        result = export_to_csv(report, filepath)

        self.assertTrue(os.path.exists(result))
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 1)

    def test_roster_export_csv(self):
        """Should export roster to CSV."""
        roster = create_sample_roster()
        employees = create_sample_employees()
        filepath = os.path.join(self.temp_dir, 'roster.csv')

        result = export_roster_to_csv(roster, employees, filepath)

        self.assertTrue(os.path.exists(result))
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), len(roster))

    def test_roster_csv_contains_expected_fields(self):
        """Exported roster CSV should have expected columns."""
        roster = create_sample_roster()
        employees = create_sample_employees()
        filepath = os.path.join(self.temp_dir, 'roster.csv')

        export_roster_to_csv(roster, employees, filepath)

        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            self.assertIn('date', headers)
            self.assertIn('employee_name', headers)
            self.assertIn('role', headers)
            self.assertIn('hours', headers)


class TestJSONExport(unittest.TestCase):
    """Test JSON export functionality."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_labour_report_json_export(self):
        """Should export labour report to JSON."""
        generator = ReportGenerator('venue_001', 'Test Venue')
        roster = create_sample_roster()
        pay_calcs = create_sample_pay_calculations()

        report = generator.generate_labour_cost_report(roster, pay_calcs)
        filepath = os.path.join(self.temp_dir, 'labour_report.json')

        result = export_to_json(report, filepath)

        self.assertTrue(os.path.exists(result))
        with open(filepath, 'r') as f:
            data = json.load(f)
            self.assertIn('total_cost', data)
            self.assertIn('total_hours', data)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def setUp(self):
        self.generator = ReportGenerator('venue_001', 'Test Venue')

    def test_empty_roster_returns_zero_cost(self):
        """Empty roster should return zero cost report."""
        report = self.generator.generate_labour_cost_report([], [])
        self.assertEqual(report.total_cost, 0)

    def test_single_shift_report(self):
        """Should handle single shift correctly."""
        roster = [create_sample_roster()[0]]
        pay_calcs = [create_sample_pay_calculations()[0]]
        report = self.generator.generate_labour_cost_report(roster, pay_calcs)
        self.assertEqual(len(report.cost_by_day), 1)

    def test_forecast_with_zero_demand(self):
        """Should handle zero demand gracefully."""
        forecasts = [{'date': '2025-04-01', 'hour': 12, 'predicted_covers': 0, 'source': 'test'}]
        actuals = [{'date': '2025-04-01', 'hour': 12, 'actual_covers': 0}]
        report = self.generator.generate_forecast_accuracy(forecasts, actuals)
        self.assertGreaterEqual(report.overall_accuracy_pct, 0)


if __name__ == '__main__':
    unittest.main()
