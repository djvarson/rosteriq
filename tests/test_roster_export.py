"""
Tests for Roster Export Module

Tests roster export functionality:
- CSV generation (headers, rows, escaping)
- JSON serialization
- HTML table generation (styling, grouping)
- Weekly grid format
- Daily breakdown with hourly analysis
- Summary calculations (hours, costs, headcount)
- Grouping by date/employee/role
- Cost and break filtering
- Edge cases (empty roster, single shift, overnight shifts)
"""

import unittest
from datetime import datetime, date, timedelta
from rosteriq.roster_export import (
    ExportedShift,
    ExportConfig,
    ExportFormat,
    RosterView,
    RosterExport,
    build_roster_export,
    export_csv,
    export_json,
    export_html_table,
    format_weekly_grid,
    format_daily_breakdown,
    calculate_export_summary,
)


class TestExportedShiftDataclass(unittest.TestCase):
    """Test ExportedShift dataclass."""

    def test_exported_shift_creation(self):
        """Test creating an ExportedShift."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice Smith",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=7.5,
            break_minutes=30,
            base_cost=187.50,
            penalty_cost=0.0,
            total_cost=187.50,
        )
        self.assertEqual(shift.employee_id, "e1")
        self.assertEqual(shift.employee_name, "Alice Smith")
        self.assertEqual(shift.hours, 7.5)

    def test_exported_shift_with_notes(self):
        """Test ExportedShift with notes."""
        shift = ExportedShift(
            employee_id="e2",
            employee_name="Bob Jones",
            role="kitchen",
            date="2026-04-20",
            start_time="09:00",
            end_time="17:00",
            hours=8.0,
            notes="Training session",
        )
        self.assertEqual(shift.notes, "Training session")


class TestCalculateSummary(unittest.TestCase):
    """Test summary calculation."""

    def test_empty_roster_summary(self):
        """Test summary for empty roster."""
        summary = calculate_export_summary([])
        self.assertEqual(summary["total_hours"], 0.0)
        self.assertEqual(summary["total_cost"], 0.0)
        self.assertEqual(summary["headcount"], 0)
        self.assertEqual(summary["shifts_count"], 0)

    def test_single_shift_summary(self):
        """Test summary for single shift."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=7.5,
            base_cost=187.50,
            total_cost=187.50,
        )
        summary = calculate_export_summary([shift])
        self.assertEqual(summary["total_hours"], 7.5)
        self.assertEqual(summary["total_cost"], 187.50)
        self.assertEqual(summary["headcount"], 1)
        self.assertEqual(summary["shifts_count"], 1)

    def test_multiple_shifts_summary(self):
        """Test summary for multiple shifts."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
                base_cost=200.0,
                total_cost=200.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="kitchen",
                date="2026-04-20",
                start_time="09:00",
                end_time="17:00",
                hours=8.0,
                base_cost=180.0,
                total_cost=180.0,
            ),
        ]
        summary = calculate_export_summary(shifts)
        self.assertEqual(summary["total_hours"], 16.0)
        self.assertEqual(summary["total_cost"], 380.0)
        self.assertEqual(summary["headcount"], 2)
        self.assertEqual(summary["shifts_count"], 2)

    def test_summary_with_penalty_costs(self):
        """Test summary includes penalty costs."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            base_cost=200.0,
            penalty_cost=40.0,
            total_cost=240.0,
        )
        summary = calculate_export_summary([shift])
        self.assertEqual(summary["total_base_cost"], 200.0)
        self.assertEqual(summary["total_penalty_cost"], 40.0)
        self.assertEqual(summary["total_cost"], 240.0)


class TestBuildRosterExport(unittest.TestCase):
    """Test build_roster_export function."""

    def test_build_basic_export(self):
        """Test building a basic roster export."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
                base_cost=200.0,
                total_cost=200.0,
            ),
        ]
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=shifts,
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
        )
        self.assertEqual(export.venue_id, "v1")
        self.assertEqual(export.venue_name, "The Bar")
        self.assertEqual(len(export.shifts), 1)
        self.assertEqual(export.summary["total_hours"], 8.0)

    def test_build_export_with_config(self):
        """Test building export with custom config."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
                break_minutes=30,
                base_cost=200.0,
                total_cost=200.0,
            ),
        ]
        config = ExportConfig(
            include_costs=False,
            include_breaks=False,
            include_notes=True,
        )
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=shifts,
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        # Costs should be zeroed
        self.assertEqual(export.shifts[0].base_cost, 0.0)
        self.assertEqual(export.shifts[0].break_minutes, 0)


class TestCsvExport(unittest.TestCase):
    """Test CSV export generation."""

    def test_csv_empty_roster(self):
        """Test CSV export for empty roster."""
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[],
        )
        csv_content = export_csv(export)
        self.assertIn("date", csv_content)
        self.assertIn("employee_id", csv_content)
        self.assertIn("employee_name", csv_content)

    def test_csv_headers(self):
        """Test CSV headers are correct."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        csv_content = export_csv(export)
        lines = csv_content.strip().split("\n")
        self.assertGreater(len(lines), 1)
        header = lines[0]
        self.assertIn("date", header)
        self.assertIn("employee_id", header)
        self.assertIn("employee_name", header)
        self.assertIn("role", header)
        self.assertIn("hours", header)

    def test_csv_data_rows(self):
        """Test CSV data rows are formatted correctly."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice Smith",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        csv_content = export_csv(export)
        self.assertIn("2026-04-20", csv_content)
        self.assertIn("Alice Smith", csv_content)
        self.assertIn("10:00", csv_content)
        self.assertIn("18:00", csv_content)

    def test_csv_includes_costs_when_present(self):
        """Test CSV includes cost columns when shifts have costs."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            base_cost=200.0,
            penalty_cost=25.0,
            total_cost=225.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        csv_content = export_csv(export)
        self.assertIn("base_cost", csv_content)
        self.assertIn("penalty_cost", csv_content)
        self.assertIn("total_cost", csv_content)

    def test_csv_escaping(self):
        """Test CSV properly escapes special characters."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name='Alice "The Bar Master" Smith',
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            notes='Trained by "Bob", excellent',
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        csv_content = export_csv(export)
        # CSV properly escapes quotes in quoted fields
        self.assertIn('"Alice ""The Bar Master"" Smith"', csv_content)


class TestJsonExport(unittest.TestCase):
    """Test JSON export generation."""

    def test_json_structure(self):
        """Test JSON export has correct structure."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        json_content = export_json(export)
        import json
        data = json.loads(json_content)
        self.assertEqual(data["venue_id"], "v1")
        self.assertEqual(data["venue_name"], "The Bar")
        self.assertEqual(len(data["shifts"]), 1)
        self.assertIn("summary", data)

    def test_json_shift_data(self):
        """Test JSON includes shift details."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            base_cost=200.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        json_content = export_json(export)
        import json
        data = json.loads(json_content)
        shift_data = data["shifts"][0]
        self.assertEqual(shift_data["employee_id"], "e1")
        self.assertEqual(shift_data["employee_name"], "Alice")
        self.assertEqual(shift_data["hours"], 8.0)


class TestHtmlExport(unittest.TestCase):
    """Test HTML table export generation."""

    def test_html_structure(self):
        """Test HTML export has basic structure."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        html = export_html_table(export)
        self.assertIn("<html", html)
        self.assertIn("</html>", html)
        self.assertIn("<table>", html)
        self.assertIn("</table>", html)

    def test_html_includes_venue_info(self):
        """Test HTML includes venue name and period."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        html = export_html_table(export)
        self.assertIn("The Bar", html)
        self.assertIn("2026-04-20", html)
        self.assertIn("2026-04-26", html)

    def test_html_escape_special_characters(self):
        """Test HTML properly escapes special characters."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice & Bob <Corp>",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            notes="Special chars: \"quotes\" & 'apostrophes'",
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar & Grill <Ltd>",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        html = export_html_table(export)
        self.assertIn("&amp;", html)
        self.assertIn("&lt;", html)
        self.assertIn("&gt;", html)
        self.assertNotIn("<Corp>", html)

    def test_html_highlights_weekends(self):
        """Test HTML applies weekend styling."""
        # Saturday is 2026-04-25
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-25",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        html = export_html_table(export)
        self.assertIn("weekend", html)
        self.assertIn("2026-04-25", html)

    def test_html_includes_costs(self):
        """Test HTML includes cost information."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            base_cost=200.0,
            penalty_cost=25.0,
            total_cost=225.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
            summary=calculate_export_summary([shift]),
        )
        html = export_html_table(export)
        self.assertIn("225.00", html)

    def test_html_summary_section(self):
        """Test HTML includes summary section."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
            summary=calculate_export_summary([shift]),
        )
        html = export_html_table(export)
        self.assertIn("Summary", html)
        self.assertIn("Total Hours", html)


class TestWeeklyGrid(unittest.TestCase):
    """Test weekly grid format."""

    def test_weekly_grid_structure(self):
        """Test weekly grid has correct structure."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="kitchen",
                date="2026-04-21",
                start_time="09:00",
                end_time="17:00",
                hours=8.0,
            ),
        ]
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=shifts,
        )
        grid = format_weekly_grid(export)
        self.assertEqual(len(grid[0]), 8)  # Header + 7 days
        self.assertEqual(grid[0][0], "Employee")

    def test_weekly_grid_shows_shift_times(self):
        """Test weekly grid shows shift times."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        grid = format_weekly_grid(export)
        # Find row with Alice
        alice_row = None
        for row in grid:
            if row[0] == "Alice":
                alice_row = row
                break
        self.assertIsNotNone(alice_row)
        # Check that some cell contains the shift time
        self.assertIn("10:00-18:00", alice_row)

    def test_weekly_grid_empty(self):
        """Test weekly grid for empty roster."""
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[],
        )
        grid = format_weekly_grid(export)
        self.assertEqual(len(grid), 1)  # Just header


class TestDailyBreakdown(unittest.TestCase):
    """Test daily breakdown generation."""

    def test_daily_breakdown_empty(self):
        """Test daily breakdown for day with no shifts."""
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.DAILY,
            shifts=[],
        )
        breakdown = format_daily_breakdown(export, "2026-04-20")
        self.assertEqual(breakdown["date"], "2026-04-20")
        self.assertEqual(len(breakdown["shifts"]), 0)
        self.assertEqual(breakdown["total_hours"], 0.0)

    def test_daily_breakdown_with_shifts(self):
        """Test daily breakdown includes all shifts for that day."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
                base_cost=200.0,
                total_cost=200.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="kitchen",
                date="2026-04-20",
                start_time="09:00",
                end_time="17:00",
                hours=8.0,
                base_cost=180.0,
                total_cost=180.0,
            ),
        ]
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.DAILY,
            shifts=shifts,
        )
        breakdown = format_daily_breakdown(export, "2026-04-20")
        self.assertEqual(len(breakdown["shifts"]), 2)
        self.assertEqual(breakdown["total_hours"], 16.0)
        self.assertEqual(breakdown["total_cost"], 380.0)
        self.assertEqual(breakdown["unique_staff"], 2)

    def test_daily_breakdown_headcount_by_hour(self):
        """Test daily breakdown calculates headcount by hour."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="14:00",
                hours=4.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="kitchen",
                date="2026-04-20",
                start_time="12:00",
                end_time="18:00",
                hours=6.0,
            ),
        ]
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.DAILY,
            shifts=shifts,
        )
        breakdown = format_daily_breakdown(export, "2026-04-20")
        headcount = breakdown["headcount_by_hour"]
        self.assertEqual(headcount.get("10:00"), 1)
        self.assertEqual(headcount.get("12:00"), 2)
        self.assertEqual(headcount.get("14:00"), 1)


class TestGrouping(unittest.TestCase):
    """Test shift grouping functionality."""

    def test_grouping_by_date(self):
        """Test shifts are grouped by date."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-21",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
        ]
        config = ExportConfig(group_by="date")
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=shifts,
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        # After grouping, 2026-04-20 should come first
        self.assertEqual(export.shifts[0].date, "2026-04-20")
        self.assertEqual(export.shifts[1].date, "2026-04-21")

    def test_grouping_by_employee(self):
        """Test shifts are grouped by employee."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Zoe",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
        ]
        config = ExportConfig(group_by="employee")
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=shifts,
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        # After grouping, Alice should come before Zoe
        self.assertEqual(export.shifts[0].employee_name, "Alice")
        self.assertEqual(export.shifts[1].employee_name, "Zoe")

    def test_grouping_by_role(self):
        """Test shifts are grouped by role."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="kitchen",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
            ExportedShift(
                employee_id="e2",
                employee_name="Bob",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="18:00",
                hours=8.0,
            ),
        ]
        config = ExportConfig(group_by="role")
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=shifts,
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        # After grouping, bar should come before kitchen
        self.assertEqual(export.shifts[0].role, "bar")
        self.assertEqual(export.shifts[1].role, "kitchen")


class TestConfigFiltering(unittest.TestCase):
    """Test config-based filtering."""

    def test_exclude_costs(self):
        """Test cost fields are zeroed when include_costs=False."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            base_cost=200.0,
            penalty_cost=25.0,
            total_cost=225.0,
        )
        config = ExportConfig(include_costs=False)
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=[shift],
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        self.assertEqual(export.shifts[0].base_cost, 0.0)
        self.assertEqual(export.shifts[0].penalty_cost, 0.0)
        self.assertEqual(export.shifts[0].total_cost, 0.0)

    def test_exclude_breaks(self):
        """Test break fields are zeroed when include_breaks=False."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
            break_minutes=30,
        )
        config = ExportConfig(include_breaks=False)
        export = build_roster_export(
            venue_id="v1",
            venue_name="The Bar",
            shifts=[shift],
            period_start="2026-04-20",
            period_end="2026-04-26",
            config=config,
        )
        self.assertEqual(export.shifts[0].break_minutes, 0)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and special scenarios."""

    def test_overnight_shift_parsing(self):
        """Test handling of overnight shifts."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="Alice",
            role="bar",
            date="2026-04-20",
            start_time="22:00",
            end_time="06:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.DAILY,
            shifts=[shift],
        )
        breakdown = format_daily_breakdown(export, "2026-04-20")
        self.assertEqual(len(breakdown["shifts"]), 1)

    def test_multiple_shifts_same_employee_same_day(self):
        """Test multiple shifts for same employee on same day."""
        shifts = [
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="10:00",
                end_time="14:00",
                hours=4.0,
            ),
            ExportedShift(
                employee_id="e1",
                employee_name="Alice",
                role="bar",
                date="2026-04-20",
                start_time="16:00",
                end_time="20:00",
                hours=4.0,
            ),
        ]
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.DAILY,
            shifts=shifts,
        )
        breakdown = format_daily_breakdown(export, "2026-04-20")
        self.assertEqual(len(breakdown["shifts"]), 2)
        self.assertEqual(breakdown["total_hours"], 8.0)
        self.assertEqual(breakdown["unique_staff"], 1)

    def test_special_characters_in_names(self):
        """Test handling of special characters in employee names."""
        shift = ExportedShift(
            employee_id="e1",
            employee_name="François O'Brien-Smith",
            role="bar",
            date="2026-04-20",
            start_time="10:00",
            end_time="18:00",
            hours=8.0,
        )
        export = RosterExport(
            venue_id="v1",
            venue_name="The Bar & Grill",
            period_start="2026-04-20",
            period_end="2026-04-26",
            view_type=RosterView.WEEKLY,
            shifts=[shift],
        )
        csv_content = export_csv(export)
        self.assertIn("François", csv_content)


if __name__ == "__main__":
    unittest.main()
