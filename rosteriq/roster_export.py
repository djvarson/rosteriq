"""
RosterIQ Roster Export Module

Provides comprehensive roster export functionality with multiple formats:
- CSV (comma-separated, full detail with grouping options)
- JSON (structured data for integrations)
- HTML (inline-styled tables for email/print)
- Weekly grid view (Mon-Sun, employee x day matrix)
- Daily breakdown with hourly headcount analysis

Features:
- Multi-view export (WEEKLY, DAILY, BY_ROLE, BY_EMPLOYEE)
- Configurable grouping (by date, employee, or role)
- Cost tracking (base, penalty, total)
- Break and note inclusion options
- Summary statistics (total hours, cost, headcount peaks)
- Proper CSV escaping and HTML entity handling
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from enum import Enum
import json
import csv
from io import StringIO


# ============================================================================
# ENUMS
# ============================================================================

class ExportFormat(str, Enum):
    """Export output format."""
    CSV = "csv"
    JSON = "json"
    HTML = "html"


class RosterView(str, Enum):
    """View/grouping type for roster export."""
    WEEKLY = "weekly"
    DAILY = "daily"
    BY_ROLE = "by_role"
    BY_EMPLOYEE = "by_employee"


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class ExportedShift:
    """Individual shift record for export."""
    employee_id: str
    employee_name: str
    role: str
    date: str  # YYYY-MM-DD
    start_time: str  # HH:MM
    end_time: str  # HH:MM
    hours: float
    break_minutes: int = 0
    base_cost: float = 0.0
    penalty_cost: float = 0.0
    total_cost: float = 0.0
    notes: str = ""


@dataclass
class ExportConfig:
    """Configuration for roster export."""
    include_costs: bool = True
    include_breaks: bool = True
    include_notes: bool = False
    group_by: str = "date"  # "date", "employee", or "role"


@dataclass
class RosterExport:
    """Complete roster export with metadata and summary."""
    venue_id: str
    venue_name: str
    period_start: str  # YYYY-MM-DD
    period_end: str  # YYYY-MM-DD
    view_type: RosterView
    shifts: List[ExportedShift]
    summary: Dict[str, Any] = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ============================================================================
# SUMMARY CALCULATIONS
# ============================================================================

def calculate_export_summary(shifts: List[ExportedShift]) -> Dict[str, Any]:
    """
    Calculate summary statistics for a roster.

    Args:
        shifts: List of exported shifts

    Returns:
        Dict with totals: total_hours, total_cost, total_base_cost, total_penalty_cost,
                         headcount, shifts_count, unique_employees, cost_per_hour
    """
    if not shifts:
        return {
            "total_hours": 0.0,
            "total_cost": 0.0,
            "total_base_cost": 0.0,
            "total_penalty_cost": 0.0,
            "headcount": 0,
            "shifts_count": 0,
            "unique_employees": 0,
            "cost_per_hour": 0.0,
        }

    total_hours = sum(s.hours for s in shifts)
    total_base_cost = sum(s.base_cost for s in shifts)
    total_penalty_cost = sum(s.penalty_cost for s in shifts)
    total_cost = total_base_cost + total_penalty_cost
    unique_employees = len(set(s.employee_id for s in shifts))
    shifts_count = len(shifts)

    cost_per_hour = total_cost / total_hours if total_hours > 0 else 0.0

    return {
        "total_hours": round(total_hours, 2),
        "total_cost": round(total_cost, 2),
        "total_base_cost": round(total_base_cost, 2),
        "total_penalty_cost": round(total_penalty_cost, 2),
        "headcount": unique_employees,
        "shifts_count": shifts_count,
        "unique_employees": unique_employees,
        "cost_per_hour": round(cost_per_hour, 2),
    }


# ============================================================================
# CORE EXPORT BUILDER
# ============================================================================

def build_roster_export(
    venue_id: str,
    venue_name: str,
    shifts: List[ExportedShift],
    period_start: str,
    period_end: str,
    view_type: RosterView = RosterView.WEEKLY,
    config: Optional[ExportConfig] = None,
) -> RosterExport:
    """
    Assemble roster export data with summary calculations.

    Args:
        venue_id: Venue identifier
        venue_name: Venue name
        shifts: List of ExportedShift records
        period_start: Period start (YYYY-MM-DD)
        period_end: Period end (YYYY-MM-DD)
        view_type: Export view type (default WEEKLY)
        config: Export configuration (default: all included)

    Returns:
        RosterExport with summary data
    """
    if config is None:
        config = ExportConfig()

    # Filter shifts based on config
    filtered_shifts = shifts
    if not config.include_costs:
        for shift in filtered_shifts:
            shift.base_cost = 0.0
            shift.penalty_cost = 0.0
            shift.total_cost = 0.0
    if not config.include_breaks:
        for shift in filtered_shifts:
            shift.break_minutes = 0

    # Group shifts if requested
    grouped_shifts = _group_shifts(filtered_shifts, config.group_by)

    summary = calculate_export_summary(filtered_shifts)

    return RosterExport(
        venue_id=venue_id,
        venue_name=venue_name,
        period_start=period_start,
        period_end=period_end,
        view_type=view_type,
        shifts=grouped_shifts,
        summary=summary,
        generated_at=datetime.now().isoformat(),
    )


def _group_shifts(shifts: List[ExportedShift], group_by: str) -> List[ExportedShift]:
    """
    Sort shifts by grouping preference.

    Args:
        shifts: List of shifts
        group_by: "date", "employee", or "role"

    Returns:
        Sorted list of shifts
    """
    if group_by == "date":
        return sorted(shifts, key=lambda s: (s.date, s.start_time, s.employee_name))
    elif group_by == "employee":
        return sorted(shifts, key=lambda s: (s.employee_name, s.date, s.start_time))
    elif group_by == "role":
        return sorted(shifts, key=lambda s: (s.role, s.date, s.employee_name))
    else:
        return sorted(shifts, key=lambda s: (s.date, s.start_time))


# ============================================================================
# CSV EXPORT
# ============================================================================

def export_csv(export: RosterExport) -> str:
    """
    Generate CSV string from roster export.

    Columns: date, employee_id, employee_name, role, start_time, end_time,
             hours, break_minutes (if enabled), base_cost, penalty_cost,
             total_cost (if enabled), notes (if enabled)

    Args:
        export: RosterExport instance

    Returns:
        CSV string with headers and rows
    """
    output = StringIO()

    if not export.shifts:
        # Empty roster: just headers
        headers = [
            "date",
            "employee_id",
            "employee_name",
            "role",
            "start_time",
            "end_time",
            "hours",
        ]
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        return output.getvalue()

    # Build headers based on first shift
    headers = [
        "date",
        "employee_id",
        "employee_name",
        "role",
        "start_time",
        "end_time",
        "hours",
    ]

    if export.shifts[0].break_minutes > 0 or any(
        s.break_minutes > 0 for s in export.shifts
    ):
        headers.append("break_minutes")

    if any(s.base_cost > 0 or s.penalty_cost > 0 or s.total_cost > 0 for s in export.shifts):
        headers.extend(["base_cost", "penalty_cost", "total_cost"])

    if any(s.notes for s in export.shifts):
        headers.append("notes")

    # Build rows
    rows = []
    for shift in export.shifts:
        row = {
            "date": shift.date,
            "employee_id": shift.employee_id,
            "employee_name": shift.employee_name,
            "role": shift.role,
            "start_time": shift.start_time,
            "end_time": shift.end_time,
            "hours": f"{shift.hours:.2f}",
        }
        if "break_minutes" in headers:
            row["break_minutes"] = shift.break_minutes
        if "base_cost" in headers:
            row["base_cost"] = f"{shift.base_cost:.2f}"
            row["penalty_cost"] = f"{shift.penalty_cost:.2f}"
            row["total_cost"] = f"{shift.total_cost:.2f}"
        if "notes" in headers:
            row["notes"] = shift.notes
        rows.append(row)

    # Write CSV
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

    return output.getvalue()


# ============================================================================
# JSON EXPORT
# ============================================================================

def export_json(export: RosterExport) -> str:
    """
    Generate JSON string from roster export.

    Args:
        export: RosterExport instance

    Returns:
        JSON string with export data
    """
    data = {
        "venue_id": export.venue_id,
        "venue_name": export.venue_name,
        "period_start": export.period_start,
        "period_end": export.period_end,
        "view_type": export.view_type.value,
        "generated_at": export.generated_at,
        "summary": export.summary,
        "shifts": [asdict(s) for s in export.shifts],
    }
    return json.dumps(data, indent=2, default=str)


# ============================================================================
# HTML EXPORT
# ============================================================================

def export_html_table(export: RosterExport) -> str:
    """
    Generate HTML table from roster export.

    Features:
    - Header with venue name, period, generated timestamp
    - Grouped rows by date or employee
    - Subtotals per group
    - Grand total footer
    - Color coding: weekends highlighted, cost warnings

    Args:
        export: RosterExport instance

    Returns:
        HTML string with styled table
    """
    html_parts = []

    # HTML header
    html_parts.append('<!DOCTYPE html>')
    html_parts.append('<html lang="en">')
    html_parts.append('<head>')
    html_parts.append('  <meta charset="UTF-8">')
    html_parts.append('  <meta name="viewport" content="width=device-width, initial-scale=1.0">')
    html_parts.append('  <title>Roster Export</title>')
    html_parts.append('  <style>')
    html_parts.append('    body { font-family: Arial, sans-serif; margin: 20px; }')
    html_parts.append('    .header { margin-bottom: 20px; border-bottom: 2px solid #333; padding-bottom: 10px; }')
    html_parts.append('    .header h1 { margin: 0; color: #333; }')
    html_parts.append('    .header p { margin: 5px 0; color: #666; }')
    html_parts.append('    table { width: 100%; border-collapse: collapse; margin-top: 10px; }')
    html_parts.append('    th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; font-weight: bold; }')
    html_parts.append('    td { padding: 10px; border-bottom: 1px solid #ddd; }')
    html_parts.append('    tr:hover { background-color: #f5f5f5; }')
    html_parts.append('    .weekend { background-color: #fff3cd; }')
    html_parts.append('    .group-header { background-color: #e9ecef; font-weight: bold; }')
    html_parts.append('    .subtotal { background-color: #f8f9fa; font-weight: bold; border-top: 1px solid #333; }')
    html_parts.append('    .total { background-color: #e9ecef; font-weight: bold; border-top: 2px solid #333; }')
    html_parts.append('    .numeric { text-align: right; }')
    html_parts.append('    .warning { color: #d32f2f; font-weight: bold; }')
    html_parts.append('  </style>')
    html_parts.append('</head>')
    html_parts.append('<body>')

    # Header section
    html_parts.append(f'<div class="header">')
    html_parts.append(f'  <h1>{_escape_html(export.venue_name)}</h1>')
    html_parts.append(
        f'  <p><strong>Period:</strong> {export.period_start} to {export.period_end}</p>'
    )
    html_parts.append(f'  <p><strong>Generated:</strong> {export.generated_at}</p>')
    html_parts.append(f'</div>')

    # Table
    html_parts.append('<table>')

    # Table headers
    html_parts.append('  <thead>')
    html_parts.append('    <tr>')
    html_parts.append('      <th>Date</th>')
    html_parts.append('      <th>Employee</th>')
    html_parts.append('      <th>Role</th>')
    html_parts.append('      <th>Start</th>')
    html_parts.append('      <th>End</th>')
    html_parts.append('      <th class="numeric">Hours</th>')
    if any(s.break_minutes > 0 for s in export.shifts):
        html_parts.append('      <th class="numeric">Break (min)</th>')
    if any(s.base_cost > 0 or s.penalty_cost > 0 or s.total_cost > 0 for s in export.shifts):
        html_parts.append('      <th class="numeric">Cost</th>')
    if any(s.notes for s in export.shifts):
        html_parts.append('      <th>Notes</th>')
    html_parts.append('    </tr>')
    html_parts.append('  </thead>')
    html_parts.append('  <tbody>')

    # Group shifts for display
    grouped = _group_shifts_for_html(export.shifts)
    group_subtotals = {}
    prev_group = None

    for shift in export.shifts:
        group_key = _get_group_key(shift)
        if group_key != prev_group and prev_group is not None:
            # Emit subtotal for previous group
            _emit_group_subtotal(html_parts, prev_group, group_subtotals.get(prev_group, {}))
            group_subtotals[prev_group] = {}
        if group_key not in group_subtotals:
            group_subtotals[group_key] = {
                "hours": 0.0,
                "cost": 0.0,
            }

        # Update group subtotal
        group_subtotals[group_key]["hours"] += shift.hours
        group_subtotals[group_key]["cost"] += shift.total_cost

        # Determine row class
        row_class = ""
        if _is_weekend(shift.date):
            row_class = ' class="weekend"'

        # Emit shift row
        html_parts.append(f'    <tr{row_class}>')
        html_parts.append(f'      <td>{shift.date}</td>')
        html_parts.append(f'      <td>{_escape_html(shift.employee_name)}</td>')
        html_parts.append(f'      <td>{_escape_html(shift.role)}</td>')
        html_parts.append(f'      <td>{shift.start_time}</td>')
        html_parts.append(f'      <td>{shift.end_time}</td>')
        html_parts.append(f'      <td class="numeric">{shift.hours:.2f}</td>')
        if any(s.break_minutes > 0 for s in export.shifts):
            html_parts.append(f'      <td class="numeric">{shift.break_minutes}</td>')
        if any(s.base_cost > 0 or s.penalty_cost > 0 or s.total_cost > 0 for s in export.shifts):
            cost_html = f'${shift.total_cost:.2f}'
            if shift.penalty_cost > 0:
                cost_html += f' <span class="warning">(+${shift.penalty_cost:.2f})</span>'
            html_parts.append(f'      <td class="numeric">{cost_html}</td>')
        if any(s.notes for s in export.shifts):
            html_parts.append(f'      <td>{_escape_html(shift.notes)}</td>')
        html_parts.append('    </tr>')

        prev_group = group_key

    # Final group subtotal
    if prev_group is not None:
        _emit_group_subtotal(html_parts, prev_group, group_subtotals.get(prev_group, {}))

    # Grand total
    html_parts.append('    <tr class="total">')
    html_parts.append('      <td colspan="6"><strong>TOTAL</strong></td>')
    if any(s.break_minutes > 0 for s in export.shifts):
        html_parts.append('      <td></td>')
    if any(s.base_cost > 0 or s.penalty_cost > 0 or s.total_cost > 0 for s in export.shifts):
        total_hours = export.summary.get("total_hours", 0)
        total_cost = export.summary.get("total_cost", 0)
        html_parts.append(f'      <td class="numeric"><strong>${total_cost:.2f}</strong></td>')
    if any(s.notes for s in export.shifts):
        html_parts.append('      <td></td>')
    html_parts.append('    </tr>')

    html_parts.append('  </tbody>')
    html_parts.append('</table>')

    # Summary statistics
    if export.summary:
        html_parts.append('<div style="margin-top: 20px; padding: 10px; background-color: #f5f5f5; border-left: 4px solid #4CAF50;">')
        html_parts.append('  <h3>Summary</h3>')
        html_parts.append('  <ul>')
        html_parts.append(f'    <li>Total Hours: <strong>{export.summary.get("total_hours", 0):.2f}</strong></li>')
        html_parts.append(f'    <li>Total Cost: <strong>${export.summary.get("total_cost", 0):.2f}</strong></li>')
        html_parts.append(f'    <li>Staff Count: <strong>{export.summary.get("unique_employees", 0)}</strong></li>')
        html_parts.append(f'    <li>Shifts Count: <strong>{export.summary.get("shifts_count", 0)}</strong></li>')
        html_parts.append('  </ul>')
        html_parts.append('</div>')

    html_parts.append('</body>')
    html_parts.append('</html>')

    return '\n'.join(html_parts)


def _get_group_key(shift: ExportedShift) -> str:
    """Get grouping key for a shift (date by default)."""
    return shift.date


def _group_shifts_for_html(shifts: List[ExportedShift]) -> Dict[str, List[ExportedShift]]:
    """Group shifts by date for HTML display."""
    grouped = {}
    for shift in shifts:
        key = shift.date
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(shift)
    return grouped


def _emit_group_subtotal(html_parts: List[str], group_key: str, subtotal: Dict[str, float]):
    """Emit a group subtotal row."""
    if subtotal.get("hours", 0) > 0:
        html_parts.append(f'    <tr class="subtotal">')
        html_parts.append(f'      <td colspan="6"><strong>Subtotal ({group_key})</strong></td>')
        html_parts.append(f'      <td class="numeric">{subtotal.get("hours", 0):.2f}</td>')
        if subtotal.get("cost", 0) > 0:
            html_parts.append(f'      <td class="numeric">${subtotal.get("cost", 0):.2f}</td>')
        html_parts.append('    </tr>')


def _is_weekend(date_str: str) -> bool:
    """Check if date is a weekend (Saturday or Sunday)."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return d.weekday() >= 5  # Saturday=5, Sunday=6
    except ValueError:
        return False


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


# ============================================================================
# WEEKLY GRID FORMAT
# ============================================================================

def format_weekly_grid(export: RosterExport) -> List[List[str]]:
    """
    Build a Mon-Sun grid for weekly view.

    Rows = employees, columns = days of week.
    Cells show shift times or empty.

    Args:
        export: RosterExport instance

    Returns:
        List of lists: [["Employee", "Mon", "Tue", ...], ...]
    """
    if not export.shifts:
        return [["Employee", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]]

    # Get date range for week
    first_date = datetime.strptime(export.period_start, "%Y-%m-%d").date()
    # Find Monday of that week
    monday = first_date - timedelta(days=first_date.weekday())

    # Build employee set
    employees = sorted(set(s.employee_name for s in export.shifts))

    # Build grid
    grid = [["Employee", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]]

    for emp_name in employees:
        row = [emp_name]
        emp_shifts = {
            datetime.strptime(s.date, "%Y-%m-%d").date(): s
            for s in export.shifts
            if s.employee_name == emp_name
        }

        for i in range(7):
            current_date = monday + timedelta(days=i)
            if current_date in emp_shifts:
                shift = emp_shifts[current_date]
                cell = f"{shift.start_time}-{shift.end_time}"
            else:
                cell = ""
            row.append(cell)

        grid.append(row)

    return grid


# ============================================================================
# DAILY BREAKDOWN
# ============================================================================

def format_daily_breakdown(
    export: RosterExport, target_date: str
) -> Dict[str, Any]:
    """
    Generate single-day detailed breakdown.

    Args:
        export: RosterExport instance
        target_date: Target date (YYYY-MM-DD)

    Returns:
        Dict with shifts, headcount by hour, totals for that day
    """
    day_shifts = [s for s in export.shifts if s.date == target_date]

    if not day_shifts:
        return {
            "date": target_date,
            "shifts": [],
            "headcount_by_hour": {},
            "total_hours": 0.0,
            "total_cost": 0.0,
            "unique_staff": 0,
        }

    # Parse hours and build headcount
    headcount_by_hour = {}
    for shift in day_shifts:
        try:
            start_h = int(shift.start_time.split(":")[0])
            end_h = int(shift.end_time.split(":")[0])
            if end_h <= start_h:
                end_h += 24  # Next day

            for hour in range(start_h, end_h):
                hour_key = f"{hour % 24:02d}:00"
                headcount_by_hour[hour_key] = headcount_by_hour.get(hour_key, 0) + 1
        except (ValueError, IndexError):
            pass

    total_hours = sum(s.hours for s in day_shifts)
    total_cost = sum(s.total_cost for s in day_shifts)
    unique_staff = len(set(s.employee_id for s in day_shifts))

    return {
        "date": target_date,
        "shifts": [asdict(s) for s in day_shifts],
        "headcount_by_hour": headcount_by_hour,
        "total_hours": round(total_hours, 2),
        "total_cost": round(total_cost, 2),
        "unique_staff": unique_staff,
    }
