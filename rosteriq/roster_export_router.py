"""
Roster Export Router for RosterIQ

REST API endpoints for roster export in multiple formats:
- CSV: comma-separated values with configurable grouping
- JSON: structured data for integrations
- HTML: inline-styled tables for email/print
- Weekly grid: Mon-Sun employee roster view
- Daily breakdown: detailed hourly analysis

All endpoints require L1_SUPERVISOR+ access.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

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

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ── Router Setup ───────────────────────────────────────────────────────────
router = APIRouter(prefix="/api/v1/export", tags=["export"])


# ── Pydantic Models ────────────────────────────────────────────────────────

class ShiftExportData(BaseModel):
    """Shift data for export request."""
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


class RosterExportRequest(BaseModel):
    """Request body for POST /api/v1/export/roster"""
    venue_id: str = Field(..., description="Venue identifier")
    venue_name: str = Field(..., description="Venue name")
    period_start: str = Field(..., description="Period start (YYYY-MM-DD)")
    period_end: str = Field(..., description="Period end (YYYY-MM-DD)")
    shifts: List[ShiftExportData] = Field(..., description="List of shifts to export")
    format: str = Field("csv", description="Export format: csv, json, or html")
    view_type: str = Field("weekly", description="View type: weekly, daily, by_role, by_employee")
    include_costs: bool = Field(True, description="Include cost breakdowns")
    include_breaks: bool = Field(True, description="Include break durations")
    include_notes: bool = Field(False, description="Include shift notes")
    group_by: str = Field("date", description="Grouping: date, employee, or role")


class RosterExportResponse(BaseModel):
    """Response from POST /api/v1/export/roster"""
    venue_id: str
    venue_name: str
    period_start: str
    period_end: str
    format: str
    generated_at: str
    summary: Dict[str, Any]
    csv_content: Optional[str] = None
    json_content: Optional[str] = None
    html_content: Optional[str] = None


class WeeklyGridResponse(BaseModel):
    """Response from GET /api/v1/export/roster/{venue_id}/weekly"""
    venue_id: str
    week_start: str
    grid: List[List[str]]


class DailyBreakdownResponse(BaseModel):
    """Response from GET /api/v1/export/roster/{venue_id}/daily/{date}"""
    date: str
    venue_id: str
    shifts: List[Dict[str, Any]]
    headcount_by_hour: Dict[str, int]
    total_hours: float
    total_cost: float
    unique_staff: int


class SummaryStatsResponse(BaseModel):
    """Response from GET /api/v1/export/roster/{venue_id}/summary"""
    venue_id: str
    period_start: str
    period_end: str
    summary: Dict[str, Any]


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.post(
    "/roster",
    response_model=RosterExportResponse,
    summary="Generate roster export",
    description="Generate roster export in specified format (CSV, JSON, HTML)",
)
async def generate_roster_export(
    request: Request,
    body: RosterExportRequest,
) -> RosterExportResponse:
    """
    Generate a roster export in the specified format.

    Body:
    - venue_id: Venue identifier
    - venue_name: Human-readable venue name
    - period_start: Period start date (YYYY-MM-DD)
    - period_end: Period end date (YYYY-MM-DD)
    - shifts: List of shift records
    - format: Export format (csv, json, html)
    - view_type: View type (weekly, daily, by_role, by_employee)
    - include_costs: Include cost breakdowns
    - include_breaks: Include break durations
    - include_notes: Include shift notes
    - group_by: Grouping preference (date, employee, role)

    Returns:
    - Structured export with selected content (csv_content, json_content, or html_content)
    """
    await _gate(request, "L1_SUPERVISOR")

    # Convert request shifts to internal model
    exported_shifts = [
        ExportedShift(
            employee_id=s.employee_id,
            employee_name=s.employee_name,
            role=s.role,
            date=s.date,
            start_time=s.start_time,
            end_time=s.end_time,
            hours=s.hours,
            break_minutes=s.break_minutes,
            base_cost=s.base_cost,
            penalty_cost=s.penalty_cost,
            total_cost=s.total_cost,
            notes=s.notes,
        )
        for s in body.shifts
    ]

    # Parse format
    try:
        format_enum = ExportFormat(body.format.lower())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format '{body.format}'. Must be csv, json, or html.",
        )

    # Parse view type
    try:
        view_enum = RosterView(body.view_type.lower())
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid view_type '{body.view_type}'. Must be weekly, daily, by_role, or by_employee.",
        )

    # Build config
    config = ExportConfig(
        include_costs=body.include_costs,
        include_breaks=body.include_breaks,
        include_notes=body.include_notes,
        group_by=body.group_by,
    )

    # Build export
    export = build_roster_export(
        venue_id=body.venue_id,
        venue_name=body.venue_name,
        shifts=exported_shifts,
        period_start=body.period_start,
        period_end=body.period_end,
        view_type=view_enum,
        config=config,
    )

    # Generate format-specific content
    csv_content = None
    json_content = None
    html_content = None

    if format_enum == ExportFormat.CSV:
        csv_content = export_csv(export)
    elif format_enum == ExportFormat.JSON:
        json_content = export_json(export)
    elif format_enum == ExportFormat.HTML:
        html_content = export_html_table(export)

    return RosterExportResponse(
        venue_id=export.venue_id,
        venue_name=export.venue_name,
        period_start=export.period_start,
        period_end=export.period_end,
        format=body.format.lower(),
        generated_at=export.generated_at,
        summary=export.summary,
        csv_content=csv_content,
        json_content=json_content,
        html_content=html_content,
    )


@router.get(
    "/roster/{venue_id}/weekly",
    response_model=WeeklyGridResponse,
    summary="Weekly roster grid",
    description="Get Mon-Sun roster grid (employees x days)",
)
async def get_weekly_grid(
    request: Request,
    venue_id: str,
    week_start: Optional[str] = None,
) -> WeeklyGridResponse:
    """
    Get a weekly roster grid (employee rows, Mon-Sun columns).

    Query Params:
    - week_start: Monday of target week (YYYY-MM-DD). If omitted, uses today.

    Returns:
    - 2D grid with employee names and shift times
    """
    await _gate(request, "L1_SUPERVISOR")

    # TODO: In production, fetch actual shifts from database for venue_id and week
    # For now, return a template structure
    grid = [
        ["Employee", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    ]

    return WeeklyGridResponse(
        venue_id=venue_id,
        week_start=week_start or datetime.now().date().isoformat(),
        grid=grid,
    )


@router.get(
    "/roster/{venue_id}/daily/{date}",
    response_model=DailyBreakdownResponse,
    summary="Daily roster breakdown",
    description="Get detailed hourly breakdown for a single day",
)
async def get_daily_breakdown(
    request: Request,
    venue_id: str,
    date: str,
) -> DailyBreakdownResponse:
    """
    Get a detailed breakdown for a single day.

    Path Params:
    - date: Target date (YYYY-MM-DD)

    Returns:
    - List of shifts, headcount by hour, totals for that day
    """
    await _gate(request, "L1_SUPERVISOR")

    # Validate date format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD.",
        )

    # TODO: In production, fetch actual shifts from database
    # For now, return template structure
    return DailyBreakdownResponse(
        date=date,
        venue_id=venue_id,
        shifts=[],
        headcount_by_hour={},
        total_hours=0.0,
        total_cost=0.0,
        unique_staff=0,
    )


@router.get(
    "/roster/{venue_id}/summary",
    response_model=SummaryStatsResponse,
    summary="Period summary statistics",
    description="Get roster summary statistics for a period",
)
async def get_summary_stats(
    request: Request,
    venue_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> SummaryStatsResponse:
    """
    Get summary statistics for a roster period.

    Query Params:
    - date_from: Period start (YYYY-MM-DD)
    - date_to: Period end (YYYY-MM-DD)

    Returns:
    - Summary stats: total hours, cost, headcount, etc.
    """
    await _gate(request, "L1_SUPERVISOR")

    # Validate dates
    if date_from:
        try:
            datetime.strptime(date_from, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date_from format. Use YYYY-MM-DD.",
            )

    if date_to:
        try:
            datetime.strptime(date_to, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date_to format. Use YYYY-MM-DD.",
            )

    # TODO: In production, fetch actual shifts from database and calculate summary
    # For now, return template structure
    summary = calculate_export_summary([])

    return SummaryStatsResponse(
        venue_id=venue_id,
        period_start=date_from or "",
        period_end=date_to or "",
        summary=summary,
    )
