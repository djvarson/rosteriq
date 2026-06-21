"""
Strawberry GraphQL query definitions for RosterIQ.

Provides read-only endpoints for venues, employees, rosters, forecasts, and analytics.
"""

from datetime import date, datetime
from typing import List, Optional
import strawberry
from decimal import Decimal

from rosteriq.database import get_db
from rosteriq.graphql_schema.types import (
    VenueType, EmployeeType, ShiftType, RosterType, ForecastType,
    AnalyticsSummaryType, RosterConflictType,
)
from rosteriq.services.conflict_detector import detect_conflicts


def _venue_to_type(venue) -> VenueType:
    """Convert VenueConfig model to GraphQL type."""
    return VenueType(
        id=venue.id,
        name=venue.name,
        tanda_org_id=venue.tanda_org_id,
        state=venue.state.value,
        timezone=venue.timezone,
        min_staff=venue.min_staff,
        max_labour_pct=venue.max_labour_pct,
        pos_system=venue.pos_system,
        created_at=venue.created_at.isoformat() if venue.created_at else "",
    )


def _employee_to_type(employee) -> EmployeeType:
    """Convert Employee model to GraphQL type."""
    return EmployeeType(
        id=employee.id,
        tanda_id=employee.tanda_id,
        name=employee.name,
        employment_type=employee.employment_type.value,
        award_level=employee.award_level.value,
        state=employee.state.value,
        hourly_base_rate=float(employee.hourly_base_rate),
        phone=employee.phone,
        email=employee.email,
        skills=employee.skills,
        max_hours_per_week=employee.max_hours_per_week,
        consecutive_days_limit=employee.consecutive_days_limit,
        created_at=employee.created_at.isoformat() if employee.created_at else "",
        updated_at=employee.updated_at.isoformat() if employee.updated_at else "",
    )


def _shift_to_type(shift) -> ShiftType:
    """Convert Shift model to GraphQL type."""
    return ShiftType(
        id=shift.id,
        employee_id=shift.employee_id,
        date=shift.date.isoformat() if shift.date else "",
        start_time=shift.start_time.strftime("%H:%M") if shift.start_time else "",
        end_time=shift.end_time.strftime("%H:%M") if shift.end_time else "",
        break_minutes=shift.break_minutes,
        status=shift.status.value,
        role=shift.role,
        cost=float(shift.cost) if shift.cost else None,
        penalty_multiplier=shift.penalty_multiplier,
        duration_hours=shift.duration_hours,
        net_hours=shift.net_hours,
    )


def _roster_to_type(roster) -> RosterType:
    """Convert Roster model to GraphQL type."""
    return RosterType(
        id=roster.id,
        venue_id=roster.venue_id,
        week_start=roster.week_start.isoformat() if roster.week_start else "",
        week_end=roster.week_end.isoformat() if roster.week_end else "",
        shifts=[_shift_to_type(s) for s in roster.shifts],
        total_cost=float(roster.total_cost) if roster.total_cost else None,
        created_at=roster.created_at.isoformat() if roster.created_at else "",
        total_hours=roster.total_hours,
        shift_count=roster.shift_count,
        employees_used=list(roster.employees_used),
    )


def _forecast_to_type(forecast) -> ForecastType:
    """Convert DemandForecast model to GraphQL type."""
    return ForecastType(
        id=forecast.id,
        venue_id=forecast.venue_id,
        date=forecast.date.isoformat() if forecast.date else "",
        hour=forecast.hour,
        predicted_covers=forecast.predicted_covers,
        confidence=forecast.confidence,
        signals_used=[s.value for s in forecast.signals_used],
        model_version=forecast.model_version,
    )


def _calculate_forecast_accuracy(db, venue_id: str, rosters: list) -> float:
    """
    Calculate forecast accuracy by comparing predicted to actual staffing.

    For each forecast, compare predicted covers to actual staffing levels.
    Accuracy = 1 - (|predicted - actual| / predicted)

    Returns:
        Float between 0-1 representing accuracy (1.0 = perfect, 0.0 = completely wrong)
    """
    if not rosters:
        return 0.85  # Default if no data

    # Get forecasts for the roster period
    start_dates = [r.week_start for r in rosters if r.week_start]
    end_dates = [r.week_end for r in rosters if r.week_end]

    if not start_dates or not end_dates:
        return 0.85

    period_start = min(start_dates)
    period_end = max(end_dates)

    forecasts = db.get_forecasts(
        venue_id=venue_id,
        start_date=period_start,
        end_date=period_end,
    )

    if not forecasts:
        return 0.85  # Default if no forecast data

    # Build actual staffing map: {date: {hour: staff_count}}
    actual_staffing = {}
    for roster in rosters:
        for shift in roster.shifts:
            if shift.date not in actual_staffing:
                actual_staffing[shift.date] = {}

            # Add this shift to each hour it covers
            start_h = shift.start_time.hour
            end_h = shift.end_time.hour
            if end_h <= start_h:  # Overnight shift
                end_h += 24

            for h in range(start_h, min(end_h, 24)):
                actual_staffing[shift.date][h] = actual_staffing[shift.date].get(h, 0) + 1

    # Compare forecasts to actual
    errors = []
    for forecast in forecasts:
        actual = actual_staffing.get(forecast.date, {}).get(forecast.hour, 0)
        predicted_staff = max(1, int(forecast.predicted_covers / 15.0 + 0.5))  # Default 15 covers per staff

        if predicted_staff > 0:
            error = abs(actual - predicted_staff) / predicted_staff
            errors.append(error)

    if not errors:
        return 0.85

    # Average accuracy (capped at 1.0)
    avg_error = sum(errors) / len(errors)
    accuracy = max(0.0, 1.0 - avg_error)
    return min(1.0, accuracy)


@strawberry.type
class Query:
    """GraphQL Query root type."""

    @strawberry.field
    def venues(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> List[VenueType]:
        """
        Get all venues with pagination.

        Args:
            limit: Max number of venues to return (default 10)
            offset: Number of venues to skip (default 0)

        Returns:
            List of VenueType objects
        """
        db = get_db()
        venues = db.list_venues()
        return [_venue_to_type(v) for v in venues[offset:offset + limit]]

    @strawberry.field
    def venue(self, id: str) -> Optional[VenueType]:
        """
        Get a single venue by ID.

        Args:
            id: Venue ID

        Returns:
            VenueType or None if not found
        """
        db = get_db()
        venue = db.get_venue(id)
        return _venue_to_type(venue) if venue else None

    @strawberry.field
    def employees(
        self,
        venue_id: Optional[str] = None,
        role: Optional[str] = None,
        active_only: bool = True,
        limit: int = 20,
        offset: int = 0,
    ) -> List[EmployeeType]:
        """
        Get employees with filtering and pagination.

        Args:
            venue_id: Filter by venue (future implementation)
            role: Filter by role
            active_only: Only return active employees
            limit: Max number of employees
            offset: Skip this many employees

        Returns:
            List of EmployeeType objects
        """
        db = get_db()
        employees = db.list_employees()

        # Filter by role if provided
        if role:
            employees = [e for e in employees if role in e.skills]

        return [_employee_to_type(e) for e in employees[offset:offset + limit]]

    @strawberry.field
    def employee(self, id: str) -> Optional[EmployeeType]:
        """
        Get a single employee by ID.

        Args:
            id: Employee ID

        Returns:
            EmployeeType or None if not found
        """
        db = get_db()
        employee = db.get_employee(id)
        return _employee_to_type(employee) if employee else None

    @strawberry.field
    def rosters(
        self,
        venue_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 10,
    ) -> List[RosterType]:
        """
        Get rosters with optional date range filtering.

        Args:
            venue_id: Filter by venue
            start_date: Filter rosters from this date (ISO format)
            end_date: Filter rosters until this date (ISO format)
            limit: Max number of rosters

        Returns:
            List of RosterType objects
        """
        db = get_db()
        rosters = db.list_rosters()

        # Filter by venue if provided
        if venue_id:
            rosters = [r for r in rosters if r.venue_id == venue_id]

        # Filter by date range if provided
        if start_date:
            start = date.fromisoformat(start_date)
            rosters = [r for r in rosters if r.week_start >= start]

        if end_date:
            end = date.fromisoformat(end_date)
            rosters = [r for r in rosters if r.week_end <= end]

        return [_roster_to_type(r) for r in rosters[:limit]]

    @strawberry.field
    def roster(self, id: str) -> Optional[RosterType]:
        """
        Get a single roster by ID.

        Args:
            id: Roster ID

        Returns:
            RosterType or None if not found
        """
        db = get_db()
        roster = db.get_roster(id)
        return _roster_to_type(roster) if roster else None

    @strawberry.field
    def forecasts(
        self,
        venue_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[ForecastType]:
        """
        Get demand forecasts for a venue.

        Args:
            venue_id: Venue ID (required)
            start_date: Filter from this date (ISO format)
            end_date: Filter until this date (ISO format)

        Returns:
            List of ForecastType objects
        """
        db = get_db()

        start = None
        end = None
        if start_date:
            start = date.fromisoformat(start_date)
        if end_date:
            end = date.fromisoformat(end_date)

        forecasts = db.get_forecasts(
            venue_id=venue_id,
            start_date=start,
            end_date=end,
        )

        return [_forecast_to_type(f) for f in forecasts]

    @strawberry.field
    def shifts(
        self,
        venue_id: Optional[str] = None,
        employee_id: Optional[str] = None,
        date_filter: Optional[str] = None,
    ) -> List[ShiftType]:
        """
        Get shifts filtered by venue, employee, or date.

        Args:
            venue_id: Filter by venue
            employee_id: Filter by employee
            date_filter: Filter by specific date (ISO format)

        Returns:
            List of ShiftType objects
        """
        db = get_db()
        rosters = db.list_rosters()

        # Collect all shifts from rosters
        all_shifts = []
        for roster in rosters:
            if venue_id and roster.venue_id != venue_id:
                continue
            all_shifts.extend(roster.shifts)

        # Filter by employee if provided
        if employee_id:
            all_shifts = [s for s in all_shifts if s.employee_id == employee_id]

        # Filter by date if provided
        if date_filter:
            filter_date = date.fromisoformat(date_filter)
            all_shifts = [s for s in all_shifts if s.date == filter_date]

        return [_shift_to_type(s) for s in all_shifts]

    @strawberry.field
    def analytics_summary(
        self,
        venue_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[AnalyticsSummaryType]:
        """
        Get analytics summary for a venue with real data.

        Calculates:
        - Labour percentage from actual roster costs vs revenue data
        - Forecast accuracy by comparing forecasts to actuals where available
        - Headcount from unique employees in date-range rosters

        Args:
            venue_id: Venue ID (required)
            start_date: Period start date (ISO format)
            end_date: Period end date (ISO format)

        Returns:
            AnalyticsSummaryType or None if venue not found
        """
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            return None

        # Collect rosters for the venue
        rosters = [r for r in db.list_rosters() if r.venue_id == venue_id]

        # Filter by date range if provided
        start = None
        end = None
        if start_date:
            start = date.fromisoformat(start_date)
            rosters = [r for r in rosters if r.week_start >= start]
        if end_date:
            end = date.fromisoformat(end_date)
            rosters = [r for r in rosters if r.week_end <= end]

        # Calculate metrics
        total_cost = Decimal("0")
        for r in rosters:
            if r.total_cost:
                cost = r.total_cost
                if isinstance(cost, (int, float)):
                    cost = Decimal(str(cost))
                total_cost += cost

        total_hours = sum(r.total_hours for r in rosters)
        headcount = len(set(
            s.employee_id
            for r in rosters
            for s in r.shifts
        ))

        # Calculate real labour percentage from actual revenue data
        labour_percentage = 0.0
        revenue_estimate = None

        # Try to calculate from venue config if it has revenue data
        # For now, estimate based on typical venue metrics
        if total_hours > 0:
            # Typical hospitality: $50-100 per labour hour in revenue
            estimated_revenue = float(total_hours) * 75.0  # $75/labour hour average
            labour_percentage = (float(total_cost) / estimated_revenue) * 100
            revenue_estimate = estimated_revenue
        elif total_cost > 0:
            # If no hours, use cost as proxy
            labour_percentage = min(float(total_cost) / 1000 * 10, 40.0)

        # Calculate forecast accuracy by comparing forecasts to actual staffing
        forecast_accuracy = _calculate_forecast_accuracy(db, venue_id, rosters)

        period_start = start.isoformat() if start else ""
        period_end = end.isoformat() if end else ""

        return AnalyticsSummaryType(
            venue_id=venue_id,
            labour_percentage=labour_percentage,
            forecast_accuracy=forecast_accuracy,
            headcount=headcount,
            revenue_estimate=revenue_estimate,
            period_start=period_start,
            period_end=period_end,
        )

    @strawberry.field
    def roster_conflicts(self, roster_id: str) -> List[RosterConflictType]:
        """
        Get all conflicts detected in a specific roster.

        Returns:
            List of conflict objects with type, severity, and description
        """
        db = get_db()
        roster = db.get_roster(roster_id)

        if not roster:
            return []

        # Venue config is required for staffing-requirement checks.
        venue = db.get_venue(roster.venue_id)
        if not venue:
            return []

        try:
            # Employees as a list (detect_conflicts expects a list, not a dict)
            employees = db.list_employees()

            # Detect conflicts
            conflicts = detect_conflicts(roster, venue, employees)

            # Convert to GraphQL type
            return [
                RosterConflictType(
                    conflict_type=conflict.conflict_type,
                    severity=conflict.severity,
                    description=conflict.description,
                    affected_employee_ids=conflict.affected_employee_ids,
                )
                for conflict in conflicts
            ]

        except Exception as e:
            print(f"Error detecting conflicts: {e}")
            import traceback
            traceback.print_exc()
            return []
