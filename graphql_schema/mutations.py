"""
Strawberry GraphQL mutation definitions for RosterIQ.

Provides write operations for roster generation, shift management, and availability.
"""

from datetime import date, time as dt_time, datetime, timedelta
from typing import List, Optional, AsyncGenerator
import strawberry
from decimal import Decimal
import asyncio

from rosteriq.database import get_db
from rosteriq.models import (
    Shift, Roster, DemandForecast, Employee,
    ShiftStatus, EmploymentType, AwardLevel, State,
)
from rosteriq.graphql_schema.types import (
    RosterType, ShiftType, ForecastType, SwapResultType, AvailabilityType,
)
from rosteriq.roster_optimiser import generate_weekly_roster


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


@strawberry.type
class Mutation:
    """GraphQL Mutation root type."""

    @strawberry.mutation
    def generate_roster(
        self,
        venue_id: str,
        date_str: str,
        strategy: str = "balanced",
    ) -> Optional[RosterType]:
        """
        Generate a new roster for a venue using the roster optimiser.

        Args:
            venue_id: Venue ID
            date_str: Start date for roster (ISO format, should be a Monday)
            strategy: Rostering strategy ("balanced", "cost_optimized", "coverage_first")

        Returns:
            Generated RosterType or None if failed
        """
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            return None

        try:
            # Parse date
            week_start = date.fromisoformat(date_str)

            # Ensure we're starting on a Monday by going back to the previous Monday if needed
            days_since_monday = week_start.weekday()
            if days_since_monday > 0:
                week_start = week_start - timedelta(days=days_since_monday)

            week_end = week_start + timedelta(days=6)

            # Fetch demand forecasts for the week
            forecasts = db.get_forecasts(
                venue_id=venue_id,
                start_date=week_start,
                end_date=week_end,
            )

            if not forecasts:
                # No forecasts available
                return None

            # Fetch employees from DB
            employees = db.list_employees()

            if not employees:
                # No employees available
                return None

            # Generate roster using the optimiser
            covers_per_staff = 15.0  # Default ratio

            # Map strategy to covers_per_staff (lower ratio = more conservative staffing)
            if strategy == "cost_optimized":
                covers_per_staff = 18.0  # More aggressive staffing
            elif strategy == "coverage_first":
                covers_per_staff = 12.0  # More conservative staffing
            # "balanced" uses default 15.0

            roster = generate_weekly_roster(
                week_start=week_start,
                weekly_forecasts=forecasts,
                employees=employees,
                venue_config=venue,
                covers_per_staff=covers_per_staff,
            )

            # Save the roster to DB
            db.save_roster(roster)
            return _roster_to_type(roster)

        except Exception as e:
            print(f"Error generating roster: {e}")
            import traceback
            traceback.print_exc()
            return None

    @strawberry.mutation
    def update_shift(
        self,
        shift_id: str,
        start_time: str,
        end_time: str,
        role: str,
    ) -> Optional[ShiftType]:
        """
        Update an existing shift.

        Args:
            shift_id: Shift ID to update
            start_time: New start time (HH:MM format)
            end_time: New end time (HH:MM format)
            role: New role

        Returns:
            Updated ShiftType or None if not found
        """
        db = get_db()
        rosters = db.list_rosters()

        # Find the shift in rosters
        target_shift = None
        target_roster = None
        for roster in rosters:
            for shift in roster.shifts:
                if shift.id == shift_id:
                    target_shift = shift
                    target_roster = roster
                    break

        if not target_shift or not target_roster:
            return None

        try:
            # Parse times
            start = dt_time.fromisoformat(start_time)
            end = dt_time.fromisoformat(end_time)

            # Update shift
            target_shift.start_time = start
            target_shift.end_time = end
            target_shift.role = role

            # Save updated roster
            db.save_roster(target_roster)

            return _shift_to_type(target_shift)

        except Exception as e:
            print(f"Error updating shift: {e}")
            return None

    @strawberry.mutation
    def swap_shift(
        self,
        shift_id: str,
        target_employee_id: str,
    ) -> SwapResultType:
        """
        Swap a shift to a different employee.

        Args:
            shift_id: Shift ID to swap
            target_employee_id: Employee ID to assign shift to

        Returns:
            SwapResultType with operation result
        """
        db = get_db()
        rosters = db.list_rosters()

        # Find the shift
        target_shift = None
        target_roster = None
        for roster in rosters:
            for shift in roster.shifts:
                if shift.id == shift_id:
                    target_shift = shift
                    target_roster = roster
                    break

        if not target_shift or not target_roster:
            return SwapResultType(
                success=False,
                shift_id=shift_id,
                old_employee_id="",
                new_employee_id=target_employee_id,
                message="Shift not found",
            )

        try:
            old_employee_id = target_shift.employee_id
            target_shift.employee_id = target_employee_id
            db.save_roster(target_roster)

            return SwapResultType(
                success=True,
                shift_id=shift_id,
                old_employee_id=old_employee_id,
                new_employee_id=target_employee_id,
                message=f"Shift swapped from {old_employee_id} to {target_employee_id}",
            )

        except Exception as e:
            return SwapResultType(
                success=False,
                shift_id=shift_id,
                old_employee_id=target_shift.employee_id,
                new_employee_id=target_employee_id,
                message=f"Swap failed: {str(e)}",
            )

    @strawberry.mutation
    def update_availability(
        self,
        employee_id: str,
        day: str,
        blocks: List[str],
    ) -> AvailabilityType:
        """
        Update employee availability for a day.

        Args:
            employee_id: Employee ID
            day: Day of week (e.g. "monday")
            blocks: List of available time blocks (e.g. ["09:00-17:00"])

        Returns:
            Updated AvailabilityType
        """
        db = get_db()
        employee = db.get_employee(employee_id)

        if not employee:
            return AvailabilityType(
                employee_id=employee_id,
                day=day,
                blocks=blocks,
                updated_at="",
            )

        try:
            # Update availability
            if not employee.availability:
                employee.availability = {}

            employee.availability[day.lower()] = [
                {
                    "start": block.split("-")[0],
                    "end": block.split("-")[1]
                }
                for block in blocks
            ]

            db.save_employee(employee)

            from datetime import datetime
            return AvailabilityType(
                employee_id=employee_id,
                day=day,
                blocks=blocks,
                updated_at=datetime.now().isoformat(),
            )

        except Exception as e:
            print(f"Error updating availability: {e}")
            return AvailabilityType(
                employee_id=employee_id,
                day=day,
                blocks=blocks,
                updated_at="",
            )

    @strawberry.mutation
    def add_forecast(
        self,
        venue_id: str,
        date_str: str,
        hour: int,
        demand: float,
    ) -> Optional[ForecastType]:
        """
        Add a demand forecast for a venue.

        Args:
            venue_id: Venue ID
            date_str: Forecast date (ISO format)
            hour: Hour of day (0-23)
            demand: Predicted demand/covers

        Returns:
            Created ForecastType or None if failed
        """
        db = get_db()
        venue = db.get_venue(venue_id)

        if not venue:
            return None

        try:
            forecast_date = date.fromisoformat(date_str)

            forecast = DemandForecast(
                id=f"forecast_{venue_id}_{forecast_date.isoformat()}_{hour}",
                venue_id=venue_id,
                date=forecast_date,
                hour=hour,
                predicted_covers=demand,
                confidence=0.85,
                signals_used=[],
                model_version="1.0",
            )

            db.add_forecasts([forecast])

            return _forecast_to_type(forecast)

        except Exception as e:
            print(f"Error adding forecast: {e}")
            return None
