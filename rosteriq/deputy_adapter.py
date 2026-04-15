"""
Deputy Workforce Management Adapter for RosterIQ.

This module provides a pluggable adapter for Deputy WFM platform,
with full implementations for real Deputy API and a realistic demo adapter for development.

Deputy API documentation: https://www.deputy.com/api/v1/
"""

import asyncio
import logging
import os
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Tuple
import random

from rosteriq.deputy_integration import DeputyClient, DeputyCredentials
from rosteriq.tanda_adapter import (
    SchedulingPlatformAdapter,
    Employee,
    Availability,
    Leave,
    Shift,
    Timesheet,
    ForecastRevenue,
    EmploymentType,
    LeaveType,
    ShiftStatus,
    DepartmentCategory,
    categorise_department,
    AWARD_RATES,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Real Deputy Adapter
# ============================================================================

class DeputyAdapter(SchedulingPlatformAdapter):
    """
    Production Deputy WFM adapter using real API calls.

    Requires Deputy credentials (DEPUTY_SUBDOMAIN and DEPUTY_ACCESS_TOKEN
    or DEPUTY_PERMANENT_TOKEN).
    """

    def __init__(self, client: DeputyClient):
        """
        Initialize Deputy adapter.

        Args:
            client: Configured DeputyClient instance
        """
        self.client = client
        self._employee_cache: Dict[str, Employee] = {}
        self._department_cache: Dict[str, str] = {}  # dept_id -> name

    async def _refresh_department_cache(self, org_id: str) -> Dict[str, str]:
        """
        Populate and return the dept_id → dept_name cache for an
        organisation. Called lazily from get_employees so employee
        records can be enriched with human-readable department names
        and normalised RosterIQ categories.
        """
        try:
            dept_data = await self.client.paginate("/department")
            self._department_cache = {
                d.get("Id"): d.get("Name", "") for d in dept_data if d.get("Id")
            }
            logger.info(
                f"Refreshed Deputy department cache: {len(self._department_cache)} departments"
            )
        except Exception as e:
            logger.warning(f"Failed to refresh Deputy department cache: {e}")
        return self._department_cache

    async def get_employees(self, org_id: str) -> List[Employee]:
        """
        Retrieve all active employees from Deputy, enriched with their
        primary department name and normalised RosterIQ category.

        Args:
            org_id: Deputy organization ID (may be unused; Deputy uses subdomain)

        Returns:
            List of Employee objects
        """
        try:
            if not self._department_cache:
                await self._refresh_department_cache(org_id)

            employees_data = await self.client.paginate("/employee")

            employees = []
            for emp_data in employees_data:
                # Deputy uses different field names: FirstName, LastName, DisplayName
                first_name = emp_data.get("FirstName", "")
                last_name = emp_data.get("LastName", "")
                display_name = emp_data.get("DisplayName")
                name = display_name or f"{first_name} {last_name}".strip()

                # Department mapping
                dept_id = emp_data.get("DepartmentId")
                dept_name = (
                    self._department_cache.get(dept_id)
                    if dept_id
                    else None
                )
                category = categorise_department(
                    dept_name or emp_data.get("Role") or ""
                )

                # Employment type mapping (Deputy may use different values)
                employment_type_raw = emp_data.get("EmploymentType", "casual").lower()
                if "full" in employment_type_raw:
                    employment_type = EmploymentType.FULL_TIME.value
                elif "part" in employment_type_raw:
                    employment_type = EmploymentType.PART_TIME.value
                else:
                    employment_type = EmploymentType.CASUAL.value

                # Award level and hourly rate
                award_level = emp_data.get("AwardLevel", 1)
                try:
                    award_level = int(award_level)
                    hourly_rate = AWARD_RATES.get(award_level, AWARD_RATES[1])
                except (TypeError, ValueError):
                    hourly_rate = AWARD_RATES[1]

                # Handle missing fields gracefully
                employee = Employee(
                    id=emp_data.get("Id"),
                    name=name or "Unknown",
                    email=emp_data.get("Email", ""),
                    phone=emp_data.get("Phone"),
                    role=emp_data.get("Role", "general_staff"),
                    employment_type=employment_type,
                    hourly_rate=hourly_rate,
                    skills=emp_data.get("Skills", []),
                    active=emp_data.get("Active", True),
                    department_id=dept_id,
                    department_name=dept_name,
                    department_category=category,
                )
                employees.append(employee)
                self._employee_cache[employee.id] = employee

            logger.info(f"Retrieved {len(employees)} employees from Deputy")
            return employees

        except Exception as e:
            logger.error(f"Failed to get employees from Deputy: {e}")
            raise

    async def get_availability(
        self,
        employee_ids: List[str],
        date_range: Tuple[date, date],
    ) -> Dict[str, List[Availability]]:
        """
        Retrieve availability for employees over date range.

        Args:
            employee_ids: List of employee IDs
            date_range: (start_date, end_date) tuple

        Returns:
            Dict mapping employee_id to list of Availability objects
        """
        try:
            result: Dict[str, List[Availability]] = {}
            start_date, end_date = date_range

            for emp_id in employee_ids:
                # Deputy availability endpoint may vary
                availability_data = await self.client.get(
                    f"/employee/{emp_id}/availability",
                )

                availabilities = []
                for avail_block in availability_data.get("availability", []):
                    avail = Availability(
                        employee_id=emp_id,
                        day_of_week=avail_block.get("DayOfWeek", 0),
                        start_time=(
                            datetime.fromisoformat(avail_block.get("StartTime")).time()
                            if avail_block.get("StartTime") else None
                        ),
                        end_time=(
                            datetime.fromisoformat(avail_block.get("EndTime")).time()
                            if avail_block.get("EndTime") else None
                        ),
                        recurring=avail_block.get("Recurring", True),
                    )
                    availabilities.append(avail)

                result[emp_id] = availabilities

            logger.info(f"Retrieved availability for {len(employee_ids)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get availability from Deputy: {e}")
            raise

    async def get_leave(
        self,
        employee_ids: List[str],
        date_range: Tuple[date, date],
    ) -> Dict[str, List[Leave]]:
        """
        Retrieve approved and pending leave.

        Args:
            employee_ids: List of employee IDs
            date_range: (start_date, end_date) tuple

        Returns:
            Dict mapping employee_id to list of Leave objects
        """
        try:
            result: Dict[str, List[Leave]] = {}
            start_date, end_date = date_range

            for emp_id in employee_ids:
                leave_data = await self.client.paginate(
                    f"/employee/{emp_id}/leave",
                )

                leaves = []
                for leave_block in leave_data:
                    # Deputy uses StartDate/EndDate (capitalized)
                    try:
                        leave_start = datetime.fromisoformat(
                            leave_block.get("StartDate")
                        ).date()
                        leave_end = datetime.fromisoformat(
                            leave_block.get("EndDate")
                        ).date()
                    except (TypeError, ValueError):
                        continue

                    leave = Leave(
                        id=leave_block.get("Id"),
                        employee_id=emp_id,
                        start_date=leave_start,
                        end_date=leave_end,
                        leave_type=leave_block.get("LeaveType", LeaveType.ANNUAL.value).lower(),
                        status=leave_block.get("Status", "approved").lower(),
                    )
                    leaves.append(leave)

                result[emp_id] = leaves

            logger.info(f"Retrieved leave for {len(employee_ids)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get leave from Deputy: {e}")
            raise

    async def get_shifts(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Shift]:
        """
        Retrieve published shifts (called "roster" in Deputy).

        Args:
            org_id: Organization ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Shift objects
        """
        try:
            start_date, end_date = date_range

            shifts_data = await self.client.paginate(
                "/roster",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            )

            shifts = []
            for shift_data in shifts_data:
                try:
                    shift_date = datetime.fromisoformat(shift_data.get("Date")).date()
                    start_time = datetime.fromisoformat(
                        shift_data.get("StartTime")
                    ).time()
                    end_time = datetime.fromisoformat(
                        shift_data.get("EndTime")
                    ).time()
                except (TypeError, ValueError):
                    continue

                shift = Shift(
                    id=shift_data.get("Id"),
                    employee_id=shift_data.get("EmployeeId"),
                    date=shift_date,
                    start_time=start_time,
                    end_time=end_time,
                    role=shift_data.get("Role", "general_staff"),
                    status=shift_data.get("Status", ShiftStatus.PUBLISHED.value).lower(),
                    break_minutes=shift_data.get("BreakMinutes", 0),
                )
                shifts.append(shift)

            logger.info(f"Retrieved {len(shifts)} published shifts")
            return shifts

        except Exception as e:
            logger.error(f"Failed to get shifts from Deputy: {e}")
            raise

    async def get_timesheets(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Timesheet]:
        """
        Retrieve actual hours worked.

        Args:
            org_id: Organization ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Timesheet objects
        """
        try:
            start_date, end_date = date_range

            timesheets_data = await self.client.paginate(
                "/timesheet",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            )

            timesheets = []
            for ts_data in timesheets_data:
                try:
                    ts_date = datetime.fromisoformat(ts_data.get("Date")).date()
                except (TypeError, ValueError):
                    continue

                timesheet = Timesheet(
                    id=ts_data.get("Id"),
                    employee_id=ts_data.get("EmployeeId"),
                    date=ts_date,
                    hours=ts_data.get("TotalHours", 0.0),
                    shifts=ts_data.get("Shifts", []),
                )
                timesheets.append(timesheet)

            logger.info(f"Retrieved {len(timesheets)} timesheets")
            return timesheets

        except Exception as e:
            logger.error(f"Failed to get timesheets from Deputy: {e}")
            raise

    async def push_draft_roster(
        self,
        org_id: str,
        shifts: List[Shift],
    ) -> Dict[str, Any]:
        """
        Push AI-generated shifts as draft roster to Deputy.

        Args:
            org_id: Organization ID
            shifts: List of Shift objects to create

        Returns:
            Dict with results: {"created": [...], "errors": [...]}
        """
        try:
            results = {"created": [], "errors": []}

            for shift in shifts:
                try:
                    shift_data = {
                        "EmployeeId": shift.employee_id,
                        "Date": shift.date.isoformat(),
                        "StartTime": shift.start_time.isoformat(),
                        "EndTime": shift.end_time.isoformat(),
                        "BreakMinutes": shift.break_minutes,
                        "Status": ShiftStatus.DRAFT.value,
                    }

                    created = await self.client.post(
                        "/roster",
                        json=shift_data,
                    )
                    results["created"].append(created)

                except Exception as e:
                    logger.error(f"Failed to create shift for {shift.employee_id}: {e}")
                    results["errors"].append({
                        "shift": shift,
                        "error": str(e),
                    })

            logger.info(
                f"Pushed roster: {len(results['created'])} created, "
                f"{len(results['errors'])} errors"
            )
            return results

        except Exception as e:
            logger.error(f"Failed to push roster: {e}")
            raise

    async def get_forecast_revenue(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[ForecastRevenue]:
        """
        Deputy does not provide a revenue forecast API endpoint.

        Returns:
            Empty list (Deputy forecasting not available)
        """
        logger.info("Deputy does not provide revenue forecast API")
        return []

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process incoming Deputy webhook events.

        Args:
            event_type: Event type string (e.g., "employee.created")
            payload: Event payload

        Returns:
            Dict with processing result
        """
        try:
            logger.info(f"Processing webhook: {event_type}")
            return {"status": "processed", "event": event_type}

        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
            raise


# ============================================================================
# Demo Deputy Adapter
# ============================================================================

class DemoDeputyAdapter(SchedulingPlatformAdapter):
    """
    Demo adapter for Deputy-style development and testing.

    Returns realistic data for a wine bar venue with:
    - 10 employees across 4 departments
    - Mix of employment types (60% casual, 25% part-time, 15% full-time)
    - Australian names and skills
    - Realistic leave and availability patterns
    """

    def __init__(self):
        """Initialize demo adapter with realistic data."""
        self._employees = self._generate_employees()
        self._shifts = self._generate_shifts()
        self._leave = self._generate_leave()

    def _generate_employees(self) -> List[Employee]:
        """Generate realistic demo employees for Deputy."""
        names = [
            "Alex Rivera", "Bailey Cooper", "Casey Morgan", "Dakota Ellis",
            "Eden Knight", "Finley Palmer", "Greer Quinn", "Harper Reed",
            "India Russell", "Jordan Scott",
        ]

        # Department structure for wine bar: Bar (4), Floor (3), Management (2), Host (1)
        departments = {
            "Bar": ["wine", "cocktails", "service"],
            "Floor": ["floor", "wine_service", "table_service"],
            "Management": ["management", "supervision"],
            "Host": ["host", "greeting"],
        }

        employment_dist = [EmploymentType.CASUAL.value] * 6 + \
                         [EmploymentType.PART_TIME.value] * 2 + \
                         [EmploymentType.FULL_TIME.value] * 2
        random.shuffle(employment_dist)

        employees = []
        emp_id = 1

        for i, name in enumerate(names):
            dept_name = list(departments.keys())[
                min(i // 3, len(departments) - 1)
            ]

            # Award level based on employment type
            if employment_dist[i] == EmploymentType.FULL_TIME.value:
                level = random.choice([4, 5, 6])
            elif employment_dist[i] == EmploymentType.PART_TIME.value:
                level = random.choice([2, 3, 4])
            else:
                level = random.choice([1, 2, 3])

            employee = Employee(
                id=f"deputy_{emp_id}",
                name=name,
                email=f"{name.lower().replace(' ', '.')}@rosteriq.com",
                phone=f"04{random.randint(10000000, 99999999)}",
                role=dept_name.lower(),
                employment_type=employment_dist[i],
                hourly_rate=AWARD_RATES[level],
                skills=random.sample(
                    departments[dept_name],
                    k=min(len(departments[dept_name]), random.randint(1, 3))
                ),
                active=True,
                department_id=f"dept_{dept_name.lower()}",
                department_name=dept_name,
                department_category=categorise_department(dept_name),
            )
            employees.append(employee)
            emp_id += 1

        return employees

    def _generate_shifts(self) -> List[Shift]:
        """Generate realistic demo shifts."""
        shifts = []
        shift_id = 1

        # Generate shifts for next 14 days
        today = date.today()
        for days_ahead in range(14):
            shift_date = today + timedelta(days=days_ahead)

            # Skip some days, generate 3-5 shifts per day
            if random.random() > 0.85:
                continue

            num_shifts = random.randint(3, 5)
            for _ in range(num_shifts):
                employee = random.choice(self._employees)

                # Morning, afternoon, or evening shift
                shift_type = random.choice(["morning", "afternoon", "evening"])
                if shift_type == "morning":
                    start_hour = random.randint(7, 9)
                    duration = random.randint(5, 7)
                elif shift_type == "afternoon":
                    start_hour = random.randint(11, 13)
                    duration = random.randint(5, 7)
                else:
                    start_hour = random.randint(16, 18)
                    duration = random.randint(5, 7)

                # Ensure end time is valid and after start time
                # Cap duration to prevent wrapping past midnight
                max_duration = 23 - start_hour
                actual_duration = min(duration, max_duration)
                if actual_duration <= 0:
                    actual_duration = 5  # Default duration if near end of day

                end_hour = start_hour + actual_duration

                shift = Shift(
                    id=f"shift_{shift_id}",
                    employee_id=employee.id,
                    date=shift_date,
                    start_time=time(start_hour, 0),
                    end_time=time(end_hour, 0),
                    role=employee.role,
                    status=ShiftStatus.PUBLISHED.value if days_ahead > 7 else ShiftStatus.DRAFT.value,
                    break_minutes=30 if actual_duration >= 6 else 15,
                )
                shifts.append(shift)
                shift_id += 1

        return shifts

    def _generate_leave(self) -> Dict[str, List[Leave]]:
        """Generate realistic demo leave."""
        leave_by_emp: Dict[str, List[Leave]] = {}

        # 1-2 employees on leave this week
        leave_employees = random.sample(self._employees, k=random.randint(1, 2))

        today = date.today()
        leave_id = 1

        for emp in leave_employees:
            leave_start = today + timedelta(days=random.randint(0, 3))
            leave_duration = random.randint(1, 4)
            leave_end = leave_start + timedelta(days=leave_duration)

            leave_type = random.choice([
                LeaveType.ANNUAL.value,
                LeaveType.SICK.value,
            ])

            leave = Leave(
                id=f"leave_{leave_id}",
                employee_id=emp.id,
                start_date=leave_start,
                end_date=leave_end,
                leave_type=leave_type,
                status="approved" if leave_type == LeaveType.ANNUAL.value else "pending",
            )

            if emp.id not in leave_by_emp:
                leave_by_emp[emp.id] = []

            leave_by_emp[emp.id].append(leave)
            leave_id += 1

        return leave_by_emp

    async def get_employees(self, org_id: str) -> List[Employee]:
        """Return all demo employees."""
        logger.info(f"[DEMO] Retrieved {len(self._employees)} employees")
        return self._employees

    async def get_availability(
        self,
        employee_ids: List[str],
        date_range: Tuple[date, date],
    ) -> Dict[str, List[Availability]]:
        """Return realistic availability patterns."""
        result: Dict[str, List[Availability]] = {}

        for emp_id in employee_ids:
            employee = next(
                (e for e in self._employees if e.id == emp_id),
                None,
            )
            if not employee:
                continue

            availabilities = []

            # Full-time: available most days
            if employee.employment_type == EmploymentType.FULL_TIME.value:
                for day_of_week in range(6):  # Mon-Sat
                    availabilities.append(Availability(
                        employee_id=emp_id,
                        day_of_week=day_of_week,
                        start_time=time(7, 0),
                        end_time=time(23, 0),
                        recurring=True,
                    ))

            # Part-time: available 3-4 days
            elif employee.employment_type == EmploymentType.PART_TIME.value:
                available_days = random.sample(range(7), k=random.randint(3, 4))
                for day_of_week in available_days:
                    start = time(random.randint(11, 13), 0)
                    end = time(random.randint(18, 23), 0)
                    availabilities.append(Availability(
                        employee_id=emp_id,
                        day_of_week=day_of_week,
                        start_time=start,
                        end_time=end,
                        recurring=True,
                    ))

            # Casual: varied availability
            else:
                available_days = random.sample(range(7), k=random.randint(2, 6))
                for day_of_week in available_days:
                    start = time(random.randint(7, 13), 0)
                    end = time(random.randint(17, 23), 0)
                    availabilities.append(Availability(
                        employee_id=emp_id,
                        day_of_week=day_of_week,
                        start_time=start,
                        end_time=end,
                        recurring=True,
                    ))

            result[emp_id] = availabilities

        logger.info(f"[DEMO] Retrieved availability for {len(result)} employees")
        return result

    async def get_leave(
        self,
        employee_ids: List[str],
        date_range: Tuple[date, date],
    ) -> Dict[str, List[Leave]]:
        """Return demo leave records."""
        result: Dict[str, List[Leave]] = {}

        for emp_id in employee_ids:
            if emp_id in self._leave:
                result[emp_id] = self._leave[emp_id]
            else:
                result[emp_id] = []

        logger.info(f"[DEMO] Retrieved leave for {len(result)} employees")
        return result

    async def get_shifts(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Shift]:
        """Return demo shifts."""
        start_date, end_date = date_range

        filtered_shifts = [
            s for s in self._shifts
            if start_date <= s.date <= end_date
        ]

        logger.info(f"[DEMO] Retrieved {len(filtered_shifts)} shifts")
        return filtered_shifts

    async def get_timesheets(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Timesheet]:
        """Return demo timesheets."""
        timesheets = []
        start_date, end_date = date_range

        # Generate timesheets based on shifts
        for shift in self._shifts:
            if not (start_date <= shift.date <= end_date):
                continue

            hours = (
                (shift.end_time.hour - shift.start_time.hour) -
                (shift.break_minutes / 60)
            )

            timesheet = Timesheet(
                id=f"ts_{shift.id}",
                employee_id=shift.employee_id,
                date=shift.date,
                hours=max(0, hours),
                shifts=[shift.__dict__],
            )
            timesheets.append(timesheet)

        logger.info(f"[DEMO] Retrieved {len(timesheets)} timesheets")
        return timesheets

    async def push_draft_roster(
        self,
        org_id: str,
        shifts: List[Shift],
    ) -> Dict[str, Any]:
        """Simulate pushing draft roster."""
        logger.info(f"[DEMO] Simulating push of {len(shifts)} shifts")
        return {
            "created": [{"id": f"created_{i}", "shift": s} for i, s in enumerate(shifts)],
            "errors": [],
        }

    async def get_forecast_revenue(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[ForecastRevenue]:
        """
        Deputy does not provide revenue forecast API.

        Returns:
            Empty list
        """
        logger.info("[DEMO] Deputy does not provide revenue forecast")
        return []

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Simulate webhook handling."""
        logger.info(f"[DEMO] Processing webhook: {event_type}")
        return {"status": "processed", "event": event_type}
