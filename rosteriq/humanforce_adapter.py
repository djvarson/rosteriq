"""
HumanForce Workforce Management Adapter for RosterIQ.

This module provides a pluggable adapter for HumanForce WFM platform,
with full implementations for real HumanForce API and a realistic demo adapter for development.

HumanForce API documentation: https://api.humanforce.com/
"""

import asyncio
import logging
import os
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Tuple
import random

from rosteriq.humanforce_integration import HumanForceClient, HumanForceCredentials
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
# Real HumanForce Adapter
# ============================================================================

class HumanForceAdapter(SchedulingPlatformAdapter):
    """
    Production HumanForce WFM adapter using real API calls.

    Requires HumanForce credentials (HUMANFORCE_API_KEY or
    HUMANFORCE_CLIENT_ID + HUMANFORCE_CLIENT_SECRET).
    """

    def __init__(self, client: HumanForceClient):
        """
        Initialize HumanForce adapter.

        Args:
            client: Configured HumanForceClient instance
        """
        self.client = client
        self._employee_cache: Dict[str, Employee] = {}
        self._location_cache: Dict[str, str] = {}  # location_id -> name

    async def _refresh_location_cache(self, org_id: str) -> Dict[str, str]:
        """
        Populate and return the location_id → location_name cache for an
        organisation. Called lazily from get_employees so employee
        records can be enriched with human-readable location names.
        """
        try:
            location_items = []
            async for location in self.client.paginate("/locations"):
                location_items.append(location)

            self._location_cache = {
                loc.get("id"): loc.get("name", "") for loc in location_items if loc.get("id")
            }
            logger.info(
                f"Refreshed HumanForce location cache: {len(self._location_cache)} locations"
            )
        except Exception as e:
            logger.warning(f"Failed to refresh HumanForce location cache: {e}")
        return self._location_cache

    async def get_employees(self, org_id: str) -> List[Employee]:
        """
        Retrieve all active employees from HumanForce, enriched with their
        location name and normalised RosterIQ category.

        Args:
            org_id: HumanForce organization ID

        Returns:
            List of Employee objects
        """
        try:
            if not self._location_cache:
                await self._refresh_location_cache(org_id)

            employees = []
            async for emp_data in self.client.paginate("/employees"):
                # HumanForce uses camelCase: firstName, lastName
                first_name = emp_data.get("firstName", "")
                last_name = emp_data.get("lastName", "")
                name = f"{first_name} {last_name}".strip()

                # Location mapping
                location_id = emp_data.get("locationId")
                location_name = (
                    self._location_cache.get(location_id)
                    if location_id
                    else None
                )
                category = categorise_department(
                    location_name or emp_data.get("position") or ""
                )

                # Employment type mapping
                employment_type_raw = emp_data.get("employmentType", "casual").lower()
                if "full" in employment_type_raw:
                    employment_type = EmploymentType.FULL_TIME.value
                elif "part" in employment_type_raw:
                    employment_type = EmploymentType.PART_TIME.value
                else:
                    employment_type = EmploymentType.CASUAL.value

                # Award level and hourly rate
                award_level = emp_data.get("awardLevel", 1)
                try:
                    award_level = int(award_level)
                    hourly_rate = AWARD_RATES.get(award_level, AWARD_RATES[1])
                except (TypeError, ValueError):
                    hourly_rate = AWARD_RATES[1]

                employee = Employee(
                    id=emp_data.get("id"),
                    name=name or "Unknown",
                    email=emp_data.get("email", ""),
                    phone=emp_data.get("phone"),
                    role=emp_data.get("position", "general_staff"),
                    employment_type=employment_type,
                    hourly_rate=hourly_rate,
                    skills=emp_data.get("skills", []),
                    active=emp_data.get("active", True),
                    department_id=location_id,
                    department_name=location_name,
                    department_category=category,
                )
                employees.append(employee)
                self._employee_cache[employee.id] = employee

            logger.info(f"Retrieved {len(employees)} employees from HumanForce")
            return employees

        except Exception as e:
            logger.error(f"Failed to get employees from HumanForce: {e}")
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
                try:
                    availability_data = await self.client.get(
                        f"/employees/{emp_id}/availability",
                    )

                    availabilities = []
                    for avail_block in availability_data.get("availability", []):
                        avail = Availability(
                            employee_id=emp_id,
                            day_of_week=avail_block.get("dayOfWeek", 0),
                            start_time=(
                                datetime.fromisoformat(avail_block.get("startTime")).time()
                                if avail_block.get("startTime") else None
                            ),
                            end_time=(
                                datetime.fromisoformat(avail_block.get("endTime")).time()
                                if avail_block.get("endTime") else None
                            ),
                            recurring=avail_block.get("recurring", True),
                        )
                        availabilities.append(avail)

                    result[emp_id] = availabilities
                except Exception as e:
                    logger.warning(f"Failed to get availability for {emp_id}: {e}")
                    result[emp_id] = []

            logger.info(f"Retrieved availability for {len(result)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get availability from HumanForce: {e}")
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
                try:
                    leave_items = []
                    async for leave_block in self.client.paginate(
                        f"/employees/{emp_id}/leave-requests",
                    ):
                        # HumanForce uses camelCase: startDateTime, endDateTime
                        try:
                            leave_start = datetime.fromisoformat(
                                leave_block.get("startDateTime")
                            ).date()
                            leave_end = datetime.fromisoformat(
                                leave_block.get("endDateTime")
                            ).date()
                        except (TypeError, ValueError):
                            continue

                        leave = Leave(
                            id=leave_block.get("id"),
                            employee_id=emp_id,
                            start_date=leave_start,
                            end_date=leave_end,
                            leave_type=leave_block.get("leaveType", LeaveType.ANNUAL.value).lower(),
                            status=leave_block.get("status", "approved").lower(),
                        )
                        leave_items.append(leave)

                    result[emp_id] = leave_items
                except Exception as e:
                    logger.warning(f"Failed to get leave for {emp_id}: {e}")
                    result[emp_id] = []

            logger.info(f"Retrieved leave for {len(result)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get leave from HumanForce: {e}")
            raise

    async def get_shifts(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Shift]:
        """
        Retrieve published shifts (called "schedules" in HumanForce).

        Args:
            org_id: Organization ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Shift objects
        """
        try:
            start_date, end_date = date_range

            shifts = []
            async for shift_data in self.client.paginate(
                "/schedules",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            ):
                try:
                    # HumanForce uses camelCase: startDateTime, endDateTime
                    shift_date = datetime.fromisoformat(shift_data.get("date")).date()
                    start_time = datetime.fromisoformat(
                        shift_data.get("startDateTime")
                    ).time()
                    end_time = datetime.fromisoformat(
                        shift_data.get("endDateTime")
                    ).time()
                except (TypeError, ValueError):
                    continue

                shift = Shift(
                    id=shift_data.get("id"),
                    employee_id=shift_data.get("employeeId"),
                    date=shift_date,
                    start_time=start_time,
                    end_time=end_time,
                    role=shift_data.get("position", "general_staff"),
                    status=shift_data.get("status", ShiftStatus.PUBLISHED.value).lower(),
                    break_minutes=shift_data.get("breakMinutes", 0),
                )
                shifts.append(shift)

            logger.info(f"Retrieved {len(shifts)} published shifts")
            return shifts

        except Exception as e:
            logger.error(f"Failed to get shifts from HumanForce: {e}")
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

            timesheets = []
            async for ts_data in self.client.paginate(
                "/timesheets",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            ):
                try:
                    ts_date = datetime.fromisoformat(ts_data.get("date")).date()
                except (TypeError, ValueError):
                    continue

                timesheet = Timesheet(
                    id=ts_data.get("id"),
                    employee_id=ts_data.get("employeeId"),
                    date=ts_date,
                    hours=ts_data.get("totalHours", 0.0),
                    shifts=ts_data.get("shifts", []),
                )
                timesheets.append(timesheet)

            logger.info(f"Retrieved {len(timesheets)} timesheets")
            return timesheets

        except Exception as e:
            logger.error(f"Failed to get timesheets from HumanForce: {e}")
            raise

    async def push_draft_roster(
        self,
        org_id: str,
        shifts: List[Shift],
    ) -> Dict[str, Any]:
        """
        Push AI-generated shifts as draft roster to HumanForce.

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
                        "employeeId": shift.employee_id,
                        "date": shift.date.isoformat(),
                        "startDateTime": datetime.combine(shift.date, shift.start_time).isoformat(),
                        "endDateTime": datetime.combine(shift.date, shift.end_time).isoformat(),
                        "breakMinutes": shift.break_minutes,
                        "status": ShiftStatus.DRAFT.value,
                    }

                    created = await self.client.post(
                        "/schedules",
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
        HumanForce does not provide a revenue forecast API endpoint.

        Returns:
            Empty list (HumanForce forecasting not available)
        """
        logger.info("HumanForce does not provide revenue forecast API")
        return []

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process incoming HumanForce webhook events.

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
# Demo HumanForce Adapter
# ============================================================================

class DemoHumanForceAdapter(SchedulingPlatformAdapter):
    """
    Demo adapter for HumanForce-style development and testing.

    Returns realistic data for a larger pub group venue with:
    - 45 employees across 3 locations (Sydney, Melbourne, Brisbane)
    - Mix of employment types (60% full-time/part-time, 40% casual)
    - Mix of roles (management, chefs, bar, floor, bottle shop)
    - Australian names and realistic hierarchy
    - Realistic leave and availability patterns
    """

    def __init__(self):
        """Initialize demo adapter with realistic data."""
        self._employees = self._generate_employees()
        self._shifts = self._generate_shifts()
        self._leave = self._generate_leave()

    def _generate_employees(self) -> List[Employee]:
        """Generate realistic demo employees for HumanForce."""
        # Sovereign Hotel Group: 3 locations (Sydney, Melbourne, Brisbane)
        locations = {
            "SYD_001": "Sovereign Sydney",
            "MEL_001": "Sovereign Melbourne",
            "BNE_001": "Sovereign Brisbane",
        }

        # Management roles (1 per location)
        management_roles = ["General Manager", "Assistant Manager", "Duty Manager"]

        # Staff roles by department
        bar_roles = ["Bar Manager", "Lead Bartender", "Bartender", "Bar Back"]
        kitchen_roles = ["Head Chef", "Sous Chef", "Line Chef", "Commis Chef", "Kitchen Hand"]
        floor_roles = ["Floor Lead", "Server", "Floor Host"]
        other_roles = ["Bottle Shop Attendant", "Cleaner", "Security"]

        all_roles = management_roles + bar_roles + kitchen_roles + floor_roles + other_roles
        all_names = [
            # Management
            "James Stewart", "Linda Chen", "Michael O'Brien",
            # Bar staff
            "Sarah Mitchell", "David Thompson", "Emma Wilson", "Joshua Brown",
            "Lucas Anderson", "Olivia Taylor", "Noah Garcia", "Sophia Martinez",
            "Ethan Rodriguez", "Ava Lopez", "Mason Hernandez", "Isabella Davis",
            # Kitchen staff
            "Oliver Johnson", "Charlotte Smith", "Benjamin Lee", "Amelia Harris",
            "Elijah Martin", "Mia Jackson", "Logan White", "Harper Allen",
            "Alexander Young", "Evelyn King", "Jacob Scott", "Abigail Green",
            "Michael Adams", "Elizabeth Nelson",
            # Floor staff
            "Daniel Carter", "Sofia Mitchell", "Matthew Roberts", "Emily Phillips",
            "John Campbell", "Lily Parker", "Andrew Evans", "Grace Edwards",
            "Ryan Collins", "Victoria Reeves",
            # Bottle Shop
            "Andrew Murphy", "Jessica Walsh", "Ryan Bennett",
            # Additional staff
            "Christopher Lee", "Laura Bennett", "Steven Wright",
        ]

        # Shuffle employment distribution
        employment_dist = (
            [EmploymentType.FULL_TIME.value] * 15 +
            [EmploymentType.PART_TIME.value] * 12 +
            [EmploymentType.CASUAL.value] * 18
        )
        random.shuffle(employment_dist)

        employees = []
        emp_id = 1
        location_rotation = 0

        for i, name in enumerate(all_names):
            # Distribute across locations
            location_id = list(locations.keys())[i % 3]
            location_name = locations[location_id]

            # Assign role (management roles to first 3, then rotate)
            if i < 3:
                role = management_roles[i]
                dept_category = DepartmentCategory.MANAGEMENT.value
            else:
                role = all_roles[i % len(all_roles)]
                if any(x in role.lower() for x in ["chef", "kitchen", "commis"]):
                    dept_category = DepartmentCategory.KITCHEN.value
                elif any(x in role.lower() for x in ["bar", "bartender"]):
                    dept_category = DepartmentCategory.BAR.value
                elif any(x in role.lower() for x in ["server", "floor", "host"]):
                    dept_category = DepartmentCategory.FLOOR.value
                elif "manager" in role.lower():
                    dept_category = DepartmentCategory.MANAGEMENT.value
                else:
                    dept_category = DepartmentCategory.OTHER.value

            # Award level based on employment type
            if employment_dist[i] == EmploymentType.FULL_TIME.value:
                level = random.choice([4, 5, 6])
            elif employment_dist[i] == EmploymentType.PART_TIME.value:
                level = random.choice([2, 3, 4])
            else:
                level = random.choice([1, 2, 3])

            employee = Employee(
                id=f"humanforce_{emp_id}",
                name=name,
                email=f"{name.lower().replace(' ', '.')}@sovereignhotel.com",
                phone=f"04{random.randint(10000000, 99999999)}",
                role=role,
                employment_type=employment_dist[i],
                hourly_rate=AWARD_RATES[level],
                skills=["RSA"],  # Basic RSA for all pub staff
                active=True,
                department_id=location_id,
                department_name=location_name,
                department_category=dept_category,
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

            # Skip some days, generate 5-8 shifts per day (larger venue)
            if random.random() > 0.85:
                continue

            num_shifts = random.randint(5, 8)
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
                    duration = random.randint(5, 8)

                # Ensure end time is valid and after start time
                max_duration = 23 - start_hour
                actual_duration = min(duration, max_duration)
                if actual_duration <= 0:
                    actual_duration = 5

                end_hour = start_hour + actual_duration

                shift = Shift(
                    id=f"shift_{shift_id}",
                    employee_id=employee.id,
                    date=shift_date,
                    start_time=time(start_hour, 0),
                    end_time=time(end_hour, 0),
                    role=employee.role,
                    status=ShiftStatus.PUBLISHED.value if days_ahead > 7 else ShiftStatus.DRAFT.value,
                    break_minutes=45 if actual_duration >= 6 else 30,
                )
                shifts.append(shift)
                shift_id += 1

        return shifts

    def _generate_leave(self) -> Dict[str, List[Leave]]:
        """Generate realistic demo leave."""
        leave_by_emp: Dict[str, List[Leave]] = {}

        # 3-4 employees on leave this week (larger venue)
        leave_employees = random.sample(self._employees, k=random.randint(3, 4))

        today = date.today()
        leave_id = 1

        for emp in leave_employees:
            leave_start = today + timedelta(days=random.randint(0, 3))
            leave_duration = random.randint(2, 5)
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
        HumanForce does not provide revenue forecast API.

        Returns:
            Empty list
        """
        logger.info("[DEMO] HumanForce does not provide revenue forecast")
        return []

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Simulate webhook handling."""
        logger.info(f"[DEMO] Processing webhook: {event_type}")
        return {"status": "processed", "event": event_type}
