"""
Tanda Workforce Management Integration for RosterIQ.

This module provides bidirectional synchronization with Tanda's workforce management platform,
enabling RosterIQ to pull employee data, availability, leave, and timesheets, and push generated
rosters back to Tanda as shifts.

Tanda API documentation: https://my.tanda.co/api/v2/documentation
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
import json
from contextlib import asynccontextmanager

import httpx
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# Enums and Constants
# ============================================================================

class EmploymentType(str, Enum):
    """Employment types in Tanda."""
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CASUAL = "casual"
    CONTRACT = "contract"


class LeaveType(str, Enum):
    """Leave types in Tanda."""
    ANNUAL = "annual"
    SICK = "sick"
    UNPAID = "unpaid"
    PARENTAL = "parental"
    LONG_SERVICE = "long_service"
    PUBLIC_HOLIDAY = "public_holiday"


class ShiftStatus(str, Enum):
    """Shift status in Tanda."""
    DRAFT = "draft"
    APPROVED = "approved"
    PUBLISHED = "published"
    WORKED = "worked"
    PENDING_APPROVAL = "pending_approval"


class TimesheetStatus(str, Enum):
    """Timesheet status in Tanda."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    APPROVED = "approved"
    REJECTED = "rejected"


class WebhookEvent(str, Enum):
    """Tanda webhook event types."""
    USER_CREATED = "user.created"
    USER_UPDATED = "user.updated"
    USER_DELETED = "user.deleted"
    SHIFT_CREATED = "shift.created"
    SHIFT_UPDATED = "shift.updated"
    SHIFT_DELETED = "shift.deleted"
    LEAVE_CREATED = "leave.created"
    LEAVE_UPDATED = "leave.updated"
    TIMESHEET_SUBMITTED = "timesheet.submitted"


TANDA_BASE_URL = "https://my.tanda.co/api/v2"
TANDA_RATE_LIMIT = 100  # requests per minute
TANDA_REQUEST_TIMEOUT = 30


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class TandaCredentials:
    """OAuth 2.0 credentials for Tanda API."""
    client_id: str
    client_secret: str
    access_token: str
    refresh_token: Optional[str] = None
    organisation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert credentials to dictionary."""
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "organisation_id": self.organisation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TandaCredentials":
        """Create credentials from dictionary."""
        return cls(**data)


@dataclass
class TandaEmployee:
    """Employee record from Tanda."""
    id: str
    name: str
    email: str
    phone: Optional[str] = None
    photo_url: Optional[str] = None
    department_ids: List[str] = field(default_factory=list)
    qualification_ids: List[str] = field(default_factory=list)
    employment_type: str = EmploymentType.CASUAL.value
    hourly_rate: Optional[float] = None
    date_of_birth: Optional[date] = None
    start_date: Optional[date] = None
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "phone": self.phone,
            "photo_url": self.photo_url,
            "department_ids": self.department_ids,
            "qualification_ids": self.qualification_ids,
            "employment_type": self.employment_type,
            "hourly_rate": self.hourly_rate,
            "date_of_birth": self.date_of_birth.isoformat() if self.date_of_birth else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "active": self.active,
            "metadata": self.metadata,
        }


@dataclass
class TandaDepartment:
    """Department/location record from Tanda."""
    id: str
    name: str
    location_id: Optional[str] = None
    location_name: Optional[str] = None
    manager_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "location_id": self.location_id,
            "location_name": self.location_name,
            "manager_id": self.manager_id,
            "metadata": self.metadata,
        }


@dataclass
class TandaShift:
    """Shift record from Tanda."""
    id: str
    user_id: str
    date: date
    start_time: time
    finish_time: time
    department_id: str
    break_length: int = 0  # minutes
    status: str = ShiftStatus.DRAFT.value
    cost: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "date": self.date.isoformat(),
            "start_time": self.start_time.isoformat(),
            "finish_time": self.finish_time.isoformat(),
            "department_id": self.department_id,
            "break_length": self.break_length,
            "status": self.status,
            "cost": self.cost,
            "metadata": self.metadata,
        }


@dataclass
class TandaLeave:
    """Leave record from Tanda."""
    id: str
    user_id: str
    start_date: date
    end_date: date
    leave_type: str = LeaveType.ANNUAL.value
    status: str = "approved"
    hours: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "leave_type": self.leave_type,
            "status": self.status,
            "hours": self.hours,
            "metadata": self.metadata,
        }


@dataclass
class TandaAvailability:
    """Availability record from Tanda."""
    user_id: str
    day_of_week: int  # 0=Monday, 6=Sunday
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    recurring: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "user_id": self.user_id,
            "day_of_week": self.day_of_week,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "recurring": self.recurring,
            "metadata": self.metadata,
        }


@dataclass
class TandaTimesheet:
    """Timesheet record from Tanda."""
    id: str
    user_id: str
    date: date
    shifts: List[Dict[str, Any]] = field(default_factory=list)
    total_hours: float = 0.0
    total_cost: Optional[float] = None
    status: str = TimesheetStatus.PENDING.value
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "date": self.date.isoformat(),
            "shifts": self.shifts,
            "total_hours": self.total_hours,
            "total_cost": self.total_cost,
            "status": self.status,
            "metadata": self.metadata,
        }


@dataclass
class TandaQualification:
    """Qualification/skill record from Tanda."""
    id: str
    name: str
    description: Optional[str] = None
    required_for_roles: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Tanda Client - Async HTTP Client with OAuth and Rate Limiting
# ============================================================================

class TandaClient:
    """
    Async HTTP client for Tanda API with OAuth 2.0 token refresh,
    rate limiting, and pagination handling.
    """

    def __init__(
        self,
        credentials: TandaCredentials,
        base_url: str = TANDA_BASE_URL,
        timeout: int = TANDA_REQUEST_TIMEOUT,
        rate_limit: int = TANDA_RATE_LIMIT,
    ):
        """
        Initialize Tanda client.

        Args:
            credentials: OAuth credentials for Tanda API
            base_url: Tanda API base URL
            timeout: Request timeout in seconds
            rate_limit: Rate limit in requests per minute
        """
        self.credentials = credentials
        self.base_url = base_url
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.request_count = 0
        self.last_reset = datetime.now()
        self.client: Optional[httpx.AsyncClient] = None

    @asynccontextmanager
    async def session(self):
        """Context manager for HTTP session."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            self.client = client
            yield client
            self.client = None

    async def _refresh_token(self) -> bool:
        """
        Refresh OAuth access token.

        Returns:
            True if refresh successful, False otherwise
        """
        if not self.credentials.refresh_token:
            logger.warning("No refresh token available for token refresh")
            return False

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/oauth/token",
                    data={
                        "grant_type": "refresh_token",
                        "client_id": self.credentials.client_id,
                        "client_secret": self.credentials.client_secret,
                        "refresh_token": self.credentials.refresh_token,
                    },
                    timeout=self.timeout,
                )
                response.raise_for_status()

                data = response.json()
                self.credentials.access_token = data["access_token"]
                if "refresh_token" in data:
                    self.credentials.refresh_token = data["refresh_token"]

                logger.info("OAuth token refreshed successfully")
                return True

        except httpx.HTTPError as e:
            logger.error(f"Token refresh failed: {e}")
            return False

    async def _check_rate_limit(self):
        """Check and enforce rate limiting."""
        now = datetime.now()
        if (now - self.last_reset).total_seconds() >= 60:
            self.request_count = 0
            self.last_reset = now

        if self.request_count >= self.rate_limit:
            sleep_time = 60 - (now - self.last_reset).total_seconds()
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping {sleep_time}s")
                await asyncio.sleep(sleep_time)
                self.request_count = 0
                self.last_reset = datetime.now()

        self.request_count += 1

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with OAuth token."""
        return {
            "Authorization": f"Bearer {self.credentials.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        retry_on_401: bool = True,
    ) -> Dict[str, Any]:
        """
        Make authenticated HTTP request to Tanda API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH)
            endpoint: API endpoint (without base URL)
            data: Request body data
            params: Query parameters
            retry_on_401: Retry on 401 Unauthorized with token refresh

        Returns:
            Response JSON data

        Raises:
            httpx.HTTPError: If request fails
        """
        await self._check_rate_limit()

        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        async with self.session() as client:
            try:
                response = await client.request(
                    method,
                    url,
                    json=data,
                    params=params,
                    headers=headers,
                )

                if response.status_code == 401 and retry_on_401:
                    logger.warning("Received 401, attempting token refresh")
                    if await self._refresh_token():
                        return await self.request(
                            method,
                            endpoint,
                            data=data,
                            params=params,
                            retry_on_401=False,
                        )

                response.raise_for_status()
                return response.json()

            except httpx.HTTPError as e:
                logger.error(f"Request failed: {method} {endpoint}: {e}")
                raise

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """GET request."""
        return await self.request("GET", endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """POST request."""
        return await self.request("POST", endpoint, data=data)

    async def put(
        self,
        endpoint: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PUT request."""
        return await self.request("PUT", endpoint, data=data)

    async def patch(
        self,
        endpoint: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PATCH request."""
        return await self.request("PATCH", endpoint, data=data)

    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """DELETE request."""
        return await self.request("DELETE", endpoint)

    async def paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get all paginated results from endpoint.

        Args:
            endpoint: API endpoint
            params: Query parameters
            page_size: Items per page

        Returns:
            List of all items across all pages
        """
        if params is None:
            params = {}

        items = []
        page = 1

        while True:
            params["page"] = page
            params["limit"] = page_size

            response = await self.get(endpoint, params=params)
            page_items = response.get("data", response.get("results", []))

            if not page_items:
                break

            items.extend(page_items)

            if len(page_items) < page_size:
                break

            page += 1

        return items


# ============================================================================
# Mapping Functions
# ============================================================================

def tanda_employee_to_rosteriq(tanda_emp: Dict[str, Any]) -> TandaEmployee:
    """
    Convert Tanda employee API response to TandaEmployee model.

    Args:
        tanda_emp: Raw employee data from Tanda API

    Returns:
        TandaEmployee object
    """
    return TandaEmployee(
        id=tanda_emp.get("id"),
        name=tanda_emp.get("name", ""),
        email=tanda_emp.get("email", ""),
        phone=tanda_emp.get("phone"),
        photo_url=tanda_emp.get("photo_url"),
        department_ids=tanda_emp.get("department_ids", []),
        qualification_ids=tanda_emp.get("qualification_ids", []),
        employment_type=tanda_emp.get("employment_type", EmploymentType.CASUAL.value),
        hourly_rate=tanda_emp.get("hourly_rate"),
        date_of_birth=(
            datetime.fromisoformat(tanda_emp.get("date_of_birth")).date()
            if tanda_emp.get("date_of_birth")
            else None
        ),
        start_date=(
            datetime.fromisoformat(tanda_emp.get("start_date")).date()
            if tanda_emp.get("start_date")
            else None
        ),
        active=tanda_emp.get("active", True),
        metadata=tanda_emp.get("metadata", {}),
    )


def rosteriq_shift_to_tanda(
    shift: Dict[str, Any],
    employee_id: str,
    department_id: str,
) -> TandaShift:
    """
    Convert RosterIQ shift to Tanda shift format.

    Args:
        shift: Shift data from RosterIQ
        employee_id: Tanda employee ID
        department_id: Tanda department ID

    Returns:
        TandaShift object
    """
    shift_date = (
        datetime.fromisoformat(shift["date"]).date()
        if isinstance(shift["date"], str)
        else shift["date"]
    )
    start_time = (
        datetime.fromisoformat(shift["start_time"]).time()
        if isinstance(shift["start_time"], str)
        else shift["start_time"]
    )
    finish_time = (
        datetime.fromisoformat(shift["finish_time"]).time()
        if isinstance(shift["finish_time"], str)
        else shift["finish_time"]
    )

    return TandaShift(
        id=shift.get("id", ""),
        user_id=employee_id,
        date=shift_date,
        start_time=start_time,
        finish_time=finish_time,
        department_id=department_id,
        break_length=shift.get("break_length", 0),
        status=shift.get("status", ShiftStatus.DRAFT.value),
        cost=shift.get("cost"),
        metadata=shift.get("metadata", {}),
    )


def map_department_to_role(department: TandaDepartment) -> str:
    """
    Map Tanda department to RosterIQ role.

    Args:
        department: Tanda department record

    Returns:
        Role string for RosterIQ
    """
    # Simple mapping - can be extended
    dept_name = department.name.lower()

    role_mappings = {
        "kitchen": "chef",
        "bar": "bartender",
        "restaurant": "server",
        "front": "host",
        "management": "manager",
        "admin": "admin",
    }

    for keyword, role in role_mappings.items():
        if keyword in dept_name:
            return role

    return "general_staff"


# ============================================================================
# Tanda Sync Manager - Bidirectional Synchronization
# ============================================================================

class TandaSync:
    """
    Bidirectional synchronization with Tanda workforce management system.
    Handles pulling employees, availability, leave, timesheets and pushing rosters.
    """

    def __init__(self, client: TandaClient):
        """
        Initialize sync manager.

        Args:
            client: Initialized TandaClient instance
        """
        self.client = client
        self._employee_cache: Dict[str, TandaEmployee] = {}
        self._department_cache: Dict[str, TandaDepartment] = {}

    async def sync_employees(self) -> List[TandaEmployee]:
        """
        Pull all active employees from Tanda.

        Returns:
            List of TandaEmployee objects
        """
        try:
            employees_data = await self.client.paginate("/users")
            employees = [tanda_employee_to_rosteriq(emp) for emp in employees_data]

            # Update cache
            self._employee_cache = {emp.id: emp for emp in employees}

            logger.info(f"Synced {len(employees)} employees from Tanda")
            return employees

        except Exception as e:
            logger.error(f"Failed to sync employees: {e}")
            raise

    async def sync_departments(self) -> List[TandaDepartment]:
        """
        Pull department and location structure from Tanda.

        Returns:
            List of TandaDepartment objects
        """
        try:
            departments_data = await self.client.paginate("/departments")
            departments = [
                TandaDepartment(
                    id=dept.get("id"),
                    name=dept.get("name", ""),
                    location_id=dept.get("location_id"),
                    location_name=dept.get("location_name"),
                    manager_id=dept.get("manager_id"),
                    metadata=dept.get("metadata", {}),
                )
                for dept in departments_data
            ]

            # Update cache
            self._department_cache = {dept.id: dept for dept in departments}

            logger.info(f"Synced {len(departments)} departments from Tanda")
            return departments

        except Exception as e:
            logger.error(f"Failed to sync departments: {e}")
            raise

    async def sync_availability(
        self,
        employee_ids: List[str],
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> Dict[str, List[TandaAvailability]]:
        """
        Pull availability and leave for employees over date range.

        Args:
            employee_ids: List of Tanda user IDs
            date_from: Start date (default: today)
            date_to: End date (default: today + 30 days)

        Returns:
            Dict mapping employee_id to list of availability records
        """
        if date_from is None:
            date_from = date.today()
        if date_to is None:
            date_to = date_from + timedelta(days=30)

        result: Dict[str, List[TandaAvailability]] = {}

        try:
            for emp_id in employee_ids:
                availability = []

                # Get regular availability patterns
                avail_data = await self.client.get(
                    f"/users/{emp_id}/availability",
                    params={
                        "from": date_from.isoformat(),
                        "to": date_to.isoformat(),
                    },
                )

                for avail in avail_data.get("availability", []):
                    availability.append(
                        TandaAvailability(
                            user_id=emp_id,
                            day_of_week=avail.get("day_of_week", 0),
                            start_time=(
                                datetime.fromisoformat(avail.get("start_time")).time()
                                if avail.get("start_time")
                                else None
                            ),
                            end_time=(
                                datetime.fromisoformat(avail.get("end_time")).time()
                                if avail.get("end_time")
                                else None
                            ),
                            recurring=avail.get("recurring", True),
                        )
                    )

                result[emp_id] = availability

            logger.info(f"Synced availability for {len(employee_ids)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to sync availability: {e}")
            raise

    async def sync_qualifications(self) -> Dict[str, TandaQualification]:
        """
        Pull all qualifications/certifications available in Tanda.

        Returns:
            Dict mapping qualification_id to TandaQualification
        """
        try:
            quals_data = await self.client.paginate("/qualifications")
            qualifications = {
                qual.get("id"): TandaQualification(
                    id=qual.get("id"),
                    name=qual.get("name", ""),
                    description=qual.get("description"),
                    required_for_roles=qual.get("required_for_roles", []),
                    metadata=qual.get("metadata", {}),
                )
                for qual in quals_data
            }

            logger.info(f"Synced {len(qualifications)} qualifications from Tanda")
            return qualifications

        except Exception as e:
            logger.error(f"Failed to sync qualifications: {e}")
            raise

    async def push_roster(self, roster: Dict[str, Any]) -> Dict[str, Any]:
        """
        Push a generated roster to Tanda as shifts.

        Args:
            roster: Roster dict with structure {employee_id: [shifts]}

        Returns:
            Dict with push results and any errors
        """
        results = {"created": [], "updated": [], "errors": []}

        try:
            for emp_id, shifts in roster.items():
                for shift in shifts:
                    try:
                        result = await self.push_shift(shift, emp_id)
                        if result.get("id"):
                            results["created"].append(result)
                    except Exception as e:
                        results["errors"].append(
                            {"shift": shift, "employee_id": emp_id, "error": str(e)}
                        )

            logger.info(
                f"Pushed roster: {len(results['created'])} created, "
                f"{len(results['errors'])} errors"
            )
            return results

        except Exception as e:
            logger.error(f"Failed to push roster: {e}")
            raise

    async def push_shift(self, shift: Dict[str, Any], employee_id: str) -> Dict[str, Any]:
        """
        Create or update a single shift in Tanda.

        Args:
            shift: Shift data
            employee_id: Tanda user ID

        Returns:
            Created/updated shift data from Tanda
        """
        # Get department from shift or use default
        dept_id = shift.get("department_id", "")

        # Get department from cache if available
        if not dept_id and self._department_cache:
            dept_id = next(iter(self._department_cache.keys()))

        shift_data = {
            "user_id": employee_id,
            "date": (
                shift["date"].isoformat()
                if isinstance(shift["date"], date)
                else shift["date"]
            ),
            "start_time": (
                shift["start_time"].isoformat()
                if isinstance(shift["start_time"], time)
                else shift["start_time"]
            ),
            "finish_time": (
                shift["finish_time"].isoformat()
                if isinstance(shift["finish_time"], time)
                else shift["finish_time"]
            ),
            "department_id": dept_id,
            "break_length": shift.get("break_length", 0),
            "status": shift.get("status", ShiftStatus.DRAFT.value),
        }

        if shift.get("id"):
            # Update existing shift
            return await self.client.patch(f"/shifts/{shift['id']}", data=shift_data)
        else:
            # Create new shift
            return await self.client.post("/shifts", data=shift_data)

    async def push_shifts(self, shifts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create or update multiple shifts in Tanda.

        Args:
            shifts: List of shift dicts with employee_id field

        Returns:
            List of created/updated shifts
        """
        results = []
        errors = []

        for shift in shifts:
            try:
                emp_id = shift.get("employee_id", "")
                result = await self.push_shift(shift, emp_id)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to push shift: {e}")
                errors.append({"shift": shift, "error": str(e)})

        if errors:
            logger.warning(f"Push shifts completed with {len(errors)} errors")

        return results

    async def sync_timesheets(
        self,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> List[TandaTimesheet]:
        """
        Pull timesheets (actual vs rostered hours) for analysis.

        Args:
            date_from: Start date
            date_to: End date

        Returns:
            List of timesheet records
        """
        if date_from is None:
            date_from = date.today() - timedelta(days=7)
        if date_to is None:
            date_to = date.today()

        try:
            timesheets_data = await self.client.paginate(
                "/timesheets",
                params={
                    "from": date_from.isoformat(),
                    "to": date_to.isoformat(),
                },
            )

            timesheets = [
                TandaTimesheet(
                    id=ts.get("id"),
                    user_id=ts.get("user_id", ""),
                    date=datetime.fromisoformat(ts.get("date", "")).date(),
                    shifts=ts.get("shifts", []),
                    total_hours=ts.get("total_hours", 0.0),
                    total_cost=ts.get("total_cost"),
                    status=ts.get("status", TimesheetStatus.PENDING.value),
                    metadata=ts.get("metadata", {}),
                )
                for ts in timesheets_data
            ]

            logger.info(f"Synced {len(timesheets)} timesheets from Tanda")
            return timesheets

        except Exception as e:
            logger.error(f"Failed to sync timesheets: {e}")
            raise

    async def get_award_tags(self) -> List[Dict[str, Any]]:
        """
        Pull award interpretation tags for compliance.

        Returns:
            List of award tag configurations
        """
        try:
            tags = await self.client.paginate("/awards/tags")
            logger.info(f"Synced {len(tags)} award tags from Tanda")
            return tags
        except Exception as e:
            logger.error(f"Failed to sync award tags: {e}")
            raise

    async def health_check(self) -> Dict[str, Any]:
        """
        Check Tanda API connectivity and auth.

        Returns:
            Health check result with status
        """
        try:
            response = await self.client.get("/users", params={"limit": 1})
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "response": response,
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
            }


# ============================================================================
# Webhook Handler
# ============================================================================

class TandaWebhook:
    """
    Handler for incoming Tanda webhooks for real-time updates.
    Processes employee changes, leave requests, and timesheet updates.
    """

    def __init__(self, sync: TandaSync):
        """
        Initialize webhook handler.

        Args:
            sync: TandaSync instance for processing updates
        """
        self.sync = sync
        self.router = APIRouter(prefix="/webhooks/tanda", tags=["tanda-webhooks"])
        self._setup_routes()

    def _setup_routes(self):
        """Setup FastAPI routes for webhooks."""
        @self.router.post("/events")
        async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
            """Handle incoming Tanda webhook event."""
            try:
                payload = await request.json()
                event_type = payload.get("event_type")
                data = payload.get("data", {})

                logger.info(f"Received Tanda webhook: {event_type}")

                # Process event in background
                if event_type == WebhookEvent.USER_CREATED.value:
                    background_tasks.add_task(self._handle_user_created, data)
                elif event_type == WebhookEvent.USER_UPDATED.value:
                    background_tasks.add_task(self._handle_user_updated, data)
                elif event_type == WebhookEvent.SHIFT_CREATED.value:
                    background_tasks.add_task(self._handle_shift_created, data)
                elif event_type == WebhookEvent.SHIFT_UPDATED.value:
                    background_tasks.add_task(self._handle_shift_updated, data)
                elif event_type == WebhookEvent.LEAVE_CREATED.value:
                    background_tasks.add_task(self._handle_leave_created, data)
                elif event_type == WebhookEvent.TIMESHEET_SUBMITTED.value:
                    background_tasks.add_task(self._handle_timesheet_submitted, data)

                return {"status": "accepted"}

            except Exception as e:
                logger.error(f"Webhook processing error: {e}")
                raise HTTPException(status_code=400, detail=str(e))

    async def _handle_user_created(self, data: Dict[str, Any]):
        """Handle user created event."""
        try:
            user_id = data.get("id")
            logger.info(f"User created event: {user_id}")
            # Trigger employee sync
            await self.sync.sync_employees()
        except Exception as e:
            logger.error(f"Error handling user created: {e}")

    async def _handle_user_updated(self, data: Dict[str, Any]):
        """Handle user updated event."""
        try:
            user_id = data.get("id")
            logger.info(f"User updated event: {user_id}")
            # Trigger employee sync
            await self.sync.sync_employees()
        except Exception as e:
            logger.error(f"Error handling user updated: {e}")

    async def _handle_shift_created(self, data: Dict[str, Any]):
        """Handle shift created event."""
        try:
            shift_id = data.get("id")
            logger.info(f"Shift created event: {shift_id}")
        except Exception as e:
            logger.error(f"Error handling shift created: {e}")

    async def _handle_shift_updated(self, data: Dict[str, Any]):
        """Handle shift updated event."""
        try:
            shift_id = data.get("id")
            logger.info(f"Shift updated event: {shift_id}")
        except Exception as e:
            logger.error(f"Error handling shift updated: {e}")

    async def _handle_leave_created(self, data: Dict[str, Any]):
        """Handle leave created event."""
        try:
            leave_id = data.get("id")
            logger.info(f"Leave created event: {leave_id}")
        except Exception as e:
            logger.error(f"Error handling leave created: {e}")

    async def _handle_timesheet_submitted(self, data: Dict[str, Any]):
        """Handle timesheet submitted event."""
        try:
            timesheet_id = data.get("id")
            logger.info(f"Timesheet submitted event: {timesheet_id}")
        except Exception as e:
            logger.error(f"Error handling timesheet submitted: {e}")

    def get_router(self) -> APIRouter:
        """Get FastAPI router for webhooks."""
        return self.router


# ============================================================================
# Factory Function
# ============================================================================

async def create_tanda_integration(
    credentials: TandaCredentials,
) -> Tuple[TandaSync, TandaWebhook]:
    """
    Factory function to create and initialize Tanda integration.

    Args:
        credentials: OAuth credentials for Tanda API

    Returns:
        Tuple of (TandaSync, TandaWebhook) instances
    """
    client = TandaClient(credentials)
    sync = TandaSync(client)
    webhook = TandaWebhook(sync)

    logger.info("Tanda integration created successfully")
    return sync, webhook
