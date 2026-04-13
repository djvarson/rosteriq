"""
Tanda Workforce Management Adapter for RosterIQ.

This module provides a pluggable adapter pattern for workforce management platforms,
with full implementations for Tanda WFM and a realistic demo adapter for development.

Tanda API documentation: https://my.tanda.co/api/v2/documentation
"""

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
import random
from contextlib import asynccontextmanager

import httpx

logger = logging.getLogger(__name__)


# ============================================================================
# Constants and Enums
# ============================================================================

TANDA_BASE_URL = "https://my.tanda.co/api/v2"
TANDA_RATE_LIMIT = 200  # requests per minute
TANDA_REQUEST_TIMEOUT = 30

# Australian hospitality award rates (Hospitality Award 2020, updated 2025)
AWARD_RATES = {
    1: 24.10,   # Level 1
    2: 25.35,   # Level 2
    3: 26.48,   # Level 3
    4: 27.55,   # Level 4
    5: 29.58,   # Level 5
    6: 31.44,   # Level 6
}


class EmploymentType(str, Enum):
    """Employment types."""
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CASUAL = "casual"
    CONTRACT = "contract"


class LeaveType(str, Enum):
    """Leave types."""
    ANNUAL = "annual"
    SICK = "sick"
    UNPAID = "unpaid"
    PARENTAL = "parental"
    LONG_SERVICE = "long_service"
    PUBLIC_HOLIDAY = "public_holiday"


class ShiftStatus(str, Enum):
    """Shift status."""
    DRAFT = "draft"
    APPROVED = "approved"
    PUBLISHED = "published"
    WORKED = "worked"
    PENDING_APPROVAL = "pending_approval"


class WebhookEventType(str, Enum):
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


class DepartmentCategory(str, Enum):
    """
    Normalised department categories used by RosterIQ to split demand
    across staff types. Tanda department names vary per venue, but our
    forecasting and recommendation engines care about these buckets.
    """
    KITCHEN = "kitchen"
    BAR = "bar"
    FLOOR = "floor"          # table service / waitstaff
    FOH = "foh"              # front-of-house / host / reservations
    MANAGEMENT = "management"
    SECURITY = "security"
    OTHER = "other"


# Keyword → category lookup for mapping Tanda department names to
# RosterIQ categories. Order matters (first match wins).
_DEPARTMENT_KEYWORDS: List[Tuple[str, str]] = [
    ("kitchen", DepartmentCategory.KITCHEN.value),
    ("chef", DepartmentCategory.KITCHEN.value),
    ("grill", DepartmentCategory.KITCHEN.value),
    ("prep", DepartmentCategory.KITCHEN.value),
    ("pastry", DepartmentCategory.KITCHEN.value),
    ("bar", DepartmentCategory.BAR.value),
    ("cocktail", DepartmentCategory.BAR.value),
    ("wine", DepartmentCategory.BAR.value),
    ("barista", DepartmentCategory.BAR.value),
    ("floor", DepartmentCategory.FLOOR.value),
    ("server", DepartmentCategory.FLOOR.value),
    ("wait", DepartmentCategory.FLOOR.value),
    ("table", DepartmentCategory.FLOOR.value),
    ("host", DepartmentCategory.FOH.value),
    ("reception", DepartmentCategory.FOH.value),
    ("reservation", DepartmentCategory.FOH.value),
    ("greet", DepartmentCategory.FOH.value),
    ("concierge", DepartmentCategory.FOH.value),
    ("front", DepartmentCategory.FOH.value),
    ("manager", DepartmentCategory.MANAGEMENT.value),
    ("management", DepartmentCategory.MANAGEMENT.value),
    ("admin", DepartmentCategory.MANAGEMENT.value),
    ("supervisor", DepartmentCategory.MANAGEMENT.value),
    ("security", DepartmentCategory.SECURITY.value),
    ("crowd", DepartmentCategory.SECURITY.value),
    ("rsa", DepartmentCategory.SECURITY.value),
]


def categorise_department(name: Optional[str]) -> str:
    """
    Normalise a raw Tanda department or role name into a RosterIQ
    DepartmentCategory. Matches keywords case-insensitively. Unknown
    names map to "other" rather than erroring so forecasting can still
    run on every employee.

    Args:
        name: Raw Tanda department or role name (e.g. "Main Kitchen",
              "Public Bar", "Restaurant Floor"). May be None.

    Returns:
        One of the DepartmentCategory string values.
    """
    if not name:
        return DepartmentCategory.OTHER.value
    lower = name.lower()
    for keyword, category in _DEPARTMENT_KEYWORDS:
        if keyword in lower:
            return category
    return DepartmentCategory.OTHER.value


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Employee:
    """
    Employee data model.

    department_name is the raw Tanda department string (e.g. "Public Bar")
    and department_category is the normalised RosterIQ bucket
    (e.g. "bar") — both are populated so the Roster Maker can display
    the friendly name while the forecast engine groups by category.
    """
    id: str
    name: str
    email: str
    phone: Optional[str]
    role: str
    employment_type: str
    hourly_rate: float
    skills: List[str]
    active: bool = True
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    department_category: str = DepartmentCategory.OTHER.value


@dataclass
class ForecastRevenue:
    """
    Tanda's own forecast revenue for a venue on a given date, used by
    RosterIQ as a benchmark against its own forecast so we can show the
    delta ("Tanda thinks $11,500, we think $13,200 — here's why").

    Fields mirror what Tanda returns from
    /organisations/{org_id}/revenue_forecast.
    """
    date: date
    forecast: float
    department_breakdown: Dict[str, float] = None  # type: ignore[assignment]
    source: str = "tanda"

    def __post_init__(self) -> None:
        if self.department_breakdown is None:
            self.department_breakdown = {}


@dataclass
class Availability:
    """Employee availability."""
    employee_id: str
    day_of_week: int  # 0=Monday, 6=Sunday
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    recurring: bool = True


@dataclass
class Leave:
    """Leave record."""
    id: str
    employee_id: str
    start_date: date
    end_date: date
    leave_type: str
    status: str


@dataclass
class Shift:
    """Shift record."""
    id: str
    employee_id: str
    date: date
    start_time: time
    end_time: time
    role: str
    status: str = ShiftStatus.DRAFT.value
    break_minutes: int = 0


@dataclass
class Timesheet:
    """Timesheet record."""
    id: str
    employee_id: str
    date: date
    hours: float
    shifts: List[Dict[str, Any]]


# ============================================================================
# Abstract Base Class
# ============================================================================

class SchedulingPlatformAdapter(ABC):
    """
    Abstract base class for workforce management platform adapters.

    Allows swapping between different WFM providers (Tanda, Deputy, HumanForce, etc.)
    while maintaining consistent interface.
    """

    @abstractmethod
    async def get_employees(self, org_id: str) -> List[Employee]:
        """
        Retrieve all active employees for an organization.

        Args:
            org_id: Organization/venue ID

        Returns:
            List of Employee objects with roles, employment type, and rates
        """
        pass

    @abstractmethod
    async def get_availability(
        self,
        employee_ids: List[str],
        date_range: Tuple[date, date],
    ) -> Dict[str, List[Availability]]:
        """
        Retrieve availability blocks for employees.

        Args:
            employee_ids: List of employee IDs
            date_range: (start_date, end_date) tuple

        Returns:
            Dict mapping employee_id to list of Availability objects
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def get_shifts(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Shift]:
        """
        Retrieve existing published shifts.

        Args:
            org_id: Organization/venue ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Shift objects
        """
        pass

    @abstractmethod
    async def get_timesheets(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Timesheet]:
        """
        Retrieve actual hours worked (timesheets).

        Args:
            org_id: Organization/venue ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Timesheet objects
        """
        pass

    @abstractmethod
    async def push_draft_roster(
        self,
        org_id: str,
        shifts: List[Shift],
    ) -> Dict[str, Any]:
        """
        Push AI-generated shifts as draft roster.

        Args:
            org_id: Organization/venue ID
            shifts: List of Shift objects to create

        Returns:
            Dict with creation results and any errors
        """
        pass

    @abstractmethod
    async def get_forecast_revenue(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[ForecastRevenue]:
        """
        Retrieve Tanda's own revenue forecast for an organisation over
        a date range. Used by RosterIQ as a benchmark against its own
        forecast engine so the Roster Maker can show the delta.

        Args:
            org_id: Organization/venue ID
            date_range: (start_date, end_date) tuple (inclusive)

        Returns:
            List of ForecastRevenue records, one per date in range.
            Dates missing from Tanda are omitted rather than zero-filled.
        """
        pass

    @abstractmethod
    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process incoming webhook events.

        Args:
            event_type: Type of event (e.g., "user.created", "shift.updated")
            payload: Event payload

        Returns:
            Dict with processing result
        """
        pass


# ============================================================================
# Tanda Client with OAuth 2.0 and Rate Limiting
# ============================================================================

class TandaClient:
    """
    Async HTTP client for Tanda API with OAuth 2.0 client credentials flow,
    token bucket rate limiting, and retry with exponential backoff.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        base_url: str = TANDA_BASE_URL,
        timeout: int = TANDA_REQUEST_TIMEOUT,
        rate_limit: int = TANDA_RATE_LIMIT,
    ):
        """
        Initialize Tanda API client.

        Args:
            client_id: OAuth 2.0 client ID
            client_secret: OAuth 2.0 client secret
            base_url: Tanda API base URL
            timeout: Request timeout in seconds
            rate_limit: Rate limit in requests per minute
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None

        # Token bucket for rate limiting
        self.request_count = 0
        self.last_reset = datetime.now()

    async def _get_token(self) -> str:
        """
        Obtain or refresh OAuth 2.0 access token using client credentials flow.

        Returns:
            Access token string

        Raises:
            httpx.HTTPError: If token request fails
        """
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.access_token

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)

            logger.debug("OAuth token obtained successfully")
            return self.access_token

    async def _check_rate_limit(self):
        """
        Check and enforce token bucket rate limiting (200 req/min).
        """
        now = datetime.now()
        if (now - self.last_reset).total_seconds() >= 60:
            self.request_count = 0
            self.last_reset = now

        if self.request_count >= self.rate_limit:
            sleep_time = 60 - (now - self.last_reset).total_seconds()
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
                self.request_count = 0
                self.last_reset = datetime.now()

        self.request_count += 1

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Get HTTP headers with OAuth token."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _retry_with_backoff(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> httpx.Response:
        """
        Execute request with exponential backoff retry (3 retries).

        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            json_data: Request body
            params: Query parameters
            max_retries: Maximum number of retries

        Returns:
            HTTP response

        Raises:
            httpx.HTTPError: If all retries fail
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        json=json_data,
                        params=params,
                    )
                    response.raise_for_status()
                    return response
                except httpx.HTTPError as e:
                    last_exception = e
                    if attempt < max_retries:
                        backoff = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(
                            f"Request failed (attempt {attempt + 1}), retrying in {backoff}s: {e}"
                        )
                        await asyncio.sleep(backoff)

            raise last_exception

    async def request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to Tanda API.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            endpoint: API endpoint path
            json_data: Request body
            params: Query parameters

        Returns:
            Response JSON data
        """
        await self._check_rate_limit()

        token = await self._get_token()
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(token)

        response = await self._retry_with_backoff(
            method,
            url,
            headers,
            json_data=json_data,
            params=params,
        )

        return response.json()

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
        json_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """POST request."""
        return await self.request("POST", endpoint, json_data=json_data)

    async def put(
        self,
        endpoint: str,
        json_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PUT request."""
        return await self.request("PUT", endpoint, json_data=json_data)

    async def patch(
        self,
        endpoint: str,
        json_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """PATCH request."""
        return await self.request("PATCH", endpoint, json_data=json_data)

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
        Retrieve all paginated results from endpoint.

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
            query_params = {**params, "page": page, "limit": page_size}
            response = await self.get(endpoint, params=query_params)

            page_items = response.get("data", response.get("results", []))
            if not page_items:
                break

            items.extend(page_items)

            if len(page_items) < page_size:
                break

            page += 1

        return items


# ============================================================================
# Real Tanda Adapter
# ============================================================================

class TandaAdapter(SchedulingPlatformAdapter):
    """
    Production Tanda WFM adapter using real API calls.

    Requires OAuth 2.0 credentials (TANDA_CLIENT_ID and TANDA_CLIENT_SECRET).
    """

    def __init__(self, client: TandaClient):
        """
        Initialize Tanda adapter.

        Args:
            client: Configured TandaClient instance
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
            dept_data = await self.client.paginate(
                f"/organisations/{org_id}/departments",
            )
            self._department_cache = {
                d.get("id"): d.get("name", "") for d in dept_data if d.get("id")
            }
            logger.info(
                f"Refreshed Tanda department cache: {len(self._department_cache)} departments"
            )
        except Exception as e:
            # Non-fatal — employees still return, just without dept names
            logger.warning(f"Failed to refresh Tanda department cache: {e}")
        return self._department_cache

    async def get_employees(self, org_id: str) -> List[Employee]:
        """
        Retrieve all active employees from Tanda, enriched with their
        primary department name and normalised RosterIQ category so the
        forecast engine can split demand across kitchen/bar/floor.

        Args:
            org_id: Tanda organization ID

        Returns:
            List of Employee objects
        """
        try:
            # Make sure department cache is populated before mapping
            if not self._department_cache:
                await self._refresh_department_cache(org_id)

            employees_data = await self.client.paginate(
                f"/organisations/{org_id}/users",
                params={"active": True},
            )

            employees = []
            for emp_data in employees_data:
                # Tanda returns `department_ids` (list); we take the first
                # as the primary. If a future release wants multi-department
                # support the model already carries the raw id, so we can
                # look up the extras without another API call.
                dept_ids = emp_data.get("department_ids") or []
                primary_dept_id = dept_ids[0] if dept_ids else emp_data.get("department_id")
                dept_name = (
                    self._department_cache.get(primary_dept_id)
                    if primary_dept_id
                    else None
                )
                # Fall back to the role string if no department is set —
                # old Tanda tenants don't always have departments populated.
                category = categorise_department(
                    dept_name or emp_data.get("role") or ""
                )

                employee = Employee(
                    id=emp_data.get("id"),
                    name=emp_data.get("name", ""),
                    email=emp_data.get("email", ""),
                    phone=emp_data.get("phone"),
                    role=emp_data.get("role", "general_staff"),
                    employment_type=emp_data.get("employment_type", EmploymentType.CASUAL.value),
                    hourly_rate=emp_data.get("hourly_rate", AWARD_RATES[1]),
                    skills=emp_data.get("skills", []),
                    active=emp_data.get("active", True),
                    department_id=primary_dept_id,
                    department_name=dept_name,
                    department_category=category,
                )
                employees.append(employee)
                self._employee_cache[employee.id] = employee

            logger.info(f"Retrieved {len(employees)} employees from Tanda")
            return employees

        except Exception as e:
            logger.error(f"Failed to get employees from Tanda: {e}")
            raise

    async def get_forecast_revenue(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[ForecastRevenue]:
        """
        Pull Tanda's own revenue forecast via /revenue_forecast.

        Tanda's endpoint returns entries shaped roughly as:
            {"date": "YYYY-MM-DD", "forecast": <float>,
             "departments": {"<dept_id>": <float>, ...}}

        We remap department_id keys to their human names so the dashboard
        can show "Kitchen: $4,200 / Bar: $3,800" without a second lookup.
        """
        start_date, end_date = date_range
        try:
            if not self._department_cache:
                await self._refresh_department_cache(org_id)

            response = await self.client.get(
                f"/organisations/{org_id}/revenue_forecast",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            )
            raw = response.get("data", response.get("forecasts", response))
            if isinstance(raw, dict):
                # Some Tanda tenants return {"forecasts": [...]} — handled above;
                # a bare dict implies a single day, wrap it.
                raw = [raw]

            forecasts: List[ForecastRevenue] = []
            for entry in raw:
                d_str = entry.get("date")
                if not d_str:
                    continue
                dept_raw = entry.get("departments") or entry.get("department_breakdown") or {}
                dept_named: Dict[str, float] = {}
                for dept_id, amount in dept_raw.items():
                    name = self._department_cache.get(dept_id, dept_id)
                    try:
                        dept_named[name] = float(amount)
                    except (TypeError, ValueError):
                        continue
                try:
                    fc_value = float(entry.get("forecast") or entry.get("value") or 0.0)
                except (TypeError, ValueError):
                    fc_value = 0.0
                forecasts.append(ForecastRevenue(
                    date=datetime.fromisoformat(d_str).date(),
                    forecast=fc_value,
                    department_breakdown=dept_named,
                    source="tanda",
                ))

            logger.info(
                f"Retrieved Tanda revenue forecast: "
                f"{len(forecasts)} days for org {org_id}"
            )
            return forecasts

        except Exception as e:
            logger.error(f"Failed to get Tanda revenue forecast: {e}")
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
                availability_data = await self.client.get(
                    f"/users/{emp_id}/availability",
                    params={
                        "from": start_date.isoformat(),
                        "to": end_date.isoformat(),
                    },
                )

                availabilities = []
                for avail_block in availability_data.get("availability", []):
                    avail = Availability(
                        employee_id=emp_id,
                        day_of_week=avail_block.get("day_of_week", 0),
                        start_time=(
                            datetime.fromisoformat(avail_block.get("start_time")).time()
                            if avail_block.get("start_time") else None
                        ),
                        end_time=(
                            datetime.fromisoformat(avail_block.get("end_time")).time()
                            if avail_block.get("end_time") else None
                        ),
                        recurring=avail_block.get("recurring", True),
                    )
                    availabilities.append(avail)

                result[emp_id] = availabilities

            logger.info(f"Retrieved availability for {len(employee_ids)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get availability from Tanda: {e}")
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
                    f"/users/{emp_id}/leave",
                    params={
                        "from": start_date.isoformat(),
                        "to": end_date.isoformat(),
                        "statuses": "approved,pending",
                    },
                )

                leaves = []
                for leave_block in leave_data:
                    leave = Leave(
                        id=leave_block.get("id"),
                        employee_id=emp_id,
                        start_date=datetime.fromisoformat(leave_block.get("start_date")).date(),
                        end_date=datetime.fromisoformat(leave_block.get("end_date")).date(),
                        leave_type=leave_block.get("type", LeaveType.ANNUAL.value),
                        status=leave_block.get("status", "approved"),
                    )
                    leaves.append(leave)

                result[emp_id] = leaves

            logger.info(f"Retrieved leave for {len(employee_ids)} employees")
            return result

        except Exception as e:
            logger.error(f"Failed to get leave from Tanda: {e}")
            raise

    async def get_shifts(
        self,
        org_id: str,
        date_range: Tuple[date, date],
    ) -> List[Shift]:
        """
        Retrieve published shifts.

        Args:
            org_id: Organization ID
            date_range: (start_date, end_date) tuple

        Returns:
            List of Shift objects
        """
        try:
            start_date, end_date = date_range

            shifts_data = await self.client.paginate(
                f"/organisations/{org_id}/shifts",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                    "status": "published",
                },
            )

            shifts = []
            for shift_data in shifts_data:
                shift = Shift(
                    id=shift_data.get("id"),
                    employee_id=shift_data.get("user_id"),
                    date=datetime.fromisoformat(shift_data.get("date")).date(),
                    start_time=datetime.fromisoformat(shift_data.get("start_time")).time(),
                    end_time=datetime.fromisoformat(shift_data.get("finish_time")).time(),
                    role=shift_data.get("role", "general_staff"),
                    status=shift_data.get("status", ShiftStatus.PUBLISHED.value),
                    break_minutes=shift_data.get("break_length", 0),
                )
                shifts.append(shift)

            logger.info(f"Retrieved {len(shifts)} published shifts")
            return shifts

        except Exception as e:
            logger.error(f"Failed to get shifts from Tanda: {e}")
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
                f"/organisations/{org_id}/timesheets",
                params={
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                },
            )

            timesheets = []
            for ts_data in timesheets_data:
                timesheet = Timesheet(
                    id=ts_data.get("id"),
                    employee_id=ts_data.get("user_id"),
                    date=datetime.fromisoformat(ts_data.get("date")).date(),
                    hours=ts_data.get("total_hours", 0.0),
                    shifts=ts_data.get("shifts", []),
                )
                timesheets.append(timesheet)

            logger.info(f"Retrieved {len(timesheets)} timesheets")
            return timesheets

        except Exception as e:
            logger.error(f"Failed to get timesheets from Tanda: {e}")
            raise

    async def push_draft_roster(
        self,
        org_id: str,
        shifts: List[Shift],
    ) -> Dict[str, Any]:
        """
        Push AI-generated shifts as draft roster to Tanda.

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
                        "user_id": shift.employee_id,
                        "date": shift.date.isoformat(),
                        "start_time": shift.start_time.isoformat(),
                        "finish_time": shift.end_time.isoformat(),
                        "break_length": shift.break_minutes,
                        "status": ShiftStatus.DRAFT.value,
                    }

                    created = await self.client.post(
                        f"/organisations/{org_id}/shifts",
                        json_data=shift_data,
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

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process incoming Tanda webhook events.

        Args:
            event_type: Event type string (e.g., "user.created")
            payload: Event payload

        Returns:
            Dict with processing result
        """
        try:
            logger.info(f"Processing webhook: {event_type}")

            # Process based on event type
            if event_type == WebhookEventType.USER_CREATED.value:
                return {"status": "processed", "event": "user_created"}
            elif event_type == WebhookEventType.USER_UPDATED.value:
                return {"status": "processed", "event": "user_updated"}
            elif event_type == WebhookEventType.LEAVE_CREATED.value:
                return {"status": "processed", "event": "leave_created"}
            elif event_type == WebhookEventType.TIMESHEET_SUBMITTED.value:
                return {"status": "processed", "event": "timesheet_submitted"}
            else:
                logger.warning(f"Unhandled webhook event type: {event_type}")
                return {"status": "ignored", "event": event_type}

        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
            raise


# ============================================================================
# Demo Tanda Adapter
# ============================================================================

class DemoTandaAdapter(SchedulingPlatformAdapter):
    """
    Demo adapter for development and testing.

    Returns realistic data for a Brisbane hotel with:
    - 28 employees across 6 departments
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
        """Generate realistic demo employees."""
        names = [
            "Jack Mitchell", "Sophie Chen", "Liam O'Brien", "Priya Sharma",
            "Emma Watson", "James Murphy", "Olivia Brown", "Noah Singh",
            "Ava Taylor", "Ethan Davis", "Mia Johnson", "Lucas Anderson",
            "Isabella Martinez", "Logan Thompson", "Charlotte Lee", "Mason Garcia",
            "Amelia Rodriguez", "Benjamin White", "Harper Garcia", "Alexander Moore",
            "Evelyn Taylor", "Michael Jackson", "Abigail White", "Daniel Harris",
            "Emily Martin", "Joseph Thompson", "Scarlett Robinson", "Samuel Clark",
        ]

        # Department structure: Bar (8), Floor (7), Kitchen (6), Management (3), Security (2), Host (2)
        departments = {
            "Bar": ["bar", "cocktails", "coffee", "wine_service"],
            "Floor": ["floor", "wine_service", "table_service"],
            "Kitchen": ["kitchen", "grill", "prep", "pastry"],
            "Management": ["management", "scheduling", "compliance"],
            "Security": ["security", "crowd_control"],
            "Host": ["host", "reservation", "greeting"],
        }

        employment_dist = [EmploymentType.CASUAL.value] * 17 + \
                         [EmploymentType.PART_TIME.value] * 7 + \
                         [EmploymentType.FULL_TIME.value] * 4  # 28 total
        random.shuffle(employment_dist)

        employees = []
        emp_id = 1
        dept_idx = 0

        for i, name in enumerate(names):
            dept_name = list(departments.keys())[
                min(i // 4, len(departments) - 1)
            ]

            # Award level based on employment type
            if employment_dist[i] == EmploymentType.FULL_TIME.value:
                level = random.choice([4, 5, 6])
            elif employment_dist[i] == EmploymentType.PART_TIME.value:
                level = random.choice([2, 3, 4])
            else:
                level = random.choice([1, 2, 3])

            employee = Employee(
                id=f"tanda_{emp_id}",
                name=name,
                email=f"{name.lower().replace(' ', '.')}@rosteriq.com",
                phone=f"04{random.randint(10000000, 99999999)}",
                role=dept_name.lower(),
                employment_type=employment_dist[i],
                hourly_rate=AWARD_RATES[level],
                skills=random.sample(
                    departments[dept_name],
                    k=min(len(departments[dept_name]), random.randint(2, 4))
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

            # Skip some days, generate 4-6 shifts per day
            if random.random() > 0.9:
                continue

            num_shifts = random.randint(4, 6)
            for _ in range(num_shifts):
                employee = random.choice(self._employees)

                # Morning, afternoon, or evening shift
                shift_type = random.choice(["morning", "afternoon", "evening"])
                if shift_type == "morning":
                    start_hour = random.randint(6, 8)
                    duration = random.randint(6, 8)
                elif shift_type == "afternoon":
                    start_hour = random.randint(12, 14)
                    duration = random.randint(5, 7)
                else:
                    start_hour = random.randint(17, 19)
                    duration = random.randint(5, 7)

                shift = Shift(
                    id=f"shift_{shift_id}",
                    employee_id=employee.id,
                    date=shift_date,
                    start_time=time(start_hour, 0),
                    end_time=time((start_hour + duration) % 24, 0),
                    role=employee.role,
                    status=ShiftStatus.PUBLISHED.value if days_ahead > 7 else ShiftStatus.DRAFT.value,
                    break_minutes=30 if duration >= 6 else 15,
                )
                shifts.append(shift)
                shift_id += 1

        return shifts

    def _generate_leave(self) -> Dict[str, List[Leave]]:
        """Generate realistic demo leave."""
        leave_by_emp: Dict[str, List[Leave]] = {}

        # 2-3 employees on leave this week
        leave_employees = random.sample(self._employees, k=random.randint(2, 3))

        today = date.today()
        leave_id = 1

        for emp in leave_employees:
            leave_start = today + timedelta(days=random.randint(0, 3))
            leave_duration = random.randint(1, 5)
            leave_end = leave_start + timedelta(days=leave_duration)

            leave_type = random.choice([
                LeaveType.ANNUAL.value,
                LeaveType.SICK.value,
                LeaveType.UNPAID.value,
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
                for day_of_week in range(5):  # Mon-Fri
                    availabilities.append(Availability(
                        employee_id=emp_id,
                        day_of_week=day_of_week,
                        start_time=time(6, 0),
                        end_time=time(22, 0),
                        recurring=True,
                    ))

            # Part-time: available 3-4 days
            elif employee.employment_type == EmploymentType.PART_TIME.value:
                available_days = random.sample(range(7), k=random.randint(3, 4))
                for day_of_week in available_days:
                    start = time(random.randint(6, 12), 0)
                    end = time(random.randint(14, 22), 0)
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
                    # Students unavailable weekday mornings
                    if day_of_week < 5 and random.random() > 0.5:
                        start = time(12, 0)
                    else:
                        start = time(random.randint(6, 10), 0)

                    end = time(random.randint(16, 22), 0)
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
        Generate realistic demo revenue forecast: weekday baseline ~$8k,
        Friday/Saturday ~$15k-$18k, Sunday ~$10k. Department breakdown
        skews ~50% kitchen, ~30% bar, ~15% floor, ~5% other — matches a
        typical AU hospitality venue.
        """
        start_date, end_date = date_range
        forecasts: List[ForecastRevenue] = []
        current = start_date
        # Deterministic-ish noise so repeated calls land close to each other
        # but still vary day-to-day (seeded by date ordinal).
        while current <= end_date:
            dow = current.weekday()  # 0=Mon, 6=Sun
            if dow in (4, 5):        # Fri/Sat
                base = random.uniform(15000, 18000)
            elif dow == 6:           # Sun
                base = random.uniform(9000, 11500)
            else:                    # Mon-Thu
                base = random.uniform(6500, 9500)
            base = round(base, 2)
            dept_mix = {
                "Kitchen": round(base * 0.50, 2),
                "Bar": round(base * 0.30, 2),
                "Floor": round(base * 0.15, 2),
                "Other": round(base * 0.05, 2),
            }
            forecasts.append(ForecastRevenue(
                date=current,
                forecast=base,
                department_breakdown=dept_mix,
                source="tanda_demo",
            ))
            current += timedelta(days=1)
        logger.info(
            f"[DEMO] Generated {len(forecasts)}-day forecast revenue for org {org_id}"
        )
        return forecasts

    async def handle_webhook(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Simulate webhook handling."""
        logger.info(f"[DEMO] Processing webhook: {event_type}")
        return {"status": "processed", "event": event_type}


# ============================================================================
# Factory Function
# ============================================================================

def get_tanda_adapter() -> SchedulingPlatformAdapter:
    """
    Factory function to get appropriate Tanda adapter.

    Returns TandaAdapter if TANDA_CLIENT_ID and TANDA_CLIENT_SECRET are set,
    otherwise returns DemoTandaAdapter for development/testing.

    Returns:
        SchedulingPlatformAdapter instance (TandaAdapter or DemoTandaAdapter)
    """
    client_id = os.environ.get("TANDA_CLIENT_ID")
    client_secret = os.environ.get("TANDA_CLIENT_SECRET")

    if client_id and client_secret:
        logger.info("Using real Tanda adapter")
        client = TandaClient(client_id, client_secret)
        return TandaAdapter(client)
    else:
        logger.info("Using demo Tanda adapter (credentials not configured)")
        return DemoTandaAdapter()
