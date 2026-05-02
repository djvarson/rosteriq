"""
Async HTTP adapter for the Deputy workforce management API.

Handles authentication (OAuth2 with token refresh), rate limiting,
and mapping Deputy's data format to RosterIQ models.

Deputy API uses /api/v1/resource/{Resource} pattern.
Base URL: https://{subdomain}.au.deputy.com/api/v1/

Usage:
    async with DeputyAdapter(credentials) as deputy:
        employees = await deputy.get_employees()
        shifts = await deputy.get_shifts(start_date, end_date)
"""

import asyncio
import time
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any

import httpx

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    APIError,
    EmploymentType,
    AwardLevel,
    ShiftStatus,
    State,
)

logger = logging.getLogger(__name__)


class DeputyAPIError(Exception):
    """Exception raised for Deputy API errors."""

    def __init__(self, api_error: APIError):
        self.api_error = api_error
        super().__init__(f"Deputy API error {api_error.status_code}: {api_error.message}")


class DeputyRateLimiter:
    """
    Token bucket rate limiter for Deputy API.
    Deputy allows approximately 60 requests per minute.
    """

    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: list[float] = []

    async def acquire(self) -> None:
        """Wait if necessary to stay within rate limits."""
        self._clean_old_requests()
        if len(self.requests) >= self.max_requests:
            oldest = self.requests[0]
            wait_time = self.window_seconds - (time.time() - oldest)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._clean_old_requests()
        self.requests.append(time.time())

    def _clean_old_requests(self) -> None:
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# Mapping from Deputy employment types to RosterIQ types
_DEPUTY_EMPLOYMENT_MAP = {
    1: EmploymentType.full_time,   # Full-time
    2: EmploymentType.part_time,   # Part-time
    3: EmploymentType.casual,      # Casual
    4: EmploymentType.casual,      # Contract (map to casual)
}

# Mapping from Deputy shift statuses
_DEPUTY_STATUS_MAP = {
    0: ShiftStatus.scheduled,     # Open
    1: ShiftStatus.confirmed,     # Accepted
    2: ShiftStatus.in_progress,   # In progress
    3: ShiftStatus.completed,     # Completed
    4: ShiftStatus.cancelled,     # Cancelled
}

# Deputy uses numeric state/territory codes
_DEPUTY_STATE_MAP = {
    "VIC": State.vic,
    "NSW": State.nsw,
    "QLD": State.qld,
    "WA": State.wa,
    "SA": State.sa,
    "TAS": State.tas,
    "ACT": State.act,
    "NT": State.nt,
}


class DeputyCredentials:
    """Stores Deputy OAuth2 credentials for a venue."""

    def __init__(
        self,
        subdomain: str,
        client_id: str,
        client_secret: str,
        access_token: str,
        refresh_token: str = "",
        token_expires_at: Optional[datetime] = None,
    ):
        self.subdomain = subdomain
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expires_at = token_expires_at or (datetime.now() + timedelta(hours=24))

    @property
    def base_url(self) -> str:
        return f"https://{self.subdomain}.au.deputy.com/api/v1"

    @property
    def is_expired(self) -> bool:
        return datetime.now() >= self.token_expires_at


class DeputyOAuth:
    """
    Handles OAuth2 authorization code flow for Deputy.

    Flow:
    1. Redirect user to get_authorize_url()
    2. Deputy redirects back with ?code=...
    3. Call exchange_code(code) to get access + refresh tokens
    """

    AUTHORIZE_URL = "https://once.deputy.com/my/oauth/login"
    TOKEN_URL = "https://once.deputy.com/my/oauth/access_token"

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def get_authorize_url(self, scope: str = "longlife_refresh_token") -> str:
        """Build the URL to redirect the venue owner to for Deputy authorization."""
        import urllib.parse
        params = urllib.parse.urlencode({
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": scope,
        })
        return f"{self.AUTHORIZE_URL}?{params}"

    async def exchange_code(self, code: str, subdomain: str) -> DeputyCredentials:
        """Exchange an authorization code for access and refresh tokens."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.TOKEN_URL, data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "code": code,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
            })

        if response.status_code != 200:
            raise DeputyAPIError(APIError(
                status_code=response.status_code,
                message=f"Token exchange failed: {response.text}",
                detail={"response": response.text},
            ))

        data = response.json()
        return DeputyCredentials(
            subdomain=subdomain,
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", ""),
            token_expires_at=datetime.now() + timedelta(
                seconds=data.get("expires_in", 86400)
            ),
        )


class DeputyAdapter:
    """
    Async adapter for the Deputy workforce management API.

    Maps Deputy resources to RosterIQ models:
    - Deputy Employee → RosterIQ Employee
    - Deputy Roster (shift) → RosterIQ Shift
    - Deputy Location/Area → venue/department context
    """

    def __init__(self, credentials: DeputyCredentials, state: State = State.vic):
        self.credentials = credentials
        self.state = state
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = DeputyRateLimiter()

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=self.credentials.base_url,
            headers={
                "Authorization": f"Bearer {self.credentials.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=30.0,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make an authenticated request to the Deputy API with rate limiting."""
        await self._rate_limiter.acquire()

        if self.credentials.is_expired:
            await self._refresh_token()

        try:
            response = await self._client.request(
                method=method,
                url=path,
                json=data,
                params=params,
            )
        except httpx.HTTPError as e:
            raise DeputyAPIError(APIError(
                status_code=0,
                message=f"HTTP error: {str(e)}",
                detail={"error": str(e)},
            ))

        if response.status_code == 401:
            # Token expired mid-request, refresh and retry
            await self._refresh_token()
            response = await self._client.request(
                method=method,
                url=path,
                json=data,
                params=params,
            )

        if response.status_code >= 400:
            raise DeputyAPIError(APIError(
                status_code=response.status_code,
                message=f"Deputy API error: {response.text}",
                detail={"response": response.text, "path": path},
            ))

        return response.json()

    async def _refresh_token(self) -> None:
        """Refresh the OAuth2 access token."""
        if not self.credentials.refresh_token:
            raise DeputyAPIError(APIError(
                status_code=401,
                message="No refresh token available",
                detail={},
            ))

        async with httpx.AsyncClient(timeout=30.0) as refresh_client:
            response = await refresh_client.post(
                DeputyOAuth.TOKEN_URL,
                data={
                    "client_id": self.credentials.client_id,
                    "client_secret": self.credentials.client_secret,
                    "refresh_token": self.credentials.refresh_token,
                    "grant_type": "refresh_token",
                },
            )

        if response.status_code != 200:
            raise DeputyAPIError(APIError(
                status_code=response.status_code,
                message="Token refresh failed",
                detail={"response": response.text},
            ))

        data = response.json()
        self.credentials.access_token = data["access_token"]
        if "refresh_token" in data:
            self.credentials.refresh_token = data["refresh_token"]
        self.credentials.token_expires_at = datetime.now() + timedelta(
            seconds=data.get("expires_in", 86400)
        )

        # Update client headers
        self._client.headers["Authorization"] = f"Bearer {self.credentials.access_token}"
        logger.info("Deputy access token refreshed successfully")

    # ── Employee Methods ─────────────────────────────────────────────

    async def get_employees(self, active_only: bool = True) -> List[Employee]:
        """Fetch all employees from Deputy and map to RosterIQ Employee model."""
        search_filter = {"Active": 1} if active_only else {}
        data = await self._request(
            "POST",
            "/resource/Employee/QUERY",
            data={
                "search": search_filter,
                "sort": {"LastName": "asc"},
                "max": 500,
            },
        )

        employees = []
        for emp in data:
            employees.append(self._map_employee(emp))
        return employees

    async def get_employee(self, employee_id: int) -> Employee:
        """Fetch a single employee by ID."""
        data = await self._request("GET", f"/resource/Employee/{employee_id}")
        return self._map_employee(data)

    def _map_employee(self, data: Dict[str, Any]) -> Employee:
        """Map a Deputy employee record to RosterIQ Employee model."""
        emp_type = _DEPUTY_EMPLOYMENT_MAP.get(
            data.get("EmpType", 3), EmploymentType.casual
        )

        # Deputy stores hourly rate in EmployeeAgreement, default to 0
        hourly_rate = Decimal(str(data.get("HourlyRate", 0) or 0))

        return Employee(
            id=str(data["Id"]),
            external_id=str(data["Id"]),
            external_source="deputy",
            first_name=data.get("FirstName", ""),
            last_name=data.get("LastName", ""),
            email=data.get("Email", ""),
            phone=data.get("Phone", ""),
            employment_type=emp_type,
            award_level=AwardLevel.level_1,  # Default, refine from agreement
            hourly_rate=hourly_rate,
            skills=self._extract_skills(data),
            is_active=bool(data.get("Active", True)),
            venue_id=str(data.get("Company", "")),
        )

    def _extract_skills(self, data: Dict[str, Any]) -> List[str]:
        """Extract employee skills/training from Deputy data."""
        skills = []
        # Deputy stores training records separately
        # Map common role fields
        if data.get("Role"):
            role = data["Role"]
            if isinstance(role, str):
                skills.append(role)
            elif isinstance(role, dict) and role.get("ReportingName"):
                skills.append(role["ReportingName"])
        return skills

    # ── Shift / Roster Methods ───────────────────────────────────────

    async def get_shifts(
        self,
        start_date: date,
        end_date: date,
        location_id: Optional[int] = None,
    ) -> List[Shift]:
        """Fetch shifts (rosters) within a date range."""
        search_filter = {
            "StartTime": {
                "gte": datetime.combine(start_date, datetime.min.time()).isoformat(),
            },
            "EndTime": {
                "lte": datetime.combine(end_date, datetime.max.time()).isoformat(),
            },
        }
        if location_id:
            search_filter["OperationalUnit"] = location_id

        data = await self._request(
            "POST",
            "/resource/Roster/QUERY",
            data={
                "search": search_filter,
                "sort": {"StartTime": "asc"},
                "max": 500,
            },
        )

        shifts = []
        for roster_item in data:
            shifts.append(self._map_shift(roster_item))
        return shifts

    def _map_shift(self, data: Dict[str, Any]) -> Shift:
        """Map a Deputy roster item to RosterIQ Shift model."""
        start_time = datetime.fromisoformat(
            data.get("StartTime", datetime.now().isoformat())
        )
        end_time = datetime.fromisoformat(
            data.get("EndTime", (datetime.now() + timedelta(hours=4)).isoformat())
        )

        status = _DEPUTY_STATUS_MAP.get(
            data.get("ConfirmStatus", 0), ShiftStatus.scheduled
        )

        return Shift(
            id=str(data["Id"]),
            external_id=str(data["Id"]),
            external_source="deputy",
            employee_id=str(data.get("Employee", "")),
            venue_id=str(data.get("Company", "")),
            department=data.get("OperationalUnitName", ""),
            start_time=start_time,
            end_time=end_time,
            break_minutes=int(data.get("Mealbreak", 0) or 0),
            status=status,
            hourly_rate=Decimal(str(data.get("Cost", 0) or 0)),
            notes=data.get("Comment", ""),
        )

    async def create_shift(
        self,
        employee_id: int,
        start_time: datetime,
        end_time: datetime,
        location_id: int,
        area_id: Optional[int] = None,
        comment: str = "",
    ) -> Shift:
        """Create a new shift (roster entry) in Deputy."""
        data = await self._request(
            "POST",
            "/resource/Roster",
            data={
                "Employee": employee_id,
                "StartTime": start_time.isoformat(),
                "EndTime": end_time.isoformat(),
                "OperationalUnit": location_id,
                "Comment": comment,
                "Warning": "",
                "WarningOverrideComment": "",
                "Published": False,
            },
        )
        return self._map_shift(data)

    async def publish_roster(
        self,
        start_date: date,
        end_date: date,
        location_id: int,
        notify_employees: bool = True,
    ) -> Dict[str, Any]:
        """Publish a roster for a date range, optionally notifying staff."""
        return await self._request(
            "POST",
            "/resource/Roster/PUBLISH",
            data={
                "intStartTimestamp": int(
                    datetime.combine(start_date, datetime.min.time()).timestamp()
                ),
                "intEndTimestamp": int(
                    datetime.combine(end_date, datetime.max.time()).timestamp()
                ),
                "intOpunitId": location_id,
                "blnNotify": notify_employees,
            },
        )

    # ── Timesheet Methods ────────────────────────────────────────────

    async def get_timesheets(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch timesheets (actual clock-in/out records)."""
        search_filter = {
            "Date": {
                "gte": start_date.isoformat(),
                "lte": end_date.isoformat(),
            },
        }
        if employee_id:
            search_filter["Employee"] = employee_id

        return await self._request(
            "POST",
            "/resource/Timesheet/QUERY",
            data={
                "search": search_filter,
                "sort": {"Date": "asc"},
                "max": 500,
            },
        )

    # ── Location / Area Methods ──────────────────────────────────────

    async def get_locations(self) -> List[Dict[str, Any]]:
        """Fetch all locations (venues/sites)."""
        return await self._request(
            "POST",
            "/resource/Company/QUERY",
            data={"search": {"Active": 1}, "max": 100},
        )

    async def get_areas(self, location_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch operational units (departments/areas) for a location."""
        search_filter = {"Active": 1}
        if location_id:
            search_filter["Company"] = location_id

        return await self._request(
            "POST",
            "/resource/OperationalUnit/QUERY",
            data={"search": search_filter, "max": 100},
        )

    # ── Webhook Registration ─────────────────────────────────────────

    async def register_webhook(
        self,
        topic: str,
        callback_url: str,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """
        Register a webhook with Deputy.

        Topics: Roster.*, Timesheet.*, Employee.*
        """
        return await self._request(
            "POST",
            "/resource/Webhook",
            data={
                "Topic": topic,
                "Enabled": enabled,
                "Type": "URL",
                "Address": callback_url,
            },
        )

    async def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all registered webhooks."""
        return await self._request(
            "POST",
            "/resource/Webhook/QUERY",
            data={"search": {}, "max": 100},
        )

    # ── Utility Methods ──────────────────────────────────────────────

    async def get_me(self) -> Dict[str, Any]:
        """Get the authenticated user's info — useful for testing connectivity."""
        return await self._request("GET", "/me")

    async def test_connection(self) -> bool:
        """Test if the Deputy connection is working."""
        try:
            await self.get_me()
            return True
        except DeputyAPIError:
            return False
