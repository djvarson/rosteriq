"""
Async HTTP adapter for the HumanForce workforce management API.

Handles authentication (OAuth2 with token refresh), rate limiting,
and mapping HumanForce's data format to RosterIQ models.

HumanForce API uses /v1/{resource} pattern.
Base URL: https://api.humanforce.com/v1
OAuth: https://auth.humanforce.com/oauth2/token

Usage:
    async with HumanForceAdapter(credentials) as hf:
        employees = await hf.sync_employees()
        shifts = await hf.sync_shifts(start_date, end_date)
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
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


class HumanForceAPIError(Exception):
    """Exception raised for HumanForce API errors."""

    def __init__(self, api_error: APIError):
        self.api_error = api_error
        super().__init__(f"HumanForce API error {api_error.status_code}: {api_error.message}")


class HumanForceRateLimiter:
    """
    Sliding window rate limiter for HumanForce API.
    HumanForce allows approximately 120 requests per minute.
    """

    def __init__(self, max_requests: int = 120, window_seconds: int = 60):
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
                logger.debug(f"HumanForce rate limit reached, waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            self._clean_old_requests()
        self.requests.append(time.time())

    def _clean_old_requests(self) -> None:
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# Mapping from HumanForce employment types to RosterIQ types
_HF_EMPLOYMENT_MAP = {
    "FullTime": EmploymentType.full_time,
    "Full-Time": EmploymentType.full_time,
    "full_time": EmploymentType.full_time,
    "PartTime": EmploymentType.part_time,
    "Part-Time": EmploymentType.part_time,
    "part_time": EmploymentType.part_time,
    "Casual": EmploymentType.casual,
    "casual": EmploymentType.casual,
    "Contract": EmploymentType.casual,
    "contract": EmploymentType.casual,
}

# Mapping from HumanForce shift statuses
_HF_STATUS_MAP = {
    "draft": ShiftStatus.scheduled,
    "published": ShiftStatus.confirmed,
    "in_progress": ShiftStatus.in_progress,
    "completed": ShiftStatus.completed,
    "cancelled": ShiftStatus.cancelled,
    "open": ShiftStatus.scheduled,
    "accepted": ShiftStatus.confirmed,
}

# HumanForce uses Australian state abbreviations
_HF_STATE_MAP = {
    "VIC": State.vic,
    "NSW": State.nsw,
    "QLD": State.qld,
    "WA": State.wa,
    "SA": State.sa,
    "TAS": State.tas,
    "ACT": State.act,
    "NT": State.nt,
}


@dataclass
class HumanForceCredentials:
    """Stores HumanForce OAuth2 credentials for a venue."""

    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str = ""
    token_expiry: Optional[datetime] = None
    base_url: str = "https://api.humanforce.com/v1"

    def __post_init__(self):
        if self.token_expiry is None:
            self.token_expiry = datetime.now() + timedelta(hours=1)

    @property
    def is_expired(self) -> bool:
        return datetime.now() >= self.token_expiry


class HumanForceOAuth:
    """
    Handles OAuth2 authorization code flow for HumanForce.

    Flow:
    1. Redirect user to get_auth_url()
    2. HumanForce redirects back with ?code=...
    3. Call exchange_code(code) to get access + refresh tokens
    """

    AUTH_URL = "https://auth.humanforce.com/oauth2/authorize"
    TOKEN_URL = "https://auth.humanforce.com/oauth2/token"

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def get_auth_url(self, scope: str = "read write") -> str:
        """Build the URL to redirect the venue owner to for HumanForce authorization."""
        import urllib.parse
        params = urllib.parse.urlencode({
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": scope,
        })
        return f"{self.AUTH_URL}?{params}"

    async def exchange_code(self, code: str) -> HumanForceCredentials:
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
            raise HumanForceAPIError(APIError(
                status_code=response.status_code,
                message=f"Token exchange failed: {response.text}",
                detail={"response": response.text},
            ))

        data = response.json()
        return HumanForceCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", ""),
            token_expiry=datetime.now() + timedelta(
                seconds=data.get("expires_in", 3600)
            ),
        )

    async def refresh_token(self, credentials: HumanForceCredentials) -> HumanForceCredentials:
        """Refresh an expired access token."""
        if not credentials.refresh_token:
            raise HumanForceAPIError(APIError(
                status_code=401,
                message="No refresh token available",
                detail={},
            ))

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.TOKEN_URL, data={
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
                "refresh_token": credentials.refresh_token,
                "grant_type": "refresh_token",
            })

        if response.status_code != 200:
            raise HumanForceAPIError(APIError(
                status_code=response.status_code,
                message=f"Token refresh failed: {response.text}",
                detail={"response": response.text},
            ))

        data = response.json()
        credentials.access_token = data["access_token"]
        if "refresh_token" in data:
            credentials.refresh_token = data["refresh_token"]
        credentials.token_expiry = datetime.now() + timedelta(
            seconds=data.get("expires_in", 3600)
        )
        return credentials


class HumanForceAdapter:
    """
    Async adapter for the HumanForce workforce management API.

    Maps HumanForce resources to RosterIQ models:
    - HumanForce Employee -> RosterIQ Employee
    - HumanForce Shift -> RosterIQ Shift
    - HumanForce Location -> venue context
    """

    MAX_RETRIES = 3
    RETRY_BACKOFF = 1.0  # seconds, doubles each retry

    def __init__(self, credentials: HumanForceCredentials, state: State = State.vic):
        self.credentials = credentials
        self.state = state
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = HumanForceRateLimiter()

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

    async def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Make an authenticated request to the HumanForce API with rate limiting
        and automatic retry with exponential backoff.
        """
        await self._rate_limiter.acquire()

        if self.credentials.is_expired:
            await self._refresh_token()

        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                response = await self._client.request(
                    method=method,
                    url=path,
                    json=data,
                    params=params,
                )
            except httpx.HTTPError as e:
                last_error = HumanForceAPIError(APIError(
                    status_code=0,
                    message=f"HTTP error: {str(e)}",
                    detail={"error": str(e)},
                ))
                if attempt < self.MAX_RETRIES - 1:
                    wait = self.RETRY_BACKOFF * (2 ** attempt)
                    logger.warning(f"HumanForce request failed (attempt {attempt + 1}), retrying in {wait}s: {e}")
                    await asyncio.sleep(wait)
                    continue
                raise last_error

            if response.status_code == 401:
                # Token expired mid-request, refresh and retry
                await self._refresh_token()
                continue

            if response.status_code == 429:
                # Rate limited by HumanForce, back off
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning(f"HumanForce rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                continue

            if response.status_code >= 500 and attempt < self.MAX_RETRIES - 1:
                wait = self.RETRY_BACKOFF * (2 ** attempt)
                logger.warning(
                    f"HumanForce server error {response.status_code} (attempt {attempt + 1}), "
                    f"retrying in {wait}s"
                )
                await asyncio.sleep(wait)
                continue

            if response.status_code >= 400:
                raise HumanForceAPIError(APIError(
                    status_code=response.status_code,
                    message=f"HumanForce API error: {response.text}",
                    detail={"response": response.text, "path": path},
                ))

            return response.json()

        raise last_error or HumanForceAPIError(APIError(
            status_code=0,
            message="Max retries exceeded",
            detail={"path": path},
        ))

    async def _refresh_token(self) -> None:
        """Refresh the OAuth2 access token."""
        if not self.credentials.refresh_token:
            raise HumanForceAPIError(APIError(
                status_code=401,
                message="No refresh token available",
                detail={},
            ))

        async with httpx.AsyncClient(timeout=30.0) as refresh_client:
            response = await refresh_client.post(
                HumanForceOAuth.TOKEN_URL,
                data={
                    "client_id": self.credentials.client_id,
                    "client_secret": self.credentials.client_secret,
                    "refresh_token": self.credentials.refresh_token,
                    "grant_type": "refresh_token",
                },
            )

        if response.status_code != 200:
            raise HumanForceAPIError(APIError(
                status_code=response.status_code,
                message="Token refresh failed",
                detail={"response": response.text},
            ))

        data = response.json()
        self.credentials.access_token = data["access_token"]
        if "refresh_token" in data:
            self.credentials.refresh_token = data["refresh_token"]
        self.credentials.token_expiry = datetime.now() + timedelta(
            seconds=data.get("expires_in", 3600)
        )

        # Update client headers
        self._client.headers["Authorization"] = f"Bearer {self.credentials.access_token}"
        logger.info("HumanForce access token refreshed successfully")

    # -- Employee Methods -----------------------------------------------------

    async def sync_employees(self, active_only: bool = True) -> List[Employee]:
        """Fetch all employees from HumanForce and map to RosterIQ Employee model."""
        params: Dict[str, Any] = {
            "page_size": 200,
            "page": 1,
        }
        if active_only:
            params["status"] = "active"

        all_employees: List[Employee] = []
        while True:
            data = await self._make_request("GET", "/employees", params=params)
            records = data.get("data", data) if isinstance(data, dict) else data
            if not records:
                break

            for emp in records:
                all_employees.append(self._map_employee(emp))

            # Handle pagination
            if isinstance(data, dict) and data.get("has_more"):
                params["page"] += 1
            else:
                break

        logger.info(f"Synced {len(all_employees)} employees from HumanForce")
        return all_employees

    def _map_employee(self, data: Dict[str, Any]) -> Employee:
        """Map a HumanForce employee record to RosterIQ Employee model."""
        emp_type = _HF_EMPLOYMENT_MAP.get(
            data.get("employment_type", "Casual"), EmploymentType.casual
        )

        hourly_rate = Decimal(str(data.get("hourly_rate", 0) or data.get("pay_rate", 0) or 0))

        skills = []
        if data.get("qualifications"):
            for qual in data["qualifications"]:
                if isinstance(qual, str):
                    skills.append(qual)
                elif isinstance(qual, dict) and qual.get("name"):
                    skills.append(qual["name"])
        if data.get("roles"):
            for role in data["roles"]:
                if isinstance(role, str):
                    skills.append(role)
                elif isinstance(role, dict) and role.get("name"):
                    skills.append(role["name"])

        return Employee(
            id=str(data.get("id", "")),
            external_id=str(data.get("id", "")),
            external_source="humanforce",
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            email=data.get("email", ""),
            phone=data.get("mobile", "") or data.get("phone", ""),
            employment_type=emp_type,
            award_level=AwardLevel.level_1,  # Default, refine from pay conditions
            hourly_rate=hourly_rate,
            skills=skills,
            is_active=data.get("status", "active") == "active",
            venue_id=str(data.get("location_id", "") or data.get("organisation_id", "")),
        )

    # -- Shift / Schedule Methods ---------------------------------------------

    async def sync_shifts(
        self,
        start_date: date,
        end_date: date,
        location_id: Optional[str] = None,
    ) -> List[Shift]:
        """Fetch shifts/schedules within a date range from HumanForce."""
        params: Dict[str, Any] = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page_size": 200,
            "page": 1,
        }
        if location_id:
            params["location_id"] = location_id

        all_shifts: List[Shift] = []
        while True:
            data = await self._make_request("GET", "/shifts", params=params)
            records = data.get("data", data) if isinstance(data, dict) else data
            if not records:
                break

            for shift_data in records:
                all_shifts.append(self._map_shift(shift_data))

            if isinstance(data, dict) and data.get("has_more"):
                params["page"] += 1
            else:
                break

        logger.info(
            f"Synced {len(all_shifts)} shifts from HumanForce "
            f"({start_date} to {end_date})"
        )
        return all_shifts

    def _map_shift(self, data: Dict[str, Any]) -> Shift:
        """Map a HumanForce shift record to RosterIQ Shift model."""
        start_time = datetime.fromisoformat(
            data.get("start_time", datetime.now().isoformat())
        )
        end_time = datetime.fromisoformat(
            data.get("end_time", (datetime.now() + timedelta(hours=4)).isoformat())
        )

        status = _HF_STATUS_MAP.get(
            data.get("status", "draft"), ShiftStatus.scheduled
        )

        return Shift(
            id=str(data.get("id", "")),
            external_id=str(data.get("id", "")),
            external_source="humanforce",
            employee_id=str(data.get("employee_id", "")),
            venue_id=str(data.get("location_id", "") or data.get("organisation_id", "")),
            department=data.get("department_name", "") or data.get("area_name", ""),
            start_time=start_time,
            end_time=end_time,
            break_minutes=int(data.get("break_duration_minutes", 0) or data.get("break_minutes", 0) or 0),
            status=status,
            hourly_rate=Decimal(str(data.get("cost_per_hour", 0) or data.get("pay_rate", 0) or 0)),
            notes=data.get("notes", "") or data.get("comments", ""),
        )

    # -- Push Roster ----------------------------------------------------------

    async def push_roster(
        self,
        roster: Roster,
        location_id: str,
        publish: bool = True,
        notify_employees: bool = True,
    ) -> Dict[str, Any]:
        """
        Push an optimised roster back to HumanForce.

        Creates or updates shifts in HumanForce based on the RosterIQ roster,
        then optionally publishes and notifies staff.
        """
        shifts_payload = []
        for shift in roster.shifts:
            shifts_payload.append({
                "employee_id": shift.employee_id,
                "location_id": location_id,
                "department_name": shift.department,
                "start_time": shift.start_time.isoformat(),
                "end_time": shift.end_time.isoformat(),
                "break_duration_minutes": shift.break_minutes,
                "notes": shift.notes or "",
            })

        result = await self._make_request(
            "POST",
            "/rosters",
            data={
                "location_id": location_id,
                "shifts": shifts_payload,
                "publish": publish,
                "notify_employees": notify_employees,
                "source": "rosteriq",
            },
        )

        logger.info(
            f"Pushed roster with {len(shifts_payload)} shifts to HumanForce "
            f"(location {location_id}, publish={publish})"
        )
        return result

    # -- Timesheet Methods ----------------------------------------------------

    async def get_timesheets(
        self,
        start_date: date,
        end_date: date,
        employee_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch timesheet data (actual clock-in/out records) from HumanForce."""
        params: Dict[str, Any] = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "page_size": 200,
            "page": 1,
        }
        if employee_id:
            params["employee_id"] = employee_id

        all_timesheets: List[Dict[str, Any]] = []
        while True:
            data = await self._make_request("GET", "/timesheets", params=params)
            records = data.get("data", data) if isinstance(data, dict) else data
            if not records:
                break

            all_timesheets.extend(records)

            if isinstance(data, dict) and data.get("has_more"):
                params["page"] += 1
            else:
                break

        logger.info(
            f"Fetched {len(all_timesheets)} timesheets from HumanForce "
            f"({start_date} to {end_date})"
        )
        return all_timesheets

    # -- Location Methods -----------------------------------------------------

    async def get_locations(self) -> List[Dict[str, Any]]:
        """Fetch all venue locations from HumanForce."""
        params: Dict[str, Any] = {
            "page_size": 100,
            "status": "active",
        }
        data = await self._make_request("GET", "/locations", params=params)
        records = data.get("data", data) if isinstance(data, dict) else data
        logger.info(f"Fetched {len(records)} locations from HumanForce")
        return records

    # -- Utility Methods ------------------------------------------------------

    async def get_me(self) -> Dict[str, Any]:
        """Get the authenticated user's info -- useful for testing connectivity."""
        return await self._make_request("GET", "/me")

    async def test_connection(self) -> bool:
        """Test if the HumanForce connection is working."""
        try:
            await self.get_me()
            return True
        except HumanForceAPIError:
            return False
