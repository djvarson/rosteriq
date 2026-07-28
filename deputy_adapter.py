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
        # Permanent tokens never expire — use far-future sentinel
        if token_expires_at is not None:
            self.token_expires_at = token_expires_at
        elif refresh_token:
            # OAuth token with refresh — default 24h until we know better
            self.token_expires_at = datetime.now() + timedelta(hours=24)
        else:
            # Permanent token — never expires
            self.token_expires_at = datetime(2099, 12, 31)

    @property
    def is_permanent(self) -> bool:
        """True if this is a permanent token (no refresh available)."""
        return not self.refresh_token or self.client_id == "permanent_token"

    @property
    def base_url(self) -> str:
        return f"https://{self.subdomain}.au.deputy.com/api/v1"

    @property
    def is_expired(self) -> bool:
        if self.is_permanent:
            return False  # Permanent tokens don't expire
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

    def get_authorize_url(self, scope: str = "longlife_refresh_token", state: str = "") -> str:
        """Build the URL to redirect the venue owner to for Deputy authorization.

        Args:
            scope: OAuth scope (default: longlife_refresh_token for long-lived tokens)
            state: Opaque state string passed through the OAuth flow (carries venue_id etc.)
        """
        import urllib.parse
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": scope,
        }
        if state:
            params["state"] = state
        return f"{self.AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

    async def exchange_code(self, code: str, subdomain: str = "") -> DeputyCredentials:
        """Exchange an authorization code for access and refresh tokens.

        Args:
            code: Authorization code from Deputy callback.
            subdomain: Deputy subdomain. If empty, auto-detected from the
                       ``endpoint`` field in the token response.
        """
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

        # Auto-detect subdomain from the endpoint field if not provided
        if not subdomain and data.get("endpoint"):
            import re
            endpoint = data["endpoint"].rstrip("/")
            # e.g. "https://mycompany.au.deputy.com" → "mycompany"
            m = re.match(r'https?://([^.]+)', endpoint)
            if m:
                subdomain = m.group(1)
            else:
                subdomain = endpoint

        if not subdomain:
            raise DeputyAPIError(APIError(
                status_code=0,
                message="Could not determine Deputy subdomain from token response. "
                        "Please reconnect and provide your subdomain.",
                detail={"response_keys": list(data.keys())},
            ))

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


# Transient HTTP status codes that should trigger a retry
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0  # seconds — 1s, 2s, 4s


class DeputyAdapter:
    """
    Async adapter for the Deputy workforce management API.

    Maps Deputy resources to RosterIQ models:
    - Deputy Employee → RosterIQ Employee
    - Deputy Roster (shift) → RosterIQ Shift
    - Deputy Location/Area → venue/department context
    """

    def __init__(
        self,
        credentials: DeputyCredentials,
        state: State = State.vic,
        on_token_refresh=None,
    ):
        self.credentials = credentials
        self.state = state
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = DeputyRateLimiter()
        self._on_token_refresh = on_token_refresh  # callback(credentials) to persist new tokens
        # Deputy employee ids whose pay rate wasn't provided by the API and got
        # the flagged DEFAULT_SYNC_RATE placeholder (reset on each get_employees).
        self._rate_review_ids: List[str] = []

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=self.credentials.base_url,
            headers={
                "Authorization": f"Bearer {self.credentials.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0),
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
        """Make an authenticated request to the Deputy API with rate limiting and retry."""
        await self._rate_limiter.acquire()

        if self.credentials.is_expired:
            await self._refresh_token()

        last_error = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = await self._client.request(
                    method=method,
                    url=path,
                    json=data,
                    params=params,
                )
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                    httpx.PoolTimeout, httpx.ConnectTimeout) as e:
                last_error = e
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        f"Deputy request to {path} failed (attempt {attempt + 1}/"
                        f"{_MAX_RETRIES + 1}): {e}. Retrying in {wait:.1f}s..."
                    )
                    await asyncio.sleep(wait)
                    continue
                raise DeputyAPIError(APIError(
                    status_code=0,
                    message=f"Deputy unreachable after {_MAX_RETRIES + 1} attempts: {e}",
                    detail={"error": str(e), "path": path, "attempts": _MAX_RETRIES + 1},
                ))
            except httpx.HTTPError as e:
                raise DeputyAPIError(APIError(
                    status_code=0,
                    message=f"HTTP error: {str(e)}",
                    detail={"error": str(e)},
                ))

            # Handle 401 — token expired mid-request
            if response.status_code == 401 and not self.credentials.is_permanent:
                try:
                    await self._refresh_token()
                    response = await self._client.request(
                        method=method, url=path, json=data, params=params,
                    )
                except DeputyAPIError:
                    raise DeputyAPIError(APIError(
                        status_code=401,
                        message="Deputy authentication failed — token expired and refresh failed. Please reconnect.",
                        detail={"path": path},
                    ))
            elif response.status_code == 401 and self.credentials.is_permanent:
                raise DeputyAPIError(APIError(
                    status_code=401,
                    message="Deputy authentication failed — permanent token rejected. Generate a new token from Deputy admin.",
                    detail={"path": path},
                ))

            # Retry on transient server errors
            if response.status_code in _RETRYABLE_STATUSES and attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                # For 429, respect Retry-After header if present
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait = max(wait, int(retry_after))
                logger.warning(
                    f"Deputy {response.status_code} on {path} (attempt {attempt + 1}/"
                    f"{_MAX_RETRIES + 1}). Retrying in {wait:.1f}s..."
                )
                await asyncio.sleep(wait)
                continue

            if response.status_code >= 400:
                raise DeputyAPIError(APIError(
                    status_code=response.status_code,
                    message=f"Deputy API error: {response.text}",
                    detail={"response": response.text, "path": path},
                ))

            return response.json()

        # Should not reach here, but safety net
        raise DeputyAPIError(APIError(
            status_code=0,
            message=f"Deputy request to {path} failed after all retries",
            detail={"last_error": str(last_error)},
        ))

    async def _request_paginated(
        self,
        path: str,
        data: Dict[str, Any],
        page_size: int = 500,
        max_pages: int = 20,
    ) -> List[Any]:
        """
        Make a paginated QUERY request, fetching all pages.
        Deputy QUERY endpoints support 'start' + 'max' for pagination.
        """
        all_results = []
        for page in range(max_pages):
            page_data = {**data, "start": page * page_size, "max": page_size}
            results = await self._request("POST", path, data=page_data)

            if not isinstance(results, list):
                # Single-object response or error
                return [results] if results else []

            all_results.extend(results)

            # If we got fewer than page_size, we've reached the end
            if len(results) < page_size:
                break
        else:
            logger.warning(
                f"Deputy pagination hit max_pages ({max_pages}) for {path} — "
                f"some records may be missing. Total fetched: {len(all_results)}"
            )

        return all_results

    async def _refresh_token(self) -> None:
        """Refresh the OAuth2 access token."""
        if self.credentials.is_permanent:
            logger.debug("Permanent token — skipping refresh")
            return

        if not self.credentials.refresh_token:
            raise DeputyAPIError(APIError(
                status_code=401,
                message="No refresh token available — please reconnect Deputy.",
                detail={},
            ))

        try:
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
        except httpx.HTTPError as e:
            raise DeputyAPIError(APIError(
                status_code=0,
                message=f"Token refresh request failed: {e}",
                detail={"error": str(e)},
            ))

        if response.status_code != 200:
            raise DeputyAPIError(APIError(
                status_code=response.status_code,
                message="Token refresh failed — please reconnect Deputy.",
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

        # Persist refreshed tokens if a callback was provided
        if self._on_token_refresh:
            try:
                self._on_token_refresh(self.credentials)
            except Exception as e:
                logger.warning(f"Failed to persist refreshed Deputy token: {e}")

    # ── Employee Methods ─────────────────────────────────────────────

    async def get_employees(self, active_only: bool = True) -> List[Employee]:
        """Fetch all employees from Deputy and map to RosterIQ Employee model."""
        self._rate_review_ids = []  # fresh per sync
        # Deputy QUERY search syntax: named clauses of {field, type, data}.
        # The previous {"Active": 1} shape was silently ignored by Deputy.
        search_filter = (
            {"s1": {"field": "Active", "type": "eq", "data": True}} if active_only else {}
        )
        data = await self._request_paginated(
            "/resource/Employee/QUERY",
            data={
                "search": search_filter,
                "sort": {"LastName": "asc"},
            },
        )

        employees = []
        failed = 0
        for emp in data:
            try:
                employees.append(self._map_employee(emp))
            except Exception as e:
                failed += 1
                logger.warning(f"Failed to map Deputy employee {emp.get('Id', '?')}: {e}")
        if failed:
            logger.warning(f"Skipped {failed}/{len(data)} employees due to mapping errors")
        return employees

    async def get_employee(self, employee_id: int) -> Employee:
        """Fetch a single employee by ID."""
        data = await self._request("GET", f"/resource/Employee/{employee_id}")
        return self._map_employee(data)

    # Placeholder when Deputy provides no pay rate (rates live in
    # EmployeeAgreement, which the basic Employee resource often omits).
    # Every such employee is flagged in employees_needing_rate_review so the
    # sync response / operator can correct rates before trusting cost numbers.
    DEFAULT_SYNC_RATE = Decimal("30.00")

    def _map_employee(self, data: Dict[str, Any]) -> Employee:
        """Map a Deputy employee record to the RosterIQ Employee model."""
        emp_type = _DEPUTY_EMPLOYMENT_MAP.get(
            data.get("EmpType", 3), EmploymentType.casual
        )

        # Deputy stores hourly rate in EmployeeAgreement; the Employee resource
        # often has no rate. The model requires rate > 0, so use a flagged
        # placeholder rather than dropping the person from the sync.
        try:
            hourly_rate = Decimal(str(data.get("HourlyRate", 0) or 0))
        except Exception:
            hourly_rate = Decimal("0")
        if hourly_rate <= 0:
            hourly_rate = self.DEFAULT_SYNC_RATE
            self._rate_review_ids.append(str(data.get("Id", "?")))

        name = " ".join(
            p for p in (data.get("FirstName", ""), data.get("LastName", "")) if p
        ).strip() or str(data.get("DisplayName") or f"Deputy employee {data['Id']}")

        now = datetime.utcnow()
        # Prefix the id so a synced employee can never collide with a manually
        # created one, and re-syncing the same person upserts (idempotent).
        return Employee(
            id=f"deputy-{data['Id']}",
            name=name,
            email=data.get("Email") or None,
            phone=str(data.get("Phone") or "") or None,
            employment_type=emp_type,
            award_level=AwardLevel.level_1,  # Default, refine from agreement
            state=self.state,
            hourly_base_rate=hourly_rate,
            skills=self._extract_skills(data),
            created_at=now,
            updated_at=now,
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
        # Deputy QUERY search uses named clauses of {field, type, data} with
        # epoch-second values — both discovered in the live rehearsal against a
        # real install (mongo-style {field: {op: val}} and ISO datetimes are
        # rejected with "Invalid search parameter").
        start_epoch = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_epoch = int(datetime.combine(end_date, datetime.max.time()).timestamp())
        search_filter = {
            "s1": {"field": "StartTime", "type": "ge", "data": start_epoch},
            "s2": {"field": "EndTime", "type": "le", "data": end_epoch},
        }
        if location_id:
            search_filter["s3"] = {"field": "OperationalUnit", "type": "eq", "data": location_id}

        data = await self._request_paginated(
            "/resource/Roster/QUERY",
            data={
                "search": search_filter,
                "sort": {"StartTime": "asc"},
            },
        )

        shifts = []
        failed = 0
        for roster_item in data:
            try:
                shifts.append(self._map_shift(roster_item))
            except Exception as e:
                failed += 1
                logger.warning(f"Failed to map Deputy shift {roster_item.get('Id', '?')}: {e}")
        if failed:
            logger.warning(f"Skipped {failed}/{len(data)} shifts due to mapping errors")
        return shifts

    def _map_shift(self, data: Dict[str, Any]) -> Shift:
        """Map a Deputy roster item to RosterIQ Shift model."""
        raw_start = data.get("StartTime")
        raw_end = data.get("EndTime")

        # Deputy may return timestamps as epoch integers or ISO strings
        def _parse_dt(val, fallback_label: str) -> datetime:
            if isinstance(val, (int, float)) and val > 0:
                return datetime.fromtimestamp(val)
            if isinstance(val, str) and val:
                try:
                    return datetime.fromisoformat(val)
                except ValueError:
                    logger.warning(f"Unparseable Deputy {fallback_label}: {val!r}")
            raise ValueError(f"Missing or invalid {fallback_label} in Deputy shift {data.get('Id', '?')}")

        start_time = _parse_dt(raw_start, "StartTime")
        end_time = _parse_dt(raw_end, "EndTime")

        status = _DEPUTY_STATUS_MAP.get(
            data.get("ConfirmStatus", 0), ShiftStatus.scheduled
        )

        # Deputy Mealbreak is a datetime-ish value, not minutes — derive minutes
        # only when it's a plain number; otherwise default to 0 (conservative).
        raw_break = data.get("Mealbreak", 0)
        break_minutes = int(raw_break) if isinstance(raw_break, (int, float)) and 0 <= raw_break < 24 * 60 else 0

        raw_cost = data.get("Cost", 0) or 0
        try:
            cost = Decimal(str(raw_cost))
        except Exception:
            cost = Decimal("0")

        # Prefix the id so a synced shift can never collide with a RosterIQ-
        # generated one, and re-syncing the same shift upserts (idempotent).
        return Shift(
            id=f"deputy-{data['Id']}",
            employee_id=f"deputy-{data.get('Employee', '')}",
            date=start_time.date(),
            start_time=start_time.time(),
            end_time=end_time.time(),
            break_minutes=break_minutes,
            status=status,
            role=str(data.get("OperationalUnitName") or "general"),
            cost=cost if cost > 0 else None,
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
