"""
Async HTTP adapter for the Tanda workforce management API.

Handles authentication (OAuth2 with token refresh), rate limiting (100 req/min),
and mapping Tanda's data format to RosterIQ models.

Usage:
    async with TandaAdapter(credentials, state=State.vic) as tanda:
        employees = await tanda.get_employees()
        shifts = await tanda.get_shifts(start_date, end_date)
"""

import asyncio
import time
from datetime import date, datetime, time as time_type, timedelta
from zoneinfo import ZoneInfo

# Map an Australian state to its IANA timezone, so Tanda Unix timestamps are
# resolved to the venue's LOCAL date/hour. Parsing epochs in server-local time
# (the previous behaviour) put AU shifts on the wrong day/hour on a UTC host,
# corrupting day-of-week/time-of-day penalty rates (Sat/Sun/PH/evening).
_STATE_TZ = {
    "vic": "Australia/Melbourne",
    "nsw": "Australia/Sydney",
    "act": "Australia/Sydney",
    "tas": "Australia/Hobart",
    "qld": "Australia/Brisbane",
    "sa": "Australia/Adelaide",
    "nt": "Australia/Darwin",
    "wa": "Australia/Perth",
}
from decimal import Decimal
from typing import Optional

import httpx

from rosteriq.models import (
    Employee,
    Shift,
    Roster,
    TandaCredentials,
    APIError,
    EmploymentType,
    AwardLevel,
    ShiftStatus,
    State,
)


class TandaAPIError(Exception):
    """Exception raised for Tanda API errors."""

    def __init__(self, api_error: APIError):
        self.api_error = api_error
        super().__init__(f"Tanda API error {api_error.status_code}: {api_error.message}")


class RateLimiter:
    """
    Token bucket rate limiter for Tanda API.

    Tanda allows 100 requests per 60-second window.
    """

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: list[float] = []

    async def acquire(self) -> None:
        """Wait if necessary to stay within rate limits."""
        self._clean_old_requests()

        if len(self.requests) >= self.max_requests:
            # Calculate how long to wait
            oldest = self.requests[0]
            wait_time = self.window_seconds - (time.time() - oldest)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._clean_old_requests()

        self.requests.append(time.time())

    def _clean_old_requests(self) -> None:
        """Remove request timestamps outside the current window."""
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# Mapping from Tanda employment types to ours
_TANDA_EMPLOYMENT_MAP = {
    "full_time": EmploymentType.full_time,
    "full-time": EmploymentType.full_time,
    "part_time": EmploymentType.part_time,
    "part-time": EmploymentType.part_time,
    "casual": EmploymentType.casual,
}

# Mapping from Tanda shift statuses to ours
_TANDA_STATUS_MAP = {
    "pending": ShiftStatus.scheduled,
    "confirmed": ShiftStatus.confirmed,
    "in_progress": ShiftStatus.in_progress,
    "completed": ShiftStatus.completed,
    "cancelled": ShiftStatus.cancelled,
    "no_show": ShiftStatus.no_show,
}


class TandaOAuth:
    """
    Handles the initial OAuth2 authorization code flow for Tanda.

    Flow:
    1. Redirect user to get_authorize_url()
    2. Tanda redirects back with ?code=...
    3. Call exchange_code(code) to get access + refresh tokens
    """

    AUTHORIZE_URL = "https://my.tanda.co/api/oauth/authorize"
    TOKEN_URL = "https://my.tanda.co/api/oauth/token"

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def get_authorize_url(self, scope: str = "me roster timesheet cost") -> str:
        """
        Build the URL to redirect the venue owner to for Tanda authorization.

        Args:
            scope: Space-separated OAuth scopes to request.

        Returns:
            Full authorization URL to redirect the user to.
        """
        import urllib.parse
        params = urllib.parse.urlencode({
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": scope,
        })
        return f"{self.AUTHORIZE_URL}?{params}"

    async def exchange_code(self, code: str) -> TandaCredentials:
        """
        Exchange an authorization code for access and refresh tokens.

        Args:
            code: The authorization code from Tanda's callback.

        Returns:
            TandaCredentials with valid access_token and refresh_token.

        Raises:
            TandaAPIError: If the token exchange fails.
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
            raise TandaAPIError(APIError(
                status_code=response.status_code,
                message=f"Token exchange failed: {response.text}",
                detail={"response": response.text},
            ))

        data = response.json()
        return TandaCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", ""),
            token_expires_at=datetime.now() + timedelta(
                seconds=data.get("expires_in", 7200)
            ),
        )


class TandaWebhookHandler:
    """
    Processes incoming Tanda webhook events.

    Supported events:
    - clockin.updated: Employee clocked in/out
    - user.created / user.updated: Employee record changed
    - shift.updated: Shift was modified
    - roster.published: A roster was published
    """

    def __init__(self, webhook_secret: Optional[str] = None):
        self.webhook_secret = webhook_secret
        self._handlers: dict[str, list] = {}

    def on(self, event_type: str, handler):
        """Register a handler for a webhook event type."""
        self._handlers.setdefault(event_type, []).append(handler)

    def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify the webhook signature from Tanda."""
        if not self.webhook_secret:
            return True  # No secret configured, skip verification
        import hmac
        import hashlib
        expected = hmac.new(
            self.webhook_secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    async def process(self, event_type: str, payload: dict) -> list:
        """
        Dispatch a webhook event to registered handlers.

        Args:
            event_type: The Tanda event type (e.g. "clockin.updated").
            payload: The parsed JSON payload from Tanda.

        Returns:
            List of results from each handler.
        """
        handlers = self._handlers.get(event_type, [])
        results = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                results.append(await handler(payload))
            else:
                results.append(handler(payload))
        return results


class TandaAdapter:
    """
    Async adapter for Tanda workforce management API.

    Provides methods to fetch employees, shifts, and rosters from Tanda,
    converting them to RosterIQ data models. Handles OAuth token refresh,
    rate limiting, and retry logic.
    """

    BASE_URL = "https://my.tanda.co/api/v2"
    MAX_RETRIES = 3
    RETRY_BACKOFF = [1, 2, 4]  # seconds

    def __init__(
        self,
        credentials: TandaCredentials,
        state: State = State.vic,
        on_token_refresh=None,
    ):
        """
        Initialise the Tanda adapter.

        Args:
            credentials: Tanda OAuth credentials.
            state: Default Australian state for employee mapping.
            on_token_refresh: Optional callback(credentials) invoked after a
                successful token refresh, so refreshed tokens can be persisted
                back to storage (mirrors Deputy/MYOB/HumanForce).
        """
        self.credentials = credentials
        self.state = state
        self.rate_limiter = RateLimiter()
        self._client: Optional[httpx.AsyncClient] = None
        self._on_token_refresh = on_token_refresh  # callback(credentials) to persist new tokens

    async def __aenter__(self) -> "TandaAdapter":
        """Context manager entry — create HTTP client."""
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=httpx.Timeout(30.0),
            headers=self._auth_headers(),
        )
        return self

    async def __aexit__(self, *args) -> None:
        """Context manager exit — close HTTP client."""
        await self.close()

    def _auth_headers(self) -> dict[str, str]:
        """Build authorization headers."""
        headers = {"Content-Type": "application/json"}
        if self.credentials.access_token:
            headers["Authorization"] = f"Bearer {self.credentials.access_token}"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client with auth headers."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=httpx.Timeout(30.0),
                headers=self._auth_headers(),
            )
        return self._client

    async def _request(
        self,
        method: str,
        endpoint: str,
        retry_count: int = 0,
        **kwargs,
    ) -> dict | list:
        """
        Make an authenticated API request with rate limiting and error handling.

        Handles:
        - Rate limiting (acquires token before each request)
        - 401: refreshes OAuth token and retries once
        - 429: reads Retry-After header, waits, then retries
        - 5xx: retries up to 3 times with exponential backoff
        - Other 4xx: raises TandaAPIError immediately

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., '/users')
            retry_count: Current retry attempt number.
            **kwargs: Additional arguments passed to httpx request.

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            TandaAPIError: For unrecoverable API errors.
        """
        await self.rate_limiter.acquire()
        client = await self._get_client()

        try:
            response = await client.request(method, endpoint, **kwargs)
        except httpx.HTTPError as e:
            raise TandaAPIError(
                APIError(
                    status_code=0,
                    message=f"HTTP connection error: {str(e)}",
                    detail=str(e),
                )
            )

        if response.status_code == 200:
            return response.json()

        # Handle 401 — refresh token and retry
        if response.status_code == 401 and retry_count == 0:
            await self.refresh_token()
            # Update client headers
            client.headers.update(self._auth_headers())
            return await self._request(method, endpoint, retry_count=1, **kwargs)

        # Handle 429 — rate limited
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", "60"))
            await asyncio.sleep(retry_after)
            return await self._request(method, endpoint, retry_count=retry_count, **kwargs)

        # Handle 5xx — retry with backoff
        if response.status_code >= 500 and retry_count < self.MAX_RETRIES:
            await asyncio.sleep(self.RETRY_BACKOFF[retry_count])
            return await self._request(
                method, endpoint, retry_count=retry_count + 1, **kwargs
            )

        # Unrecoverable error
        detail = None
        try:
            body = response.json()
            detail = body.get("error", body.get("message", str(body)))
        except Exception:
            detail = response.text[:500] if response.text else None

        raise TandaAPIError(
            APIError(
                status_code=response.status_code,
                message=f"Tanda API error on {method} {endpoint}",
                detail=detail,
                retry_after=(
                    float(response.headers.get("Retry-After", "0"))
                    if response.status_code == 429
                    else None
                ),
            )
        )

    async def refresh_token(self) -> None:
        """
        Refresh the OAuth access token using the refresh token.

        Updates self.credentials with new access_token, refresh_token,
        and token_expires_at.

        Raises:
            TandaAPIError: If token refresh fails.
        """
        if not self.credentials.refresh_token:
            raise TandaAPIError(
                APIError(
                    status_code=401,
                    message="No refresh token available",
                    detail="Cannot refresh — no refresh_token in credentials",
                )
            )

        # Token refresh bypasses rate limiter — uses a direct request.
        # Use the same endpoint + form encoding as the OAuth code exchange
        # (TandaOAuth.TOKEN_URL). The previous code POSTed JSON to the wrong
        # path (/oauth/token, missing /api), so refresh 404'd/415'd in prod.
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            response = await client.post(
                "https://my.tanda.co/api/oauth/token",
                data={
                    "client_id": self.credentials.client_id,
                    "client_secret": self.credentials.client_secret,
                    "refresh_token": self.credentials.refresh_token,
                    "grant_type": "refresh_token",
                },
            )

        if response.status_code != 200:
            raise TandaAPIError(
                APIError(
                    status_code=response.status_code,
                    message="Token refresh failed",
                    detail=response.text[:500],
                )
            )

        data = response.json()
        self.credentials.access_token = data["access_token"]
        self.credentials.refresh_token = data.get(
            "refresh_token", self.credentials.refresh_token
        )

        expires_in = data.get("expires_in", 7200)
        self.credentials.token_expires_at = datetime.now().replace(
            microsecond=0
        ) + timedelta(seconds=expires_in)

        # Update client auth header so subsequent requests use the new token.
        if self._client is not None:
            self._client.headers["Authorization"] = (
                f"Bearer {self.credentials.access_token}"
            )

        # Persist refreshed tokens if a callback was provided, so the new
        # access/refresh tokens survive a restart (mirrors Deputy).
        if self._on_token_refresh:
            try:
                self._on_token_refresh(self.credentials)
            except Exception:
                # Persistence failure must not break the in-flight request;
                # the refreshed token still works for this process.
                pass

    async def get_employees(self) -> list[Employee]:
        """
        Fetch all employees from Tanda and convert to RosterIQ models.

        Returns:
            List of Employee objects.
        """
        data = await self._request("GET", "/users")
        if not isinstance(data, list):
            data = data.get("users", data.get("data", []))

        employees = []
        for user_data in data:
            try:
                emp = self._map_tanda_user(user_data)
                employees.append(emp)
            except (KeyError, ValueError):
                continue  # Skip malformed records

        return employees

    async def get_shifts(self, start_date: date, end_date: date) -> list[Shift]:
        """
        Fetch shifts for a date range.

        Args:
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of Shift objects.
        """
        data = await self._request(
            "GET",
            "/shifts",
            params={
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
            },
        )
        if not isinstance(data, list):
            data = data.get("shifts", data.get("data", []))

        shifts = []
        for shift_data in data:
            try:
                shift = self._map_tanda_shift(shift_data)
                shifts.append(shift)
            except (KeyError, ValueError):
                continue

        return shifts

    async def get_roster(self, week_start: date) -> Optional[Roster]:
        """
        Fetch a roster for a specific week.

        Args:
            week_start: Start date of the roster week (Monday).

        Returns:
            Roster object, or None if no roster exists for that week.
        """
        week_end = week_start + timedelta(days=6)

        try:
            data = await self._request(
                "GET",
                "/rosters",
                params={"from": week_start.isoformat(), "to": week_end.isoformat()},
            )
        except TandaAPIError:
            return None

        if not data:
            return None

        roster_data = data[0] if isinstance(data, list) else data
        shifts = []

        for shift_data in roster_data.get("shifts", []):
            try:
                shifts.append(self._map_tanda_shift(shift_data))
            except (KeyError, ValueError):
                continue

        return Roster(
            id=str(roster_data.get("id", "")),
            venue_id=self.credentials.org_id,
            week_start=week_start,
            week_end=week_end,
            shifts=shifts,
            created_at=datetime.now(),
        )

    async def get_daily_schedule(self, target_date: date) -> list[Shift]:
        """
        Fetch all shifts for a specific day.

        Args:
            target_date: The date to fetch shifts for.

        Returns:
            List of Shift objects for that day.
        """
        return await self.get_shifts(target_date, target_date)

    def _map_tanda_user(self, data: dict) -> Employee:
        """
        Map a Tanda API user response to our Employee model.

        Args:
            data: Raw Tanda user JSON dict.

        Returns:
            Employee model instance.
        """
        emp_type = _TANDA_EMPLOYMENT_MAP.get(
            data.get("employment_type", "casual"),
            EmploymentType.casual,
        )

        hourly_rate = data.get("hourly_rate", data.get("base_hourly_rate", 25.0))

        return Employee(
            id=str(data["id"]),
            tanda_id=str(data["id"]),
            name=data.get("name", data.get("first_name", "Unknown")),
            employment_type=emp_type,
            award_level=AwardLevel.level_1,  # Default — would be mapped from Tanda classification
            state=self.state,
            hourly_base_rate=Decimal(str(hourly_rate)),
            phone=data.get("phone", data.get("mobile")),
            email=data.get("email"),
            skills=data.get("qualifications", []),
            availability={},
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    def _map_tanda_shift(self, data: dict) -> Shift:
        """
        Map a Tanda API shift response to our Shift model.

        Args:
            data: Raw Tanda shift JSON dict.

        Returns:
            Shift model instance.
        """
        # Parse start/finish times — Tanda uses Unix timestamps or ISO strings.
        # Resolve epochs in the venue's LOCAL timezone so the shift lands on the
        # correct date/hour (drives day-of-week/time-of-day penalty rates).
        start = data.get("start", data.get("start_time"))
        finish = data.get("finish", data.get("end_time", data.get("end")))
        tz = ZoneInfo(_STATE_TZ.get(getattr(self.state, "value", str(self.state)), "Australia/Melbourne"))

        if isinstance(start, (int, float)):
            start_dt = datetime.fromtimestamp(start, tz=tz)
        else:
            start_dt = datetime.fromisoformat(str(start))

        if isinstance(finish, (int, float)):
            end_dt = datetime.fromtimestamp(finish, tz=tz)
        else:
            end_dt = datetime.fromisoformat(str(finish))

        # Calculate break minutes from Tanda breaks array
        break_minutes = 0
        for brk in data.get("breaks", []):
            if isinstance(brk, dict):
                brk_start = brk.get("start", 0)
                brk_end = brk.get("finish", brk.get("end", 0))
                if isinstance(brk_start, (int, float)) and isinstance(brk_end, (int, float)):
                    break_minutes += int((brk_end - brk_start) / 60)
                elif "length" in brk:
                    break_minutes += int(brk["length"])
            elif isinstance(brk, (int, float)):
                break_minutes += int(brk)

        status = _TANDA_STATUS_MAP.get(
            data.get("status", "pending"), ShiftStatus.scheduled
        )

        return Shift(
            id=str(data.get("id", "")),
            employee_id=str(data.get("user_id", data.get("employee_id", ""))),
            date=start_dt.date(),
            start_time=start_dt.time().replace(second=0, microsecond=0),
            end_time=end_dt.time().replace(second=0, microsecond=0),
            break_minutes=max(0, break_minutes),
            status=status,
            role=data.get("department", data.get("role", "general")),
        )

    async def health_check(self) -> bool:
        """
        Check if the Tanda API is accessible and credentials are valid.

        Returns:
            True if API is accessible, False otherwise.
        """
        try:
            await self._request("GET", "/users", params={"page": 1, "per_page": 1})
            return True
        except TandaAPIError:
            return False

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
