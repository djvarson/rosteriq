"""
Async HTTP adapter for the MYOB Business / AccountRight API.

Handles OAuth2 authentication, company file selection, and mapping
MYOB data to RosterIQ models for employees, timesheets, and payroll.

MYOB API uses two base URLs:
  - AccountRight: https://api.myob.com/accountright/
  - Business:     https://api.myob.com/au/essentials/businesses/{id}/

This adapter supports both products through a unified interface.
For pilot venues, we also support direct API-key auth (no OAuth dance).

Usage:
    async with MYOBAdapter(credentials) as myob:
        employees = await myob.get_employees()
        timesheets = await myob.get_timesheets(start_date, end_date)
        await myob.push_timesheet(employee_id, entries)
"""

import asyncio
import time
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

import httpx

from rosteriq.models import (
    Employee,
    Shift,
    APIError,
    EmploymentType,
    AwardLevel,
    ShiftStatus,
    State,
)

logger = logging.getLogger(__name__)

# Transient HTTP status codes that should trigger a retry with backoff.
# 429 is handled separately (Retry-After), so it is intentionally excluded here.
_RETRYABLE_STATUSES = {500, 502, 503, 504}
MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0  # seconds — 1s, 2s, 4s


class MYOBAPIError(Exception):
    """Exception raised for MYOB API errors."""

    def __init__(self, api_error: APIError):
        self.api_error = api_error
        super().__init__(f"MYOB API error {api_error.status_code}: {api_error.message}")


class MYOBRateLimiter:
    """
    Token bucket rate limiter for MYOB API.
    MYOB allows approximately 60 requests per minute per company file.
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


# Mapping from MYOB employment basis to RosterIQ types
_MYOB_EMPLOYMENT_MAP = {
    "Individual": EmploymentType.full_time,
    "FullTime": EmploymentType.full_time,
    "PartTime": EmploymentType.part_time,
    "Casual": EmploymentType.casual,
    "Contract": EmploymentType.casual,
    "Labour Hire": EmploymentType.casual,
    "Other": EmploymentType.casual,
}

# MYOB state mapping
_MYOB_STATE_MAP = {
    "VIC": State.vic,
    "NSW": State.nsw,
    "QLD": State.qld,
    "WA": State.wa,
    "SA": State.sa,
    "TAS": State.tas,
    "ACT": State.act,
    "NT": State.nt,
    "Victoria": State.vic,
    "New South Wales": State.nsw,
    "Queensland": State.qld,
    "Western Australia": State.wa,
    "South Australia": State.sa,
    "Tasmania": State.tas,
    "Australian Capital Territory": State.act,
    "Northern Territory": State.nt,
}


@dataclass
class MYOBCredentials:
    """Stores MYOB API credentials for a venue."""

    api_key: str
    api_secret: str
    access_token: str
    refresh_token: str = ""
    token_expires_at: Optional[datetime] = None
    company_file_id: str = ""
    company_file_uri: str = ""
    cf_username: str = ""
    cf_password: str = ""
    product: str = "AccountRight"  # "AccountRight" or "Business"

    @property
    def base_url(self) -> str:
        if self.product == "Business":
            return f"https://api.myob.com/au/essentials/businesses/{self.company_file_id}"
        return f"{self.company_file_uri}" if self.company_file_uri else "https://api.myob.com/accountright"

    @property
    def is_expired(self) -> bool:
        if not self.token_expires_at:
            return False
        return datetime.now() >= self.token_expires_at


class MYOBOAuth:
    """
    Handles OAuth2 authorization code flow for MYOB.

    Flow:
    1. Redirect user to get_authorize_url()
    2. MYOB redirects back with ?code=...
    3. Call exchange_code(code) to get access + refresh tokens
    """

    AUTHORIZE_URL = "https://secure.myob.com/oauth2/account/authorize"
    TOKEN_URL = "https://secure.myob.com/oauth2/v1/authorize"

    def __init__(self, api_key: str, api_secret: str, redirect_uri: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_uri = redirect_uri

    def get_authorize_url(self, scope: str = "CompanyFile") -> str:
        """Build the URL to redirect the venue owner to for MYOB authorization."""
        import urllib.parse
        params = urllib.parse.urlencode({
            "client_id": self.api_key,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": scope,
        })
        return f"{self.AUTHORIZE_URL}?{params}"

    async def exchange_code(self, code: str) -> dict:
        """Exchange an authorization code for access and refresh tokens."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.TOKEN_URL, data={
                "client_id": self.api_key,
                "client_secret": self.api_secret,
                "code": code,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
            })

        if response.status_code != 200:
            raise MYOBAPIError(APIError(
                status_code=response.status_code,
                message=f"MYOB token exchange failed: {response.text}",
                detail={"response": response.text},
            ))

        return response.json()

    async def refresh_access_token(
        self, refresh_token: str
    ) -> dict:
        """Refresh an expired access token."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.TOKEN_URL, data={
                "client_id": self.api_key,
                "client_secret": self.api_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            })

        if response.status_code != 200:
            raise MYOBAPIError(APIError(
                status_code=response.status_code,
                message=f"MYOB token refresh failed: {response.text}",
                detail={"response": response.text},
            ))

        return response.json()


class MYOBAdapter:
    """
    Async adapter for the MYOB AccountRight / Business API.

    Maps MYOB resources to RosterIQ models:
    - MYOB Employee Contact → RosterIQ Employee
    - MYOB Timesheet entries → RosterIQ Shift
    - MYOB Payroll Categories → wage rates
    """

    def __init__(
        self,
        credentials: MYOBCredentials,
        state: State = State.wa,
        on_token_refresh=None,
    ):
        self.credentials = credentials
        self.state = state
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = MYOBRateLimiter()
        self._on_token_refresh = on_token_refresh  # callback(credentials) to persist new tokens

    async def _ensure_valid_token(self) -> None:
        """Refresh the access token if it has expired (proactive refresh)."""
        if self.credentials.is_expired and self.credentials.refresh_token:
            await self._refresh_token()

    async def _refresh_token(self) -> None:
        """Refresh the MYOB OAuth2 access token and update the client + credentials."""
        if not self.credentials.refresh_token:
            raise MYOBAPIError(APIError(
                status_code=401,
                message="No MYOB refresh token available — please reconnect MYOB.",
                detail={},
            ))

        oauth = MYOBOAuth(
            api_key=self.credentials.api_key,
            api_secret=self.credentials.api_secret,
            redirect_uri="",
        )
        data = await oauth.refresh_access_token(self.credentials.refresh_token)

        self.credentials.access_token = data["access_token"]
        if data.get("refresh_token"):
            self.credentials.refresh_token = data["refresh_token"]
        # MYOB access tokens are short-lived (~20 min); default if absent.
        expires_in = int(data.get("expires_in", 1200))
        self.credentials.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        # Update the live client's auth header so in-flight clients pick it up.
        if self._client is not None:
            self._client.headers["Authorization"] = f"Bearer {self.credentials.access_token}"
        logger.info("MYOB access token refreshed successfully")

        # Persist refreshed tokens if a callback was provided.
        if self._on_token_refresh:
            try:
                self._on_token_refresh(self.credentials)
            except Exception as e:
                logger.warning(f"Failed to persist refreshed MYOB token: {e}")

    async def __aenter__(self):
        headers = {
            "Authorization": f"Bearer {self.credentials.access_token}",
            "x-myobapi-key": self.credentials.api_key,
            "x-myobapi-version": "v2",
            "Accept": "application/json",
        }
        # AccountRight company files require basic auth header too
        if self.credentials.cf_username:
            import base64
            cf_auth = base64.b64encode(
                f"{self.credentials.cf_username}:{self.credentials.cf_password}".encode()
            ).decode()
            headers["x-myobapi-cftoken"] = cf_auth

        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        url: str,
        _retry_on_auth: bool = True,
        **kwargs,
    ) -> Any:
        """Make a rate-limited request to the MYOB API with retry + backoff."""
        # Proactively refresh an expired token before spending a request.
        await self._ensure_valid_token()

        for attempt in range(MAX_RETRIES + 1):
            await self._rate_limiter.acquire()

            try:
                response = await self._client.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.ConnectError,
                    httpx.ReadError, httpx.WriteError, httpx.PoolTimeout) as e:
                # Transient network/timeout error — retry with exponential backoff.
                if attempt < MAX_RETRIES:
                    wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        f"MYOB request to {url} failed (attempt {attempt + 1}/"
                        f"{MAX_RETRIES + 1}): {e}. Retrying in {wait:.1f}s..."
                    )
                    await asyncio.sleep(wait)
                    continue
                raise MYOBAPIError(APIError(
                    status_code=408,
                    message=f"MYOB API request failed after {MAX_RETRIES + 1} attempts: {e}",
                    detail={"url": url, "error": str(e), "attempts": MAX_RETRIES + 1},
                ))

            if response.status_code == 401:
                # The token may have expired server-side before token_expires_at.
                # Refresh once and retry before surfacing an auth error.
                if _retry_on_auth and self.credentials.refresh_token:
                    try:
                        await self._refresh_token()
                    except MYOBAPIError:
                        pass
                    else:
                        return await self._request(
                            method, url, _retry_on_auth=False, **kwargs
                        )
                raise MYOBAPIError(APIError(
                    status_code=401,
                    message="MYOB authentication failed — token may be expired",
                    detail={"url": url},
                ))

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "60"))
                await asyncio.sleep(retry_after)
                return await self._request(method, url, **kwargs)

            # Retry transient 5xx server errors with exponential backoff.
            if response.status_code in _RETRYABLE_STATUSES and attempt < MAX_RETRIES:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    f"MYOB {response.status_code} on {url} (attempt {attempt + 1}/"
                    f"{MAX_RETRIES + 1}). Retrying in {wait:.1f}s..."
                )
                await asyncio.sleep(wait)
                continue

            if response.status_code >= 400:
                error_body = response.text
                raise MYOBAPIError(APIError(
                    status_code=response.status_code,
                    message=f"MYOB API error: {error_body}",
                    detail={"url": url, "response": error_body},
                ))

            if response.status_code == 204:
                return {}

            return response.json()

        # Safety net — exhausted retries on a retryable 5xx without returning.
        raise MYOBAPIError(APIError(
            status_code=503,
            message=f"MYOB API request to {url} failed after {MAX_RETRIES + 1} attempts",
            detail={"url": url, "attempts": MAX_RETRIES + 1},
        ))

    async def _get(self, url: str, **kwargs) -> Any:
        return await self._request("GET", url, **kwargs)

    async def _put(self, url: str, **kwargs) -> Any:
        return await self._request("PUT", url, **kwargs)

    async def _post(self, url: str, **kwargs) -> Any:
        return await self._request("POST", url, **kwargs)

    # ================================================================
    # Company Files
    # ================================================================

    async def list_company_files(self) -> List[Dict[str, Any]]:
        """List available MYOB company files for this account."""
        data = await self._get("https://api.myob.com/accountright/")
        files = []
        for cf in data if isinstance(data, list) else []:
            files.append({
                "id": cf.get("Id", ""),
                "name": cf.get("Name", ""),
                "serial_number": cf.get("SerialNumber", ""),
                "uri": cf.get("Uri", ""),
                "product_version": cf.get("ProductVersion", ""),
            })
        return files

    async def get_company_info(self) -> Dict[str, Any]:
        """Get current company file info."""
        url = f"{self.credentials.base_url}/Info"
        try:
            return await self._get(url)
        except Exception:
            return {"Name": "Unknown", "Uri": self.credentials.base_url}

    # ================================================================
    # Employees
    # ================================================================

    async def get_employees(self, active_only: bool = True) -> List[Employee]:
        """
        Fetch employees from MYOB and map to RosterIQ Employee models.

        MYOB AccountRight: /Contact/Employee
        MYOB Business: /contacts/employees
        """
        url = f"{self.credentials.base_url}/Contact/Employee"
        if active_only:
            url += "?$filter=IsActive eq true"

        data = await self._get(url)
        items = data.get("Items", []) if isinstance(data, dict) else data

        employees = []
        for emp_data in items:
            try:
                employee = self._map_employee(emp_data)
                employees.append(employee)
            except Exception as e:
                logger.warning(f"Failed to map MYOB employee {emp_data.get('UID', '?')}: {e}")

        logger.info(f"Fetched {len(employees)} employees from MYOB")
        return employees

    def _map_employee(self, data: Dict[str, Any]) -> Employee:
        """Map a MYOB employee record to a RosterIQ Employee model."""
        uid = str(data.get("UID", ""))

        # Name
        first = data.get("FirstName", "")
        last = data.get("LastName", "")
        name = f"{first} {last}".strip() or data.get("DisplayID", uid)

        # Employment type
        emp_basis = data.get("EmploymentBasis", "Individual")
        employment_type = _MYOB_EMPLOYMENT_MAP.get(emp_basis, EmploymentType.casual)

        # Contact details
        phone = ""
        email = ""
        addresses = data.get("Addresses", [])
        for addr in addresses:
            if addr.get("Phone1"):
                phone = phone or addr["Phone1"]
            if addr.get("Email"):
                email = email or addr["Email"]

        # State from address
        emp_state = self.state
        for addr in addresses:
            state_str = addr.get("State", "")
            if state_str in _MYOB_STATE_MAP:
                emp_state = _MYOB_STATE_MAP[state_str]
                break

        # Hourly rate from payroll details
        hourly_rate = Decimal("0.00")
        payroll = data.get("PayrollDetails", {})
        if payroll:
            wage = payroll.get("Wage", {})
            if wage:
                hourly_rate = Decimal(str(wage.get("HourlyRate", 0) or 0))
            if hourly_rate == 0:
                hourly_rate = Decimal(str(wage.get("AnnualSalary", 0) or 0))
                if hourly_rate > 0:
                    # Convert annual to approximate hourly
                    hourly_rate = (hourly_rate / 52 / 38).quantize(Decimal("0.01"))

        # Default to casual rate if no rate found
        if hourly_rate == 0:
            hourly_rate = Decimal("28.26")  # Hospitality Award L3 casual

        return Employee(
            id=f"myob_{uid}",
            tanda_id=uid,
            name=name,
            employment_type=employment_type,
            award_level=AwardLevel.level_3,
            state=emp_state,
            hourly_base_rate=hourly_rate,
            phone=phone or None,
            email=email or None,
            skills=[],
            availability={
                "monday": {"start": "06:00", "end": "23:00"},
                "tuesday": {"start": "06:00", "end": "23:00"},
                "wednesday": {"start": "06:00", "end": "23:00"},
                "thursday": {"start": "06:00", "end": "23:00"},
                "friday": {"start": "06:00", "end": "23:00"},
                "saturday": {"start": "06:00", "end": "23:00"},
                "sunday": {"start": "06:00", "end": "23:00"},
            },
            max_hours_per_week=38.0 if employment_type == EmploymentType.full_time else 25.0,
        )

    # ================================================================
    # Payroll Categories (wage rates, super, deductions)
    # ================================================================

    async def get_payroll_categories(self) -> Dict[str, Any]:
        """Fetch payroll wage categories from MYOB."""
        url = f"{self.credentials.base_url}/Payroll/PayrollCategory/Wage"
        data = await self._get(url)
        items = data.get("Items", []) if isinstance(data, dict) else data
        categories = []
        for cat in items:
            categories.append({
                "uid": cat.get("UID", ""),
                "name": cat.get("Name", ""),
                "type": cat.get("WageType", ""),
                "hourly_rate": cat.get("FixedHourlyRate", 0),
                "multiplier": cat.get("MultiplierPercentage", 100),
                "is_active": cat.get("IsActive", True),
            })
        return {"categories": categories, "count": len(categories)}

    # ================================================================
    # Timesheets
    # ================================================================

    async def get_timesheets(
        self,
        start_date: date,
        end_date: date,
        employee_uid: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch timesheet entries from MYOB.

        MYOB timesheets are per-employee, per-week.
        """
        if employee_uid:
            url = f"{self.credentials.base_url}/Payroll/Timesheet/{employee_uid}"
            params = {
                "StartDate": start_date.isoformat(),
                "EndDate": end_date.isoformat(),
            }
            data = await self._get(url, params=params)
            return [data] if data else []

        # Fetch for all employees
        employees = await self.get_employees()
        timesheets = []
        for emp in employees:
            # Extract MYOB UID from our prefixed ID
            uid = emp.tanda_id
            if not uid:
                continue
            try:
                url = f"{self.credentials.base_url}/Payroll/Timesheet/{uid}"
                params = {
                    "StartDate": start_date.isoformat(),
                    "EndDate": end_date.isoformat(),
                }
                data = await self._get(url, params=params)
                if data:
                    timesheets.append({
                        "employee_uid": uid,
                        "employee_name": emp.name,
                        "timesheet": data,
                    })
            except MYOBAPIError as e:
                if e.api_error.status_code != 404:
                    logger.warning(f"Failed to fetch timesheet for {emp.name}: {e}")

        return timesheets

    async def push_timesheet(
        self,
        employee_uid: str,
        start_date: date,
        entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Push timesheet entries to MYOB for a single employee.

        entries format:
        [
            {
                "payroll_category_uid": "...",
                "date": "2026-06-10",
                "hours": 8.0,
                "notes": "Bar shift"
            },
            ...
        ]
        """
        url = f"{self.credentials.base_url}/Payroll/Timesheet/{employee_uid}"

        # Build MYOB timesheet payload
        lines = []
        for entry in entries:
            line = {
                "PayrollCategory": {"UID": entry["payroll_category_uid"]},
                "Activity": entry.get("activity", {}),
                "Job": entry.get("job", {}),
            }
            # MYOB uses day-indexed entries within a week
            entry_date = date.fromisoformat(entry["date"]) if isinstance(entry["date"], str) else entry["date"]
            day_of_week = entry_date.weekday()  # 0=Mon, 6=Sun
            day_entries = [0.0] * 7
            day_entries[day_of_week] = float(entry.get("hours", 0))
            line["Entries"] = [{"Hours": h} for h in day_entries]
            lines.append(line)

        payload = {
            "Employee": {"UID": employee_uid},
            "StartDate": start_date.isoformat(),
            "Lines": lines,
        }

        result = await self._put(url, json=payload)
        logger.info(f"Pushed timesheet to MYOB for employee {employee_uid}")
        return result

    # ================================================================
    # Pay Runs
    # ================================================================

    async def get_pay_runs(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch pay run records from MYOB."""
        url = f"{self.credentials.base_url}/Payroll/PayRun"
        filters = []
        if start_date:
            filters.append(f"PayPeriodStartDate ge datetime'{start_date.isoformat()}'")
        if end_date:
            filters.append(f"PayPeriodEndDate le datetime'{end_date.isoformat()}'")

        params = {}
        if filters:
            params["$filter"] = " and ".join(filters)

        data = await self._get(url, params=params)
        items = data.get("Items", []) if isinstance(data, dict) else data

        runs = []
        for run in items:
            runs.append({
                "uid": run.get("UID", ""),
                "pay_period_start": run.get("PayPeriodStartDate", ""),
                "pay_period_end": run.get("PayPeriodEndDate", ""),
                "payment_date": run.get("DateOfPayment", ""),
                "status": "processed" if run.get("IsPosted") else "draft",
            })
        return runs

    # ================================================================
    # General Ledger (for revenue data)
    # ================================================================

    async def get_accounts(self) -> List[Dict[str, Any]]:
        """Fetch chart of accounts — useful for revenue account mapping."""
        url = f"{self.credentials.base_url}/GeneralLedger/Account"
        params = {"$filter": "Type eq 'Income' or Type eq 'Revenue'"}
        data = await self._get(url, params=params)
        items = data.get("Items", []) if isinstance(data, dict) else data
        return [
            {
                "uid": acct.get("UID", ""),
                "name": acct.get("Name", ""),
                "number": acct.get("Number", ""),
                "type": acct.get("Type", ""),
                "is_active": acct.get("IsActive", True),
                "current_balance": acct.get("CurrentBalance", 0),
            }
            for acct in items
        ]

    # ================================================================
    # Supplier Bills (Outbound)
    # ================================================================

    async def push_bill(
        self,
        invoice: dict,
        account_display_id: str = "5-1000",
        tax_code: str = "GST",
    ) -> dict:
        """
        Push a RosterIQ supplier invoice to MYOB as a Service purchase bill.

        MYOB references everything by UID, so this resolves in order:
        1. Supplier contact UID by CompanyName — created on the fly if the
           supplier does not exist in MYOB yet
        2. Account UID from its DisplayID (e.g. "5-1000")
        3. Tax code UID from its code (e.g. "GST")
        then creates a tax-inclusive Purchase/Bill/Service with one line per
        invoice item and a JournalMemo referencing the RosterIQ invoice id
        for the audit trail.

        NOTE — semantic difference vs Xero: Xero bills land as DRAFT for
        review, but MYOB AccountRight has no draft state, so this creates a
        real open Service Bill immediately. That is correct here because a
        RosterIQ supplier invoice is already the verified actual delivery,
        not a proposal.

        Args:
            invoice: RosterIQ supplier invoice record (from list_supplier_invoices)
            account_display_id: DisplayID of the MYOB purchases/COGS account
                for the lines (default "5-1000")
            tax_code: MYOB tax code for the lines (default "GST" — AU GST)

        Returns:
            {"myob_bill_uid", "myob_bill_number", "status"} parsed from the
            bill body MYOB returns
        """
        # 1. Supplier UID — match by CompanyName, create if missing.
        supplier_name = invoice.get("supplier") or "Unknown supplier"
        # MYOB OData filters escape single quotes by doubling them.
        escaped_name = supplier_name.replace("'", "''")
        data = await self._get(
            f"{self.credentials.base_url}/Contact/Supplier",
            params={"$filter": f"CompanyName eq '{escaped_name}'"},
        )
        items = data.get("Items", []) if isinstance(data, dict) else data
        if items:
            supplier_uid = items[0].get("UID")
        else:
            created = await self._post(
                f"{self.credentials.base_url}/Contact/Supplier",
                params={"returnBody": "true"},
                json={"CompanyName": supplier_name, "IsIndividual": False},
            )
            supplier_uid = created.get("UID") if isinstance(created, dict) else None

        # 2. Account UID from its DisplayID.
        data = await self._get(
            f"{self.credentials.base_url}/GeneralLedger/Account",
            params={"$filter": f"DisplayID eq '{account_display_id}'"},
        )
        items = data.get("Items", []) if isinstance(data, dict) else data
        if not items:
            raise ValueError(
                f"MYOB account {account_display_id} not found — pass "
                "account_display_id of an existing purchases/COGS account"
            )
        account_uid = items[0].get("UID")

        # 3. TaxCode UID from its code.
        data = await self._get(
            f"{self.credentials.base_url}/GeneralLedger/TaxCode",
            params={"$filter": f"Code eq '{tax_code}'"},
        )
        items = data.get("Items", []) if isinstance(data, dict) else data
        if not items:
            raise ValueError(
                f"MYOB tax code {tax_code} not found — pass "
                "tax_code of an existing tax code"
            )
        tax_code_uid = items[0].get("UID")

        # MYOB wants YYYY-MM-DD; the stored invoice_date may be a date, an ISO
        # string (PostgreSQL round-trip), or missing — fall back to today.
        raw_date = invoice.get("invoice_date")
        if isinstance(raw_date, datetime):
            bill_date = raw_date.date()
        elif isinstance(raw_date, date):
            bill_date = raw_date
        elif raw_date:
            try:
                bill_date = date.fromisoformat(str(raw_date)[:10])
            except ValueError:
                bill_date = date.today()
        else:
            bill_date = date.today()

        lines = []
        for item in invoice.get("items", []) or []:
            name = item.get("name")
            packs = item.get("packs")
            pack_size = item.get("pack_size")
            unit = item.get("unit")
            lines.append({
                "Type": "Transaction",
                "Description": f"{name} ({packs} x {pack_size}{unit or ''})",
                "Total": item.get("line_total"),
                "Account": {"UID": account_uid},
                "TaxCode": {"UID": tax_code_uid},
            })

        # 4. Create the bill (returnBody so we get the UID back).
        body = await self._post(
            f"{self.credentials.base_url}/Purchase/Bill/Service",
            params={"returnBody": "true"},
            json={
                "Supplier": {"UID": supplier_uid},
                "Date": bill_date.strftime("%Y-%m-%d"),
                "SupplierInvoiceNumber": invoice.get("invoice_number"),
                "IsTaxInclusive": True,
                "JournalMemo": f"RosterIQ {invoice.get('id')}",
                "Lines": lines,
            },
        )

        bill_uid = body.get("UID") if isinstance(body, dict) else None
        if not bill_uid:
            raise ValueError(
                f"MYOB returned no bill for invoice {invoice.get('invoice_number')} "
                "— the bill was not created"
            )

        logger.info(f"Pushed supplier invoice {invoice.get('id')} to MYOB as bill {bill_uid}")
        return {
            "myob_bill_uid": bill_uid,
            "myob_bill_number": body.get("Number"),
            "status": body.get("Status") or "Open",
        }

    # ================================================================
    # Connection verification
    # ================================================================

    async def verify_connection(self) -> Dict[str, Any]:
        """Verify the MYOB connection is working and return account info."""
        try:
            info = await self.get_company_info()
            return {
                "connected": True,
                "company_name": info.get("Name", "Unknown"),
                "product": self.credentials.product,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
            }
