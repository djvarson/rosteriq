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

    def __init__(self, credentials: MYOBCredentials, state: State = State.wa):
        self.credentials = credentials
        self.state = state
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = MYOBRateLimiter()

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
        **kwargs,
    ) -> Any:
        """Make a rate-limited request to the MYOB API."""
        await self._rate_limiter.acquire()

        try:
            response = await self._client.request(method, url, **kwargs)
        except httpx.TimeoutException:
            raise MYOBAPIError(APIError(
                status_code=408,
                message="MYOB API request timed out",
                detail={"url": url},
            ))

        if response.status_code == 401:
            raise MYOBAPIError(APIError(
                status_code=401,
                message="MYOB authentication failed — token may be expired",
                detail={"url": url},
            ))

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            await asyncio.sleep(retry_after)
            return await self._request(method, url, **kwargs)

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
