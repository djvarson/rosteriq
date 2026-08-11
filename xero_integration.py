"""
Xero accounting integration for RosterIQ.

Provides:
- OAuth2 PKCE flow for secure authorization
- Revenue sync (daily pulls from Xero bank/invoices)
- Labour cost export (daily journal entries for wages)
- Supplier bill push (draft ACCPAY bills from RosterIQ supplier invoices)
- P&L reporting with labour % calculations
- Australian context: GST 10%, super 11.5%, BAS periods

Usage:
    async with XeroClient(credentials, db) as xero:
        revenue = await xero.pull_revenue('2026-04-23', venue_id='v1')
        await xero.push_labour_costs(journal_entry, venue_id='v1')
"""

import asyncio
import base64
import hashlib
import secrets
import time
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any
from urllib.parse import urlencode, parse_qs, urlparse

import httpx
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)


# ============================================================================
# Models
# ============================================================================

class XeroCredentials(BaseModel):
    """OAuth2 PKCE credentials for Xero."""

    venue_id: str
    client_id: str
    client_secret: str
    tenant_id: str  # Xero Organization ID (UUID)
    access_token: str
    refresh_token: str
    token_expires: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        arbitrary_types_allowed = True


class RevenueSnapshot(BaseModel):
    """Daily revenue snapshot from Xero."""

    venue_id: str
    snapshot_date: date
    total_revenue: Decimal
    food_revenue: Decimal
    beverage_revenue: Decimal
    gaming_revenue: Decimal
    function_revenue: Decimal
    gst_collected: Decimal  # 10% for AU
    source: str = "xero"
    captured_at: datetime

    class Config:
        arbitrary_types_allowed = True


class LabourCostJournal(BaseModel):
    """Labour cost journal entry for Xero."""

    venue_id: str
    journal_date: date
    total_wages_expense: Decimal
    total_super_payable: Decimal  # 11.5% of gross wages
    breakdown: Dict[str, Dict[str, Decimal]]  # {venue: {role: amount}}
    award_levels: Dict[str, Decimal]  # {level: amount}
    description: str = ""

    class Config:
        arbitrary_types_allowed = True


class PnLReport(BaseModel):
    """P&L extract from Xero with labour metrics."""

    venue_id: str
    period_start: date
    period_end: date
    total_revenue: Decimal
    cogs: Decimal
    gross_profit: Decimal
    labour_costs: Decimal
    labour_percentage: Decimal  # labour_costs / total_revenue * 100
    other_expenses: Decimal
    net_profit: Decimal
    generated_at: datetime

    class Config:
        arbitrary_types_allowed = True


# ============================================================================
# Xero OAuth2 PKCE Flow
# ============================================================================

class XeroOAuth:
    """
    Xero OAuth2 PKCE flow implementation.

    Flow:
    1. generate_authorize_url() -> user clicks and logs into Xero
    2. Xero redirects back with ?code=... and ?state=...
    3. exchange_code(code) -> get access + refresh tokens
    4. Token refresh handled automatically on expiry
    """

    AUTHORIZE_URL = "https://login.xero.com/identity/connect/authorize"
    TOKEN_URL = "https://identity.xero.com/connect/token"
    API_URL = "https://api.xero.com/api.xro/2.0"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        scopes: Optional[List[str]] = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scopes = scopes or [
            "offline_access",
            "openid",
            "profile",
            "email",
            # Xero apps created after 2 March 2026 only accept GRANULAR
            # scopes (the old broad "accounting" scope returns invalid_scope
            # at the consent screen). These names are the canonical list from
            # the app's Configuration page — one per shipped feature:
            "accounting.invoices",                    # ACCPAY bills push
            "accounting.contacts",                    # supplier contacts on bills
            "accounting.settings.read",               # org/account metadata
            "accounting.banktransactions.read",       # revenue pull
            "accounting.reports.profitandloss.read",  # P&L pull
            "accounting.manualjournals",              # labour-cost journal push
            "payroll.timesheets",                     # payroll timesheet push
        ]

    def generate_authorize_url(self) -> tuple[str, str, str]:
        """
        Generate authorization URL, state, and code_verifier for PKCE flow.

        Returns:
            (authorize_url, state, code_verifier) — store state AND code_verifier
            in session; redirect user to authorize_url.
        """
        state = secrets.token_urlsafe(32)
        code_verifier = secrets.token_urlsafe(64)

        # PKCE S256: base64url(sha256(code_verifier))
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

        params = urlencode({
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": " ".join(self.scopes),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        })

        return f"{self.AUTHORIZE_URL}?{params}", state, code_verifier

    async def exchange_code(
        self, code: str, state: str, code_verifier: str
    ) -> XeroCredentials:
        """
        Exchange authorization code for tokens.

        Args:
            code: Authorization code from Xero callback
            state: State from authorize URL (for validation)
            code_verifier: Code verifier from authorize URL (PKCE)

        Returns:
            XeroCredentials with tokens and tenant info
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.redirect_uri,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code_verifier": code_verifier,
                },
            )

            if response.status_code != 200:
                logger.error(f"Token exchange failed with status {response.status_code}")
                raise ValueError(f"Xero token exchange failed: {response.status_code}")

            data = response.json()
            expires = datetime.utcnow() + timedelta(
                seconds=data.get("expires_in", 1800)
            )

            access_token = data["access_token"]

            # Fetch tenant_id from Xero connections endpoint
            tenant_id = ""
            try:
                conn_resp = await client.get(
                    "https://api.xero.com/connections",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                if conn_resp.status_code == 200:
                    connections = conn_resp.json()
                    if connections:
                        tenant_id = connections[0].get("tenantId", "")
            except Exception as e:
                logger.warning(f"Could not fetch Xero connections: {e}")

            return XeroCredentials(
                venue_id="",  # Will be set by caller
                client_id=self.client_id,
                client_secret=self.client_secret,
                tenant_id=tenant_id,
                access_token=access_token,
                refresh_token=data["refresh_token"],
                token_expires=expires,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )

    async def refresh_access_token(
        self, credentials: XeroCredentials
    ) -> XeroCredentials:
        """Refresh expired access token."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": credentials.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )

            if response.status_code != 200:
                logger.error(f"Token refresh failed: {response.text}")
                raise ValueError("Xero token refresh failed")

            data = response.json()
            expires = datetime.utcnow() + timedelta(
                seconds=data.get("expires_in", 1800)
            )

            credentials.access_token = data["access_token"]
            credentials.refresh_token = data.get("refresh_token", credentials.refresh_token)
            credentials.token_expires = expires
            credentials.updated_at = datetime.utcnow()

            return credentials


# ============================================================================
# Rate Limiter
# ============================================================================

class RateLimiter:
    """
    Token bucket rate limiter for Xero API.

    Xero allows 60 requests per 60-second window.
    """

    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []

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
        """Remove request timestamps outside the current window."""
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# ============================================================================
# Xero API Client
# ============================================================================

class XeroClient:
    """
    Async HTTP client for Xero API.

    Handles:
    - OAuth2 PKCE token management
    - Rate limiting (60 req/min)
    - Retry with exponential backoff
    - Revenue sync (bank transactions, invoices)
    - Labour cost journals
    - P&L reporting
    """

    def __init__(
        self,
        credentials: XeroCredentials,
        db,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.credentials = credentials
        self.db = db
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.rate_limiter = RateLimiter(max_requests=60, window_seconds=60)
        self._http_client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._http_client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._http_client:
            await self._http_client.aclose()

    async def _ensure_token_valid(self) -> None:
        """Refresh token if expired."""
        if datetime.utcnow() >= self.credentials.token_expires:
            oauth = XeroOAuth(
                self.credentials.client_id,
                self.credentials.client_secret,
                "",  # Not needed for refresh
            )
            self.credentials = await oauth.refresh_access_token(self.credentials)
            # Update database
            await self._save_credentials()

    async def _save_credentials(self) -> None:
        """Persist refreshed credentials so the next request reuses the new token."""
        try:
            await save_xero_credentials(self.db, self.credentials)
        except Exception as e:
            logger.warning(f"Failed to persist refreshed Xero credentials: {e}")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make authenticated request to Xero API with retry.

        Args:
            method: HTTP method (GET, POST, PUT)
            endpoint: API endpoint (e.g., "Invoices")
            data: Request body (for POST/PUT)
            params: Query parameters

        Returns:
            JSON response
        """
        await self._ensure_token_valid()
        await self.rate_limiter.acquire()

        url = f"{XeroOAuth.API_URL}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.credentials.access_token}",
            "Xero-tenant-id": self.credentials.tenant_id,
            "Accept": "application/json",
        }

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if method == "GET":
                    response = await self._http_client.get(url, headers=headers, params=params)
                elif method == "POST":
                    response = await self._http_client.post(
                        url, headers=headers, json=data, params=params
                    )
                elif method == "PUT":
                    response = await self._http_client.put(
                        url, headers=headers, json=data, params=params
                    )
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if response.status_code in (200, 201):
                    return response.json()
                elif response.status_code == 400:
                    logger.error(f"Bad request: {response.text}")
                    raise ValueError(f"Xero API error: {response.text}")
                elif response.status_code == 401:
                    # Token might be stale
                    self.credentials.token_expires = datetime.utcnow()
                    retry_count += 1
                    continue
                elif response.status_code == 429:
                    # Rate limited
                    wait = int(response.headers.get("Retry-After", 60))
                    await asyncio.sleep(wait)
                    retry_count += 1
                    continue
                else:
                    logger.error(f"Xero API error {response.status_code}: {response.text}")
                    raise ValueError(f"Xero API error: {response.status_code}")

            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                retry_count += 1
                if retry_count < self.max_retries:
                    await asyncio.sleep(
                        self.backoff_factor ** retry_count
                    )
                else:
                    raise

        raise RuntimeError("Max retries exceeded")

    # ========================================================================
    # Revenue Sync (Inbound)
    # ========================================================================

    async def pull_revenue(
        self,
        snapshot_date: date,
        venue_id: str,
    ) -> RevenueSnapshot:
        """
        Pull daily revenue from Xero bank transactions.

        Fetches bank transactions for the date and categorizes by tracking category:
        - Food revenue
        - Beverage revenue
        - Gaming revenue
        - Function revenue

        Args:
            snapshot_date: Date to pull revenue for
            venue_id: RosterIQ venue ID

        Returns:
            RevenueSnapshot with categorized revenue
        """
        # Fetch bank transactions for the date
        # Xero uses DateString format: YYYY-MM-DD
        where_clause = f'Status=="AUTHORISED" AND DateString>="{snapshot_date}" AND DateString<"{snapshot_date + timedelta(days=1)}"'

        data = await self._make_request(
            "GET",
            "BankTransactions",
            params={"where": where_clause},
        )

        revenue_by_category = {
            "food": Decimal("0"),
            "beverage": Decimal("0"),
            "gaming": Decimal("0"),
            "function": Decimal("0"),
        }

        for txn in data.get("BankTransactions", []):
            line_items = txn.get("LineItems", [])
            for line in line_items:
                # Tracking category: TrackingCategoryName contains revenue stream
                category_name = line.get("TrackingCategoryName", "").lower()
                net_amount = Decimal(str(line.get("LineAmount", 0)))

                if "food" in category_name:
                    revenue_by_category["food"] += net_amount
                elif "beverage" in category_name or "drink" in category_name:
                    revenue_by_category["beverage"] += net_amount
                elif "gaming" in category_name or "game" in category_name:
                    revenue_by_category["gaming"] += net_amount
                elif "function" in category_name or "event" in category_name:
                    revenue_by_category["function"] += net_amount

        total_revenue = sum(revenue_by_category.values())
        gst = total_revenue / Decimal("11")  # Reverse GST: 10 / 110

        return RevenueSnapshot(
            venue_id=venue_id,
            snapshot_date=snapshot_date,
            total_revenue=total_revenue,
            food_revenue=revenue_by_category["food"],
            beverage_revenue=revenue_by_category["beverage"],
            gaming_revenue=revenue_by_category["gaming"],
            function_revenue=revenue_by_category["function"],
            gst_collected=gst,
            captured_at=datetime.utcnow(),
        )

    async def pull_revenue_range(
        self,
        start_date: date,
        end_date: date,
        venue_id: str,
    ) -> List[RevenueSnapshot]:
        """
        Pull revenue for a date range (backfill on first connect).

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            venue_id: RosterIQ venue ID

        Returns:
            List of RevenueSnapshot for each day
        """
        snapshots = []
        current = start_date

        while current <= end_date:
            try:
                snapshot = await self.pull_revenue(current, venue_id)
                snapshots.append(snapshot)
            except Exception as e:
                logger.error(f"Failed to pull revenue for {current}: {e}")

            current += timedelta(days=1)
            # Rate limit between days
            await asyncio.sleep(1)

        return snapshots

    # ========================================================================
    # Labour Cost Export (Outbound)
    # ========================================================================

    async def push_labour_costs(
        self,
        journal: LabourCostJournal,
    ) -> Dict[str, Any]:
        """
        Push labour cost journal entry to Xero.

        Creates a manual journal with:
        - Debit: Wages Expense (6000-6999 range)
        - Credit: Wages Payable (2000-2999 range)
        - Debit: Superannuation Expense (6000-6999 range)
        - Credit: Superannuation Payable (2000-2999 range)

        Breakdown by venue, role, and award level for audit trail.

        Args:
            journal: LabourCostJournal with cost breakdown

        Returns:
            Xero ManualJournal response
        """
        # Build line items: debit wages, credit wages payable
        line_items = []

        # Wages expense debit
        line_items.append({
            "Description": f"Wages Expense - {journal.journal_date.strftime('%Y-%m-%d')}",
            "LineAmount": float(journal.total_wages_expense),
            "AccountCode": "6000",  # Wages Expense
            "TrackingName": "Department",
            "TrackingOption": journal.breakdown.get("venue", {}).get("primary", "Default"),
        })

        # Wages payable credit
        line_items.append({
            "Description": "Wages Payable - Accrual",
            "LineAmount": -float(journal.total_wages_expense),
            "AccountCode": "2005",  # Wages Payable
        })

        # Superannuation expense debit (11.5% of gross)
        line_items.append({
            "Description": f"Superannuation Expense - {journal.journal_date.strftime('%Y-%m-%d')} (11.5%)",
            "LineAmount": float(journal.total_super_payable),
            "AccountCode": "6200",  # Superannuation Expense
        })

        # Superannuation payable credit
        line_items.append({
            "Description": "Superannuation Payable - Accrual",
            "LineAmount": -float(journal.total_super_payable),
            "AccountCode": "2010",  # Superannuation Payable
        })

        # Create manual journal
        manual_journal = {
            "Status": "DRAFT",  # Keep as draft for review before posting
            "LineItems": line_items,
            "Notes": journal.description or f"Labour costs for {journal.journal_date}",
        }

        response = await self._make_request(
            "POST",
            "ManualJournals",
            data={"ManualJournals": [manual_journal]},
        )

        return response

    # ========================================================================
    # Supplier Bill Push (Outbound)
    # ========================================================================

    async def push_bill(
        self,
        invoice: dict,
        account_code: str = "300",
        tax_type: str = "INPUT",
    ) -> dict:
        """
        Push a RosterIQ supplier invoice to Xero as a draft ACCPAY bill.

        Creates an accounts-payable invoice with:
        - One line item per invoice item (packs x pack cost, GST inclusive)
        - Contact matched by supplier name
        - Due date 14 days after the invoice date
        - Reference back to the RosterIQ invoice id for the audit trail

        Kept as DRAFT so the bookkeeper reviews before approval.

        Args:
            invoice: RosterIQ supplier invoice record (from list_supplier_invoices)
            account_code: Xero expense account for the lines (default "300")
            tax_type: Xero tax type for the lines (default "INPUT" — AU GST on expenses)

        Returns:
            {"xero_invoice_id", "xero_invoice_number", "status"} parsed from
            Xero's Invoices response
        """
        # Xero wants YYYY-MM-DD; the stored invoice_date may be a date, an ISO
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

        line_items = []
        for item in invoice.get("items", []) or []:
            name = item.get("name")
            packs = item.get("packs")
            pack_size = item.get("pack_size")
            unit = item.get("unit")
            line_items.append({
                "Description": f"{name} ({packs} x {pack_size}{unit or ''})",
                "Quantity": packs,
                "UnitAmount": item.get("pack_cost"),
                "AccountCode": account_code,
                "TaxType": tax_type,
            })

        bill = {
            "Type": "ACCPAY",
            "Contact": {"Name": invoice.get("supplier") or "Unknown supplier"},
            "Date": bill_date.strftime("%Y-%m-%d"),
            "DueDate": (bill_date + timedelta(days=14)).strftime("%Y-%m-%d"),
            "InvoiceNumber": invoice.get("invoice_number"),
            "Reference": f"RosterIQ {invoice.get('id')}",
            "Status": "DRAFT",  # Keep as draft for review before approval
            "LineAmountTypes": "Inclusive",
            "LineItems": line_items,
        }

        response = await self._make_request(
            "POST",
            "Invoices",
            data={"Invoices": [bill]},
        )

        pushed = response.get("Invoices") or []
        if not pushed:
            raise ValueError(
                f"Xero returned no invoice for bill {invoice.get('invoice_number')} "
                "— the bill was not created"
            )

        created = pushed[0]
        return {
            "xero_invoice_id": created.get("InvoiceID"),
            "xero_invoice_number": created.get("InvoiceNumber"),
            "status": created.get("Status"),
        }

    # ========================================================================
    # P&L Reporting
    # ========================================================================

    async def get_pnl_report(
        self,
        period_start: date,
        period_end: date,
    ) -> PnLReport:
        """
        Pull P&L report from Xero and calculate labour percentage.

        Args:
            period_start: Start date (inclusive)
            period_end: End date (inclusive)

        Returns:
            PnLReport with labour % calculation
        """
        # Xero Reports endpoint: ProfitAndLoss
        params = {
            "fromDate": period_start.strftime("%Y-%m-%d"),
            "toDate": period_end.strftime("%Y-%m-%d"),
        }

        data = await self._make_request(
            "GET",
            "Reports/ProfitAndLoss",
            params=params,
        )

        # Parse P&L report from Xero's nested structure
        # Xero returns rows with Title, Rows, Values
        rows = data.get("Rows", [])

        def find_account_total(title_pattern: str) -> Decimal:
            """Find account total by title pattern."""
            for row in rows:
                if title_pattern.lower() in row.get("Title", "").lower():
                    values = row.get("Values", [])
                    if values:
                        try:
                            return Decimal(str(values[0].get("Value", 0)))
                        except (ValueError, TypeError, KeyError, IndexError):
                            return Decimal("0")
            return Decimal("0")

        total_revenue = find_account_total("Revenue") or find_account_total("Sales")
        cogs = find_account_total("Cost of Goods Sold") or find_account_total("COGS")
        labour_costs = (
            find_account_total("Wages") + find_account_total("Superannuation")
        )
        other_expenses = find_account_total("Expenses") - labour_costs

        gross_profit = total_revenue - cogs
        net_profit = gross_profit - labour_costs - other_expenses

        labour_pct = (
            (labour_costs / total_revenue * 100)
            if total_revenue > 0
            else Decimal("0")
        )

        return PnLReport(
            venue_id="",  # Set by caller
            period_start=period_start,
            period_end=period_end,
            total_revenue=total_revenue,
            cogs=cogs,
            gross_profit=gross_profit,
            labour_costs=labour_costs,
            labour_percentage=labour_pct,
            other_expenses=other_expenses,
            net_profit=net_profit,
            generated_at=datetime.utcnow(),
        )


# API-facing alias: newer integrations (supplier bill push) construct the client
# under this name — same class as XeroClient above.
XeroApiClient = XeroClient


# ============================================================================
# Database Integration (credentials storage)
# ============================================================================

async def save_xero_credentials(db, credentials: XeroCredentials) -> None:
    """
    Save Xero credentials to database.

    For PostgreSQL, uses xero_credentials table (to be created).
    For in-memory, stores in dict.
    """
    # PostgreSQL
    if hasattr(db, "_conn"):
        with db._cursor() as cur:
            cur.execute("""
                INSERT INTO xero_credentials
                    (venue_id, client_id, client_secret, tenant_id, access_token,
                     refresh_token, token_expires, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET
                    access_token=EXCLUDED.access_token,
                    refresh_token=EXCLUDED.refresh_token,
                    token_expires=EXCLUDED.token_expires,
                    updated_at=EXCLUDED.updated_at
            """, (
                credentials.venue_id,
                credentials.client_id,
                credentials.client_secret,
                credentials.tenant_id,
                credentials.access_token,
                credentials.refresh_token,
                credentials.token_expires,
                credentials.created_at,
                credentials.updated_at,
            ))
    else:
        # In-memory
        if not hasattr(db, "_xero_credentials"):
            db._xero_credentials = {}
        db._xero_credentials[credentials.venue_id] = credentials


async def get_xero_credentials(db, venue_id: str) -> Optional[XeroCredentials]:
    """Retrieve Xero credentials from database."""
    # PostgreSQL
    if hasattr(db, "_conn"):
        with db._cursor() as cur:
            cur.execute(
                "SELECT * FROM xero_credentials WHERE venue_id = %s",
                (venue_id,)
            )
            row = cur.fetchone()
            if row:
                return XeroCredentials(
                    venue_id=row["venue_id"],
                    client_id=row["client_id"],
                    client_secret=row["client_secret"],
                    tenant_id=row["tenant_id"],
                    access_token=row["access_token"],
                    refresh_token=row["refresh_token"],
                    token_expires=row["token_expires"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
    else:
        # In-memory
        if hasattr(db, "_xero_credentials"):
            return db._xero_credentials.get(venue_id)

    return None


async def delete_xero_credentials(db, venue_id: str) -> None:
    """Delete Xero credentials (on disconnect)."""
    # PostgreSQL
    if hasattr(db, "_conn"):
        with db._cursor() as cur:
            cur.execute("DELETE FROM xero_credentials WHERE venue_id = %s", (venue_id,))
    else:
        # In-memory
        if hasattr(db, "_xero_credentials"):
            db._xero_credentials.pop(venue_id, None)
