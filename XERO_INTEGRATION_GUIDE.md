# Xero Integration Guide for RosterIQ

This guide covers the Xero accounting integration module for RosterIQ, enabling bi-directional sync of financial data with Australian hospitality venues.

## Overview

The Xero integration provides:

1. **Revenue Sync (Inbound)** — Pull daily revenue from Xero bank transactions
2. **Labour Cost Export (Outbound)** — Push daily wages journals to Xero
3. **P&L Reporting** — Calculate labour % of revenue for dashboard
4. **OAuth2 PKCE Security** — Industry-standard token-based authorization
5. **Rate Limiting & Retry** — 60 req/min with exponential backoff
6. **Australian Compliance** — GST 10%, super 11.5%, BAS periods

## Architecture

### Files

- **`xero_integration.py`** — Core client, models, OAuth2 flow
- **`xero_routes.py`** — FastAPI routes for OAuth callback, sync, reporting
- **`schema.sql`** — PostgreSQL tables for credentials, revenue snapshots, labour journals
- **`database.py`** — PostgreSQL methods for credential storage

### Key Classes

#### XeroOAuth
Handles OAuth2 PKCE flow for secure authorization:
- `generate_authorize_url()` — Returns URL to redirect user to Xero login
- `exchange_code(code, state, code_verifier)` — Exchanges auth code for tokens
- `refresh_access_token(credentials)` — Refreshes expired tokens

#### XeroClient
Main async HTTP client for Xero API:
- `pull_revenue(date, venue_id)` — Fetch daily revenue
- `pull_revenue_range(start, end, venue_id)` — Backfill last 90 days
- `push_labour_costs(journal)` — Export wages journal
- `get_pnl_report(start, end)` — Pull P&L with labour %

#### Models
- `XeroCredentials` — OAuth tokens + tenant info
- `RevenueSnapshot` — Categorized daily revenue
- `LabourCostJournal` — Wages + super breakdown
- `PnLReport` — Financial metrics with labour calculations

## Setup

### 1. Xero API Application

Create an app in the Xero Developer Portal:

1. Go to https://developer.xero.com/
2. Create an OAuth2 app (choose "Web")
3. Configure OAuth scopes:
   - `offline_access` (refresh tokens)
   - `openid`, `profile`, `email` (identity)
   - `accounting` (read/write invoices, journals)
4. Set redirect URI: `https://rosteriq.com.au/api/xero/callback`
5. Note client_id and client_secret

### 2. Environment Variables

```bash
export XERO_CLIENT_ID="..."
export XERO_CLIENT_SECRET="..."
export XERO_REDIRECT_URI="https://rosteriq.com.au/api/xero/callback"
```

### 3. Database Setup

```bash
psql -d rosteriq -f schema.sql
```

This creates:
- `xero_credentials` — OAuth tokens (encrypted in production)
- `xero_revenue_snapshots` — Daily revenue by category
- `xero_labour_journals` — Wages journals pushed to Xero

### 4. Register Routes

In `api.py`:

```python
from rosteriq.xero_routes import setup_xero_routes

app = FastAPI(...)
db = get_db()

# ... other setup ...

setup_xero_routes(app, db)
```

## API Endpoints

### OAuth2 Flow

#### 1. Initiate Authorization

```bash
POST /api/xero/connect
Content-Type: application/json

{
  "venue_id": "v1"
}
```

Response:
```json
{
  "authorize_url": "https://login.xero.com/identity/connect/authorize?...",
  "state": "abc123..."
}
```

Frontend redirects user to `authorize_url`. Xero prompts to log in and authorize.

#### 2. OAuth Callback

Xero redirects back to:

```
GET /api/xero/callback?code=...&state=...
```

**Note:** In production, you'll need to:
1. Store `code_verifier` in the session when calling `/connect`
2. Retrieve `code_verifier` in the callback to exchange the code
3. Currently the code_verifier is empty — implement session storage

Response:
```json
{
  "status": "success",
  "message": "Xero connected for venue v1",
  "venue_id": "v1",
  "tenant_id": "00000000-0000-0000-0000-000000000000"
}
```

### Connection Management

#### Check Status

```bash
GET /api/xero/status/v1
```

Response:
```json
{
  "venue_id": "v1",
  "connected": true,
  "client_id": "...",
  "tenant_id": "...",
  "token_expires": "2026-04-24T10:30:00+00:00",
  "last_synced": null
}
```

#### Disconnect

```bash
POST /api/xero/disconnect/v1
```

### Revenue Sync

#### Manual Sync

```bash
POST /api/xero/sync/revenue
Content-Type: application/json

{
  "venue_id": "v1",
  "start_date": "2026-04-15",
  "end_date": "2026-04-23"
}
```

Response:
```json
{
  "venue_id": "v1",
  "snapshots_count": 9,
  "total_revenue": 45000.00,
  "period_start": "2026-04-15",
  "period_end": "2026-04-23"
}
```

**On first connect:** Schedule a backfill of last 90 days:

```python
async with XeroClient(credentials, db) as xero:
    today = date.today()
    ninety_days_ago = today - timedelta(days=90)
    snapshots = await xero.pull_revenue_range(ninety_days_ago, today, venue_id)
    # Save snapshots to database
```

#### Revenue Categories

The revenue sync maps Xero tracking categories to:
- **Food revenue** — "food" in tracking category name
- **Beverage revenue** — "beverage", "drink" in name
- **Gaming revenue** — "gaming", "game" in name
- **Function revenue** — "function", "event" in name

GST is reversed: `GST = Total Revenue / 11` (removes the 10% component)

### Labour Cost Export

#### Push Wages Journal

```bash
POST /api/xero/sync/labour-costs
Content-Type: application/json

{
  "venue_id": "v1",
  "journal_date": "2026-04-23"
}
```

**Current:** Placeholder response (awaiting payroll data hookup)

**When integrated with RosterIQ payroll:**

1. Fetch labour costs from today's roster/shifts:
   ```python
   shifts = roster.shifts  # for today
   labour_breakdown = {
       "venue": {"v1": Decimal("1200.50")},
       "roles": {"bartender": Decimal("300"), "chef": Decimal("900.50")},
   }
   ```

2. Build labour journal with:
   - **Wages Expense** (debit, account 6000) — total wages
   - **Wages Payable** (credit, account 2005) — accrual
   - **Super Expense** (debit, account 6200) — 11.5% of wages
   - **Super Payable** (credit, account 2010) — accrual

3. Push to Xero as draft journal for review

Example journal entry (daily):

| Account | Description | Debit | Credit |
|---------|-------------|-------|--------|
| 6000 | Wages Expense - 2026-04-23 | $1,200.50 | |
| 2005 | Wages Payable | | $1,200.50 |
| 6200 | Superannuation Expense (11.5%) | $138.06 | |
| 2010 | Superannuation Payable | | $138.06 |

**Note:** Journals stay in draft until approved in Xero, avoiding double-posting errors.

### P&L Reporting

#### Get Labour % Metrics

```bash
GET /api/xero/pnl/v1?period_days=30
```

Response:
```json
{
  "venue_id": "v1",
  "period_start": "2026-03-24",
  "period_end": "2026-04-23",
  "total_revenue": 95000.00,
  "cogs": 28500.00,
  "gross_profit": 66500.00,
  "labour_costs": 28750.00,
  "labour_percentage": 30.26,
  "other_expenses": 15000.00,
  "net_profit": 22750.00
}
```

Used on the RosterIQ dashboard to show labour efficiency. Target for Australian hospitality: 28–32% of revenue.

## Implementation Details

### Token Management

Tokens auto-refresh when expired:

```python
async with XeroClient(credentials, db) as xero:
    # _ensure_token_valid() runs before each API call
    revenue = await xero.pull_revenue(date.today(), "v1")
    # If token expired, refresh happens transparently
```

### Rate Limiting

Xero allows 60 requests per 60 seconds. The client enforces this:

```python
limiter = RateLimiter(max_requests=60, window_seconds=60)
await limiter.acquire()  # Waits if needed
```

### Retry Logic

Failed requests retry up to 3 times with exponential backoff:

```python
# 401 (expired token) → refresh → retry
# 429 (rate limited) → wait Retry-After → retry
# Other 5xx → exponential backoff (0.5^n seconds)
```

### Error Handling

```python
try:
    async with XeroClient(credentials, db) as xero:
        revenue = await xero.pull_revenue(date.today(), "v1")
except ValueError as e:
    # "Xero API error: 400" or "Xero token exchange failed"
    log_error(e)
except RuntimeError:
    # Max retries exceeded
    notify_admin()
```

## Australian Compliance

### Superannuation (11.5%)

Super is calculated as:
```
Super = Gross Wages × 11.5%
```

No super on wages below the superannuation guarantee threshold (~$20,800/year).

### GST (10%)

Xero typically returns *inclusive* revenue (inc. GST). To extract GST:
```
GST = Total Revenue / 11
GST-Exclusive Revenue = Total Revenue - GST
```

### BAS (Business Activity Statement)

BAS is quarterly (Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec). Xero tracks:
- GST collected (from sales)
- GST paid (on expenses)
- PAYG tax withheld
- Payroll deductions

This integration doesn't directly file BAS, but provides the data.

### Fair Work Compliance

The labour journal should track:
- Award level (e.g., Level 1–4 Hospitality Award)
- Penalty rates (weekends, public holidays)
- Overtime (1.5x or 2x rates)
- Minimum engagement penalties

Currently, RosterIQ's `cost_calculator.py` handles this. The journal export should include breakdown by award level for audit trail.

## Testing

### Unit Tests

```python
# tests/test_xero_integration.py

@pytest.mark.asyncio
async def test_oauth_flow():
    oauth = XeroOAuth(client_id, client_secret, redirect_uri)
    url, state = oauth.generate_authorize_url()
    assert "login.xero.com" in url
    assert len(state) > 20

@pytest.mark.asyncio
async def test_rate_limiter():
    limiter = RateLimiter(max_requests=2, window_seconds=1)
    start = time.time()
    for i in range(3):
        await limiter.acquire()
    elapsed = time.time() - start
    assert elapsed > 1.0  # 3rd request waited

@pytest.mark.asyncio
async def test_revenue_sync():
    credentials = XeroCredentials(...)
    async with XeroClient(credentials, db) as xero:
        snapshot = await xero.pull_revenue(date.today(), "v1")
        assert snapshot.total_revenue > 0
        assert snapshot.gst_collected > 0
```

### Integration Tests

```bash
# Live Xero sandbox
export XERO_CLIENT_ID="sandbox_client_id"
export XERO_CLIENT_SECRET="sandbox_secret"
python -m pytest tests/test_xero_integration.py -v
```

## Troubleshooting

### "Token exchange failed: 400"

**Cause:** Invalid code_verifier or missing code_verifier in callback

**Fix:** Implement session storage for code_verifier:
```python
session["code_verifier"] = secrets.token_urlsafe(32)
# In callback:
code_verifier = session.pop("code_verifier")
credentials = await oauth.exchange_code(code, state, code_verifier)
```

### "Xero API error 401"

**Cause:** Token expired or invalid

**Fix:** Check `token_expires` timestamp. Client should auto-refresh, but if not:
```python
if datetime.utcnow() >= credentials.token_expires:
    credentials = await oauth.refresh_access_token(credentials)
```

### "Xero API error 429"

**Cause:** Rate limited (>60 requests per 60 seconds)

**Fix:** Client handles this automatically with backoff. Check logs for:
```
"Xero API error 429: Rate Limited"
```

### "BankTransactions endpoint empty"

**Cause:** Wrong account or date range

**Fix:**
1. Check Xero UI: Banking → Transactions
2. Verify bank account is linked in Xero
3. Ensure date range has transactions
4. Check where clause: `DateString>="2026-04-23"`

## Future Enhancements

1. **Webhook Integration** — Xero webhooks on invoice/transaction changes
2. **Real-time Sync** — Push labour costs immediately after shift completion
3. **BAS Automation** — Auto-file quarterly BAS with ATO
4. **Multi-entity** — Support venue groups with consolidated reporting
5. **Forecasting** — Use historical revenue for demand prediction
6. **Cost Optimization** — Suggest staffing levels to meet labour % targets
7. **Custom Reports** — Revenue by shift, employee productivity ratios

## References

- Xero API Docs: https://developer.xero.com/documentation/apis/accounting/accounting-api-overview/
- OAuth2 PKCE: https://tools.ietf.org/html/rfc7636
- Fair Work Hospitality Award: https://www.fairwork.gov.au/awards-and-agreements/awards/find-my-award
- Australian Super: https://www.australiansuper.com.au/

---

**Author:** RosterIQ Team  
**Updated:** 2026-04-23  
**Status:** Production Ready (code_verifier session storage pending)
