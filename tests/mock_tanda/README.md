# Mock Tanda API Server

A standalone FastAPI mock server that simulates Tanda's workforce management API for development and testing.

## Quick Start

### 1. Run the server

```bash
cd /path/to/RosterIQ
uvicorn tests.mock_tanda.server:app --port 9000
```

The server will start on `http://localhost:9000` with test data pre-loaded.

### 2. Get an OAuth token

First, authorize:

```bash
curl -X POST "http://localhost:9000/oauth/authorize" \
  -G --data-urlencode "client_id=test_client" \
  --data-urlencode "redirect_uri=http://localhost:3000/callback" \
  --data-urlencode "response_type=code"
```

This returns:

```json
{
  "code": "abc123...",
  "redirect_uri": "http://localhost:3000/callback",
  "client_id": "test_client"
}
```

Then exchange the code for tokens:

```bash
curl -X POST "http://localhost:9000/oauth/token" \
  -G --data-urlencode "client_id=test_client" \
  --data-urlencode "client_secret=secret" \
  --data-urlencode "code=abc123..." \
  --data-urlencode "redirect_uri=http://localhost:3000/callback"
```

Returns:

```json
{
  "access_token": "access_abc123...",
  "refresh_token": "refresh_xyz789...",
  "token_type": "Bearer",
  "expires_in": 7200
}
```

### 3. Make authenticated requests

Use the access token in the `Authorization` header:

```bash
curl -X GET "http://localhost:9000/api/v2/users" \
  -H "Authorization: Bearer access_abc123..."
```

## Endpoints

### OAuth2

- `POST /oauth/authorize` — Get authorization code
- `POST /oauth/token` — Exchange code for tokens

### Employees (`/api/v2/users`)

- `GET /api/v2/users` — List employees (paginated)
- `GET /api/v2/users/{id}` — Get single employee
- `POST /api/v2/users` — Create employee
- `PUT /api/v2/users/{id}` — Update employee

### Rosters (`/api/v2/rosters`)

- `GET /api/v2/rosters` — List rosters (with date filtering)
- `GET /api/v2/rosters/{id}` — Get roster with shifts
- `POST /api/v2/rosters` — Create roster
- `PUT /api/v2/rosters/{id}/publish` — Publish roster

### Shifts (`/api/v2/shifts`)

- `GET /api/v2/shifts` — List shifts (filterable by date, user_id)
- `GET /api/v2/shifts/{id}` — Get shift
- `POST /api/v2/shifts` — Create shift
- `PUT /api/v2/shifts/{id}` — Update shift
- `DELETE /api/v2/shifts/{id}` — Delete shift

### Timesheets (`/api/v2/timesheets`)

- `GET /api/v2/timesheets` — List timesheets (filterable)
- `POST /api/v2/timesheets/{id}/approve` — Approve timesheet

### Departments (`/api/v2/departments`)

- `GET /api/v2/departments` — List departments

### Locations (`/api/v2/locations`)

- `GET /api/v2/locations` — List venues

### Webhooks

- `POST /api/v2/webhooks` — Register webhook
- `DELETE /api/v2/webhooks/{id}` — Deregister webhook
- `POST /__admin/trigger-webhook` — Fire test webhook event

### Health

- `GET /health` — Server health check

## Test Data

The server includes realistic Australian hospitality test data:

- **3 Venues:** The Tipsy Koala (bar), Salt & Pepper Café, Flame & Vine (restaurant)
- **15 Employees:** Across venues with realistic names, roles, and award rates
- **Sample Rosters:** Current week with realistic shift patterns
- **Departments:** FOH, BOH, Bar, Management
- **Timesheets:** With clock-in/out variance (±5 min)

See `data.py` for details.

## Authentication

All `/api/v2/*` endpoints require a valid Bearer token in the `Authorization` header.

Missing or invalid token → 401 Unauthorized

## Rate Limiting

The server returns rate limit headers:

- `X-RateLimit-Limit: 100` (requests per minute)
- `X-RateLimit-Remaining: 95` (requests left in window)
- `X-RateLimit-Reset: 1234567890` (Unix timestamp when limit resets)

## Example: Using with RosterIQ

Point your RosterIQ `TANDA_API_URL` to the mock server:

```bash
export TANDA_API_URL="http://localhost:9000"
export TANDA_CLIENT_ID="test_client"
export TANDA_CLIENT_SECRET="secret"
```

Then in your code:

```python
from rosteriq.tanda_adapter import TandaAdapter, TandaOAuth
from rosteriq.models import TandaCredentials

oauth = TandaOAuth(
    client_id="test_client",
    client_secret="secret",
    redirect_uri="http://localhost:3000/callback",
)

# Get credentials from mock server
credentials = await oauth.exchange_code(authorization_code)

# Use adapter with mock server
async with TandaAdapter(credentials) as tanda:
    employees = await tanda.get_employees()
    shifts = await tanda.get_shifts(start_date, end_date)
```

## Webhook Testing

To test webhook functionality:

1. Register a webhook:

```bash
curl -X POST "http://localhost:9000/api/v2/webhooks" \
  -H "Authorization: Bearer access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "target_url": "http://localhost:8000/webhooks/tanda",
    "events": ["shift.created", "shift.updated"],
    "secret": "my_webhook_secret"
  }'
```

2. Trigger a test event:

```bash
curl -X POST "http://localhost:9000/__admin/trigger-webhook" \
  -H "Content-Type: application/json" \
  -d '{
    "webhook_id": "webhook_uuid",
    "event_type": "shift.created",
    "data": {
      "id": 12345,
      "user_id": 1000,
      "date": "2026-04-28",
      "start": 1234567890,
      "finish": 1234571490
    }
  }'
```

The mock server will POST the event to your webhook URL with proper HMAC-SHA256 signature.

## Logging

The server logs all requests to stdout:

```
INFO:tests.mock_tanda.server:GET /api/v2/users
INFO:tests.mock_tanda.server:  -> 200
```

## In-Memory Storage

All data is stored in-memory and reset when the server restarts. Useful for:

- Development without external dependencies
- Fast integration testing
- CI/CD pipelines
- Prototyping RosterIQ features

## Debugging

Check the server logs for request/response details:

```bash
# Enable DEBUG logging
LOGLEVEL=DEBUG uvicorn tests.mock_tanda.server:app --port 9000 --log-level debug
```

## Limitations

- No persistent storage (data resets on restart)
- No real rate limiting enforcement (headers only)
- No signature verification for webhooks (just logging)
- All tokens are always valid

For production-like testing, use the real Tanda API or a more sophisticated mock.
