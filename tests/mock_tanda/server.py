"""
Standalone mock Tanda API server for development and testing.

Simulates Tanda's workforce management API with realistic endpoints:
- OAuth2 token exchange
- Employee/user management
- Roster and shift management
- Timesheet tracking
- Webhook registration and triggering

Run independently:
    uvicorn tests.mock_tanda.server:app --port 9000

All data is stored in-memory as dicts. Requests are logged to stdout.
"""

import json
import logging
from datetime import datetime, timedelta, date as date_type
from decimal import Decimal
from typing import Dict, List, Any, Optional
from uuid import uuid4
import hmac
import hashlib

from fastapi import FastAPI, HTTPException, Header, Query, Request, BackgroundTasks
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel

from .data import generate_mock_data

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================


class TokenResponse(BaseModel):
    """OAuth2 token response."""

    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int = 7200


class ErrorResponse(BaseModel):
    """API error response."""

    error: str
    message: str
    status_code: int


# ============================================================================
# MOCK SERVER STATE
# ============================================================================


class MockTandaServer:
    """In-memory Tanda API state."""

    def __init__(self):
        """Initialize mock server with test data."""
        data = generate_mock_data()

        # Store all data
        self.venues = {v["id"]: v for v in data["venues"]}
        self.departments = {d["id"]: d for d in data["departments"]}
        self.employees = {e["id"]: e for e in data["employees"]}
        self.shifts = {s["id"]: s for s in data["shifts"]}
        self.rosters = {r["id"]: r for r in data["rosters"]}
        self.timesheets = {ts["id"]: ts for ts in data["timesheets"]}
        self.webhooks: Dict[str, Dict[str, Any]] = {}

        # OAuth state
        self.oauth_codes: Dict[str, Dict[str, Any]] = {}
        self.tokens: Dict[str, Dict[str, Any]] = {}

        # Request tracking
        self.request_count = 0
        self.rate_limit = 100  # requests per minute
        self.rate_limit_window = 60  # seconds

    def get_rate_limit_headers(self) -> Dict[str, str]:
        """Return rate limit headers."""
        return {
            "X-RateLimit-Limit": str(self.rate_limit),
            "X-RateLimit-Remaining": str(max(0, self.rate_limit - self.request_count % self.rate_limit)),
            "X-RateLimit-Reset": str(int((datetime.now() + timedelta(seconds=self.rate_limit_window)).timestamp())),
        }

    def is_valid_token(self, token: str) -> bool:
        """Validate an access token."""
        if not token:
            return False
        if token.startswith("Bearer "):
            token = token[7:]
        return token in self.tokens

    def get_bearer_token(self, auth_header: Optional[str]) -> Optional[str]:
        """Extract and validate Bearer token from Authorization header."""
        if not auth_header:
            return None
        if not auth_header.startswith("Bearer "):
            return None
        token = auth_header[7:]
        return token if self.is_valid_token(token) else None


# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(title="Mock Tanda API", version="1.0.0")
server = MockTandaServer()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests and responses."""
    logger.info(f"{request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"  -> {response.status_code}")
    return response


@app.middleware("http")
async def add_rate_limit_headers(request: Request, call_next):
    """Add rate limit headers to all responses."""
    server.request_count += 1
    response = await call_next(request)
    headers = server.get_rate_limit_headers()
    for k, v in headers.items():
        response.headers[k] = v
    return response


# ============================================================================
# OAUTH2 ENDPOINTS
# ============================================================================


@app.post("/oauth/authorize")
async def oauth_authorize(
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    response_type: str = Query("code"),
    scope: str = Query(""),
):
    """
    Mock OAuth authorization endpoint.

    Returns a mock authorization code (in real flow, redirects to redirect_uri).
    For testing, just return the code directly.
    """
    auth_code = str(uuid4())
    server.oauth_codes[auth_code] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "created_at": datetime.now().isoformat(),
    }

    logger.info(f"OAuth authorize: code={auth_code}, client={client_id}")

    return {
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
    }


@app.post("/oauth/token")
async def oauth_token(
    client_id: str = Query(...),
    client_secret: str = Query(...),
    code: str = Query(...),
    redirect_uri: str = Query(...),
    grant_type: str = Query("authorization_code"),
):
    """
    Mock OAuth token exchange endpoint.

    Exchanges authorization code for access/refresh tokens.
    """
    if code not in server.oauth_codes:
        raise HTTPException(status_code=400, detail="Invalid authorization code")

    auth_data = server.oauth_codes[code]
    if auth_data["client_id"] != client_id:
        raise HTTPException(status_code=400, detail="Client ID mismatch")

    # Generate tokens
    access_token = f"access_{uuid4()}"
    refresh_token = f"refresh_{uuid4()}"

    server.tokens[access_token] = {
        "client_id": client_id,
        "type": "access",
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(hours=2)).isoformat(),
    }

    server.tokens[refresh_token] = {
        "client_id": client_id,
        "type": "refresh",
        "created_at": datetime.now().isoformat(),
    }

    # Remove used code
    del server.oauth_codes[code]

    logger.info(f"OAuth token: client={client_id}, access_token={access_token[:20]}...")

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=7200,
    )


# ============================================================================
# AUTHENTICATION HELPER
# ============================================================================


def require_auth(authorization: Optional[str] = Header(None)) -> str:
    """Validate authorization header and return token."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = server.get_bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return token


# ============================================================================
# EMPLOYEES / USERS ENDPOINTS
# ============================================================================


@app.get("/api/v2/users")
async def list_employees(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    authorization: Optional[str] = Header(None),
):
    """
    List all employees with pagination.

    Returns paginated employee data.
    """
    require_auth(authorization)

    employees_list = list(server.employees.values())
    total = len(employees_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "users": employees_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


@app.get("/api/v2/users/{user_id}")
async def get_employee(
    user_id: int,
    authorization: Optional[str] = Header(None),
):
    """Get a single employee by ID."""
    require_auth(authorization)

    if user_id not in server.employees:
        raise HTTPException(status_code=404, detail="Employee not found")

    return server.employees[user_id]


@app.post("/api/v2/users")
async def create_employee(
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Create a new employee."""
    require_auth(authorization)

    data = await request.json()

    # Generate new ID
    new_id = max(server.employees.keys()) + 1 if server.employees else 1000

    employee = {
        "id": new_id,
        **data,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }

    server.employees[new_id] = employee

    logger.info(f"Created employee: {new_id}")

    return employee


@app.put("/api/v2/users/{user_id}")
async def update_employee(
    user_id: int,
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Update an employee."""
    require_auth(authorization)

    if user_id not in server.employees:
        raise HTTPException(status_code=404, detail="Employee not found")

    data = await request.json()

    employee = server.employees[user_id]
    employee.update(data)
    employee["updated_at"] = datetime.now().isoformat()

    logger.info(f"Updated employee: {user_id}")

    return employee


# ============================================================================
# ROSTERS ENDPOINTS
# ============================================================================


@app.get("/api/v2/rosters")
async def list_rosters(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    authorization: Optional[str] = Header(None),
):
    """
    List rosters with optional date filtering.

    Query params:
    - from: Start date (ISO format)
    - to: End date (ISO format)
    """
    require_auth(authorization)

    rosters_list = list(server.rosters.values())

    # Filter by date range if provided
    if from_date and to_date:
        from_d = date_type.fromisoformat(from_date)
        to_d = date_type.fromisoformat(to_date)
        rosters_list = [
            r for r in rosters_list
            if date_type.fromisoformat(r["week_start"]) <= to_d
            and date_type.fromisoformat(r["week_end"]) >= from_d
        ]

    total = len(rosters_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "rosters": rosters_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


@app.get("/api/v2/rosters/{roster_id}")
async def get_roster(
    roster_id: str,
    authorization: Optional[str] = Header(None),
):
    """Get a single roster with its shifts."""
    require_auth(authorization)

    if roster_id not in server.rosters:
        raise HTTPException(status_code=404, detail="Roster not found")

    return server.rosters[roster_id]


@app.post("/api/v2/rosters")
async def create_roster(
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Create a new roster."""
    require_auth(authorization)

    data = await request.json()

    roster_id = str(uuid4())
    roster = {
        "id": roster_id,
        **data,
        "created_at": datetime.now().isoformat(),
    }

    server.rosters[roster_id] = roster

    logger.info(f"Created roster: {roster_id}")

    return roster


@app.put("/api/v2/rosters/{roster_id}/publish")
async def publish_roster(
    roster_id: str,
    authorization: Optional[str] = Header(None),
):
    """Publish a roster (mark as final)."""
    require_auth(authorization)

    if roster_id not in server.rosters:
        raise HTTPException(status_code=404, detail="Roster not found")

    roster = server.rosters[roster_id]
    roster["status"] = "published"
    roster["published_at"] = datetime.now().isoformat()

    logger.info(f"Published roster: {roster_id}")

    return roster


# ============================================================================
# SHIFTS ENDPOINTS
# ============================================================================


@app.get("/api/v2/shifts")
async def list_shifts(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    authorization: Optional[str] = Header(None),
):
    """
    List shifts with optional filtering.

    Query params:
    - from: Start date (ISO format)
    - to: End date (ISO format)
    - user_id: Filter by employee ID
    """
    require_auth(authorization)

    shifts_list = list(server.shifts.values())

    # Filter by date range
    if from_date and to_date:
        from_d = date_type.fromisoformat(from_date)
        to_d = date_type.fromisoformat(to_date)
        shifts_list = [
            s for s in shifts_list
            if date_type.fromisoformat(s["date"]) <= to_d
            and date_type.fromisoformat(s["date"]) >= from_d
        ]

    # Filter by user
    if user_id:
        shifts_list = [s for s in shifts_list if s["user_id"] == user_id]

    total = len(shifts_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "shifts": shifts_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


@app.get("/api/v2/shifts/{shift_id}")
async def get_shift(
    shift_id: int,
    authorization: Optional[str] = Header(None),
):
    """Get a single shift."""
    require_auth(authorization)

    if shift_id not in server.shifts:
        raise HTTPException(status_code=404, detail="Shift not found")

    return server.shifts[shift_id]


@app.post("/api/v2/shifts")
async def create_shift(
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Create a new shift."""
    require_auth(authorization)

    data = await request.json()

    # Generate new ID
    new_id = max(server.shifts.keys()) + 1 if server.shifts else 1

    shift = {
        "id": new_id,
        **data,
        "created_at": datetime.now().isoformat(),
    }

    server.shifts[new_id] = shift

    logger.info(f"Created shift: {new_id}")

    return shift


@app.put("/api/v2/shifts/{shift_id}")
async def update_shift(
    shift_id: int,
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Update a shift."""
    require_auth(authorization)

    if shift_id not in server.shifts:
        raise HTTPException(status_code=404, detail="Shift not found")

    data = await request.json()

    shift = server.shifts[shift_id]
    shift.update(data)
    shift["updated_at"] = datetime.now().isoformat()

    logger.info(f"Updated shift: {shift_id}")

    return shift


@app.delete("/api/v2/shifts/{shift_id}")
async def delete_shift(
    shift_id: int,
    authorization: Optional[str] = Header(None),
):
    """Delete a shift."""
    require_auth(authorization)

    if shift_id not in server.shifts:
        raise HTTPException(status_code=404, detail="Shift not found")

    del server.shifts[shift_id]

    logger.info(f"Deleted shift: {shift_id}")

    return {"status": "deleted", "id": shift_id}


# ============================================================================
# TIMESHEETS ENDPOINTS
# ============================================================================


@app.get("/api/v2/timesheets")
async def list_timesheets(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    authorization: Optional[str] = Header(None),
):
    """
    List timesheets with optional filtering.

    Query params:
    - from: Start date (ISO format)
    - to: End date (ISO format)
    - user_id: Filter by employee ID
    """
    require_auth(authorization)

    timesheets_list = list(server.timesheets.values())

    # Filter by date range
    if from_date and to_date:
        from_d = date_type.fromisoformat(from_date)
        to_d = date_type.fromisoformat(to_date)
        timesheets_list = [
            ts for ts in timesheets_list
            if date_type.fromisoformat(ts["date"]) <= to_d
            and date_type.fromisoformat(ts["date"]) >= from_d
        ]

    # Filter by user
    if user_id:
        timesheets_list = [ts for ts in timesheets_list if ts["user_id"] == user_id]

    total = len(timesheets_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "timesheets": timesheets_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


@app.post("/api/v2/timesheets/{timesheet_id}/approve")
async def approve_timesheet(
    timesheet_id: int,
    authorization: Optional[str] = Header(None),
):
    """Approve a timesheet."""
    require_auth(authorization)

    if timesheet_id not in server.timesheets:
        raise HTTPException(status_code=404, detail="Timesheet not found")

    timesheet = server.timesheets[timesheet_id]
    timesheet["status"] = "approved"
    timesheet["approved_at"] = datetime.now().isoformat()

    logger.info(f"Approved timesheet: {timesheet_id}")

    return timesheet


# ============================================================================
# DEPARTMENTS ENDPOINTS
# ============================================================================


@app.get("/api/v2/departments")
async def list_departments(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    authorization: Optional[str] = Header(None),
):
    """List all departments."""
    require_auth(authorization)

    departments_list = list(server.departments.values())
    total = len(departments_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "departments": departments_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


# ============================================================================
# LOCATIONS/VENUES ENDPOINTS
# ============================================================================


@app.get("/api/v2/locations")
async def list_locations(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    authorization: Optional[str] = Header(None),
):
    """List all venue locations."""
    require_auth(authorization)

    venues_list = list(server.venues.values())
    total = len(venues_list)
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "locations": venues_list[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    }


# ============================================================================
# WEBHOOKS ENDPOINTS
# ============================================================================


@app.post("/api/v2/webhooks")
async def register_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """
    Register a webhook subscription.

    Expected JSON:
    {
        "target_url": "https://example.com/webhook",
        "events": ["shift.created", "shift.updated"],
        "secret": "optional_webhook_secret"
    }
    """
    require_auth(authorization)

    data = await request.json()

    webhook_id = str(uuid4())
    webhook = {
        "id": webhook_id,
        "target_url": data.get("target_url"),
        "events": data.get("events", []),
        "secret": data.get("secret", ""),
        "active": True,
        "created_at": datetime.now().isoformat(),
    }

    server.webhooks[webhook_id] = webhook

    logger.info(f"Registered webhook: {webhook_id} -> {webhook['target_url']}")

    return webhook


@app.delete("/api/v2/webhooks/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    authorization: Optional[str] = Header(None),
):
    """Deregister a webhook."""
    require_auth(authorization)

    if webhook_id not in server.webhooks:
        raise HTTPException(status_code=404, detail="Webhook not found")

    del server.webhooks[webhook_id]

    logger.info(f"Deleted webhook: {webhook_id}")

    return {"status": "deleted", "id": webhook_id}


@app.post("/__admin/trigger-webhook")
async def trigger_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Admin endpoint to trigger a test webhook event.

    Expected JSON:
    {
        "webhook_id": "webhook_uuid",
        "event_type": "shift.created",
        "data": { ... }
    }

    This fires the webhook asynchronously.
    """
    data = await request.json()

    webhook_id = data.get("webhook_id")
    event_type = data.get("event_type")
    payload = data.get("data", {})

    if webhook_id not in server.webhooks:
        raise HTTPException(status_code=404, detail="Webhook not found")

    webhook = server.webhooks[webhook_id]

    # Queue the webhook fire as a background task
    background_tasks.add_task(
        _fire_webhook,
        webhook["target_url"],
        webhook.get("secret"),
        event_type,
        payload,
    )

    logger.info(f"Triggered webhook {webhook_id}: {event_type}")

    return {
        "status": "queued",
        "webhook_id": webhook_id,
        "event_type": event_type,
    }


async def _fire_webhook(
    target_url: str,
    secret: Optional[str],
    event_type: str,
    payload: Dict[str, Any],
):
    """Fire a webhook to the target URL."""
    webhook_payload = {
        "event_type": event_type,
        "data": payload,
        "timestamp": datetime.now().isoformat(),
    }

    headers = {
        "Content-Type": "application/json",
    }

    # Sign the payload if secret provided
    if secret:
        payload_bytes = json.dumps(webhook_payload, default=str).encode()
        signature = hmac.new(
            secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        headers["X-Tanda-Signature"] = signature

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                target_url,
                json=webhook_payload,
                headers=headers,
            )
            logger.info(
                f"Webhook fired to {target_url}: {event_type} -> {response.status_code}"
            )
    except Exception as e:
        logger.error(f"Failed to fire webhook to {target_url}: {e}")


# ============================================================================
# HEALTH CHECK
# ============================================================================


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "service": "mock-tanda-api",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=9000)
