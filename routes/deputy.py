"""
Deputy integration API routes.

Endpoints for OAuth installation, data sync, and roster push-back
for venues using Deputy as their workforce management platform.

Routes:
    POST /api/deputy/install          -- Generate OAuth authorize URL
    GET  /api/deputy/callback         -- OAuth callback (code exchange)
    POST /api/deputy/uninstall        -- Remove Deputy connection
    GET  /api/deputy/status           -- Check connection status for a venue
    POST /api/deputy/sync/employees   -- Pull employees from Deputy
    POST /api/deputy/sync/shifts      -- Pull shifts for a date range
    POST /api/deputy/push/roster      -- Push roster back to Deputy
    GET  /api/deputy/timesheets       -- Get timesheets for a date range
    GET  /api/deputy/locations        -- Get Deputy locations
"""

import os
import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.deputy_adapter import (
    DeputyAdapter,
    DeputyOAuth,
    DeputyCredentials,
    DeputyAPIError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/deputy", tags=["deputy"])

# Environment config
DEPUTY_CLIENT_ID = os.environ.get("DEPUTY_CLIENT_ID", "")
DEPUTY_CLIENT_SECRET = os.environ.get("DEPUTY_CLIENT_SECRET", "")
DEPUTY_REDIRECT_URI = os.environ.get(
    "DEPUTY_REDIRECT_URI", "https://api.rosteriq.com.au/api/deputy/callback"
)

# Key prefix to distinguish Deputy installs from Tanda in plugin_installs table
DEPUTY_ORG_PREFIX = "deputy_"


# ============================================================================
# Pydantic Request Models
# ============================================================================


class InstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    subdomain: str = Field(..., description="Deputy instance subdomain (e.g. 'mycompany')")
    scope: str = Field(default="longlife_refresh_token", description="OAuth scope")


class UninstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to disconnect")


class SyncEmployeesRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    active_only: bool = Field(default=True, description="Only fetch active employees")


class SyncShiftsRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    location_id: Optional[int] = Field(default=None, description="Deputy location ID filter")


class PushRosterRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    location_id: int = Field(..., description="Deputy location/operational-unit ID")
    notify_employees: bool = Field(default=True, description="Send push notifications to staff")


def _get_oauth() -> DeputyOAuth:
    """Build a DeputyOAuth helper from environment variables."""
    if not DEPUTY_CLIENT_ID or not DEPUTY_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Deputy OAuth not configured (DEPUTY_CLIENT_ID / DEPUTY_CLIENT_SECRET missing)",
        )
    return DeputyOAuth(
        client_id=DEPUTY_CLIENT_ID,
        client_secret=DEPUTY_CLIENT_SECRET,
        redirect_uri=DEPUTY_REDIRECT_URI,
    )


def _build_credentials(install: dict) -> DeputyCredentials:
    """Reconstruct DeputyCredentials from a stored plugin_install record."""
    tokens = install.get("tokens", {})
    if not tokens or not tokens.get("access_token"):
        raise HTTPException(
            status_code=401,
            detail="Deputy credentials missing or incomplete. Please re-install.",
        )
    return DeputyCredentials(
        subdomain=tokens["subdomain"],
        client_id=DEPUTY_CLIENT_ID,
        client_secret=DEPUTY_CLIENT_SECRET,
        access_token=tokens["access_token"],
        refresh_token=tokens.get("refresh_token", ""),
        token_expires_at=(
            datetime.fromisoformat(tokens["token_expires_at"])
            if tokens.get("token_expires_at")
            else None
        ),
    )


def _org_key(venue_id: str) -> str:
    """Generate the organisation_id key used in plugin_installs for Deputy."""
    return f"{DEPUTY_ORG_PREFIX}{venue_id}"


def _get_install_or_404(venue_id: str) -> dict:
    """Fetch the Deputy install record for a venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install or install.get("status") == "uninstalled":
        raise HTTPException(
            status_code=404,
            detail=f"No active Deputy connection for venue {venue_id}",
        )
    return install


# ============================================================================
# OAuth Install Flow
# ============================================================================


@router.post("/install")
async def install(body: InstallRequest) -> dict:
    """
    Start the Deputy OAuth install flow.

    Returns the authorize URL to redirect the venue owner to.
    """
    oauth = _get_oauth()
    authorize_url = oauth.get_authorize_url(scope=body.scope)

    # Persist a pending install so the callback can find the venue/subdomain
    db = get_db()
    db.save_plugin_install({
        "organisation_id": _org_key(body.venue_id),
        "venue_id": body.venue_id,
        "status": "pending",
        "tokens": {"subdomain": body.subdomain},
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    logger.info(f"Deputy install initiated for venue {body.venue_id} ({body.subdomain})")
    return {
        "status": "pending",
        "authorize_url": authorize_url,
        "venue_id": body.venue_id,
        "message": "Redirect the venue owner to authorize_url to complete installation.",
    }


# ============================================================================
# OAuth Callback
# ============================================================================


@router.get("/callback")
async def oauth_callback(
    code: str = Query(..., description="Authorization code from Deputy"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    OAuth callback endpoint.

    Deputy redirects here with ?code=...&venue_id=... after the owner authorises.
    Exchanges the code for tokens and saves credentials.
    """
    db = get_db()
    org_key = _org_key(venue_id)
    install = db.get_plugin_install(org_key)

    if not install:
        raise HTTPException(
            status_code=404,
            detail=f"No pending install found for venue {venue_id}. Call /install first.",
        )

    subdomain = install.get("tokens", {}).get("subdomain")
    if not subdomain:
        raise HTTPException(
            status_code=400,
            detail="Subdomain missing from pending install record",
        )

    oauth = _get_oauth()

    try:
        credentials = await oauth.exchange_code(code, subdomain)
    except DeputyAPIError as e:
        logger.error(f"Deputy token exchange failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=401, detail="OAuth token exchange failed")

    # Verify the connection works
    try:
        async with DeputyAdapter(credentials) as adapter:
            me = await adapter.get_me()
    except DeputyAPIError as e:
        logger.error(f"Deputy connectivity check failed for venue {venue_id}: {e}")
        raise HTTPException(
            status_code=502,
            detail="Token obtained but Deputy API connectivity check failed",
        )

    # Save the full credentials
    install["status"] = "active"
    install["tokens"] = {
        "subdomain": subdomain,
        "access_token": credentials.access_token,
        "refresh_token": credentials.refresh_token,
        "token_expires_at": credentials.token_expires_at.isoformat(),
    }
    install["updated_at"] = datetime.utcnow()
    db.save_plugin_install(install)

    logger.info(f"Deputy OAuth complete for venue {venue_id} ({subdomain})")
    return {
        "status": "success",
        "venue_id": venue_id,
        "deputy_subdomain": subdomain,
        "deputy_user": me.get("Name", "unknown"),
        "message": "Deputy connected successfully.",
    }


# ============================================================================
# Uninstall
# ============================================================================


@router.post("/uninstall")
async def uninstall(body: UninstallRequest) -> dict:
    """
    Remove the Deputy connection for a venue.
    """
    install = _get_install_or_404(body.venue_id)

    install["status"] = "uninstalled"
    install["tokens"] = {}  # Wipe credentials
    install["updated_at"] = datetime.utcnow()

    db = get_db()
    db.save_plugin_install(install)

    logger.info(f"Deputy uninstalled for venue {body.venue_id}")
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "message": "Deputy connection removed.",
    }


# ============================================================================
# Status Check
# ============================================================================


@router.get("/status")
async def get_status(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Check whether a venue has an active Deputy connection.

    Returns connection status and metadata (subdomain, token expiry).
    """
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))

    if not install:
        return {
            "connected": False,
            "venue_id": venue_id,
            "status": "not_installed",
        }

    tokens = install.get("tokens", {})
    return {
        "connected": install.get("status") == "active",
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "deputy_subdomain": tokens.get("subdomain"),
        "token_expires_at": tokens.get("token_expires_at"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Sync Employees
# ============================================================================


@router.post("/sync/employees")
async def sync_employees(body: SyncEmployeesRequest) -> dict:
    """
    Pull employees from Deputy and return them as RosterIQ Employee models.
    """
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with DeputyAdapter(credentials) as adapter:
            employees = await adapter.get_employees(active_only=body.active_only)
    except DeputyAPIError as e:
        logger.error(f"Deputy employee sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    logger.info(f"Synced {len(employees)} employees from Deputy for venue {body.venue_id}")
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "count": len(employees),
        "employees": [emp.dict() for emp in employees],
    }


# ============================================================================
# Sync Shifts
# ============================================================================


@router.post("/sync/shifts")
async def sync_shifts(body: SyncShiftsRequest) -> dict:
    """
    Pull shifts from Deputy for a date range.
    """
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with DeputyAdapter(credentials) as adapter:
            shifts = await adapter.get_shifts(
                start_date=body.start_date,
                end_date=body.end_date,
                location_id=body.location_id,
            )
    except DeputyAPIError as e:
        logger.error(f"Deputy shift sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    logger.info(
        f"Synced {len(shifts)} shifts from Deputy for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date})"
    )
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(shifts),
        "shifts": [shift.dict() for shift in shifts],
    }


# ============================================================================
# Push Roster
# ============================================================================


@router.post("/push/roster")
async def push_roster(body: PushRosterRequest) -> dict:
    """
    Push a roster back to Deputy (publish shifts for a date range).
    """
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with DeputyAdapter(credentials) as adapter:
            result = await adapter.publish_roster(
                start_date=body.start_date,
                end_date=body.end_date,
                location_id=body.location_id,
                notify_employees=body.notify_employees,
            )
    except DeputyAPIError as e:
        logger.error(f"Deputy roster push failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    logger.info(
        f"Published roster to Deputy for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date}, location {body.location_id})"
    )
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "location_id": body.location_id,
        "notify_employees": body.notify_employees,
        "deputy_response": result,
    }


# ============================================================================
# Timesheets
# ============================================================================


@router.get("/timesheets")
async def get_timesheets(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    employee_id: Optional[int] = Query(None, description="Filter by Deputy employee ID"),
) -> dict:
    """
    Get timesheets (actual clock-in/out records) from Deputy for a date range.
    """
    try:
        start_dt = date.fromisoformat(start_date)
        end_dt = date.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD.",
        )

    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with DeputyAdapter(credentials) as adapter:
            timesheets = await adapter.get_timesheets(
                start_date=start_dt,
                end_date=end_dt,
                employee_id=employee_id,
            )
    except DeputyAPIError as e:
        logger.error(f"Deputy timesheet fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    logger.info(
        f"Fetched {len(timesheets)} timesheets from Deputy for venue {venue_id} "
        f"({start_date} to {end_date})"
    )
    return {
        "status": "success",
        "venue_id": venue_id,
        "start_date": start_date,
        "end_date": end_date,
        "count": len(timesheets),
        "timesheets": timesheets,
    }


# ============================================================================
# Locations
# ============================================================================


@router.get("/locations")
async def get_locations(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Get all locations (companies/sites) from the connected Deputy account.
    """
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with DeputyAdapter(credentials) as adapter:
            locations = await adapter.get_locations()
    except DeputyAPIError as e:
        logger.error(f"Deputy locations fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    logger.info(f"Fetched {len(locations)} locations from Deputy for venue {venue_id}")
    return {
        "status": "success",
        "venue_id": venue_id,
        "count": len(locations),
        "locations": locations,
    }
