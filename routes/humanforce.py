"""
HumanForce integration API routes.

Endpoints for OAuth installation, data sync, and roster push-back
for venues using HumanForce as their workforce management platform.

Routes:
    POST /api/humanforce/install          -- Generate OAuth authorize URL
    GET  /api/humanforce/callback         -- OAuth callback (code exchange)
    POST /api/humanforce/uninstall        -- Remove HumanForce connection
    GET  /api/humanforce/status           -- Check connection status for a venue
    POST /api/humanforce/sync/employees   -- Pull employees from HumanForce
    POST /api/humanforce/sync/shifts      -- Pull shifts for a date range
    POST /api/humanforce/push/roster      -- Push roster back to HumanForce
    GET  /api/humanforce/timesheets       -- Get timesheets for a date range
    GET  /api/humanforce/locations        -- Get HumanForce locations
"""

import os
import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.humanforce_adapter import (
    HumanForceAdapter,
    HumanForceOAuth,
    HumanForceCredentials,
    HumanForceAPIError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/humanforce", tags=["humanforce"])

# Environment config
HUMANFORCE_CLIENT_ID = os.environ.get("HUMANFORCE_CLIENT_ID", "")
HUMANFORCE_CLIENT_SECRET = os.environ.get("HUMANFORCE_CLIENT_SECRET", "")
HUMANFORCE_REDIRECT_URI = os.environ.get(
    "HUMANFORCE_REDIRECT_URI", "https://api.rosteriq.com.au/api/humanforce/callback"
)

# Key prefix to distinguish HumanForce installs in plugin_installs table
HUMANFORCE_ORG_PREFIX = "humanforce_"


# ============================================================================
# Pydantic Request Models
# ============================================================================


class InstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    scope: str = Field(default="read write", description="OAuth scope")


class UninstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to disconnect")


class SyncEmployeesRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    active_only: bool = Field(default=True, description="Only fetch active employees")


class SyncShiftsRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    location_id: Optional[str] = Field(default=None, description="HumanForce location ID filter")


class PushRosterRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    roster_id: str = Field(..., description="RosterIQ roster ID to push")
    location_id: str = Field(..., description="HumanForce location ID")
    publish: bool = Field(default=True, description="Publish roster immediately")
    notify_employees: bool = Field(default=True, description="Send notifications to staff")


def _get_oauth() -> HumanForceOAuth:
    """Build a HumanForceOAuth helper from environment variables."""
    if not HUMANFORCE_CLIENT_ID or not HUMANFORCE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="HumanForce OAuth not configured (HUMANFORCE_CLIENT_ID / HUMANFORCE_CLIENT_SECRET missing)",
        )
    return HumanForceOAuth(
        client_id=HUMANFORCE_CLIENT_ID,
        client_secret=HUMANFORCE_CLIENT_SECRET,
        redirect_uri=HUMANFORCE_REDIRECT_URI,
    )


def _build_credentials(install: dict) -> HumanForceCredentials:
    """Reconstruct HumanForceCredentials from a stored plugin_install record."""
    tokens = install.get("tokens", {})
    if not tokens or not tokens.get("access_token"):
        raise HTTPException(
            status_code=401,
            detail="HumanForce credentials missing or incomplete. Please re-install.",
        )
    return HumanForceCredentials(
        client_id=HUMANFORCE_CLIENT_ID,
        client_secret=HUMANFORCE_CLIENT_SECRET,
        access_token=tokens["access_token"],
        refresh_token=tokens.get("refresh_token", ""),
        token_expiry=(
            datetime.fromisoformat(tokens["token_expiry"])
            if tokens.get("token_expiry")
            else None
        ),
        base_url=tokens.get("base_url", "https://api.humanforce.com/v1"),
    )


def _persist_refreshed_tokens(install: dict):
    """
    Build an on_token_refresh callback that writes refreshed HumanForce tokens
    back into the venue's plugin_install record, so the next request reuses the
    new access/refresh token instead of starting from the expired one.
    """
    def _save(creds) -> None:
        tokens = install.setdefault("tokens", {})
        tokens["access_token"] = creds.access_token
        tokens["refresh_token"] = creds.refresh_token
        tokens["token_expiry"] = creds.token_expiry.isoformat() if creds.token_expiry else None
        tokens["base_url"] = creds.base_url
        try:
            get_db().save_plugin_install(install)
        except Exception as e:  # noqa: BLE001 — never let persistence failure break the call
            logger.warning(f"Failed to persist refreshed HumanForce tokens: {e}")

    return _save


def _org_key(venue_id: str) -> str:
    """Generate the organisation_id key used in plugin_installs for HumanForce."""
    return f"{HUMANFORCE_ORG_PREFIX}{venue_id}"


def _get_install_or_404(venue_id: str) -> dict:
    """Fetch the HumanForce install record for a venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install or install.get("status") == "uninstalled":
        raise HTTPException(
            status_code=404,
            detail=f"No active HumanForce connection for venue {venue_id}",
        )
    return install


# ============================================================================
# OAuth Install Flow
# ============================================================================


@router.post("/install")
async def install(body: InstallRequest) -> dict:
    """
    Start the HumanForce OAuth install flow.

    Returns the authorize URL to redirect the venue owner to.
    """
    enforce_venue_manager(body.venue_id)
    oauth = _get_oauth()
    authorize_url = oauth.get_auth_url(scope=body.scope)

    # Persist a pending install so the callback can find the venue
    db = get_db()
    db.save_plugin_install({
        "organisation_id": _org_key(body.venue_id),
        "venue_id": body.venue_id,
        "status": "pending",
        "tokens": {},
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    logger.info(f"HumanForce install initiated for venue {body.venue_id}")
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
    code: str = Query(..., description="Authorization code from HumanForce"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    OAuth callback endpoint.

    HumanForce redirects here with ?code=...&venue_id=... after the owner authorises.
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

    oauth = _get_oauth()

    try:
        credentials = await oauth.exchange_code(code)
    except HumanForceAPIError as e:
        logger.error(f"HumanForce token exchange failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=401, detail="OAuth token exchange failed")

    # Verify the connection works
    try:
        async with HumanForceAdapter(credentials) as adapter:
            me = await adapter.get_me()
    except HumanForceAPIError as e:
        logger.error(f"HumanForce connectivity check failed for venue {venue_id}: {e}")
        raise HTTPException(
            status_code=502,
            detail="Token obtained but HumanForce API connectivity check failed",
        )

    # Save the full credentials
    install["status"] = "active"
    install["tokens"] = {
        "access_token": credentials.access_token,
        "refresh_token": credentials.refresh_token,
        "token_expiry": credentials.token_expiry.isoformat() if credentials.token_expiry else None,
        "base_url": credentials.base_url,
    }
    install["updated_at"] = datetime.utcnow()
    db.save_plugin_install(install)

    logger.info(f"HumanForce OAuth complete for venue {venue_id}")
    return {
        "status": "success",
        "venue_id": venue_id,
        "humanforce_user": me.get("name", me.get("email", "unknown")),
        "message": "HumanForce connected successfully.",
    }


# ============================================================================
# Uninstall
# ============================================================================


@router.post("/uninstall")
async def uninstall(body: UninstallRequest) -> dict:
    """
    Remove the HumanForce connection for a venue.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)

    install["status"] = "uninstalled"
    install["tokens"] = {}  # Wipe credentials
    install["updated_at"] = datetime.utcnow()

    db = get_db()
    db.save_plugin_install(install)

    logger.info(f"HumanForce uninstalled for venue {body.venue_id}")
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "message": "HumanForce connection removed.",
    }


# ============================================================================
# Status Check
# ============================================================================


@router.get("/status")
async def get_status(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Check whether a venue has an active HumanForce connection.

    Returns connection status and metadata.
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
        "token_expiry": tokens.get("token_expiry"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Sync Employees
# ============================================================================


@router.post("/sync/employees")
async def sync_employees(body: SyncEmployeesRequest) -> dict:
    """
    Pull employees from HumanForce and return them as RosterIQ Employee models.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with HumanForceAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            employees = await adapter.sync_employees(active_only=body.active_only)
    except HumanForceAPIError as e:
        logger.error(f"HumanForce employee sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"HumanForce API error: {e}")

    logger.info(f"Synced {len(employees)} employees from HumanForce for venue {body.venue_id}")
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
    Pull shifts from HumanForce for a date range.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with HumanForceAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            shifts = await adapter.sync_shifts(
                start_date=body.start_date,
                end_date=body.end_date,
                location_id=body.location_id,
            )
    except HumanForceAPIError as e:
        logger.error(f"HumanForce shift sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"HumanForce API error: {e}")

    logger.info(
        f"Synced {len(shifts)} shifts from HumanForce for venue {body.venue_id} "
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
    Push an optimised roster back to HumanForce.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    # Load the roster from the database
    db = get_db()
    roster_data = db.get_roster(body.roster_id) if hasattr(db, "get_roster") else None
    if not roster_data:
        raise HTTPException(
            status_code=404,
            detail=f"Roster {body.roster_id} not found",
        )

    try:
        async with HumanForceAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            result = await adapter.push_roster(
                roster=roster_data,
                location_id=body.location_id,
                publish=body.publish,
                notify_employees=body.notify_employees,
            )
    except HumanForceAPIError as e:
        logger.error(f"HumanForce roster push failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"HumanForce API error: {e}")

    logger.info(
        f"Pushed roster {body.roster_id} to HumanForce for venue {body.venue_id} "
        f"(location {body.location_id})"
    )
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "roster_id": body.roster_id,
        "location_id": body.location_id,
        "publish": body.publish,
        "notify_employees": body.notify_employees,
        "humanforce_response": result,
    }


# ============================================================================
# Timesheets
# ============================================================================


@router.get("/timesheets")
async def get_timesheets(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    employee_id: Optional[str] = Query(None, description="Filter by HumanForce employee ID"),
) -> dict:
    """
    Get timesheets (actual clock-in/out records) from HumanForce for a date range.
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
        async with HumanForceAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            timesheets = await adapter.get_timesheets(
                start_date=start_dt,
                end_date=end_dt,
                employee_id=employee_id,
            )
    except HumanForceAPIError as e:
        logger.error(f"HumanForce timesheet fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"HumanForce API error: {e}")

    logger.info(
        f"Fetched {len(timesheets)} timesheets from HumanForce for venue {venue_id} "
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
    Get all locations from the connected HumanForce account.
    """
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with HumanForceAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            locations = await adapter.get_locations()
    except HumanForceAPIError as e:
        logger.error(f"HumanForce locations fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"HumanForce API error: {e}")

    logger.info(f"Fetched {len(locations)} locations from HumanForce for venue {venue_id}")
    return {
        "status": "success",
        "venue_id": venue_id,
        "count": len(locations),
        "locations": locations,
    }
