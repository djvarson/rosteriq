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
import json
import base64
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager
from rosteriq.services.visa import preserve_recorded_work_rights
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
    "DEPUTY_REDIRECT_URI", "https://rosteriq-production-6aaf.up.railway.app/deputy/callback"
)

# Key prefix to distinguish Deputy installs from Tanda in plugin_installs table
DEPUTY_ORG_PREFIX = "deputy_"


# ============================================================================
# OAuth State Helpers (encode venue_id into the OAuth state parameter)
# ============================================================================


def _encode_state(venue_id: str) -> str:
    """Encode venue_id into a base64 OAuth state string."""
    payload = json.dumps({"venue_id": venue_id})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_state(state: str) -> dict:
    """Decode the OAuth state string back to a dict with venue_id."""
    try:
        payload = base64.urlsafe_b64decode(state.encode()).decode()
        return json.loads(payload)
    except Exception:
        return {}


# ============================================================================
# Pydantic Request Models
# ============================================================================


class InstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    subdomain: str = Field(default="", description="Deputy instance subdomain (auto-detected via OAuth)")
    scope: str = Field(default="longlife_refresh_token", description="OAuth scope")


class InstallTokenRequest(BaseModel):
    """Direct permanent-token install — no OAuth app registration needed."""
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    subdomain: str = Field(..., min_length=1, max_length=100, description="Deputy instance subdomain (e.g. 'mycompany')")
    access_token: str = Field(..., min_length=10, description="Permanent access token from Deputy")

    def model_post_init(self, __context) -> None:
        """Validate subdomain format."""
        import re
        cleaned = self.subdomain.strip().lower()
        # Remove trailing .au.deputy.com if user pasted full URL
        cleaned = re.sub(r'\.au\.deputy\.com.*$', '', cleaned)
        cleaned = re.sub(r'^https?://', '', cleaned)
        if not re.match(r'^[a-z0-9]([a-z0-9\-]*[a-z0-9])?$', cleaned):
            raise ValueError(
                f"Invalid Deputy subdomain: '{self.subdomain}'. "
                "Use just the subdomain part (e.g. 'mycompany'), not the full URL."
            )
        object.__setattr__(self, 'subdomain', cleaned)


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
            status_code=503,
            detail="Deputy OAuth sign-in is not configured on this server. "
                   "Connect with the Access Token method instead: in Deputy, open "
                   "Business Settings and generate a permanent token under API "
                   "access (oauth_clients), then paste it on the Connections page.",
        )
    return DeputyOAuth(
        client_id=DEPUTY_CLIENT_ID,
        client_secret=DEPUTY_CLIENT_SECRET,
        redirect_uri=DEPUTY_REDIRECT_URI,
    )


def _make_token_refresh_callback(org_key: str):
    """Create a callback that persists refreshed tokens to the DB.

    The adapter calls this with the full DeputyCredentials object after a token refresh.
    """
    def on_token_refresh(credentials: DeputyCredentials):
        try:
            db = get_db()
            install = db.get_plugin_install(org_key)
            if install and install.get("tokens"):
                install["tokens"]["access_token"] = credentials.access_token
                if credentials.refresh_token:
                    install["tokens"]["refresh_token"] = credentials.refresh_token
                if credentials.token_expires_at:
                    install["tokens"]["token_expires_at"] = credentials.token_expires_at.isoformat()
                install["updated_at"] = datetime.utcnow()
                db.save_plugin_install(install)
                logger.info(f"Persisted refreshed Deputy token for {org_key}")
        except Exception as e:
            logger.error(f"Failed to persist refreshed Deputy token for {org_key}: {e}")
    return on_token_refresh


def _build_credentials(install: dict) -> DeputyCredentials:
    """Reconstruct DeputyCredentials from a stored plugin_install record."""
    tokens = install.get("tokens", {})
    if not tokens or not tokens.get("access_token"):
        raise HTTPException(
            status_code=401,
            detail="Deputy credentials missing or incomplete. Please re-install.",
        )
    # Permanent tokens don't need client_id/secret for refresh
    is_permanent = tokens.get("token_type") == "permanent"
    return DeputyCredentials(
        subdomain=tokens["subdomain"],
        client_id=DEPUTY_CLIENT_ID if not is_permanent else "permanent_token",
        client_secret=DEPUTY_CLIENT_SECRET if not is_permanent else "",
        access_token=tokens["access_token"],
        refresh_token=tokens.get("refresh_token", ""),
        token_expires_at=(
            datetime.fromisoformat(tokens["token_expires_at"])
            if tokens.get("token_expires_at")
            else None
        ),
    )


def _build_adapter(install: dict) -> DeputyAdapter:
    """Build a DeputyAdapter with credentials and token-refresh callback wired up."""
    credentials = _build_credentials(install)
    org_key = install.get("organisation_id", "")
    return DeputyAdapter(
        credentials,
        on_token_refresh=_make_token_refresh_callback(org_key),
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
    The venue_id is encoded into the OAuth ``state`` parameter so it
    survives the redirect through Deputy and back to our callback.
    """
    enforce_venue_manager(body.venue_id)
    oauth = _get_oauth()
    state = _encode_state(body.venue_id)
    authorize_url = oauth.get_authorize_url(scope=body.scope, state=state)

    # Persist a pending install so the callback can find the venue
    db = get_db()
    db.save_plugin_install({
        "organisation_id": _org_key(body.venue_id),
        "venue_id": body.venue_id,
        "status": "pending",
        "tokens": {"subdomain": body.subdomain} if body.subdomain else {},
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    logger.info(f"Deputy OAuth install initiated for venue {body.venue_id}")
    return {
        "status": "pending",
        "authorize_url": authorize_url,
        "venue_id": body.venue_id,
        "message": "Redirect the venue owner to authorize_url to complete installation.",
    }


# ============================================================================
# Direct Token Install (permanent token — no OAuth app needed)
# ============================================================================


@router.post("/install-token")
async def install_token(body: InstallTokenRequest) -> dict:
    """
    Connect Deputy using a permanent access token.

    The venue owner generates this from their Deputy admin panel at:
    https://{subdomain}.{geo}.deputy.com/exec/devapp/oauth_clients

    No DEPUTY_CLIENT_ID or DEPUTY_CLIENT_SECRET required — the token
    is self-contained and long-lived.
    """
    enforce_venue_manager(body.venue_id)
    db = get_db()
    org_key = _org_key(body.venue_id)

    # Build credentials from the permanent token
    credentials = DeputyCredentials(
        subdomain=body.subdomain,
        client_id="permanent_token",
        client_secret="",
        access_token=body.access_token,
        refresh_token="",
        token_expires_at=None,
    )

    # Verify the connection works — MUST succeed or we reject the install
    try:
        async with DeputyAdapter(credentials) as adapter:
            me = await adapter.get_me()
            deputy_user = me.get("Name", me.get("DisplayName", "unknown"))
    except DeputyAPIError as e:
        logger.error(f"Deputy token verification failed for venue {body.venue_id}: {e}")
        raise HTTPException(
            status_code=401,
            detail=f"Token verification failed: could not connect to Deputy. "
                   f"Please check your subdomain and access token. Error: {e}",
        )
    except Exception as e:
        logger.error(f"Deputy token verification failed for venue {body.venue_id}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Connection to Deputy failed unexpectedly. "
                   f"Please check your subdomain and try again.",
        )

    # Verification passed — save the install record
    install_record = {
        "organisation_id": org_key,
        "venue_id": body.venue_id,
        "provider": "deputy",
        "status": "active",
        "tokens": {
            "subdomain": body.subdomain,
            "access_token": body.access_token,
            "refresh_token": "",
            "token_type": "permanent",
        },
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)

    logger.info(
        f"Deputy connected via permanent token for venue {body.venue_id} "
        f"({body.subdomain}, user: {deputy_user})"
    )

    # Auto-sync employees and shifts on connect
    synced_employees = 0
    synced_shifts = 0
    employee_errors = 0
    shift_errors = 0
    try:
        on_refresh = _make_token_refresh_callback(org_key)
        async with DeputyAdapter(credentials, on_token_refresh=on_refresh) as adapter:
            # Pull employees
            employees = await adapter.get_employees(active_only=True)
            synced_employees = len(employees)

            # Store employees in database
            for emp in employees:
                try:
                    emp.venue_id = body.venue_id
                    db.save_employee(preserve_recorded_work_rights(db, emp))
                except Exception as e:
                    employee_errors += 1
                    logger.warning(
                        f"Failed to save employee {getattr(emp, 'external_id', '?')} "
                        f"for venue {body.venue_id}: {e}"
                    )

            # Pull current shifts (next 14 days)
            today = date.today()
            shifts = await adapter.get_shifts(
                start_date=today,
                end_date=today + timedelta(days=14),
            )
            synced_shifts = len(shifts)

            # Store shifts in database
            for shift in shifts:
                try:
                    db.save_shift(shift)
                except Exception as e:
                    shift_errors += 1
                    logger.warning(
                        f"Failed to save shift {getattr(shift, 'external_id', '?')} "
                        f"for venue {body.venue_id}: {e}"
                    )

        logger.info(
            f"Auto-synced {synced_employees} employees ({employee_errors} errors) and "
            f"{synced_shifts} shifts ({shift_errors} errors) from Deputy for venue {body.venue_id}"
        )
    except Exception as e:
        logger.warning(f"Deputy auto-sync failed for venue {body.venue_id}: {e}")

    return {
        "status": "success",
        "venue_id": body.venue_id,
        "deputy_subdomain": body.subdomain,
        "deputy_user": deputy_user,
        "synced_employees": synced_employees,
        "synced_shifts": synced_shifts,
        "sync_errors": employee_errors + shift_errors,
        "message": "Deputy connected successfully via permanent token.",
    }


# ============================================================================
# OAuth Callback
# ============================================================================


def _callback_html(success: bool, message: str, detail: str = "") -> HTMLResponse:
    """Return an HTML page for the OAuth callback that shows status and redirects to dashboard."""
    status_color = "#22c55e" if success else "#ef4444"
    status_icon = "&#10003;" if success else "&#10007;"
    status_text = "Connected" if success else "Connection Failed"
    redirect_js = "setTimeout(function(){ window.location.href = '/static/dashboard.html?deputy_connected=1'; }, 2000);" if success else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RosterIQ — Deputy {status_text}</title>
<style>
  body {{ margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
         background:#0f172a; font-family:-apple-system,system-ui,sans-serif; color:#e2e8f0; }}
  .card {{ background:#1e293b; border-radius:16px; padding:48px; text-align:center; max-width:420px;
           box-shadow:0 25px 50px rgba(0,0,0,0.4); }}
  .icon {{ font-size:48px; color:{status_color}; margin-bottom:16px; }}
  h1 {{ font-size:24px; margin:0 0 8px; color:#f8fafc; }}
  .msg {{ font-size:14px; color:#94a3b8; line-height:1.6; margin:0 0 24px; }}
  .detail {{ font-size:12px; color:#64748b; }}
  .spinner {{ display:inline-block; width:16px; height:16px; border:2px solid #334155;
              border-top-color:#f59e0b; border-radius:50%; animation:spin .8s linear infinite;
              vertical-align:middle; margin-right:8px; }}
  @keyframes spin {{ to {{ transform:rotate(360deg); }} }}
</style>
</head>
<body>
<div class="card">
  <div class="icon">{status_icon}</div>
  <h1>Deputy {status_text}</h1>
  <p class="msg">{message}</p>
  {"<p class='detail'><span class='spinner'></span>Redirecting to dashboard...</p>" if success else f"<p class='detail'>{detail}</p>"}
</div>
<script>{redirect_js}</script>
</body></html>"""
    return HTMLResponse(content=html, status_code=200 if success else 400)


@router.get("/callback")
async def oauth_callback(
    code: str = Query("", description="Authorization code from Deputy"),
    state: str = Query("", description="OAuth state parameter carrying venue_id"),
    error: str = Query("", description="OAuth error code, when the flow failed"),
):
    """
    OAuth callback endpoint.

    Deputy redirects the user's browser here with ``?code=...&state=...``
    after the owner authorises. Exchanges the code for tokens, saves
    credentials, and returns an HTML page that redirects to the dashboard.
    On denial/failure Deputy sends ``?error=...`` instead of a code — show a
    friendly page rather than a raw validation error (code used to be a
    required param, so error redirects surfaced as an ugly 422 JSON).
    """
    if error or not code:
        logger.warning(f"Deputy OAuth callback returned error={error!r} (no code)")
        return _callback_html(
            False,
            "Deputy connection was not completed.",
            f"Deputy reported: {error or 'no authorization code returned'}. "
            "You can close this page and try connecting again from the "
            "Connections page — the paste-a-token method is the most reliable.",
        )

    # Decode venue_id from state parameter
    state_data = _decode_state(state)
    venue_id = state_data.get("venue_id", "")

    if not venue_id:
        logger.error(f"Deputy callback missing venue_id in state: {state!r}")
        return _callback_html(False, "Missing venue information.", "The OAuth state parameter was invalid or missing. Please try connecting again from the dashboard.")

    db = get_db()
    org_key = _org_key(venue_id)
    install = db.get_plugin_install(org_key)

    if not install:
        logger.error(f"No pending install found for venue {venue_id}")
        return _callback_html(False, "No pending connection found.", "Please start the connection again from the dashboard Integrations page.")

    # Subdomain may have been provided upfront, or will be auto-detected from token response
    subdomain = install.get("tokens", {}).get("subdomain", "")

    oauth = _get_oauth()

    try:
        credentials = await oauth.exchange_code(code, subdomain)
    except DeputyAPIError as e:
        logger.error(f"Deputy token exchange failed for venue {venue_id}: {e}")
        return _callback_html(False, "Token exchange failed.", "Deputy rejected the authorization code. Please try connecting again.")

    # Verify the connection works
    try:
        async with DeputyAdapter(credentials) as adapter:
            me = await adapter.get_me()
    except DeputyAPIError as e:
        logger.error(f"Deputy connectivity check failed for venue {venue_id}: {e}")
        return _callback_html(False, "Connection verification failed.", "Got a token from Deputy but couldn't verify the connection. Please try again.")

    # Save the full credentials — use the subdomain from credentials (may have been auto-detected)
    resolved_subdomain = credentials.subdomain
    install["status"] = "active"
    install["provider"] = "deputy"
    install["tokens"] = {
        "subdomain": resolved_subdomain,
        "access_token": credentials.access_token,
        "refresh_token": credentials.refresh_token,
        "token_expires_at": credentials.token_expires_at.isoformat() if credentials.token_expires_at else None,
    }
    install["updated_at"] = datetime.utcnow()
    db.save_plugin_install(install)

    deputy_user = me.get("Name", me.get("DisplayName", "unknown"))
    logger.info(f"Deputy OAuth complete for venue {venue_id} ({resolved_subdomain}, user: {deputy_user})")

    # Auto-sync employees and shifts
    synced_employees = 0
    synced_shifts = 0
    try:
        on_refresh = _make_token_refresh_callback(org_key)
        async with DeputyAdapter(credentials, on_token_refresh=on_refresh) as adapter:
            employees = await adapter.get_employees(active_only=True)
            synced_employees = len(employees)
            for emp in employees:
                try:
                    emp.venue_id = venue_id
                    db.save_employee(preserve_recorded_work_rights(db, emp))
                except Exception as e:
                    logger.warning(f"Failed to save employee: {e}")

            today = date.today()
            shifts = await adapter.get_shifts(start_date=today, end_date=today + timedelta(days=14))
            synced_shifts = len(shifts)
            for shift in shifts:
                try:
                    db.save_shift(shift)
                except Exception as e:
                    logger.warning(f"Failed to save shift: {e}")

        logger.info(f"Auto-synced {synced_employees} employees, {synced_shifts} shifts for venue {venue_id}")
    except Exception as e:
        logger.warning(f"Deputy auto-sync failed for venue {venue_id}: {e}")

    sync_msg = f"Synced {synced_employees} staff members and {synced_shifts} shifts." if synced_employees else "Connected as " + deputy_user + "."
    return _callback_html(True, sync_msg)


# ============================================================================
# Uninstall
# ============================================================================


@router.post("/uninstall")
async def uninstall(body: UninstallRequest) -> dict:
    """
    Remove the Deputy connection for a venue.
    """
    enforce_venue_manager(body.venue_id)
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
    verify: bool = Query(False, description="If true, test the connection live against Deputy API"),
) -> dict:
    """
    Check whether a venue has an active Deputy connection.

    Returns connection status and metadata (subdomain, token expiry).
    With ?verify=true, also makes a live API call to confirm the token still works.
    """
    enforce_venue_access(venue_id)
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))

    if not install:
        return {
            "connected": False,
            "venue_id": venue_id,
            "status": "not_installed",
        }

    tokens = install.get("tokens", {})
    result = {
        "connected": install.get("status") == "active",
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "deputy_subdomain": tokens.get("subdomain"),
        "token_expires_at": tokens.get("token_expires_at"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }

    # Live verification — actually hit the Deputy API
    if verify and result["connected"]:
        try:
            async with _build_adapter(install) as adapter:
                me = await adapter.get_me()
                result["live_verified"] = True
                result["deputy_user"] = me.get("Name", me.get("DisplayName", "unknown"))
        except DeputyAPIError as e:
            result["live_verified"] = False
            result["live_error"] = str(e)
            logger.warning(f"Deputy live verification failed for venue {venue_id}: {e}")
        except Exception as e:
            result["live_verified"] = False
            result["live_error"] = f"Unexpected error: {e}"
            logger.warning(f"Deputy live verification failed for venue {venue_id}: {e}")

    return result


# ============================================================================
# Sync Employees
# ============================================================================


@router.post("/sync/employees")
async def sync_employees(body: SyncEmployeesRequest) -> dict:
    """
    Pull employees from Deputy, persist to DB, and return them.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    db = get_db()

    try:
        async with _build_adapter(install) as adapter:
            employees = await adapter.get_employees(active_only=body.active_only)
            rate_review_ids = set(getattr(adapter, "_rate_review_ids", []))
    except DeputyAPIError as e:
        logger.error(f"Deputy employee sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    # Persist to database
    saved = 0
    errors = 0
    rate_review_names = []
    for emp in employees:
        try:
            emp.venue_id = body.venue_id
            db.save_employee(preserve_recorded_work_rights(db, emp))
            saved += 1
            if emp.id.removeprefix("deputy-") in rate_review_ids:
                rate_review_names.append(emp.name)
        except Exception as e:
            errors += 1
            logger.warning(
                f"Failed to save employee {getattr(emp, 'id', '?')} "
                f"for venue {body.venue_id}: {e}"
            )

    logger.info(
        f"Synced {len(employees)} employees from Deputy for venue {body.venue_id} "
        f"(saved: {saved}, errors: {errors}, rate review needed: {len(rate_review_names)})"
    )
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "count": len(employees),
        "saved": saved,
        "errors": errors,
        # Deputy often omits pay rates (they live in EmployeeAgreement). These
        # staff were imported with a flagged placeholder rate — correct their
        # hourly rates in Staff before trusting any cost numbers.
        "rate_review_needed": rate_review_names,
        "employees": [emp.dict() for emp in employees],
    }


# ============================================================================
# Sync Shifts
# ============================================================================


@router.post("/sync/shifts")
async def sync_shifts(body: SyncShiftsRequest) -> dict:
    """
    Pull shifts from Deputy for a date range, persist to DB, and return them.
    """
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    db = get_db()

    try:
        async with _build_adapter(install) as adapter:
            shifts = await adapter.get_shifts(
                start_date=body.start_date,
                end_date=body.end_date,
                location_id=body.location_id,
            )
    except DeputyAPIError as e:
        logger.error(f"Deputy shift sync failed for venue {body.venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Deputy API error: {e}")

    # Persist to database
    saved = 0
    errors = 0
    for shift in shifts:
        try:
            db.save_shift(shift)
            saved += 1
        except Exception as e:
            errors += 1
            logger.warning(
                f"Failed to save shift {getattr(shift, 'external_id', '?')} "
                f"for venue {body.venue_id}: {e}"
            )

    logger.info(
        f"Synced {len(shifts)} shifts from Deputy for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date}, saved: {saved}, errors: {errors})"
    )
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(shifts),
        "saved": saved,
        "errors": errors,
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
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)

    try:
        async with _build_adapter(install) as adapter:
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
    enforce_venue_access(venue_id)
    try:
        start_dt = date.fromisoformat(start_date)
        end_dt = date.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD.",
        )

    install = _get_install_or_404(venue_id)

    try:
        async with _build_adapter(install) as adapter:
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
    enforce_venue_access(venue_id)
    install = _get_install_or_404(venue_id)

    try:
        async with _build_adapter(install) as adapter:
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
