"""
Tanda Marketplace plugin API routes.

Endpoints for marketplace installation, uninstallation, and health checks.
These are called by the Tanda Marketplace when organisations install/uninstall
the RosterIQ plugin.

Routes:
    POST /api/tanda/plugin/install — Install handler
    POST /api/tanda/plugin/uninstall — Uninstall handler
    GET /api/tanda/plugin/status/{org_id} — Status check
    GET /api/tanda/plugin/health — Marketplace health check
"""

import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query, Header

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_owner
from rosteriq.services.tanda_plugin import TandaPluginService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tanda/plugin", tags=["tanda-plugin"])

# Initialize the plugin service (lazily on first request)
_plugin_service: Optional[TandaPluginService] = None


def get_plugin_service() -> TandaPluginService:
    """Get or create the plugin service singleton."""
    global _plugin_service
    if _plugin_service is None:
        _plugin_service = TandaPluginService(
            db=get_db(),
            tanda_client_id=os.environ.get("TANDA_CLIENT_ID"),
            tanda_client_secret=os.environ.get("TANDA_CLIENT_SECRET"),
            tanda_webhook_secret=os.environ.get("TANDA_WEBHOOK_SECRET"),
            redirect_base_uri=os.environ.get(
                "REDIRECT_BASE_URI", "https://api.rosteriq.com.au"
            ),
        )
    return _plugin_service


# ============================================================================
# Install Handler
# ============================================================================


class InstallRequest:
    """Marketplace install callback payload."""

    def __init__(self, data: dict):
        self.organisation_id = data.get("organisation_id")
        self.auth_code = data.get("auth_code")
        self.redirect_uri = data.get("redirect_uri")
        self.state = data.get("state", "vic")


@router.post("/install")
async def handle_install(request: Request) -> dict:
    """
    Handle plugin installation from Tanda Marketplace.

    Called by Tanda when a venue installs the RosterIQ plugin from the marketplace.
    The marketplace sends:
    - organisation_id: The Tanda organisation ID
    - auth_code: OAuth authorization code
    - redirect_uri: OAuth redirect URI
    - state: Australian state code (optional)

    The endpoint:
    1. Exchanges auth_code for OAuth tokens
    2. Verifies tokens are valid by calling Tanda API
    3. Creates a venue record
    4. Initializes onboarding flow
    5. Returns install status

    Args:
        request: HTTP request with install payload

    Returns:
        Installation status and next steps

    Raises:
        400: If payload is invalid
        401: If OAuth token exchange fails
        500: If venue creation fails
    """
    # Interim hardening (2026-08-30): a marketplace install creates a venue and
    # stores org credentials — restrict to platform owners until the Tanda HMAC
    # signature check (verify_tanda_signature, currently unwired) is properly
    # wired and this route is moved to the signed-webhook auth path.
    enforce_owner()
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse install request: {e}")
        raise HTTPException(status_code=400, detail="Invalid request payload")

    # Validate required fields
    required = ["organisation_id", "auth_code", "redirect_uri"]
    for field in required:
        if field not in payload:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required field: {field}"
            )

    org_id = payload["organisation_id"]
    auth_code = payload["auth_code"]
    redirect_uri = payload["redirect_uri"]
    state = payload.get("state", "vic")

    try:
        service = get_plugin_service()
        result = await service.handle_install(org_id, auth_code, redirect_uri, state)
        logger.info(f"Plugin install successful for org {org_id}")
        return result

    except ValueError as e:
        logger.error(f"Plugin install validation error for {org_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Plugin install failed for {org_id}: {e}")
        raise HTTPException(status_code=500, detail="Installation failed")


# ============================================================================
# Uninstall Handler
# ============================================================================


@router.post("/uninstall")
async def handle_uninstall(request: Request) -> dict:
    """
    Handle plugin uninstallation from Tanda Marketplace.

    Called by Tanda when a venue uninstalls the RosterIQ plugin.
    The marketplace sends:
    - organisation_id: The Tanda organisation ID

    The endpoint:
    1. Finds the venue and install record
    2. Revokes OAuth tokens
    3. Marks venue as inactive
    4. Cancels any active subscription
    5. Returns uninstall status

    Args:
        request: HTTP request with uninstall payload

    Returns:
        Uninstallation status

    Raises:
        400: If organisation_id is missing
        404: If organisation not found
        500: If uninstall fails
    """
    # Interim hardening: platform-owner-only (see handle_install note).
    enforce_owner()
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse uninstall request: {e}")
        raise HTTPException(status_code=400, detail="Invalid request payload")

    org_id = payload.get("organisation_id")
    if not org_id:
        raise HTTPException(status_code=400, detail="Missing organisation_id")

    try:
        service = get_plugin_service()
        result = await service.handle_uninstall(org_id)
        logger.info(f"Plugin uninstall successful for org {org_id}")
        return result

    except ValueError as e:
        logger.error(f"Plugin uninstall not found for {org_id}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Plugin uninstall failed for {org_id}: {e}")
        raise HTTPException(status_code=500, detail="Uninstallation failed")


# ============================================================================
# Status Check
# ============================================================================


@router.get("/status/{org_id}")
async def get_status(org_id: str) -> dict:
    """
    Get plugin installation status for an organisation.

    Returns current status including:
    - Installation date
    - Onboarding progress
    - Current subscription tier
    - Token expiration date

    Args:
        org_id: Tanda organisation ID

    Returns:
        Status dict, or 404 if not installed
    """
    try:
        service = get_plugin_service()
        status = service.get_plugin_status(org_id)

        if status is None:
            raise HTTPException(
                status_code=404,
                detail=f"No plugin installation found for {org_id}"
            )

        return status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get status for {org_id}: {e}")
        raise HTTPException(status_code=500, detail="Status check failed")


# ============================================================================
# Health Check
# ============================================================================


@router.get("/health")
async def health_check() -> dict:
    """
    Marketplace health check endpoint.

    Called periodically by Tanda Marketplace to verify the plugin
    service is operational. Returns basic status and version info.

    Returns:
        Health status with version and uptime
    """
    try:
        db = get_db()
        service = get_plugin_service()

        # Quick database connectivity check
        try:
            venues = db.list_venues()
            db_ok = True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            db_ok = False

        return {
            "status": "healthy" if db_ok else "degraded",
            "service": "rosteriq-plugin",
            "version": "1.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "ok" if db_ok else "error",
            "oauth": "configured" if service.client_id else "unconfigured",
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


# ============================================================================
# Utility: Parse and validate callback signature (middleware)
# ============================================================================


async def verify_tanda_signature(
    request: Request,
    x_tanda_signature: str = Header(None),
) -> bool:
    """
    Verify that an incoming request came from Tanda.

    Tanda sends X-Tanda-Signature header with HMAC-SHA256 hash of request body.
    This can be used as a middleware to protect install/uninstall endpoints.

    Args:
        request: HTTP request
        x_tanda_signature: X-Tanda-Signature header value

    Returns:
        True if signature is valid

    Raises:
        HTTPException: If signature is invalid or missing
    """
    if not x_tanda_signature:
        # In development, allow requests without signature
        if os.environ.get("ENV") == "development":
            return True
        raise HTTPException(status_code=401, detail="Missing X-Tanda-Signature header")

    # Get request body
    body = await request.body()
    signature = x_tanda_signature

    service = get_plugin_service()
    if not service.validate_marketplace_token(body.decode(), signature):
        logger.warning(f"Invalid signature in request: {x_tanda_signature[:20]}...")
        raise HTTPException(status_code=401, detail="Invalid signature")

    return True
