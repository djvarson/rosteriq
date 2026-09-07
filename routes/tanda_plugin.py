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

from fastapi import APIRouter, HTTPException, Request, Query, Header, Depends

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_owner, enforce_venue_access
from rosteriq.routes.webhook_routes import verify_hmac_signature
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
# Marketplace request authentication (HMAC signature)
# ============================================================================
#
# Who calls install/uninstall: the Tanda Marketplace itself, directly. At
# install time no RosterIQ venue or user exists yet (the install is what CREATES
# the venue), so there is no JWT to present — these routes therefore live on the
# signed-webhook auth path (see WEBHOOK_EXEMPT in middleware/tenant.py and
# WEBHOOK_PATHS in middleware/auth.py, which wave them past TenantMiddleware's
# JWT check) and are authenticated solely by Tanda's HMAC signature here.


async def verify_tanda_signature(
    request: Request,
    x_tanda_signature: str = Header(None),
) -> bool:
    """
    Dependency that authenticates an inbound Tanda Marketplace call.

    Tanda signs the raw request body with HMAC-SHA256 (keyed on
    TANDA_WEBHOOK_SECRET) and sends the hex digest in X-Tanda-Signature. That
    signature is the ONLY authentication install/uninstall have, so this fails
    CLOSED, mirroring the inbound webhook receiver (routes/webhook_routes.py):

      - no TANDA_WEBHOOK_SECRET configured -> reject with 503, UNLESS ENVIRONMENT
        is an explicit dev/test value. An unset ENVIRONMENT counts as production,
        so a deploy that forgets the secret rejects rather than accepting forged,
        unsigned marketplace calls;
      - secret set but signature header missing -> 401;
      - secret set but signature does not match the body -> 401.

    The secret is read live from the environment (not from the cached plugin
    service singleton, whose secret is captured at construction time) so the
    check is correct even if the singleton was built before the secret was set.

    Returns True on success so it can gate a route via ``Depends``.
    """
    secret = os.environ.get("TANDA_WEBHOOK_SECRET", "")

    if not secret:
        if os.environ.get("ENVIRONMENT", "").lower() not in (
            "development", "dev", "test", "local"
        ):
            logger.error(
                "TANDA_WEBHOOK_SECRET not set — rejecting Tanda plugin call "
                "(fail closed; unset ENVIRONMENT is treated as production)."
            )
            raise HTTPException(
                status_code=503,
                detail="Marketplace signature verification not configured",
            )
        logger.warning(
            "TANDA_WEBHOOK_SECRET not set — Tanda plugin call signatures are NOT "
            "verified (dev mode). Set it in production to reject forged calls."
        )
        return True

    if not x_tanda_signature:
        raise HTTPException(status_code=401, detail="Missing X-Tanda-Signature header")

    # HMAC is computed over the exact raw body bytes Tanda signed. request.body()
    # caches the body, so the handler's later request.json() reads the same bytes.
    body = await request.body()
    if not verify_hmac_signature(body, x_tanda_signature, secret):
        logger.warning("Invalid X-Tanda-Signature on Tanda plugin call")
        raise HTTPException(status_code=401, detail="Invalid signature")

    return True


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
async def handle_install(
    request: Request,
    _verified: bool = Depends(verify_tanda_signature),
) -> dict:
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
    # Authenticated by Tanda's HMAC signature (verify_tanda_signature dependency
    # above); this route is on the signed-webhook auth path, not the JWT path.
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
async def handle_uninstall(
    request: Request,
    _verified: bool = Depends(verify_tanda_signature),
) -> dict:
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
    # Authenticated by Tanda's HMAC signature (verify_tanda_signature dependency
    # above); this route is on the signed-webhook auth path, not the JWT path.
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

        # An install record is tenant data (venue id, tier, subscription,
        # token expiry). Only members of the mapped venue may read it; an
        # install with no venue mapping is platform-owner territory.
        venue_id = status.get("venue_id")
        if venue_id:
            enforce_venue_access(venue_id)
        else:
            enforce_owner()

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
            # install/uninstall fail closed (503) in production until this is
            # configured — surface it so "healthy" can't hide a dead install path
            "marketplace_signature": ("configured" if os.environ.get("TANDA_WEBHOOK_SECRET")
                                      else "unconfigured"),
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")
