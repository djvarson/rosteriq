"""
FastAPI Router for Tanda Marketplace Plugin Integration

Endpoints:
- GET /api/v1/tanda/marketplace/manifest.json — returns plugin manifest (public)
- GET /api/v1/tanda/marketplace/install — redirects to Tanda OAuth authorize
- GET /api/v1/tanda/marketplace/install/callback — handles OAuth callback
- GET /api/v1/tanda/marketplace/listing — returns marketplace listing page
- GET /api/v1/tanda/marketplace/installs — list all installs (OWNER gated)
- DELETE /api/v1/tanda/marketplace/installs/{install_id} — revoke install (OWNER gated)
- POST /api/v1/tanda/marketplace/installs/{install_id}/verify-webhook — verify endpoint
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse

from rosteriq.tanda_marketplace import render_manifest, validate_manifest, DEFAULT_MANIFEST
from rosteriq.tanda_install_flow import (
    handle_oauth_callback,
    verify_webhook_endpoint,
    get_install_store,
    TandaInstall,
)

# Try to import auth; if not available, skip auth gating
try:
    from rosteriq.auth import require_access, AccessLevel, User
except ImportError:
    require_access = None
    AccessLevel = None
    User = None

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

# Get config from env or use defaults
TANDA_MARKETPLACE_CLIENT_ID = os.getenv("TANDA_MARKETPLACE_CLIENT_ID")
TANDA_MARKETPLACE_CLIENT_SECRET = os.getenv("TANDA_MARKETPLACE_CLIENT_SECRET")
ROSTERIQ_PUBLIC_URL = os.getenv("ROSTERIQ_PUBLIC_URL", "http://localhost:8000")

# Tanda OAuth endpoints
TANDA_OAUTH_AUTHORIZE_URL = "https://my.tanda.co/oauth/authorize"

# ============================================================================
# Router
# ============================================================================

router = APIRouter(prefix="/api/v1/tanda/marketplace", tags=["tanda-marketplace"])


# ============================================================================
# Helpers
# ============================================================================


def _install_to_dict(install: TandaInstall, include_token: bool = False) -> Dict[str, Any]:
    """Convert TandaInstall to dict, optionally hiding token."""
    data = install.to_dict()
    if not include_token:
        data.pop("access_token", None)
        data.pop("refresh_token", None)
    return data


# ============================================================================
# Public Endpoints (No Auth)
# ============================================================================


@router.get("/manifest.json")
async def get_manifest() -> Dict[str, Any]:
    """
    Get plugin manifest in JSON format.

    This endpoint is public and called by Tanda's review team.
    Returns the rendered manifest with URLs substituted.
    """
    logger.info("Manifest requested")
    manifest = render_manifest(ROSTERIQ_PUBLIC_URL)
    return manifest


@router.get("/install")
async def start_install(state: Optional[str] = Query(None)) -> RedirectResponse:
    """
    Redirect to Tanda OAuth authorize endpoint.

    Initiates the OAuth 2.0 authorization flow. User is redirected to Tanda
    to grant RosterIQ permission to access their organization.

    Args:
        state: CSRF state parameter (generated if not provided)

    Returns:
        RedirectResponse to Tanda OAuth authorize URL
    """
    if not TANDA_MARKETPLACE_CLIENT_ID:
        return RedirectResponse(
            url="/static/error-no-config.html",
            status_code=302,
        )

    # Generate state if not provided
    if not state:
        import secrets
        state = secrets.token_urlsafe(32)

    # Build authorize URL
    authorize_url = (
        f"{TANDA_OAUTH_AUTHORIZE_URL}"
        f"?client_id={TANDA_MARKETPLACE_CLIENT_ID}"
        f"&response_type=code"
        f"&scope=read:employees%20read:schedules%20write:schedules%20read:timesheets%20read:leave%20webhooks"
        f"&redirect_uri={ROSTERIQ_PUBLIC_URL}/api/v1/tanda/marketplace/install/callback"
        f"&state={state}"
    )

    logger.info(f"Redirecting to Tanda OAuth authorize (state={state[:8]}...)")
    return RedirectResponse(url=authorize_url, status_code=302)


@router.get("/install/callback")
async def handle_install_callback(
    code: str = Query(...),
    state: str = Query(...),
    error: Optional[str] = Query(None),
) -> RedirectResponse | JSONResponse:
    """
    Handle OAuth callback from Tanda.

    Exchanges authorization code for access token, creates install record,
    and redirects to success page.

    Args:
        code: Authorization code from Tanda
        state: State parameter for CSRF
        error: Optional error from Tanda (if present, auth failed)

    Returns:
        RedirectResponse to success page or JSON error
    """
    # Handle auth errors
    if error:
        logger.warning(f"OAuth error: {error}")
        return JSONResponse(
            status_code=400,
            content={"error": error, "message": "Authorization failed"},
        )

    if not TANDA_MARKETPLACE_CLIENT_ID or not TANDA_MARKETPLACE_CLIENT_SECRET:
        logger.error("OAuth credentials not configured")
        raise HTTPException(
            status_code=500,
            detail="Marketplace not configured (missing credentials)",
        )

    try:
        # Exchange code for token
        install = await handle_oauth_callback(
            code=code,
            state=state,
            client_id=TANDA_MARKETPLACE_CLIENT_ID,
            client_secret=TANDA_MARKETPLACE_CLIENT_SECRET,
            redirect_uri=f"{ROSTERIQ_PUBLIC_URL}/api/v1/tanda/marketplace/install/callback",
        )

        logger.info(f"Install created: {install.install_id}")

        # Verify webhook endpoint
        webhook_url = f"{ROSTERIQ_PUBLIC_URL}/api/v1/tanda/webhook"
        try:
            webhook_ok = await verify_webhook_endpoint(install, webhook_url)
            if not webhook_ok:
                logger.warning(f"Webhook verification failed for {install.install_id}")
        except Exception as e:
            logger.warning(f"Webhook verification error: {e}")

        # Redirect to success page with query params
        success_url = (
            f"/static/tanda_installed.html"
            f"?install_id={install.install_id}"
            f"&org={install.tanda_org_name}"
            f"&scopes={','.join(install.scopes_granted)}"
        )
        return RedirectResponse(url=success_url, status_code=302)

    except ValueError as e:
        logger.error(f"OAuth callback error: {e}")
        return JSONResponse(
            status_code=400,
            content={"error": "token_exchange_failed", "message": str(e)},
        )
    except Exception as e:
        logger.exception(f"Unexpected error during OAuth callback")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/listing")
async def get_listing() -> FileResponse | Dict[str, Any]:
    """
    Get marketplace listing page.

    Serves the marketing-quality listing page for Tanda marketplace.
    """
    from pathlib import Path

    listing_path = Path(__file__).parent.parent / "static" / "tanda_listing.html"

    if listing_path.exists():
        return FileResponse(
            path=listing_path,
            media_type="text/html",
        )
    else:
        raise HTTPException(status_code=404, detail="Listing not found")


# ============================================================================
# Admin Endpoints (OWNER-gated if auth available)
# ============================================================================


@router.get("/installs")
async def list_installs(
    request: Request,
    user: Optional[User] = None,
) -> List[Dict[str, Any]]:
    """
    List all RosterIQ installs across all Tanda organizations.

    OWNER-gated: requires require_access(AccessLevel.OWNER) if auth is enabled.

    Returns:
        List of install records (without tokens)
    """
    # If auth is available, check access
    if require_access:
        try:
            await require_access(AccessLevel.OWNER)(request=request)
        except HTTPException:
            raise HTTPException(status_code=403, detail="Owner access required")

    store = get_install_store()
    installs = store.list_all()

    return [_install_to_dict(install, include_token=False) for install in installs]


@router.delete("/installs/{install_id}")
async def revoke_install(
    install_id: str,
    request: Request,
) -> Dict[str, Any]:
    """
    Revoke a RosterIQ install.

    OWNER-gated: requires require_access(AccessLevel.OWNER) if auth is enabled.

    Args:
        install_id: Install ID to revoke

    Returns:
        Updated install record
    """
    # If auth is available, check access
    if require_access:
        try:
            await require_access(AccessLevel.OWNER)(request=request)
        except HTTPException:
            raise HTTPException(status_code=403, detail="Owner access required")

    store = get_install_store()
    install = store.get(install_id)

    if not install:
        raise HTTPException(status_code=404, detail="Install not found")

    success = store.revoke(install_id)
    if success:
        logger.info(f"Revoked install {install_id}")
        return _install_to_dict(install, include_token=False)
    else:
        raise HTTPException(status_code=500, detail="Failed to revoke install")


@router.post("/installs/{install_id}/verify-webhook")
async def verify_webhook(
    install_id: str,
    request: Request,
) -> Dict[str, Any]:
    """
    Trigger webhook endpoint verification.

    Posts a verification ping to our webhook endpoint to ensure it's reachable.
    Useful for testing or re-verifying after deployment changes.

    Args:
        install_id: Install ID to verify

    Returns:
        Verification result
    """
    # If auth is available, check access
    if require_access:
        try:
            await require_access(AccessLevel.OWNER)(request=request)
        except HTTPException:
            raise HTTPException(status_code=403, detail="Owner access required")

    store = get_install_store()
    install = store.get(install_id)

    if not install:
        raise HTTPException(status_code=404, detail="Install not found")

    webhook_url = f"{ROSTERIQ_PUBLIC_URL}/api/v1/tanda/webhook"

    try:
        result = await verify_webhook_endpoint(install, webhook_url)
        return {
            "install_id": install_id,
            "webhook_url": webhook_url,
            "verified": result,
        }
    except Exception as e:
        logger.error(f"Webhook verification error: {e}")
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")
