"""
Tanda OAuth 2.0 Install Flow for RosterIQ Plugin

Handles the install flow: user clicks "Install" → redirected to Tanda OAuth →
callback exchanges code for token → stores install info.
"""

from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, List
from enum import Enum

# httpx is lazy-imported inside async methods that need it, so this module
# can be imported in environments where httpx is not installed (sandbox/tests).

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

TANDA_OAUTH_TOKEN_URL = "https://my.tanda.co/oauth/token"
TANDA_API_BASE = "https://my.tanda.co/api/v2"


# ============================================================================
# Enums
# ============================================================================


class InstallStatus(str, Enum):
    """Status of a Tanda install."""
    ACTIVE = "active"
    REVOKED = "revoked"
    ERROR = "error"


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class TandaInstall:
    """Represents an installed RosterIQ plugin for a Tanda organization."""

    install_id: str
    tanda_org_id: str
    tanda_org_name: str
    access_token: str
    refresh_token: Optional[str]
    expires_at: datetime
    scopes_granted: List[str]
    installed_at: datetime
    installed_by_user_email: Optional[str]
    status: InstallStatus = InstallStatus.ACTIVE

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "install_id": self.install_id,
            "tanda_org_id": self.tanda_org_id,
            "tanda_org_name": self.tanda_org_name,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at.isoformat(),
            "scopes_granted": self.scopes_granted,
            "installed_at": self.installed_at.isoformat(),
            "installed_by_user_email": self.installed_by_user_email,
            "status": self.status.value,
        }


# ============================================================================
# In-Memory Install Store (Stdlib)
# ============================================================================


class TandaInstallStore:
    """In-memory store for Tanda installs (development; use database in production)."""

    def __init__(self):
        self._installs: Dict[str, TandaInstall] = {}  # install_id -> TandaInstall
        self._org_index: Dict[str, str] = {}  # tanda_org_id -> install_id

    def create(self, install: TandaInstall) -> TandaInstall:
        """Store a new install."""
        self._installs[install.install_id] = install
        self._org_index[install.tanda_org_id] = install.install_id
        logger.info(f"Created install {install.install_id} for org {install.tanda_org_id}")
        return install

    def get(self, install_id: str) -> Optional[TandaInstall]:
        """Get install by ID."""
        return self._installs.get(install_id)

    def get_by_org(self, org_id: str) -> Optional[TandaInstall]:
        """Get install by Tanda org ID."""
        install_id = self._org_index.get(org_id)
        if install_id:
            return self._installs.get(install_id)
        return None

    def list_all(self) -> List[TandaInstall]:
        """List all installs."""
        return list(self._installs.values())

    def revoke(self, install_id: str) -> bool:
        """Revoke an install by setting status to REVOKED."""
        install = self._installs.get(install_id)
        if install:
            install.status = InstallStatus.REVOKED
            logger.info(f"Revoked install {install_id}")
            return True
        return False

    def clear(self) -> None:
        """Clear all installs (for testing)."""
        self._installs.clear()
        self._org_index.clear()
        logger.info("Cleared all installs")


# ============================================================================
# Global Install Store Singleton
# ============================================================================

_install_store: Optional[TandaInstallStore] = None


def get_install_store() -> TandaInstallStore:
    """Get or create the global install store."""
    global _install_store
    if _install_store is None:
        _install_store = TandaInstallStore()
    return _install_store


# ============================================================================
# OAuth Callback Handler
# ============================================================================


async def handle_oauth_callback(
    code: str,
    state: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> TandaInstall:
    """
    Exchange OAuth authorization code for access token and create install.

    Args:
        code: Authorization code from Tanda
        state: State parameter for CSRF protection (not verified here)
        client_id: OAuth client ID
        client_secret: OAuth client secret
        redirect_uri: Redirect URI used in auth request

    Returns:
        TandaInstall object with token and org info

    Raises:
        ValueError: If token exchange fails
        httpx.HTTPError: If API call fails
    """
    logger.info(f"Exchanging authorization code for token (state={state[:8]}...)")

    import httpx  # lazy import — optional dep

    # Exchange code for token
    async with httpx.AsyncClient(timeout=30.0) as client:
        token_response = await client.post(
            TANDA_OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
            },
        )

    if token_response.status_code != 200:
        error_detail = token_response.text
        logger.error(f"Token exchange failed: {token_response.status_code} - {error_detail}")
        raise ValueError(f"Token exchange failed: {token_response.status_code}")

    token_data = token_response.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in", 3600)

    if not access_token:
        logger.error("Token response missing access_token")
        raise ValueError("Token response missing access_token")

    # Fetch org info from Tanda API
    logger.info("Fetching organization info from Tanda")
    async with httpx.AsyncClient(timeout=30.0) as client:
        org_response = await client.get(
            f"{TANDA_API_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if org_response.status_code != 200:
        logger.error(f"Failed to fetch org info: {org_response.status_code}")
        raise ValueError(f"Failed to fetch org info: {org_response.status_code}")

    org_data = org_response.json()
    tanda_org_id = str(org_data.get("organisation_id", org_data.get("id")))
    tanda_org_name = org_data.get("name", "Unknown")
    user_email = org_data.get("email")

    # Calculate expiry time
    from datetime import timedelta
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # Determine scopes granted (Tanda may not return this; default to requested)
    scopes_granted = token_data.get("scope", "").split() if token_data.get("scope") else [
        "read:employees",
        "read:schedules",
        "write:schedules",
        "read:timesheets",
        "read:leave",
        "webhooks",
    ]

    # Create TandaInstall
    install_id = f"inst_{secrets.token_hex(16)}"
    install = TandaInstall(
        install_id=install_id,
        tanda_org_id=tanda_org_id,
        tanda_org_name=tanda_org_name,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        scopes_granted=scopes_granted,
        installed_at=datetime.now(timezone.utc),
        installed_by_user_email=user_email,
        status=InstallStatus.ACTIVE,
    )

    # Store install
    store = get_install_store()
    store.create(install)

    logger.info(f"OAuth callback successful: install_id={install_id}, org={tanda_org_id}")
    return install


# ============================================================================
# Webhook Verification
# ============================================================================


async def verify_webhook_endpoint(
    install: TandaInstall,
    webhook_url: str,
) -> bool:
    """
    Verify that our webhook endpoint is reachable (for Tanda install verification).

    Posts a verification ping to our own webhook endpoint to prove it works.

    Args:
        install: TandaInstall with access token
        webhook_url: Full URL of our webhook endpoint

    Returns:
        True if verification succeeds, False otherwise
    """
    logger.info(f"Verifying webhook endpoint: {webhook_url}")

    import httpx  # lazy import — optional dep

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                webhook_url,
                json={
                    "event_type": "webhook.verify",
                    "install_id": install.install_id,
                    "org_id": install.tanda_org_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                headers={
                    "Authorization": f"Bearer {install.access_token}",
                    "X-RosterIQ-Verify": "true",
                },
            )

        if response.status_code in (200, 201, 204):
            logger.info(f"Webhook verification successful: {response.status_code}")
            return True
        else:
            logger.warning(f"Webhook verification returned: {response.status_code}")
            return False

    except httpx.TimeoutException:
        logger.error("Webhook verification timed out")
        return False
    except httpx.HTTPError as e:
        logger.error(f"Webhook verification failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during webhook verification: {e}")
        return False
