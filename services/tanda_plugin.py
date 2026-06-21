"""
Tanda Marketplace plugin lifecycle service.

Handles installation, uninstallation, and status tracking for RosterIQ
in the Tanda Marketplace. Manages OAuth token exchange, venue creation,
and subscription initialization.

Usage:
    service = TandaPluginService(db, config)
    install = await service.handle_install(org_id, auth_code, redirect_uri)
    await service.handle_uninstall(org_id)
"""

import os
import logging
import hashlib
import hmac

import httpx
from datetime import datetime, timedelta
from typing import Optional
from decimal import Decimal

from rosteriq.database import BaseStore
from rosteriq.models import VenueConfig, State, TandaCredentials
from rosteriq.tanda_adapter import TandaOAuth, TandaAdapter

logger = logging.getLogger(__name__)


class TandaPluginService:
    """
    Manages RosterIQ plugin lifecycle in the Tanda Marketplace.

    Responsibilities:
    - OAuth token exchange when venue installs
    - Venue record creation and initial setup
    - Onboarding flow initiation
    - Token validation and revocation on uninstall
    - Health and status monitoring
    """

    def __init__(
        self,
        db: BaseStore,
        tanda_client_id: Optional[str] = None,
        tanda_client_secret: Optional[str] = None,
        tanda_webhook_secret: Optional[str] = None,
        redirect_base_uri: str = "https://api.rosteriq.com.au",
    ):
        """
        Initialize the plugin service.

        Args:
            db: Database store instance
            tanda_client_id: OAuth client ID (from env or config)
            tanda_client_secret: OAuth client secret (from env or config)
            tanda_webhook_secret: Webhook signature secret from Tanda
            redirect_base_uri: Base URI for OAuth redirects
        """
        self.db = db
        self.client_id = tanda_client_id or os.environ.get("TANDA_CLIENT_ID", "")
        self.client_secret = tanda_client_secret or os.environ.get("TANDA_CLIENT_SECRET", "")
        self.webhook_secret = tanda_webhook_secret or os.environ.get("TANDA_WEBHOOK_SECRET", "")
        self.redirect_base_uri = redirect_base_uri

    async def handle_install(
        self,
        organisation_id: str,
        auth_code: str,
        redirect_uri: str,
        state_code: str = "vic",
    ) -> dict:
        """
        Handle marketplace installation: exchange auth code, create venue, start onboarding.

        Called when a Tanda organisation installs the RosterIQ plugin.
        Exchanges the authorization code for OAuth tokens, creates a venue
        record, and initializes the onboarding flow.

        Args:
            organisation_id: Tanda organisation ID
            auth_code: Authorization code from Tanda OAuth flow
            redirect_uri: OAuth redirect URI (must match registered)
            state_code: Australian state code for wage calculations (default: vic)

        Returns:
            dict with install status, venue_id, and next steps

        Raises:
            ValueError: If token exchange fails or venue creation fails
        """
        try:
            # Step 1: Exchange authorization code for tokens
            oauth = TandaOAuth(self.client_id, self.client_secret, redirect_uri)
            credentials = await oauth.exchange_code(auth_code)

            # Update credentials with org_id
            credentials.org_id = organisation_id

            # Step 2: Verify credentials by calling Tanda API
            async with TandaAdapter(credentials, state=State(state_code)) as adapter:
                is_healthy = await adapter.health_check()
                if not is_healthy:
                    raise ValueError("Failed to verify Tanda API access")

            # Step 3: Create venue record
            venue_id = self._generate_venue_id(organisation_id)
            venue = VenueConfig(
                id=venue_id,
                name=f"Venue {organisation_id}",  # Will be updated during onboarding
                tanda_org_id=organisation_id,
                state=State(state_code),
                timezone="Australia/Melbourne",  # Default, can be updated
                min_staff={},  # Will be configured during onboarding
                max_labour_pct=Decimal("30.0"),  # Default target, configurable
                pos_system="unknown",  # Will be detected during onboarding
                created_at=datetime.utcnow(),
            )
            self.db.save_venue(venue)

            # Step 4: Save plugin install state
            install_state = {
                "venue_id": venue_id,
                "organisation_id": organisation_id,
                "status": "installed",
                "auth_code": auth_code,  # For refresh/revocation later
                "access_token": credentials.access_token,
                "refresh_token": credentials.refresh_token,
                "token_expires_at": credentials.token_expires_at,
                "installed_at": datetime.utcnow(),
                "onboarding_completed": False,
                "tier": "starter",  # Default tier
            }
            self.db.save_plugin_install(install_state)

            # Step 5: Initialize onboarding state
            onboarding_state = {
                "venue_id": venue_id,
                "organisation_id": organisation_id,
                "step": "venue_details",  # First step: collect venue name, timezone
                "completed_steps": [],
                "started_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
            self.db.save_onboarding_state(onboarding_state)

            logger.info(
                f"Plugin installed for org {organisation_id}, venue {venue_id}"
            )

            return {
                "status": "success",
                "venue_id": venue_id,
                "organisation_id": organisation_id,
                "next_step": "/onboarding/venue-details",
                "message": "Installation successful. Please complete onboarding.",
            }

        except Exception as e:
            logger.error(f"Plugin install failed for {organisation_id}: {e}")
            raise ValueError(f"Installation failed: {str(e)}")

    def build_adapter(
        self,
        organisation_id: str,
        state_code: str = "vic",
    ) -> TandaAdapter:
        """
        Construct a TandaAdapter from a stored plugin install record, wired so
        that any OAuth token refresh is persisted back to the install record.

        Without this persistence callback, a token refreshed mid-session is
        only updated in memory and is lost on process restart (the bug that
        Deputy/MYOB/HumanForce already avoid via on_token_refresh). Here we
        construct credentials from the stored install and pass a callback that
        writes the refreshed access/refresh tokens back to the database.

        Args:
            organisation_id: Tanda organisation ID with a stored install.
            state_code: Australian state code for employee mapping.

        Returns:
            A TandaAdapter ready to use as an async context manager.

        Raises:
            ValueError: If no install record exists for the organisation.
        """
        install = self.db.get_plugin_install(organisation_id)
        if not install:
            raise ValueError(f"No install found for org {organisation_id}")

        credentials = TandaCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=install.get("access_token"),
            refresh_token=install.get("refresh_token"),
            token_expires_at=install.get("token_expires_at"),
            org_id=organisation_id,
        )

        def _persist_refreshed_tokens(creds: TandaCredentials) -> None:
            # Re-read the latest install so we don't clobber concurrent writes,
            # then persist the refreshed tokens.
            record = self.db.get_plugin_install(organisation_id) or install
            record["access_token"] = creds.access_token
            record["refresh_token"] = creds.refresh_token
            record["token_expires_at"] = creds.token_expires_at
            self.db.save_plugin_install(record)
            logger.info(
                f"Persisted refreshed Tanda token for org {organisation_id}"
            )

        return TandaAdapter(
            credentials,
            state=State(state_code),
            on_token_refresh=_persist_refreshed_tokens,
        )

    async def handle_uninstall(self, organisation_id: str) -> dict:
        """
        Handle uninstallation: revoke tokens, mark venue inactive, cancel billing.

        Called when a Tanda organisation uninstalls the RosterIQ plugin.
        Cleans up credentials, marks the venue as inactive, and cancels
        any active subscription.

        Args:
            organisation_id: Tanda organisation ID to uninstall

        Returns:
            dict with uninstall status

        Raises:
            ValueError: If venue not found
        """
        try:
            # Step 1: Find the install record
            install = self.db.get_plugin_install(organisation_id)
            if not install:
                raise ValueError(f"No install found for org {organisation_id}")

            venue_id = install["venue_id"]

            # Step 2: Revoke OAuth tokens at Tanda (best-effort).
            tokens_revoked = False
            try:
                tokens_revoked = await self._revoke_oauth_tokens(
                    install.get("access_token", ""),
                    install.get("refresh_token", ""),
                )
            except Exception as e:
                logger.warning(f"Failed to revoke tokens for {organisation_id}: {e}")

            # Step 3: Note: venue record remains in database for audit trail
            # In production, add an is_active field to VenueConfig if needed
            venue = self.db.get_venue(venue_id)
            if venue:
                # Venue remains in database but marked as deactivated via install status
                logger.info(f"Venue {venue_id} marked for deactivation")

            # Step 4: Mark install as uninstalled
            install["status"] = "uninstalled"
            install["uninstalled_at"] = datetime.utcnow()
            self.db.save_plugin_install(install)

            # Step 5: Cancel billing subscription (if any)
            try:
                subscription = self.db.get_subscription(venue_id)
                if subscription and subscription["status"] == "active":
                    subscription["status"] = "cancelled"
                    subscription["cancelled_at"] = datetime.utcnow()
                    self.db.save_subscription(subscription)
                    logger.info(f"Cancelled subscription for venue {venue_id}")
            except Exception as e:
                logger.warning(f"Failed to cancel subscription for {venue_id}: {e}")

            logger.info(f"Plugin uninstalled for org {organisation_id}, venue {venue_id}")

            return {
                "status": "success",
                "tokens_revoked": tokens_revoked,
                "message": (
                    "Uninstallation complete. Tokens revoked and venue deactivated."
                    if tokens_revoked
                    else "Uninstallation complete and venue deactivated. Token "
                         "revocation could not be confirmed with Tanda; tokens may "
                         "remain valid until they expire."
                ),
            }

        except Exception as e:
            logger.error(f"Plugin uninstall failed for {organisation_id}: {e}")
            raise ValueError(f"Uninstallation failed: {str(e)}")

    def get_plugin_status(self, organisation_id: str) -> Optional[dict]:
        """
        Get current plugin installation status for an organisation.

        Args:
            organisation_id: Tanda organisation ID

        Returns:
            dict with install status, or None if not installed

        Example return:
            {
                "status": "installed",
                "venue_id": "venue_123",
                "organisation_id": "org_456",
                "installed_at": "2026-04-25T10:30:00Z",
                "onboarding_completed": True,
                "tier": "professional",
                "active": True,
            }
        """
        try:
            install = self.db.get_plugin_install(organisation_id)
            if not install:
                return None

            venue = self.db.get_venue(install["venue_id"])
            subscription = self.db.get_subscription(install["venue_id"])
            onboarding = self.db.get_onboarding_state(install["venue_id"])

            return {
                "status": install.get("status", "unknown"),
                "venue_id": install["venue_id"],
                "organisation_id": organisation_id,
                "installed_at": install.get("installed_at"),
                "uninstalled_at": install.get("uninstalled_at"),
                "onboarding_completed": install.get("onboarding_completed", False),
                "onboarding_step": onboarding.get("step") if onboarding else None,
                "tier": install.get("tier", "starter"),
                "subscription_status": subscription.get("status") if subscription else "none",
                "venue_exists": venue is not None,
                "token_expires_at": install.get("token_expires_at"),
            }

        except Exception as e:
            logger.error(f"Failed to get status for {organisation_id}: {e}")
            return None

    def validate_marketplace_token(self, token: str, signature: str) -> bool:
        """
        Validate that a callback signature came from Tanda Marketplace.

        Marketplace sends webhook callbacks with an HMAC-SHA256 signature.
        This method verifies the signature matches our webhook secret.

        Args:
            token: The request body/token to verify
            signature: The X-Tanda-Signature header value

        Returns:
            True if signature is valid, False otherwise
        """
        if not self.webhook_secret:
            logger.warning("No webhook secret configured — skipping verification")
            return True

        try:
            expected = hmac.new(
                self.webhook_secret.encode(),
                token.encode(),
                hashlib.sha256,
            ).hexdigest()
            return hmac.compare_digest(expected, signature)
        except Exception as e:
            logger.error(f"Signature validation failed: {e}")
            return False

    def _generate_venue_id(self, organisation_id: str) -> str:
        """Generate a unique venue ID from Tanda organisation ID."""
        timestamp = datetime.utcnow().strftime("%s")
        return f"venue_{organisation_id}_{timestamp}"

    REVOKE_URL = "https://my.tanda.co/api/oauth/revoke"

    async def _revoke_oauth_tokens(self, access_token: str, refresh_token: str = "") -> bool:
        """
        Revoke OAuth tokens at Tanda's revocation endpoint (RFC 7009, best-effort).

        Returns True if at least one token was accepted for revocation. Never
        raises — revocation failure must not block uninstall.
        """
        revoked_any = False
        async with httpx.AsyncClient(timeout=15.0) as client:
            for token, hint in ((access_token, "access_token"), (refresh_token, "refresh_token")):
                if not token:
                    continue
                try:
                    resp = await client.post(
                        self.REVOKE_URL,
                        data={
                            "token": token,
                            "token_type_hint": hint,
                            "client_id": self.client_id,
                            "client_secret": self.client_secret,
                        },
                    )
                    # RFC 7009: a successful revocation returns 200 (Tanda may
                    # also use 204). 200 is returned even for an unknown token.
                    if resp.status_code in (200, 204):
                        logger.info(f"Revoked Tanda {hint}")
                        revoked_any = True
                    else:
                        logger.warning(f"Tanda {hint} revoke returned {resp.status_code}")
                except httpx.HTTPError as e:
                    logger.warning(f"Tanda {hint} revoke request failed: {e}")
        return revoked_any
