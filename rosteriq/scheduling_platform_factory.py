"""
Factory for scheduling platform adapters.

Provides a unified interface to instantiate the correct adapter based on
environment configuration (platform and data mode).
"""

import logging
import os
from typing import Optional

from rosteriq.tanda_adapter import (
    TandaAdapter,
    DemoTandaAdapter,
    TandaClient,
    SchedulingPlatformAdapter,
)
from rosteriq.deputy_adapter import (
    DeputyAdapter,
    DemoDeputyAdapter,
)
from rosteriq.deputy_integration import DeputyClient

logger = logging.getLogger(__name__)


def get_scheduling_adapter(platform: Optional[str] = None) -> SchedulingPlatformAdapter:
    """
    Factory function to get appropriate scheduling platform adapter.

    Reads environment variables:
    - ROSTERIQ_PLATFORM: "tanda", "deputy", "demo_tanda", or "demo_deputy"
                         (default: "tanda")
    - ROSTERIQ_DATA_MODE: "live" or "demo" (default: "demo" if credentials missing)

    For Tanda (live):
    - TANDA_CLIENT_ID
    - TANDA_CLIENT_SECRET

    For Deputy (live):
    - DEPUTY_SUBDOMAIN
    - DEPUTY_ACCESS_TOKEN (or DEPUTY_PERMANENT_TOKEN)

    Args:
        platform: Override environment platform selection (optional)

    Returns:
        SchedulingPlatformAdapter instance (TandaAdapter, DeputyAdapter,
        DemoTandaAdapter, or DemoDeputyAdapter)

    Raises:
        ValueError: If platform is unknown or required credentials are missing
    """
    # Determine platform
    if platform is None:
        platform = os.environ.get("ROSTERIQ_PLATFORM", "tanda").lower().strip()
        if not platform:
            platform = "tanda"

    # Determine data mode
    data_mode = os.environ.get("ROSTERIQ_DATA_MODE", "").lower()

    # Handle explicit demo/live platform names
    if platform in ("demo_tanda", "tanda_demo"):
        logger.info("Using demo Tanda adapter (explicit platform selection)")
        return DemoTandaAdapter()

    if platform in ("demo_deputy", "deputy_demo"):
        logger.info("Using demo Deputy adapter (explicit platform selection)")
        return DemoDeputyAdapter()

    # For "tanda" and "deputy" platforms, check data_mode or credentials
    if platform == "tanda":
        if data_mode == "demo":
            logger.info("Using demo Tanda adapter (ROSTERIQ_DATA_MODE=demo)")
            return DemoTandaAdapter()

        # Try live mode if credentials present
        client_id = os.environ.get("TANDA_CLIENT_ID")
        client_secret = os.environ.get("TANDA_CLIENT_SECRET")

        if client_id and client_secret:
            logger.info("Using real Tanda adapter")
            client = TandaClient(client_id, client_secret)
            return TandaAdapter(client)
        else:
            logger.info("Using demo Tanda adapter (credentials not configured)")
            return DemoTandaAdapter()

    elif platform == "deputy":
        if data_mode == "demo":
            logger.info("Using demo Deputy adapter (ROSTERIQ_DATA_MODE=demo)")
            return DemoDeputyAdapter()

        # Try live mode if credentials present
        subdomain = os.environ.get("DEPUTY_SUBDOMAIN")
        access_token = os.environ.get("DEPUTY_ACCESS_TOKEN")
        permanent_token = os.environ.get("DEPUTY_PERMANENT_TOKEN")

        if subdomain and (access_token or permanent_token):
            logger.info("Using real Deputy adapter")
            client = DeputyClient(
                subdomain=subdomain,
                access_token=access_token,
                permanent_token=permanent_token,
            )
            return DeputyAdapter(client)
        else:
            logger.info("Using demo Deputy adapter (credentials not configured)")
            return DemoDeputyAdapter()

    else:
        raise ValueError(
            f"Unknown scheduling platform: {platform}. "
            f"Supported: tanda, deputy, demo_tanda, demo_deputy"
        )
