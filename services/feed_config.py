"""
Feed configuration service for managing data feed settings per venue.

Stores per-venue feed configs: which feeds are enabled, API keys (encrypted),
poll intervals, and custom parameters. Sensitive fields are masked in responses.

Usage:
    from rosteriq.services.feed_config import feed_config_service

    # Get config for a feed at a venue
    config = await feed_config_service.get_config(venue_id, "weather")

    # Set config
    await feed_config_service.set_config(venue_id, "weather", {
        "api_key": "sk_...",
        "poll_interval_minutes": 30,
    })

    # Test connectivity
    result = await feed_config_service.test_feed(venue_id, "weather")
"""

import os
import json
import logging
from typing import Optional, Any
from datetime import datetime
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken

from rosteriq.database import get_db

logger = logging.getLogger(__name__)

# Encryption key for API keys at rest
ENCRYPTION_KEY = os.getenv("FEED_CONFIG_ENCRYPTION_KEY", "dev-key-not-secure")
if ENCRYPTION_KEY.startswith("dev"):
    logger.warning("FEED_CONFIG_ENCRYPTION_KEY is using dev default — set in production")

try:
    cipher = Fernet(ENCRYPTION_KEY.encode() if len(ENCRYPTION_KEY) < 44 else ENCRYPTION_KEY)
except Exception:
    # Fall back to a basic cipher if key is malformed
    cipher = None
    logger.warning("Could not initialize Fernet cipher — encryption disabled")


# ============================================================================
# Feed metadata
# ============================================================================

AVAILABLE_FEEDS = {
    "weather": {
        "display_name": "Weather (BOM)",
        "description": "Australian Bureau of Meteorology weather data",
        "requires_api_key": False,
        "requires_location": True,
        "default_poll_interval_minutes": 60,
        "config_params": [
            {"name": "latitude", "type": "number", "required": True},
            {"name": "longitude", "type": "number", "required": True},
        ],
    },
    "events": {
        "display_name": "Events",
        "description": "Local events and festivals",
        "requires_api_key": True,
        "default_poll_interval_minutes": 120,
        "config_params": [
            {"name": "api_key", "type": "password", "required": True},
            {"name": "radius_km", "type": "number", "default": 15},
        ],
    },
    "foot_traffic": {
        "display_name": "Foot Traffic",
        "description": "Foot traffic sensors and footpath data",
        "requires_api_key": True,
        "default_poll_interval_minutes": 30,
        "config_params": [
            {"name": "api_key", "type": "password", "required": True},
            {"name": "sensor_id", "type": "string", "required": True},
        ],
    },
    "reservations": {
        "display_name": "Reservations (Resdiary)",
        "description": "Restaurant reservation system data",
        "requires_api_key": True,
        "default_poll_interval_minutes": 15,
        "config_params": [
            {"name": "api_key", "type": "password", "required": True},
            {"name": "venue_code", "type": "string", "required": True},
        ],
    },
    "swiftpos": {
        "display_name": "POS (SwiftPOS)",
        "description": "Point of sale transaction data",
        "requires_api_key": True,
        "default_poll_interval_minutes": 10,
        "config_params": [
            {"name": "api_key", "type": "password", "required": True},
            {"name": "location_id", "type": "string", "required": True},
        ],
    },
    "calendar": {
        "display_name": "Calendar Events",
        "description": "Private calendar events and bookings",
        "requires_api_key": False,
        "default_poll_interval_minutes": 30,
        "config_params": [],
    },
    "sports": {
        "display_name": "Sports Events (AFL/NRL)",
        "description": "Australian sports fixtures and schedules",
        "requires_api_key": False,
        "default_poll_interval_minutes": 1440,
        "config_params": [],
    },
    "delivery": {
        "display_name": "Delivery Platforms",
        "description": "Food delivery orders (Uber Eats, Menulog, etc)",
        "requires_api_key": True,
        "default_poll_interval_minutes": 5,
        "config_params": [
            {"name": "api_key", "type": "password", "required": True},
        ],
    },
    "tourism": {
        "display_name": "Tourism Data",
        "description": "Local tourism and visitor trends",
        "requires_api_key": False,
        "default_poll_interval_minutes": 240,
        "config_params": [],
    },
    "nearby_venues": {
        "display_name": "Nearby Venues",
        "description": "Competitor and nearby venue data",
        "requires_api_key": False,
        "requires_location": True,
        "default_poll_interval_minutes": 120,
        "config_params": [],
    },
    "economic": {
        "display_name": "Economic Indicators",
        "description": "Economic data and business sentiment",
        "requires_api_key": False,
        "default_poll_interval_minutes": 1440,
        "config_params": [],
    },
}


# ============================================================================
# Encryption utilities
# ============================================================================

def _encrypt_api_key(api_key: str) -> str:
    """Encrypt an API key using Fernet."""
    if not cipher:
        return api_key  # Fallback: store plaintext (development only)
    try:
        encrypted = cipher.encrypt(api_key.encode())
        return encrypted.decode()
    except Exception as e:
        logger.error(f"Failed to encrypt API key: {e}")
        return api_key


def _decrypt_api_key(encrypted_key: str) -> str:
    """Decrypt an API key using Fernet."""
    if not cipher:
        return encrypted_key  # Fallback
    try:
        decrypted = cipher.decrypt(encrypted_key.encode())
        return decrypted.decode()
    except (InvalidToken, Exception) as e:
        logger.error(f"Failed to decrypt API key: {e}")
        return ""


def _mask_api_key(api_key: str, show_chars: int = 4) -> str:
    """Mask an API key, showing only last N characters."""
    if not api_key or len(api_key) <= show_chars:
        return "***"
    return f"{'*' * (len(api_key) - show_chars)}{api_key[-show_chars:]}"


# ============================================================================
# Feed config service
# ============================================================================

class FeedConfigService:
    """Manages data feed configuration per venue."""

    def __init__(self, db=None):
        self.db = db or get_db()

    async def get_config(
        self,
        venue_id: str,
        feed_name: str,
        mask_sensitive: bool = True,
    ) -> Optional[dict]:
        """
        Get config for a feed at a venue.

        Args:
            venue_id: Venue ID
            feed_name: Name of the feed (e.g. "weather")
            mask_sensitive: If True, mask API keys in response

        Returns:
            Config dict with enabled status, poll interval, custom params, or None
        """
        config = self.db.get_feed_config(venue_id, feed_name)

        if config is None:
            return None

        # Decrypt API keys if present
        config_copy = dict(config)
        if "api_key" in config_copy and config_copy["api_key"]:
            try:
                config_copy["api_key"] = _decrypt_api_key(config_copy["api_key"])
            except Exception:
                config_copy["api_key"] = ""

        # Mask sensitive fields in response
        if mask_sensitive:
            if "api_key" in config_copy and config_copy["api_key"]:
                config_copy["api_key_masked"] = _mask_api_key(config_copy["api_key"])
                del config_copy["api_key"]

        return config_copy

    async def set_config(
        self,
        venue_id: str,
        feed_name: str,
        config: dict,
    ) -> dict:
        """
        Set or update config for a feed at a venue.

        Args:
            venue_id: Venue ID
            feed_name: Feed name
            config: Config dict (api_key, poll_interval_minutes, custom params)

        Returns:
            Updated config dict (with masked API key)
        """
        # Validate feed exists
        if feed_name not in AVAILABLE_FEEDS:
            raise ValueError(f"Unknown feed: {feed_name}")

        # Get existing config or create new
        existing = self.db.get_feed_config(venue_id, feed_name)
        updated_config = existing.copy() if existing else {}

        # Merge in new values
        updated_config.update(config)

        # Encrypt API key if present
        if "api_key" in updated_config and updated_config["api_key"]:
            updated_config["api_key"] = _encrypt_api_key(updated_config["api_key"])

        # Set defaults
        updated_config.setdefault("feed_name", feed_name)
        updated_config.setdefault("venue_id", venue_id)
        updated_config.setdefault("enabled", True)
        updated_config.setdefault("poll_interval_minutes",
                                 AVAILABLE_FEEDS[feed_name]["default_poll_interval_minutes"])
        updated_config.setdefault("last_updated_at", datetime.utcnow())
        updated_config.setdefault("last_tested_at", None)
        updated_config.setdefault("last_test_status", None)

        # Save to database
        self.db.save_feed_config(venue_id, feed_name, updated_config)

        # Return masked response
        return await self.get_config(venue_id, feed_name, mask_sensitive=True)

    async def list_enabled_feeds(self, venue_id: str) -> list[str]:
        """List names of enabled feeds for a venue."""
        configs = self.db.list_feed_configs(venue_id)
        return [c["feed_name"] for c in configs if c.get("enabled", False)]

    async def list_all_configs(self, venue_id: str) -> list[dict]:
        """List all feed configs for a venue (with masked API keys)."""
        configs = self.db.list_feed_configs(venue_id)
        result = []
        for cfg in configs:
            cfg_copy = dict(cfg)
            if "api_key" in cfg_copy and cfg_copy["api_key"]:
                try:
                    decrypted = _decrypt_api_key(cfg_copy["api_key"])
                    cfg_copy["api_key_masked"] = _mask_api_key(decrypted)
                except Exception:
                    cfg_copy["api_key_masked"] = "***"
                del cfg_copy["api_key"]
            result.append(cfg_copy)
        return result

    async def test_feed(self, venue_id: str, feed_name: str) -> dict:
        """
        Test connectivity to a feed.

        Returns dict with:
            success: bool
            message: str
            status_code: int (200 for success, otherwise error code)
            latency_ms: float
            feed_name: str
        """
        import time

        config = await self.get_config(venue_id, feed_name, mask_sensitive=False)

        if config is None:
            return {
                "success": False,
                "message": f"No config found for feed '{feed_name}' at venue",
                "status_code": 404,
                "latency_ms": 0,
                "feed_name": feed_name,
            }

        # Try to import and instantiate the feed adapter
        try:
            start_time = time.time()

            # Import the adapter
            module_name = feed_name
            if feed_name == "swiftpos":
                module_name = "swiftpos"
            elif feed_name == "foot_traffic":
                module_name = "foot_traffic"
            elif feed_name == "nearby_venues":
                module_name = "nearby_venues"

            module = __import__(f"rosteriq.data_feeds.{module_name}", fromlist=[feed_name])

            # Find adapter class (usually named like WeatherAdapter, EventsAdapter, etc)
            adapter_class = None
            for name in dir(module):
                obj = getattr(module, name)
                if (isinstance(obj, type) and
                    hasattr(obj, "fetch_signals") and
                    name.endswith("Adapter")):
                    adapter_class = obj
                    break

            if not adapter_class:
                return {
                    "success": False,
                    "message": f"Could not find adapter class for '{feed_name}'",
                    "status_code": 500,
                    "latency_ms": (time.time() - start_time) * 1000,
                    "feed_name": feed_name,
                }

            # Instantiate with config
            api_key = config.get("api_key", "")
            adapter = adapter_class(api_key=api_key)

            # Call is_available() to test
            is_available = await adapter.is_available()
            latency_ms = (time.time() - start_time) * 1000

            if is_available:
                # Update last test timestamp
                config["last_tested_at"] = datetime.utcnow()
                config["last_test_status"] = "success"
                self.db.save_feed_config(venue_id, feed_name, config)

                return {
                    "success": True,
                    "message": f"Connected to {AVAILABLE_FEEDS[feed_name]['display_name']}",
                    "status_code": 200,
                    "latency_ms": latency_ms,
                    "feed_name": feed_name,
                }
            else:
                config["last_tested_at"] = datetime.utcnow()
                config["last_test_status"] = "failed"
                self.db.save_feed_config(venue_id, feed_name, config)

                return {
                    "success": False,
                    "message": f"Feed returned unavailable status",
                    "status_code": 503,
                    "latency_ms": latency_ms,
                    "feed_name": feed_name,
                }

        except Exception as e:
            logger.exception(f"Error testing feed {feed_name}: {e}")
            config["last_tested_at"] = datetime.utcnow()
            config["last_test_status"] = "error"
            self.db.save_feed_config(venue_id, feed_name, config)

            return {
                "success": False,
                "message": f"Error testing feed: {str(e)}",
                "status_code": 500,
                "latency_ms": 0,
                "feed_name": feed_name,
            }

    def get_available_feeds(self) -> dict[str, dict]:
        """Get list of all available feeds with descriptions."""
        return AVAILABLE_FEEDS


# Convenience instance
feed_config_service = FeedConfigService()
