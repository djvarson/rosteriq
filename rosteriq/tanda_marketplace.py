"""
Tanda Marketplace Plugin Manifest and Configuration

Defines the RosterIQ plugin manifest for Tanda's marketplace,
including metadata, OAuth scopes, pricing tiers, and validation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from enum import Enum

# ============================================================================
# Enums
# ============================================================================


class PricingTier(str, Enum):
    """Pricing tier levels for RosterIQ."""
    STARTUP = "startup"
    PRO = "pro"
    ENTERPRISE = "enterprise"


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class PricingInfo:
    """Pricing information for a tier."""
    tier: str
    name: str
    price_per_employee_per_month: float  # AUD
    features: List[str]
    description: str


@dataclass
class PluginManifest:
    """RosterIQ marketplace plugin manifest."""

    plugin_id: str
    name: str
    publisher: str
    version: str
    category: str
    short_description: str
    long_description: str
    required_scopes: List[str]
    webhook_url: str
    oauth_redirect_uri: str
    install_callback_url: str
    support_email: str
    logo_url: str
    screenshot_urls: List[str]
    pricing_tiers: List[PricingInfo] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "plugin_id": self.plugin_id,
            "name": self.name,
            "publisher": self.publisher,
            "version": self.version,
            "category": self.category,
            "short_description": self.short_description,
            "long_description": self.long_description,
            "required_scopes": self.required_scopes,
            "webhook_url": self.webhook_url,
            "oauth_redirect_uri": self.oauth_redirect_uri,
            "install_callback_url": self.install_callback_url,
            "support_email": self.support_email,
            "logo_url": self.logo_url,
            "screenshot_urls": self.screenshot_urls,
            "pricing_tiers": [
                {
                    "tier": t.tier,
                    "name": t.name,
                    "price_per_employee_per_month": t.price_per_employee_per_month,
                    "features": t.features,
                    "description": t.description,
                }
                for t in self.pricing_tiers
            ],
        }


# ============================================================================
# Module-level configuration
# ============================================================================

# App version — read from env or hardcode
APP_VERSION = os.getenv("ROSTERIQ_APP_VERSION", "0.5.0")


# ============================================================================
# Default Manifest
# ============================================================================

DEFAULT_MANIFEST = PluginManifest(
    plugin_id="rosteriq",
    name="RosterIQ",
    publisher="RosterIQ Pty Ltd",
    version=APP_VERSION,
    category="Scheduling Intelligence",
    short_description="AI demand forecasting + accountability for your Tanda roster",
    long_description=(
        "RosterIQ analyzes demand signals—POS sales, weather, events, bookings, foot traffic—"
        "to generate optimized roster recommendations. Three tiers: Basic (draft generation), "
        "Pro (multi-source forecasting), Enterprise (real-time notifications + multi-venue). "
        "Expected labour cost savings: 10–15% per venue per month."
    ),
    required_scopes=[
        "read:employees",
        "read:schedules",
        "write:schedules",
        "read:timesheets",
        "read:leave",
        "webhooks",
    ],
    webhook_url="{BASE_URL}/api/v1/tanda/webhook",
    oauth_redirect_uri="{BASE_URL}/api/v1/tanda/marketplace/install/callback",
    install_callback_url="{BASE_URL}/api/v1/tanda/marketplace/install/callback",
    support_email="support@rosteriq.io",
    logo_url="{BASE_URL}/static/rosteriq-logo.png",
    screenshot_urls=[
        "{BASE_URL}/static/screenshot-1.png",
        "{BASE_URL}/static/screenshot-2.png",
    ],
    pricing_tiers=[
        PricingInfo(
            tier=PricingTier.STARTUP.value,
            name="Startup",
            price_per_employee_per_month=1.50,
            features=[
                "Draft roster generation",
                "Basic demand forecasting",
                "Shift recommendations",
            ],
            description="Perfect for small teams. Generate AI-drafted rosters.",
        ),
        PricingInfo(
            tier=PricingTier.PRO.value,
            name="Pro",
            price_per_employee_per_month=3.00,
            features=[
                "All Startup features",
                "Multi-source forecasting (POS, weather, events, bookings)",
                "Extended forecast horizon (14 days)",
                "On-shift recommendations",
            ],
            description="For growing venues. Deep demand insights + on-shift tools.",
        ),
        PricingInfo(
            tier=PricingTier.ENTERPRISE.value,
            name="Enterprise",
            price_per_employee_per_month=5.50,
            features=[
                "All Pro features",
                "Real-time push notifications",
                "Multi-venue management",
                "Conversational AI recommendations",
                "Custom integrations",
                "Priority support",
            ],
            description="For multi-venue operators. Real-time intelligence + conversational AI.",
        ),
    ],
)


# ============================================================================
# Public API Functions
# ============================================================================


def render_manifest(public_url: str) -> Dict[str, Any]:
    """
    Render manifest with {BASE_URL} substituted.

    Args:
        public_url: The public URL of RosterIQ (e.g., https://rosteriq.io)

    Returns:
        Dictionary representation of the manifest with URLs substituted.
    """
    manifest_dict = DEFAULT_MANIFEST.to_dict()

    # Recursively substitute {BASE_URL} in all string values
    def substitute_urls(obj: Any, base_url: str) -> Any:
        if isinstance(obj, dict):
            return {k: substitute_urls(v, base_url) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [substitute_urls(item, base_url) for item in obj]
        elif isinstance(obj, str):
            return obj.replace("{BASE_URL}", base_url)
        else:
            return obj

    return substitute_urls(manifest_dict, public_url)


def validate_manifest(manifest: Dict[str, Any]) -> List[str]:
    """
    Validate manifest structure and required fields.

    Args:
        manifest: Dictionary representation of manifest

    Returns:
        List of validation errors (empty list = valid).
    """
    errors: List[str] = []

    # Required top-level fields
    required_fields = [
        "plugin_id",
        "name",
        "publisher",
        "version",
        "category",
        "short_description",
        "long_description",
        "required_scopes",
        "webhook_url",
        "oauth_redirect_uri",
        "install_callback_url",
        "support_email",
        "logo_url",
        "screenshot_urls",
    ]

    for field_name in required_fields:
        if field_name not in manifest:
            errors.append(f"Missing required field: {field_name}")
        elif isinstance(manifest[field_name], str) and not manifest[field_name].strip():
            errors.append(f"Required field '{field_name}' is empty")

    # Validate scopes
    if "required_scopes" in manifest:
        scopes = manifest["required_scopes"]
        if not isinstance(scopes, list):
            errors.append("required_scopes must be a list")
        else:
            valid_scopes = {
                "read:employees",
                "read:schedules",
                "write:schedules",
                "read:timesheets",
                "read:leave",
                "webhooks",
            }
            for scope in scopes:
                if scope not in valid_scopes:
                    errors.append(f"Invalid scope: {scope}")

    # Validate pricing tiers if present
    if "pricing_tiers" in manifest:
        tiers = manifest["pricing_tiers"]
        if not isinstance(tiers, list):
            errors.append("pricing_tiers must be a list")
        else:
            for idx, tier in enumerate(tiers):
                if not isinstance(tier, dict):
                    errors.append(f"pricing_tiers[{idx}] must be a dict")
                    continue
                tier_required = ["tier", "name", "price_per_employee_per_month", "features"]
                for field in tier_required:
                    if field not in tier:
                        errors.append(f"pricing_tiers[{idx}] missing field: {field}")

    # Validate screenshot URLs (should not have {BASE_URL} placeholders)
    if "screenshot_urls" in manifest:
        for url in manifest["screenshot_urls"]:
            if "{BASE_URL}" in url:
                errors.append(f"Screenshot URL contains unsubstituted placeholder: {url}")

    return errors
