"""
Unified POS Aggregator for RosterIQ
====================================

Aggregates signals from multiple POS providers (SwiftPOS, Lightspeed, Square)
into a single unified interface consumed by the RosterIQ variance engine.

This module:
- Manages multiple POS provider instances with independent configuration
- Fetches signals from all enabled POS systems in parallel
- Deduplicates and weights signals across venues with multiple systems
- Provides health checks and fallback resilience
- Uses lazy imports to avoid hard dependencies on individual adapter modules

Usage:
    configs = [
        POSConfig(
            provider=POSProvider.SWIFTPOS,
            credentials={"api_url": "...", "clerk_id": "...", ...},
            location_id="LOC001",
            location_name="The Royal Oak",
        ),
        POSConfig(
            provider=POSProvider.SQUARE,
            credentials={"access_token": "..."},
            location_id="LOC001",
            location_name="The Royal Oak",
        ),
    ]

    aggregator = create_pos_aggregator(configs)
    await aggregator.initialise()
    signals = await aggregator.fetch_all_signals()
    demand_value, demand_confidence = await aggregator.get_combined_demand_signal()
    health = await aggregator.get_venue_health()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger("rosteriq.data_feeds.pos_aggregator")


# ---------------------------------------------------------------------------
# Enums & Constants
# ---------------------------------------------------------------------------

class POSProvider(Enum):
    """Supported POS system providers."""
    SWIFTPOS = "swiftpos"
    LIGHTSPEED = "lightspeed"
    SQUARE = "square"


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class POSConfig:
    """Configuration for a single POS system connection."""
    provider: POSProvider
    credentials: dict[str, Any]
    location_id: str
    location_name: str
    enabled: bool = True
    fetch_interval_minutes: int = 15

    def __post_init__(self):
        """Validate configuration."""
        if not self.location_id:
            raise ValueError("location_id is required")
        if not self.location_name:
            raise ValueError("location_name is required")
        if not isinstance(self.credentials, dict):
            raise ValueError("credentials must be a dictionary")
        if self.fetch_interval_minutes < 1:
            raise ValueError("fetch_interval_minutes must be >= 1")


@dataclass
class POSSignal:
    """
    Unified signal format from any POS provider.

    Signals represent demand indicators extracted from POS data, normalized
    to a standard format for the RosterIQ variance engine.
    """
    provider: POSProvider
    signal_type: str
    value: float  # -1.0 (very quiet) to 1.0 (surge)
    confidence: float  # 0.0 (very uncertain) to 1.0 (very certain)
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate signal values."""
        if not -1.0 <= self.value <= 1.0:
            raise ValueError(f"value must be in [-1.0, 1.0], got {self.value}")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0.0, 1.0], got {self.confidence}")


# ---------------------------------------------------------------------------
# POS Aggregator
# ---------------------------------------------------------------------------

class POSAggregator:
    """
    Central aggregator that manages multiple POS provider instances,
    fetches signals in parallel, and provides unified interface to
    the RosterIQ variance engine.

    Handles:
    - Parallel signal fetching from multiple POS systems
    - Signal deduplication and weighting for venues with multiple POS
    - Confidence-based weighted averaging
    - Health checks and fault resilience
    - Lazy loading of provider adapters (no hard dependencies)
    """

    def __init__(self, configs: list[POSConfig]):
        """
        Initialize aggregator with list of POS configurations.

        Args:
            configs: List of POSConfig objects, each defining a POS system.
                   Multiple configs can reference the same venue (location_id)
                   for cross-validation.

        Raises:
            ValueError: If configs list is empty or contains invalid config.
        """
        if not configs:
            raise ValueError("At least one POSConfig is required")

        self.configs = [c for c in configs if c.enabled]
        if not self.configs:
            raise ValueError("At least one enabled POSConfig is required")

        self._adapters: dict[str, Any] = {}  # provider → adapter instance
        self._adapter_configs: dict[str, list[POSConfig]] = {}  # provider → list of configs
        self._location_adapters: dict[str, list[str]] = {}  # location_id → list of adapter keys

        # Group configs by provider and location
        for config in self.configs:
            provider_key = config.provider.value
            if provider_key not in self._adapter_configs:
                self._adapter_configs[provider_key] = []
            self._adapter_configs[provider_key].append(config)

            if config.location_id not in self._location_adapters:
                self._location_adapters[config.location_id] = []
            self._location_adapters[config.location_id].append(provider_key)

    async def initialise(self) -> dict[str, bool]:
        """
        Initialize all POS adapter instances and build historical data.

        This should be called once on startup, then periodically (e.g., daily)
        to refresh trading patterns and historical context.

        Returns:
            dict mapping provider → success status. If a provider fails to
            initialise, it will still be available but may have degraded
            signal quality until re-initialized.
        """
        results = {}

        for provider_key in self._adapter_configs.keys():
            try:
                adapter = await self._get_or_create_adapter(provider_key)
                if hasattr(adapter, "initialise"):
                    await adapter.initialise()
                results[provider_key] = True
                logger.info(f"Initialised {provider_key} adapter")
            except Exception as e:
                results[provider_key] = False
                logger.error(f"Failed to initialise {provider_key}: {e}")

        return results

    async def _get_or_create_adapter(self, provider_key: str) -> Any:
        """
        Lazily create and cache a provider adapter instance.

        Uses lazy imports to avoid hard dependencies on adapter modules.
        If a provider's dependencies aren't installed, adapter creation fails
        and the provider is marked unavailable.

        Args:
            provider_key: Provider enum value (e.g., 'swiftpos')

        Returns:
            Adapter instance

        Raises:
            ImportError: If provider module or dependencies not available
            RuntimeError: If provider not supported or no config found
        """
        if provider_key in self._adapters:
            return self._adapters[provider_key]

        configs = self._adapter_configs.get(provider_key)
        if not configs:
            raise RuntimeError(f"No configuration found for provider {provider_key}")

        # Lazy import based on provider
        if provider_key == "swiftpos":
            try:
                from rosteriq.data_feeds.swiftpos import (
                    SwiftPOSAdapter,
                    SwiftPOSCredentials,
                )
                config = configs[0]  # Use first config for this provider
                creds = SwiftPOSCredentials(**config.credentials)
                adapter = SwiftPOSAdapter(
                    credentials=creds,
                    location_id=config.location_id,
                    location_name=config.location_name,
                    fetch_interval_minutes=config.fetch_interval_minutes,
                )
                self._adapters[provider_key] = adapter
                return adapter
            except ImportError as e:
                logger.error(f"Failed to import SwiftPOS adapter: {e}")
                raise

        elif provider_key == "lightspeed":
            try:
                from rosteriq.data_feeds.lightspeed import (
                    LightspeedAdapter,
                    LightspeedCredentials,
                )
                config = configs[0]
                creds = LightspeedCredentials(**config.credentials)
                adapter = LightspeedAdapter(
                    credentials=creds,
                    location_id=config.location_id,
                    location_name=config.location_name,
                    fetch_interval_minutes=config.fetch_interval_minutes,
                )
                self._adapters[provider_key] = adapter
                return adapter
            except ImportError as e:
                logger.error(f"Failed to import Lightspeed adapter: {e}")
                raise

        elif provider_key == "square":
            try:
                from rosteriq.data_feeds.square import (
                    SquareAdapter,
                    SquareCredentials,
                )
                config = configs[0]
                creds = SquareCredentials(**config.credentials)
                adapter = SquareAdapter(
                    credentials=creds,
                    location_id=config.location_id,
                    location_name=config.location_name,
                    fetch_interval_minutes=config.fetch_interval_minutes,
                )
                self._adapters[provider_key] = adapter
                return adapter
            except ImportError as e:
                logger.error(f"Failed to import Square adapter: {e}")
                raise

        else:
            raise RuntimeError(f"Unsupported POS provider: {provider_key}")

    async def fetch_all_signals(self) -> list[POSSignal]:
        """
        Fetch signals from all enabled POS systems in parallel.

        This is the primary method for the variance engine to consume
        aggregated demand signals. Handles adapter failures gracefully—
        if one provider fails, others still return signals.

        Returns:
            List of POSSignal objects from all available providers.
            If all providers fail, returns empty list.

        Strategy:
            1. Fetch signals from all providers concurrently
            2. Convert raw signal format to unified POSSignal
            3. Deduplicate if same venue has multiple POS systems
            4. Log failures without stopping execution
        """
        tasks = []
        provider_keys = list(self._adapter_configs.keys())

        for provider_key in provider_keys:
            tasks.append(self._fetch_provider_signals(provider_key))

        # Execute all fetches in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_signals = []
        for provider_key, result in zip(provider_keys, results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch from {provider_key}: {result}")
                continue
            all_signals.extend(result)

        return all_signals

    async def _fetch_provider_signals(self, provider_key: str) -> list[POSSignal]:
        """
        Fetch and normalize signals from a single provider.

        Converts provider-specific signal format to unified POSSignal.

        Args:
            provider_key: Provider enum value

        Returns:
            List of POSSignal objects, empty list if fetch fails
        """
        try:
            adapter = await self._get_or_create_adapter(provider_key)
            provider_enum = POSProvider(provider_key)

            # Different adapters have different fetch method names
            if hasattr(adapter, "fetch_signals"):
                raw_signals = await adapter.fetch_signals()
            elif hasattr(adapter, "fetch_all_signals"):
                raw_signals = await adapter.fetch_all_signals()
            else:
                logger.warning(f"Adapter {provider_key} has no fetch method")
                return []

            if not raw_signals:
                return []

            unified_signals = []
            for raw in raw_signals:
                try:
                    signal = POSSignal(
                        provider=provider_enum,
                        signal_type=raw.get("signal_type", "foot_traffic"),
                        value=float(raw.get("value", 0.0)),
                        confidence=float(raw.get("confidence", 0.5)),
                        timestamp=self._parse_timestamp(raw.get("timestamp")),
                        metadata=raw.get("metadata", {}),
                    )
                    unified_signals.append(signal)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Failed to normalize signal from {provider_key}: {e}")
                    continue

            return unified_signals

        except Exception as e:
            logger.error(f"Error fetching from {provider_key}: {e}")
            return []

    def _parse_timestamp(self, ts: Any) -> datetime:
        """
        Parse timestamp from various formats.

        Handles ISO strings, datetime objects, and defaults to now.
        """
        if isinstance(ts, datetime):
            # Ensure UTC
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts

        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                pass

        logger.warning(f"Could not parse timestamp: {ts}, using now()")
        return datetime.now(timezone.utc)

    async def get_combined_demand_signal(self) -> tuple[float, float]:
        """
        Get weighted average demand signal across all POS systems.

        For venues with multiple POS systems, intelligently combines signals:
        - Weights by confidence score
        - Higher confidence POS systems have more influence
        - Cross-validates (e.g., SwiftPOS sales + Square payments)

        Returns:
            tuple of (value, confidence) where:
            - value: float in [-1.0, 1.0] representing demand
            - confidence: float in [0.0, 1.0] representing certainty

        Strategy:
            1. Fetch all signals
            2. Filter to foot_traffic / demand-related signals
            3. Group by venue (location_id)
            4. If single venue/provider: return its signal
            5. If multiple: weighted average by confidence
            6. Boost confidence if multiple sources agree on direction
        """
        signals = await self.fetch_all_signals()

        # Filter to demand signals
        demand_signals = [
            s for s in signals
            if "demand" in s.signal_type.lower() or "traffic" in s.signal_type.lower()
        ]

        if not demand_signals:
            logger.warning("No demand signals available")
            return 0.0, 0.0

        # Group by venue
        by_venue = {}
        for sig in demand_signals:
            venue = sig.metadata.get("location", "unknown")
            if venue not in by_venue:
                by_venue[venue] = []
            by_venue[venue].append(sig)

        # Combine signals per venue
        combined_values = []
        combined_confidences = []

        for venue, venue_signals in by_venue.items():
            if len(venue_signals) == 1:
                # Single source: use directly
                sig = venue_signals[0]
                combined_values.append(sig.value)
                combined_confidences.append(sig.confidence)
            else:
                # Multiple sources: weighted average
                total_weight = sum(s.confidence for s in venue_signals)
                if total_weight == 0:
                    continue

                weighted_value = sum(
                    s.value * s.confidence for s in venue_signals
                ) / total_weight
                combined_values.append(weighted_value)

                # Boost confidence if multiple sources agree
                agreement = 1.0 - (max(abs(s.value) for s in venue_signals) -
                                   min(abs(s.value) for s in venue_signals))
                base_confidence = total_weight / len(venue_signals)
                boosted_confidence = base_confidence * (0.9 + 0.1 * agreement)
                combined_confidences.append(min(boosted_confidence, 1.0))

        if not combined_values:
            return 0.0, 0.0

        # Final average across all venues
        avg_value = sum(combined_values) / len(combined_values)
        avg_confidence = sum(combined_confidences) / len(combined_confidences)

        return round(avg_value, 2), round(avg_confidence, 2)

    async def get_venue_health(self) -> dict:
        """
        Get health status across all connected POS systems.

        Returns:
            dict with structure:
            {
                "overall_status": "healthy" | "degraded" | "error",
                "total_providers": int,
                "healthy_providers": int,
                "providers": {
                    "swiftpos": {"status": "healthy", "locations": 1, ...},
                    "square": {"status": "error", "error": "..."},
                    ...
                },
            }

        A system is "healthy" if at least one provider is connected.
        A system is "degraded" if some but not all providers are down.
        A system is "error" if all providers are down.
        """
        provider_health = {}
        healthy_count = 0

        for provider_key in self._adapter_configs.keys():
            try:
                adapter = await self._get_or_create_adapter(provider_key)
                if hasattr(adapter, "health_check"):
                    health = await adapter.health_check()
                    provider_health[provider_key] = health
                    if health.get("status") == "healthy" or health.get("connected"):
                        healthy_count += 1
                else:
                    provider_health[provider_key] = {"status": "unknown"}
            except Exception as e:
                provider_health[provider_key] = {
                    "status": "error",
                    "error": str(e),
                }

        total = len(self._adapter_configs)
        if healthy_count == total:
            overall = "healthy"
        elif healthy_count > 0:
            overall = "degraded"
        else:
            overall = "error"

        return {
            "overall_status": overall,
            "total_providers": total,
            "healthy_providers": healthy_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "providers": provider_health,
        }

    async def close(self):
        """Close all adapter connections gracefully."""
        for adapter in self._adapters.values():
            if hasattr(adapter, "close"):
                try:
                    await adapter.close()
                except Exception as e:
                    logger.warning(f"Error closing adapter: {e}")


# ---------------------------------------------------------------------------
# Factory Function
# ---------------------------------------------------------------------------

def create_pos_aggregator(configs: list[POSConfig]) -> POSAggregator:
    """
    Factory function to create a POS aggregator from configuration list.

    Validates all configs and creates the aggregator instance.

    Args:
        configs: List of POSConfig objects describing POS systems.

    Returns:
        POSAggregator instance ready to use.

    Raises:
        ValueError: If configs is empty, contains invalid config, or
                   no enabled configs are found.

    Example:
        configs = [
            POSConfig(
                provider=POSProvider.SWIFTPOS,
                credentials={
                    "api_url": "https://api.swiftpos.com.au/v1",
                    "clerk_id": "CLERK001",
                    "client_id": "CLIENT001",
                    "customer_id": "CUST001",
                },
                location_id="LOC001",
                location_name="The Royal Oak",
            ),
            POSConfig(
                provider=POSProvider.SQUARE,
                credentials={"access_token": "sq_live_..."},
                location_id="LOC001",
                location_name="The Royal Oak",
            ),
        ]

        aggregator = create_pos_aggregator(configs)
        await aggregator.initialise()
        signals = await aggregator.fetch_all_signals()
    """
    if not configs:
        raise ValueError("At least one POSConfig required")

    for config in configs:
        if not isinstance(config, POSConfig):
            raise ValueError(f"All items must be POSConfig, got {type(config)}")

    return POSAggregator(configs)
