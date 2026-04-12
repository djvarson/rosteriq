"""
Unit Tests for POS Aggregator
==============================

Comprehensive test coverage for the unified POS aggregator module.

Tests cover:
- POSConfig creation and validation
- POSSignal creation and validation
- Single vs. multi-provider scenarios
- Signal weighting and combination logic
- Confidence-based averaging
- Health checks
- Fallback behavior on provider failure
- Factory function validation
"""

from __future__ import annotations

import sys
from pathlib import Path
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)) if str(ROOT) not in sys.path else None

from rosteriq.data_feeds.pos_aggregator import (
    POSAggregator,
    POSConfig,
    POSProvider,
    POSSignal,
    create_pos_aggregator,
)


# ---------------------------------------------------------------------------
# Test Fixtures as Helper Functions
# ---------------------------------------------------------------------------

def _swiftpos_config():
    """Valid SwiftPOS configuration."""
    return POSConfig(
        provider=POSProvider.SWIFTPOS,
        credentials={
            "api_url": "https://api.swiftpos.com.au/v1",
            "clerk_id": "CLERK001",
            "client_id": "CLIENT001",
            "customer_id": "CUST001",
        },
        location_id="LOC001",
        location_name="The Royal Oak",
    )


def _square_config():
    """Valid Square configuration."""
    return POSConfig(
        provider=POSProvider.SQUARE,
        credentials={"access_token": "sq_live_abc123xyz"},
        location_id="LOC001",
        location_name="The Royal Oak",
    )


def _lightspeed_config():
    """Valid Lightspeed configuration."""
    return POSConfig(
        provider=POSProvider.LIGHTSPEED,
        credentials={"api_key": "ls_key_123", "location_id": "LOC001"},
        location_id="LOC001",
        location_name="The Royal Oak",
    )


def _sample_signal():
    """Sample POSSignal for testing."""
    return POSSignal(
        provider=POSProvider.SWIFTPOS,
        signal_type="foot_traffic",
        value=0.25,
        confidence=0.8,
        timestamp=datetime.now(timezone.utc),
        metadata={"location": "The Royal Oak", "revenue": 1500.0},
    )


# ---------------------------------------------------------------------------
# Test POSConfig
# ---------------------------------------------------------------------------

class TestPOSConfig:
    """Tests for POSConfig dataclass."""

    def test_create_valid_config(self):
        """POSConfig creation with valid parameters."""
        swiftpos_config = _swiftpos_config()
        assert swiftpos_config.provider == POSProvider.SWIFTPOS
        assert swiftpos_config.location_id == "LOC001"
        assert swiftpos_config.location_name == "The Royal Oak"
        assert swiftpos_config.enabled is True
        assert swiftpos_config.fetch_interval_minutes == 15

    def test_config_custom_interval(self):
        """POSConfig with custom fetch interval."""
        swiftpos_config = _swiftpos_config()
        config = POSConfig(
            provider=POSProvider.SWIFTPOS,
            credentials=swiftpos_config.credentials,
            location_id="LOC001",
            location_name="The Royal Oak",
            fetch_interval_minutes=5,
        )
        assert config.fetch_interval_minutes == 5

    def test_config_disabled(self):
        """POSConfig can be disabled."""
        swiftpos_config = _swiftpos_config()
        config = POSConfig(
            provider=POSProvider.SWIFTPOS,
            credentials=swiftpos_config.credentials,
            location_id="LOC001",
            location_name="The Royal Oak",
            enabled=False,
        )
        assert config.enabled is False

    def test_config_missing_location_id(self):
        """POSConfig requires location_id."""
        try:
            POSConfig(
                provider=POSProvider.SWIFTPOS,
                credentials={},
                location_id="",
                location_name="Venue",
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "location_id" in str(e)

    def test_config_missing_location_name(self):
        """POSConfig requires location_name."""
        try:
            POSConfig(
                provider=POSProvider.SWIFTPOS,
                credentials={},
                location_id="LOC001",
                location_name="",
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "location_name" in str(e)

    def test_config_invalid_credentials(self):
        """POSConfig requires credentials to be dict."""
        try:
            POSConfig(
                provider=POSProvider.SWIFTPOS,
                credentials="not_a_dict",
                location_id="LOC001",
                location_name="Venue",
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "credentials must be a dictionary" in str(e)

    def test_config_invalid_interval(self):
        """POSConfig requires positive fetch_interval_minutes."""
        swiftpos_config = _swiftpos_config()
        try:
            POSConfig(
                provider=POSProvider.SWIFTPOS,
                credentials=swiftpos_config.credentials,
                location_id="LOC001",
                location_name="Venue",
                fetch_interval_minutes=0,
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "fetch_interval_minutes" in str(e)


# ---------------------------------------------------------------------------
# Test POSSignal
# ---------------------------------------------------------------------------

class TestPOSSignal:
    """Tests for POSSignal dataclass."""

    def test_create_valid_signal(self):
        """POSSignal creation with valid parameters."""
        sample_signal = _sample_signal()
        assert sample_signal.provider == POSProvider.SWIFTPOS
        assert sample_signal.signal_type == "foot_traffic"
        assert sample_signal.value == 0.25
        assert sample_signal.confidence == 0.8

    def test_signal_value_boundaries(self):
        """POSSignal value must be in [-1.0, 1.0]."""
        try:
            POSSignal(
                provider=POSProvider.SWIFTPOS,
                signal_type="test",
                value=1.5,  # Too high
                confidence=0.5,
                timestamp=datetime.now(timezone.utc),
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "value must be" in str(e)

    def test_signal_confidence_boundaries(self):
        """POSSignal confidence must be in [0.0, 1.0]."""
        try:
            POSSignal(
                provider=POSProvider.SWIFTPOS,
                signal_type="test",
                value=0.5,
                confidence=1.5,  # Too high
                timestamp=datetime.now(timezone.utc),
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "confidence must be" in str(e)

    def test_signal_negative_value(self):
        """POSSignal accepts negative values (quiet)."""
        signal = POSSignal(
            provider=POSProvider.SWIFTPOS,
            signal_type="foot_traffic",
            value=-0.5,
            confidence=0.7,
            timestamp=datetime.now(timezone.utc),
        )
        assert signal.value == -0.5

    def test_signal_zero_confidence(self):
        """POSSignal accepts zero confidence."""
        signal = POSSignal(
            provider=POSProvider.SWIFTPOS,
            signal_type="test",
            value=0.0,
            confidence=0.0,
            timestamp=datetime.now(timezone.utc),
        )
        assert signal.confidence == 0.0


# ---------------------------------------------------------------------------
# Test POSAggregator - Configuration & Creation
# ---------------------------------------------------------------------------

class TestPOSAggregatorCreation:
    """Tests for POSAggregator initialization and factory."""

    def test_aggregator_requires_configs(self):
        """POSAggregator requires at least one config."""
        try:
            POSAggregator([])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "At least one POSConfig" in str(e)

    def test_aggregator_requires_enabled_configs(self):
        """POSAggregator requires at least one enabled config."""
        swiftpos_config = _swiftpos_config()
        swiftpos_config.enabled = False
        try:
            POSAggregator([swiftpos_config])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "at least one enabled" in str(e).lower()

    def test_aggregator_single_config(self):
        """POSAggregator with single config."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])
        assert len(agg.configs) == 1
        assert agg.configs[0].provider == POSProvider.SWIFTPOS

    def test_aggregator_multiple_configs(self):
        """POSAggregator with multiple configs for same venue."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])
        assert len(agg.configs) == 2

    def test_aggregator_filters_disabled(self):
        """POSAggregator filters out disabled configs."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        square_config.enabled = False
        agg = POSAggregator([swiftpos_config, square_config])
        assert len(agg.configs) == 1
        assert agg.configs[0].provider == POSProvider.SWIFTPOS

    def test_factory_function_valid(self):
        """create_pos_aggregator factory with valid config."""
        swiftpos_config = _swiftpos_config()
        agg = create_pos_aggregator([swiftpos_config])
        assert isinstance(agg, POSAggregator)
        assert len(agg.configs) == 1

    def test_factory_function_empty_list(self):
        """create_pos_aggregator rejects empty list."""
        try:
            create_pos_aggregator([])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "At least one" in str(e)

    def test_factory_function_invalid_type(self):
        """create_pos_aggregator validates config types."""
        try:
            create_pos_aggregator(["not_a_config"])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "must be POSConfig" in str(e)


# ---------------------------------------------------------------------------
# Test POSAggregator - Signal Fetching
# ---------------------------------------------------------------------------

class TestPOSAggregatorFetching:
    """Tests for signal fetching and normalization."""

    async def test_fetch_all_signals_empty(self):
        """fetch_all_signals returns empty list when no signals available."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])

        # Mock adapter with no signals
        mock_adapter = AsyncMock()
        mock_adapter.fetch_signals = AsyncMock(return_value=[])
        agg._adapters["swiftpos"] = mock_adapter

        signals = await agg.fetch_all_signals()
        assert signals == []

    async def test_fetch_all_signals_single_provider(self):
        """fetch_all_signals from single provider."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])

        now = datetime.now(timezone.utc).isoformat()
        mock_adapter = AsyncMock()
        mock_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.3,
                    "confidence": 0.8,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["swiftpos"] = mock_adapter

        signals = await agg.fetch_all_signals()
        assert len(signals) == 1
        assert signals[0].provider == POSProvider.SWIFTPOS
        assert signals[0].value == 0.3
        assert signals[0].confidence == 0.8

    async def test_fetch_all_signals_multiple_providers(self):
        """fetch_all_signals from multiple providers in parallel."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        now = datetime.now(timezone.utc).isoformat()

        # Mock both adapters
        swift_adapter = AsyncMock()
        swift_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.2,
                    "confidence": 0.75,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.25,
                    "confidence": 0.85,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["square"] = square_adapter

        signals = await agg.fetch_all_signals()
        assert len(signals) == 2
        providers = {s.provider for s in signals}
        assert POSProvider.SWIFTPOS in providers
        assert POSProvider.SQUARE in providers

    async def test_fetch_all_signals_provider_failure(self):
        """fetch_all_signals continues when one provider fails."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        now = datetime.now(timezone.utc).isoformat()

        # SwiftPOS fails
        swift_adapter = AsyncMock()
        swift_adapter.fetch_signals = AsyncMock(side_effect=Exception("Connection error"))
        agg._adapters["swiftpos"] = swift_adapter

        # Square succeeds
        square_adapter = AsyncMock()
        square_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.3,
                    "confidence": 0.8,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["square"] = square_adapter

        signals = await agg.fetch_all_signals()
        assert len(signals) == 1
        assert signals[0].provider == POSProvider.SQUARE


# ---------------------------------------------------------------------------
# Test POSAggregator - Signal Combination & Weighting
# ---------------------------------------------------------------------------

class TestPOSAggregatorCombination:
    """Tests for signal combination and weighted averaging."""

    async def test_combined_demand_signal_no_signals(self):
        """get_combined_demand_signal returns zero when no signals available."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])

        mock_adapter = AsyncMock()
        mock_adapter.fetch_signals = AsyncMock(return_value=[])
        agg._adapters["swiftpos"] = mock_adapter

        value, confidence = await agg.get_combined_demand_signal()
        assert value == 0.0
        assert confidence == 0.0

    async def test_combined_demand_signal_single_source(self):
        """get_combined_demand_signal with single provider."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])

        now = datetime.now(timezone.utc).isoformat()
        mock_adapter = AsyncMock()
        mock_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.3,
                    "confidence": 0.8,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["swiftpos"] = mock_adapter

        value, confidence = await agg.get_combined_demand_signal()
        assert value == 0.3
        assert confidence == 0.8

    async def test_combined_demand_signal_weighted_averaging(self):
        """get_combined_demand_signal with weighted averaging."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        now = datetime.now(timezone.utc).isoformat()

        # SwiftPOS: high confidence
        swift_adapter = AsyncMock()
        swift_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.4,
                    "confidence": 0.9,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["swiftpos"] = swift_adapter

        # Square: lower confidence
        square_adapter = AsyncMock()
        square_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.2,
                    "confidence": 0.5,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["square"] = square_adapter

        value, confidence = await agg.get_combined_demand_signal()

        # Weighted average: (0.4 * 0.9 + 0.2 * 0.5) / (0.9 + 0.5)
        # = (0.36 + 0.1) / 1.4 = 0.46 / 1.4 ≈ 0.33
        assert 0.32 < value < 0.34
        # Confidence should be boosted when sources agree
        assert 0.5 <= confidence <= 1.0

    async def test_combined_demand_signal_opposing_signals(self):
        """get_combined_demand_signal when providers disagree."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        now = datetime.now(timezone.utc).isoformat()

        # SwiftPOS: high confidence, positive signal
        swift_adapter = AsyncMock()
        swift_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": 0.6,
                    "confidence": 0.9,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["swiftpos"] = swift_adapter

        # Square: high confidence, opposite signal
        square_adapter = AsyncMock()
        square_adapter.fetch_signals = AsyncMock(
            return_value=[
                {
                    "signal_type": "foot_traffic",
                    "value": -0.4,
                    "confidence": 0.85,
                    "timestamp": now,
                    "metadata": {"location": "The Royal Oak"},
                }
            ]
        )
        agg._adapters["square"] = square_adapter

        value, confidence = await agg.get_combined_demand_signal()

        # Weighted average with disagreement
        # (0.6 * 0.9 + -0.4 * 0.85) / (0.9 + 0.85)
        # = (0.54 - 0.34) / 1.75 = 0.2 / 1.75 ≈ 0.11
        assert 0.10 < value < 0.12
        # Confidence should be reduced when sources disagree
        assert confidence < 0.90


# ---------------------------------------------------------------------------
# Test POSAggregator - Health Checks
# ---------------------------------------------------------------------------

class TestPOSAggregatorHealth:
    """Tests for health check functionality."""

    async def test_health_check_all_healthy(self):
        """get_venue_health when all providers healthy."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.health_check = AsyncMock(
            return_value={"status": "healthy", "connected": True}
        )
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.health_check = AsyncMock(
            return_value={"status": "healthy", "connected": True}
        )
        agg._adapters["square"] = square_adapter

        health = await agg.get_venue_health()
        assert health["overall_status"] == "healthy"
        assert health["healthy_providers"] == 2
        assert health["total_providers"] == 2

    async def test_health_check_degraded(self):
        """get_venue_health when some providers unhealthy."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.health_check = AsyncMock(
            return_value={"status": "healthy", "connected": True}
        )
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.health_check = AsyncMock(
            return_value={"status": "error", "connected": False}
        )
        agg._adapters["square"] = square_adapter

        health = await agg.get_venue_health()
        assert health["overall_status"] == "degraded"
        assert health["healthy_providers"] == 1
        assert health["total_providers"] == 2

    async def test_health_check_all_error(self):
        """get_venue_health when all providers down."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.health_check = AsyncMock(
            return_value={"status": "error", "connected": False}
        )
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.health_check = AsyncMock(
            return_value={"status": "error", "connected": False}
        )
        agg._adapters["square"] = square_adapter

        health = await agg.get_venue_health()
        assert health["overall_status"] == "error"
        assert health["healthy_providers"] == 0

    async def test_health_check_missing_method(self):
        """get_venue_health handles adapters without health_check."""
        swiftpos_config = _swiftpos_config()
        agg = POSAggregator([swiftpos_config])

        mock_adapter = AsyncMock(spec=[])  # No health_check method
        agg._adapters["swiftpos"] = mock_adapter

        health = await agg.get_venue_health()
        assert "swiftpos" in health["providers"]
        assert health["providers"]["swiftpos"]["status"] == "unknown"


# ---------------------------------------------------------------------------
# Test POSAggregator - Initialization
# ---------------------------------------------------------------------------

class TestPOSAggregatorInitialisation:
    """Tests for aggregator initialization."""

    async def test_initialise_all_success(self):
        """initialise succeeds when all adapters initialize."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.initialise = AsyncMock()
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.initialise = AsyncMock()
        agg._adapters["square"] = square_adapter

        results = await agg.initialise()
        assert results["swiftpos"] is True
        assert results["square"] is True

    async def test_initialise_partial_failure(self):
        """initialise continues when one adapter fails."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.initialise = AsyncMock(side_effect=Exception("Init failed"))
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.initialise = AsyncMock()
        agg._adapters["square"] = square_adapter

        results = await agg.initialise()
        assert results["swiftpos"] is False
        assert results["square"] is True


# ---------------------------------------------------------------------------
# Test POSAggregator - Cleanup
# ---------------------------------------------------------------------------

class TestPOSAggregatorCleanup:
    """Tests for resource cleanup."""

    async def test_close_all_adapters(self):
        """close calls cleanup on all adapters."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.close = AsyncMock()
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.close = AsyncMock()
        agg._adapters["square"] = square_adapter

        await agg.close()

        swift_adapter.close.assert_called_once()
        square_adapter.close.assert_called_once()

    async def test_close_handles_failures(self):
        """close handles failures gracefully."""
        swiftpos_config = _swiftpos_config()
        square_config = _square_config()
        agg = POSAggregator([swiftpos_config, square_config])

        swift_adapter = AsyncMock()
        swift_adapter.close = AsyncMock(side_effect=Exception("Close failed"))
        agg._adapters["swiftpos"] = swift_adapter

        square_adapter = AsyncMock()
        square_adapter.close = AsyncMock()
        agg._adapters["square"] = square_adapter

        # Should not raise
        await agg.close()

        square_adapter.close.assert_called_once()


if __name__ == "__main__":
    import asyncio as _asyncio
    passed = failed = 0
    for name, obj in list(globals().items()):
        if isinstance(obj, type) and name.startswith("Test"):
            inst = obj()
            for mname in sorted(dir(inst)):
                if mname.startswith("test_"):
                    try:
                        result = getattr(inst, mname)()
                        if _asyncio.iscoroutine(result):
                            _asyncio.run(result)
                        passed += 1
                        print(f"  PASS {name}.{mname}")
                    except AssertionError as e:
                        failed += 1
                        print(f"  FAIL {name}.{mname}: {e}")
                    except Exception as e:
                        failed += 1
                        print(f"  ERROR {name}.{mname}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
