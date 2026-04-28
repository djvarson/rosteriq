"""
Signal Aggregator for RosterIQ data feeds.

Central aggregator that collects signals from ALL data feed adapters,
normalises them, deduplicates, weights them dynamically, and converts
them into VarianceSignal objects for the variance engine.

The aggregator:
1. Registers and groups adapters by category
2. Fetches signals in parallel from all adapters
3. Deduplicates and merges signals for the same date+hour+category
4. Dynamically adjusts weights based on signal availability
5. Converts FeedSignal objects to VarianceSignal objects
6. Provides summary dashboard of active signals
7. Supports async context manager for resource cleanup
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Optional, Any

from rosteriq.data_feeds.base import (
    FeedSignal,
    DataFeedAdapter,
    FeedCategory,
    Location,
    SignalStrength,
    SignalCache,
)
from rosteriq.models import VarianceSignal, SignalType, VenueConfig

logger = logging.getLogger(__name__)


# ============================================================================
# Category-to-SignalType mapping
# ============================================================================

CATEGORY_TO_SIGNAL_TYPE: dict[FeedCategory, SignalType] = {
    FeedCategory.weather: SignalType.weather,
    FeedCategory.events: SignalType.events,
    FeedCategory.sports: SignalType.events,
    FeedCategory.school_holidays: SignalType.events,
    FeedCategory.calendar: SignalType.events,
    FeedCategory.reservations: SignalType.bookings,
    FeedCategory.foot_traffic: SignalType.pos_trends,
    FeedCategory.nearby_venues: SignalType.pos_trends,
    FeedCategory.delivery: SignalType.pos_trends,
    FeedCategory.tourism: SignalType.historical,
    FeedCategory.transport: SignalType.historical,
    FeedCategory.economic: SignalType.historical,
}


# ============================================================================
# Signal Aggregator class
# ============================================================================


class SignalAggregator:
    """
    Central aggregator for all RosterIQ data feed signals.

    Collects signals from multiple adapters, normalises them, deduplicates,
    applies dynamic weighting, and converts to VarianceSignal objects for
    the variance engine.

    Supports async context manager for resource cleanup:
        async with SignalAggregator(...) as agg:
            signals = await agg.get_variance_signals(...)
    """

    def __init__(
        self,
        default_weights: Optional[dict[SignalType, float]] = None,
        cache: Optional[SignalCache] = None,
    ):
        """
        Initialize the Signal Aggregator.

        Args:
            default_weights: Dictionary mapping SignalType to default weights.
                If not provided, uses the weights from variance_engine.DEFAULT_WEIGHTS.
                Must sum to 1.0.
            cache: Optional SignalCache for deduplication. If not provided,
                creates a new cache with 30-minute TTL.
        """
        self._adapters: dict[FeedCategory, list[DataFeedAdapter]] = defaultdict(list)
        self._all_adapters: list[DataFeedAdapter] = []
        self.cache = cache or SignalCache(default_ttl_minutes=30)

        # Import here to avoid circular imports
        from rosteriq.variance_engine import DEFAULT_WEIGHTS as DEFAULT_ENGINE_WEIGHTS

        self.default_weights = default_weights or DEFAULT_ENGINE_WEIGHTS
        self._validate_weights(self.default_weights)

        # Track available signal types for dynamic weighting
        self._available_signal_types: set[SignalType] = set()
        self._signal_confidences: dict[SignalType, list[float]] = defaultdict(list)

    def _validate_weights(self, weights: dict[SignalType, float]) -> None:
        """Validate that weights sum to 1.0 (with small tolerance for float precision)."""
        total = sum(weights.values())
        if not (0.99 <= total <= 1.01):
            logger.warning(
                f"Weights sum to {total:.4f}, expected ~1.0. "
                f"Weights will be normalised."
            )

    def register(self, adapter: DataFeedAdapter) -> None:
        """
        Register a single data feed adapter.

        Args:
            adapter: DataFeedAdapter instance to register.

        Raises:
            ValueError: If adapter category is not in CATEGORY_TO_SIGNAL_TYPE mapping.
        """
        if adapter.category not in CATEGORY_TO_SIGNAL_TYPE:
            raise ValueError(
                f"Adapter category {adapter.category} not in supported categories. "
                f"Supported: {list(CATEGORY_TO_SIGNAL_TYPE.keys())}"
            )

        self._adapters[adapter.category].append(adapter)
        self._all_adapters.append(adapter)
        logger.info(
            f"Registered adapter {adapter.source_name} "
            f"for category {adapter.category}"
        )

    def register_all(self, adapters: list[DataFeedAdapter]) -> None:
        """
        Register multiple data feed adapters at once.

        Args:
            adapters: List of DataFeedAdapter instances to register.
        """
        for adapter in adapters:
            self.register(adapter)

    async def fetch_all(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch signals from all registered adapters in parallel.

        Failures in individual adapters do not block others.
        Expired signals are filtered out automatically.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects, deduplicated and merged where applicable.

        Logs:
            - Adapter failures (one adapter failing doesn't block others)
            - Number of expired signals filtered
        """
        if not self._all_adapters:
            logger.warning("No adapters registered. Returning empty signal list.")
            return []

        # Fetch from all adapters in parallel
        tasks = [
            self._safe_fetch(adapter, location, start_date, end_date, venue_id)
            for adapter in self._all_adapters
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect all signals, handling exceptions
        all_signals: list[FeedSignal] = []
        for adapter, result in zip(self._all_adapters, results):
            if isinstance(result, Exception):
                logger.error(
                    f"Adapter {adapter.source_name} failed: {result}",
                    exc_info=result,
                )
            else:
                all_signals.extend(result)

        # Filter expired signals
        initial_count = len(all_signals)
        all_signals = [s for s in all_signals if not s.is_expired]
        expired_count = initial_count - len(all_signals)

        if expired_count > 0:
            logger.info(f"Filtered {expired_count} expired signals")

        # Deduplicate and merge
        deduplicated = self._deduplicate_and_merge(all_signals)

        logger.info(
            f"Aggregated signals: {initial_count} fetched, {expired_count} expired, "
            f"{len(deduplicated)} unique after deduplication"
        )

        return deduplicated

    async def _safe_fetch(
        self,
        adapter: DataFeedAdapter,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str],
    ) -> list[FeedSignal]:
        """Safely fetch from an adapter, catching exceptions."""
        try:
            return await adapter.fetch_signals(location, start_date, end_date, venue_id)
        except Exception as e:
            logger.error(
                f"Error fetching from {adapter.source_name}: {e}",
                exc_info=e,
            )
            return []

    def _deduplicate_and_merge(self, signals: list[FeedSignal]) -> list[FeedSignal]:
        """
        Deduplicate signals for the same date+hour+category.

        When multiple adapters return signals for the same date+hour+category:
        - Average their values
        - Take the highest confidence
        - Merge descriptions
        - Use the most recent fetched_at timestamp

        Args:
            signals: List of FeedSignal objects to deduplicate.

        Returns:
            List of deduplicated FeedSignal objects.
        """
        # Group by (date, hour, category)
        groups: dict[tuple, list[FeedSignal]] = defaultdict(list)
        for signal in signals:
            key = (signal.signal_date, signal.signal_hour, signal.category)
            groups[key].append(signal)

        merged = []
        for (sig_date, sig_hour, category), group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Multiple signals for same date+hour+category: merge them
                merged_signal = self._merge_signal_group(group)
                merged.append(merged_signal)

        return merged

    def _merge_signal_group(self, signals: list[FeedSignal]) -> FeedSignal:
        """
        Merge a group of signals for the same date+hour+category.

        Args:
            signals: List of FeedSignal objects to merge (all same date+hour+category).

        Returns:
            Single merged FeedSignal with averaged values and highest confidence.
        """
        # Average values
        avg_value = sum(s.value for s in signals) / len(signals)

        # Take highest confidence
        max_confidence = max(s.confidence for s in signals)

        # Combine descriptions
        descriptions = [s.description for s in signals if s.description]
        merged_description = " | ".join(descriptions)

        # Most recent fetch time
        latest_fetched = max(signals, key=lambda s: s.fetched_at)

        # Merge raw data
        merged_raw = {}
        for signal in signals:
            merged_raw.update(signal.raw_data)

        # Use first signal as template
        template = signals[0]

        # Determine strength based on averaged value
        strength = self._value_to_strength(avg_value)

        merged = FeedSignal(
            category=template.category,
            source=f"merged_{len(signals)}_adapters",
            signal_date=template.signal_date,
            signal_hour=template.signal_hour,
            strength=strength,
            value=avg_value,
            confidence=max_confidence,
            description=merged_description or "Merged signal from multiple adapters",
            raw_data=merged_raw,
            fetched_at=latest_fetched.fetched_at,
            expires_at=latest_fetched.expires_at,
            venue_id=template.venue_id,
        )

        return merged

    def _value_to_strength(self, value: float) -> SignalStrength:
        """
        Convert a normalised value (-1.0 to 1.0) to a SignalStrength enum.

        Args:
            value: Normalised value (-1.0 to 1.0).

        Returns:
            SignalStrength enum matching the value range.
        """
        if value <= -0.8:
            return SignalStrength.extreme_negative
        elif value <= -0.5:
            return SignalStrength.strong_negative
        elif value <= -0.2:
            return SignalStrength.moderate_negative
        elif value < 0:
            return SignalStrength.weak_negative
        elif value == 0:
            return SignalStrength.neutral
        elif value <= 0.2:
            return SignalStrength.weak_positive
        elif value <= 0.5:
            return SignalStrength.moderate_positive
        elif value <= 0.8:
            return SignalStrength.strong_positive
        else:
            return SignalStrength.extreme_positive

    async def get_variance_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[VarianceSignal]:
        """
        Fetch all signals and convert them to VarianceSignal objects.

        This is the main entry point for the variance engine. Signals are
        converted to VarianceSignal format with dynamically adjusted weights.

        Args:
            location: Venue geographic location.
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of VarianceSignal objects ready for the variance engine.
        """
        # Fetch all feed signals
        feed_signals = await self.fetch_all(location, start_date, end_date, venue_id)

        if not feed_signals:
            logger.info("No feed signals available, returning empty variance signals")
            return []

        # Track available signal types and confidences for dynamic weighting
        self._available_signal_types.clear()
        self._signal_confidences.clear()

        for signal in feed_signals:
            signal_type = CATEGORY_TO_SIGNAL_TYPE.get(
                signal.category, SignalType.historical
            )
            self._available_signal_types.add(signal_type)
            self._signal_confidences[signal_type].append(signal.confidence)

        # Compute dynamic weights
        adjusted_weights = self._compute_adjusted_weights()

        # Convert to VarianceSignal objects
        variance_signals: list[VarianceSignal] = []
        for feed_signal in feed_signals:
            signal_type = CATEGORY_TO_SIGNAL_TYPE.get(
                feed_signal.category, SignalType.historical
            )
            weight = adjusted_weights.get(signal_type, 0.0)

            variance_signal = VarianceSignal(
                signal_type=signal_type,
                value=feed_signal.to_variance_value(),
                weight=weight,
                confidence=feed_signal.confidence,
                source=feed_signal.source,
                timestamp=feed_signal.fetched_at,
            )
            variance_signals.append(variance_signal)

        logger.info(
            f"Converted {len(feed_signals)} feed signals to {len(variance_signals)} "
            f"variance signals with {len(self._available_signal_types)} signal types"
        )

        return variance_signals

    def _compute_adjusted_weights(self) -> dict[SignalType, float]:
        """
        Compute dynamically adjusted weights based on signal availability.

        Strategy:
        1. If a signal type has no data, redistribute its weight to other types
        2. If booking data is very high confidence (>0.95), boost its weight slightly
        3. Ensure weights sum to 1.0

        Returns:
            Dictionary mapping SignalType to adjusted weight.
        """
        weights = dict(self.default_weights)

        # Step 1: Identify unavailable signal types
        unavailable_types = set(weights.keys()) - self._available_signal_types
        available_types = self._available_signal_types & set(weights.keys())

        if not available_types:
            logger.warning(
                "No available signal types detected. Using default weights."
            )
            return weights

        # Redistribute weights from unavailable types
        if unavailable_types:
            redistribution_amount = sum(weights.pop(t) for t in unavailable_types)
            if available_types:
                # Distribute evenly across available types
                per_type = redistribution_amount / len(available_types)
                for signal_type in available_types:
                    weights[signal_type] = weights.get(signal_type, 0.0) + per_type

        # Step 2: Boost high-confidence bookings slightly
        if (
            SignalType.bookings in available_types
            and SignalType.bookings in self._signal_confidences
        ):
            avg_booking_confidence = sum(
                self._signal_confidences[SignalType.bookings]
            ) / len(self._signal_confidences[SignalType.bookings])

            if avg_booking_confidence > 0.95:
                # Boost by 5% of its current weight
                current_booking_weight = weights.get(SignalType.bookings, 0.0)
                boost = current_booking_weight * 0.05
                weights[SignalType.bookings] = current_booking_weight + boost

                # Reduce other types proportionally
                other_types = [t for t in weights.keys() if t != SignalType.bookings]
                if other_types:
                    per_type_reduction = boost / len(other_types)
                    for signal_type in other_types:
                        weights[signal_type] = max(
                            0.0, weights[signal_type] - per_type_reduction
                        )

        # Step 3: Normalise to sum to 1.0
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}

        logger.debug(
            f"Adjusted weights: {weights} (available types: {self._available_signal_types})"
        )

        return weights

    async def get_demand_outlook(
        self,
        location: Location,
        target_date: date,
        venue_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Compute overall demand outlook for a target date.

        Analyzes signals from one day before to one day after target_date
        to assess whether demand is expected to be bullish, bearish, or neutral.

        Args:
            location: Venue geographic location.
            target_date: The date to assess demand for.
            venue_id: Optional venue ID.

        Returns:
            Dictionary with keys:
            - outlook: "bullish", "bearish", or "neutral"
            - confidence: 0-1, overall confidence in the assessment
            - summary: Human-readable string
            - signals_used: List of signal types that contributed
            - average_variance: Weighted average variance across all signals
        """
        from datetime import timedelta

        start = target_date - timedelta(days=1)
        end = target_date + timedelta(days=1)

        variance_signals = await self.get_variance_signals(
            location, start, end, venue_id
        )

        if not variance_signals:
            return {
                "outlook": "neutral",
                "confidence": 0.0,
                "summary": "Insufficient data for demand assessment",
                "signals_used": [],
                "average_variance": 0.0,
            }

        # Calculate weighted average variance
        total_weighted_value = 0.0
        total_weight_confidence = 0.0

        for signal in variance_signals:
            effective_weight = signal.weight * signal.confidence
            total_weighted_value += signal.value * effective_weight
            total_weight_confidence += effective_weight

        if total_weight_confidence > 0:
            average_variance = total_weighted_value / total_weight_confidence
            average_confidence = total_weight_confidence / len(variance_signals)
        else:
            average_variance = 0.0
            average_confidence = 0.0

        # Determine outlook
        if average_variance > 0.2:
            outlook = "bullish"
            interpretation = "Demand expected to exceed forecast"
        elif average_variance < -0.2:
            outlook = "bearish"
            interpretation = "Demand expected to fall short of forecast"
        else:
            outlook = "neutral"
            interpretation = "Demand expected to match forecast"

        signals_used = list(self._available_signal_types)

        return {
            "outlook": outlook,
            "confidence": min(1.0, average_confidence),
            "summary": interpretation,
            "signals_used": [t.value for t in signals_used],
            "average_variance": average_variance,
        }

    def get_signal_dashboard(self) -> dict[str, Any]:
        """
        Get a summary dashboard of all currently active signals.

        Returns a dict grouped by category, showing:
        - Count of signals per category
        - Average confidence per category
        - Latest fetch time
        - Active signal types

        Returns:
            Dictionary with dashboard data:
            - by_category: Dict[str, dict] with count, avg_confidence, latest_fetch
            - total_signals: Total count of signals in cache
            - available_types: List of available signal types
            - cache_size: Number of items in the cache
        """
        dashboard: dict[str, Any] = {
            "by_category": {},
            "total_signals": 0,
            "available_types": [t.value for t in self._available_signal_types],
            "cache_size": self.cache.size,
        }

        # Count and aggregate by category (from available signal types)
        category_stats: dict[FeedCategory, dict[str, Any]] = {}
        for signal_type in self._available_signal_types:
            # Find categories that map to this signal type
            for category, mapped_type in CATEGORY_TO_SIGNAL_TYPE.items():
                if mapped_type == signal_type:
                    if category not in category_stats:
                        confidences = self._signal_confidences.get(signal_type, [])
                        avg_conf = (
                            sum(confidences) / len(confidences)
                            if confidences
                            else 0.0
                        )
                        category_stats[category] = {
                            "count": len(confidences),
                            "avg_confidence": avg_conf,
                        }

        dashboard["by_category"] = {
            cat.value: stats for cat, stats in category_stats.items()
        }
        dashboard["total_signals"] = sum(
            stat["count"] for stat in category_stats.values()
        )

        logger.debug(f"Signal dashboard: {dashboard}")
        return dashboard

    async def __aenter__(self) -> SignalAggregator:
        """
        Async context manager entry point.

        Returns:
            Self for use in async with statement.
        """
        logger.debug("Entering SignalAggregator context")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Async context manager exit point.

        Performs cleanup of adapter resources (e.g., HTTP clients).

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        logger.debug("Exiting SignalAggregator context")

        # Cleanup: close any HTTP clients or connections in adapters
        for adapter in self._all_adapters:
            if hasattr(adapter, "close"):
                try:
                    if asyncio.iscoroutinefunction(adapter.close):
                        await adapter.close()
                    else:
                        adapter.close()
                except Exception as e:
                    logger.error(f"Error closing adapter {adapter.source_name}: {e}")

        # Clear cache
        self.cache.clear_expired()
        logger.debug(f"Cache cleanup: {self.cache.size} items remain")
