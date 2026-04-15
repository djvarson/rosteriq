"""
Signal Feeds V2 - Enriched Signal Aggregator
=============================================

Extends SignalAggregator with new BOM weather and events adapters via signal_bridge.

EnrichedSignalAggregator:
  - Subclass of SignalAggregator
  - Overrides collect_all_signals to fetch from BOM weather and events adapters
  - Uses signal_bridge.weather_to_signals and signal_bridge.events_to_signals
  - Deduplicates signals: if both self.weather (legacy) and new WeatherAdapter
    produce signals for the same date, prefers the new one (marked with
    raw_data["bridge_version"] = "v2")
  - Falls back to demo adapters if ROSTERIQ_DATA_MODE=demo

Configuration:
  ROSTERIQ_DATA_MODE: "demo" (default) or "live"
  - demo: uses DemoWeatherAdapter and DemoEventsAdapter
  - live: caller must provide weather_adapter and events_adapter instances
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date
from typing import Optional

from rosteriq.signal_feeds import SignalAggregator, Signal, SignalSourceType
from rosteriq.signal_bridge import weather_to_signals, events_to_signals
from rosteriq.pattern_signals import patterns_to_signals

logger = logging.getLogger("rosteriq.signal_feeds_v2")


class EnrichedSignalAggregator(SignalAggregator):
    """
    Enhanced SignalAggregator that combines legacy feeds with new BOM and events
    adapters via signal_bridge, plus patterns from ShiftEventStore.

    Attributes:
        weather_adapter: WeatherAdapter for BOM forecasts (DemoWeatherAdapter or BOMAdapter)
        events_adapter: EventsAdapter for events (DemoEventsAdapter, CompositeEventsAdapter, etc.)
        pattern_store: ShiftEventStore for learning patterns from logged events
    """

    def __init__(
        self,
        weather_adapter=None,
        events_adapter=None,
        pattern_store=None,
    ):
        """
        Initialize EnrichedSignalAggregator.

        Args:
            weather_adapter: WeatherAdapter instance. If None, auto-detects from ROSTERIQ_DATA_MODE
            events_adapter: EventsAdapter instance. If None, auto-detects from ROSTERIQ_DATA_MODE
            pattern_store: ShiftEventStore instance for pattern learning. If None, patterns disabled.
        """
        super().__init__()

        self.pattern_store = pattern_store
        data_mode = os.getenv("ROSTERIQ_DATA_MODE", "demo")

        # Auto-detect weather adapter if not provided
        if weather_adapter is None:
            if data_mode == "demo":
                try:
                    from rosteriq.data_feeds.bom import DemoWeatherAdapter

                    self.weather_adapter = DemoWeatherAdapter()
                    logger.info("EnrichedSignalAggregator: using DemoWeatherAdapter")
                except Exception as e:
                    logger.error(f"Failed to init DemoWeatherAdapter: {e}")
                    self.weather_adapter = None
            else:
                logger.info(
                    "EnrichedSignalAggregator: live mode, awaiting weather_adapter from caller"
                )
                self.weather_adapter = None
        else:
            self.weather_adapter = weather_adapter
            logger.info("EnrichedSignalAggregator: using provided weather_adapter")

        # Auto-detect events adapter if not provided
        if events_adapter is None:
            if data_mode == "demo":
                try:
                    from rosteriq.data_feeds.events import DemoEventsAdapter

                    self.events_adapter = DemoEventsAdapter()
                    logger.info("EnrichedSignalAggregator: using DemoEventsAdapter")
                except Exception as e:
                    logger.error(f"Failed to init DemoEventsAdapter: {e}")
                    self.events_adapter = None
            else:
                logger.info(
                    "EnrichedSignalAggregator: live mode, awaiting events_adapter from caller"
                )
                self.events_adapter = None
        else:
            self.events_adapter = events_adapter
            logger.info("EnrichedSignalAggregator: using provided events_adapter")

    async def collect_all_signals(
        self,
        venue_id: str,
        target_date: date,
        location_lat: Optional[float] = None,
        location_lng: Optional[float] = None,
    ) -> list[Signal]:
        """
        Collect signals from all sources: legacy feeds + new adapters.

        Override of parent SignalAggregator.collect_all_signals that:
        1. Calls parent to get legacy signals (bookings, foot traffic, delivery)
        2. Fetches new weather signals from BOM adapter
        3. Fetches event signals from events adapter
        4. Deduplicates: if both legacy self.weather and new WeatherAdapter produce
           signals for the same date, prefers the new one (marked with
           raw_data["bridge_version"] = "v2")
        5. Returns merged list

        Args:
            venue_id: Venue identifier
            target_date: Single date to collect signals for
            location_lat: Venue latitude (passed to adapters)
            location_lng: Venue longitude (passed to adapters)

        Returns:
            List of Signal objects from all sources, deduped
        """
        # Get legacy signals (parent class)
        signals = await super().collect_all_signals(
            venue_id, target_date, location_lat, location_lng
        )

        # Build signal dict for dedup: weather signals by date
        # After dedup, prefer new over legacy
        weather_signals_by_date = {}
        non_weather_signals = []

        for sig in signals:
            if sig.source == SignalSourceType.WEATHER:
                # Try to extract date from raw_data
                sig_date = sig.raw_data.get("date")
                if sig_date:
                    # Store or replace (legacy will be replaced by new)
                    weather_signals_by_date[sig_date] = sig
                else:
                    non_weather_signals.append(sig)
            else:
                non_weather_signals.append(sig)

        # Fetch new weather signals from BOM adapter
        if self.weather_adapter:
            try:
                date_range = (target_date, target_date)
                new_weather_signals = await weather_to_signals(
                    self.weather_adapter,
                    venue_id,
                    date_range,
                    lat=location_lat,
                    lng=location_lng,
                )

                # Mark as v2 and replace legacy weather signals for same date
                for sig in new_weather_signals:
                    sig.raw_data["bridge_version"] = "v2"
                    sig_date = sig.raw_data.get("date")
                    if sig_date:
                        logger.debug(
                            f"Weather signal dedup: replacing legacy with v2 for {sig_date}"
                        )
                        weather_signals_by_date[sig_date] = sig
            except Exception as e:
                logger.warning(
                    f"Failed to fetch weather signals from new adapter: {e}"
                )

        # Fetch event signals
        if self.events_adapter:
            try:
                date_range = (target_date, target_date)
                event_signals = await events_to_signals(
                    self.events_adapter,
                    venue_id,
                    date_range,
                    lat=location_lat,
                    lng=location_lng,
                )
                event_signals = [s for s in event_signals if s is not None]
                non_weather_signals.extend(event_signals)
            except Exception as e:
                logger.warning(f"Failed to fetch event signals: {e}")

        # Inject pattern signals if store is available
        pattern_signals = []
        if self.pattern_store:
            try:
                pattern_signals = await patterns_to_signals(
                    self.pattern_store,
                    venue_id,
                    target_date,
                    hour=None,  # Collect all patterns for the target_date's weekday
                )
                if pattern_signals:
                    logger.debug(
                        f"Injected {len(pattern_signals)} pattern signals for {venue_id}"
                    )
            except Exception as e:
                logger.warning(f"Failed to fetch pattern signals: {e}")

        # Reconstruct final signal list: non-weather + deduped weather + patterns
        final_signals = (
            non_weather_signals
            + list(weather_signals_by_date.values())
            + pattern_signals
        )

        logger.debug(
            f"collect_all_signals: {venue_id} on {target_date} collected "
            f"{len(final_signals)} total signals "
            f"({len(weather_signals_by_date)} weather, {len(event_signals)} events, "
            f"{len(pattern_signals)} patterns)"
        )

        return final_signals

    async def close(self):
        """Close parent and clean up new adapters."""
        # Close new adapters if they have close methods
        if self.weather_adapter and hasattr(self.weather_adapter, "close"):
            try:
                await self.weather_adapter.close()
            except Exception as e:
                logger.warning(f"Failed to close weather_adapter: {e}")

        if self.events_adapter and hasattr(self.events_adapter, "close"):
            try:
                await self.events_adapter.close()
            except Exception as e:
                logger.warning(f"Failed to close events_adapter: {e}")

        # Close parent
        await super().close()
