"""
Delivery platforms data feed adapter for RosterIQ.

Integrates order volume data from multiple delivery platforms (Uber Eats, DoorDash,
Menulog) to forecast kitchen and front-of-house staffing needs. Tracks delivery
orders separately from dine-in orders, as they represent kitchen load without
floor staff requirements.

Delivery peaks occur at specific times (lunch 11:30-13:30, dinner 17:30-20:30)
and are distinct from dine-in patterns. Signals track:
- Delivery order volume spikes (kitchen load)
- Platform promotions (expected surge)
- Platform outages (orders flow to dine-in)
- Average prep times (kitchen capacity)
"""

from __future__ import annotations

import abc
import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Optional

import httpx

from rosteriq.data_feeds.base import (
    FeedSignal,
    Location,
    FeedCategory,
    SignalStrength,
    DataFeedAdapter,
    SignalCache,
)

logger = logging.getLogger(__name__)

# Delivery platform baseline peaks
DELIVERY_PEAK_HOURS = {
    "lunch": (11, 13),      # 11:30-13:30
    "dinner": (17, 20),     # 17:30-20:30
}

# Default HTTP client timeout (seconds)
DEFAULT_TIMEOUT = 15


# ============================================================================
# Data classes
# ============================================================================


@dataclass(frozen=True)
class DeliverySnapshot:
    """Snapshot of delivery activity for a specific hour."""

    date: date
    hour: int
    order_count: int
    avg_order_value: float
    platform: str
    prep_time_minutes: float

    @property
    def cache_key(self) -> str:
        """Unique key for caching this snapshot."""
        parts = f"delivery_snapshot:{self.date}:{self.hour}:{self.platform}"
        return hashlib.md5(parts.encode()).hexdigest()


# ============================================================================
# Uber Eats Adapter
# ============================================================================


class UberEatsAdapter(DataFeedAdapter):
    """
    Uber Eats Merchant API adapter.

    Fetches order volume and timing data from venues that are active on Uber Eats.
    API endpoint: https://api.uber.com/v1/eats/

    Requires: Uber API key and merchant account access.
    """

    BASE_URL = "https://api.uber.com/v1/eats"
    DEFAULT_CACHE_TTL = 10  # minutes

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        merchant_ids: Optional[dict[str, str]] = None,
    ):
        """
        Initialize Uber Eats adapter.

        Args:
            api_key: Uber API authentication token.
            cache: Shared signal cache instance.
            merchant_ids: Mapping of venue_id -> uber_merchant_id.
        """
        super().__init__(api_key=api_key, cache=cache)
        self.merchant_ids = merchant_ids or {}
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        """This adapter serves delivery platform signals."""
        return FeedCategory.delivery

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "uber_eats"

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client connection."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def is_available(self) -> bool:
        """Check if Uber Eats API is reachable and credentials are valid."""
        if not self.api_key:
            return False
        try:
            client = await self._get_client()
            response = await client.get("/v1/eats/health")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Uber Eats health check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch delivery order signals from Uber Eats.

        Args:
            location: Venue location (unused for Uber Eats, requires merchant_id).
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Venue identifier to look up merchant ID.

        Returns:
            List of FeedSignal objects with delivery order volume per hour.
        """
        signals = []

        if not venue_id or venue_id not in self.merchant_ids:
            logger.warning(f"No Uber Eats merchant ID found for venue {venue_id}")
            return signals

        merchant_id = self.merchant_ids[venue_id]

        try:
            client = await self._get_client()
            current = start_date

            while current <= end_date:
                try:
                    # Fetch orders for this day
                    response = await client.get(
                        f"/v1/eats/merchants/{merchant_id}/orders",
                        params={
                            "date": current.isoformat(),
                            "limit": 100,
                        },
                    )

                    if response.status_code != 200:
                        logger.warning(
                            f"Uber Eats API returned {response.status_code} for {current}"
                        )
                        current += timedelta(days=1)
                        continue

                    data = response.json()
                    orders = data.get("orders", [])

                    # Group by hour and generate signals
                    hourly_stats = await self._aggregate_hourly_orders(
                        orders, current
                    )
                    signals.extend(
                        await self._generate_signals(
                            hourly_stats, current, venue_id
                        )
                    )

                except Exception as e:
                    logger.error(f"Error fetching Uber Eats data for {current}: {e}")

                current += timedelta(days=1)

        except Exception as e:
            logger.error(f"Uber Eats adapter error: {e}")

        return signals

    async def _aggregate_hourly_orders(
        self, orders: list[dict], signal_date: date
    ) -> dict[int, dict[str, Any]]:
        """
        Aggregate orders by hour.

        Args:
            orders: List of order objects from API.
            signal_date: Date for this batch of orders.

        Returns:
            Dict mapping hour (0-23) to aggregated stats.
        """
        hourly = {}
        for order in orders:
            try:
                created_time = datetime.fromisoformat(order.get("created_at", ""))
                hour = created_time.hour
                total = float(order.get("total_price", 0))
                prep_time = float(order.get("estimated_prep_time_minutes", 20))

                if hour not in hourly:
                    hourly[hour] = {
                        "count": 0,
                        "total_value": 0.0,
                        "total_prep_time": 0.0,
                    }

                hourly[hour]["count"] += 1
                hourly[hour]["total_value"] += total
                hourly[hour]["total_prep_time"] += prep_time

            except Exception as e:
                logger.debug(f"Error parsing Uber Eats order: {e}")

        return hourly

    async def _generate_signals(
        self,
        hourly_stats: dict[int, dict[str, Any]],
        signal_date: date,
        venue_id: str,
    ) -> list[FeedSignal]:
        """
        Generate FeedSignal objects from hourly aggregates.

        Args:
            hourly_stats: Aggregated hourly order stats.
            signal_date: Date these signals apply to.
            venue_id: Venue identifier.

        Returns:
            List of FeedSignal objects.
        """
        signals = []

        for hour, stats in hourly_stats.items():
            count = stats["count"]
            avg_value = stats["total_value"] / count if count > 0 else 0.0
            avg_prep = stats["total_prep_time"] / count if count > 0 else 20.0

            # Determine signal strength based on volume
            # (This would ideally compare to baseline/last week)
            if count > 30:
                strength = SignalStrength.moderate_positive
            elif count > 15:
                strength = SignalStrength.weak_positive
            else:
                strength = SignalStrength.neutral

            signal = self._make_signal(
                signal_date=signal_date,
                strength=strength,
                description=f"Uber Eats: {count} orders at hour {hour} (avg {avg_prep:.1f}min prep)",
                confidence=0.85,
                hour=hour,
                raw_data={
                    "platform": "uber_eats",
                    "order_count": count,
                    "avg_order_value": avg_value,
                    "prep_time_minutes": avg_prep,
                    "staff_type": "kitchen",  # Delivery primarily affects kitchen
                },
                venue_id=venue_id,
                ttl_minutes=self.DEFAULT_CACHE_TTL,
            )
            signals.append(signal)

        return signals


# ============================================================================
# DoorDash Adapter
# ============================================================================


class DoorDashAdapter(DataFeedAdapter):
    """
    DoorDash Merchant API adapter.

    Fetches order volume and metrics from DoorDash merchants.
    API endpoint: https://openapi.doordash.com/drive/v2/

    Requires: DoorDash API key and merchant account.
    """

    BASE_URL = "https://openapi.doordash.com/drive/v2"
    DEFAULT_CACHE_TTL = 10  # minutes

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        merchant_ids: Optional[dict[str, str]] = None,
    ):
        """
        Initialize DoorDash adapter.

        Args:
            api_key: DoorDash API authentication token.
            cache: Shared signal cache instance.
            merchant_ids: Mapping of venue_id -> doordash_merchant_id.
        """
        super().__init__(api_key=api_key, cache=cache)
        self.merchant_ids = merchant_ids or {}
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        """This adapter serves delivery platform signals."""
        return FeedCategory.delivery

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "doordash"

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client connection."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def is_available(self) -> bool:
        """Check if DoorDash API is reachable and credentials are valid."""
        if not self.api_key:
            return False
        try:
            client = await self._get_client()
            response = await client.get("/v2/health")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"DoorDash health check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch delivery order signals from DoorDash.

        Args:
            location: Venue location (unused for DoorDash, requires merchant_id).
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Venue identifier to look up merchant ID.

        Returns:
            List of FeedSignal objects with delivery order volume per hour.
        """
        signals = []

        if not venue_id or venue_id not in self.merchant_ids:
            logger.warning(f"No DoorDash merchant ID found for venue {venue_id}")
            return signals

        merchant_id = self.merchant_ids[venue_id]

        try:
            client = await self._get_client()
            current = start_date

            while current <= end_date:
                try:
                    # Fetch metrics for this day
                    response = await client.get(
                        f"/v2/merchants/{merchant_id}/metrics",
                        params={
                            "date": current.isoformat(),
                        },
                    )

                    if response.status_code != 200:
                        logger.warning(
                            f"DoorDash API returned {response.status_code} for {current}"
                        )
                        current += timedelta(days=1)
                        continue

                    data = response.json()
                    metrics = data.get("hourly_metrics", [])

                    # Generate signals from metrics
                    signals.extend(
                        await self._generate_signals(metrics, current, venue_id)
                    )

                except Exception as e:
                    logger.error(f"Error fetching DoorDash data for {current}: {e}")

                current += timedelta(days=1)

        except Exception as e:
            logger.error(f"DoorDash adapter error: {e}")

        return signals

    async def _generate_signals(
        self,
        metrics: list[dict],
        signal_date: date,
        venue_id: str,
    ) -> list[FeedSignal]:
        """
        Generate FeedSignal objects from DoorDash metrics.

        Args:
            metrics: List of hourly metric objects.
            signal_date: Date these signals apply to.
            venue_id: Venue identifier.

        Returns:
            List of FeedSignal objects.
        """
        signals = []

        for metric in metrics:
            try:
                hour = metric.get("hour", 0)
                order_count = metric.get("order_count", 0)
                avg_prep = metric.get("avg_prep_time_minutes", 20.0)

                # Determine signal strength
                if order_count > 35:
                    strength = SignalStrength.moderate_positive
                elif order_count > 15:
                    strength = SignalStrength.weak_positive
                else:
                    strength = SignalStrength.neutral

                signal = self._make_signal(
                    signal_date=signal_date,
                    strength=strength,
                    description=f"DoorDash: {order_count} orders at hour {hour} (avg {avg_prep:.1f}min prep)",
                    confidence=0.85,
                    hour=hour,
                    raw_data={
                        "platform": "doordash",
                        "order_count": order_count,
                        "prep_time_minutes": avg_prep,
                        "staff_type": "kitchen",
                    },
                    venue_id=venue_id,
                    ttl_minutes=self.DEFAULT_CACHE_TTL,
                )
                signals.append(signal)

            except Exception as e:
                logger.debug(f"Error processing DoorDash metric: {e}")

        return signals


# ============================================================================
# Menulog Adapter
# ============================================================================


class MenulogAdapter(DataFeedAdapter):
    """
    Menulog (Just Eat AU) Merchant API adapter.

    Menulog is the major Australian delivery platform. This adapter fetches
    order volume and performance data for AU venues.
    API endpoint: https://api.menulog.com.au/

    Requires: Menulog API key and merchant account.
    """

    BASE_URL = "https://api.menulog.com.au"
    DEFAULT_CACHE_TTL = 10  # minutes

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        merchant_ids: Optional[dict[str, str]] = None,
    ):
        """
        Initialize Menulog adapter.

        Args:
            api_key: Menulog API authentication token.
            cache: Shared signal cache instance.
            merchant_ids: Mapping of venue_id -> menulog_merchant_id.
        """
        super().__init__(api_key=api_key, cache=cache)
        self.merchant_ids = merchant_ids or {}
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def category(self) -> FeedCategory:
        """This adapter serves delivery platform signals."""
        return FeedCategory.delivery

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "menulog"

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client connection."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def is_available(self) -> bool:
        """Check if Menulog API is reachable and credentials are valid."""
        if not self.api_key:
            return False
        try:
            client = await self._get_client()
            response = await client.get("/api/health")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Menulog health check failed: {e}")
            return False

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch delivery order signals from Menulog.

        Args:
            location: Venue location (unused for Menulog, requires merchant_id).
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Venue identifier to look up merchant ID.

        Returns:
            List of FeedSignal objects with delivery order volume per hour.
        """
        signals = []

        if not venue_id or venue_id not in self.merchant_ids:
            logger.warning(f"No Menulog merchant ID found for venue {venue_id}")
            return signals

        merchant_id = self.merchant_ids[venue_id]

        try:
            client = await self._get_client()
            current = start_date

            while current <= end_date:
                try:
                    # Fetch order analytics for this day
                    response = await client.get(
                        f"/api/v1/merchants/{merchant_id}/analytics/orders",
                        params={
                            "date": current.isoformat(),
                            "granularity": "hourly",
                        },
                    )

                    if response.status_code != 200:
                        logger.warning(
                            f"Menulog API returned {response.status_code} for {current}"
                        )
                        current += timedelta(days=1)
                        continue

                    data = response.json()
                    hourly_data = data.get("hourly_breakdown", [])

                    # Generate signals from hourly data
                    signals.extend(
                        await self._generate_signals(
                            hourly_data, current, venue_id
                        )
                    )

                except Exception as e:
                    logger.error(f"Error fetching Menulog data for {current}: {e}")

                current += timedelta(days=1)

        except Exception as e:
            logger.error(f"Menulog adapter error: {e}")

        return signals

    async def _generate_signals(
        self,
        hourly_data: list[dict],
        signal_date: date,
        venue_id: str,
    ) -> list[FeedSignal]:
        """
        Generate FeedSignal objects from Menulog hourly breakdown.

        Args:
            hourly_data: List of hourly analytics objects.
            signal_date: Date these signals apply to.
            venue_id: Venue identifier.

        Returns:
            List of FeedSignal objects.
        """
        signals = []

        for hour_data in hourly_data:
            try:
                hour = hour_data.get("hour", 0)
                orders = hour_data.get("orders", 0)
                avg_prep = hour_data.get("avg_prep_time_minutes", 20.0)
                avg_value = hour_data.get("avg_order_value_aud", 0.0)

                # Determine signal strength (Menulog is AU, often higher volume)
                if orders > 40:
                    strength = SignalStrength.moderate_positive
                elif orders > 20:
                    strength = SignalStrength.weak_positive
                else:
                    strength = SignalStrength.neutral

                signal = self._make_signal(
                    signal_date=signal_date,
                    strength=strength,
                    description=f"Menulog: {orders} orders at hour {hour} (avg {avg_prep:.1f}min prep, ${avg_value:.2f})",
                    confidence=0.85,
                    hour=hour,
                    raw_data={
                        "platform": "menulog",
                        "order_count": orders,
                        "avg_order_value": avg_value,
                        "prep_time_minutes": avg_prep,
                        "staff_type": "kitchen",
                    },
                    venue_id=venue_id,
                    ttl_minutes=self.DEFAULT_CACHE_TTL,
                )
                signals.append(signal)

            except Exception as e:
                logger.debug(f"Error processing Menulog hourly data: {e}")

        return signals


# ============================================================================
# Webhook Receiver
# ============================================================================


class DeliveryWebhookReceiver:
    """
    Webhook receiver for delivery platform events.

    Platforms like Uber, DoorDash, and Menulog can push real-time order events
    to a webhook endpoint. This class validates and processes those events,
    converting them into FeedSignal objects for immediate ingestion.

    Supports:
    - order.created
    - order.completed
    - platform.promotion_started
    - platform.promotion_ended
    - platform.outage
    """

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize webhook receiver.

        Args:
            cache: Shared signal cache instance.
        """
        self.cache = cache or SignalCache()

    async def handle_event(
        self, event: dict[str, Any], venue_id: str
    ) -> Optional[FeedSignal]:
        """
        Process a webhook event from a delivery platform.

        Args:
            event: Event payload from platform webhook.
            venue_id: Venue this event applies to.

        Returns:
            FeedSignal object if event is processable, None otherwise.
        """
        event_type = event.get("type", "")
        platform = event.get("platform", "unknown").lower()

        logger.info(f"Processing delivery webhook: {event_type} from {platform}")

        if event_type == "order.created":
            return self._handle_order_created(event, platform, venue_id)
        elif event_type == "order.completed":
            return self._handle_order_completed(event, platform, venue_id)
        elif event_type == "platform.promotion_started":
            return self._handle_promotion_started(event, platform, venue_id)
        elif event_type == "platform.promotion_ended":
            return self._handle_promotion_ended(event, platform, venue_id)
        elif event_type == "platform.outage":
            return self._handle_platform_outage(event, platform, venue_id)
        else:
            logger.debug(f"Unhandled event type: {event_type}")
            return None

    def _handle_order_created(
        self, event: dict[str, Any], platform: str, venue_id: str
    ) -> Optional[FeedSignal]:
        """Handle a new order event."""
        try:
            order_time = datetime.fromisoformat(event.get("created_at", ""))
            prep_time = float(event.get("estimated_prep_time_minutes", 20))

            signal = FeedSignal(
                category=FeedCategory.delivery,
                source=f"webhook_{platform}",
                signal_date=order_time.date(),
                signal_hour=order_time.hour,
                strength=SignalStrength.weak_positive,
                description=f"{platform.title()} order created (prep ~{prep_time:.0f}min)",
                confidence=0.95,  # Webhook data is highly accurate
                raw_data={
                    "event_type": "order.created",
                    "platform": platform,
                    "order_id": event.get("order_id"),
                    "prep_time_minutes": prep_time,
                    "staff_type": "kitchen",
                },
                venue_id=venue_id,
            )
            self.cache.put(signal, ttl_minutes=10)
            return signal

        except Exception as e:
            logger.error(f"Error handling order.created webhook: {e}")
            return None

    def _handle_order_completed(
        self, event: dict[str, Any], platform: str, venue_id: str
    ) -> Optional[FeedSignal]:
        """Handle an order completion event."""
        try:
            completed_time = datetime.fromisoformat(event.get("completed_at", ""))
            actual_prep = float(event.get("actual_prep_time_minutes", 20))

            signal = FeedSignal(
                category=FeedCategory.delivery,
                source=f"webhook_{platform}",
                signal_date=completed_time.date(),
                signal_hour=completed_time.hour,
                strength=SignalStrength.neutral,
                description=f"{platform.title()} order completed (actual prep {actual_prep:.0f}min)",
                confidence=0.95,
                raw_data={
                    "event_type": "order.completed",
                    "platform": platform,
                    "order_id": event.get("order_id"),
                    "actual_prep_time_minutes": actual_prep,
                    "staff_type": "kitchen",
                },
                venue_id=venue_id,
            )
            self.cache.put(signal, ttl_minutes=10)
            return signal

        except Exception as e:
            logger.error(f"Error handling order.completed webhook: {e}")
            return None

    def _handle_promotion_started(
        self, event: dict[str, Any], platform: str, venue_id: str
    ) -> Optional[FeedSignal]:
        """Handle a promotion start event (expect order surge)."""
        try:
            start_time = datetime.fromisoformat(event.get("start_time", ""))

            signal = FeedSignal(
                category=FeedCategory.delivery,
                source=f"webhook_{platform}",
                signal_date=start_time.date(),
                signal_hour=start_time.hour,
                strength=SignalStrength.moderate_positive,
                description=f"{platform.title()} promotion started: {event.get('description', 'unknown')}",
                confidence=0.8,
                raw_data={
                    "event_type": "platform.promotion_started",
                    "platform": platform,
                    "promotion_id": event.get("promotion_id"),
                    "description": event.get("description"),
                    "staff_type": "kitchen",
                },
                venue_id=venue_id,
            )
            self.cache.put(signal, ttl_minutes=60)
            return signal

        except Exception as e:
            logger.error(f"Error handling promotion_started webhook: {e}")
            return None

    def _handle_promotion_ended(
        self, event: dict[str, Any], platform: str, venue_id: str
    ) -> Optional[FeedSignal]:
        """Handle a promotion end event."""
        try:
            end_time = datetime.fromisoformat(event.get("end_time", ""))

            signal = FeedSignal(
                category=FeedCategory.delivery,
                source=f"webhook_{platform}",
                signal_date=end_time.date(),
                signal_hour=end_time.hour,
                strength=SignalStrength.weak_negative,
                description=f"{platform.title()} promotion ended",
                confidence=0.9,
                raw_data={
                    "event_type": "platform.promotion_ended",
                    "platform": platform,
                    "promotion_id": event.get("promotion_id"),
                    "staff_type": "kitchen",
                },
                venue_id=venue_id,
            )
            self.cache.put(signal, ttl_minutes=60)
            return signal

        except Exception as e:
            logger.error(f"Error handling promotion_ended webhook: {e}")
            return None

    def _handle_platform_outage(
        self, event: dict[str, Any], platform: str, venue_id: str
    ) -> Optional[FeedSignal]:
        """Handle a platform outage event (orders may flow to dine-in)."""
        try:
            outage_time = datetime.fromisoformat(event.get("start_time", ""))
            is_resolved = event.get("is_resolved", False)

            strength = (
                SignalStrength.weak_positive if is_resolved
                else SignalStrength.weak_negative
            )
            description = (
                f"{platform.title()} outage resolved"
                if is_resolved
                else f"{platform.title()} platform outage"
            )

            signal = FeedSignal(
                category=FeedCategory.delivery,
                source=f"webhook_{platform}",
                signal_date=outage_time.date(),
                signal_hour=outage_time.hour,
                strength=strength,
                description=description,
                confidence=0.95,
                raw_data={
                    "event_type": "platform.outage",
                    "platform": platform,
                    "is_resolved": is_resolved,
                    "expected_duration_minutes": event.get(
                        "expected_duration_minutes"
                    ),
                    "staff_type": "both",  # Outage affects both kitchen and floor
                },
                venue_id=venue_id,
            )
            self.cache.put(signal, ttl_minutes=30)
            return signal

        except Exception as e:
            logger.error(f"Error handling platform_outage webhook: {e}")
            return None


# ============================================================================
# Composite Delivery Aggregator
# ============================================================================


class DeliveryAggregator(DataFeedAdapter):
    """
    Composite delivery adapter that aggregates signals from all platform adapters.

    Manages multiple delivery platform adapters (Uber, DoorDash, Menulog) and the
    webhook receiver, coordinating their data into unified delivery signals that
    account for total platform volume and cross-platform patterns.

    This adapter handles:
    - Coordinating fetches from all platform adapters
    - Deduplicating signals from multiple sources
    - Aggregating hourly totals across platforms
    - Comparing current performance to historical baselines
    - Detecting platform outages or anomalies
    """

    def __init__(
        self,
        cache: Optional[SignalCache] = None,
        uber_eats_key: Optional[str] = None,
        doordash_key: Optional[str] = None,
        menulog_key: Optional[str] = None,
    ):
        """
        Initialize delivery aggregator with platform adapters.

        Args:
            cache: Shared signal cache instance.
            uber_eats_key: Uber Eats API key.
            doordash_key: DoorDash API key.
            menulog_key: Menulog API key.
        """
        super().__init__(cache=cache)
        self.cache = cache or SignalCache()

        self.uber_eats = UberEatsAdapter(api_key=uber_eats_key, cache=self.cache)
        self.doordash = DoorDashAdapter(api_key=doordash_key, cache=self.cache)
        self.menulog = MenulogAdapter(api_key=menulog_key, cache=self.cache)
        self.webhook_receiver = DeliveryWebhookReceiver(cache=self.cache)

    @property
    def category(self) -> FeedCategory:
        """This adapter serves delivery platform signals."""
        return FeedCategory.delivery

    @property
    def source_name(self) -> str:
        """Unique identifier for this data source."""
        return "delivery_aggregator"

    async def close(self) -> None:
        """Close all platform adapters."""
        await self.uber_eats.close()
        await self.doordash.close()
        await self.menulog.close()

    async def is_available(self) -> bool:
        """
        Check if at least one delivery platform adapter is available.

        Returns:
            True if any platform adapter is reachable.
        """
        results = await asyncio.gather(
            self.uber_eats.is_available(),
            self.doordash.is_available(),
            self.menulog.is_available(),
            return_exceptions=True,
        )
        return any(r is True for r in results)

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch aggregated delivery signals from all available platforms.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Venue identifier.

        Returns:
            List of aggregated FeedSignal objects.
        """
        signals = []

        # Fetch from all platforms in parallel
        results = await asyncio.gather(
            self.uber_eats.fetch_signals(location, start_date, end_date, venue_id),
            self.doordash.fetch_signals(location, start_date, end_date, venue_id),
            self.menulog.fetch_signals(location, start_date, end_date, venue_id),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, list):
                signals.extend(result)
            elif isinstance(result, Exception):
                logger.warning(f"Error fetching from platform adapter: {result}")

        # Aggregate signals by date+hour and generate composite signals
        aggregated = await self._aggregate_signals(signals, venue_id)

        return aggregated

    async def _aggregate_signals(
        self, signals: list[FeedSignal], venue_id: Optional[str]
    ) -> list[FeedSignal]:
        """
        Aggregate signals from multiple platforms into unified delivery signals.

        Args:
            signals: Signals from all platform adapters.
            venue_id: Venue identifier.

        Returns:
            Aggregated FeedSignal objects.
        """
        # Group by date+hour
        by_hour = {}
        for signal in signals:
            key = (signal.signal_date, signal.signal_hour)
            if key not in by_hour:
                by_hour[key] = []
            by_hour[key].append(signal)

        aggregated = []

        for (sig_date, hour), hour_signals in by_hour.items():
            total_orders = sum(
                sig.raw_data.get("order_count", 0) for sig in hour_signals
            )
            avg_prep = (
                sum(
                    sig.raw_data.get("prep_time_minutes", 20)
                    for sig in hour_signals
                )
                / len(hour_signals)
                if hour_signals
                else 20.0
            )

            # Determine aggregated strength
            if total_orders > 70:
                strength = SignalStrength.moderate_positive
            elif total_orders > 40:
                strength = SignalStrength.weak_positive
            else:
                strength = SignalStrength.neutral

            platforms = [
                sig.raw_data.get("platform", "unknown") for sig in hour_signals
            ]
            desc = f"Delivery total: {total_orders} orders across {len(hour_signals)} platform(s) ({', '.join(set(platforms))})"

            composite_signal = FeedSignal(
                category=FeedCategory.delivery,
                source=self.source_name,
                signal_date=sig_date,
                signal_hour=hour,
                strength=strength,
                description=desc,
                confidence=0.85,
                raw_data={
                    "total_order_count": total_orders,
                    "platform_count": len(hour_signals),
                    "avg_prep_time_minutes": avg_prep,
                    "platforms": platforms,
                    "staff_type": "kitchen",
                },
                venue_id=venue_id,
            )
            self.cache.put(composite_signal, ttl_minutes=10)
            aggregated.append(composite_signal)

        return aggregated
