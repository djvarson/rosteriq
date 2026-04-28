"""
Kounta (Lightspeed K Series) integration adapter for RosterIQ.

Kounta is a cloud-based POS system popular in Australian restaurants and cafés,
especially integrated into the Lightspeed ecosystem. This adapter pulls sales data,
payment methods, and category breakdowns to generate demand signals.

Supports:
- Sales data by date range with hourly aggregation
- Product categories and menu structure
- Payment method breakdown (cash vs card — affects float management)
- Register/site data
- GST extraction and AUD currency handling
- AEST/AEDT timezone conversion

Authentication:
    - API key authentication (X-API-Key header)
    - Simpler than OAuth; requires a single API key
    - Rate limit: 5 req/sec
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from rosteriq.data_feeds.base import (
    DataFeedAdapter,
    FeedCategory,
    FeedSignal,
    Location,
    SignalCache,
    SignalStrength,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Data classes
# ============================================================================


@dataclass
class HourlySalesData:
    """
    Hourly sales aggregation from Kounta for demand signal generation.
    """

    date: date
    hour: Optional[int] = None
    total_revenue: float = 0.0  # AUD (GST-inclusive)
    total_revenue_excl_gst: float = 0.0
    gst_amount: float = 0.0
    total_items: int = 0
    total_covers: float = 0.0
    discount_amount: float = 0.0
    transactions: int = 0

    # Category breakdown
    food_revenue: float = 0.0
    beverage_revenue: float = 0.0
    other_revenue: float = 0.0

    # Payment methods
    cash_amount: float = 0.0
    card_amount: float = 0.0
    other_payment: float = 0.0

    def __post_init__(self) -> None:
        """Calculate derived metrics."""
        if self.total_revenue > 0 and self.gst_amount == 0:
            self.gst_amount = self.total_revenue / 11.0
            self.total_revenue_excl_gst = self.total_revenue - self.gst_amount


@dataclass
class PaymentBreakdown:
    """
    Payment method split for a venue on a date.
    Useful for float management (cash ratio affects physical cash handling).
    """

    date: date
    venue_id: str
    cash_amount: float = 0.0
    card_amount: float = 0.0
    other_amount: float = 0.0
    total_amount: float = 0.0

    @property
    def cash_ratio(self) -> float:
        """Percentage of revenue in cash."""
        if self.total_amount <= 0:
            return 0.0
        return self.cash_amount / self.total_amount

    @property
    def card_ratio(self) -> float:
        """Percentage of revenue by card."""
        if self.total_amount <= 0:
            return 0.0
        return self.card_amount / self.total_amount


# ============================================================================
# Kounta Adapter
# ============================================================================


class KountaAdapter(DataFeedAdapter):
    """
    Adapter for Kounta (Lightspeed K Series) POS.

    Kounta provides REST API access to sales, products, and payment data.
    This adapter maps sales patterns and payment methods into demand signals
    and float management insights.

    Authentication:
        - API key in X-API-Key header
        - Simpler than OAuth
    """

    category = FeedCategory.reservations
    source_name = "kounta"
    base_url = "https://api.kounta.com/v1"

    # Category mapping
    CATEGORY_MAPPING = {
        "food": "food",
        "beverage": "beverage",
        "drink": "beverage",
        "beer": "beverage",
        "wine": "beverage",
        "spirits": "beverage",
        "coffee": "beverage",
        "hot beverage": "beverage",
        "dessert": "food",
        "appetiser": "food",
        "appetizer": "food",
        "main": "food",
        "lunch": "food",
        "dinner": "food",
        "breakfast": "food",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize Kounta adapter.

        Args:
            api_key: Kounta API key.
            cache: Signal cache instance.
            http_client: Reusable httpx AsyncClient.
        """
        super().__init__(api_key=api_key, cache=cache or SignalCache(default_ttl_minutes=5))
        self._http_client = http_client
        self._owns_client = http_client is None
        self._rate_limit_reset: float = 0.0
        self._request_times: list[float] = []

    async def __aenter__(self) -> KountaAdapter:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize httpx client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            self._owns_client = True
        return self._http_client

    async def _wait_rate_limit(self) -> None:
        """
        Respect Kounta's 5 req/sec rate limit using sliding window.
        """
        now = datetime.utcnow().timestamp()
        # Remove old requests outside the 1-second window
        self._request_times = [t for t in self._request_times if now - t < 1.0]

        if len(self._request_times) >= 5:
            # We've hit the limit; wait until the oldest request is outside the window
            sleep_time = 1.0 - (now - self._request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            now = datetime.utcnow().timestamp()
            self._request_times = [t for t in self._request_times if now - t < 1.0]

        self._request_times.append(now)

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Make an authenticated API request.

        Args:
            method: HTTP method ('GET', 'POST', etc.).
            endpoint: API endpoint path.
            params: Query parameters.

        Returns:
            JSON response dict.

        Raises:
            httpx.HTTPError: If request fails.
        """
        if not self.api_key:
            raise ValueError("Kounta API key required")

        url = f"{self.base_url}{endpoint}"
        headers = {"X-API-Key": self.api_key}

        await self._wait_rate_limit()

        try:
            response = await self.http_client.request(method, url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Kounta API error: {e}")
            raise

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch sales signals from Kounta.

        Creates hourly demand signals based on sales trends and payment patterns.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Kounta company/venue ID.

        Returns:
            List of FeedSignal objects.
        """
        if not venue_id:
            logger.warning("Kounta requires venue_id; skipping fetch")
            return []

        signals = []
        current_date = start_date

        while current_date <= end_date:
            try:
                hourly_records = await self._fetch_daily_sales(venue_id, current_date)

                for record in hourly_records:
                    if record.total_revenue == 0:
                        continue

                    strength = self._calculate_signal_strength(record)
                    description = (
                        f"Kounta: {record.total_items} items, "
                        f"${record.total_revenue:.2f} revenue (inc. ${record.gst_amount:.2f} GST), "
                        f"cash ratio {record.cash_amount/record.total_revenue*100 if record.total_revenue > 0 else 0:.1f}%"
                    )

                    signal = self._make_signal(
                        signal_date=current_date,
                        strength=strength,
                        description=description,
                        confidence=self._calculate_confidence(record),
                        hour=record.hour,
                        raw_data={"snapshot": vars(record)},
                        venue_id=venue_id,
                        ttl_minutes=5,
                    )
                    signals.append(signal)

            except Exception as e:
                logger.error(f"Kounta fetch failed for {venue_id} on {current_date}: {e}")

            current_date += timedelta(days=1)

        return signals

    async def _fetch_daily_sales(
        self,
        venue_id: str,
        target_date: date,
    ) -> list[HourlySalesData]:
        """
        Fetch and aggregate sales for a single day.

        Kounta API returns orders; we aggregate by hour.

        Args:
            venue_id: Kounta company/venue ID.
            target_date: Date to fetch.

        Returns:
            List of HourlySalesData (one per hour with activity).
        """
        try:
            # Convert to AEST/AEDT timestamps
            aest = timezone(timedelta(hours=10))  # AEST
            start_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=aest)
            end_dt = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=aest)

            endpoint = f"/companies/{venue_id}/orders"
            params = {
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "limit": 1000,
            }

            response = await self._make_request("GET", endpoint, params=params)
            orders = response.get("orders", [])

            if not isinstance(orders, list):
                orders = [orders] if orders else []

            # Aggregate by hour
            hourly: dict[int, dict[str, Any]] = {}

            for order in orders:
                if not self._is_valid_order(order):
                    continue

                # Parse order timestamp (AEST)
                created_time = order.get("created_at", "")
                try:
                    order_dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
                    order_dt_aest = order_dt.astimezone(aest)
                    hour = order_dt_aest.hour
                except (ValueError, AttributeError):
                    hour = 12  # Default

                if hour not in hourly:
                    hourly[hour] = {
                        "total_revenue": 0.0,
                        "total_items": 0,
                        "total_covers": 0.0,
                        "discount_amount": 0.0,
                        "transactions": 0,
                        "food_revenue": 0.0,
                        "beverage_revenue": 0.0,
                        "other_revenue": 0.0,
                        "cash_amount": 0.0,
                        "card_amount": 0.0,
                        "other_payment": 0.0,
                    }

                # Order total (gross)
                total = float(order.get("total", 0.0))
                hourly[hour]["total_revenue"] += total
                hourly[hour]["transactions"] += 1

                # Discounts
                discount = float(order.get("discount_amount", 0.0))
                hourly[hour]["discount_amount"] += discount

                # Line items
                items = order.get("items", [])
                if not isinstance(items, list):
                    items = [items] if items else []

                for item in items:
                    hourly[hour]["total_items"] += 1
                    category = self._map_category(item.get("category_name", ""))
                    item_total = float(item.get("total", 0.0))

                    if category == "food":
                        hourly[hour]["food_revenue"] += item_total
                    elif category == "beverage":
                        hourly[hour]["beverage_revenue"] += item_total
                    else:
                        hourly[hour]["other_revenue"] += item_total

                # Payment method breakdown
                payment_method = order.get("payment_method", "").lower()
                if payment_method in ("cash", "cash payment"):
                    hourly[hour]["cash_amount"] += total
                elif payment_method in ("card", "credit card", "debit card"):
                    hourly[hour]["card_amount"] += total
                else:
                    hourly[hour]["other_payment"] += total

                # Covers estimate (1 per transaction if not available)
                covers = float(order.get("covers", 1.0))
                hourly[hour]["total_covers"] += covers

            # Convert to HourlySalesData objects
            records = []
            for hour, data in sorted(hourly.items()):
                record = HourlySalesData(
                    date=target_date,
                    hour=hour,
                    total_revenue=data["total_revenue"],
                    total_items=data["total_items"],
                    total_covers=data["total_covers"],
                    discount_amount=data["discount_amount"],
                    transactions=data["transactions"],
                    food_revenue=data["food_revenue"],
                    beverage_revenue=data["beverage_revenue"],
                    other_revenue=data["other_revenue"],
                    cash_amount=data["cash_amount"],
                    card_amount=data["card_amount"],
                    other_payment=data["other_payment"],
                )
                records.append(record)

            return records

        except Exception as e:
            logger.error(f"Kounta sales fetch failed: {e}")
            return []

    async def get_payment_breakdown(
        self,
        venue_id: str,
        target_date: date,
    ) -> Optional[PaymentBreakdown]:
        """
        Get payment method breakdown for a venue on a specific date.

        Useful for float management (cash ratio tells us physical cash
        requirements for the day).

        Args:
            venue_id: Kounta venue ID.
            target_date: Date to analyze.

        Returns:
            PaymentBreakdown object or None if fetch fails.
        """
        try:
            hourly_records = await self._fetch_daily_sales(venue_id, target_date)

            total_cash = sum(r.cash_amount for r in hourly_records)
            total_card = sum(r.card_amount for r in hourly_records)
            total_other = sum(r.other_payment for r in hourly_records)
            total = total_cash + total_card + total_other

            return PaymentBreakdown(
                date=target_date,
                venue_id=venue_id,
                cash_amount=total_cash,
                card_amount=total_card,
                other_amount=total_other,
                total_amount=total,
            )
        except Exception as e:
            logger.error(f"Payment breakdown fetch failed: {e}")
            return None

    def _is_valid_order(self, order: dict[str, Any]) -> bool:
        """Check if an order should be included in aggregation."""
        if not order:
            return False
        # Exclude voided/refunded orders
        if order.get("void") or order.get("refunded"):
            return False
        return True

    def _map_category(self, category_name: str) -> str:
        """Map Kounta category to RosterIQ department."""
        if not category_name:
            return "other"
        normalized = category_name.lower().strip()
        return self.CATEGORY_MAPPING.get(normalized, "other")

    def _calculate_signal_strength(self, snapshot: HourlySalesData) -> SignalStrength:
        """Map sales metrics to signal strength."""
        if snapshot.total_revenue > 1000:
            return SignalStrength.strong_positive
        elif snapshot.total_revenue > 500:
            return SignalStrength.moderate_positive
        elif snapshot.total_revenue > 200:
            return SignalStrength.neutral
        elif snapshot.total_revenue > 100:
            return SignalStrength.moderate_negative
        else:
            return SignalStrength.weak_negative

    def _calculate_confidence(self, snapshot: HourlySalesData) -> float:
        """Calculate confidence in the signal."""
        confidence = 0.7

        if snapshot.transactions > 50:
            confidence += 0.2
        elif snapshot.transactions > 20:
            confidence += 0.1

        return min(1.0, confidence)

    async def is_available(self) -> bool:
        """Check if Kounta API is reachable."""
        if not self.api_key:
            return False

        try:
            endpoint = "/health"
            await self._make_request("GET", endpoint)
            return True
        except Exception as e:
            logger.debug(f"Kounta health check failed: {e}")
            return False

    def _make_signal(
        self,
        signal_date: date,
        strength: SignalStrength,
        description: str,
        confidence: float,
        hour: Optional[int],
        raw_data: dict,
        venue_id: Optional[str],
        ttl_minutes: int,
    ) -> FeedSignal:
        """Helper to create a FeedSignal object."""
        signal = FeedSignal(
            category=self.category,
            source=self.source_name,
            signal_date=signal_date,
            signal_hour=hour,
            strength=strength,
            description=description,
            confidence=confidence,
            raw_data=raw_data,
            venue_id=venue_id,
        )
        self.cache.put(signal, ttl_minutes=ttl_minutes)
        return signal
