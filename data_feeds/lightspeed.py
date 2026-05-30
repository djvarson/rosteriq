"""
Lightspeed Restaurant (K Series) integration adapter for RosterIQ.

Lightspeed is a popular POS system in Australian cafés and restaurants.
This adapter pulls hourly sales data, categories, and receipt details
to generate demand signals for the forecasting engine.

Supports:
- Hourly sales data by category (food, beverage, etc.)
- Receipt detail analysis for transaction patterns
- Category → RosterIQ department mapping
- Void/refund tracking
- GST extraction (÷11 from inclusive amounts)
- OAuth2 authorization code flow

Authentication:
    - client_id: Lightspeed app ID
    - client_secret: Lightspeed app secret
    - refresh_token: OAuth2 refresh token
    - Rate limit: 1 req/sec (enforced with exponential backoff)
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
class HourlySalesRecord:
    """
    Hourly sales snapshot from Lightspeed for demand signal generation.
    """

    date: date
    hour: Optional[int] = None
    total_revenue: float = 0.0  # AUD (GST-inclusive)
    total_revenue_excl_gst: float = 0.0  # AUD (GST-exclusive)
    gst_amount: float = 0.0  # GST component
    total_items: int = 0
    total_covers: float = 0.0
    void_amount: float = 0.0
    void_rate: float = 0.0
    avg_transaction_value: float = 0.0
    transactions: int = 0

    # Category breakdown
    food_revenue: float = 0.0
    beverage_revenue: float = 0.0
    other_revenue: float = 0.0

    def __post_init__(self) -> None:
        """Calculate derived metrics."""
        if self.total_revenue > 0 and self.gst_amount == 0:
            # Extract GST: amount ÷ 11 = GST component
            self.gst_amount = self.total_revenue / 11.0
            self.total_revenue_excl_gst = self.total_revenue - self.gst_amount
        if self.transactions > 0:
            self.avg_transaction_value = self.total_revenue / self.transactions
        if self.total_revenue > 0:
            self.void_rate = self.void_amount / self.total_revenue


# ============================================================================
# Lightspeed Adapter
# ============================================================================


class LightspeedAdapter(DataFeedAdapter):
    """
    Adapter for Lightspeed Restaurant (K Series) POS.

    Lightspeed API provides receipt-level and aggregated sales data.
    This adapter maps hourly sales patterns into demand signals.

    OAuth2 Authentication:
        - client_id: Lightspeed app ID
        - client_secret: Lightspeed app secret
        - refresh_token: OAuth2 refresh token
        - Tokens obtained via OAuth2 refresh flow
    """

    category = FeedCategory.reservations
    source_name = "lightspeed"
    base_url = "https://api.merchantos.com/API/Account"

    # Category → department mapping for Australian venues
    CATEGORY_MAPPING = {
        "food": "food",
        "beverage": "beverage",
        "drink": "beverage",
        "beer": "beverage",
        "wine": "beverage",
        "spirits": "beverage",
        "coffee": "beverage",
        "dessert": "food",
        "appetiser": "food",
        "main": "food",
        "lunch": "food",
        "dinner": "food",
        "breakfast": "food",
    }

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize Lightspeed adapter.

        Args:
            client_id: OAuth2 client ID.
            client_secret: OAuth2 client secret.
            refresh_token: OAuth2 refresh token.
            cache: Signal cache instance.
            http_client: Reusable httpx AsyncClient.
        """
        super().__init__(api_key=client_id, cache=cache or SignalCache(default_ttl_minutes=5))
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._http_client = http_client
        self._owns_client = http_client is None
        self._rate_limit_reset: float = 0.0

    async def __aenter__(self) -> LightspeedAdapter:
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
        Respect Lightspeed's 1 req/sec rate limit.
        """
        now = datetime.utcnow().timestamp()
        if now < self._rate_limit_reset:
            wait_time = self._rate_limit_reset - now
            await asyncio.sleep(wait_time)
        self._rate_limit_reset = (datetime.utcnow() + timedelta(seconds=1)).timestamp()

    async def _refresh_token_if_needed(self) -> str:
        """
        Obtain or refresh OAuth2 access token.

        Returns:
            Access token string.

        Raises:
            ValueError: If token refresh fails.
        """
        if self._access_token and self._token_expires_at and datetime.utcnow() < self._token_expires_at:
            return self._access_token

        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise ValueError("Lightspeed client_id, client_secret, and refresh_token required")

        await self._wait_rate_limit()

        url = "https://api.merchantos.com/oauth/token.php"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        try:
            response = await self.http_client.post(url, data=data)
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)
            # Update refresh token if a new one was issued
            new_refresh = token_data.get("refresh_token")
            if new_refresh:
                self.refresh_token = new_refresh
            return self._access_token
        except httpx.ConnectError as e:
            logger.error(f"Lightspeed token refresh connection failed: {e}")
            raise ValueError(f"Cannot connect to Lightspeed for token refresh: {e}")
        except httpx.TimeoutException as e:
            logger.error(f"Lightspeed token refresh timed out: {e}")
            raise ValueError(f"Lightspeed token refresh timed out: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Lightspeed token refresh returned {e.response.status_code}: {e}")
            raise ValueError(f"Failed to refresh Lightspeed access token: {e}")
        except httpx.HTTPError as e:
            logger.error(f"Lightspeed token refresh failed: {e}")
            raise ValueError(f"Failed to refresh Lightspeed access token: {e}")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Make an authenticated API request with retry logic.

        Args:
            method: HTTP method ('GET', 'POST', etc.).
            endpoint: API endpoint path.
            params: Query parameters.
            retry_count: Current retry attempt.
            max_retries: Maximum retry attempts.

        Returns:
            JSON response dict.

        Raises:
            httpx.HTTPError: If request fails after retries.
        """
        token = await self._refresh_token_if_needed()
        url = f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {token}"}

        await self._wait_rate_limit()

        try:
            response = await self.http_client.request(method, url, params=params, headers=headers)

            if response.status_code == 401:
                # Token expired, force refresh and retry
                self._access_token = None
                if retry_count < max_retries:
                    return await self._make_request(method, endpoint, params, retry_count + 1, max_retries)
                response.raise_for_status()

            if response.status_code in (429, 503):
                # Rate limit or service unavailable; exponential backoff
                if retry_count < max_retries:
                    backoff = (2 ** retry_count)
                    logger.warning(f"Lightspeed API returned {response.status_code}; backing off {backoff}s")
                    await asyncio.sleep(backoff)
                    return await self._make_request(method, endpoint, params, retry_count + 1, max_retries)
                response.raise_for_status()

            response.raise_for_status()
            return response.json()
        except httpx.ConnectError as e:
            logger.warning(f"Lightspeed connection failed: {e}")
            raise
        except httpx.TimeoutException as e:
            logger.warning(f"Lightspeed request timed out: {e}")
            raise
        except httpx.HTTPStatusError as e:
            logger.warning(f"Lightspeed API returned {e.response.status_code}: {e}")
            raise
        except httpx.HTTPError as e:
            logger.error(f"Lightspeed API error: {e}")
            raise

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch sales signals from Lightspeed.

        Creates hourly demand signals based on sales trends.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: Lightspeed account ID.

        Returns:
            List of FeedSignal objects with hourly breakdowns.
        """
        if not venue_id:
            logger.warning("Lightspeed requires venue_id; skipping fetch")
            return []

        signals = []
        current_date = start_date

        while current_date <= end_date:
            try:
                # Fetch daily summary
                hourly_data = await self._fetch_daily_sales(venue_id, current_date)

                if not hourly_data:
                    current_date += timedelta(days=1)
                    continue

                # Process each hour
                for record in hourly_data:
                    snapshot = record
                    if snapshot.total_revenue == 0:
                        continue

                    strength = self._calculate_signal_strength(snapshot)
                    description = (
                        f"Lightspeed: {snapshot.total_items} items, "
                        f"${snapshot.total_revenue:.2f} revenue (inc. ${snapshot.gst_amount:.2f} GST), "
                        f"void rate {snapshot.void_rate*100:.1f}%"
                    )

                    signal = self._make_signal(
                        signal_date=current_date,
                        strength=strength,
                        description=description,
                        confidence=self._calculate_confidence(snapshot),
                        hour=snapshot.hour,
                        raw_data={"snapshot": vars(snapshot)},
                        venue_id=venue_id,
                        ttl_minutes=5,
                    )
                    signals.append(signal)

            except Exception as e:
                logger.error(f"Lightspeed fetch failed for {venue_id} on {current_date}: {e}")

            current_date += timedelta(days=1)

        return signals

    async def _fetch_daily_sales(
        self,
        venue_id: str,
        target_date: date,
    ) -> list[HourlySalesRecord]:
        """
        Fetch sales data for a single day and aggregate by hour.

        Lightspeed API returns receipts; we aggregate them hourly.

        Args:
            venue_id: Lightspeed account ID.
            target_date: Date to fetch.

        Returns:
            List of HourlySalesRecord objects (one per hour with activity).
        """
        try:
            # Build date range for query
            start_datetime = datetime.combine(target_date, datetime.min.time())
            end_datetime = datetime.combine(target_date, datetime.max.time())

            endpoint = f"/{venue_id}/Orders.json"
            params = {
                "load_relations": ["OrderItems"],
                "filters": [
                    f"completeTime>={int(start_datetime.timestamp())}",
                    f"completeTime<={int(end_datetime.timestamp())}",
                ],
            }

            response = await self._make_request("GET", endpoint, params=params)
            orders = response.get("Order", [])

            if not isinstance(orders, list):
                orders = [orders] if orders else []

            # Aggregate by hour
            hourly: dict[int, dict[str, Any]] = {}

            for order in orders:
                if not self._is_valid_order(order):
                    continue

                completed_time = order.get("completeTime", 0)
                order_dt = datetime.fromtimestamp(int(completed_time))
                hour = order_dt.hour

                if hour not in hourly:
                    hourly[hour] = {
                        "total_revenue": 0.0,
                        "total_items": 0,
                        "total_covers": 0.0,
                        "void_amount": 0.0,
                        "transactions": 0,
                        "food_revenue": 0.0,
                        "beverage_revenue": 0.0,
                        "other_revenue": 0.0,
                    }

                # Add order totals
                revenue = float(order.get("total", 0.0))
                hourly[hour]["total_revenue"] += revenue
                hourly[hour]["transactions"] += 1

                # Count items and categorize
                items = order.get("OrderItems", [])
                if not isinstance(items, list):
                    items = [items] if items else []

                for item in items:
                    hourly[hour]["total_items"] += 1
                    category = self._map_category(item.get("category", ""))
                    item_revenue = float(item.get("price", 0.0))

                    if category == "food":
                        hourly[hour]["food_revenue"] += item_revenue
                    elif category == "beverage":
                        hourly[hour]["beverage_revenue"] += item_revenue
                    else:
                        hourly[hour]["other_revenue"] += item_revenue

                # Covers (estimate 1 per transaction if not available)
                covers = float(order.get("covers", 1.0))
                hourly[hour]["total_covers"] += covers

                # Void/refund detection
                if order.get("voided") or order.get("refunded"):
                    hourly[hour]["void_amount"] += revenue

            # Convert to HourlySalesRecord objects
            records = []
            for hour, data in sorted(hourly.items()):
                record = HourlySalesRecord(
                    date=target_date,
                    hour=hour,
                    total_revenue=data["total_revenue"],
                    total_items=data["total_items"],
                    total_covers=data["total_covers"],
                    void_amount=data["void_amount"],
                    transactions=data["transactions"],
                    food_revenue=data["food_revenue"],
                    beverage_revenue=data["beverage_revenue"],
                    other_revenue=data["other_revenue"],
                )
                records.append(record)

            return records

        except Exception as e:
            logger.error(f"Lightspeed sales fetch failed: {e}")
            return []

    def _is_valid_order(self, order: dict[str, Any]) -> bool:
        """Check if an order record is valid and should be included."""
        if not order:
            return False
        # Exclude deleted orders
        if order.get("deleted"):
            return False
        return True

    def _map_category(self, category_name: str) -> str:
        """Map Lightspeed category to RosterIQ department."""
        if not category_name:
            return "other"
        normalized = category_name.lower().strip()
        return self.CATEGORY_MAPPING.get(normalized, "other")

    def _calculate_signal_strength(self, snapshot: HourlySalesRecord) -> SignalStrength:
        """
        Map sales metrics to signal strength.

        Args:
            snapshot: Hourly sales record.

        Returns:
            SignalStrength enum.
        """
        if snapshot.void_rate > 0.10:
            return SignalStrength.moderate_negative

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

    def _calculate_confidence(self, snapshot: HourlySalesRecord) -> float:
        """Calculate confidence in the signal."""
        confidence = 0.7

        if snapshot.transactions > 50:
            confidence += 0.2
        elif snapshot.transactions > 20:
            confidence += 0.1

        if snapshot.void_rate < 0.05:
            confidence += 0.1

        return min(1.0, confidence)

    async def is_available(self) -> bool:
        """Check if Lightspeed API is reachable with stored credentials."""
        if not self.client_id or not self.client_secret or not self.refresh_token:
            return False

        try:
            await self._refresh_token_if_needed()
            return True
        except Exception as e:
            logger.debug(f"Lightspeed health check failed: {e}")
            return False

    async def validate_credentials(self) -> bool:
        """
        Validate that the provided OAuth credentials actually work.

        Makes a lightweight API call (GET /Account.json) to verify the
        token grants real access.

        Returns:
            True if credentials are valid and API is accessible.
        """
        if not self.client_id or not self.client_secret or not self.refresh_token:
            return False

        try:
            token = await self._refresh_token_if_needed()
            await self._wait_rate_limit()
            headers = {"Authorization": f"Bearer {token}"}
            response = await self.http_client.get(
                f"{self.base_url}.json",
                headers=headers,
            )
            response.raise_for_status()
            return True
        except httpx.ConnectError as e:
            logger.warning(f"Lightspeed credential validation connection failed: {e}")
            return False
        except httpx.TimeoutException as e:
            logger.warning(f"Lightspeed credential validation timed out: {e}")
            return False
        except httpx.HTTPStatusError as e:
            logger.warning(f"Lightspeed credential validation returned {e.response.status_code}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Lightspeed credential validation failed: {e}")
            return False

    async def refresh_token(self) -> str:
        """
        Force-refresh the OAuth2 access token.

        Clears the cached token and requests a new one using the
        stored refresh_token.

        Returns:
            New access token string.

        Raises:
            ValueError: If token refresh fails.
        """
        self._access_token = None
        self._token_expires_at = None
        return await self._refresh_token_if_needed()

    @staticmethod
    def parse_csv(csv_content: str) -> list[dict]:
        """
        Parse a Lightspeed Restaurant CSV export into normalised records.

        Expected columns: Date/Sale Date, Time/Sale Time, Total/Receipt Total,
        Items, Category.

        Returns:
            List of dicts with keys: date, hour, revenue, transactions,
            department_breakdown.
        """
        reader = csv.DictReader(io.StringIO(csv_content))
        if not reader.fieldnames:
            return []

        from rosteriq.pos_import import (
            LIGHTSPEED_RESTAURANT_COLUMN_MAP,
            _resolve_column,
            _parse_date,
            _parse_time,
            _parse_float,
            _parse_int,
        )

        header = list(reader.fieldnames)
        date_col = _resolve_column(header, LIGHTSPEED_RESTAURANT_COLUMN_MAP["date"])
        time_col = _resolve_column(header, LIGHTSPEED_RESTAURANT_COLUMN_MAP["time"])
        revenue_col = _resolve_column(header, LIGHTSPEED_RESTAURANT_COLUMN_MAP["revenue"])
        items_col = _resolve_column(header, LIGHTSPEED_RESTAURANT_COLUMN_MAP["items"])
        category_col = _resolve_column(header, LIGHTSPEED_RESTAURANT_COLUMN_MAP.get("category", []))

        aggregated: dict[tuple, dict] = {}

        for row_values in reader:
            row = list(row_values.values())
            if not row or all(c.strip() == "" for c in row):
                continue

            if date_col is None:
                continue
            row_date = _parse_date(row[date_col])
            if row_date is None:
                continue

            hour = 12
            if time_col is not None:
                parsed_hour = _parse_time(row[time_col])
                if parsed_hour is not None:
                    hour = parsed_hour

            revenue = _parse_float(row[revenue_col]) if revenue_col is not None else 0.0
            category = row[category_col].strip().lower() if category_col is not None and category_col < len(row) else "other"

            # Map category through the adapter's mapping
            dept = LightspeedAdapter.CATEGORY_MAPPING.get(category, "other")

            key = (row_date, hour)
            if key not in aggregated:
                aggregated[key] = {
                    "date": row_date.isoformat(),
                    "hour": hour,
                    "revenue": 0.0,
                    "transactions": 0,
                    "department_breakdown": {},
                }

            aggregated[key]["revenue"] += revenue
            aggregated[key]["transactions"] += 1

            breakdown = aggregated[key]["department_breakdown"]
            breakdown[dept] = breakdown.get(dept, 0.0) + revenue

        return sorted(aggregated.values(), key=lambda r: (r["date"], r["hour"]))

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
