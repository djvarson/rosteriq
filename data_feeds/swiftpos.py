"""
SwiftPOS integration adapter for RosterIQ.

SwiftPOS is a major Australian hospitality POS system. This adapter pulls
hourly sales data by department/category and provides demand signals for
the forecasting engine.

Supports:
- Hourly sales data by department (food, beverage, gaming, etc.)
- Product mix analysis for demand pattern detection
- Void/refund tracking as operational health indicators
- OAuth2 client credentials authentication
- Mapping to FeedSignal format for ensemble forecasting
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
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
# Rate limiter
# ============================================================================


class SwiftPOSRateLimiter:
    """
    Sliding window rate limiter for SwiftPOS API.

    SwiftPOS allows approximately 60 requests per 60-second window.
    """

    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: list[float] = []

    async def acquire(self) -> None:
        """Wait if necessary to stay within rate limits."""
        self._clean_old_requests()
        if len(self.requests) >= self.max_requests:
            oldest = self.requests[0]
            wait_time = self.window_seconds - (time.time() - oldest)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._clean_old_requests()
        self.requests.append(time.time())

    def _clean_old_requests(self) -> None:
        """Remove request timestamps outside the current window."""
        cutoff = time.time() - self.window_seconds
        self.requests = [t for t in self.requests if t > cutoff]


# ============================================================================
# Data classes
# ============================================================================


@dataclass
class SalesSnapshot:
    """
    Point-in-time snapshot of SwiftPOS sales data for a specific date/hour.

    Captures raw sales metrics used to generate demand signals.
    """

    date: date
    hour: Optional[int] = None  # None = all-day aggregate
    total_revenue: float = 0.0  # Total sales (AUD)
    total_items: int = 0  # Total items sold
    total_covers: float = 0.0  # Estimated covers (if available)
    void_amount: float = 0.0  # Voids and refunds (AUD)
    void_rate: float = 0.0  # Void amount / total revenue
    avg_transaction_value: float = 0.0  # Revenue per transaction
    transactions: int = 0  # Number of transactions

    # Department breakdown
    food_revenue: float = 0.0
    beverage_revenue: float = 0.0
    gaming_revenue: float = 0.0
    other_revenue: float = 0.0

    def __post_init__(self) -> None:
        """Calculate derived metrics on instantiation."""
        if self.transactions > 0:
            self.avg_transaction_value = self.total_revenue / self.transactions
        if self.total_revenue > 0:
            self.void_rate = self.void_amount / self.total_revenue


# ============================================================================
# SwiftPOS Adapter
# ============================================================================


class SwiftPOSAdapter(DataFeedAdapter):
    """
    Adapter for SwiftPOS, a major Australian hospitality POS system.

    SwiftPOS API provides transaction-level and aggregated sales data.
    This adapter maps hourly sales patterns into demand signals.

    OAuth2 Authentication:
        - client_id: SwiftPOS application ID
        - client_secret: SwiftPOS application secret
        - Tokens obtained via OAuth2 client credentials flow
    """

    category = FeedCategory.reservations  # Use reservations category for covers
    source_name = "swiftpos"
    base_url = "https://api.swiftpos.com.au/v2"

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        cache: Optional[SignalCache] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        """
        Initialize SwiftPOS adapter.

        Args:
            client_id: OAuth2 client ID.
            client_secret: OAuth2 client secret.
            cache: Signal cache instance.
            http_client: Reusable httpx AsyncClient for pooled connections.
        """
        super().__init__(api_key=client_id, cache=cache or SignalCache(default_ttl_minutes=5))
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._http_client = http_client
        self._owns_client = http_client is None
        self._rate_limiter = SwiftPOSRateLimiter()

    async def __aenter__(self) -> SwiftPOSAdapter:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit: clean up client if we created it."""
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize or return existing httpx client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            self._owns_client = True
        return self._http_client

    async def _get_access_token(self) -> str:
        """
        Obtain or refresh OAuth2 access token.

        Returns:
            Access token string.

        Raises:
            ValueError: If authentication fails.
        """
        if self._access_token and self._token_expires_at and datetime.utcnow() < self._token_expires_at:
            return self._access_token

        if not self.client_id or not self.client_secret:
            raise ValueError("SwiftPOS client_id and client_secret required")

        url = f"{self.base_url}/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        await self._rate_limiter.acquire()

        try:
            response = await self.http_client.post(url, data=data)
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)
            return self._access_token
        except httpx.ConnectError as e:
            logger.error(f"SwiftPOS connection failed: {e}")
            raise ValueError(f"Cannot connect to SwiftPOS: {e}")
        except httpx.TimeoutException as e:
            logger.error(f"SwiftPOS token request timed out: {e}")
            raise ValueError(f"SwiftPOS token request timed out: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"SwiftPOS OAuth2 token request returned {e.response.status_code}: {e}")
            raise ValueError(f"Failed to obtain SwiftPOS access token: {e}")
        except httpx.HTTPError as e:
            logger.error(f"SwiftPOS OAuth2 token request failed: {e}")
            raise ValueError(f"Failed to obtain SwiftPOS access token: {e}")

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch sales signals from SwiftPOS.

        Creates hourly demand signals based on sales trends across departments.

        Args:
            location: Venue location.
            start_date: First date to fetch.
            end_date: Last date to fetch (inclusive).
            venue_id: SwiftPOS venue/terminal ID.

        Returns:
            List of FeedSignal objects with hourly breakdowns.
        """
        if not venue_id:
            logger.warning("SwiftPOS requires venue_id; skipping fetch")
            return []

        signals = []
        current_date = start_date

        while current_date <= end_date:
            try:
                # Fetch hourly sales data
                hourly_data = await self._fetch_hourly_sales(venue_id, current_date)

                if not hourly_data:
                    current_date += timedelta(days=1)
                    continue

                # Process each hour
                for hour, sales_data in enumerate(hourly_data):
                    snapshot = self._process_sales(sales_data, current_date, hour)

                    # Skip hours with no activity
                    if snapshot.total_revenue == 0:
                        continue

                    # Map void rate to signal (high voids = operational issues = lower demand confidence)
                    strength = self._calculate_signal_strength(snapshot)
                    description = (
                        f"SwiftPOS: {snapshot.total_items} items, "
                        f"${snapshot.total_revenue:.2f} revenue, "
                        f"void rate {snapshot.void_rate*100:.1f}%"
                    )

                    signal = self._make_signal(
                        signal_date=current_date,
                        strength=strength,
                        description=description,
                        confidence=self._calculate_confidence(snapshot),
                        hour=hour,
                        raw_data={"snapshot": vars(snapshot)},
                        venue_id=venue_id,
                        ttl_minutes=5,
                    )
                    signals.append(signal)

            except Exception as e:
                logger.error(f"SwiftPOS fetch failed for {venue_id} on {current_date}: {e}")

            current_date += timedelta(days=1)

        return signals

    async def _fetch_hourly_sales(
        self,
        venue_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """
        Fetch hourly sales breakdown from SwiftPOS.

        Args:
            venue_id: Venue/terminal ID.
            target_date: Date to fetch sales for.

        Returns:
            List of sales dicts, one per hour (24 entries for a full day).
        """
        token = await self._get_access_token()
        url = f"{self.base_url}/venues/{venue_id}/sales/hourly"
        params = {
            "date": target_date.isoformat(),
        }
        headers = {"Authorization": f"Bearer {token}"}

        await self._rate_limiter.acquire()

        try:
            response = await self.http_client.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get("hourly", [])
        except httpx.ConnectError as e:
            logger.warning(f"SwiftPOS connection failed for {venue_id}: {e}")
            return []
        except httpx.TimeoutException as e:
            logger.warning(f"SwiftPOS request timed out for {venue_id}: {e}")
            return []
        except httpx.HTTPStatusError as e:
            logger.warning(f"SwiftPOS API returned {e.response.status_code} for {venue_id}: {e}")
            return []
        except httpx.HTTPError as e:
            logger.error(f"SwiftPOS API error for {venue_id}: {e}")
            return []

    def _process_sales(
        self,
        sales_data: dict[str, Any],
        sale_date: date,
        hour: int,
    ) -> SalesSnapshot:
        """
        Process raw SwiftPOS sales data into a SalesSnapshot.

        Args:
            sales_data: Sales data dict from API.
            sale_date: Date of the sales.
            hour: Hour (0-23).

        Returns:
            SalesSnapshot with aggregated metrics.
        """
        snapshot = SalesSnapshot(date=sale_date, hour=hour)

        # Top-level revenue
        snapshot.total_revenue = float(sales_data.get("total_sales", 0.0))
        snapshot.total_items = int(sales_data.get("total_items", 0))
        snapshot.transactions = int(sales_data.get("num_transactions", 0))
        snapshot.void_amount = float(sales_data.get("void_amount", 0.0))
        snapshot.total_covers = float(sales_data.get("covers", 0.0))

        # Department breakdown
        departments = sales_data.get("departments", {})
        snapshot.food_revenue = float(departments.get("food", {}).get("revenue", 0.0))
        snapshot.beverage_revenue = float(departments.get("beverage", {}).get("revenue", 0.0))
        snapshot.gaming_revenue = float(departments.get("gaming", {}).get("revenue", 0.0))
        snapshot.other_revenue = float(departments.get("other", {}).get("revenue", 0.0))

        return snapshot

    def _calculate_signal_strength(self, snapshot: SalesSnapshot) -> SignalStrength:
        """
        Map sales metrics to signal strength.

        High revenue and low void rate = stronger demand signal.
        High void rate suggests operational issues = lower confidence.

        Args:
            snapshot: Sales snapshot.

        Returns:
            SignalStrength enum value.
        """
        # Void rate over 10% is concerning (operational issue)
        if snapshot.void_rate > 0.10:
            return SignalStrength.moderate_negative

        # Revenue proxy for demand
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

    def _calculate_confidence(self, snapshot: SalesSnapshot) -> float:
        """
        Calculate confidence in the signal based on data completeness.

        Args:
            snapshot: Sales snapshot.

        Returns:
            Confidence score (0.0 to 1.0).
        """
        confidence = 0.7  # Base confidence

        # More transactions = higher confidence
        if snapshot.transactions > 50:
            confidence += 0.2
        elif snapshot.transactions > 20:
            confidence += 0.1

        # Low void rate increases confidence
        if snapshot.void_rate < 0.05:
            confidence += 0.1

        return min(1.0, confidence)

    async def is_available(self) -> bool:
        """Check if SwiftPOS API is reachable with stored credentials."""
        if not self.client_id or not self.client_secret:
            return False

        try:
            await self._get_access_token()
            return True
        except Exception as e:
            logger.debug(f"SwiftPOS health check failed: {e}")
            return False

    async def validate_credentials(self) -> bool:
        """
        Validate that the provided OAuth credentials actually work.

        Makes a lightweight API call to verify authentication beyond
        just obtaining a token.

        Returns:
            True if credentials are valid and API is accessible.
        """
        if not self.client_id or not self.client_secret:
            return False

        try:
            token = await self._get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            await self._rate_limiter.acquire()
            response = await self.http_client.get(
                f"{self.base_url}/me",
                headers=headers,
            )
            response.raise_for_status()
            return True
        except httpx.ConnectError as e:
            logger.warning(f"SwiftPOS credential validation connection failed: {e}")
            return False
        except httpx.TimeoutException as e:
            logger.warning(f"SwiftPOS credential validation timed out: {e}")
            return False
        except httpx.HTTPStatusError as e:
            logger.warning(f"SwiftPOS credential validation returned {e.response.status_code}: {e}")
            return False
        except Exception as e:
            logger.warning(f"SwiftPOS credential validation failed: {e}")
            return False

    async def refresh_token(self) -> str:
        """
        Force-refresh the OAuth2 access token.

        SwiftPOS uses client_credentials flow, so this re-authenticates
        with the client_id/client_secret pair.

        Returns:
            New access token string.

        Raises:
            ValueError: If re-authentication fails.
        """
        self._access_token = None
        self._token_expires_at = None
        return await self._get_access_token()

    @staticmethod
    def parse_csv(csv_content: str) -> list[dict]:
        """
        Parse a SwiftPOS CSV export into normalised records.

        Expected columns: Date, Time, Department, Items Sold,
        Gross Sales, Voids/Refunds.

        Returns:
            List of dicts with keys: date, hour, revenue, transactions,
            department_breakdown.
        """
        reader = csv.DictReader(io.StringIO(csv_content))
        if not reader.fieldnames:
            return []

        # Resolve column names (flexible matching)
        from rosteriq.pos_import import (
            SWIFTPOS_COLUMN_MAP,
            _resolve_column,
            _parse_date,
            _parse_time,
            _parse_float,
            _parse_int,
        )

        header = list(reader.fieldnames)
        date_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP["date"])
        time_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP["time"])
        revenue_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP["revenue"])
        items_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP["items"])
        dept_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP.get("department", []))
        voids_col = _resolve_column(header, SWIFTPOS_COLUMN_MAP.get("voids", []))

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
            items = _parse_int(row[items_col]) if items_col is not None else 0
            dept = row[dept_col].strip() if dept_col is not None and dept_col < len(row) else "other"
            void_amt = _parse_float(row[voids_col]) if voids_col is not None else 0.0

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

            dept_lower = dept.lower()
            breakdown = aggregated[key]["department_breakdown"]
            breakdown[dept_lower] = breakdown.get(dept_lower, 0.0) + revenue

        return sorted(aggregated.values(), key=lambda r: (r["date"], r["hour"]))
