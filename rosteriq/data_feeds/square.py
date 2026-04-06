"""
Square POS Integration Adapter for RosterIQ
============================================

Pulls sales data, order volumes, payment patterns, and trading metrics
from Square API to feed the RosterIQ variance engine.

Square API requires:
  - access_token:  OAuth 2.0 bearer token or API key
  - location_id:   Square location UUID
  - environment:   "production" (default) or "sandbox"

Endpoints used:
  - POST /v2/orders/search      → Search orders by location and date range
  - GET  /v2/payments           → List payments with filters
  - GET  /v2/locations          → Get business locations
  - GET  /v2/catalog/list       → Product catalog
  - GET  /v2/inventory/batch    → Batch inventory data

Money amounts are in smallest currency unit (cents for AUD).
Available in Australia with AUD pricing.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger("rosteriq.data_feeds.square")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SQUARE_DEFAULT_TIMEOUT = 30  # seconds
SQUARE_MAX_RETRIES = 3
SQUARE_RETRY_DELAY = 2  # seconds
SQUARE_PRODUCTION_URL = "https://connect.squareup.com/v2"
SQUARE_SANDBOX_URL = "https://connect.squareup.com/v2"  # Same base for demo

AU_TZ = timezone(timedelta(hours=10))  # AEST


class SquareError(Exception):
    """Base exception for Square adapter errors."""
    pass


class SquareAuthError(SquareError):
    """Authentication failed."""
    pass


class SquareRateLimitError(SquareError):
    """Rate limit exceeded."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class SquareCredentials:
    """Square API credentials."""
    access_token: str
    location_id: str
    environment: str = "production"

    def __post_init__(self):
        if self.environment not in ("production", "sandbox"):
            self.environment = "production"


@dataclass
class OrderSnapshot:
    """Point-in-time order data snapshot."""
    timestamp: datetime
    location_id: str
    location_name: str
    total_revenue: float  # in dollars (converted from cents)
    order_count: int
    avg_order_value: float
    completed_orders: int = 0
    open_orders: int = 0
    canceled_orders: int = 0
    hourly_breakdown: dict[int, float] = field(default_factory=dict)  # hour → revenue
    payment_methods: dict[str, float] = field(default_factory=dict)  # method → revenue


@dataclass
class TradingPattern:
    """Analysed trading pattern for demand forecasting."""
    day_of_week: int  # 0=Monday
    hour: int  # 0-23
    avg_revenue: float
    avg_orders: int
    volatility: float  # std dev as % of mean
    trend: float  # positive = growing, negative = declining


@dataclass
class LocationInfo:
    """Square location mapping."""
    location_id: str
    location_name: str
    currency: str = "AUD"
    timezone: str = "Australia/Sydney"
    is_active: bool = True


class DemandSignal(Enum):
    """Demand signal strength derived from order data."""
    SURGE = "surge"          # >30% above average
    HIGH = "high"            # 10-30% above average
    NORMAL = "normal"        # within 10% of average
    LOW = "low"              # 10-30% below average
    QUIET = "quiet"          # >30% below average


DEMAND_MULTIPLIERS = {
    DemandSignal.SURGE: 0.35,
    DemandSignal.HIGH: 0.15,
    DemandSignal.NORMAL: 0.0,
    DemandSignal.LOW: -0.15,
    DemandSignal.QUIET: -0.30,
}


# ---------------------------------------------------------------------------
# Square API Client
# ---------------------------------------------------------------------------

class SquareClient:
    """Low-level async client for Square REST API."""

    def __init__(self, credentials: SquareCredentials):
        self.creds = credentials
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            base_url = SQUARE_PRODUCTION_URL
            self._client = httpx.AsyncClient(
                base_url=base_url,
                timeout=SQUARE_DEFAULT_TIMEOUT,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.creds.access_token}",
                    "Square-Version": "2024-04-18",
                },
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make authenticated API request with retries and pagination support."""
        client = await self._get_client()

        for attempt in range(SQUARE_MAX_RETRIES):
            try:
                resp = await client.request(method, path, **kwargs)

                if resp.status_code == 401:
                    raise SquareAuthError("Invalid Square access token")

                if resp.status_code == 403:
                    raise SquareAuthError("Insufficient permissions for Square API")

                if resp.status_code == 429:
                    wait = SQUARE_RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Square rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code in (401, 403):
                    raise SquareAuthError(f"Auth failed: {e.response.status_code}") from e
                if e.response.status_code == 429:
                    continue
                raise SquareError(f"Request failed: {e.response.status_code}") from e
            except httpx.RequestError as e:
                if attempt == SQUARE_MAX_RETRIES - 1:
                    raise SquareError(f"Request failed after {SQUARE_MAX_RETRIES} attempts: {e}") from e
                await asyncio.sleep(SQUARE_RETRY_DELAY)

        raise SquareError("Max retries exceeded")

    async def get(self, path: str, params: dict = None) -> dict:
        """Make GET request."""
        return await self._request("GET", path, params=params)

    async def post(self, path: str, json: dict = None) -> dict:
        """Make POST request."""
        return await self._request("POST", path, json=json)

    # ----- Convenience endpoints -----

    async def get_locations(self) -> list[dict]:
        """Get all Square locations."""
        data = await self.get("/locations")
        return data.get("locations", [])

    async def search_orders(
        self,
        location_id: str,
        from_timestamp: datetime,
        to_timestamp: datetime,
        cursor: str = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Search orders for a location and date range.
        Returns (orders, next_cursor) tuple for pagination.
        """
        payload = {
            "location_ids": [location_id],
            "query": {
                "filter": {
                    "date_time_filter": {
                        "created_at": {
                            "start_at": from_timestamp.isoformat(),
                            "end_at": to_timestamp.isoformat(),
                        }
                    }
                },
                "sort": {"sort_field": "CREATED_AT", "sort_order": "DESC"},
            },
            "limit": 100,
        }

        if cursor:
            payload["cursor"] = cursor

        data = await self.post("/orders/search", json=payload)
        orders = data.get("orders", [])
        next_cursor = data.get("cursor")

        return orders, next_cursor

    async def get_payments(
        self,
        location_id: str,
        from_timestamp: datetime = None,
        to_timestamp: datetime = None,
        cursor: str = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Get payments for a location.
        Returns (payments, next_cursor) tuple for pagination.
        """
        params = {
            "location_id": location_id,
            "limit": 100,
            "sort_order": "DESC",
        }

        if from_timestamp:
            params["begin_created_at"] = from_timestamp.isoformat()
        if to_timestamp:
            params["end_created_at"] = to_timestamp.isoformat()
        if cursor:
            params["cursor"] = cursor

        data = await self.get("/payments", params=params)
        payments = data.get("payments", [])
        next_cursor = data.get("cursor")

        return payments, next_cursor

    async def get_catalog(self) -> list[dict]:
        """Get product catalog."""
        data = await self.get("/catalog/list")
        return data.get("objects", [])


# ---------------------------------------------------------------------------
# Order Data Analyser
# ---------------------------------------------------------------------------

class OrderAnalyser:
    """Analyses Square order data to derive demand signals."""

    def __init__(self, lookback_weeks: int = 8):
        self.lookback_weeks = lookback_weeks
        self._historical_patterns: dict[str, list[TradingPattern]] = {}

    def build_order_snapshot(
        self,
        location_id: str,
        location_name: str,
        orders: list[dict],
        timestamp: datetime = None,
    ) -> OrderSnapshot:
        """Build an OrderSnapshot from raw order data."""
        if timestamp is None:
            timestamp = datetime.now(AU_TZ)

        total_revenue = 0.0
        completed_orders = 0
        open_orders = 0
        canceled_orders = 0
        hourly_totals: dict[int, float] = {}
        payment_methods: dict[str, float] = {}

        for order in orders:
            # Extract state
            state = order.get("state", "OPEN")
            if state == "COMPLETED":
                completed_orders += 1
            elif state == "OPEN":
                open_orders += 1
            elif state == "CANCELED":
                canceled_orders += 1

            # Extract total (in cents, convert to dollars)
            total_money = order.get("total_money", {})
            amount_cents = int(total_money.get("amount", 0) or 0)
            amount_dollars = amount_cents / 100.0
            total_revenue += amount_dollars

            # Hourly breakdown
            created_at = order.get("created_at", "")
            if created_at:
                try:
                    if isinstance(created_at, str):
                        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    else:
                        dt = created_at
                    hour = dt.hour
                    hourly_totals[hour] = hourly_totals.get(hour, 0) + amount_dollars
                except (ValueError, AttributeError):
                    pass

            # Payment method tracking
            tenders = order.get("tenders", [])
            for tender in tenders:
                tender_type = tender.get("type", "UNKNOWN")
                tender_amount = tender.get("amount_money", {})
                tender_cents = int(tender_amount.get("amount", 0) or 0)
                tender_dollars = tender_cents / 100.0
                payment_methods[tender_type] = payment_methods.get(tender_type, 0) + tender_dollars

        order_count = completed_orders + open_orders
        avg_value = total_revenue / max(order_count, 1)

        return OrderSnapshot(
            timestamp=timestamp,
            location_id=location_id,
            location_name=location_name,
            total_revenue=round(total_revenue, 2),
            order_count=order_count,
            avg_order_value=round(avg_value, 2),
            completed_orders=completed_orders,
            open_orders=open_orders,
            canceled_orders=canceled_orders,
            hourly_breakdown=hourly_totals,
            payment_methods=payment_methods,
        )

    def analyse_trading_patterns(
        self,
        historical_snapshots: list[OrderSnapshot],
    ) -> list[TradingPattern]:
        """Analyse historical snapshots to derive trading patterns."""
        # Group by day_of_week + hour
        buckets: dict[tuple[int, int], list[OrderSnapshot]] = {}

        for snap in historical_snapshots:
            dow = snap.timestamp.weekday()
            for hour, revenue in snap.hourly_breakdown.items():
                key = (dow, hour)
                if key not in buckets:
                    buckets[key] = []
                buckets[key].append(snap)

        patterns = []
        for (dow, hour), snaps in sorted(buckets.items()):
            revenues = []
            order_counts = []

            for s in snaps:
                rev = s.hourly_breakdown.get(hour, 0)
                revenues.append(rev)
                # Estimate hourly orders as proportion of daily total
                if s.total_revenue > 0:
                    hour_pct = rev / s.total_revenue
                    order_counts.append(int(s.order_count * hour_pct))
                else:
                    order_counts.append(0)

            avg_rev = sum(revenues) / max(len(revenues), 1)
            avg_ord = sum(order_counts) // max(len(order_counts), 1)

            # Calculate volatility
            if avg_rev > 0 and len(revenues) > 1:
                variance = sum((r - avg_rev) ** 2 for r in revenues) / len(revenues)
                std_dev = variance ** 0.5
                volatility = round(std_dev / avg_rev * 100, 1)
            else:
                volatility = 0.0

            # Calculate trend (simple linear regression slope)
            n = len(revenues)
            if n >= 4:
                x_mean = (n - 1) / 2
                y_mean = avg_rev
                num = sum((i - x_mean) * (revenues[i] - y_mean) for i in range(n))
                den = sum((i - x_mean) ** 2 for i in range(n))
                slope = num / max(den, 0.001)
                trend = round(slope / max(avg_rev, 0.01) * 100, 2)
            else:
                trend = 0.0

            patterns.append(TradingPattern(
                day_of_week=dow,
                hour=hour,
                avg_revenue=round(avg_rev, 2),
                avg_orders=avg_ord,
                volatility=volatility,
                trend=trend,
            ))

        return patterns

    def get_demand_signal(
        self,
        current_snapshot: OrderSnapshot,
        patterns: list[TradingPattern],
    ) -> tuple[DemandSignal, float]:
        """
        Compare current trading against historical patterns to derive
        a demand signal and multiplier for the variance engine.

        Returns (signal, multiplier) tuple.
        """
        now = current_snapshot.timestamp
        dow = now.weekday()
        hour = now.hour

        # Find matching pattern
        matching = [p for p in patterns if p.day_of_week == dow and p.hour == hour]
        if not matching:
            return DemandSignal.NORMAL, 0.0

        pattern = matching[0]
        if pattern.avg_revenue == 0:
            return DemandSignal.NORMAL, 0.0

        # Compare current hourly revenue to historical average
        current_hourly = current_snapshot.hourly_breakdown.get(hour, 0)
        deviation = (current_hourly - pattern.avg_revenue) / pattern.avg_revenue

        if deviation > 0.30:
            signal = DemandSignal.SURGE
        elif deviation > 0.10:
            signal = DemandSignal.HIGH
        elif deviation > -0.10:
            signal = DemandSignal.NORMAL
        elif deviation > -0.30:
            signal = DemandSignal.LOW
        else:
            signal = DemandSignal.QUIET

        return signal, DEMAND_MULTIPLIERS[signal]


# ---------------------------------------------------------------------------
# Square Data Feed Adapter (integrates with RosterIQ variance engine)
# ---------------------------------------------------------------------------

class SquareAdapter:
    """
    High-level adapter that connects Square order data to the
    RosterIQ variance engine via FeedSignal interface.

    Usage:
        adapter = SquareAdapter(credentials, location_id="loc_abc123")
        signals = await adapter.fetch_signals()
        # signals are ready to feed into SignalAggregator
    """

    FEED_CATEGORY = "pos_sales"
    SIGNAL_TYPE = "foot_traffic"  # Maps to existing SignalType for demand

    def __init__(
        self,
        credentials: SquareCredentials,
        location_id: str,
        location_name: str = "Venue",
        lookback_weeks: int = 8,
        fetch_interval_minutes: int = 15,
    ):
        self.client = SquareClient(credentials)
        self.analyser = OrderAnalyser(lookback_weeks)
        self.location_id = location_id
        self.location_name = location_name
        self.lookback_weeks = lookback_weeks
        self.fetch_interval_minutes = fetch_interval_minutes
        self._patterns: list[TradingPattern] = []
        self._last_pattern_build: Optional[datetime] = None
        self._cache: dict[str, Any] = {}

    async def initialise(self):
        """
        Build historical trading patterns. Call once on startup,
        then periodically (e.g. daily) to refresh.
        """
        logger.info(f"Building trading patterns for {self.location_name} ({self.location_id})")
        now = datetime.now(AU_TZ)
        snapshots = []

        # Fetch last N weeks of daily data
        for week in range(self.lookback_weeks):
            for day in range(7):
                target_date = now - timedelta(weeks=week, days=day)
                start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end = target_date.replace(hour=23, minute=59, second=59, microsecond=0)

                try:
                    orders, _ = await self.client.search_orders(
                        self.location_id, start, end
                    )
                    if orders:
                        snap = self.analyser.build_order_snapshot(
                            self.location_id, self.location_name,
                            orders, start,
                        )
                        snapshots.append(snap)
                except SquareError as e:
                    logger.warning(f"Failed to fetch {target_date.date()}: {e}")
                    continue

                # Small delay to avoid rate limits
                await asyncio.sleep(0.2)

        self._patterns = self.analyser.analyse_trading_patterns(snapshots)
        self._last_pattern_build = now
        logger.info(f"Built {len(self._patterns)} trading patterns from {len(snapshots)} snapshots")

    async def fetch_signals(self) -> list[dict]:
        """
        Fetch current order data and return signals compatible with
        the RosterIQ FeedSignal format.

        Returns list of signal dicts with keys:
            - signal_type: str
            - category: str
            - value: float (multiplier -1.0 to 1.0)
            - confidence: float (0.0 to 1.0)
            - source: str
            - timestamp: str (ISO format)
            - metadata: dict
        """
        now = datetime.now(AU_TZ)

        # Rebuild patterns daily
        if (
            not self._patterns
            or not self._last_pattern_build
            or (now - self._last_pattern_build) > timedelta(hours=24)
        ):
            await self.initialise()

        # Fetch current period orders
        period_start = now - timedelta(minutes=self.fetch_interval_minutes)
        try:
            orders, _ = await self.client.search_orders(
                self.location_id, period_start, now
            )
        except SquareError as e:
            logger.error(f"Failed to fetch current orders: {e}")
            return []

        if not orders:
            return []

        # Build current snapshot
        snapshot = self.analyser.build_order_snapshot(
            self.location_id, self.location_name, orders, now
        )

        # Get demand signal
        signal, multiplier = self.analyser.get_demand_signal(snapshot, self._patterns)

        # Calculate confidence based on data quality
        confidence = self._calculate_confidence(snapshot)

        signals = []

        # Primary demand signal
        signals.append({
            "signal_type": self.SIGNAL_TYPE,
            "category": self.FEED_CATEGORY,
            "value": multiplier,
            "confidence": confidence,
            "source": "square",
            "timestamp": now.isoformat(),
            "metadata": {
                "demand_level": signal.value,
                "revenue": snapshot.total_revenue,
                "orders": snapshot.order_count,
                "completed_orders": snapshot.completed_orders,
                "avg_order_value": snapshot.avg_order_value,
                "location": self.location_name,
                "period_minutes": self.fetch_interval_minutes,
            },
        })

        # Payment method signal (detects shift in payment patterns)
        payment_signal = self._analyse_payment_shift(snapshot)
        if payment_signal:
            signals.append(payment_signal)

        # Order velocity signal (orders per minute)
        velocity_signal = self._analyse_velocity(snapshot)
        if velocity_signal:
            signals.append(velocity_signal)

        # Average order value signal
        aov_signal = self._analyse_avg_order_value(snapshot)
        if aov_signal:
            signals.append(aov_signal)

        return signals

    def _calculate_confidence(self, snapshot: OrderSnapshot) -> float:
        """Calculate confidence score based on data completeness."""
        score = 0.5  # base

        if snapshot.order_count > 0:
            score += 0.2
        if snapshot.completed_orders > 0:
            score += 0.1
        if len(snapshot.payment_methods) > 1:
            score += 0.1
        if len(self._patterns) > 50:
            score += 0.1

        return min(round(score, 2), 1.0)

    def _analyse_payment_shift(self, snapshot: OrderSnapshot) -> Optional[dict]:
        """
        Detect unusual shifts in payment method mix that might
        indicate a different type of trading (e.g. event vs regular).
        """
        if not snapshot.payment_methods or snapshot.total_revenue == 0:
            return None

        # Check if card-heavy (indicates online/event crowd vs cash bar)
        card_types = {"CARD", "CREDIT_CARD", "DEBIT_CARD"}
        card_revenue = sum(
            v for k, v in snapshot.payment_methods.items()
            if k in card_types
        )

        card_ratio = card_revenue / snapshot.total_revenue

        # Signal if card-heavy (event/online crowd)
        if card_ratio > 0.8:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_payment_mix",
                "value": 0.05,
                "confidence": 0.6,
                "source": "square_payment",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "card_ratio": round(card_ratio, 2),
                    "pattern": "card_heavy",
                    "staffing_hint": "Likely online/event orders — check order management systems",
                },
            }

        return None

    def _analyse_velocity(self, snapshot: OrderSnapshot) -> Optional[dict]:
        """
        Analyse order velocity (orders per minute) to detect
        rush periods that might need immediate staffing response.
        """
        if snapshot.order_count == 0:
            return None

        velocity = snapshot.order_count / max(self.fetch_interval_minutes, 1)

        # Find historical average velocity for this time
        now = snapshot.timestamp
        dow = now.weekday()
        hour = now.hour

        matching = [
            p for p in self._patterns
            if p.day_of_week == dow and p.hour == hour
        ]

        if not matching:
            return None

        avg_ord_per_min = matching[0].avg_orders / 60.0
        if avg_ord_per_min == 0:
            return None

        velocity_ratio = velocity / avg_ord_per_min

        if velocity_ratio > 1.5:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_velocity",
                "value": 0.2,
                "confidence": 0.7,
                "source": "square_velocity",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "orders_per_minute": round(velocity, 2),
                    "avg_orders_per_minute": round(avg_ord_per_min, 2),
                    "velocity_ratio": round(velocity_ratio, 2),
                    "alert": "Order rush detected — consider calling in backup staff",
                },
            }

        return None

    def _analyse_avg_order_value(self, snapshot: OrderSnapshot) -> Optional[dict]:
        """
        Detect shifts in average order value that indicate
        customer spending pattern changes.
        """
        if snapshot.order_count == 0 or not self._patterns:
            return None

        # Find historical average order value for this time
        now = snapshot.timestamp
        dow = now.weekday()
        hour = now.hour

        matching = [
            p for p in self._patterns
            if p.day_of_week == dow and p.hour == hour
        ]

        if not matching:
            return None

        pattern = matching[0]
        if pattern.avg_revenue == 0 or pattern.avg_orders == 0:
            return None

        hist_avg_aov = pattern.avg_revenue / max(pattern.avg_orders, 1)
        if hist_avg_aov == 0:
            return None

        aov_ratio = snapshot.avg_order_value / hist_avg_aov
        deviation = aov_ratio - 1.0

        # Signal significant shift in order value
        if abs(deviation) > 0.2:  # >20% change
            direction = "increased" if deviation > 0 else "decreased"
            value_adjustment = 0.1 if deviation > 0 else -0.1

            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_avg_order_value",
                "value": value_adjustment,
                "confidence": 0.65,
                "source": "square_aov",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "current_aov": round(snapshot.avg_order_value, 2),
                    "historical_aov": round(hist_avg_aov, 2),
                    "aov_ratio": round(aov_ratio, 2),
                    "direction": direction,
                    "staffing_hint": "Customer spending patterns shifted — adjust service intensity",
                },
            }

        return None

    async def get_locations(self) -> list[LocationInfo]:
        """Get available Square locations for venue mapping."""
        try:
            raw = await self.client.get_locations()
            return [
                LocationInfo(
                    location_id=loc.get("id", ""),
                    location_name=loc.get("name", "Unknown"),
                    currency=loc.get("currency", "AUD"),
                    timezone=loc.get("timezone", "Australia/Sydney"),
                    is_active=loc.get("status") == "ACTIVE",
                )
                for loc in raw
            ]
        except SquareError as e:
            logger.error(f"Failed to fetch locations: {e}")
            return []

    async def health_check(self) -> dict:
        """Check Square API connectivity and auth."""
        try:
            locations = await self.client.get_locations()
            if not any(loc.get("id") == self.location_id for loc in locations):
                return {
                    "status": "location_not_found",
                    "connected": True,
                    "location_id": self.location_id,
                    "available_locations": len(locations),
                }

            return {
                "status": "healthy",
                "connected": True,
                "locations_found": len(locations),
                "location_id": self.location_id,
            }
        except SquareAuthError:
            return {"status": "auth_failed", "connected": False}
        except SquareError as e:
            return {"status": "error", "connected": False, "error": str(e)}

    async def close(self):
        """Close the HTTP client."""
        await self.client.close()


# ---------------------------------------------------------------------------
# Factory function for easy setup
# ---------------------------------------------------------------------------

def create_square_adapter(
    access_token: str,
    location_id: str,
    location_name: str = "Venue",
    environment: str = "production",
) -> SquareAdapter:
    """
    Factory function to create a Square adapter.

    Example:
        adapter = create_square_adapter(
            access_token="sq_live_abc123xyz789",
            location_id="LNHCCCCAAA111BBB",
            location_name="The Royal Oak",
            environment="production",
        )
        signals = await adapter.fetch_signals()
    """
    creds = SquareCredentials(
        access_token=access_token,
        location_id=location_id,
        environment=environment,
    )
    return SquareAdapter(
        credentials=creds,
        location_id=location_id,
        location_name=location_name,
    )
