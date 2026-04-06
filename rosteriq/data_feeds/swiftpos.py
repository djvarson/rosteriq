"""
SwiftPOS POS Integration Adapter for RosterIQ
===============================================

Pulls sales data, transaction volumes, product mix, and trading patterns
from SwiftPOS API to feed the RosterIQ variance engine.

SwiftPOS API requires:
  - api_url:     Base URL for the SwiftPOS Web API
  - clerk_id:    Clerk with API auth permissions
  - client_id:   Web API-enabled location ID
  - customer_id: SwiftPOS registration/customer reference number

Requires SwiftPOS version 10.58+ for API compatibility.

Endpoints used:
  - POST /api/authorize          → Get auth token
  - GET  /api/transactions       → Historical transaction data
  - GET  /api/sales/search       → Sales records search
  - GET  /api/locations           → Venue locations/departments
  - GET  /api/products/categories → Product category breakdown
  - GET  /api/members             → Loyalty member data
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger("rosteriq.data_feeds.swiftpos")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SWIFTPOS_DEFAULT_TIMEOUT = 30  # seconds
SWIFTPOS_MAX_RETRIES = 3
SWIFTPOS_RETRY_DELAY = 2  # seconds

AU_TZ = timezone(timedelta(hours=10))  # AEST


class SwiftPOSError(Exception):
    """Base exception for SwiftPOS adapter errors."""
    pass


class SwiftPOSAuthError(SwiftPOSError):
    """Authentication failed."""
    pass


class SwiftPOSRateLimitError(SwiftPOSError):
    """Rate limit exceeded."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class SwiftPOSCredentials:
    """SwiftPOS API credentials."""
    api_url: str
    clerk_id: str
    client_id: str
    customer_id: str

    def __post_init__(self):
        self.api_url = self.api_url.rstrip("/")


@dataclass
class SalesSnapshot:
    """Point-in-time sales data snapshot."""
    timestamp: datetime
    location_id: str
    location_name: str
    total_revenue: float
    transaction_count: int
    avg_transaction_value: float
    covers: int = 0  # number of customers/covers
    top_categories: dict[str, float] = field(default_factory=dict)
    hourly_breakdown: dict[int, float] = field(default_factory=dict)  # hour → revenue


@dataclass
class TradingPattern:
    """Analysed trading pattern for demand forecasting."""
    day_of_week: int  # 0=Monday
    hour: int  # 0-23
    avg_revenue: float
    avg_transactions: int
    avg_covers: int
    volatility: float  # std dev as % of mean
    trend: float  # positive = growing, negative = declining


@dataclass
class LocationDepartment:
    """SwiftPOS location/department mapping."""
    location_id: str
    location_name: str
    department_id: str
    department_name: str
    is_active: bool = True


class DemandSignal(Enum):
    """Demand signal strength derived from sales data."""
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
# Auth Token Cache
# ---------------------------------------------------------------------------

@dataclass
class _TokenCache:
    token: Optional[str] = None
    expires_at: Optional[datetime] = None

    @property
    def is_valid(self) -> bool:
        if not self.token or not self.expires_at:
            return False
        return datetime.now(timezone.utc) < self.expires_at - timedelta(minutes=2)


# ---------------------------------------------------------------------------
# SwiftPOS API Client
# ---------------------------------------------------------------------------

class SwiftPOSClient:
    """Low-level async client for SwiftPOS REST API."""

    def __init__(self, credentials: SwiftPOSCredentials):
        self.creds = credentials
        self._token_cache = _TokenCache()
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.creds.api_url,
                timeout=SWIFTPOS_DEFAULT_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _authenticate(self) -> str:
        """Authenticate and return bearer token."""
        if self._token_cache.is_valid:
            return self._token_cache.token

        client = await self._get_client()
        payload = {
            "clerkId": self.creds.clerk_id,
            "clientId": self.creds.client_id,
            "customerId": self.creds.customer_id,
        }

        try:
            resp = await client.post("/api/authorize", json=payload)
            if resp.status_code == 401:
                raise SwiftPOSAuthError("Invalid SwiftPOS credentials")
            if resp.status_code == 429:
                raise SwiftPOSRateLimitError("SwiftPOS rate limit exceeded")
            resp.raise_for_status()
            data = resp.json()

            self._token_cache.token = data.get("token") or data.get("accessToken")
            # Default to 1 hour expiry if not specified
            expires_in = data.get("expiresIn", 3600)
            self._token_cache.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            return self._token_cache.token

        except httpx.HTTPStatusError as e:
            raise SwiftPOSError(f"Auth failed: {e.response.status_code}") from e
        except httpx.RequestError as e:
            raise SwiftPOSError(f"Connection error: {e}") from e

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make authenticated API request with retries."""
        client = await self._get_client()
        token = await self._authenticate()

        for attempt in range(SWIFTPOS_MAX_RETRIES):
            try:
                resp = await client.request(
                    method,
                    path,
                    headers={"Authorization": f"Bearer {token}"},
                    **kwargs,
                )

                if resp.status_code == 401:
                    # Token expired, re-auth
                    self._token_cache.token = None
                    token = await self._authenticate()
                    continue

                if resp.status_code == 429:
                    wait = SWIFTPOS_RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except httpx.RequestError as e:
                if attempt == SWIFTPOS_MAX_RETRIES - 1:
                    raise SwiftPOSError(f"Request failed after {SWIFTPOS_MAX_RETRIES} attempts: {e}") from e
                await asyncio.sleep(SWIFTPOS_RETRY_DELAY)

        raise SwiftPOSError("Max retries exceeded")

    async def get(self, path: str, params: dict = None) -> dict:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, json: dict = None) -> dict:
        return await self._request("POST", path, json=json)

    # ----- Convenience endpoints -----

    async def get_locations(self) -> list[dict]:
        """Get all venue locations/departments."""
        data = await self.get("/api/locations")
        return data if isinstance(data, list) else data.get("locations", [])

    async def get_transactions(
        self,
        location_id: str,
        from_date: datetime,
        to_date: datetime,
    ) -> list[dict]:
        """Get transaction records for a location and date range."""
        params = {
            "locationId": location_id,
            "fromDate": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "toDate": to_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        data = await self.get("/api/transactions", params=params)
        return data if isinstance(data, list) else data.get("transactions", [])

    async def search_sales(
        self,
        location_id: str,
        from_date: datetime,
        to_date: datetime,
    ) -> list[dict]:
        """Search sales records."""
        payload = {
            "locationId": location_id,
            "fromDate": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "toDate": to_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        data = await self.post("/api/sales/search", json=payload)
        return data if isinstance(data, list) else data.get("sales", [])

    async def get_product_categories(self) -> list[dict]:
        """Get all product categories."""
        data = await self.get("/api/products/categories")
        return data if isinstance(data, list) else data.get("categories", [])

    async def get_members(self, search: str = None) -> list[dict]:
        """Get loyalty members."""
        params = {"search": search} if search else None
        data = await self.get("/api/members", params=params)
        return data if isinstance(data, list) else data.get("members", [])


# ---------------------------------------------------------------------------
# Sales Data Analyser
# ---------------------------------------------------------------------------

class SalesAnalyser:
    """Analyses SwiftPOS sales data to derive demand signals."""

    def __init__(self, lookback_weeks: int = 8):
        self.lookback_weeks = lookback_weeks
        self._historical_patterns: dict[str, list[TradingPattern]] = {}

    def build_sales_snapshot(
        self,
        location_id: str,
        location_name: str,
        transactions: list[dict],
        timestamp: datetime = None,
    ) -> SalesSnapshot:
        """Build a SalesSnapshot from raw transaction data."""
        if timestamp is None:
            timestamp = datetime.now(AU_TZ)

        total_revenue = 0.0
        transaction_count = len(transactions)
        covers = 0
        category_totals: dict[str, float] = {}
        hourly_totals: dict[int, float] = {}

        for txn in transactions:
            amount = float(txn.get("total", 0) or txn.get("amount", 0) or 0)
            total_revenue += amount

            # Extract covers/guests if available
            covers += int(txn.get("covers", 0) or txn.get("guests", 0) or 0)

            # Category breakdown
            category = txn.get("category", txn.get("productCategory", "Other"))
            if category:
                category_totals[category] = category_totals.get(category, 0) + amount

            # Hourly breakdown
            txn_time = txn.get("transactionTime", txn.get("dateTime", ""))
            if txn_time:
                try:
                    if isinstance(txn_time, str):
                        dt = datetime.fromisoformat(txn_time.replace("Z", "+00:00"))
                    else:
                        dt = txn_time
                    hour = dt.hour
                    hourly_totals[hour] = hourly_totals.get(hour, 0) + amount
                except (ValueError, AttributeError):
                    pass

        avg_value = total_revenue / max(transaction_count, 1)

        return SalesSnapshot(
            timestamp=timestamp,
            location_id=location_id,
            location_name=location_name,
            total_revenue=round(total_revenue, 2),
            transaction_count=transaction_count,
            avg_transaction_value=round(avg_value, 2),
            covers=covers,
            top_categories=dict(sorted(category_totals.items(), key=lambda x: -x[1])[:10]),
            hourly_breakdown=hourly_totals,
        )

    def analyse_trading_patterns(
        self,
        historical_snapshots: list[SalesSnapshot],
    ) -> list[TradingPattern]:
        """Analyse historical snapshots to derive trading patterns."""
        # Group by day_of_week + hour
        buckets: dict[tuple[int, int], list[SalesSnapshot]] = {}

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
            txn_counts = []
            cover_counts = []

            for s in snaps:
                rev = s.hourly_breakdown.get(hour, 0)
                revenues.append(rev)
                # Estimate hourly transactions as proportion of daily total
                if s.total_revenue > 0:
                    hour_pct = rev / s.total_revenue
                    txn_counts.append(int(s.transaction_count * hour_pct))
                    cover_counts.append(int(s.covers * hour_pct))
                else:
                    txn_counts.append(0)
                    cover_counts.append(0)

            avg_rev = sum(revenues) / max(len(revenues), 1)
            avg_txn = sum(txn_counts) // max(len(txn_counts), 1)
            avg_cov = sum(cover_counts) // max(len(cover_counts), 1)

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
                avg_transactions=avg_txn,
                avg_covers=avg_cov,
                volatility=volatility,
                trend=trend,
            ))

        return patterns

    def get_demand_signal(
        self,
        current_snapshot: SalesSnapshot,
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
# SwiftPOS Data Feed Adapter (integrates with RosterIQ variance engine)
# ---------------------------------------------------------------------------

class SwiftPOSAdapter:
    """
    High-level adapter that connects SwiftPOS sales data to the
    RosterIQ variance engine via FeedSignal interface.

    Usage:
        adapter = SwiftPOSAdapter(credentials, location_id="LOC001")
        signals = await adapter.fetch_signals()
        # signals are ready to feed into SignalAggregator
    """

    FEED_CATEGORY = "pos_sales"
    SIGNAL_TYPE = "foot_traffic"  # Maps to existing SignalType for demand

    def __init__(
        self,
        credentials: SwiftPOSCredentials,
        location_id: str,
        location_name: str = "Venue",
        lookback_weeks: int = 8,
        fetch_interval_minutes: int = 15,
    ):
        self.client = SwiftPOSClient(credentials)
        self.analyser = SalesAnalyser(lookback_weeks)
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
                    transactions = await self.client.get_transactions(
                        self.location_id, start, end
                    )
                    if transactions:
                        snap = self.analyser.build_sales_snapshot(
                            self.location_id, self.location_name,
                            transactions, start,
                        )
                        snapshots.append(snap)
                except SwiftPOSError as e:
                    logger.warning(f"Failed to fetch {target_date.date()}: {e}")
                    continue

                # Small delay to avoid rate limits
                await asyncio.sleep(0.2)

        self._patterns = self.analyser.analyse_trading_patterns(snapshots)
        self._last_pattern_build = now
        logger.info(f"Built {len(self._patterns)} trading patterns from {len(snapshots)} snapshots")

    async def fetch_signals(self) -> list[dict]:
        """
        Fetch current sales data and return signals compatible with
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

        # Fetch current period transactions
        period_start = now - timedelta(minutes=self.fetch_interval_minutes)
        try:
            transactions = await self.client.get_transactions(
                self.location_id, period_start, now
            )
        except SwiftPOSError as e:
            logger.error(f"Failed to fetch current transactions: {e}")
            return []

        if not transactions:
            return []

        # Build current snapshot
        snapshot = self.analyser.build_sales_snapshot(
            self.location_id, self.location_name, transactions, now
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
            "source": "swiftpos",
            "timestamp": now.isoformat(),
            "metadata": {
                "demand_level": signal.value,
                "revenue": snapshot.total_revenue,
                "transactions": snapshot.transaction_count,
                "avg_transaction": snapshot.avg_transaction_value,
                "covers": snapshot.covers,
                "location": self.location_name,
                "period_minutes": self.fetch_interval_minutes,
            },
        })

        # Category mix signal (identifies unusual product mix)
        cat_signal = self._analyse_category_shift(snapshot)
        if cat_signal:
            signals.append(cat_signal)

        # Trading velocity signal (transactions per minute)
        velocity_signal = self._analyse_velocity(snapshot)
        if velocity_signal:
            signals.append(velocity_signal)

        return signals

    def _calculate_confidence(self, snapshot: SalesSnapshot) -> float:
        """Calculate confidence score based on data completeness."""
        score = 0.5  # base

        if snapshot.transaction_count > 0:
            score += 0.2
        if snapshot.covers > 0:
            score += 0.1
        if len(snapshot.top_categories) > 2:
            score += 0.1
        if len(self._patterns) > 50:
            score += 0.1

        return min(round(score, 2), 1.0)

    def _analyse_category_shift(self, snapshot: SalesSnapshot) -> Optional[dict]:
        """
        Detect unusual shifts in product category mix that might
        indicate a different type of trading (e.g. event crowd vs regulars).
        """
        if not snapshot.top_categories or snapshot.total_revenue == 0:
            return None

        # Check if beverage-heavy (indicates bar/event crowd)
        beverage_keywords = {"beverage", "drink", "beer", "wine", "spirit", "cocktail", "bar"}
        food_keywords = {"food", "meal", "kitchen", "main", "entree", "dessert"}

        bev_revenue = sum(
            v for k, v in snapshot.top_categories.items()
            if any(bw in k.lower() for bw in beverage_keywords)
        )
        food_revenue = sum(
            v for k, v in snapshot.top_categories.items()
            if any(fw in k.lower() for fw in food_keywords)
        )

        total = bev_revenue + food_revenue
        if total == 0:
            return None

        bev_ratio = bev_revenue / total

        # Signal if beverage-heavy (event/bar crowd needs different staffing)
        if bev_ratio > 0.7:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_category_mix",
                "value": 0.1,  # Slight upward pressure — bar crowds need more floor staff
                "confidence": 0.6,
                "source": "swiftpos_category",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "beverage_ratio": round(bev_ratio, 2),
                    "pattern": "beverage_heavy",
                    "staffing_hint": "Prioritise bar and floor staff over kitchen",
                },
            }

        return None

    def _analyse_velocity(self, snapshot: SalesSnapshot) -> Optional[dict]:
        """
        Analyse transaction velocity (transactions per minute) to detect
        rush periods that might need immediate staffing response.
        """
        if snapshot.transaction_count == 0:
            return None

        velocity = snapshot.transaction_count / max(self.fetch_interval_minutes, 1)

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

        avg_txn_per_min = matching[0].avg_transactions / 60.0
        if avg_txn_per_min == 0:
            return None

        velocity_ratio = velocity / avg_txn_per_min

        if velocity_ratio > 1.5:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_velocity",
                "value": 0.2,
                "confidence": 0.7,
                "source": "swiftpos_velocity",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "txn_per_minute": round(velocity, 2),
                    "avg_txn_per_minute": round(avg_txn_per_min, 2),
                    "velocity_ratio": round(velocity_ratio, 2),
                    "alert": "Rush detected — consider calling in backup staff",
                },
            }

        return None

    async def get_location_departments(self) -> list[LocationDepartment]:
        """Get available SwiftPOS locations for venue mapping."""
        try:
            raw = await self.client.get_locations()
            return [
                LocationDepartment(
                    location_id=str(loc.get("id", loc.get("locationId", ""))),
                    location_name=loc.get("name", loc.get("locationName", "Unknown")),
                    department_id=str(loc.get("departmentId", "")),
                    department_name=loc.get("departmentName", ""),
                    is_active=loc.get("isActive", True),
                )
                for loc in raw
            ]
        except SwiftPOSError as e:
            logger.error(f"Failed to fetch locations: {e}")
            return []

    async def health_check(self) -> dict:
        """Check SwiftPOS API connectivity and auth."""
        try:
            await self.client._authenticate()
            locations = await self.client.get_locations()
            return {
                "status": "healthy",
                "connected": True,
                "locations_found": len(locations),
                "api_url": self.client.creds.api_url,
            }
        except SwiftPOSAuthError:
            return {"status": "auth_failed", "connected": False}
        except SwiftPOSError as e:
            return {"status": "error", "connected": False, "error": str(e)}

    async def close(self):
        """Close the HTTP client."""
        await self.client.close()


# ---------------------------------------------------------------------------
# Factory function for easy setup
# ---------------------------------------------------------------------------

def create_swiftpos_adapter(
    api_url: str,
    clerk_id: str,
    client_id: str,
    customer_id: str,
    location_id: str,
    location_name: str = "Venue",
) -> SwiftPOSAdapter:
    """
    Factory function to create a SwiftPOS adapter.

    Example:
        adapter = create_swiftpos_adapter(
            api_url="https://api.swiftpos.com.au/v1",
            clerk_id="CLERK001",
            client_id="CLIENT001",
            customer_id="CUST001",
            location_id="LOC001",
            location_name="The Royal Oak",
        )
        signals = await adapter.fetch_signals()
    """
    creds = SwiftPOSCredentials(
        api_url=api_url,
        clerk_id=clerk_id,
        client_id=client_id,
        customer_id=customer_id,
    )
    return SwiftPOSAdapter(
        credentials=creds,
        location_id=location_id,
        location_name=location_name,
    )
