"""
Lightspeed Restaurant K-Series POS Integration Adapter for RosterIQ
====================================================================

Pulls sales data, transaction volumes, revenue center breakdown, and trading patterns
from Lightspeed Restaurant (K-Series, formerly Kounta) API to feed the RosterIQ
variance engine.

Lightspeed Restaurant K-Series API requires:
  - client_id:           OAuth 2.0 client ID
  - client_secret:       OAuth 2.0 client secret
  - refresh_token:       OAuth 2.0 refresh token for long-lived access
  - business_location_id: The location/venue identifier in Lightspeed

Requires Lightspeed Restaurant K-Series API v2+.

Endpoints used:
  - POST  /oauth/token                           → Refresh OAuth token
  - GET   /api/financial/{businessLocationId}/sales        → Sales transactions
  - GET   /api/financial/{businessLocationId}/daily-financials → Daily summaries
  - GET   /api/venues/{businessLocationId}      → Venue/location metadata
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger("rosteriq.data_feeds.lightspeed")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIGHTSPEED_API_BASE_URL = "https://api.lsk.lightspeed.app"
LIGHTSPEED_DEFAULT_TIMEOUT = 30  # seconds
LIGHTSPEED_MAX_RETRIES = 3
LIGHTSPEED_RETRY_DELAY = 2  # seconds

AU_TZ = timezone(timedelta(hours=10))  # AEST


class LightspeedError(Exception):
    """Base exception for Lightspeed adapter errors."""
    pass


class LightspeedAuthError(LightspeedError):
    """Authentication or token refresh failed."""
    pass


class LightspeedRateLimitError(LightspeedError):
    """Rate limit exceeded."""
    pass


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class LightspeedCredentials:
    """Lightspeed Restaurant K-Series OAuth credentials."""
    client_id: str
    client_secret: str
    refresh_token: str
    business_location_id: str

    def __post_init__(self):
        """Validate credentials are non-empty."""
        if not all([self.client_id, self.client_secret, self.refresh_token, self.business_location_id]):
            raise ValueError("All Lightspeed credentials must be non-empty")


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
    top_revenue_centers: dict[str, float] = field(default_factory=dict)  # revenue center → revenue
    hourly_breakdown: dict[int, float] = field(default_factory=dict)  # hour → revenue
    hourly_covers: dict[int, int] = field(default_factory=dict)  # hour → covers


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
class RevenueCenterBreakdown:
    """Revenue centre performance metrics."""
    revenue_center: str
    revenue: float
    transaction_count: int
    covers: int
    avg_transaction: float


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
# OAuth Token Cache
# ---------------------------------------------------------------------------

@dataclass
class _TokenCache:
    """Internal OAuth token cache with expiry tracking."""
    token: Optional[str] = None
    expires_at: Optional[datetime] = None

    @property
    def is_valid(self) -> bool:
        """Check if cached token is still valid (with 2-minute buffer)."""
        if not self.token or not self.expires_at:
            return False
        return datetime.now(timezone.utc) < self.expires_at - timedelta(minutes=2)


# ---------------------------------------------------------------------------
# Lightspeed API Client
# ---------------------------------------------------------------------------

class LightspeedClient:
    """Low-level async client for Lightspeed Restaurant K-Series REST API."""

    def __init__(self, credentials: LightspeedCredentials):
        """
        Initialize Lightspeed client with OAuth credentials.

        Args:
            credentials: LightspeedCredentials with OAuth details and business location ID
        """
        self.creds = credentials
        self._token_cache = _TokenCache()
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=LIGHTSPEED_API_BASE_URL,
                timeout=LIGHTSPEED_DEFAULT_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _refresh_oauth_token(self) -> str:
        """
        Refresh OAuth 2.0 access token using refresh_token.

        Returns:
            New bearer token

        Raises:
            LightspeedAuthError: If token refresh fails
        """
        if self._token_cache.is_valid:
            return self._token_cache.token

        client = await self._get_client()
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.creds.client_id,
            "client_secret": self.creds.client_secret,
            "refresh_token": self.creds.refresh_token,
        }

        try:
            resp = await client.post("/oauth/token", data=payload)
            if resp.status_code == 401:
                raise LightspeedAuthError("Invalid Lightspeed OAuth credentials")
            if resp.status_code == 429:
                raise LightspeedRateLimitError("Lightspeed rate limit exceeded")
            resp.raise_for_status()
            data = resp.json()

            self._token_cache.token = data.get("access_token")
            if not self._token_cache.token:
                raise LightspeedAuthError("No access_token in OAuth response")

            # Token expires_in is in seconds
            expires_in = data.get("expires_in", 3600)
            self._token_cache.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            return self._token_cache.token

        except httpx.HTTPStatusError as e:
            raise LightspeedAuthError(f"OAuth token refresh failed: {e.response.status_code}") from e
        except httpx.RequestError as e:
            raise LightspeedError(f"Connection error during token refresh: {e}") from e

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """
        Make authenticated API request with retries and token refresh.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path relative to base URL
            **kwargs: Additional arguments for httpx request

        Returns:
            Parsed JSON response

        Raises:
            LightspeedError: If request fails after retries
        """
        client = await self._get_client()
        token = await self._refresh_oauth_token()

        for attempt in range(LIGHTSPEED_MAX_RETRIES):
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
                    token = await self._refresh_oauth_token()
                    continue

                if resp.status_code == 429:
                    wait = LIGHTSPEED_RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except httpx.RequestError as e:
                if attempt == LIGHTSPEED_MAX_RETRIES - 1:
                    raise LightspeedError(f"Request failed after {LIGHTSPEED_MAX_RETRIES} attempts: {e}") from e
                await asyncio.sleep(LIGHTSPEED_RETRY_DELAY)

        raise LightspeedError("Max retries exceeded")

    async def get(self, path: str, params: dict = None) -> dict:
        """Make GET request."""
        return await self._request("GET", path, params=params)

    async def post(self, path: str, json: dict = None, data: dict = None) -> dict:
        """Make POST request."""
        if json:
            return await self._request("POST", path, json=json)
        return await self._request("POST", path, data=data)

    # ----- Convenience endpoints -----

    async def get_sales(
        self,
        from_date: datetime,
        to_date: datetime,
        include_voided: bool = False,
    ) -> list[dict]:
        """
        Get sales transactions for the business location.

        Args:
            from_date: Start date (ISO 8601)
            to_date: End date (ISO 8601)
            include_voided: Whether to include voided transactions

        Returns:
            List of sales transaction objects
        """
        params = {
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
        }
        path = f"/api/financial/{self.creds.business_location_id}/sales"
        data = await self.get(path, params=params)
        return data if isinstance(data, list) else data.get("sales", [])

    async def get_daily_financials(
        self,
        from_date: datetime,
        to_date: datetime,
    ) -> list[dict]:
        """
        Get daily financial summaries for the business location.

        Args:
            from_date: Start date (ISO 8601)
            to_date: End date (ISO 8601)

        Returns:
            List of daily financial summary objects
        """
        params = {
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
        }
        path = f"/api/financial/{self.creds.business_location_id}/daily-financials"
        data = await self.get(path, params=params)
        return data if isinstance(data, list) else data.get("daily_financials", [])

    async def get_venue(self) -> dict:
        """Get venue/location metadata."""
        path = f"/api/venues/{self.creds.business_location_id}"
        return await self.get(path)


# ---------------------------------------------------------------------------
# Sales Data Analyser
# ---------------------------------------------------------------------------

class SalesAnalyser:
    """Analyses Lightspeed sales data to derive demand and venue signals."""

    def __init__(self, lookback_weeks: int = 8):
        """
        Initialize analyser.

        Args:
            lookback_weeks: Number of weeks of historical data to analyse
        """
        self.lookback_weeks = lookback_weeks
        self._historical_patterns: dict[str, list[TradingPattern]] = {}

    def build_sales_snapshot(
        self,
        location_id: str,
        location_name: str,
        sales: list[dict],
        timestamp: datetime = None,
    ) -> SalesSnapshot:
        """
        Build a SalesSnapshot from raw Lightspeed sales transactions.

        Args:
            location_id: Venue ID
            location_name: Venue name
            sales: List of sale transaction dicts from API
            timestamp: Override timestamp (default: now in AU_TZ)

        Returns:
            SalesSnapshot with aggregated metrics
        """
        if timestamp is None:
            timestamp = datetime.now(AU_TZ)

        total_revenue = 0.0
        transaction_count = len(sales)
        covers = 0
        rc_totals: dict[str, float] = {}
        rc_txn_counts: dict[str, int] = {}
        rc_covers: dict[str, int] = {}
        hourly_totals: dict[int, float] = {}
        hourly_cover_totals: dict[int, int] = {}

        for sale in sales:
            # Sum revenue (use 'total' field, subtract tax if needed)
            amount = float(sale.get("total", 0) or 0)
            total_revenue += amount

            # Extract covers/guests if available
            covers += int(sale.get("covers", 0) or 0)

            # Revenue centre breakdown
            rc = sale.get("revenueCenter", sale.get("revenue_center", "Uncategorized"))
            if rc:
                rc_totals[rc] = rc_totals.get(rc, 0) + amount
                rc_txn_counts[rc] = rc_txn_counts.get(rc, 0) + 1
                rc_covers[rc] = rc_covers.get(rc, 0) + int(sale.get("covers", 0) or 0)

            # Hourly breakdown (parse timeClosed ISO 8601)
            time_closed = sale.get("timeClosed", sale.get("time_closed", ""))
            if time_closed:
                try:
                    if isinstance(time_closed, str):
                        dt = datetime.fromisoformat(time_closed.replace("Z", "+00:00"))
                    else:
                        dt = time_closed
                    hour = dt.hour
                    hourly_totals[hour] = hourly_totals.get(hour, 0) + amount
                    hourly_cover_totals[hour] = hourly_cover_totals.get(hour, 0) + int(sale.get("covers", 0) or 0)
                except (ValueError, AttributeError, TypeError):
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
            top_revenue_centers=dict(sorted(rc_totals.items(), key=lambda x: -x[1])[:5]),
            hourly_breakdown=hourly_totals,
            hourly_covers=hourly_cover_totals,
        )

    def analyse_trading_patterns(
        self,
        historical_snapshots: list[SalesSnapshot],
    ) -> list[TradingPattern]:
        """
        Analyse historical snapshots to derive trading patterns.

        Groups data by day-of-week and hour to calculate average revenue,
        transaction counts, covers, volatility, and trend.

        Args:
            historical_snapshots: List of SalesSnapshot objects

        Returns:
            List of TradingPattern objects indexed by (day_of_week, hour)
        """
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
        Compare current trading against historical patterns to derive demand signal.

        Calculates percentage deviation from historical average for the current
        day-of-week and hour, then maps to a DemandSignal with multiplier.

        Args:
            current_snapshot: Current SalesSnapshot
            patterns: List of historical TradingPattern objects

        Returns:
            Tuple of (DemandSignal, multiplier) where multiplier ranges -0.35 to 0.35
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
# Lightspeed Data Feed Adapter (integrates with RosterIQ variance engine)
# ---------------------------------------------------------------------------

class LightspeedAdapter:
    """
    High-level adapter that connects Lightspeed Restaurant sales data to the
    RosterIQ variance engine via FeedSignal interface.

    Fetches sales transactions, analyses demand patterns, tracks revenue centre
    performance, and generates staffing signals based on current trading conditions.

    Usage:
        adapter = LightspeedAdapter(credentials)
        signals = await adapter.fetch_signals()
        # signals are ready to feed into SignalAggregator
    """

    FEED_CATEGORY = "pos_sales"
    SIGNAL_TYPE = "foot_traffic"  # Maps to existing SignalType for demand

    def __init__(
        self,
        credentials: LightspeedCredentials,
        location_name: str = "Venue",
        lookback_weeks: int = 8,
        fetch_interval_minutes: int = 15,
    ):
        """
        Initialize Lightspeed adapter.

        Args:
            credentials: LightspeedCredentials with OAuth details
            location_name: Display name for the venue
            lookback_weeks: Historical lookback period for pattern analysis
            fetch_interval_minutes: Interval for current period data fetch
        """
        self.client = LightspeedClient(credentials)
        self.analyser = SalesAnalyser(lookback_weeks)
        self.location_id = credentials.business_location_id
        self.location_name = location_name
        self.lookback_weeks = lookback_weeks
        self.fetch_interval_minutes = fetch_interval_minutes
        self._patterns: list[TradingPattern] = []
        self._last_pattern_build: Optional[datetime] = None
        self._cache: dict[str, Any] = {}

    async def initialise(self):
        """
        Build historical trading patterns. Call once on startup,
        then periodically (e.g. daily) to refresh patterns from historical data.
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
                    sales = await self.client.get_sales(start, end)
                    if sales:
                        snap = self.analyser.build_sales_snapshot(
                            self.location_id, self.location_name,
                            sales, start,
                        )
                        snapshots.append(snap)
                except LightspeedError as e:
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

        Rebuilds patterns daily if stale, then fetches current period
        transactions and analyses demand, revenue centres, and covers velocity.

        Returns:
            List of signal dicts with keys:
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
            sales = await self.client.get_sales(period_start, now)
        except LightspeedError as e:
            logger.error(f"Failed to fetch current sales: {e}")
            return []

        if not sales:
            return []

        # Build current snapshot
        snapshot = self.analyser.build_sales_snapshot(
            self.location_id, self.location_name, sales, now
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
            "source": "lightspeed",
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

        # Revenue centre signal (identifies which areas are busiest)
        rc_signal = self._analyse_revenue_centres(snapshot)
        if rc_signal:
            signals.append(rc_signal)

        # Covers velocity signal (covers per hour vs historical average)
        velocity_signal = self._analyse_covers_velocity(snapshot)
        if velocity_signal:
            signals.append(velocity_signal)

        return signals

    def _calculate_confidence(self, snapshot: SalesSnapshot) -> float:
        """
        Calculate confidence score based on data completeness.

        Args:
            snapshot: Current SalesSnapshot

        Returns:
            Confidence score 0.0-1.0
        """
        score = 0.5  # base

        if snapshot.transaction_count > 0:
            score += 0.2
        if snapshot.covers > 0:
            score += 0.1
        if len(snapshot.top_revenue_centers) > 1:
            score += 0.1
        if len(self._patterns) > 50:
            score += 0.1

        return min(round(score, 2), 1.0)

    def _analyse_revenue_centres(self, snapshot: SalesSnapshot) -> Optional[dict]:
        """
        Detect which revenue centres (bar, dining, takeaway, etc.) are busiest.

        This signal helps identify if unusual staffing mix is needed
        (e.g. event crowd at bar vs regular service at tables).

        Args:
            snapshot: Current SalesSnapshot

        Returns:
            Signal dict or None if insufficient revenue centre data
        """
        if not snapshot.top_revenue_centers or snapshot.total_revenue == 0:
            return None

        # Find the highest-revenue centre
        top_rc = max(snapshot.top_revenue_centers.items(), key=lambda x: x[1])
        top_rc_name, top_rc_revenue = top_rc
        top_rc_pct = top_rc_revenue / snapshot.total_revenue

        # Strong signal if one centre dominates
        if top_rc_pct > 0.65:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_revenue_centre",
                "value": 0.1,  # Slight upward pressure
                "confidence": 0.65,
                "source": "lightspeed_revenue_centre",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "dominant_centre": top_rc_name,
                    "centre_revenue_pct": round(top_rc_pct * 100, 1),
                    "revenue_centres": {k: round(v, 2) for k, v in snapshot.top_revenue_centers.items()},
                    "staffing_hint": f"Focus on {top_rc_name} coverage",
                },
            }

        return None

    def _analyse_covers_velocity(self, snapshot: SalesSnapshot) -> Optional[dict]:
        """
        Analyse covers velocity (covers per hour) vs historical average.

        High velocity indicates busy service requiring more floor staff.

        Args:
            snapshot: Current SalesSnapshot

        Returns:
            Signal dict or None if insufficient data
        """
        if snapshot.covers == 0:
            return None

        velocity = snapshot.covers / max(self.fetch_interval_minutes / 60.0, 0.25)

        # Find historical average covers for this time
        now = snapshot.timestamp
        dow = now.weekday()
        hour = now.hour

        matching = [
            p for p in self._patterns
            if p.day_of_week == dow and p.hour == hour
        ]

        if not matching:
            return None

        avg_covers_per_hour = matching[0].avg_covers
        if avg_covers_per_hour == 0:
            return None

        velocity_ratio = velocity / avg_covers_per_hour

        if velocity_ratio > 1.5:
            return {
                "signal_type": self.SIGNAL_TYPE,
                "category": "pos_covers_velocity",
                "value": 0.2,
                "confidence": 0.75,
                "source": "lightspeed_covers_velocity",
                "timestamp": snapshot.timestamp.isoformat(),
                "metadata": {
                    "covers_per_hour": round(velocity, 1),
                    "avg_covers_per_hour": round(avg_covers_per_hour, 1),
                    "velocity_ratio": round(velocity_ratio, 2),
                    "alert": "High covers velocity — consider calling in backup floor staff",
                },
            }

        return None

    async def get_revenue_centres(self) -> list[RevenueCenterBreakdown]:
        """
        Get a summary of revenue centre performance.

        Returns:
            List of RevenueCenterBreakdown objects
        """
        now = datetime.now(AU_TZ)
        period_start = now - timedelta(hours=24)

        try:
            sales = await self.client.get_sales(period_start, now)
        except LightspeedError as e:
            logger.error(f"Failed to fetch revenue centre data: {e}")
            return []

        rc_data: dict[str, dict] = {}

        for sale in sales:
            rc = sale.get("revenueCenter", sale.get("revenue_center", "Uncategorized"))
            if rc not in rc_data:
                rc_data[rc] = {
                    "revenue": 0.0,
                    "transactions": 0,
                    "covers": 0,
                }

            rc_data[rc]["revenue"] += float(sale.get("total", 0) or 0)
            rc_data[rc]["transactions"] += 1
            rc_data[rc]["covers"] += int(sale.get("covers", 0) or 0)

        result = []
        for rc_name, data in rc_data.items():
            avg_txn = data["revenue"] / max(data["transactions"], 1)
            result.append(RevenueCenterBreakdown(
                revenue_center=rc_name,
                revenue=round(data["revenue"], 2),
                transaction_count=data["transactions"],
                covers=data["covers"],
                avg_transaction=round(avg_txn, 2),
            ))

        return sorted(result, key=lambda x: -x.revenue)

    async def health_check(self) -> dict:
        """
        Check Lightspeed API connectivity and OAuth authentication.

        Returns:
            Health status dict with status, connected, and details
        """
        try:
            await self.client._refresh_oauth_token()
            venue = await self.client.get_venue()
            return {
                "status": "healthy",
                "connected": True,
                "venue_id": self.location_id,
                "venue_name": venue.get("name", "Unknown"),
                "api_url": LIGHTSPEED_API_BASE_URL,
            }
        except LightspeedAuthError:
            return {"status": "auth_failed", "connected": False}
        except LightspeedError as e:
            return {"status": "error", "connected": False, "error": str(e)}

    async def close(self):
        """Close the HTTP client."""
        await self.client.close()


# ---------------------------------------------------------------------------
# Factory function for easy setup
# ---------------------------------------------------------------------------

def create_lightspeed_adapter(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    business_location_id: str,
    location_name: str = "Venue",
) -> LightspeedAdapter:
    """
    Factory function to create a Lightspeed Restaurant adapter.

    Example:
        adapter = create_lightspeed_adapter(
            client_id="your_client_id",
            client_secret="your_client_secret",
            refresh_token="your_refresh_token",
            business_location_id="loc_12345",
            location_name="The Eagle Hotel",
        )
        await adapter.initialise()
        signals = await adapter.fetch_signals()
    """
    creds = LightspeedCredentials(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        business_location_id=business_location_id,
    )
    return LightspeedAdapter(
        credentials=creds,
        location_name=location_name,
    )
