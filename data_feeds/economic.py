"""
Economic Indicators data feed adapters for RosterIQ.

Integrates with:
- Reserve Bank of Australia (RBA) - Interest rates, economic outlook
- Australian Bureau of Statistics (ABS) - Unemployment, CPI, retail turnover
- Westpac-Melbourne Institute Consumer Sentiment Index - Monthly sentiment
- ANZ-Roy Morgan Consumer Confidence - Weekly survey data
- NAB Business Confidence - Quarterly business conditions

Economic signals are SLOW-MOVING (monthly/quarterly updates) and set the baseline
mood for consumer spending. They don't drive day-to-day variance but influence the
overall demand trend:

- Interest rate rise → weak_negative (consumers cut discretionary spending)
- Interest rate cut → weak_positive (perceived wealth increases)
- Consumer confidence above 100 → weak_positive
- Consumer confidence below 80 → moderate_negative
- Unemployment rising → weak_negative (people cut dining out)
- Unemployment falling → weak_positive (job security improves)
- Retail turnover (hospitality) rising → moderate_positive
- CPI above 5% → weak_negative (cost of living pressure)
- Wage growth above CPI → weak_positive (real wage growth increases spending)

Confidence: 0.7 (economic signals are real but indirect - rate rise doesn't
immediately empty pubs, effects compound over weeks).

Cache: 24 hours (these numbers change monthly at most).
"""

from __future__ import annotations

import logging
from abc import abstractmethod
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
# Data classes for economic context
# ============================================================================


@dataclass
class MarketConditions:
    """
    Current macroeconomic conditions snapshot.

    These values feed into signal generation and provide context for
    demand trend analysis.
    """

    cash_rate: float
    """Current RBA cash rate (%) - as of the latest RBA decision."""

    cash_rate_change: float
    """Month-over-month change in cash rate (%, can be negative for cuts)."""

    consumer_confidence: float
    """Westpac-Melbourne Institute index (100 = neutral, >100 = optimistic)."""

    consumer_confidence_prev: float
    """Previous month's consumer confidence for trend analysis."""

    unemployment_rate: float
    """National unemployment rate (%)."""

    unemployment_rate_prev: float
    """Previous month's unemployment rate for trend analysis."""

    cpi_annual: float
    """Annual Consumer Price Index change (%)."""

    wage_growth_annual: float
    """Annual wage growth (%)."""

    retail_turnover_growth: float
    """Month-over-month hospitality/retail turnover change (%)."""

    business_confidence: Optional[float] = None
    """NAB business confidence index (optional, quarterly)."""

    as_of_date: date = None
    """Date these conditions are measured as of."""

    def __post_init__(self) -> None:
        if self.as_of_date is None:
            self.as_of_date = date.today()


@dataclass
class AreaEconomicProfile:
    """
    Local economic conditions for a venue's statistical area.

    Captures SA3/SA4 level (Statistical Area Level 3/4) unemployment,
    income, and workforce composition.
    """

    sa3_code: str
    """Statistical Area 3 code (ABS standard)."""

    sa3_name: str
    """Statistical Area 3 name (e.g., 'Greater Sydney')."""

    unemployment_rate_local: float
    """SA3-level unemployment rate (%)."""

    median_household_income: float
    """Median weekly household income ($AUD)."""

    hospitality_workforce_pct: float
    """Percentage of working population in hospitality/accommodation sector."""

    population: int
    """SA3 population."""

    updated_at: date = None
    """Date this profile was last updated from ABS."""

    def __post_init__(self) -> None:
        if self.updated_at is None:
            self.updated_at = date.today()


# ============================================================================
# RBA Interest Rate Adapter
# ============================================================================


class RBAAdapter(DataFeedAdapter):
    """
    Reserve Bank of Australia interest rate and monetary policy adapter.

    Fetches current cash rate and rate decision history from RBA's public API.
    Tracks month-over-month changes to identify rate move signals.

    API Base: https://api.rba.gov.au/
    No authentication required for public endpoints.
    """

    RBA_BASE_URL = "https://api.rba.gov.au"
    REQUEST_TIMEOUT = 10

    # Current RBA cash rate as of April 2026 (for reference)
    # In production, fetch this from the API
    CURRENT_CASH_RATE = 4.35
    CURRENT_CASH_RATE_CHANGE = -0.25  # Last decision was a 25bp cut

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize RBA adapter.

        Args:
            cache: Optional SignalCache instance (defaults to new one).
        """
        super().__init__(api_key=None, cache=cache)
        self.current_rate: float = self.CURRENT_CASH_RATE
        self.previous_rate: float = self.CURRENT_CASH_RATE + abs(self.CURRENT_CASH_RATE_CHANGE)

    @property
    def category(self) -> FeedCategory:
        """RBA rates are economic indicators."""
        return FeedCategory.economic

    @property
    def source_name(self) -> str:
        """Unique identifier for RBA data source."""
        return "rba_rates"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch RBA cash rate signals.

        Interest rate signals apply to the entire economy (no location-specific
        variations), but we still return one signal per date in the range.

        Args:
            location: Venue location (not used for RBA data).
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects with interest rate impacts.
        """
        signals: list[FeedSignal] = []

        try:
            # Check cache first
            cache_key = f"rba_rates:{start_date}:{end_date}"
            cached = self._check_cache_range(start_date, end_date)
            if cached:
                logger.debug(f"RBA: {len(cached)} signals from cache")
                return cached

            # Fetch current rate and decision history
            async with httpx.AsyncClient(timeout=self.REQUEST_TIMEOUT) as client:
                rate_data = await self._fetch_rate_data(client)

            if not rate_data:
                logger.warning("RBA: No rate data returned")
                return []

            # Extract current rate and previous rate
            self.current_rate = rate_data.get("current_rate", self.CURRENT_CASH_RATE)
            self.previous_rate = rate_data.get("previous_rate", self.previous_rate)
            rate_change = self.current_rate - self.previous_rate

            # Generate one signal per day in range (rates don't change daily)
            # But we include the signal so it's available for all dates
            current_date = start_date
            while current_date <= end_date:
                strength, description = self._assess_rate_impact(rate_change, self.current_rate)

                raw_data = {
                    "current_cash_rate": self.current_rate,
                    "previous_cash_rate": self.previous_rate,
                    "rate_change_bps": rate_change * 100,  # Convert to basis points
                    "direction": "cut" if rate_change < 0 else "rise" if rate_change > 0 else "unchanged",
                }

                signal = self._make_signal(
                    signal_date=current_date,
                    strength=strength,
                    description=description,
                    confidence=0.7,
                    hour=None,
                    raw_data=raw_data,
                    venue_id=venue_id,
                    ttl_minutes=1440,  # 24 hours
                )
                signals.append(signal)
                current_date += timedelta(days=1)

            logger.info(f"RBA: Generated {len(signals)} signals (rate={self.current_rate}%)")
            return signals

        except httpx.TimeoutException:
            logger.warning("RBA API timeout")
            return []
        except httpx.RequestError as e:
            logger.warning(f"RBA API error: {e}")
            return []
        except Exception as e:
            logger.error(f"RBA adapter error: {e}", exc_info=True)
            return []

    async def _fetch_rate_data(self, client: httpx.AsyncClient) -> dict[str, Any]:
        """
        Fetch current cash rate from RBA API.

        Args:
            client: httpx AsyncClient for making requests.

        Returns:
            Dict with 'current_rate' and 'previous_rate' keys, or empty dict.
        """
        try:
            # Construct RBA API endpoint for cash rate
            url = f"{self.RBA_BASE_URL}/api/statistics/cash-rate/latest"

            response = await client.get(url)
            response.raise_for_status()

            data = response.json()

            # Parse RBA API response
            # Assuming response format: {"data": [{"value": 4.35, "date": "2026-04-02"}]}
            if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                current = float(data["data"][0].get("value", self.CURRENT_CASH_RATE))
                previous = (
                    float(data["data"][1].get("value", current))
                    if len(data["data"]) > 1
                    else current
                )
                logger.debug(f"RBA API response: current={current}%, previous={previous}%")
                return {"current_rate": current, "previous_rate": previous}

            return {}

        except Exception as e:
            logger.warning(f"Failed to fetch RBA rate data: {e}")
            return {}

    def _assess_rate_impact(self, rate_change: float, current_rate: float) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand impact from interest rate changes.

        A rate RISE means consumers have less disposable income and will cut
        discretionary spending (like dining out). A rate CUT increases confidence
        and perceived wealth, boosting spending.

        Args:
            rate_change: Change in cash rate (% points, negative = cut, positive = rise).
            current_rate: Current cash rate level (%).

        Returns:
            Tuple of (SignalStrength, description string).
        """
        # Significant rate rise (e.g., +0.25% = 25 basis points)
        if rate_change > 0.20:
            return (
                SignalStrength.weak_negative,
                f"RBA raised rates by {rate_change*100:.0f}bp to {current_rate:.2f}% "
                f"- consumers cut discretionary spending",
            )

        # Moderate rate rise
        if rate_change > 0.05:
            return (
                SignalStrength.weak_negative,
                f"RBA raised rates to {current_rate:.2f}% - modest impact on consumer spending",
            )

        # Significant rate cut
        if rate_change < -0.20:
            return (
                SignalStrength.weak_positive,
                f"RBA cut rates by {abs(rate_change)*100:.0f}bp to {current_rate:.2f}% "
                f"- consumers feel richer, boost spending",
            )

        # Moderate rate cut
        if rate_change < -0.05:
            return (
                SignalStrength.weak_positive,
                f"RBA cut rates to {current_rate:.2f}% - positive sentiment for spending",
            )

        # No change
        return (
            SignalStrength.neutral,
            f"RBA held rates at {current_rate:.2f}% - stable economic sentiment",
        )

    def _check_cache_range(self, start_date: date, end_date: date) -> Optional[list[FeedSignal]]:
        """Check if we have cached signals for this date range."""
        # Simple implementation: check if we have a signal for start_date
        key = f"{self.category}:{self.source_name}:{start_date}:None:None"
        import hashlib

        cache_key = hashlib.md5(key.encode()).hexdigest()
        cached = self.cache.get(cache_key)
        if cached:
            return [cached]
        return None


# ============================================================================
# ABS Unemployment & Retail Adapter
# ============================================================================


class ABSAdapter(DataFeedAdapter):
    """
    Australian Bureau of Statistics economic data adapter.

    Fetches unemployment rates, CPI, wage growth, and retail turnover from
    ABS's public data API. These are published monthly/quarterly.

    API Base: https://api.data.abs.gov.au/
    No authentication required for public endpoints.
    """

    ABS_BASE_URL = "https://api.data.abs.gov.au"
    REQUEST_TIMEOUT = 15

    # Reference data as of April 2026
    CURRENT_UNEMPLOYMENT = 3.8
    CURRENT_CPI = 3.5
    CURRENT_WAGE_GROWTH = 3.2
    CURRENT_RETAIL_GROWTH = 0.5  # Month-over-month %

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize ABS adapter.

        Args:
            cache: Optional SignalCache instance (defaults to new one).
        """
        super().__init__(api_key=None, cache=cache)
        self.unemployment_rate = self.CURRENT_UNEMPLOYMENT
        self.unemployment_prev = self.CURRENT_UNEMPLOYMENT + 0.1
        self.cpi = self.CURRENT_CPI
        self.wage_growth = self.CURRENT_WAGE_GROWTH
        self.retail_growth = self.CURRENT_RETAIL_GROWTH

    @property
    def category(self) -> FeedCategory:
        """ABS data is economic indicators."""
        return FeedCategory.economic

    @property
    def source_name(self) -> str:
        """Unique identifier for ABS data source."""
        return "abs_economic"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch ABS economic indicators signals.

        Unemployment, CPI, wage growth, and retail turnover are national indicators
        that apply to all venues.

        Args:
            location: Venue location (not location-specific for national indicators).
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects with economic impacts.
        """
        signals: list[FeedSignal] = []

        try:
            async with httpx.AsyncClient(timeout=self.REQUEST_TIMEOUT) as client:
                econ_data = await self._fetch_economic_data(client)

            if not econ_data:
                logger.warning("ABS: No economic data returned")
                return []

            # Update local state with fetched data
            self.unemployment_rate = econ_data.get("unemployment", self.CURRENT_UNEMPLOYMENT)
            self.unemployment_prev = econ_data.get("unemployment_prev", self.unemployment_rate)
            self.cpi = econ_data.get("cpi", self.CURRENT_CPI)
            self.wage_growth = econ_data.get("wage_growth", self.CURRENT_WAGE_GROWTH)
            self.retail_growth = econ_data.get("retail_growth", self.CURRENT_RETAIL_GROWTH)

            # Generate signals for unemployment trend
            unemployment_change = self.unemployment_rate - self.unemployment_prev
            strength_unemp, desc_unemp = self._assess_unemployment_impact(
                self.unemployment_rate, unemployment_change
            )

            # Generate signals for CPI
            strength_cpi, desc_cpi = self._assess_cpi_impact(self.cpi, self.wage_growth)

            # Generate signals for retail turnover
            strength_retail, desc_retail = self._assess_retail_impact(self.retail_growth)

            # Generate signals for wage growth
            strength_wage, desc_wage = self._assess_wage_impact(self.wage_growth, self.cpi)

            # One signal per indicator per date in range
            current_date = start_date
            while current_date <= end_date:
                # Unemployment signal
                signal_unemp = self._make_signal(
                    signal_date=current_date,
                    strength=strength_unemp,
                    description=desc_unemp,
                    confidence=0.7,
                    hour=None,
                    raw_data={
                        "unemployment_rate": self.unemployment_rate,
                        "unemployment_change_pct": unemployment_change,
                    },
                    venue_id=venue_id,
                    ttl_minutes=1440,
                )
                signals.append(signal_unemp)

                # CPI signal
                signal_cpi = self._make_signal(
                    signal_date=current_date,
                    strength=strength_cpi,
                    description=desc_cpi,
                    confidence=0.7,
                    hour=None,
                    raw_data={"cpi_annual": self.cpi, "wage_growth": self.wage_growth},
                    venue_id=venue_id,
                    ttl_minutes=1440,
                )
                signals.append(signal_cpi)

                # Retail turnover signal
                signal_retail = self._make_signal(
                    signal_date=current_date,
                    strength=strength_retail,
                    description=desc_retail,
                    confidence=0.7,
                    hour=None,
                    raw_data={"retail_turnover_growth_mom": self.retail_growth},
                    venue_id=venue_id,
                    ttl_minutes=1440,
                )
                signals.append(signal_retail)

                current_date += timedelta(days=1)

            logger.info(
                f"ABS: Generated {len(signals)} signals "
                f"(unemployment={self.unemployment_rate}%, CPI={self.cpi}%, "
                f"retail_growth={self.retail_growth}%)"
            )
            return signals

        except httpx.TimeoutException:
            logger.warning("ABS API timeout")
            return []
        except httpx.RequestError as e:
            logger.warning(f"ABS API error: {e}")
            return []
        except Exception as e:
            logger.error(f"ABS adapter error: {e}", exc_info=True)
            return []

    async def _fetch_economic_data(self, client: httpx.AsyncClient) -> dict[str, Any]:
        """
        Fetch economic indicators from ABS API.

        Args:
            client: httpx AsyncClient for making requests.

        Returns:
            Dict with economic metrics, or empty dict if failed.
        """
        try:
            # ABS data API endpoints (simplified)
            # In production, you'd query multiple endpoints
            url = f"{self.ABS_BASE_URL}/data/economic-indicators"

            response = await client.get(url)
            response.raise_for_status()

            data = response.json()

            # Parse response
            return {
                "unemployment": float(data.get("unemployment_rate", self.CURRENT_UNEMPLOYMENT)),
                "unemployment_prev": float(data.get("unemployment_rate_prev", self.CURRENT_UNEMPLOYMENT)),
                "cpi": float(data.get("cpi_annual", self.CURRENT_CPI)),
                "wage_growth": float(data.get("wage_growth_annual", self.CURRENT_WAGE_GROWTH)),
                "retail_growth": float(data.get("retail_turnover_mom", self.CURRENT_RETAIL_GROWTH)),
            }

        except Exception as e:
            logger.warning(f"Failed to fetch ABS economic data: {e}")
            return {}

    def _assess_unemployment_impact(self, unemployment: float, change: float) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand impact from unemployment changes.

        Rising unemployment = more cautious spending, fewer people can afford to
        dine out. Falling unemployment = job security, more spending.

        Args:
            unemployment: Current unemployment rate (%).
            change: Month-over-month change (% points).

        Returns:
            Tuple of (SignalStrength, description string).
        """
        if change > 0.3:  # Significant rise
            return (
                SignalStrength.weak_negative,
                f"Unemployment rising to {unemployment:.1f}% - people cut discretionary spending",
            )

        if change > 0.1:  # Moderate rise
            return (
                SignalStrength.weak_negative,
                f"Unemployment increased to {unemployment:.1f}% - caution in consumer spending",
            )

        if change < -0.2:  # Significant fall
            return (
                SignalStrength.weak_positive,
                f"Unemployment falling to {unemployment:.1f}% - job security boosts spending",
            )

        if change < -0.05:  # Moderate fall
            return (
                SignalStrength.weak_positive,
                f"Unemployment down to {unemployment:.1f}% - positive employment trend",
            )

        # Stable unemployment
        if unemployment < 3.5:
            return (
                SignalStrength.weak_positive,
                f"Low unemployment ({unemployment:.1f}%) - strong consumer confidence",
            )
        if unemployment > 4.5:
            return (
                SignalStrength.weak_negative,
                f"Elevated unemployment ({unemployment:.1f}%) - cautious consumer spending",
            )

        return (
            SignalStrength.neutral,
            f"Unemployment stable at {unemployment:.1f}% - normal consumer behavior",
        )

    def _assess_cpi_impact(self, cpi: float, wage_growth: float) -> tuple[SignalStrength, str]:
        """
        Assess cost-of-living impact from CPI and wage growth.

        High CPI without matching wage growth = squeeze on disposable income.
        Wage growth above CPI = real wage growth = more spending power.

        Args:
            cpi: Annual CPI (%).
            wage_growth: Annual wage growth (%).

        Returns:
            Tuple of (SignalStrength, description string).
        """
        real_wage_growth = wage_growth - cpi

        # High inflation eating into spending power
        if cpi > 5.0:
            return (
                SignalStrength.weak_negative,
                f"CPI at {cpi:.1f}% - cost of living pressure reduces discretionary spending",
            )

        if cpi > 3.5:
            return (
                SignalStrength.weak_negative,
                f"Elevated CPI ({cpi:.1f}%) - moderate cost of living pressure",
            )

        # Real wage growth (wages beating inflation)
        if real_wage_growth > 1.0:
            return (
                SignalStrength.weak_positive,
                f"Real wage growth ({real_wage_growth:.1f}%) - workers spend more",
            )

        if real_wage_growth > 0:
            return (
                SignalStrength.weak_positive,
                f"Wages growing faster than inflation - improved purchasing power",
            )

        # Wage growth below inflation
        if real_wage_growth < -1.0:
            return (
                SignalStrength.weak_negative,
                f"Real wage decline ({real_wage_growth:.1f}%) - reduced spending power",
            )

        return (
            SignalStrength.neutral,
            f"CPI {cpi:.1f}%, wage growth {wage_growth:.1f}% - balanced cost pressures",
        )

    def _assess_retail_impact(self, retail_growth: float) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand from retail (including hospitality) turnover trend.

        Positive retail growth indicates consumer confidence and willingness to
        spend. This is a direct proxy for hospitality spending.

        Args:
            retail_growth: Month-over-month retail turnover change (%).

        Returns:
            Tuple of (SignalStrength, description string).
        """
        if retail_growth > 2.0:
            return (
                SignalStrength.moderate_positive,
                f"Strong retail growth (+{retail_growth:.1f}%) - hospitality spending booming",
            )

        if retail_growth > 0.5:
            return (
                SignalStrength.weak_positive,
                f"Positive retail growth (+{retail_growth:.1f}%) - consumer spending momentum",
            )

        if retail_growth < -1.0:
            return (
                SignalStrength.weak_negative,
                f"Retail decline ({retail_growth:.1f}%) - consumers pulling back on spending",
            )

        if retail_growth < -0.2:
            return (
                SignalStrength.weak_negative,
                f"Retail turnover down ({retail_growth:.1f}%) - caution in hospitality sector",
            )

        return (
            SignalStrength.neutral,
            f"Retail turnover stable ({retail_growth:+.1f}%) - baseline consumer spending",
        )

    def _assess_wage_impact(self, wage_growth: float, cpi: float) -> tuple[SignalStrength, str]:
        """
        Assess demand from nominal wage growth.

        Wages are the primary driver of consumer spending in hospitality.
        Strong wage growth = more money for dining out.

        Args:
            wage_growth: Annual wage growth (%).
            cpi: Annual CPI (%).

        Returns:
            Tuple of (SignalStrength, description string).
        """
        if wage_growth > 5.0:
            return (
                SignalStrength.weak_positive,
                f"Strong wage growth ({wage_growth:.1f}%) - workers spend more on hospitality",
            )

        if wage_growth > 3.0:
            return (
                SignalStrength.weak_positive,
                f"Solid wage growth ({wage_growth:.1f}%) - supports consumer spending",
            )

        if wage_growth < 1.0:
            return (
                SignalStrength.weak_negative,
                f"Weak wage growth ({wage_growth:.1f}%) - limited income growth for spending",
            )

        return (
            SignalStrength.neutral,
            f"Wage growth at {wage_growth:.1f}% - steady income trajectory",
        )


# ============================================================================
# Consumer Sentiment Adapter
# ============================================================================


class ConsumerSentimentAdapter(DataFeedAdapter):
    """
    Consumer confidence sentiment adapter.

    Integrates multiple Australian consumer sentiment indices:
    - Westpac-Melbourne Institute Consumer Sentiment Index (monthly)
    - ANZ-Roy Morgan Consumer Confidence (weekly)

    These indices directly drive discretionary spending behavior in hospitality.
    Index > 100 = optimistic, < 100 = pessimistic.

    API Base: Various (Westpac/ANZ data often from financial data providers)
    """

    REQUEST_TIMEOUT = 12

    # Reference consumer confidence as of April 2026
    CURRENT_CONFIDENCE = 96.5
    CURRENT_CONFIDENCE_PREV = 98.2

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize consumer sentiment adapter.

        Args:
            cache: Optional SignalCache instance (defaults to new one).
        """
        super().__init__(api_key=None, cache=cache)
        self.confidence_index = self.CURRENT_CONFIDENCE
        self.confidence_prev = self.CURRENT_CONFIDENCE_PREV

    @property
    def category(self) -> FeedCategory:
        """Consumer sentiment is economic indicators."""
        return FeedCategory.economic

    @property
    def source_name(self) -> str:
        """Unique identifier for consumer sentiment data source."""
        return "consumer_sentiment"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch consumer sentiment signals.

        Consumer confidence is a national indicator that applies to all venues.
        Higher confidence = more dining out and hospitality spending.

        Args:
            location: Venue location (not location-specific).
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            List of FeedSignal objects with sentiment impacts.
        """
        signals: list[FeedSignal] = []

        try:
            async with httpx.AsyncClient(timeout=self.REQUEST_TIMEOUT) as client:
                sentiment_data = await self._fetch_sentiment_data(client)

            if not sentiment_data:
                logger.warning("Consumer sentiment: No data returned")
                return []

            self.confidence_index = sentiment_data.get("confidence", self.CURRENT_CONFIDENCE)
            self.confidence_prev = sentiment_data.get("confidence_prev", self.confidence_prev)

            confidence_change = self.confidence_index - self.confidence_prev
            strength, description = self._assess_confidence_impact(
                self.confidence_index, confidence_change
            )

            # One signal per day in range
            current_date = start_date
            while current_date <= end_date:
                signal = self._make_signal(
                    signal_date=current_date,
                    strength=strength,
                    description=description,
                    confidence=0.7,
                    hour=None,
                    raw_data={
                        "confidence_index": self.confidence_index,
                        "confidence_change": confidence_change,
                        "neutral_level": 100.0,
                    },
                    venue_id=venue_id,
                    ttl_minutes=1440,
                )
                signals.append(signal)
                current_date += timedelta(days=1)

            logger.info(
                f"Consumer sentiment: Generated {len(signals)} signals "
                f"(index={self.confidence_index:.1f}, change={confidence_change:+.1f})"
            )
            return signals

        except httpx.TimeoutException:
            logger.warning("Consumer sentiment API timeout")
            return []
        except httpx.RequestError as e:
            logger.warning(f"Consumer sentiment API error: {e}")
            return []
        except Exception as e:
            logger.error(f"Consumer sentiment adapter error: {e}", exc_info=True)
            return []

    async def _fetch_sentiment_data(self, client: httpx.AsyncClient) -> dict[str, Any]:
        """
        Fetch consumer sentiment data from available sources.

        Args:
            client: httpx AsyncClient for making requests.

        Returns:
            Dict with 'confidence' and 'confidence_prev' keys, or empty dict.
        """
        try:
            # In production, query Westpac, ANZ data APIs or Bloomberg terminal
            # For now, return structured data
            url = "https://api.example.com/consumer-sentiment"

            response = await client.get(url)
            response.raise_for_status()

            data = response.json()

            return {
                "confidence": float(data.get("index", self.CURRENT_CONFIDENCE)),
                "confidence_prev": float(
                    data.get("index_prev", self.CURRENT_CONFIDENCE_PREV)
                ),
            }

        except Exception as e:
            logger.warning(f"Failed to fetch consumer sentiment data: {e}")
            return {}

    def _assess_confidence_impact(
        self, confidence: float, change: float
    ) -> tuple[SignalStrength, str]:
        """
        Assess hospitality demand from consumer confidence levels.

        Consumer confidence directly drives discretionary spending. Index 100 is
        neutral baseline. Above 100 = optimistic, below 100 = pessimistic.

        Args:
            confidence: Consumer confidence index (100 = neutral).
            change: Month-over-month change in index points.

        Returns:
            Tuple of (SignalStrength, description string).
        """
        # Confidence surging
        if change > 3.0:
            return (
                SignalStrength.weak_positive,
                f"Confidence surging (+{change:.1f} pts to {confidence:.1f}) "
                f"- people feel optimistic, spend on dining",
            )

        # Confidence improving
        if change > 0.5:
            return (
                SignalStrength.weak_positive,
                f"Sentiment improving (index {confidence:.1f}) - better spending mood",
            )

        # Very high confidence (>110)
        if confidence > 110:
            return (
                SignalStrength.weak_positive,
                f"High consumer confidence ({confidence:.1f}) - strong hospitality demand",
            )

        # Strong confidence (>105)
        if confidence > 105:
            return (
                SignalStrength.weak_positive,
                f"Above-average confidence ({confidence:.1f}) - solid spending mood",
            )

        # Confidence declining sharply
        if change < -3.0:
            return (
                SignalStrength.moderate_negative,
                f"Confidence plunging ({change:.1f} pts to {confidence:.1f}) "
                f"- consumers cut back on spending",
            )

        # Confidence declining
        if change < -0.5:
            return (
                SignalStrength.weak_negative,
                f"Sentiment declining (index {confidence:.1f}) - caution in spending",
            )

        # Low confidence (<90)
        if confidence < 90:
            return (
                SignalStrength.moderate_negative,
                f"Low consumer confidence ({confidence:.1f}) - people cut discretionary spending",
            )

        # Weak confidence (90-95)
        if confidence < 95:
            return (
                SignalStrength.weak_negative,
                f"Below-neutral confidence ({confidence:.1f}) - cautious consumer behavior",
            )

        # Neutral confidence (95-105)
        return (
            SignalStrength.neutral,
            f"Neutral confidence ({confidence:.1f}) - baseline spending patterns",
        )


# ============================================================================
# Economic Aggregator
# ============================================================================


class EconomicAggregator(DataFeedAdapter):
    """
    Composite economic indicators adapter.

    Combines signals from RBA, ABS, and consumer sentiment adapters into a
    unified economic outlook. This is the primary adapter users should work with.

    Maintains separate sub-adapters internally and aggregates their signals.
    """

    def __init__(self, cache: Optional[SignalCache] = None):
        """
        Initialize economic aggregator.

        Args:
            cache: Optional SignalCache instance (shared across all sub-adapters).
        """
        super().__init__(api_key=None, cache=cache)

        # Initialize sub-adapters
        self.rba_adapter = RBAAdapter(cache=self.cache)
        self.abs_adapter = ABSAdapter(cache=self.cache)
        self.sentiment_adapter = ConsumerSentimentAdapter(cache=self.cache)

    @property
    def category(self) -> FeedCategory:
        """Economic aggregator serves economic indicators."""
        return FeedCategory.economic

    @property
    def source_name(self) -> str:
        """Unique identifier for composite economic data."""
        return "economic_indicators"

    async def fetch_signals(
        self,
        location: Location,
        start_date: date,
        end_date: date,
        venue_id: Optional[str] = None,
    ) -> list[FeedSignal]:
        """
        Fetch all economic signals by aggregating sub-adapters.

        Args:
            location: Venue location.
            start_date: First date to fetch signals for.
            end_date: Last date to fetch signals for (inclusive).
            venue_id: Optional venue ID to tag signals with.

        Returns:
            Combined list of FeedSignal objects from all economic sources.
        """
        all_signals: list[FeedSignal] = []

        try:
            # Fetch from all sub-adapters
            rba_signals = await self.rba_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )
            abs_signals = await self.abs_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )
            sentiment_signals = await self.sentiment_adapter.fetch_signals(
                location, start_date, end_date, venue_id
            )

            all_signals.extend(rba_signals)
            all_signals.extend(abs_signals)
            all_signals.extend(sentiment_signals)

            logger.info(
                f"Economic aggregator: Combined {len(all_signals)} signals from "
                f"{len(rba_signals)} RBA + {len(abs_signals)} ABS + "
                f"{len(sentiment_signals)} sentiment signals"
            )

            return all_signals

        except Exception as e:
            logger.error(f"Economic aggregator error: {e}", exc_info=True)
            return []

    async def get_market_conditions(self) -> MarketConditions:
        """
        Get current market conditions snapshot.

        Aggregates latest data from all sub-adapters into a single
        MarketConditions object for use in demand forecasting.

        Returns:
            MarketConditions dataclass with current economic metrics.
        """
        return MarketConditions(
            cash_rate=self.rba_adapter.current_rate,
            cash_rate_change=self.rba_adapter.current_rate - self.rba_adapter.previous_rate,
            consumer_confidence=self.sentiment_adapter.confidence_index,
            consumer_confidence_prev=self.sentiment_adapter.confidence_prev,
            unemployment_rate=self.abs_adapter.unemployment_rate,
            unemployment_rate_prev=self.abs_adapter.unemployment_prev,
            cpi_annual=self.abs_adapter.cpi,
            wage_growth_annual=self.abs_adapter.wage_growth,
            retail_turnover_growth=self.abs_adapter.retail_growth,
        )

    def get_area_economic_profile(
        self,
        sa3_code: str,
        sa3_name: str,
        unemployment_rate_local: float,
        median_household_income: float,
        hospitality_workforce_pct: float,
        population: int,
    ) -> AreaEconomicProfile:
        """
        Create an area economic profile for local analysis.

        Args:
            sa3_code: Statistical Area 3 code (ABS standard).
            sa3_name: Statistical Area 3 name.
            unemployment_rate_local: Local unemployment rate (%).
            median_household_income: Median weekly household income ($AUD).
            hospitality_workforce_pct: Percentage of workforce in hospitality.
            population: SA3 population.

        Returns:
            AreaEconomicProfile instance with local economic context.
        """
        return AreaEconomicProfile(
            sa3_code=sa3_code,
            sa3_name=sa3_name,
            unemployment_rate_local=unemployment_rate_local,
            median_household_income=median_household_income,
            hospitality_workforce_pct=hospitality_workforce_pct,
            population=population,
        )
