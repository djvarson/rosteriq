"""
SwiftPOS Point-of-Sale Adapter for RosterIQ.

Provides unified interface to SwiftPOS transaction and sales data,
with demand signal extraction for roster optimization.
"""

import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, time
from typing import Optional, List, Dict, Tuple
import random
import hashlib
import hmac
from dataclasses import dataclass, asdict, field
from enum import Enum

try:
    import httpx
except ImportError:
    # httpx is optional; SwiftPOSClient requires it
    httpx = None


# ============================================================================
# Data Models
# ============================================================================

class DayOfWeek(str, Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

    @classmethod
    def from_date(cls, date: datetime) -> "DayOfWeek":
        """Get day of week from datetime."""
        days = [
            cls.MONDAY,
            cls.TUESDAY,
            cls.WEDNESDAY,
            cls.THURSDAY,
            cls.FRIDAY,
            cls.SATURDAY,
            cls.SUNDAY,
        ]
        return days[date.weekday()]


@dataclass
class HourlySales:
    """Hourly sales data."""
    hour: int  # 0-23
    revenue: float
    covers: int  # number of customer transactions
    orders: int


@dataclass
class DailySummary:
    """Daily sales summary."""
    date: datetime
    total_revenue: float
    covers: int
    avg_spend: float
    product_mix: Dict[str, float]  # {category: percentage}
    hourly_breakdown: List[HourlySales] = field(default_factory=list)


@dataclass
class Transaction:
    """Individual transaction record."""
    transaction_id: str
    timestamp: datetime
    amount: float
    covers: int
    product_categories: Dict[str, float]  # {category: amount}


@dataclass
class TradingPattern:
    """Typical trading pattern for a day of week."""
    day_of_week: DayOfWeek
    hourly_curve: List[float]  # percentage of daily revenue by hour (0-23)
    avg_revenue: float
    avg_covers: int
    product_mix: Dict[str, float]


@dataclass
class RevenueRange:
    """Predicted revenue range for a date."""
    date: datetime
    low: float
    expected: float
    high: float
    confidence: float  # 0.0-1.0


@dataclass
class ProductMix:
    """Product category mix data."""
    category: str
    revenue: float
    percentage: float
    transaction_count: int


# ============================================================================
# Abstract Base Class
# ============================================================================

class POSAdapter(ABC):
    """Abstract base class for POS adapters."""

    @abstractmethod
    async def get_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
    ) -> List[Transaction]:
        """Get transactions for date range."""
        pass

    @abstractmethod
    async def get_hourly_sales(
        self,
        date: datetime,
    ) -> List[HourlySales]:
        """Get hourly sales breakdown for a specific date."""
        pass

    @abstractmethod
    async def get_daily_summary(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[DailySummary]:
        """Get daily sales summaries for date range."""
        pass

    @abstractmethod
    async def get_product_mix(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[ProductMix]:
        """Get product category mix for date range."""
        pass

    @abstractmethod
    async def get_trading_patterns(self) -> List[TradingPattern]:
        """Get typical trading patterns by day of week."""
        pass


# ============================================================================
# SwiftPOS REST API Client
# ============================================================================

class SwiftPOSClient(POSAdapter):
    """Real SwiftPOS API client using httpx async."""

    def __init__(
        self,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
    ):
        """Initialize SwiftPOS client.

        Args:
            api_url: Base URL for SwiftPOS API (from SWIFTPOS_API_URL env var)
            api_key: API key for authentication
            secret_key: Secret key for HMAC signing
        """
        if not httpx:
            raise ImportError(
                "httpx is required for SwiftPOSClient. Install with: pip install httpx"
            )

        self.api_url = api_url or os.getenv("SWIFTPOS_API_URL", "https://api.swiftpos.com.au/v1")
        self.api_key = api_key or os.getenv("SWIFTPOS_API_KEY")
        self.secret_key = secret_key or os.getenv("SWIFTPOS_SECRET_KEY")

        if not self.api_key or not self.secret_key:
            raise ValueError(
                "SwiftPOS credentials not configured. "
                "Set SWIFTPOS_API_KEY and SWIFTPOS_SECRET_KEY."
            )

    def _sign_request(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Generate HMAC signed headers for request.

        Args:
            method: HTTP method (GET, POST, etc)
            path: Request path
            body: Request body

        Returns:
            Headers dict with authorization
        """
        timestamp = str(int(datetime.utcnow().timestamp()))
        message = f"{method}\n{path}\n{timestamp}\n{body}"

        signature = hmac.new(
            self.secret_key.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()

        return {
            "Authorization": f"HMAC-SHA256 {self.api_key}:{signature}",
            "X-Timestamp": timestamp,
            "Content-Type": "application/json",
        }

    async def get_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
    ) -> List[Transaction]:
        """Get transactions from SwiftPOS API."""
        path = "/transactions"
        headers = self._sign_request("GET", path)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if limit:
            params["limit"] = limit

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}{path}",
                headers=headers,
                params=params,
                timeout=30.0,
            )
            response.raise_for_status()

            data = response.json()
            return [
                Transaction(
                    transaction_id=t["id"],
                    timestamp=datetime.fromisoformat(t["timestamp"]),
                    amount=float(t["amount"]),
                    covers=int(t.get("covers", 1)),
                    product_categories=t.get("product_categories", {}),
                )
                for t in data.get("transactions", [])
            ]

    async def get_hourly_sales(
        self,
        date: datetime,
    ) -> List[HourlySales]:
        """Get hourly sales breakdown from SwiftPOS API."""
        path = f"/daily/{date.strftime('%Y-%m-%d')}/hourly"
        headers = self._sign_request("GET", path)

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}{path}",
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()

            data = response.json()
            return [
                HourlySales(
                    hour=int(h["hour"]),
                    revenue=float(h["revenue"]),
                    covers=int(h["covers"]),
                    orders=int(h["orders"]),
                )
                for h in data.get("hourly", [])
            ]

    async def get_daily_summary(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[DailySummary]:
        """Get daily summaries from SwiftPOS API."""
        path = "/daily"
        headers = self._sign_request("GET", path)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}{path}",
                headers=headers,
                params=params,
                timeout=30.0,
            )
            response.raise_for_status()

            data = response.json()
            summaries = []
            for d in data.get("daily", []):
                hourly = [
                    HourlySales(
                        hour=int(h["hour"]),
                        revenue=float(h["revenue"]),
                        covers=int(h["covers"]),
                        orders=int(h["orders"]),
                    )
                    for h in d.get("hourly", [])
                ]
                summaries.append(
                    DailySummary(
                        date=datetime.fromisoformat(d["date"]),
                        total_revenue=float(d["total_revenue"]),
                        covers=int(d["covers"]),
                        avg_spend=float(d["avg_spend"]),
                        product_mix=d.get("product_mix", {}),
                        hourly_breakdown=hourly,
                    )
                )
            return summaries

    async def get_product_mix(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[ProductMix]:
        """Get product category mix from SwiftPOS API."""
        path = "/products/mix"
        headers = self._sign_request("GET", path)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}{path}",
                headers=headers,
                params=params,
                timeout=30.0,
            )
            response.raise_for_status()

            data = response.json()
            return [
                ProductMix(
                    category=p["category"],
                    revenue=float(p["revenue"]),
                    percentage=float(p["percentage"]),
                    transaction_count=int(p["transaction_count"]),
                )
                for p in data.get("product_mix", [])
            ]

    async def get_trading_patterns(self) -> List[TradingPattern]:
        """Get trading patterns from SwiftPOS API."""
        path = "/patterns/trading"
        headers = self._sign_request("GET", path)

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}{path}",
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()

            data = response.json()
            return [
                TradingPattern(
                    day_of_week=DayOfWeek(p["day_of_week"]),
                    hourly_curve=p["hourly_curve"],
                    avg_revenue=float(p["avg_revenue"]),
                    avg_covers=int(p["avg_covers"]),
                    product_mix=p.get("product_mix", {}),
                )
                for p in data.get("patterns", [])
            ]


# ============================================================================
# Demand Signal Extractor
# ============================================================================

class DemandSignalExtractor:
    """Extract demand signals from POS data for roster optimization."""

    @staticmethod
    def extract_demand_curve(
        daily_summaries: List[DailySummary],
    ) -> Dict[DayOfWeek, List[float]]:
        """Extract typical demand curve by day of week.

        Args:
            daily_summaries: List of daily sales summaries

        Returns:
            Dict mapping day of week to hourly demand curve (0-23)
        """
        # Group by day of week
        by_day = {day: [] for day in DayOfWeek}

        for summary in daily_summaries:
            day = DayOfWeek.from_date(summary.date)
            daily_total = summary.total_revenue

            if daily_total > 0 and summary.hourly_breakdown:
                hourly_pcts = [
                    (h.revenue / daily_total) * 100
                    for h in summary.hourly_breakdown
                ]
                by_day[day].append(hourly_pcts)

        # Calculate averages
        curves = {}
        for day, hour_lists in by_day.items():
            if hour_lists:
                # Average across all observations of this day
                avg_curve = [
                    sum(hour_lists[i][h] for i in range(len(hour_lists)))
                    / len(hour_lists)
                    for h in range(24)
                ]
                curves[day] = avg_curve
            else:
                # Default uniform curve if no data
                curves[day] = [100.0 / 24] * 24

        return curves

    @staticmethod
    def detect_anomalies(
        recent_data: List[DailySummary],
        historical_patterns: Dict[DayOfWeek, TradingPattern],
    ) -> List[Dict]:
        """Detect unusual trading days.

        Args:
            recent_data: Recent daily summaries
            historical_patterns: Historical trading patterns by day

        Returns:
            List of dicts with anomaly details
        """
        anomalies = []

        for summary in recent_data:
            day_of_week = DayOfWeek.from_date(summary.date)
            pattern = historical_patterns.get(day_of_week)

            if not pattern:
                continue

            # Compare revenue to expected range
            expected = pattern.avg_revenue
            actual = summary.total_revenue
            deviation = abs(actual - expected) / expected if expected > 0 else 0

            # Flag if >25% deviation
            if deviation > 0.25:
                anomalies.append({
                    "date": summary.date,
                    "day_of_week": day_of_week.value,
                    "actual_revenue": actual,
                    "expected_revenue": expected,
                    "deviation_pct": deviation * 100,
                    "anomaly_type": "unusually_high" if actual > expected else "unusually_low",
                })

        return anomalies

    @staticmethod
    def calculate_covers_per_staff_hour(
        sales_data: List[DailySummary],
        roster_hours: Dict[datetime, float],
    ) -> float:
        """Calculate efficiency metric: covers per staff hour.

        Args:
            sales_data: Daily sales summaries
            roster_hours: Dict mapping date to total staff hours scheduled

        Returns:
            Average covers per staff hour
        """
        total_covers = 0
        total_hours = 0

        for summary in sales_data:
            if summary.date in roster_hours:
                total_covers += summary.covers
                total_hours += roster_hours[summary.date]

        if total_hours > 0:
            return total_covers / total_hours
        return 0.0

    @staticmethod
    def predict_revenue_range(
        date: datetime,
        day_of_week: DayOfWeek,
        signals: Dict[DayOfWeek, TradingPattern],
    ) -> RevenueRange:
        """Predict revenue range for a date.

        Args:
            date: Target date
            day_of_week: Day of week
            signals: Trading patterns by day of week

        Returns:
            Revenue range prediction with confidence
        """
        pattern = signals.get(day_of_week)

        if not pattern:
            # Default if no pattern available
            return RevenueRange(
                date=date,
                low=5000,
                expected=7500,
                high=10000,
                confidence=0.3,
            )

        expected = pattern.avg_revenue
        # Range is ±20% with 0.8 confidence
        low = expected * 0.8
        high = expected * 1.2

        return RevenueRange(
            date=date,
            low=low,
            expected=expected,
            high=high,
            confidence=0.8,
        )


# ============================================================================
# Demo SwiftPOS Adapter
# ============================================================================

class DemoSwiftPOSAdapter(POSAdapter):
    """Demo adapter with realistic Brisbane hotel data."""

    # Base daily revenue by day of week (AUD)
    BASE_REVENUE = {
        DayOfWeek.MONDAY: 4200,
        DayOfWeek.TUESDAY: 4800,
        DayOfWeek.WEDNESDAY: 6500,
        DayOfWeek.THURSDAY: 5500,
        DayOfWeek.FRIDAY: 9800,
        DayOfWeek.SATURDAY: 11500,
        DayOfWeek.SUNDAY: 7200,
    }

    # Base covers by day of week
    BASE_COVERS = {
        DayOfWeek.MONDAY: 85,
        DayOfWeek.TUESDAY: 95,
        DayOfWeek.WEDNESDAY: 130,
        DayOfWeek.THURSDAY: 110,
        DayOfWeek.FRIDAY: 210,
        DayOfWeek.SATURDAY: 260,
        DayOfWeek.SUNDAY: 155,
    }

    # Hourly distribution as % of daily revenue
    HOURLY_DISTRIBUTION = {
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
        7: 1,
        8: 2,
        9: 3,
        10: 4,
        11: 8,
        12: 15,  # lunch peak
        13: 10,  # lunch peak
        14: 5,   # afternoon lull
        15: 5,   # afternoon lull
        16: 5,   # afternoon lull
        17: 5,   # afternoon lull
        18: 10,  # dinner prep
        19: 15,  # dinner peak
        20: 15,  # dinner peak
        21: 10,  # dinner peak
        22: 4,   # late night
        23: 2,   # late night
    }

    # Product mix percentages
    PRODUCT_MIX = {
        "drinks": 0.45,
        "food": 0.35,
        "delivery": 0.15,
        "other": 0.05,
    }

    def __init__(self, seed: Optional[int] = None):
        """Initialize demo adapter.

        Args:
            seed: Random seed for reproducibility
        """
        if seed is not None:
            random.seed(seed)

        # Generate 8 weeks of historical data
        self.historical_data = self._generate_historical_data()

    def _generate_historical_data(self) -> List[DailySummary]:
        """Generate 8 weeks of realistic demo data."""
        data = []
        today = datetime.now().date()

        # Generate 8 weeks (56 days) of historical data
        for days_back in range(56, 0, -1):
            date = datetime.combine(
                today - timedelta(days=days_back),
                time(0, 0, 0),
            )

            day_of_week = DayOfWeek.from_date(date)
            base_revenue = self.BASE_REVENUE[day_of_week]
            base_covers = self.BASE_COVERS[day_of_week]

            # Add anomalies
            # Anomaly 1: Event day (2 weeks ago, Saturday, 2x normal)
            if date.date() == (today - timedelta(days=14)):
                revenue_multiplier = 2.0
                covers_multiplier = 2.0
            # Anomaly 2: Dead day (1 week ago, Monday, 0.5x normal)
            elif date.date() == (today - timedelta(days=7)):
                revenue_multiplier = 0.5
                covers_multiplier = 0.5
            else:
                # Normal ±15% variation
                revenue_multiplier = random.uniform(0.85, 1.15)
                covers_multiplier = random.uniform(0.80, 1.20)

            daily_revenue = base_revenue * revenue_multiplier
            daily_covers = int(base_covers * covers_multiplier)
            avg_spend = daily_revenue / daily_covers if daily_covers > 0 else 50

            # Generate hourly breakdown
            hourly_breakdown = []
            for hour in range(24):
                hourly_pct = self.HOURLY_DISTRIBUTION[hour]
                revenue = daily_revenue * (hourly_pct / 100)

                # Covers proportional to revenue
                covers = int((daily_covers * hourly_pct / 100)) if hourly_pct > 0 else 0
                orders = max(covers // 2, covers // 3) if covers > 0 else 0

                hourly_breakdown.append(
                    HourlySales(
                        hour=hour,
                        revenue=revenue,
                        covers=covers,
                        orders=orders,
                    )
                )

            summary = DailySummary(
                date=date,
                total_revenue=daily_revenue,
                covers=daily_covers,
                avg_spend=avg_spend,
                product_mix=self.PRODUCT_MIX.copy(),
                hourly_breakdown=hourly_breakdown,
            )
            data.append(summary)

        return data

    async def get_transactions(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
    ) -> List[Transaction]:
        """Get mock transactions."""
        transactions = []
        current = start_date

        while current <= end_date:
            # Find matching daily summary
            summary = None
            for s in self.historical_data:
                if s.date.date() == current.date():
                    summary = s
                    break

            if summary:
                # Generate transactions from summary
                day_covers = summary.covers
                covers_per_transaction = random.randint(1, 3)
                num_transactions = max(1, day_covers // covers_per_transaction)

                revenue_per_transaction = summary.total_revenue / num_transactions

                for i in range(num_transactions):
                    if limit and len(transactions) >= limit:
                        break

                    # Distribute across day with hourly pattern
                    hour = random.choices(
                        range(24),
                        weights=[self.HOURLY_DISTRIBUTION[h] for h in range(24)],
                        k=1,
                    )[0]
                    minute = random.randint(0, 59)

                    timestamp = current.replace(hour=hour, minute=minute)

                    transactions.append(
                        Transaction(
                            transaction_id=f"TXN-{current.date()}-{i:04d}",
                            timestamp=timestamp,
                            amount=revenue_per_transaction,
                            covers=covers_per_transaction,
                            product_categories={
                                k: revenue_per_transaction * v
                                for k, v in self.PRODUCT_MIX.items()
                            },
                        )
                    )

            current += timedelta(days=1)

        return transactions[:limit] if limit else transactions

    async def get_hourly_sales(
        self,
        date: datetime,
    ) -> List[HourlySales]:
        """Get hourly sales for a date."""
        for summary in self.historical_data:
            if summary.date.date() == date.date():
                return summary.hourly_breakdown

        # Return empty list if date not in historical data
        return []

    async def get_daily_summary(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[DailySummary]:
        """Get daily summaries for date range."""
        summaries = []
        for summary in self.historical_data:
            if start_date.date() <= summary.date.date() <= end_date.date():
                summaries.append(summary)
        return summaries

    async def get_product_mix(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[ProductMix]:
        """Get product mix for date range."""
        # Aggregate across all summaries in date range
        category_totals = {cat: 0.0 for cat in self.PRODUCT_MIX.keys()}
        transaction_counts = {cat: 0 for cat in self.PRODUCT_MIX.keys()}
        total_revenue = 0.0

        for summary in self.historical_data:
            if start_date.date() <= summary.date.date() <= end_date.date():
                total_revenue += summary.total_revenue
                for category, pct in self.PRODUCT_MIX.items():
                    revenue = summary.total_revenue * pct
                    category_totals[category] += revenue
                    # Estimate transactions
                    transaction_counts[category] += max(1, int(summary.covers * pct))

        product_mix = []
        for category, revenue in category_totals.items():
            percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
            product_mix.append(
                ProductMix(
                    category=category,
                    revenue=revenue,
                    percentage=percentage,
                    transaction_count=transaction_counts[category],
                )
            )

        return product_mix

    async def get_trading_patterns(self) -> List[TradingPattern]:
        """Get trading patterns by day of week."""
        patterns = []

        # Group historical data by day of week
        by_day = {day: [] for day in DayOfWeek}
        for summary in self.historical_data:
            day = DayOfWeek.from_date(summary.date)
            by_day[day].append(summary)

        for day_of_week in DayOfWeek:
            summaries = by_day[day_of_week]

            if not summaries:
                continue

            # Calculate averages
            avg_revenue = sum(s.total_revenue for s in summaries) / len(summaries)
            avg_covers = int(sum(s.covers for s in summaries) / len(summaries))

            # Build hourly curve
            hourly_revenues = [[] for _ in range(24)]
            for summary in summaries:
                for hourly in summary.hourly_breakdown:
                    hourly_revenues[hourly.hour].append(hourly.revenue)

            hourly_curve = []
            for hour_revenues in hourly_revenues:
                if hour_revenues:
                    avg_hourly = sum(hour_revenues) / len(hour_revenues)
                    pct = (avg_hourly / avg_revenue * 100) if avg_revenue > 0 else 0
                    hourly_curve.append(pct)
                else:
                    hourly_curve.append(0.0)

            patterns.append(
                TradingPattern(
                    day_of_week=day_of_week,
                    hourly_curve=hourly_curve,
                    avg_revenue=avg_revenue,
                    avg_covers=avg_covers,
                    product_mix=self.PRODUCT_MIX.copy(),
                )
            )

        return patterns


# ============================================================================
# Factory Function
# ============================================================================

def get_pos_adapter() -> POSAdapter:
    """Get POS adapter instance.

    Returns SwiftPOSClient if credentials available,
    otherwise returns demo adapter.
    """
    api_key = os.getenv("SWIFTPOS_API_KEY")
    secret_key = os.getenv("SWIFTPOS_SECRET_KEY")

    if api_key and secret_key:
        return SwiftPOSClient(
            api_key=api_key,
            secret_key=secret_key,
        )
    else:
        return DemoSwiftPOSAdapter()
