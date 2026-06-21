"""
Real-time POS sales ingestion and revenue-driven staffing recommendations.

Monitors live POS data, accumulates hourly revenue, compares against forecasts,
and triggers staffing recommendations via the decision engine.

Components:
- RevenueAccumulator: Thread-safe in-memory accumulator per venue/hour
- RealtimePOSFeed: Polling and webhook ingestion for POS sales
- StaffingTrigger: Monitors variance and triggers recommendations
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class HourlyRevenue:
    """Revenue data for a specific hour."""

    hour: int  # 0-23
    revenue: Decimal = Decimal("0.00")
    transaction_count: int = 0
    avg_ticket: Decimal = Decimal("0.00")
    timestamp: datetime = field(default_factory=datetime.now)

    def add_transaction(self, amount: Decimal) -> None:
        """Add a transaction to this hour."""
        self.revenue += amount
        self.transaction_count += 1
        if self.transaction_count > 0:
            self.avg_ticket = self.revenue / self.transaction_count


@dataclass
class VenueRevenueState:
    """Current revenue state for a venue on a specific day."""

    venue_id: str
    date: date
    hourly_data: dict[int, HourlyRevenue] = field(default_factory=dict)
    total_revenue: Decimal = Decimal("0.00")
    total_transactions: int = 0
    # POS transaction ids already counted today. Bounds memory to one day's
    # worth of sales: the whole state is recreated when the date rolls over
    # (or on reset_day), clearing this set.
    seen_transaction_ids: set[str] = field(default_factory=set)

    def get_hour_data(self, hour: int) -> HourlyRevenue:
        """Get or create hourly data for this hour."""
        if hour not in self.hourly_data:
            self.hourly_data[hour] = HourlyRevenue(hour=hour)
        return self.hourly_data[hour]


# ============================================================================
# Revenue Accumulator - Thread-safe in-memory revenue tracking
# ============================================================================


class RevenueAccumulator:
    """
    Thread-safe accumulator for real-time POS revenue per venue.

    Tracks hourly revenue, transaction counts, and average ticket size.
    Resets at midnight local time.
    """

    def __init__(self):
        """Initialize the accumulator."""
        self._lock = asyncio.Lock()
        self._state: dict[str, VenueRevenueState] = {}
        self._forecast_cache: dict[str, float] = {}  # venue_id -> forecast

    async def add_transaction(
        self,
        venue_id: str,
        amount: Decimal | float,
        timestamp: Optional[datetime] = None,
        transaction_id: Optional[str] = None,
    ) -> bool:
        """
        Add a transaction to the accumulator.

        Args:
            venue_id: Venue identifier.
            amount: Transaction amount.
            timestamp: Transaction time (defaults to now).
            transaction_id: POS transaction id. If provided and already seen
                for this venue/day, the transaction is ignored (deduplicated)
                so webhook re-delivery does not double-count revenue and fire
                false staffing triggers.

        Returns:
            True if the transaction was counted, False if it was a duplicate.
        """
        if timestamp is None:
            timestamp = datetime.now()

        if isinstance(amount, float):
            amount = Decimal(str(amount))

        hour = timestamp.hour
        today = timestamp.date()

        async with self._lock:
            key = venue_id
            if key not in self._state or self._state[key].date != today:
                self._state[key] = VenueRevenueState(venue_id=venue_id, date=today)

            state = self._state[key]

            # Dedup by POS transaction id (bounded to today's set).
            if transaction_id is not None:
                tid = str(transaction_id)
                if tid in state.seen_transaction_ids:
                    logger.info(
                        f"Ignoring duplicate POS transaction {tid} for venue {venue_id}"
                    )
                    return False
                state.seen_transaction_ids.add(tid)

            hour_data = state.get_hour_data(hour)
            hour_data.add_transaction(amount)

            state.total_revenue += amount
            state.total_transactions += 1
            return True

    async def get_current_hour(self, venue_id: str) -> Optional[dict[str, Any]]:
        """
        Get current hour revenue data.

        Returns:
            Dict with revenue, count, avg_ticket, or None if no data.
        """
        async with self._lock:
            state = self._state.get(venue_id)
            if not state:
                return None

            now = datetime.now()
            hour_data = state.hourly_data.get(now.hour)

            if hour_data is None or hour_data.transaction_count == 0:
                return None

            return {
                "hour": hour_data.hour,
                "revenue": float(hour_data.revenue),
                "transaction_count": hour_data.transaction_count,
                "avg_ticket": float(hour_data.avg_ticket),
            }

    async def get_today_total(self, venue_id: str) -> Optional[dict[str, Any]]:
        """
        Get today's total revenue.

        Returns:
            Dict with revenue, count, hours_data, or None if no data.
        """
        async with self._lock:
            state = self._state.get(venue_id)
            if not state:
                return None

            hours_data = []
            for hour in range(24):
                if hour in state.hourly_data:
                    hd = state.hourly_data[hour]
                    if hd.transaction_count > 0:
                        hours_data.append({
                            "hour": hd.hour,
                            "revenue": float(hd.revenue),
                            "count": hd.transaction_count,
                            "avg_ticket": float(hd.avg_ticket),
                        })

            return {
                "date": state.date.isoformat(),
                "total_revenue": float(state.total_revenue),
                "transaction_count": state.total_transactions,
                "hours_data": hours_data,
            }

    async def get_variance(
        self,
        venue_id: str,
        forecast_revenue: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Get variance between actual and forecast.

        Args:
            venue_id: Venue identifier.
            forecast_revenue: Expected revenue (if None, uses cached forecast).

        Returns:
            Dict with forecast, actual, variance_pct, or None if no data.
        """
        async with self._lock:
            state = self._state.get(venue_id)
            if not state:
                return None

            if forecast_revenue is None:
                forecast_revenue = self._forecast_cache.get(venue_id, 0.0)

            actual = float(state.total_revenue)

            if forecast_revenue <= 0:
                variance_pct = 0.0
            else:
                variance_pct = ((actual - forecast_revenue) / forecast_revenue) * 100

            return {
                "forecast": forecast_revenue,
                "actual": actual,
                "variance_pct": round(variance_pct, 2),
                "status": "on_track" if abs(variance_pct) <= 15 else (
                    "above_forecast" if variance_pct > 15 else "below_forecast"
                ),
            }

    async def reset_day(self, venue_id: str) -> None:
        """Reset daily accumulator (called at midnight)."""
        async with self._lock:
            if venue_id in self._state:
                del self._state[venue_id]

    async def set_forecast(self, venue_id: str, forecast: float) -> None:
        """Set the forecast for variance comparison."""
        async with self._lock:
            self._forecast_cache[venue_id] = forecast


# ============================================================================
# Staffing Trigger - Variance monitoring and recommendation
# ============================================================================


class StaffingTrigger:
    """
    Monitors revenue variance and triggers staffing recommendations.

    Compares actual revenue against forecast:
    - >15% above: consider calling in staff
    - >20% below: consider sending staff home early

    Implements cooldown to avoid duplicate alerts within 30 minutes.
    """

    def __init__(self, decision_engine, websocket_hub):
        """
        Initialize the trigger.

        Args:
            decision_engine: Module with make_decision function.
            websocket_hub: Module for broadcasting alerts.
        """
        self._decision_engine = decision_engine
        self._websocket_hub = websocket_hub
        self._last_trigger: dict[str, dict[str, datetime]] = defaultdict(dict)
        self._cooldown_minutes = 30

    async def check_and_trigger(
        self,
        venue_id: str,
        variance_data: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """
        Check variance and trigger recommendation if needed.

        Args:
            venue_id: Venue identifier.
            variance_data: Dict with forecast, actual, variance_pct.

        Returns:
            Recommendation dict if triggered, None otherwise.
        """
        variance_pct = variance_data.get("variance_pct", 0.0)
        now = datetime.now()

        # Determine action based on variance
        action = None
        if variance_pct > 15:  # Above forecast by 15%+
            action = "call_in"
        elif variance_pct < -20:  # Below forecast by 20%+
            action = "send_home"

        if action is None:
            return None

        # Check cooldown
        last_trigger = self._last_trigger[venue_id].get(action)
        if last_trigger is not None:
            elapsed = (now - last_trigger).total_seconds() / 60
            if elapsed < self._cooldown_minutes:
                return None  # Still in cooldown

        # Update last trigger time
        self._last_trigger[venue_id][action] = now

        # Build recommendation
        recommendation = {
            "venue_id": venue_id,
            "action": action,
            "variance_pct": variance_pct,
            "forecast": variance_data.get("forecast"),
            "actual": variance_data.get("actual"),
            "timestamp": now.isoformat(),
        }

        if action == "call_in":
            recommendation["description"] = (
                f"Revenue is {variance_pct:.1f}% above forecast. "
                "Consider calling in additional staff to handle demand."
            )
            recommendation["priority"] = "high"
        elif action == "send_home":
            recommendation["description"] = (
                f"Revenue is {abs(variance_pct):.1f}% below forecast. "
                "Consider sending some staff home early to control costs."
            )
            recommendation["priority"] = "medium"

        # Broadcast via WebSocket
        try:
            await self._websocket_hub.broadcast({
                "type": "staffing_recommendation",
                "venue_id": venue_id,
                "action": action,
                "variance_pct": variance_pct,
                "description": recommendation.get("description"),
                "timestamp": recommendation["timestamp"],
            })
        except Exception as e:
            logger.error(f"Failed to broadcast staffing recommendation: {e}")

        return recommendation


# ============================================================================
# Real-time POS Feed - Polling and webhook ingestion
# ============================================================================


class RealtimePOSFeed:
    """
    Manages real-time POS data ingestion via polling or webhooks.

    Supports:
    - start_polling(): Start periodic polling for venue
    - ingest_sale(): Accept push notifications from POS webhook
    - get_live_revenue(): Current hour/day totals
    - get_hourly_breakdown(): Hourly revenue details
    """

    def __init__(self, accumulator: RevenueAccumulator, trigger: StaffingTrigger):
        """
        Initialize the feed.

        Args:
            accumulator: RevenueAccumulator instance.
            trigger: StaffingTrigger instance.
        """
        self._accumulator = accumulator
        self._trigger = trigger
        self._polling_tasks: dict[str, asyncio.Task] = {}
        self._polling_intervals: dict[str, int] = {}

    async def start_polling(
        self,
        venue_id: str,
        pos_adapter: Any,
        interval_seconds: int = 60,
        forecast_revenue: Optional[float] = None,
    ) -> None:
        """
        Start polling POS for latest sales.

        Args:
            venue_id: Venue identifier.
            pos_adapter: POS adapter with get_latest_sales() method.
            interval_seconds: Poll interval in seconds (default 60).
            forecast_revenue: Expected revenue for variance comparison.
        """
        if venue_id in self._polling_tasks:
            logger.warning(f"Polling already running for {venue_id}")
            return

        if forecast_revenue is not None:
            await self._accumulator.set_forecast(venue_id, forecast_revenue)

        self._polling_intervals[venue_id] = interval_seconds
        task = asyncio.create_task(
            self._polling_loop(venue_id, pos_adapter, interval_seconds)
        )
        self._polling_tasks[venue_id] = task

        logger.info(
            f"Started polling POS for {venue_id} every {interval_seconds}s"
        )

    async def stop_polling(self, venue_id: str) -> None:
        """Stop polling for a venue."""
        task = self._polling_tasks.pop(venue_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logger.info(f"Stopped polling for {venue_id}")

    async def ingest_sale(
        self,
        venue_id: str,
        sale_data: dict[str, Any],
    ) -> None:
        """
        Ingest a sale from POS webhook.

        Args:
            venue_id: Venue identifier.
            sale_data: Sale dict with at least 'amount' and optional 'timestamp'.
        """
        amount = sale_data.get("amount")
        if amount is None:
            logger.warning(f"Sale missing amount: {sale_data}")
            return

        timestamp = sale_data.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except (ValueError, TypeError):
                timestamp = None

        # POS webhooks can re-deliver the same sale; dedup by transaction id.
        transaction_id = sale_data.get("transaction_id") or sale_data.get("id")

        counted = await self._accumulator.add_transaction(
            venue_id=venue_id,
            amount=amount,
            timestamp=timestamp,
            transaction_id=transaction_id,
        )

        # Skip variance re-evaluation for duplicates — re-delivery must not
        # fire a false 'call_in'/'send_home' staffing trigger.
        if not counted:
            return

        # Check variance after each (newly counted) transaction
        await self._check_variance_and_trigger(venue_id)

    async def get_live_revenue(self, venue_id: str) -> Optional[dict[str, Any]]:
        """
        Get current hour and day revenue.

        Returns:
            Dict with current_hour and today_total.
        """
        current_hour = await self._accumulator.get_current_hour(venue_id)
        today_total = await self._accumulator.get_today_total(venue_id)

        return {
            "venue_id": venue_id,
            "current_hour": current_hour,
            "today_total": today_total,
            "timestamp": datetime.now().isoformat(),
        }

    async def get_hourly_breakdown(
        self,
        venue_id: str,
        date: Optional[date] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Get hourly revenue breakdown.

        Args:
            venue_id: Venue identifier.
            date: Date to retrieve (defaults to today).

        Returns:
            Dict with hourly data.
        """
        today_total = await self._accumulator.get_today_total(venue_id)
        if not today_total:
            return None

        return {
            "venue_id": venue_id,
            "date": date.isoformat() if date else today_total["date"],
            "hours": today_total["hours_data"],
            "total_revenue": today_total["total_revenue"],
            "transaction_count": today_total["transaction_count"],
        }

    async def get_variance(self, venue_id: str) -> Optional[dict[str, Any]]:
        """Get current variance between forecast and actual."""
        return await self._accumulator.get_variance(venue_id)

    # Private methods

    async def _polling_loop(
        self,
        venue_id: str,
        pos_adapter: Any,
        interval_seconds: int,
    ) -> None:
        """Background polling loop."""
        while True:
            try:
                await asyncio.sleep(interval_seconds)

                # Get latest sales from POS adapter
                try:
                    sales = await pos_adapter.get_latest_sales()
                except Exception as e:
                    logger.error(f"Failed to fetch sales for {venue_id}: {e}")
                    continue

                # Ingest each sale
                for sale in sales:
                    await self.ingest_sale(venue_id, sale)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Polling loop error for {venue_id}: {e}")

    async def _check_variance_and_trigger(self, venue_id: str) -> None:
        """Check variance and trigger staffing recommendation if needed."""
        variance = await self._accumulator.get_variance(venue_id)
        if variance:
            await self._trigger.check_and_trigger(venue_id, variance)
