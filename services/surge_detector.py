"""
Real-time demand surge detection with auto staff-up suggestions.

Monitors live POS data against forecasts, detects surges/quiet periods,
and recommends on-call staff to bring in or release early.

Components:
- SurgeStatus: Current demand surge state
- OnCallEmployee: Employee available for immediate on-call
- SurgeEvent: Historical surge event
- QuietStatus: Opposite of surge (overstaffed detection)
- SurgeDetector: Main detector service
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Any, Callable

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================


@dataclass
class OnCallEmployee:
    """Employee available for immediate on-call."""

    employee_id: str
    name: str
    phone: Optional[str] = None
    skills: list[str] = field(default_factory=list)
    estimated_arrival_minutes: int = 30
    hourly_cost: Decimal = Decimal("0.00")
    response_time_score: float = 0.0  # 0-1, higher = faster

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "employee_id": self.employee_id,
            "name": self.name,
            "phone": self.phone,
            "skills": self.skills,
            "estimated_arrival_minutes": self.estimated_arrival_minutes,
            "hourly_cost": str(self.hourly_cost),
            "response_time_score": round(self.response_time_score, 2),
        }


@dataclass
class SurgeStatus:
    """Current demand surge state for a venue."""

    venue_id: str
    timestamp: str  # ISO format
    is_surging: bool
    surge_level: str  # "none", "mild", "moderate", "critical"
    deviation_pct: float  # (actual - predicted) / predicted * 100
    predicted_covers: float
    estimated_actual_covers: float
    additional_staff_needed: dict[str, int] = field(
        default_factory=dict
    )  # role -> count
    available_oncall: list[OnCallEmployee] = field(default_factory=list)
    suggested_action: str = ""
    estimated_extra_cost: Decimal = Decimal("0.00")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "venue_id": self.venue_id,
            "timestamp": self.timestamp,
            "is_surging": self.is_surging,
            "surge_level": self.surge_level,
            "deviation_pct": round(self.deviation_pct, 2),
            "predicted_covers": round(self.predicted_covers, 1),
            "estimated_actual_covers": round(self.estimated_actual_covers, 1),
            "additional_staff_needed": self.additional_staff_needed,
            "available_oncall": [emp.to_dict() for emp in self.available_oncall],
            "suggested_action": self.suggested_action,
            "estimated_extra_cost": str(self.estimated_extra_cost),
        }


@dataclass
class QuietStatus:
    """Opposite of surge: overstaffed detection."""

    venue_id: str
    timestamp: str  # ISO format
    is_quiet: bool
    deviation_pct: float  # (actual - predicted) / predicted * 100
    staff_releasable: int  # Number of staff that can be released
    estimated_savings: Decimal
    suggested_action: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "venue_id": self.venue_id,
            "timestamp": self.timestamp,
            "is_quiet": self.is_quiet,
            "deviation_pct": round(self.deviation_pct, 2),
            "staff_releasable": self.staff_releasable,
            "estimated_savings": str(self.estimated_savings),
            "suggested_action": self.suggested_action,
        }


@dataclass
class SurgeEvent:
    """Historical surge event for trend analysis."""

    venue_id: str
    timestamp: str  # ISO format
    surge_level: str
    deviation_pct: float
    actual_covers: float
    staff_called_in: int
    staff_released: int = 0


# ============================================================================
# Surge Detector - Main service
# ============================================================================


class SurgeDetector:
    """
    Detects real-time demand surges and recommends staff adjustments.

    Monitors POS data against forecasts, detects surge/quiet periods,
    and suggests on-call staff to call in or release.
    """

    def __init__(self, database, pos_realtime_feed, dispatcher=None):
        """
        Initialize the surge detector.

        Args:
            database: Database instance for fetching forecasts/employees.
            pos_realtime_feed: RealtimePOSFeed instance for live data.
            dispatcher: WSEventDispatcher for broadcasting alerts.
        """
        self._db = database
        self._pos_feed = pos_realtime_feed
        self._dispatcher = dispatcher
        self._monitoring_tasks: dict[str, asyncio.Task] = {}
        self._surge_history: dict[str, list[SurgeEvent]] = {}
        self._quiet_cooldown: dict[str, datetime] = {}  # venue -> last quiet alert

    async def check_surge(self, venue_id: str) -> SurgeStatus:
        """
        Check current surge status for a venue.

        Returns:
            SurgeStatus with current demand state.
        """
        now = datetime.now()
        current_hour = now.hour

        # Get forecast for current hour
        try:
            forecast = self._db.get_demand_forecast(
                venue_id=venue_id, hour=current_hour
            )
            predicted_covers = (
                forecast.predicted_covers if forecast else 50.0
            )  # Default 50
        except Exception as e:
            logger.error(f"Failed to get forecast for {venue_id}: {e}")
            predicted_covers = 50.0

        # Get live POS data
        try:
            live_revenue = await self._pos_feed.get_live_revenue(venue_id)
            if not live_revenue or not live_revenue.get("current_hour"):
                # No POS data yet this hour
                estimated_actual_covers = 0.0
                transaction_count = 0
            else:
                hour_data = live_revenue["current_hour"]
                transaction_count = hour_data.get("transaction_count", 0)
                avg_ticket = float(hour_data.get("avg_ticket", 50.0))

                # Estimate covers from transactions
                # Rough heuristic: covers ≈ transaction count (assuming ~1 transaction per cover)
                estimated_actual_covers = float(transaction_count)
                if estimated_actual_covers == 0 and avg_ticket > 0:
                    # Fallback: use revenue/avg_ticket
                    revenue = float(hour_data.get("revenue", 0.0))
                    estimated_actual_covers = revenue / avg_ticket if avg_ticket > 0 else 0.0
        except Exception as e:
            logger.error(f"Failed to get live revenue for {venue_id}: {e}")
            estimated_actual_covers = 0.0

        # Calculate deviation
        if predicted_covers > 0:
            deviation_pct = (
                (estimated_actual_covers - predicted_covers) / predicted_covers * 100
            )
        else:
            deviation_pct = 0.0

        # Determine surge level
        surge_level = "none"
        is_surging = False
        if deviation_pct > 60:
            surge_level = "critical"
            is_surging = True
        elif deviation_pct > 40:
            surge_level = "moderate"
            is_surging = True
        elif deviation_pct > 20:
            surge_level = "mild"
            is_surging = True

        # Calculate additional staff needed
        current_staff = self._get_current_staff_count(venue_id)
        additional_needed = self.calculate_staff_needed(
            estimated_actual_covers, predicted_covers, current_staff
        )

        # Find available on-call staff
        available_oncall = await self.find_available_oncall(
            venue_id, additional_needed
        )

        # Calculate extra cost
        extra_cost = self._estimate_extra_cost(available_oncall)

        # Build suggested action
        suggested_action = self._build_suggested_action(
            surge_level, additional_needed, available_oncall
        )

        status = SurgeStatus(
            venue_id=venue_id,
            timestamp=now.isoformat(),
            is_surging=is_surging,
            surge_level=surge_level,
            deviation_pct=deviation_pct,
            predicted_covers=predicted_covers,
            estimated_actual_covers=estimated_actual_covers,
            additional_staff_needed=additional_needed,
            available_oncall=available_oncall,
            suggested_action=suggested_action,
            estimated_extra_cost=extra_cost,
        )

        # Broadcast if surging
        if is_surging and self._dispatcher:
            try:
                await self._dispatcher.send_alert(
                    venue_id=venue_id,
                    alert_type="demand_surge",
                    severity="critical" if surge_level == "critical" else "warning",
                    message=suggested_action,
                )
            except Exception as e:
                logger.error(f"Failed to broadcast surge alert: {e}")

        return status

    async def detect_quiet_period(self, venue_id: str) -> QuietStatus:
        """
        Detect if venue is quieter than expected (overstaffed).

        Returns:
            QuietStatus with quiet period info.
        """
        now = datetime.now()
        current_hour = now.hour

        # Get forecast for current hour
        try:
            forecast = self._db.get_demand_forecast(
                venue_id=venue_id, hour=current_hour
            )
            predicted_covers = (
                forecast.predicted_covers if forecast else 50.0
            )  # Default 50
        except Exception as e:
            logger.error(f"Failed to get forecast for {venue_id}: {e}")
            predicted_covers = 50.0

        # Get live POS data
        try:
            live_revenue = await self._pos_feed.get_live_revenue(venue_id)
            if not live_revenue or not live_revenue.get("current_hour"):
                estimated_actual_covers = 0.0
            else:
                hour_data = live_revenue["current_hour"]
                transaction_count = hour_data.get("transaction_count", 0)
                estimated_actual_covers = float(transaction_count)
        except Exception as e:
            logger.error(f"Failed to get live revenue for {venue_id}: {e}")
            estimated_actual_covers = 0.0

        # Calculate deviation
        if predicted_covers > 0:
            deviation_pct = (
                (estimated_actual_covers - predicted_covers) / predicted_covers * 100
            )
        else:
            deviation_pct = 0.0

        # Detect quiet period: >20% below forecast
        is_quiet = deviation_pct < -20
        current_staff = self._get_current_staff_count(venue_id)

        # Calculate how many staff can be released
        staff_releasable = 0
        if is_quiet:
            # Rough calculation: release staff proportional to quiet level
            required_staff = max(1, int(current_staff * (1 + deviation_pct / 100)))
            staff_releasable = max(0, current_staff - required_staff)

        # Estimate savings from releasing staff
        avg_hourly_cost = self._get_average_hourly_cost(venue_id)
        estimated_savings = Decimal(str(staff_releasable)) * avg_hourly_cost

        # Build suggested action
        suggested_action = ""
        if is_quiet and staff_releasable > 0:
            suggested_action = (
                f"Venue is {abs(deviation_pct):.1f}% quieter than forecast. "
                f"Consider releasing {staff_releasable} staff to save "
                f"${estimated_savings:.2f}/hour."
            )

        status = QuietStatus(
            venue_id=venue_id,
            timestamp=now.isoformat(),
            is_quiet=is_quiet,
            deviation_pct=deviation_pct,
            staff_releasable=staff_releasable,
            estimated_savings=estimated_savings,
            suggested_action=suggested_action,
        )

        # Broadcast if quiet (only once per hour)
        if is_quiet and self._dispatcher:
            last_alert = self._quiet_cooldown.get(venue_id)
            if last_alert is None or (now - last_alert).total_seconds() > 3600:
                try:
                    await self._dispatcher.send_alert(
                        venue_id=venue_id,
                        alert_type="quiet_period",
                        severity="info",
                        message=suggested_action,
                    )
                    self._quiet_cooldown[venue_id] = now
                except Exception as e:
                    logger.error(f"Failed to broadcast quiet alert: {e}")

        return status

    async def monitor_continuous(
        self, venue_id: str, callback: Optional[Callable] = None
    ) -> None:
        """
        Start continuous surge monitoring loop for a venue.

        Checks every 5 minutes and calls callback with SurgeStatus.

        Args:
            venue_id: Venue to monitor.
            callback: Async function(status: SurgeStatus) called on each check.
        """
        if venue_id in self._monitoring_tasks:
            logger.warning(f"Monitoring already running for {venue_id}")
            return

        async def _monitor_loop() -> None:
            """Background monitoring loop."""
            while True:
                try:
                    await asyncio.sleep(300)  # Check every 5 minutes

                    status = await self.check_surge(venue_id)
                    if callback:
                        try:
                            await callback(status)
                        except Exception as e:
                            logger.error(f"Callback error for {venue_id}: {e}")

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Monitoring loop error for {venue_id}: {e}")

        task = asyncio.create_task(_monitor_loop())
        self._monitoring_tasks[venue_id] = task
        logger.info(f"Started continuous surge monitoring for {venue_id}")

    async def stop_monitoring(self, venue_id: str) -> None:
        """Stop continuous monitoring for a venue."""
        task = self._monitoring_tasks.pop(venue_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logger.info(f"Stopped surge monitoring for {venue_id}")

    def calculate_staff_needed(
        self, current_covers: float, forecast_covers: float, current_staff: int
    ) -> dict[str, int]:
        """
        Calculate additional staff needed based on demand.

        Uses ratio of covers_per_staff (typically 4-6 covers per staff member).

        Args:
            current_covers: Estimated covers right now.
            forecast_covers: Forecasted covers for this hour.
            current_staff: Number of staff currently on shift.

        Returns:
            Dict of role -> count (currently just "general" role).
        """
        covers_per_staff = 5.0  # Typical ratio for hospitality

        # Calculate staff needed for actual demand
        actual_staff_needed = max(1, int(current_covers / covers_per_staff))

        # Calculate staff needed for forecast
        forecast_staff_needed = max(1, int(forecast_covers / covers_per_staff))

        # Additional staff = actual_needed - forecast_needed
        additional = max(0, actual_staff_needed - forecast_staff_needed)

        return {"general": additional} if additional > 0 else {}

    async def find_available_oncall(
        self, venue_id: str, roles_needed: dict[str, int]
    ) -> list[OnCallEmployee]:
        """
        Find available on-call employees.

        Searches for employees not currently rostered, available, and sorts by:
        1. Response time estimate (fastest first)
        2. Hourly cost (cheapest first)
        3. Skills match

        Args:
            venue_id: Venue to find staff for.
            roles_needed: Dict of role -> count needed.

        Returns:
            List of OnCallEmployee sorted by availability.
        """
        if not roles_needed:
            return []

        try:
            # Get all employees for venue
            all_employees = self._db.list_employees(venue_id)

            # Get current shifts to find who's not working
            now = datetime.now()
            current_shifts = self._db.get_active_shifts(
                venue_id, now.date(), now.hour
            )
            active_employee_ids = {shift.employee_id for shift in current_shifts}

            available = []
            for emp in all_employees:
                # Skip if already working
                if emp.id in active_employee_ids:
                    continue

                # Check availability
                day_name = now.strftime("%A").lower()
                emp_availability = emp.availability.get(day_name, [])
                is_available = self._check_availability(emp_availability, now.hour)

                if not is_available:
                    continue

                # Create OnCallEmployee
                oncall = OnCallEmployee(
                    employee_id=emp.id,
                    name=emp.name,
                    phone=emp.phone,
                    skills=emp.skills,
                    estimated_arrival_minutes=30,  # Default 30 min
                    hourly_cost=emp.hourly_base_rate,
                    response_time_score=0.8,  # Default 0.8 (80% likely to respond)
                )
                available.append(oncall)

            # Sort by response time (fastest first), then cost (cheapest), then skills
            available.sort(
                key=lambda e: (
                    -e.response_time_score,  # Negative = descending (faster first)
                    e.hourly_cost,
                )
            )

            # Return up to the number needed (sum all role counts)
            total_needed = sum(roles_needed.values())
            return available[:total_needed]

        except Exception as e:
            logger.error(f"Failed to find available staff for {venue_id}: {e}")
            return []

    def get_surge_history(self, venue_id: str, days: int = 7) -> list[SurgeEvent]:
        """
        Get recent surge events for a venue.

        Args:
            venue_id: Venue to get history for.
            days: Number of days to look back.

        Returns:
            List of SurgeEvent sorted by timestamp (newest first).
        """
        try:
            events = self._db.get_surge_events(venue_id, days=days)
            return sorted(events, key=lambda e: e.timestamp, reverse=True)
        except Exception as e:
            logger.error(f"Failed to get surge history for {venue_id}: {e}")
            return []

    # ========================================================================
    # Private helpers
    # ========================================================================

    def _get_current_staff_count(self, venue_id: str) -> int:
        """Get number of staff currently on shift."""
        try:
            now = datetime.now()
            shifts = self._db.get_active_shifts(
                venue_id, now.date(), now.hour
            )
            return len(shifts)
        except Exception as e:
            logger.error(f"Failed to get current staff count: {e}")
            return 0

    def _get_average_hourly_cost(self, venue_id: str) -> Decimal:
        """Get average hourly cost for staff at venue."""
        try:
            employees = self._db.list_employees(venue_id)
            if not employees:
                return Decimal("25.00")  # Default fallback
            total_cost = sum(emp.hourly_base_rate for emp in employees)
            avg = total_cost / len(employees)
            return Decimal(str(avg))
        except Exception as e:
            logger.error(f"Failed to get average hourly cost: {e}")
            return Decimal("25.00")

    def _check_availability(
        self, availability: list[dict[str, str]], current_hour: int
    ) -> bool:
        """Check if employee is available during current hour."""
        if not availability:
            return False

        for slot in availability:
            try:
                start_str = slot.get("start", "")
                end_str = slot.get("end", "")
                start_hour = int(start_str.split(":")[0])
                end_hour = int(end_str.split(":")[0])

                if start_hour <= current_hour < end_hour:
                    return True
            except (ValueError, IndexError, AttributeError):
                continue

        return False

    def _estimate_extra_cost(self, available_oncall: list[OnCallEmployee]) -> Decimal:
        """Estimate total extra cost for calling in staff."""
        if not available_oncall:
            return Decimal("0.00")

        # Assume 1-2 hour minimum call
        total = Decimal("0.00")
        for emp in available_oncall:
            total += emp.hourly_cost * Decimal("1.5")  # 1.5 hour estimate

        return total

    def _build_suggested_action(
        self,
        surge_level: str,
        additional_needed: dict[str, int],
        available_oncall: list[OnCallEmployee],
    ) -> str:
        """Build human-readable suggested action."""
        total_needed = sum(additional_needed.values())
        if total_needed == 0:
            return ""

        if surge_level == "critical":
            action = f"CRITICAL: {total_needed} additional staff needed urgently."
        elif surge_level == "moderate":
            action = f"MODERATE: Consider calling in {total_needed} staff."
        else:  # mild
            action = f"MILD: {total_needed} additional staff recommended."

        if available_oncall:
            suggested_names = ", ".join(
                [emp.name for emp in available_oncall[:3]]
            )
            action += f" Available: {suggested_names}"
            if len(available_oncall) > 3:
                action += f" (+{len(available_oncall) - 3} more)"

        return action


_detector: Optional[SurgeDetector] = None


def get_detector(
    database=None, pos_realtime_feed=None, dispatcher=None
) -> SurgeDetector:
    """Get or create the global SurgeDetector instance."""
    global _detector
    if _detector is None:
        if database is None:
            from rosteriq.database import get_db
            database = get_db()
        if dispatcher is None:
            from rosteriq.services.ws_events import get_dispatcher
            dispatcher = get_dispatcher()
        # pos_realtime_feed is optional
        _detector = SurgeDetector(database, pos_realtime_feed, dispatcher)
    return _detector
