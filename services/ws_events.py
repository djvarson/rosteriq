"""
WebSocket event dispatcher for real-time roster and shift updates.

Provides a centralized service for triggering broadcasts to connected clients
when roster changes, shift swaps, reminders, alerts, and headcount updates occur.

All broadcasts are non-fatal — failures don't interrupt the main business logic.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class WSEventDispatcher:
    """Central dispatcher for WebSocket events to connected clients."""

    def __init__(self, connection_manager=None):
        """
        Initialize the dispatcher.

        Args:
            connection_manager: ConnectionManager instance from websocket_hub.
                If None, will be imported at dispatch time.
        """
        self._manager = connection_manager

    def _get_manager(self):
        """Lazily import connection manager to avoid circular imports."""
        if self._manager is None:
            from rosteriq.websocket_hub import get_connection_manager
            self._manager = get_connection_manager()
        return self._manager

    async def roster_updated(
        self,
        venue_id: str,
        roster_id: str,
        summary: str,
    ) -> None:
        """
        Broadcast that a roster was updated.

        Args:
            venue_id: Venue identifier.
            roster_id: Roster identifier.
            summary: Human-readable summary of changes.
        """
        try:
            manager = self._get_manager()
            message = {
                "type": "roster.updated",
                "venue_id": venue_id,
                "roster_id": roster_id,
                "summary": summary,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_venue(venue_id, message)
            logger.info(f"Broadcast roster.updated for {roster_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast roster.updated: {e}")

    async def roster_published(
        self,
        venue_id: str,
        roster_id: str,
        week_start: str,
    ) -> None:
        """
        Broadcast that a roster was published to staff.

        Args:
            venue_id: Venue identifier.
            roster_id: Roster identifier.
            week_start: ISO date string for start of week.
        """
        try:
            manager = self._get_manager()
            message = {
                "type": "roster.published",
                "venue_id": venue_id,
                "roster_id": roster_id,
                "week_start": week_start,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_venue(venue_id, message)
            logger.info(f"Broadcast roster.published for {roster_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast roster.published: {e}")

    async def shift_swapped(
        self,
        swap_id: str,
        from_employee: str,
        to_employee: str,
    ) -> None:
        """
        Broadcast that a shift swap was completed.

        Args:
            swap_id: Shift swap identifier.
            from_employee: Employee ID offering shift.
            to_employee: Employee ID accepting shift.
        """
        try:
            manager = self._get_manager()
            message = {
                "type": "shift.swapped",
                "swap_id": swap_id,
                "from_employee": from_employee,
                "to_employee": to_employee,
                "timestamp": datetime.utcnow().isoformat(),
            }
            # Broadcast to both employees by user_id
            await manager.broadcast_to_user(from_employee, message)
            await manager.broadcast_to_user(to_employee, message)
            logger.info(f"Broadcast shift.swapped for {swap_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast shift.swapped: {e}")

    async def send_reminder(
        self,
        employee_id: str,
        shift_id: str,
        shift_date: str,
        shift_time: str,
        starts_in_minutes: int,
    ) -> None:
        """
        Send shift reminder to an employee.

        Args:
            employee_id: Employee identifier.
            shift_id: Shift identifier.
            shift_date: ISO date string of shift.
            shift_time: Time string (e.g., "10:00-18:00").
            starts_in_minutes: Minutes until shift starts.
        """
        try:
            manager = self._get_manager()
            message = {
                "type": "shift.reminder",
                "employee_id": employee_id,
                "shift_id": shift_id,
                "shift_date": shift_date,
                "shift_time": shift_time,
                "starts_in_minutes": starts_in_minutes,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_user(employee_id, message)
            logger.info(f"Broadcast shift.reminder to {employee_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast shift.reminder: {e}")

    async def send_alert(
        self,
        venue_id: str,
        alert_type: str,
        severity: str,
        message: str,
    ) -> None:
        """
        Broadcast compliance/variance alert to venue.

        Args:
            venue_id: Venue identifier.
            alert_type: Type of alert (e.g., "undercoverage", "award_breach").
            severity: "critical", "warning", or "info".
            message: Alert message text.
        """
        try:
            manager = self._get_manager()
            event = {
                "type": "alert.new",
                "venue_id": venue_id,
                "alert_type": alert_type,
                "severity": severity,
                "message": message,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_venue(venue_id, event)
            logger.info(f"Broadcast alert.new ({alert_type}) to {venue_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast alert.new: {e}")

    async def headcount_update(
        self,
        venue_id: str,
        current: int,
        required: int,
    ) -> None:
        """
        Broadcast live headcount update to venue.

        Args:
            venue_id: Venue identifier.
            current: Current number of staff on-shift.
            required: Required number of staff.
        """
        try:
            manager = self._get_manager()
            delta = current - required
            message = {
                "type": "headcount.changed",
                "venue_id": venue_id,
                "current": current,
                "required": required,
                "delta": delta,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_venue(venue_id, message)
            logger.debug(f"Broadcast headcount.changed to {venue_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast headcount.changed: {e}")

    async def send_notification(
        self,
        user_id: str,
        title: str,
        body: str,
        action_url: Optional[str] = None,
    ) -> None:
        """
        Send generic notification to a specific user.

        Args:
            user_id: User identifier.
            title: Notification title.
            body: Notification body text.
            action_url: Optional URL for action button.
        """
        try:
            manager = self._get_manager()
            message = {
                "type": "notification.new",
                "user_id": user_id,
                "title": title,
                "body": body,
                "action_url": action_url,
                "timestamp": datetime.utcnow().isoformat(),
            }
            await manager.broadcast_to_user(user_id, message)
            logger.info(f"Broadcast notification.new to {user_id}")
        except Exception as e:
            logger.error(f"Failed to broadcast notification.new: {e}")


_dispatcher: Optional[WSEventDispatcher] = None


def get_dispatcher() -> WSEventDispatcher:
    """Get or create the global WSEventDispatcher instance."""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = WSEventDispatcher()
    return _dispatcher
