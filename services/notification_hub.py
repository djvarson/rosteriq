"""
RosterIQ Notification Hub

Unified dispatcher that routes events to email, SMS, push, and websocket
based on employee preferences, quiet hours, and rate limiting.

Handles event types:
- ROSTER_PUBLISHED: new roster is live
- SHIFT_CHANGED: shift time/role modified
- SHIFT_CANCELLED: shift removed
- SWAP_REQUESTED: shift swap proposed
- SWAP_APPROVED: shift swap confirmed
- APPROVAL_NEEDED: roster needs manager approval
- APPROVAL_COMPLETED: roster approved/rejected
- BREAK_REMINDER: upcoming break notification
- OVERTIME_WARNING: approaching max hours
- BID_OPENED: new shift available for bidding
- BID_WON: employee won a shift bid
- AVAILABILITY_CONFLICT: shift conflicts with stated availability
- COMPLIANCE_ALERT: Fair Work compliance issue detected

Usage:
    from rosteriq.services.notification_hub import get_notification_hub
    hub = get_notification_hub()
    await hub.dispatch("ROSTER_PUBLISHED", venue_id, {"week": "2026-04-27"})
"""

import asyncio
import logging
import hashlib
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List
from time import time

from rosteriq.database import get_db
from rosteriq.services.notifications import get_notification_service
from rosteriq.services.sms import get_sms_service
from rosteriq.services.push_notifications import get_push_service
from rosteriq.services.ws_events import get_dispatcher
from rosteriq.services.notification_preferences import get_preferences_service

logger = logging.getLogger(__name__)


class NotificationEventType(str, Enum):
    """Supported notification event types."""
    ROSTER_PUBLISHED = "ROSTER_PUBLISHED"
    SHIFT_CHANGED = "SHIFT_CHANGED"
    SHIFT_CANCELLED = "SHIFT_CANCELLED"
    SWAP_REQUESTED = "SWAP_REQUESTED"
    SWAP_APPROVED = "SWAP_APPROVED"
    APPROVAL_NEEDED = "APPROVAL_NEEDED"
    APPROVAL_COMPLETED = "APPROVAL_COMPLETED"
    BREAK_REMINDER = "BREAK_REMINDER"
    OVERTIME_WARNING = "OVERTIME_WARNING"
    BID_OPENED = "BID_OPENED"
    BID_WON = "BID_WON"
    AVAILABILITY_CONFLICT = "AVAILABILITY_CONFLICT"
    COMPLIANCE_ALERT = "COMPLIANCE_ALERT"


class NotificationHub:
    """
    Unified notification dispatcher.

    Routes events to email, SMS, push, and websocket channels based on:
    - Employee notification preferences (channels, types, quiet hours)
    - Rate limiting (max 20 notifications per employee per hour)
    - Deduplication (skip if same event within 5 minutes)
    - Quiet hours (queue SMS during 10pm-7am, send in morning)
    """

    def __init__(self):
        """Initialize notification hub with services."""
        self._db = get_db()
        self._email_service = get_notification_service()
        self._sms_service = get_sms_service()
        self._push_service = None  # Lazy loaded
        self._ws_dispatcher = get_dispatcher()
        self._prefs_service = get_preferences_service()

        # Deduplication: (employee_id, event_type, venue_id) -> last_sent_timestamp
        self._dedup_cache: Dict[str, float] = {}
        self._dedup_window_seconds = 300  # 5 minutes

        # Rate limiting: employee_id -> [timestamp, timestamp, ...]
        self._rate_limit_tracker: Dict[str, List[float]] = {}
        self._rate_limit_max_per_hour = 20

        # Notification audit trail (in-memory for now)
        self._audit_log: List[Dict[str, Any]] = []

    def _get_push_service(self):
        """Lazy load push service."""
        if self._push_service is None:
            self._push_service = get_push_service(self._db)
        return self._push_service

    def _make_dedup_key(self, employee_id: str, event_type: str, venue_id: str) -> str:
        """Create deduplication cache key."""
        return f"{employee_id}:{event_type}:{venue_id}"

    def _is_duplicate(self, employee_id: str, event_type: str, venue_id: str) -> bool:
        """Check if event was sent to employee recently (within 5 min)."""
        key = self._make_dedup_key(employee_id, event_type, venue_id)
        now = time()
        last_sent = self._dedup_cache.get(key, 0)

        if now - last_sent < self._dedup_window_seconds:
            logger.debug(f"Duplicate notification suppressed for {employee_id} - {event_type}")
            return True

        self._dedup_cache[key] = now
        return False

    def _check_rate_limit(self, employee_id: str) -> bool:
        """
        Check if employee has exceeded rate limit (20 per hour).

        Returns True if within limit, False if rate limited.
        """
        now = time()
        one_hour_ago = now - 3600

        # Clean old timestamps
        if employee_id in self._rate_limit_tracker:
            self._rate_limit_tracker[employee_id] = [
                ts for ts in self._rate_limit_tracker[employee_id]
                if ts > one_hour_ago
            ]
        else:
            self._rate_limit_tracker[employee_id] = []

        # Check if within limit
        count = len(self._rate_limit_tracker[employee_id])
        if count >= self._rate_limit_max_per_hour:
            logger.warning(f"Rate limit exceeded for employee {employee_id}")
            return False

        # Record this notification
        self._rate_limit_tracker[employee_id].append(now)
        return True

    def _log_audit(
        self,
        event_type: str,
        employee_id: str,
        venue_id: str,
        channels_sent: Dict[str, bool],
        status: str = "sent",
    ) -> None:
        """Log notification dispatch to audit trail."""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "employee_id": employee_id,
            "venue_id": venue_id,
            "channels_sent": channels_sent,
            "status": status,
        }
        self._audit_log.append(entry)
        # Keep only last 10000 entries in memory
        if len(self._audit_log) > 10000:
            self._audit_log = self._audit_log[-10000:]

    def _get_event_templates(self, event_type: str) -> Dict[str, Dict[str, str]]:
        """
        Get message templates for an event type.

        Returns dict with keys: email_subject, email_body, sms_text, push_title, push_body
        """
        templates = {
            NotificationEventType.ROSTER_PUBLISHED: {
                "email_subject": "New Roster Published",
                "email_body": "Your new roster has been published. Check your shifts for the week.",
                "sms_text": "New roster published! View your shifts in the app.",
                "push_title": "Roster Published",
                "push_body": "Your new roster is ready to view",
            },
            NotificationEventType.SHIFT_CHANGED: {
                "email_subject": "Your Shift Time Changed",
                "email_body": "One of your scheduled shifts has been modified. Check the details.",
                "sms_text": "Your shift time has changed. Check the app for details.",
                "push_title": "Shift Updated",
                "push_body": "Your shift time has changed",
            },
            NotificationEventType.SHIFT_CANCELLED: {
                "email_subject": "Your Shift Has Been Cancelled",
                "email_body": "One of your scheduled shifts has been cancelled.",
                "sms_text": "Your shift has been cancelled.",
                "push_title": "Shift Cancelled",
                "push_body": "One of your shifts has been cancelled",
            },
            NotificationEventType.SWAP_REQUESTED: {
                "email_subject": "Shift Swap Request",
                "email_body": "Someone has requested to swap a shift with you.",
                "sms_text": "New shift swap request! Check the app.",
                "push_title": "Swap Request",
                "push_body": "You have a new shift swap request",
            },
            NotificationEventType.SWAP_APPROVED: {
                "email_subject": "Your Shift Swap Was Approved",
                "email_body": "Your shift swap request has been approved.",
                "sms_text": "Your shift swap was approved!",
                "push_title": "Swap Approved",
                "push_body": "Your shift swap request was approved",
            },
            NotificationEventType.APPROVAL_NEEDED: {
                "email_subject": "Roster Approval Required",
                "email_body": "The roster requires your approval before publishing to staff.",
                "sms_text": "Roster needs your approval. Check the app.",
                "push_title": "Approval Needed",
                "push_body": "Roster is waiting for your approval",
            },
            NotificationEventType.APPROVAL_COMPLETED: {
                "email_subject": "Roster Has Been Approved",
                "email_body": "The roster has been approved and is now published.",
                "sms_text": "Roster approved and published!",
                "push_title": "Approval Complete",
                "push_body": "Roster has been approved and published",
            },
            NotificationEventType.BREAK_REMINDER: {
                "email_subject": "Upcoming Break Reminder",
                "email_body": "You have an upcoming break scheduled.",
                "sms_text": "Break reminder: Your break is coming up.",
                "push_title": "Break Reminder",
                "push_body": "Your break is coming up soon",
            },
            NotificationEventType.OVERTIME_WARNING: {
                "email_subject": "Overtime Warning",
                "email_body": "You are approaching your maximum hours for the week.",
                "sms_text": "You're approaching max hours. Check the app.",
                "push_title": "Overtime Warning",
                "push_body": "You're approaching your maximum hours",
            },
            NotificationEventType.BID_OPENED: {
                "email_subject": "New Shift Available for Bidding",
                "email_body": "A new shift is available for you to bid on.",
                "sms_text": "New shift available! Bid now in the app.",
                "push_title": "Bid Opened",
                "push_body": "A new shift is available for bidding",
            },
            NotificationEventType.BID_WON: {
                "email_subject": "You Won a Shift Bid",
                "email_body": "Congratulations! You have won the shift bid.",
                "sms_text": "You won the shift bid!",
                "push_title": "Bid Won",
                "push_body": "Congratulations! You won the shift bid",
            },
            NotificationEventType.AVAILABILITY_CONFLICT: {
                "email_subject": "Shift Conflicts with Your Availability",
                "email_body": "You have been scheduled for a shift during unavailable time.",
                "sms_text": "You're scheduled during unavailable time. Contact manager.",
                "push_title": "Availability Conflict",
                "push_body": "You're scheduled during unavailable time",
            },
            NotificationEventType.COMPLIANCE_ALERT: {
                "email_subject": "Compliance Alert",
                "email_body": "A compliance issue has been detected in the roster.",
                "sms_text": "Compliance alert! Check the app immediately.",
                "push_title": "Compliance Alert",
                "push_body": "A compliance issue requires attention",
            },
        }

        return templates.get(event_type, {})

    async def dispatch(
        self,
        event_type: NotificationEventType,
        venue_id: str,
        payload: Dict[str, Any],
        target_employee_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Dispatch notification to target employees.

        Routes to email, SMS, push, and websocket based on preferences.

        Args:
            event_type: Type of event
            venue_id: Venue identifier
            payload: Event-specific payload
            target_employee_ids: List of employee IDs to notify. If None, notifies all at venue.

        Returns:
            Summary of dispatch (sent counts, failed, skipped)
        """
        try:
            # Get target employees
            if target_employee_ids is None:
                # Get all employees at venue
                employees = self._db.list_employees()
                target_employee_ids = [e.id for e in employees]

            summary = {
                "event_type": event_type.value,
                "venue_id": venue_id,
                "total_targets": len(target_employee_ids),
                "sent": {"email": 0, "sms": 0, "push": 0, "ws": 0},
                "failed": {"email": 0, "sms": 0, "push": 0, "ws": 0},
                "skipped": {"duplicate": 0, "rate_limit": 0, "preference": 0, "quiet_hours": 0},
            }

            # Dispatch to each employee
            tasks = []
            for emp_id in target_employee_ids:
                tasks.append(
                    self._dispatch_to_employee(
                        event_type,
                        venue_id,
                        emp_id,
                        payload,
                        summary,
                    )
                )

            # Run dispatches concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

            logger.info(f"Dispatched {event_type.value} to {venue_id}: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Error dispatching notification: {e}")
            return {"error": str(e)}

    async def _dispatch_to_employee(
        self,
        event_type: NotificationEventType,
        venue_id: str,
        employee_id: str,
        payload: Dict[str, Any],
        summary: Dict[str, Any],
    ) -> None:
        """Dispatch notification to a single employee across all channels."""
        try:
            # Check deduplication
            if self._is_duplicate(employee_id, event_type.value, venue_id):
                summary["skipped"]["duplicate"] += 1
                return

            # Check rate limit
            if not self._check_rate_limit(employee_id):
                summary["skipped"]["rate_limit"] += 1
                return

            # Get employee details
            employee = self._db.get_employee(employee_id)
            if not employee:
                logger.debug(f"Employee {employee_id} not found")
                summary["skipped"]["preference"] += 1
                return

            # Get preferences
            prefs = self._prefs_service.get_preferences(employee_id)
            channels_sent = {}

            # Send via email
            if prefs["channels"].get("email", False):
                if await self._send_email_notification(
                    employee_id,
                    employee.email,
                    event_type,
                    payload,
                ):
                    channels_sent["email"] = True
                    summary["sent"]["email"] += 1
                else:
                    summary["failed"]["email"] += 1

            # Send via SMS (respect quiet hours)
            if prefs["channels"].get("sms", False):
                if not self._prefs_service.is_in_quiet_hours(employee_id):
                    if await self._send_sms_notification(employee_id, event_type, payload):
                        channels_sent["sms"] = True
                        summary["sent"]["sms"] += 1
                    else:
                        summary["failed"]["sms"] += 1
                else:
                    # Queue for morning delivery
                    summary["skipped"]["quiet_hours"] += 1

            # Send via push
            if prefs["channels"].get("push", False):
                if await self._send_push_notification(employee_id, event_type, payload):
                    channels_sent["push"] = True
                    summary["sent"]["push"] += 1
                else:
                    summary["failed"]["push"] += 1

            # Send via websocket (always)
            try:
                await self._send_websocket_notification(employee_id, event_type, payload)
                channels_sent["ws"] = True
                summary["sent"]["ws"] += 1
            except Exception as e:
                logger.debug(f"WebSocket notification failed for {employee_id}: {e}")
                summary["failed"]["ws"] += 1

            # Log to audit trail
            self._log_audit(event_type.value, employee_id, venue_id, channels_sent)

        except Exception as e:
            logger.error(f"Error dispatching to {employee_id}: {e}")

    async def _send_email_notification(
        self,
        employee_id: str,
        email: str,
        event_type: NotificationEventType,
        payload: Dict[str, Any],
    ) -> bool:
        """Send email notification."""
        try:
            templates = self._get_event_templates(event_type)
            subject = templates.get("email_subject", "RosterIQ Notification")
            body_text = templates.get("email_body", "You have a new notification.")

            # Build HTML email
            html = self._email_service._wrap_template(
                title=subject,
                date_str=datetime.utcnow().isoformat(),
                body=f"<p>{body_text}</p><p><strong>Details:</strong></p><ul>" +
                     "".join(f"<li>{k}: {v}</li>" for k, v in payload.items()) +
                     "</ul>",
            )

            return await self._email_service.send_email(email, subject, html)
        except Exception as e:
            logger.error(f"Email send failed for {employee_id}: {e}")
            return False

    async def _send_sms_notification(
        self,
        employee_id: str,
        event_type: NotificationEventType,
        payload: Dict[str, Any],
    ) -> bool:
        """Send SMS notification."""
        try:
            templates = self._get_event_templates(event_type)
            sms_text = templates.get("sms_text", "You have a new notification.")

            employee = self._db.get_employee(employee_id)
            if not employee or not employee.phone:
                logger.debug(f"No phone number for {employee_id}")
                return False

            return await self._sms_service.send_sms(employee.phone, sms_text)
        except Exception as e:
            logger.error(f"SMS send failed for {employee_id}: {e}")
            return False

    async def _send_push_notification(
        self,
        employee_id: str,
        event_type: NotificationEventType,
        payload: Dict[str, Any],
    ) -> bool:
        """Send push notification."""
        try:
            templates = self._get_event_templates(event_type)
            title = templates.get("push_title", "RosterIQ")
            body = templates.get("push_body", "You have a new notification.")

            push_svc = self._get_push_service()
            return push_svc.send_push(
                employee_id,
                title=title,
                body=body,
                notification_type=event_type.value,
                **payload,
            )
        except Exception as e:
            logger.error(f"Push send failed for {employee_id}: {e}")
            return False

    async def _send_websocket_notification(
        self,
        employee_id: str,
        event_type: NotificationEventType,
        payload: Dict[str, Any],
    ) -> None:
        """Send websocket notification."""
        try:
            templates = self._get_event_templates(event_type)
            title = templates.get("push_title", "RosterIQ")
            body = templates.get("push_body", "You have a new notification.")

            await self._ws_dispatcher.send_notification(
                employee_id,
                title=title,
                body=body,
            )
        except Exception as e:
            logger.error(f"WebSocket send failed for {employee_id}: {e}")
            raise

    async def dispatch_to_managers(
        self,
        event_type: NotificationEventType,
        venue_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Dispatch notification to all managers/admins at a venue.

        Args:
            event_type: Type of event
            venue_id: Venue identifier
            payload: Event-specific payload

        Returns:
            Summary of dispatch
        """
        try:
            # Get all employees with manager role at venue
            employees = self._db.list_employees()
            manager_ids = [
                e.id for e in employees
                if hasattr(e, "role") and e.role in ("manager", "admin")
            ]

            return await self.dispatch(event_type, venue_id, payload, manager_ids)
        except Exception as e:
            logger.error(f"Error dispatching to managers: {e}")
            return {"error": str(e)}

    async def dispatch_bulk(
        self,
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Batch dispatch multiple events.

        Groups by channel for efficiency.

        Args:
            events: List of dicts with keys: event_type, venue_id, payload, target_employee_ids

        Returns:
            Summary of all dispatches
        """
        try:
            tasks = []
            for event in events:
                event_type = NotificationEventType(event["event_type"])
                venue_id = event["venue_id"]
                payload = event.get("payload", {})
                target_ids = event.get("target_employee_ids")

                tasks.append(
                    self.dispatch(event_type, venue_id, payload, target_ids)
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return {"events_processed": len(events), "results": results}
        except Exception as e:
            logger.error(f"Error in bulk dispatch: {e}")
            return {"error": str(e)}

    def get_audit_log(
        self,
        venue_id: Optional[str] = None,
        employee_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get notification audit log.

        Args:
            venue_id: Filter by venue
            employee_id: Filter by employee
            event_type: Filter by event type
            limit: Max results to return

        Returns:
            List of audit entries
        """
        results = self._audit_log

        # Apply filters
        if venue_id:
            results = [r for r in results if r["venue_id"] == venue_id]
        if employee_id:
            results = [r for r in results if r["employee_id"] == employee_id]
        if event_type:
            results = [r for r in results if r["event_type"] == event_type]

        # Return most recent entries
        return results[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """
        Get notification dispatch statistics.

        Returns:
            Dict with stats: total_sent, by_channel, by_event_type, failure_rate
        """
        total_entries = len(self._audit_log)
        if not total_entries:
            return {"total_entries": 0}

        stats = {
            "total_entries": total_entries,
            "by_channel": {},
            "by_event_type": {},
            "by_status": {},
        }

        for entry in self._audit_log:
            # By status
            status = entry.get("status", "unknown")
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

            # By event type
            evt_type = entry.get("event_type", "unknown")
            stats["by_event_type"][evt_type] = stats["by_event_type"].get(evt_type, 0) + 1

            # By channel
            for channel, sent in entry.get("channels_sent", {}).items():
                if sent:
                    stats["by_channel"][channel] = stats["by_channel"].get(channel, 0) + 1

        return stats


# Singleton instance
_notification_hub: Optional[NotificationHub] = None


def get_notification_hub() -> NotificationHub:
    """Get or create the notification hub singleton."""
    global _notification_hub
    if _notification_hub is None:
        _notification_hub = NotificationHub()
    return _notification_hub
