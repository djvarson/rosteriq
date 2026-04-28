"""
RosterIQ Notification Preferences Service

Manages per-user notification preferences including channels (email, SMS, push),
notification types, and quiet hours. Preferences are stored in the database.

Usage:
    from rosteriq.services.notification_preferences import get_preferences_service
    prefs_svc = get_preferences_service()
    should_send = prefs_svc.should_notify(user_id, "sms", "shift_reminder")
"""

import logging
from datetime import time, datetime
from typing import Optional, Dict, Any

from rosteriq.database import get_db

logger = logging.getLogger(__name__)


class NotificationPreferences:
    """
    Manages per-user notification preferences.

    Stores preferences in database and provides methods to check whether
    a notification should be sent based on user settings and quiet hours.
    """

    # Default preferences
    DEFAULT_PREFS = {
        "channels": {
            "email": True,
            "sms": False,  # Disabled by default for privacy
            "push": False,
        },
        "notification_types": {
            "shift_reminders": True,
            "roster_published": True,
            "swap_updates": True,
            "compliance_alerts": True,
        },
        "quiet_hours": {
            "enabled": True,
            "start": "22:00",  # 10 PM AEST
            "end": "07:00",    # 7 AM AEST
        },
    }

    def __init__(self):
        """Initialize preferences service with database access."""
        self._db = get_db()

    def get_preferences(self, user_id: str) -> Dict[str, Any]:
        """
        Get notification preferences for a user.

        Returns default preferences if user has no custom settings.

        Args:
            user_id: User ID

        Returns:
            Dict with keys: channels, notification_types, quiet_hours
        """
        # Try to load from database
        stored = self._db.get_notification_preferences(user_id)
        if stored:
            # Merge with defaults to fill in missing keys
            return self._merge_with_defaults(stored)

        return self.DEFAULT_PREFS.copy()

    def update_preferences(
        self,
        user_id: str,
        prefs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update notification preferences for a user.

        Args:
            user_id: User ID
            prefs: Preferences dict with keys: channels, notification_types, quiet_hours

        Returns:
            Merged preferences (with defaults for missing keys)
        """
        # Merge with defaults to ensure all keys present
        merged = self._merge_with_defaults(prefs)

        # Save to database
        self._db.save_notification_preferences(user_id, merged)

        logger.info(f"Updated notification preferences for user {user_id}")
        return merged

    def should_notify(
        self,
        user_id: str,
        channel: str,
        notification_type: str,
    ) -> bool:
        """
        Check if notification should be sent to user.

        Checks:
        1. Channel is enabled
        2. Notification type is enabled
        3. Not in quiet hours

        Args:
            user_id: User ID
            channel: "email", "sms", or "push"
            notification_type: "shift_reminders", "roster_published", "swap_updates", "compliance_alerts"

        Returns:
            True if notification should be sent
        """
        prefs = self.get_preferences(user_id)

        # Check channel is enabled
        channels = prefs.get("channels", {})
        if not channels.get(channel, False):
            return False

        # Check notification type is enabled (except for compliance alerts, always send)
        if notification_type != "compliance_alerts":
            notif_types = prefs.get("notification_types", {})
            key = self._normalize_notification_type(notification_type)
            if not notif_types.get(key, False):
                return False

        # Check quiet hours (skip for compliance alerts)
        if notification_type != "compliance_alerts":
            if self._is_in_quiet_hours(prefs):
                logger.debug(
                    f"User {user_id} in quiet hours, skipping {notification_type} on {channel}"
                )
                return False

        return True

    def is_in_quiet_hours(self, user_id: str) -> bool:
        """
        Check if user is currently in quiet hours.

        Args:
            user_id: User ID

        Returns:
            True if current time is within user's quiet hours
        """
        prefs = self.get_preferences(user_id)
        return self._is_in_quiet_hours(prefs)

    def _is_in_quiet_hours(self, prefs: Dict[str, Any]) -> bool:
        """Check if current time is within quiet hours for preferences dict."""
        qh = prefs.get("quiet_hours", {})
        if not qh.get("enabled", False):
            return False

        start_str = qh.get("start", "22:00")
        end_str = qh.get("end", "07:00")

        try:
            start_time = datetime.strptime(start_str, "%H:%M").time()
            end_time = datetime.strptime(end_str, "%H:%M").time()
        except ValueError:
            logger.warning(f"Invalid quiet hours format: {start_str} - {end_str}")
            return False

        now = datetime.now().time()

        # Handle case where end time is next day (e.g., 22:00 to 07:00)
        if end_time < start_time:
            return now >= start_time or now < end_time
        else:
            return start_time <= now < end_time

    def _normalize_notification_type(self, notif_type: str) -> str:
        """Convert notification type to preference key."""
        mapping = {
            "shift_reminder": "shift_reminders",
            "shift_reminders": "shift_reminders",
            "roster_published": "roster_published",
            "swap_update": "swap_updates",
            "swap_updates": "swap_updates",
            "compliance_alert": "compliance_alerts",
            "compliance_alerts": "compliance_alerts",
        }
        return mapping.get(notif_type, notif_type)

    def _merge_with_defaults(self, prefs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge user preferences with defaults."""
        merged = self.DEFAULT_PREFS.copy()

        if "channels" in prefs:
            merged["channels"].update(prefs["channels"])
        if "notification_types" in prefs:
            merged["notification_types"].update(prefs["notification_types"])
        if "quiet_hours" in prefs:
            merged["quiet_hours"].update(prefs["quiet_hours"])

        return merged


# Singleton instance
_prefs_service: Optional[NotificationPreferences] = None


def get_preferences_service() -> NotificationPreferences:
    """Get or create the notification preferences service singleton."""
    global _prefs_service
    if _prefs_service is None:
        _prefs_service = NotificationPreferences()
    return _prefs_service
