"""
Push Notifications Service for RosterIQ

Handles Web Push API subscriptions and sending push notifications to users.
Integrates with the database to store subscription info.
"""

import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Try to import pywebpush for actual push sending
try:
    from pywebpush import webpush, WebPushException
    HAS_WEBPUSH = True
except ImportError:
    HAS_WEBPUSH = False
    logger.warning("pywebpush not installed; push notifications will be logged only")

# Try to import cryptography for VAPID key generation
try:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False
    logger.warning("cryptography not installed; VAPID key generation unavailable")


class PushService:
    """
    Service for managing push notifications.

    Handles:
    - Storing and retrieving push subscriptions
    - Sending push notifications via Web Push protocol
    - Broadcasting to all staff at a venue
    - VAPID key management
    """

    def __init__(self, db, vapid_private_key: Optional[str] = None, vapid_public_key: Optional[str] = None):
        """
        Initialize Push Service.

        Args:
            db: Database connection (expects BaseStore interface)
            vapid_private_key: VAPID private key (base64). Auto-generate if not provided
            vapid_public_key: VAPID public key (base64). Auto-generate if not provided
        """
        self.db = db
        self._vapid_private_key = vapid_private_key
        self._vapid_public_key = vapid_public_key

        # Ensure we have VAPID keys
        if not self._vapid_private_key or not self._vapid_public_key:
            self._ensure_vapid_keys()

    def _ensure_vapid_keys(self):
        """Generate VAPID keys if not set."""
        if not HAS_CRYPTOGRAPHY:
            logger.warning("cryptography not installed; using placeholder VAPID keys")
            self._vapid_private_key = "placeholder_private_key"
            self._vapid_public_key = "placeholder_public_key"
            return

        if self._vapid_private_key and self._vapid_public_key:
            return

        try:
            # Generate new VAPID keys
            private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
            private_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            public_key = private_key.public_key()
            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            self._vapid_private_key = private_pem.decode()
            self._vapid_public_key = public_pem.decode()

            logger.info("Generated new VAPID keys for push notifications")
        except Exception as e:
            logger.error(f"Error generating VAPID keys: {e}")
            self._vapid_private_key = "placeholder_private_key"
            self._vapid_public_key = "placeholder_public_key"

    def get_vapid_public_key(self) -> str:
        """Get VAPID public key for client subscription."""
        return self._vapid_public_key

    def subscribe(self, user_id: str, subscription_info: dict) -> bool:
        """
        Store a push notification subscription for a user.

        Args:
            user_id: User ID
            subscription_info: Subscription object from Push API
                {
                    "endpoint": "https://...",
                    "keys": {
                        "p256dh": "...",
                        "auth": "..."
                    }
                }

        Returns:
            True if stored successfully
        """
        try:
            self.db.save_push_subscription(user_id, subscription_info)
            logger.info(f"Stored push subscription for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing push subscription for {user_id}: {e}")
            return False

    def unsubscribe(self, user_id: str) -> bool:
        """
        Remove a push notification subscription for a user.

        Args:
            user_id: User ID

        Returns:
            True if removed successfully
        """
        try:
            self.db.delete_push_subscription(user_id)
            logger.info(f"Removed push subscription for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing push subscription for {user_id}: {e}")
            return False

    def send_push(
        self,
        user_id: str,
        title: str,
        body: str,
        url: str = "/dashboard",
        icon: Optional[str] = None,
        notification_type: Optional[str] = None,
        **extra_data
    ) -> bool:
        """
        Send a push notification to a user.

        Args:
            user_id: User ID
            title: Notification title
            body: Notification body
            url: URL to navigate to on click
            icon: Icon URL
            notification_type: Type of notification (e.g., 'roster.published')
            **extra_data: Additional data to include in notification

        Returns:
            True if sent successfully
        """
        try:
            subscription = self.db.get_push_subscription(user_id)
            if not subscription:
                logger.debug(f"No subscription found for user {user_id}")
                return False

            # Build notification payload
            payload = {
                "title": title,
                "body": body,
                "url": url,
                "type": notification_type,
                **extra_data,
            }

            if icon:
                payload["icon"] = icon

            return self._send_push_to_subscription(subscription, payload)
        except Exception as e:
            logger.error(f"Error sending push to {user_id}: {e}")
            return False

    def _send_push_to_subscription(self, subscription: dict, payload: dict) -> bool:
        """
        Send push notification to a subscription.

        Args:
            subscription: Push subscription object
            payload: Notification payload

        Returns:
            True if sent successfully
        """
        if not HAS_WEBPUSH:
            logger.info(f"Would send push to {subscription.get('endpoint')}: {payload}")
            return True

        try:
            import json

            # Convert payload to JSON
            data = json.dumps(payload)

            # Send via Web Push
            webpush(
                subscription_info=subscription,
                data=data,
                vapid_private_key=self._vapid_private_key,
                vapid_claims={"sub": "mailto:notifications@rosteriq.app"},
            )

            logger.info(f"Push notification sent to {subscription.get('endpoint')}")
            return True
        except WebPushException as e:
            if e.response and e.response.status_code == 410:
                # Subscription expired
                logger.info(f"Subscription expired: {subscription.get('endpoint')}")
                return False
            else:
                logger.error(f"Web push error: {e}")
                return False
        except Exception as e:
            logger.error(f"Error sending push notification: {e}")
            return False

    def broadcast_venue(
        self,
        venue_id: str,
        title: str,
        body: str,
        url: str = "/dashboard",
        notification_type: Optional[str] = None,
        **extra_data
    ) -> int:
        """
        Send a push notification to all staff at a venue.

        Args:
            venue_id: Venue ID
            title: Notification title
            body: Notification body
            url: URL to navigate to on click
            notification_type: Type of notification
            **extra_data: Additional data

        Returns:
            Number of notifications sent successfully
        """
        try:
            subscriptions = self.db.list_push_subscriptions(venue_id)
            sent_count = 0

            for sub_info in subscriptions:
                payload = {
                    "title": title,
                    "body": body,
                    "url": url,
                    "type": notification_type,
                    "venue_id": venue_id,
                    **extra_data,
                }

                if self._send_push_to_subscription(sub_info.get('subscription', {}), payload):
                    sent_count += 1

            logger.info(f"Broadcast to {sent_count}/{len(subscriptions)} staff at venue {venue_id}")
            return sent_count
        except Exception as e:
            logger.error(f"Error broadcasting to venue {venue_id}: {e}")
            return 0

    def send_roster_published(self, venue_id: str, roster_date: str) -> int:
        """Send roster published notification to all staff at a venue."""
        return self.broadcast_venue(
            venue_id,
            title="New Roster Published",
            body=f"The roster for {roster_date} has been published",
            url=f"/dashboard?venue={venue_id}",
            notification_type="roster.published",
            venue_id=venue_id,
            date=roster_date,
        )

    def send_shift_reminder(self, user_id: str, shift_date: str, shift_time: str) -> bool:
        """Send shift reminder notification."""
        return self.send_push(
            user_id,
            title="Shift Reminder",
            body=f"Your shift is today at {shift_time}",
            url="/staff",
            notification_type="shift.reminder",
            date=shift_date,
            time=shift_time,
            requireInteraction=True,
        )

    def send_shift_swap_request(self, user_id: str, requester: str, shift_date: str) -> bool:
        """Send shift swap request notification."""
        return self.send_push(
            user_id,
            title="Shift Swap Request",
            body=f"{requester} is requesting to swap a shift on {shift_date}",
            url="/staff",
            notification_type="shift.swap",
            shift_date=shift_date,
            requester=requester,
            requireInteraction=True,
        )

    def send_staffing_alert(self, venue_id: str, message: str) -> int:
        """Send staffing alert to all staff at a venue."""
        return self.broadcast_venue(
            venue_id,
            title="Staffing Alert",
            body=message,
            url="/dashboard",
            notification_type="alert",
        )


def get_push_service(db, vapid_private_key: Optional[str] = None, vapid_public_key: Optional[str] = None) -> PushService:
    """
    Factory function to get or create a PushService instance.

    Args:
        db: Database connection
        vapid_private_key: Optional VAPID private key
        vapid_public_key: Optional VAPID public key

    Returns:
        PushService instance
    """
    return PushService(db, vapid_private_key, vapid_public_key)
