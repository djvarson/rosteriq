"""
Outbound webhook service for RosterIQ.

Manages webhook subscriptions and delivers events to external systems.
Handles retry logic, signature verification, and delivery tracking.

Event types:
  - roster.published
  - roster.updated
  - shift.created
  - shift.swapped
  - shift.cancelled
  - employee.added
  - employee.updated
  - alert.compliance
  - alert.variance
  - forecast.updated
"""

import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import httpx

from rosteriq.database import get_db

logger = logging.getLogger(__name__)

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_DELAYS = [5, 30, 120]  # seconds: 5s, 30s, 2m
DELIVERY_TIMEOUT = 10  # seconds


class EventType(str, Enum):
    """Webhook event types."""

    ROSTER_PUBLISHED = "roster.published"
    ROSTER_UPDATED = "roster.updated"
    SHIFT_CREATED = "shift.created"
    SHIFT_SWAPPED = "shift.swapped"
    SHIFT_CANCELLED = "shift.cancelled"
    EMPLOYEE_ADDED = "employee.added"
    EMPLOYEE_UPDATED = "employee.updated"
    ALERT_COMPLIANCE = "alert.compliance"
    ALERT_VARIANCE = "alert.variance"
    FORECAST_UPDATED = "forecast.updated"


class OutboundWebhookService:
    """Service for managing outbound webhooks and event delivery."""

    def __init__(self, db=None):
        """Initialize webhook service with database."""
        self.db = db or get_db()

    # ========================================================================
    # Subscription Management
    # ========================================================================

    def register_subscription(
        self,
        venue_id: str,
        callback_url: str,
        events: list[str],
        secret: str,
    ) -> str:
        """
        Register a webhook subscription for a venue.

        Args:
            venue_id: Venue ID
            callback_url: HTTPS URL to receive webhooks
            events: List of event types to subscribe to
            secret: Secret for HMAC signing

        Returns:
            subscription_id
        """
        subscription_id = f"sub_{uuid.uuid4().hex[:16]}"

        subscription = {
            "id": subscription_id,
            "venue_id": venue_id,
            "callback_url": callback_url,
            "events": events,
            "secret": secret,
            "active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }

        self.db.save_webhook_subscription(subscription)
        logger.info(
            f"Registered webhook subscription {subscription_id} "
            f"for venue {venue_id}"
        )

        return subscription_id

    def list_subscriptions(self, venue_id: str) -> list[dict]:
        """
        List all webhook subscriptions for a venue.

        Args:
            venue_id: Venue ID

        Returns:
            List of subscription dicts
        """
        subscriptions = self.db.list_webhook_subscriptions(venue_id)
        return [
            {
                "id": sub.get("id"),
                "venue_id": sub.get("venue_id"),
                "callback_url": sub.get("callback_url"),
                "events": sub.get("events", []),
                "active": sub.get("active", True),
                "created_at": sub.get("created_at"),
                "updated_at": sub.get("updated_at"),
            }
            for sub in subscriptions
        ]

    def delete_subscription(self, subscription_id: str) -> bool:
        """
        Delete a webhook subscription.

        Args:
            subscription_id: Subscription ID

        Returns:
            True if deleted, False if not found
        """
        subscription = self.db.get_webhook_subscription(subscription_id)
        if not subscription:
            return False

        self.db.delete_webhook_subscription(subscription_id)
        logger.info(f"Deleted webhook subscription {subscription_id}")
        return True

    def update_subscription(
        self,
        subscription_id: str,
        events: Optional[list[str]] = None,
        callback_url: Optional[str] = None,
        active: Optional[bool] = None,
    ) -> Optional[dict]:
        """
        Update a webhook subscription.

        Args:
            subscription_id: Subscription ID
            events: New event list (optional)
            callback_url: New callback URL (optional)
            active: New active state (optional)

        Returns:
            Updated subscription dict, or None if not found
        """
        subscription = self.db.get_webhook_subscription(subscription_id)
        if not subscription:
            return None

        if events is not None:
            subscription["events"] = events
        if callback_url is not None:
            subscription["callback_url"] = callback_url
        if active is not None:
            subscription["active"] = active

        subscription["updated_at"] = datetime.now(timezone.utc)

        self.db.save_webhook_subscription(subscription)
        logger.info(f"Updated webhook subscription {subscription_id}")

        return subscription

    # ========================================================================
    # Event Firing & Delivery
    # ========================================================================

    async def fire_event(self, event_type: str, venue_id: str, payload: dict) -> int:
        """
        Fire a webhook event for all matching subscriptions.

        Queues delivery for all subscriptions that match the event type.
        Delivers asynchronously with retry logic.

        Args:
            event_type: Event type (e.g. 'roster.published')
            venue_id: Venue ID
            payload: Event data

        Returns:
            Number of subscriptions that matched
        """
        subscriptions = self.db.list_webhook_subscriptions(venue_id)

        matched = 0
        for subscription in subscriptions:
            if not subscription.get("active", True):
                continue

            events = subscription.get("events", [])
            if event_type not in events:
                continue

            matched += 1

            # Queue delivery
            try:
                await self.deliver_webhook(subscription, event_type, payload)
            except Exception as e:
                logger.error(
                    f"Failed to queue delivery for {subscription['id']}: {e}",
                    exc_info=True,
                )

        logger.info(
            f"Fired event {event_type} for venue {venue_id}, "
            f"matched {matched} subscriptions"
        )

        return matched

    async def deliver_webhook(
        self,
        subscription: dict,
        event_type: str,
        payload: dict,
    ) -> None:
        """
        Deliver a webhook to a subscription with retry logic.

        Args:
            subscription: Subscription dict
            event_type: Event type
            payload: Event data

        Raises:
            Exception on final failure after all retries
        """
        subscription_id = subscription.get("id")
        callback_url = subscription.get("callback_url")
        secret = subscription.get("secret")
        delivery_id = f"dlv_{uuid.uuid4().hex[:16]}"

        # Construct webhook body
        body = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "venue_id": subscription.get("venue_id"),
            "data": payload,
            "webhook_id": delivery_id,
        }

        body_json = json.dumps(body, default=str)
        body_bytes = body_json.encode()

        # Compute signature
        signature = hmac.new(
            secret.encode(),
            body_bytes,
            hashlib.sha256,
        ).hexdigest()

        # Headers
        headers = {
            "Content-Type": "application/json",
            "X-RosterIQ-Signature": f"sha256={signature}",
            "X-RosterIQ-Event": event_type,
            "X-RosterIQ-Delivery-Id": delivery_id,
        }

        # Enqueue into the DURABLE retry queue instead of retrying in-process.
        # The queue persists the full delivery (url/payload/headers) before
        # sending, so it survives a restart; the boot-started queue processor
        # sends it immediately and handles retries with exponential backoff, a
        # per-URL circuit breaker, and a dead-letter queue. (Previously this used
        # an in-process asyncio.sleep retry loop whose in-flight state was lost
        # on restart and which had no dead-letter handling.)
        #
        # The signature above is computed over json.dumps(body, default=str);
        # the queue re-serialises `payload` (== body) with the identical call,
        # so the signature the receiver verifies stays valid.
        from rosteriq.services.webhook_queue import get_webhook_queue

        queue = get_webhook_queue()
        await queue.enqueue(
            delivery_id=delivery_id,
            url=callback_url,
            payload=body,
            headers=headers,
            venue_id=subscription.get("venue_id"),
            subscription_id=subscription_id,
            event_type=event_type,
            attempt=0,
        )
        logger.info(
            f"Webhook {delivery_id} enqueued for durable delivery to {subscription_id}"
        )

    # ========================================================================
    # Delivery Log
    # ========================================================================

    def get_delivery_log(
        self,
        subscription_id: str,
        limit: int = 50,
    ) -> list[dict]:
        """
        Get delivery log for a subscription.

        Args:
            subscription_id: Subscription ID
            limit: Max records to return

        Returns:
            List of delivery records, newest first
        """
        deliveries = self.db.list_webhook_deliveries(subscription_id, limit)

        return [
            {
                "id": d.get("id"),
                "event_type": d.get("event_type"),
                "status": d.get("status"),
                "response_code": d.get("response_code"),
                "attempts": d.get("attempts"),
                "last_attempt_at": d.get("last_attempt_at"),
                "next_retry_at": d.get("next_retry_at"),
                "created_at": d.get("created_at"),
            }
            for d in deliveries
        ]


# Singleton
_service: Optional[OutboundWebhookService] = None


def get_outbound_webhook_service() -> OutboundWebhookService:
    """Get the singleton OutboundWebhookService instance."""
    global _service
    if _service is None:
        _service = OutboundWebhookService()
    return _service
