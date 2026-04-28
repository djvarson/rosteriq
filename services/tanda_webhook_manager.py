"""
Tanda webhook registration and lifecycle management.

Handles registering, listing, updating, and deregistering webhooks with Tanda API.
"""
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class WebhookSubscription:
    """Represents a registered webhook subscription with Tanda."""
    subscription_id: str
    venue_id: str
    event_type: str
    endpoint_url: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
    last_event_at: Optional[datetime] = None
    failure_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "subscription_id": self.subscription_id,
            "venue_id": self.venue_id,
            "event_type": self.event_type,
            "endpoint_url": self.endpoint_url,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "is_active": self.is_active,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "failure_count": self.failure_count,
        }


class WebhookManager:
    """Manages webhook registration and lifecycle with Tanda API."""

    def __init__(self, tanda_adapter=None, db_session=None):
        """
        Initialize webhook manager.

        Args:
            tanda_adapter: Instance of TandaAdapter for API calls
            db_session: SQLAlchemy session for persistence
        """
        self.tanda_adapter = tanda_adapter
        self.db_session = db_session
        # In-memory subscription store (backed by database)
        self._subscriptions: Dict[str, WebhookSubscription] = {}

    async def register_webhook(
        self,
        venue_id: str,
        event_type: str,
        endpoint_url: str,
    ) -> WebhookSubscription:
        """
        Register a webhook with Tanda API.

        Args:
            venue_id: Tanda venue ID
            event_type: Event type to subscribe to (e.g. 'roster.published')
            endpoint_url: Full URL where Tanda should POST events

        Returns:
            WebhookSubscription object with subscription_id
        """
        if not self.tanda_adapter:
            raise RuntimeError("TandaAdapter not configured")

        try:
            # Call Tanda API to register webhook
            response = await self.tanda_adapter.register_webhook(
                venue_id=venue_id,
                event_type=event_type,
                endpoint_url=endpoint_url,
            )

            subscription_id = response.get("subscription_id") or response.get("id")
            if not subscription_id:
                raise ValueError("Tanda API did not return subscription_id")

            # Create subscription object
            subscription = WebhookSubscription(
                subscription_id=subscription_id,
                venue_id=venue_id,
                event_type=event_type,
                endpoint_url=endpoint_url,
            )

            # Store locally
            key = f"{venue_id}:{event_type}"
            self._subscriptions[key] = subscription

            # Persist to database if session available
            if self.db_session:
                await self._persist_subscription(subscription)

            logger.info(
                f"Registered webhook {subscription_id} for {venue_id}:{event_type}"
            )
            return subscription

        except Exception as e:
            logger.error(f"Failed to register webhook: {e}")
            raise

    async def list_webhooks(self, venue_id: str) -> List[WebhookSubscription]:
        """
        List all registered webhooks for a venue.

        Args:
            venue_id: Tanda venue ID

        Returns:
            List of WebhookSubscription objects
        """
        try:
            # Get from Tanda API
            if self.tanda_adapter:
                response = await self.tanda_adapter.list_webhooks(venue_id)
                webhooks = response.get("webhooks", [])

                subscriptions = []
                for webhook in webhooks:
                    sub = WebhookSubscription(
                        subscription_id=webhook.get("id"),
                        venue_id=venue_id,
                        event_type=webhook.get("event_type"),
                        endpoint_url=webhook.get("endpoint_url"),
                    )
                    subscriptions.append(sub)
                    key = f"{venue_id}:{webhook.get('event_type')}"
                    self._subscriptions[key] = sub

                return subscriptions

            # Fallback to local cache
            return [
                sub for sub in self._subscriptions.values()
                if sub.venue_id == venue_id
            ]

        except Exception as e:
            logger.error(f"Failed to list webhooks: {e}")
            raise

    async def update_webhook(
        self,
        subscription_id: str,
        venue_id: str,
        event_type: str,
        **updates
    ) -> WebhookSubscription:
        """
        Update a webhook subscription.

        Args:
            subscription_id: Subscription ID to update
            venue_id: Venue ID (for verification)
            event_type: Event type (for lookup)
            **updates: Fields to update (endpoint_url, is_active, etc.)

        Returns:
            Updated WebhookSubscription
        """
        try:
            # Update via Tanda API if available
            if self.tanda_adapter:
                await self.tanda_adapter.update_webhook(
                    subscription_id=subscription_id,
                    **updates
                )

            # Update local subscription
            key = f"{venue_id}:{event_type}"
            if key in self._subscriptions:
                sub = self._subscriptions[key]
                for field, value in updates.items():
                    if hasattr(sub, field):
                        setattr(sub, field, value)
                sub.updated_at = datetime.utcnow()

                # Persist changes
                if self.db_session:
                    await self._persist_subscription(sub)

                logger.info(f"Updated webhook {subscription_id}")
                return sub

            raise ValueError(f"Subscription {subscription_id} not found")

        except Exception as e:
            logger.error(f"Failed to update webhook: {e}")
            raise

    async def deregister_webhook(
        self,
        subscription_id: str,
        venue_id: str,
        event_type: str,
    ) -> bool:
        """
        Deregister a webhook from Tanda API.

        Args:
            subscription_id: Subscription ID to deregister
            venue_id: Venue ID (for verification)
            event_type: Event type (for lookup)

        Returns:
            True if successful
        """
        try:
            # Deregister from Tanda API if available
            if self.tanda_adapter:
                await self.tanda_adapter.deregister_webhook(subscription_id)

            # Remove from local store
            key = f"{venue_id}:{event_type}"
            if key in self._subscriptions:
                del self._subscriptions[key]

            # Delete from database if session available
            if self.db_session:
                await self._delete_subscription(subscription_id)

            logger.info(f"Deregistered webhook {subscription_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to deregister webhook: {e}")
            raise

    async def register_all_webhooks(
        self,
        venue_id: str,
        endpoint_url: str,
        event_types: Optional[List[str]] = None,
    ) -> List[WebhookSubscription]:
        """
        Register multiple webhooks for a venue.

        Args:
            venue_id: Tanda venue ID
            endpoint_url: Base endpoint URL
            event_types: List of event types to subscribe to.
                         Defaults to all standard types.

        Returns:
            List of registered WebhookSubscription objects
        """
        if event_types is None:
            event_types = [
                "roster.published",
                "shift.created",
                "shift.updated",
                "shift.deleted",
                "employee.created",
                "employee.updated",
                "employee.deactivated",
                "timesheet.approved",
                "leave.created",
                "leave.updated",
            ]

        subscriptions = []
        for event_type in event_types:
            try:
                sub = await self.register_webhook(
                    venue_id=venue_id,
                    event_type=event_type,
                    endpoint_url=endpoint_url,
                )
                subscriptions.append(sub)
            except Exception as e:
                logger.error(f"Failed to register {event_type}: {e}")
                continue

        return subscriptions

    def record_event_received(
        self,
        venue_id: str,
        event_type: str,
    ):
        """Record that an event was received for a subscription."""
        key = f"{venue_id}:{event_type}"
        if key in self._subscriptions:
            self._subscriptions[key].last_event_at = datetime.utcnow()
            # Reset failure count on successful event
            self._subscriptions[key].failure_count = 0

    def record_event_failure(
        self,
        venue_id: str,
        event_type: str,
    ):
        """Record that event processing failed."""
        key = f"{venue_id}:{event_type}"
        if key in self._subscriptions:
            self._subscriptions[key].failure_count += 1
            # Disable subscription if too many failures
            if self._subscriptions[key].failure_count > 10:
                self._subscriptions[key].is_active = False
                logger.warning(
                    f"Disabled webhook {key} due to repeated failures"
                )

    async def _persist_subscription(self, subscription: WebhookSubscription):
        """Persist subscription to database (stub for future implementation)."""
        # This would interact with a WebhookSubscription model
        # For now, subscriptions are stored in-memory
        pass

    async def _delete_subscription(self, subscription_id: str):
        """Delete subscription from database (stub for future implementation)."""
        # This would interact with a WebhookSubscription model
        # For now, subscriptions are stored in-memory
        pass

    def get_subscription(
        self,
        venue_id: str,
        event_type: str,
    ) -> Optional[WebhookSubscription]:
        """Get a specific subscription."""
        key = f"{venue_id}:{event_type}"
        return self._subscriptions.get(key)

    def get_all_subscriptions(self) -> List[WebhookSubscription]:
        """Get all subscriptions."""
        return list(self._subscriptions.values())


# Global manager instance
_manager: Optional[WebhookManager] = None


def init_webhook_manager(tanda_adapter=None, db_session=None) -> WebhookManager:
    """Initialize the global webhook manager."""
    global _manager
    _manager = WebhookManager(tanda_adapter=tanda_adapter, db_session=db_session)
    return _manager


def get_webhook_manager() -> WebhookManager:
    """Get the global webhook manager."""
    if _manager is None:
        _manager = WebhookManager()
    return _manager
