"""
WebSocket-based real-time notification system for RosterIQ On-Shift dashboard.

Pub-sub hub for live cut/call recommendations and signal updates.
Thread-safe with optional async broadcasting to WebSocket subscribers.

Classes:
- Notification: Immutable notification with id, venue_id, kind, title, body, data, severity
- NotificationStore: Thread-safe store for last N notifications per venue
- WSHub: Pub-sub hub for subscribers (async send callables) keyed by venue_id

Singletons:
- get_hub() -> WSHub
- get_notification_store() -> NotificationStore

Helpers:
- build_cut_recommendation(venue_id, confidence_pct, staff_names, reason) -> Notification
- build_call_recommendation(venue_id, confidence_pct, role, reason) -> Notification
- build_signal_update(venue_id, signal_type, old_value, new_value) -> Notification
"""

from __future__ import annotations

import logging
import threading
import json
import time
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

logger = logging.getLogger(__name__)

# ============================================================================
# Notification Model
# ============================================================================


@dataclass
class Notification:
    """
    Immutable notification for on-shift dashboard.

    Fields:
        id: UUID hex[:12], auto-generated
        venue_id: Venue identifier
        kind: Type of notification (cut_recommendation, call_recommendation, signal_update, forecast_shift, alert)
        title: Short notification title
        body: Detailed message
        data: Additional structured data (dict)
        created_at: Notification creation timestamp (UTC)
        severity: Level (info, warning, critical)
        acknowledged: Whether manager has acknowledged
    """

    id: str = field(default_factory=lambda: uuid4().hex[:12])
    venue_id: str = ""
    kind: str = ""  # enum-ish: cut_recommendation, call_recommendation, signal_update, forecast_shift, alert
    title: str = ""
    body: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    severity: str = "info"  # info, warning, critical
    acknowledged: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert notification to JSON-serializable dict."""
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "kind": self.kind,
            "title": self.title,
            "body": self.body,
            "data": self.data,
            "created_at": self.created_at.isoformat(),
            "severity": self.severity,
            "acknowledged": self.acknowledged,
        }


# ============================================================================
# Notification Store
# ============================================================================


class NotificationStore:
    """
    Thread-safe store for notifications.

    Keeps last N (default 200) per venue. Thread-locked.
    """

    def __init__(self, max_size: int = 200):
        """
        Initialize store.

        Args:
            max_size: Maximum notifications to keep per venue
        """
        self.max_size = max_size
        self._store: Dict[str, List[Notification]] = defaultdict(list)
        self._lock = threading.Lock()

    def add(self, notification: Notification) -> Notification:
        """
        Add notification to store.

        Enforces max_size by trimming oldest if needed.

        Args:
            notification: Notification to add

        Returns:
            The notification (for chaining)
        """
        with self._lock:
            venue_id = notification.venue_id
            self._store[venue_id].append(notification)

            # Trim oldest if over max_size
            if len(self._store[venue_id]) > self.max_size:
                self._store[venue_id] = self._store[venue_id][-self.max_size :]

        return notification

    def list_for_venue(self, venue_id: str, limit: int = 50) -> List[Notification]:
        """
        List recent notifications for venue.

        Returns newest first.

        Args:
            venue_id: Venue identifier
            limit: Max notifications to return

        Returns:
            List of notifications (newest first)
        """
        with self._lock:
            all_notifs = self._store.get(venue_id, [])
            return list(reversed(all_notifs[-limit:]))

    def acknowledge(self, notification_id: str) -> bool:
        """
        Mark notification as acknowledged.

        Args:
            notification_id: Notification ID

        Returns:
            True if found and updated, False otherwise
        """
        with self._lock:
            for venue_list in self._store.values():
                for notif in venue_list:
                    if notif.id == notification_id:
                        notif.acknowledged = True
                        return True
        return False

    def clear_venue(self, venue_id: str) -> None:
        """
        Clear all notifications for a venue.

        Args:
            venue_id: Venue identifier
        """
        with self._lock:
            self._store[venue_id] = []


# ============================================================================
# WebSocket Hub (Pub-Sub)
# ============================================================================


class WSHub:
    """
    Pub-sub hub for WebSocket connections.

    Manages subscriptions (async send callables) per venue_id and broadcasts
    notifications to all subscribers.

    Thread-safe for subscribe/unsubscribe/broadcast operations.
    """

    def __init__(self):
        """Initialize hub."""
        self._subscribers: Dict[str, Set[Callable]] = defaultdict(set)
        self._lock = threading.Lock()

    def subscribe(self, venue_id: str, send_fn: Callable) -> None:
        """
        Subscribe a send callable to a venue.

        Args:
            venue_id: Venue identifier
            send_fn: Async callable that takes notification dict
        """
        with self._lock:
            self._subscribers[venue_id].add(send_fn)
        logger.debug(f"Subscribed to {venue_id}: {self.subscriber_count(venue_id)} subscribers")

    def unsubscribe(self, venue_id: str, send_fn: Callable) -> None:
        """
        Unsubscribe a send callable from a venue.

        Args:
            venue_id: Venue identifier
            send_fn: Async callable to remove
        """
        with self._lock:
            self._subscribers[venue_id].discard(send_fn)
        logger.debug(f"Unsubscribed from {venue_id}: {self.subscriber_count(venue_id)} subscribers")

    async def broadcast(self, venue_id: str, notification: Notification) -> None:
        """
        Broadcast notification to all subscribers for venue.

        Catches and logs per-subscriber errors; never raises.

        Args:
            venue_id: Venue identifier
            notification: Notification to broadcast
        """
        with self._lock:
            subscribers = list(self._subscribers.get(venue_id, []))

        for send_fn in subscribers:
            try:
                await send_fn(notification.to_dict())
            except Exception as e:
                logger.error(f"Error broadcasting to subscriber: {e}")

    def publish(self, venue_id: str, notification: Notification) -> None:
        """
        Sync wrapper: store notification and schedule async broadcast.

        If no running event loop, creates one via threading.

        Args:
            venue_id: Venue identifier
            notification: Notification to publish
        """
        # Store notification
        get_notification_store().add(notification)

        # Broadcast to subscribers
        try:
            # Try to get the running loop
            loop = asyncio.get_running_loop()
            # If we get here, we're in an async context
            asyncio.create_task(self.broadcast(venue_id, notification))
        except RuntimeError:
            # No running loop, create one in a thread
            def _run_broadcast():
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(self.broadcast(venue_id, notification))
                except Exception as e:
                    logger.error(f"Error in broadcast thread: {e}")

            thread = threading.Thread(target=_run_broadcast, daemon=True)
            thread.start()

    def subscriber_count(self, venue_id: str) -> int:
        """
        Get number of subscribers for a venue.

        Args:
            venue_id: Venue identifier

        Returns:
            Subscriber count
        """
        with self._lock:
            return len(self._subscribers.get(venue_id, []))


# ============================================================================
# Singletons
# ============================================================================

_hub_instance: Optional[WSHub] = None
_store_instance: Optional[NotificationStore] = None
_singleton_lock = threading.Lock()


def get_hub() -> WSHub:
    """
    Get or create the global WSHub instance.

    Returns:
        Singleton WSHub
    """
    global _hub_instance
    if _hub_instance is None:
        with _singleton_lock:
            if _hub_instance is None:
                _hub_instance = WSHub()
    return _hub_instance


def get_notification_store() -> NotificationStore:
    """
    Get or create the global NotificationStore instance.

    Returns:
        Singleton NotificationStore
    """
    global _store_instance
    if _store_instance is None:
        with _singleton_lock:
            if _store_instance is None:
                _store_instance = NotificationStore(max_size=200)
    return _store_instance


def reset_singletons() -> None:
    """
    Reset singletons (for testing).

    Clears the global hub and store instances.
    """
    global _hub_instance, _store_instance
    with _singleton_lock:
        _hub_instance = None
        _store_instance = None


# ============================================================================
# Helper Builders
# ============================================================================


def build_cut_recommendation(
    venue_id: str,
    confidence_pct: float,
    staff_names: List[str],
    reason: str,
) -> Notification:
    """
    Build a cut (reduce staff) recommendation notification.

    Args:
        venue_id: Venue identifier
        confidence_pct: Confidence level (0-100)
        staff_names: List of staff names to potentially cut
        reason: Reason for recommendation (e.g., "low footfall", "weather")

    Returns:
        Notification
    """
    staff_str = ", ".join(staff_names) if staff_names else "staff"
    return Notification(
        venue_id=venue_id,
        kind="cut_recommendation",
        title=f"Cut {len(staff_names)} staff ({confidence_pct:.0f}% confident)",
        body=f"Consider cutting {staff_str}. Reason: {reason}",
        data={
            "confidence_pct": confidence_pct,
            "staff_names": staff_names,
            "reason": reason,
        },
        severity="warning",
    )


def build_call_recommendation(
    venue_id: str,
    confidence_pct: float,
    role: str,
    reason: str,
) -> Notification:
    """
    Build a call-in (add staff) recommendation notification.

    Args:
        venue_id: Venue identifier
        confidence_pct: Confidence level (0-100)
        role: Role/position needed (e.g., "line cook", "waiter")
        reason: Reason for recommendation

    Returns:
        Notification
    """
    return Notification(
        venue_id=venue_id,
        kind="call_recommendation",
        title=f"Call in {role} ({confidence_pct:.0f}% confident)",
        body=f"Consider calling in a {role}. Reason: {reason}",
        data={
            "confidence_pct": confidence_pct,
            "role": role,
            "reason": reason,
        },
        severity="warning",
    )


def build_signal_update(
    venue_id: str,
    signal_type: str,
    old_value: Any,
    new_value: Any,
) -> Notification:
    """
    Build a signal update notification.

    Args:
        venue_id: Venue identifier
        signal_type: Type of signal (e.g., "footfall", "weather", "booking")
        old_value: Previous value
        new_value: New value

    Returns:
        Notification
    """
    return Notification(
        venue_id=venue_id,
        kind="signal_update",
        title=f"{signal_type.title()} updated",
        body=f"{signal_type.title()} changed from {old_value} to {new_value}",
        data={
            "signal_type": signal_type,
            "old_value": old_value,
            "new_value": new_value,
        },
        severity="info",
    )
