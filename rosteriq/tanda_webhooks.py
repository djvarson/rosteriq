"""Tanda webhook receiver for real-time event handling.

This module handles incoming Tanda webhook events with signature verification,
in-memory event storage, and event dispatching. Events are stored for debugging
and replay, and can trigger cache invalidation or downstream handlers.

Webhook events cover: shift.published, shift.updated, timesheet.approved,
employee.updated, and extensible via register_handler(event_type, coro).
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signature verification (HMAC-SHA256)
# ---------------------------------------------------------------------------


def verify_tanda_signature(
    signature_header: str, payload: bytes, secret: str
) -> bool:
    """Verify Tanda webhook signature using HMAC-SHA256.

    Implements Tanda's canonical algorithm:
    1. HMAC-SHA256 of payload bytes with secret as key
    2. Hex-encode result
    3. Compare to signature_header using hmac.compare_digest

    Args:
        signature_header: X-Tanda-Signature header value (hex string)
        payload: Raw request body bytes
        secret: Tanda webhook secret from env var TANDA_WEBHOOK_SECRET

    Returns:
        True if signature is valid, False otherwise.
    """
    expected_sig = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected_sig, signature_header)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass
class TandaWebhookEvent:
    """Parsed Tanda webhook event.

    Attributes:
        event_id: Unique event identifier
        event_type: Event type (e.g., "shift.published")
        org_id: Organization/account ID
        occurred_at: When event occurred
        data: Event payload (varies by type)
        raw: Full raw event dict (for debugging)
    """

    event_id: str
    event_type: str
    org_id: str
    occurred_at: datetime
    data: Dict[str, Any]
    raw: Dict[str, Any]


# ---------------------------------------------------------------------------
# Event Parsing
# ---------------------------------------------------------------------------


def parse_tanda_event(payload: dict) -> TandaWebhookEvent:
    """Parse a Tanda webhook payload into a TandaWebhookEvent.

    Tolerant to shape variation: expects event_type and org_id at root,
    and data nested under 'data', 'payload', or at the root.

    Args:
        payload: Raw webhook JSON dict

    Returns:
        Parsed TandaWebhookEvent

    Raises:
        ValueError: If required fields missing
    """
    event_type = payload.get("event_type")
    org_id = payload.get("org_id") or payload.get("organisation_id")

    if not event_type or not org_id:
        raise ValueError(
            f"Missing required fields: event_type={event_type}, org_id={org_id}"
        )

    # Occurred_at defaults to now if missing
    occurred_at_str = payload.get("occurred_at") or payload.get("timestamp")
    if occurred_at_str:
        try:
            if isinstance(occurred_at_str, str):
                occurred_at = datetime.fromisoformat(
                    occurred_at_str.replace("Z", "+00:00")
                )
            else:
                occurred_at = datetime.fromtimestamp(
                    occurred_at_str, tz=timezone.utc
                )
        except (ValueError, TypeError):
            occurred_at = datetime.now(tz=timezone.utc)
    else:
        occurred_at = datetime.now(tz=timezone.utc)

    # Extract event data; tolerant to shape variations
    data = payload.get("data") or payload.get("payload") or {}
    if not data:
        # If no explicit data key, use root minus metadata fields
        data = {
            k: v
            for k, v in payload.items()
            if k
            not in ["event_type", "org_id", "organisation_id", "occurred_at", "timestamp"]
        }

    event_id = str(uuid4())

    return TandaWebhookEvent(
        event_id=event_id,
        event_type=event_type,
        org_id=org_id,
        occurred_at=occurred_at,
        data=data,
        raw=payload,
    )


# ---------------------------------------------------------------------------
# Event Store
# ---------------------------------------------------------------------------


class TandaWebhookStore:
    """In-memory store for webhook events, keyed by org_id.

    Keeps the last N events per org, supports expiry by age.
    Thread-safe via GIL (single-threaded event loop).
    """

    def __init__(self, max_events_per_org: int = 100, ttl_hours: int = 24):
        """Initialize store.

        Args:
            max_events_per_org: Maximum events to keep per org
            ttl_hours: Expire events older than this many hours
        """
        self.max_events_per_org = max_events_per_org
        self.ttl_hours = ttl_hours
        self._events: Dict[str, List[TandaWebhookEvent]] = {}

    def append(self, event: TandaWebhookEvent) -> None:
        """Append event to store for its org.

        Automatically expires old events and trims if max exceeded.

        Args:
            event: Event to store
        """
        org_id = event.org_id
        if org_id not in self._events:
            self._events[org_id] = []

        self._events[org_id].append(event)

        # Expire old events and trim
        self._cleanup_org(org_id)

    def list_for_org(
        self, org_id: str, limit: int = 50
    ) -> List[TandaWebhookEvent]:
        """List recent events for an org (newest first).

        Args:
            org_id: Organization ID
            limit: Max events to return

        Returns:
            List of events, newest first
        """
        self._cleanup_org(org_id)
        events = self._events.get(org_id, [])
        return sorted(events, key=lambda e: e.occurred_at, reverse=True)[:limit]

    def get_event(self, event_id: str) -> Optional[TandaWebhookEvent]:
        """Get event by ID from any org.

        Args:
            event_id: Event ID to find

        Returns:
            Event if found, None otherwise
        """
        for events in self._events.values():
            for event in events:
                if event.event_id == event_id:
                    return event
        return None

    def clear(self) -> None:
        """Clear all stored events."""
        self._events.clear()

    def _cleanup_org(self, org_id: str) -> None:
        """Remove old events and trim to max_events_per_org for an org."""
        if org_id not in self._events:
            return

        now = datetime.now(tz=timezone.utc)
        cutoff = now - timedelta(hours=self.ttl_hours)

        # Expire old
        self._events[org_id] = [
            e for e in self._events[org_id] if e.occurred_at > cutoff
        ]

        # Trim to max
        if len(self._events[org_id]) > self.max_events_per_org:
            self._events[org_id] = self._events[org_id][
                -self.max_events_per_org :
            ]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_webhook_store: Optional[TandaWebhookStore] = None


def get_webhook_store() -> TandaWebhookStore:
    """Get or create the webhook store singleton."""
    global _webhook_store
    if _webhook_store is None:
        _webhook_store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)
    return _webhook_store


# ---------------------------------------------------------------------------
# Event Dispatcher
# ---------------------------------------------------------------------------

# Registry of event handlers: event_type -> list of coroutines
_handlers: Dict[str, List[Callable]] = {}


def register_handler(event_type: str, handler: Callable) -> None:
    """Register a handler for an event type.

    Handler should be async and accept (event: TandaWebhookEvent).

    Args:
        event_type: Event type string (e.g., "shift.published")
        handler: Async callable
    """
    if event_type not in _handlers:
        _handlers[event_type] = []
    _handlers[event_type].append(handler)
    logger.debug(f"Registered handler for {event_type}: {handler}")


async def dispatch(event: TandaWebhookEvent) -> None:
    """Dispatch an event to all registered handlers and built-in handlers.

    Built-in handlers (no-ops for now that store + log):
    - shift.published: invalidate cache
    - shift.updated: invalidate cache
    - timesheet.approved: invalidate cache
    - employee.updated: invalidate cache

    Args:
        event: Event to dispatch
    """
    store = get_webhook_store()
    store.append(event)

    logger.info(
        f"Dispatching {event.event_type} for org {event.org_id}: {event.event_id}"
    )

    # Built-in handlers
    if event.event_type in ["shift.published", "shift.updated", "timesheet.approved"]:
        await _invalidate_tanda_cache(event.org_id)

    # Call registered handlers
    if event.event_type in _handlers:
        for handler in _handlers[event.event_type]:
            try:
                await handler(event)
            except Exception as e:
                logger.error(
                    f"Handler error for {event.event_type}: {e}", exc_info=True
                )


async def _invalidate_tanda_cache(org_id: str) -> None:
    """Invalidate Tanda cache for an org if cache exists.

    Tries to import and call invalidate if available; silently continues
    if not found.

    Args:
        org_id: Organization ID
    """
    try:
        # Try to import and call cache invalidation
        from rosteriq.tanda_integration import TandaSync  # type: ignore

        # TandaSync has _employee_cache, _department_cache. We could try
        # to reset them, but without an instance, we'll just log.
        logger.debug(
            f"Tanda cache invalidation for {org_id}: "
            "TandaSync import available but no singleton instance; "
            "polling will refresh on next read"
        )
    except (ImportError, AttributeError):
        logger.debug(
            f"Tanda cache invalidation for {org_id}: "
            "No TandaSync cache found; skipping"
        )
