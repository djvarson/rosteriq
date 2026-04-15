"""Brief subscriptions — in-memory store for who gets which briefs.

Pure stdlib dict-based store. Each subscription tracks:
- venue_id: which venue
- user_id: recipient identifier
- user_role: "owner" | "manager" | "supervisor"
- email: recipient email (optional)
- phone: recipient phone (optional)
- brief_types: list of "morning" | "weekly" | "portfolio"
- delivery_channels: list of "email" | "sms"
- local_tz: recipient's timezone (defaults per venue)
- enabled: boolean flag

Seed with plausible demo data on fresh deploy so the scheduler
has someone to send to.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class BriefSubscription:
    """A single brief subscription."""
    subscription_id: str
    venue_id: str
    user_id: str
    user_role: str  # "owner" | "manager" | "supervisor"
    email: Optional[str] = None
    phone: Optional[str] = None
    brief_types: List[str] = field(default_factory=lambda: ["morning", "weekly"])
    delivery_channels: List[str] = field(default_factory=lambda: ["email", "sms"])
    local_tz: str = "Australia/Perth"
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for API responses."""
        return {
            "subscription_id": self.subscription_id,
            "venue_id": self.venue_id,
            "user_id": self.user_id,
            "user_role": self.user_role,
            "email": self.email,
            "phone": self.phone,
            "brief_types": self.brief_types,
            "delivery_channels": self.delivery_channels,
            "local_tz": self.local_tz,
            "enabled": self.enabled,
        }


class BriefSubscriptionStore:
    """In-memory dict-keyed store for brief subscriptions."""

    def __init__(self):
        self._store: Dict[str, BriefSubscription] = {}

    def create(
        self,
        venue_id: str,
        user_id: str,
        user_role: str,
        *,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        brief_types: Optional[List[str]] = None,
        delivery_channels: Optional[List[str]] = None,
        local_tz: str = "Australia/Perth",
    ) -> BriefSubscription:
        """Create a new subscription."""
        sub_id = f"sub_{uuid.uuid4().hex[:12]}"
        sub = BriefSubscription(
            subscription_id=sub_id,
            venue_id=venue_id,
            user_id=user_id,
            user_role=user_role,
            email=email,
            phone=phone,
            brief_types=brief_types or ["morning", "weekly"],
            delivery_channels=delivery_channels or ["email"],
            local_tz=local_tz,
            enabled=True,
        )
        self._store[sub_id] = sub
        return sub

    def get(self, subscription_id: str) -> Optional[BriefSubscription]:
        """Retrieve a subscription by ID."""
        return self._store.get(subscription_id)

    def list_for_venue(self, venue_id: str) -> List[BriefSubscription]:
        """List all active subscriptions for a venue."""
        return [
            s for s in self._store.values()
            if s.venue_id == venue_id and s.enabled
        ]

    def list_for_brief(
        self, venue_id: str, brief_type: str
    ) -> List[BriefSubscription]:
        """List subscriptions for a specific brief type at a venue."""
        return [
            s for s in self._store.values()
            if (s.venue_id == venue_id and
                s.enabled and
                brief_type in s.brief_types)
        ]

    def update(
        self,
        subscription_id: str,
        **kwargs,
    ) -> Optional[BriefSubscription]:
        """Update a subscription. Only 'enabled', 'delivery_channels',
        'brief_types', and 'local_tz' are mutable."""
        sub = self._store.get(subscription_id)
        if not sub:
            return None

        if "enabled" in kwargs:
            sub.enabled = bool(kwargs["enabled"])
        if "delivery_channels" in kwargs:
            sub.delivery_channels = list(kwargs["delivery_channels"])
        if "brief_types" in kwargs:
            sub.brief_types = list(kwargs["brief_types"])
        if "local_tz" in kwargs:
            sub.local_tz = str(kwargs["local_tz"])

        return sub

    def delete(self, subscription_id: str) -> bool:
        """Delete a subscription. Returns True if it existed."""
        return self._store.pop(subscription_id, None) is not None

    def clear(self):
        """Wipe store. Used by tests."""
        self._store.clear()

    def all(self) -> List[BriefSubscription]:
        """Return all subscriptions."""
        return list(self._store.values())


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[BriefSubscriptionStore] = None


def get_subscription_store() -> BriefSubscriptionStore:
    """Return the module-level singleton BriefSubscriptionStore."""
    global _store
    if _store is None:
        _store = BriefSubscriptionStore()
        seed_demo_subscriptions()
    return _store


def seed_demo_subscriptions():
    """Seed the store with plausible demo subscriptions.

    Creates a few test subscriptions per venue so fresh deploys
    have someone to send briefs to.
    """
    global _store
    if _store is None or len(_store.all()) > 0:
        return

    # Create demo venues and subscriptions
    demo_venues = ["venue_001", "venue_002"]
    demo_roles = [
        ("owner", "Dale Ingvarson", "dale@rosteriq.demo", "+61412345678", "Australia/Perth"),
        ("manager", "Sam Smith", "sam@rosteriq.demo", "+61487654321", "Australia/Brisbane"),
        ("manager", "Lee Chen", "lee@rosteriq.demo", None, "Australia/Sydney"),
    ]

    for venue_id in demo_venues:
        for i, (role, name, email, phone, tz) in enumerate(demo_roles):
            _store.create(
                venue_id=venue_id,
                user_id=f"user_{uuid.uuid4().hex[:8]}",
                user_role=role,
                email=email,
                phone=phone,
                brief_types=["morning", "weekly"] if role == "owner" else ["morning"],
                delivery_channels=["email", "sms"] if phone else ["email"],
                local_tz=tz,
            )


def reset_subscription_store_for_tests():
    """Reset singleton. Used by tests."""
    global _store
    _store = None
