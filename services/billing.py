"""
Stripe billing integration for RosterIQ.

Provides subscription management, checkout sessions, webhook handling,
and subscription tier tracking. All subscription data is persisted via
the BaseStore database layer.

Usage:
    from rosteriq.services.billing import billing_service
    session = await billing_service.create_checkout_session(
        venue_id="venue-1",
        tier="pro",
        success_url="https://example.com/success",
        cancel_url="https://example.com/cancel"
    )
"""

import os
import logging
import hashlib
import hmac
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from decimal import Decimal

logger = logging.getLogger(__name__)

# Try to import Stripe; graceful fallback if not installed
try:
    import stripe
    STRIPE_AVAILABLE = True
except ImportError:
    STRIPE_AVAILABLE = False
    logger.warning("Stripe not installed — billing features disabled. Install with: pip install stripe")


# ============================================================================
# Enums and dataclasses
# ============================================================================

class SubscriptionTier(str, Enum):
    """Subscription tier levels."""
    starter = "starter"
    pro = "pro"
    enterprise = "enterprise"


class SubscriptionStatus(str, Enum):
    """Subscription status states."""
    active = "active"
    past_due = "past_due"
    cancelled = "cancelled"
    inactive = "inactive"  # Trial expired or never paid


# Pricing in AUD cents (100 = $1)
TIER_PRICES = {
    SubscriptionTier.starter: 4900,      # $49/month
    SubscriptionTier.pro: 9900,          # $99/month
    SubscriptionTier.enterprise: 19900,  # $199/month
}

TIER_FEATURES = {
    SubscriptionTier.starter: {
        "basic_roster": True,
        "single_venue": True,
        "email_notifications": True,
        "ai_optimisation": False,
        "multi_venue": False,
        "xero_integration": False,
        "advanced_reports": False,
        "api_access": False,
        "custom_integrations": False,
        "priority_support": False,
    },
    SubscriptionTier.pro: {
        "basic_roster": True,
        "single_venue": False,
        "email_notifications": True,
        "ai_optimisation": True,
        "multi_venue": True,
        "xero_integration": True,
        "advanced_reports": True,
        "api_access": False,
        "custom_integrations": False,
        "priority_support": False,
    },
    SubscriptionTier.enterprise: {
        "basic_roster": True,
        "single_venue": False,
        "email_notifications": True,
        "ai_optimisation": True,
        "multi_venue": True,
        "xero_integration": True,
        "advanced_reports": True,
        "api_access": True,
        "custom_integrations": True,
        "priority_support": True,
    },
}


@dataclass
class SubscriptionData:
    """Represents a venue's subscription."""
    venue_id: str
    stripe_customer_id: Optional[str] = None
    stripe_subscription_id: Optional[str] = None
    tier: SubscriptionTier = SubscriptionTier.starter
    status: SubscriptionStatus = SubscriptionStatus.inactive
    current_period_start: Optional[datetime] = None
    current_period_end: Optional[datetime] = None
    payment_method: Optional[str] = None  # e.g., "card_1234"
    last_payment_date: Optional[datetime] = None
    next_billing_date: Optional[datetime] = None
    cancel_at_period_end: bool = False
    created_at: datetime = None
    updated_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "venue_id": self.venue_id,
            "stripe_customer_id": self.stripe_customer_id,
            "stripe_subscription_id": self.stripe_subscription_id,
            "tier": self.tier.value,
            "status": self.status.value,
            "current_period_start": self.current_period_start.isoformat() if self.current_period_start else None,
            "current_period_end": self.current_period_end.isoformat() if self.current_period_end else None,
            "payment_method": self.payment_method,
            "last_payment_date": self.last_payment_date.isoformat() if self.last_payment_date else None,
            "next_billing_date": self.next_billing_date.isoformat() if self.next_billing_date else None,
            "cancel_at_period_end": self.cancel_at_period_end,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SubscriptionData":
        """Create from stored dictionary."""
        def parse_datetime(dt_str):
            if dt_str is None:
                return None
            if isinstance(dt_str, datetime):
                return dt_str
            return datetime.fromisoformat(dt_str)

        return cls(
            venue_id=data["venue_id"],
            stripe_customer_id=data.get("stripe_customer_id"),
            stripe_subscription_id=data.get("stripe_subscription_id"),
            tier=SubscriptionTier(data.get("tier", "starter")),
            status=SubscriptionStatus(data.get("status", "inactive")),
            current_period_start=parse_datetime(data.get("current_period_start")),
            current_period_end=parse_datetime(data.get("current_period_end")),
            payment_method=data.get("payment_method"),
            last_payment_date=parse_datetime(data.get("last_payment_date")),
            next_billing_date=parse_datetime(data.get("next_billing_date")),
            cancel_at_period_end=data.get("cancel_at_period_end", False),
            created_at=parse_datetime(data.get("created_at")),
            updated_at=parse_datetime(data.get("updated_at")),
        )


@dataclass
class BillingEvent:
    """Record of a billing-related event."""
    event_id: str
    venue_id: str
    event_type: str  # e.g., "checkout.session.completed", "invoice.paid", "subscription.created"
    stripe_event_id: Optional[str] = None
    payload: Optional[dict] = None
    processed: bool = False
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "venue_id": self.venue_id,
            "event_type": self.event_type,
            "stripe_event_id": self.stripe_event_id,
            "payload": self.payload,
            "processed": self.processed,
            "created_at": self.created_at.isoformat(),
        }


# ============================================================================
# BillingService
# ============================================================================

class BillingService:
    """Stripe billing integration service."""

    def __init__(self):
        self.stripe_key = os.environ.get("STRIPE_SECRET_KEY")
        self.stripe_webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

        if STRIPE_AVAILABLE and self.stripe_key:
            stripe.api_key = self.stripe_key
            logger.info("Stripe configured (key found)")
        elif STRIPE_AVAILABLE:
            logger.warning("Stripe library loaded but STRIPE_SECRET_KEY not set — billing disabled")
        else:
            logger.warning("Stripe library not available — billing disabled")

    def is_enabled(self) -> bool:
        """Check if Stripe billing is enabled."""
        return STRIPE_AVAILABLE and bool(self.stripe_key)

    # ========================================================================
    # Checkout & customer management
    # ========================================================================

    async def create_checkout_session(
        self,
        venue_id: str,
        tier: str,
        success_url: str,
        cancel_url: str,
        email: Optional[str] = None,
    ) -> dict:
        """
        Create a Stripe Checkout session for subscription.

        Args:
            venue_id: The venue ID
            tier: Subscription tier ('starter', 'pro', 'enterprise')
            success_url: URL to redirect to after successful payment
            cancel_url: URL to redirect to if payment is cancelled
            email: Optional customer email for prefill

        Returns:
            Dict with session_id, client_secret, and redirect URL
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        try:
            tier_enum = SubscriptionTier(tier)
        except ValueError:
            raise ValueError(f"Invalid tier: {tier}. Must be one of {[t.value for t in SubscriptionTier]}")

        price_cents = TIER_PRICES[tier_enum]

        # Create or get customer
        customer = None
        if email:
            # Search for existing customer
            customers = stripe.Customer.list(email=email, limit=1)
            if customers.data:
                customer = customers.data[0]
            else:
                customer = stripe.Customer.create(email=email)

        # Create checkout session
        session_params = {
            "payment_method_types": ["card"],
            "line_items": [
                {
                    "price_data": {
                        "currency": "aud",
                        "product_data": {
                            "name": f"RosterIQ {tier_enum.value.capitalize()}",
                            "description": f"AI-powered rostering for hospitality ({tier_enum.value})",
                            "metadata": {"tier": tier_enum.value, "venue_id": venue_id},
                        },
                        "unit_amount": price_cents,
                        "recurring": {
                            "interval": "month",
                            "interval_count": 1,
                        },
                    },
                    "quantity": 1,
                }
            ],
            "mode": "subscription",
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata": {"venue_id": venue_id, "tier": tier},
        }

        if customer:
            session_params["customer"] = customer.id
        elif email:
            session_params["customer_email"] = email

        session = stripe.checkout.Session.create(**session_params)

        return {
            "session_id": session.id,
            "client_secret": session.client_secret,
            "url": session.url,
            "venue_id": venue_id,
            "tier": tier,
        }

    async def create_customer(
        self,
        email: str,
        name: str,
        venue_id: str,
    ) -> dict:
        """
        Create a Stripe customer.

        Args:
            email: Customer email
            name: Customer name
            venue_id: Associated venue ID

        Returns:
            Dict with customer_id and status
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        customer = stripe.Customer.create(
            email=email,
            name=name,
            metadata={"venue_id": venue_id},
        )

        return {
            "customer_id": customer.id,
            "email": customer.email,
            "name": customer.name,
        }

    # ========================================================================
    # Subscription status & management
    # ========================================================================

    async def get_subscription_status(self, venue_id: str, db=None) -> Optional[dict]:
        """
        Get current subscription status for a venue.

        Returns subscription details or None if no subscription found.
        """
        if db is None:
            from rosteriq.database import get_db
            db = get_db()

        sub_data = db.get_subscription(venue_id)
        if not sub_data:
            return None

        sub = SubscriptionData.from_dict(sub_data)
        features = TIER_FEATURES.get(sub.tier, {})

        return {
            "venue_id": venue_id,
            "tier": sub.tier.value,
            "status": sub.status.value,
            "features": features,
            "current_period_start": sub.current_period_start.isoformat() if sub.current_period_start else None,
            "current_period_end": sub.current_period_end.isoformat() if sub.current_period_end else None,
            "next_billing_date": sub.next_billing_date.isoformat() if sub.next_billing_date else None,
            "cancel_at_period_end": sub.cancel_at_period_end,
            "stripe_subscription_id": sub.stripe_subscription_id,
        }

    async def cancel_subscription(self, venue_id: str, db=None, immediate: bool = False) -> dict:
        """
        Cancel a subscription.

        Args:
            venue_id: The venue ID
            db: Database instance
            immediate: If True, cancel immediately. If False, cancel at period end.

        Returns:
            Dict with cancellation status
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        if db is None:
            from rosteriq.database import get_db
            db = get_db()

        sub_data = db.get_subscription(venue_id)
        if not sub_data:
            raise ValueError(f"No subscription found for venue {venue_id}")

        sub = SubscriptionData.from_dict(sub_data)

        if not sub.stripe_subscription_id:
            raise ValueError(f"No Stripe subscription ID for venue {venue_id}")

        # Cancel with Stripe
        if immediate:
            stripe.Subscription.delete(sub.stripe_subscription_id)
            sub.status = SubscriptionStatus.cancelled
        else:
            stripe.Subscription.modify(
                sub.stripe_subscription_id,
                cancel_at_period_end=True,
            )
            sub.cancel_at_period_end = True

        sub.updated_at = datetime.utcnow()
        db.save_subscription(sub.to_dict())

        return {
            "venue_id": venue_id,
            "status": sub.status.value,
            "cancel_at_period_end": sub.cancel_at_period_end,
            "cancelled_at": datetime.utcnow().isoformat(),
        }

    async def change_tier(self, venue_id: str, new_tier: str, db=None) -> dict:
        """
        Upgrade or downgrade subscription tier.

        Args:
            venue_id: The venue ID
            new_tier: New tier ('starter', 'pro', 'enterprise')
            db: Database instance

        Returns:
            Dict with updated subscription details
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        if db is None:
            from rosteriq.database import get_db
            db = get_db()

        try:
            new_tier_enum = SubscriptionTier(new_tier)
        except ValueError:
            raise ValueError(f"Invalid tier: {new_tier}")

        sub_data = db.get_subscription(venue_id)
        if not sub_data:
            raise ValueError(f"No subscription found for venue {venue_id}")

        sub = SubscriptionData.from_dict(sub_data)

        if not sub.stripe_subscription_id:
            raise ValueError(f"No Stripe subscription ID for venue {venue_id}")

        # Get current subscription from Stripe
        stripe_sub = stripe.Subscription.retrieve(sub.stripe_subscription_id)
        old_price_id = stripe_sub["items"].data[0].price.id

        # Get new price
        new_price_cents = TIER_PRICES[new_tier_enum]
        # In production, you'd need to create/retrieve the Stripe Price ID
        # For now, we'll update the local record and handle Stripe updates separately

        new_price = self._get_or_create_price(new_tier_enum, new_price_cents)

        # Update subscription in Stripe
        stripe.Subscription.modify(
            sub.stripe_subscription_id,
            items=[
                {
                    "id": stripe_sub["items"].data[0].id,
                    "price": new_price,
                }
            ],
            proration_behavior="create_prorations",
        )

        sub.tier = new_tier_enum
        sub.updated_at = datetime.utcnow()
        db.save_subscription(sub.to_dict())

        return {
            "venue_id": venue_id,
            "old_tier": sub.tier.value,
            "new_tier": new_tier_enum.value,
            "effective_date": datetime.utcnow().isoformat(),
        }

    async def record_usage(
        self,
        venue_id: str,
        metric: str,
        quantity: int,
        db=None,
    ) -> dict:
        """
        Record usage for a metered metric (e.g., API calls, reports generated).

        Args:
            venue_id: The venue ID
            metric: Metric name (e.g., 'api_calls', 'reports')
            quantity: Amount to record

        Returns:
            Dict with usage recorded
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        if db is None:
            from rosteriq.database import get_db
            db = get_db()

        # In production, you'd use Stripe's usage API
        # For now, just log and return

        logger.info(f"Usage recorded for {venue_id}: {metric}={quantity}")

        return {
            "venue_id": venue_id,
            "metric": metric,
            "quantity": quantity,
            "recorded_at": datetime.utcnow().isoformat(),
        }

    # ========================================================================
    # Webhook handling
    # ========================================================================

    async def handle_webhook_event(
        self,
        payload: bytes,
        sig_header: str,
        db=None,
    ) -> dict:
        """
        Verify and process a Stripe webhook event.

        Security features:
        - Verifies webhook signature using raw request body
        - Validates event timestamp (rejects replays > 5 minutes old)
        - Implements idempotency (prevents double-processing)
        - Logs all events for audit trail

        Args:
            payload: Raw webhook payload (bytes) — NOT parsed JSON
            sig_header: Stripe signature header value
            db: Database instance

        Returns:
            Dict with processing result

        Raises:
            ValueError: If signature verification fails or replay detected
            RuntimeError: If Stripe not configured
        """
        if not self.is_enabled():
            raise RuntimeError("Stripe not configured")

        if not self.stripe_webhook_secret:
            raise RuntimeError("STRIPE_WEBHOOK_SECRET not configured")

        if db is None:
            from rosteriq.database import get_db
            db = get_db()

        # STEP 1: Verify webhook signature using raw body
        # This must be done BEFORE parsing JSON
        try:
            event = stripe.Webhook.construct_event(
                payload,
                sig_header,
                self.stripe_webhook_secret,
            )
        except ValueError:
            raise ValueError("Invalid payload")
        except stripe.error.SignatureVerificationError:
            raise ValueError("Invalid webhook signature")

        event_type = event["type"]
        event_id = event["id"]
        event_timestamp = event.get("created", 0)

        # STEP 2: Replay attack protection
        # Reject events older than 5 minutes
        current_timestamp = datetime.utcnow().timestamp()
        event_age_seconds = current_timestamp - event_timestamp

        if event_age_seconds > 300:  # 5 minutes
            logger.warning(
                f"Rejected webhook {event_id} ({event_type}): "
                f"event is {event_age_seconds:.0f} seconds old (max 300)"
            )
            # Still return 200 to prevent Stripe retries
            # but mark as rejected
            return {
                "status": "rejected",
                "event_id": event_id,
                "reason": "replay_attack_detected",
                "event_age_seconds": event_age_seconds,
            }

        # STEP 3: Idempotency check
        # Don't process the same event twice
        if self._is_webhook_processed(event_id, db):
            logger.info(f"Webhook {event_id} already processed, skipping")
            return {
                "status": "duplicate",
                "event_id": event_id,
                "event_type": event_type,
            }

        logger.info(f"Processing webhook event {event_type} ({event_id})")

        # STEP 4: Process based on event type
        try:
            if event_type == "checkout.session.completed":
                result = await self._handle_checkout_completed(event, db)
            elif event_type == "invoice.paid":
                result = await self._handle_invoice_paid(event, db)
            elif event_type == "invoice.payment_failed":
                result = await self._handle_invoice_payment_failed(event, db)
            elif event_type == "customer.subscription.deleted":
                result = await self._handle_subscription_deleted(event, db)
            elif event_type == "customer.subscription.updated":
                result = await self._handle_subscription_updated(event, db)
            else:
                logger.info(f"Unhandled webhook event: {event_type}")
                result = {"status": "ignored", "event_type": event_type}

            # STEP 5: Mark event as processed (idempotency)
            self._mark_webhook_processed(event_id, db)

            # STEP 6: Audit log
            self._audit_log_webhook(event_id, event_type, "processed", db)

            return result

        except Exception as e:
            logger.exception(f"Error processing webhook {event_type} ({event_id}): {e}")
            # Audit log failure
            self._audit_log_webhook(event_id, event_type, "failed", db, error=str(e))
            raise

    async def _handle_checkout_completed(self, event: dict, db) -> dict:
        """Handle checkout.session.completed event."""
        session = event["data"]["object"]
        venue_id = session["metadata"].get("venue_id")
        tier = session["metadata"].get("tier")

        if not venue_id or not tier:
            logger.error(f"Checkout session missing venue_id or tier: {session['id']}")
            return {"status": "error", "message": "Missing metadata"}

        # Get or create subscription record
        sub_data = db.get_subscription(venue_id)
        if sub_data:
            sub = SubscriptionData.from_dict(sub_data)
        else:
            sub = SubscriptionData(venue_id=venue_id, tier=SubscriptionTier(tier))

        # Update with Stripe info
        sub.stripe_customer_id = session.get("customer")
        sub.stripe_subscription_id = session.get("subscription")
        sub.status = SubscriptionStatus.active
        sub.tier = SubscriptionTier(tier)
        sub.updated_at = datetime.utcnow()

        db.save_subscription(sub.to_dict())

        # Record event
        billing_event = BillingEvent(
            event_id=f"evt_{event['id']}",
            venue_id=venue_id,
            event_type="checkout.session.completed",
            stripe_event_id=event["id"],
            payload=event["data"]["object"],
            processed=True,
        )
        db.save_billing_event(billing_event.to_dict())

        logger.info(f"Checkout completed for venue {venue_id}, tier {tier}")
        return {"status": "processed", "venue_id": venue_id, "tier": tier}

    async def _handle_invoice_paid(self, event: dict, db) -> dict:
        """Handle invoice.paid event."""
        invoice = event["data"]["object"]
        stripe_subscription_id = invoice.get("subscription")

        # Find venue by subscription
        subscriptions = db.list_subscriptions()
        venue_id = None
        for sub_dict in subscriptions:
            if sub_dict.get("stripe_subscription_id") == stripe_subscription_id:
                venue_id = sub_dict["venue_id"]
                break

        if not venue_id:
            logger.warning(f"Invoice paid for unknown subscription: {stripe_subscription_id}")
            return {"status": "ignored", "reason": "subscription_not_found"}

        # Update subscription record
        sub_data = db.get_subscription(venue_id)
        if sub_data:
            sub = SubscriptionData.from_dict(sub_data)
            sub.last_payment_date = datetime.utcnow()
            sub.status = SubscriptionStatus.active
            sub.updated_at = datetime.utcnow()
            db.save_subscription(sub.to_dict())

        # Record event
        billing_event = BillingEvent(
            event_id=f"evt_{event['id']}",
            venue_id=venue_id,
            event_type="invoice.paid",
            stripe_event_id=event["id"],
            payload=invoice,
            processed=True,
        )
        db.save_billing_event(billing_event.to_dict())

        logger.info(f"Invoice paid for venue {venue_id}")
        return {"status": "processed", "venue_id": venue_id}

    async def _handle_invoice_payment_failed(self, event: dict, db) -> dict:
        """Handle invoice.payment_failed event."""
        invoice = event["data"]["object"]
        stripe_subscription_id = invoice.get("subscription")

        # Find venue by subscription
        subscriptions = db.list_subscriptions()
        venue_id = None
        for sub_dict in subscriptions:
            if sub_dict.get("stripe_subscription_id") == stripe_subscription_id:
                venue_id = sub_dict["venue_id"]
                break

        if not venue_id:
            logger.warning(f"Invoice payment failed for unknown subscription: {stripe_subscription_id}")
            return {"status": "ignored", "reason": "subscription_not_found"}

        # Flag account
        sub_data = db.get_subscription(venue_id)
        if sub_data:
            sub = SubscriptionData.from_dict(sub_data)
            sub.status = SubscriptionStatus.past_due
            sub.updated_at = datetime.utcnow()
            db.save_subscription(sub.to_dict())

        # Record event
        billing_event = BillingEvent(
            event_id=f"evt_{event['id']}",
            venue_id=venue_id,
            event_type="invoice.payment_failed",
            stripe_event_id=event["id"],
            payload=invoice,
            processed=True,
        )
        db.save_billing_event(billing_event.to_dict())

        logger.warning(f"Invoice payment failed for venue {venue_id}")
        return {"status": "processed", "venue_id": venue_id, "action": "marked_past_due"}

    async def _handle_subscription_deleted(self, event: dict, db) -> dict:
        """Handle customer.subscription.deleted event."""
        subscription = event["data"]["object"]
        stripe_subscription_id = subscription.get("id")

        # Find venue
        subscriptions = db.list_subscriptions()
        venue_id = None
        for sub_dict in subscriptions:
            if sub_dict.get("stripe_subscription_id") == stripe_subscription_id:
                venue_id = sub_dict["venue_id"]
                break

        if not venue_id:
            logger.warning(f"Subscription deleted for unknown ID: {stripe_subscription_id}")
            return {"status": "ignored", "reason": "subscription_not_found"}

        # Deactivate
        sub_data = db.get_subscription(venue_id)
        if sub_data:
            sub = SubscriptionData.from_dict(sub_data)
            sub.status = SubscriptionStatus.cancelled
            sub.updated_at = datetime.utcnow()
            db.save_subscription(sub.to_dict())

        # Record event
        billing_event = BillingEvent(
            event_id=f"evt_{event['id']}",
            venue_id=venue_id,
            event_type="customer.subscription.deleted",
            stripe_event_id=event["id"],
            payload=subscription,
            processed=True,
        )
        db.save_billing_event(billing_event.to_dict())

        logger.info(f"Subscription cancelled for venue {venue_id}")
        return {"status": "processed", "venue_id": venue_id, "action": "subscription_cancelled"}

    async def _handle_subscription_updated(self, event: dict, db) -> dict:
        """Handle customer.subscription.updated event."""
        subscription = event["data"]["object"]
        stripe_subscription_id = subscription.get("id")

        # Find venue
        subscriptions = db.list_subscriptions()
        venue_id = None
        for sub_dict in subscriptions:
            if sub_dict.get("stripe_subscription_id") == stripe_subscription_id:
                venue_id = sub_dict["venue_id"]
                break

        if not venue_id:
            return {"status": "ignored", "reason": "subscription_not_found"}

        # Update subscription record
        sub_data = db.get_subscription(venue_id)
        if sub_data:
            sub = SubscriptionData.from_dict(sub_data)

            # Update period dates
            if subscription.get("current_period_start"):
                sub.current_period_start = datetime.fromtimestamp(subscription["current_period_start"])
            if subscription.get("current_period_end"):
                sub.current_period_end = datetime.fromtimestamp(subscription["current_period_end"])

            # Check cancel_at_period_end
            if subscription.get("cancel_at_period_end"):
                sub.cancel_at_period_end = True

            sub.updated_at = datetime.utcnow()
            db.save_subscription(sub.to_dict())

        logger.info(f"Subscription updated for venue {venue_id}")
        return {"status": "processed", "venue_id": venue_id}

    # ========================================================================
    # Helpers & security
    # ========================================================================

    def _is_webhook_processed(self, event_id: str, db) -> bool:
        """
        Check if a webhook event has already been processed.

        Prevents double-processing of the same event (idempotency).
        """
        try:
            return db.is_billing_event_processed(event_id)
        except Exception as e:
            logger.warning(f"Failed to check webhook idempotency: {e}")
            # If database check fails, proceed anyway (at least log it)
            return False

    def _mark_webhook_processed(self, event_id: str, db):
        """
        Mark a webhook event as processed.

        Used for idempotency — ensures we don't process the same event twice.
        """
        try:
            db.mark_billing_event_processed(event_id)
        except Exception as e:
            logger.error(f"Failed to mark webhook as processed: {e}")

    def _audit_log_webhook(
        self,
        event_id: str,
        event_type: str,
        status: str,
        db,
        error: str = None,
    ):
        """
        Create an audit log entry for a webhook event.

        Logs for compliance and debugging:
        - Event ID, type, status
        - Processing timestamp
        - Error message (if failed)
        """
        try:
            audit_entry = {
                "webhook_event_id": event_id,
                "event_type": event_type,
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
                "error": error,
            }
            db.save_webhook_audit_log(audit_entry)
            logger.info(f"Audit logged: {event_type} {event_id} {status}")
        except Exception as e:
            logger.error(f"Failed to save webhook audit log: {e}")

    def _get_or_create_price(self, tier: SubscriptionTier, price_cents: int) -> str:
        """Get or create a Stripe price for a tier. Returns price ID."""
        # In production, you'd search Stripe for existing prices or create them
        # For now, return a placeholder
        # This would need proper integration with Stripe's product/price API
        return f"price_{tier.value}"


# ============================================================================
# Singleton instance
# ============================================================================

billing_service = BillingService()
