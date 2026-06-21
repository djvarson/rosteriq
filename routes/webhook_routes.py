"""
Webhook receiver routes for Tanda events.

POST /api/webhooks/tanda — receives Tanda payloads with HMAC-SHA256 verification
GET /api/webhooks/status — returns subscription status
POST /api/webhooks/register — register new webhook
POST /api/webhooks/deregister — deregister webhook
"""
import hmac
import hashlib
import json
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from uuid import uuid4

try:
    from fastapi import (
        APIRouter,
        Request,
        Response,
        BackgroundTasks,
        HTTPException,
        Depends,
        status,
    )
except ImportError:
    # Fallback for sandbox environment
    APIRouter = None
    Request = None
    Response = None
    BackgroundTasks = None
    HTTPException = None
    Depends = None
    status = None

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])

# WebSocket hub reference (will be injected)
_ws_hub = None

# Database instance (lazy-loaded)
_db = None


def get_db():
    """Get database instance (lazy-loaded singleton)."""
    global _db
    if _db is None:
        try:
            from rosteriq.database import get_db as db_factory
            _db = db_factory()
        except ImportError:
            logger.warning("Database module not available — webhook idempotency disabled")
    return _db


def set_ws_hub(hub):
    """Set the WebSocket hub instance."""
    global _ws_hub
    _ws_hub = hub


def verify_hmac_signature(payload: bytes, signature: str, secret: str) -> bool:
    """
    Verify HMAC-SHA256 signature from Tanda webhook.

    Args:
        payload: Raw request body bytes
        signature: Signature from X-Tanda-Signature header
        secret: Webhook secret from Tanda API

    Returns:
        True if signature is valid
    """
    if not secret or not signature:
        return False

    # Compute HMAC-SHA256
    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()

    # Constant-time comparison
    return hmac.compare_digest(expected, signature)


def is_webhook_duplicate(webhook_id: str, raw_payload: bytes, event_type: str) -> bool:
    """
    Check if a webhook has already been processed (database-backed idempotency).

    Args:
        webhook_id: Unique ID from webhook payload
        raw_payload: Raw request body for payload hash
        event_type: Event type string

    Returns:
        True if webhook is duplicate, False if it's new
    """
    db = get_db()
    if db is None:
        logger.warning("Database unavailable — cannot track webhook idempotency")
        return False

    # Check if webhook has been processed
    if db.is_webhook_processed(webhook_id):
        return True

    # Record the webhook event
    payload_hash = hashlib.sha256(raw_payload).hexdigest()
    try:
        db.save_webhook_event(webhook_id, event_type, payload_hash)
    except Exception as e:
        logger.error(f"Failed to save webhook event: {e}")

    return False


async def process_webhook_event(
    payload_dict: Dict[str, Any],
    raw_payload: bytes,
    webhook_signature: Optional[str],
    webhook_secret: Optional[str],
) -> Dict[str, Any]:
    """
    Process a webhook event in the background.

    Args:
        payload_dict: Parsed webhook payload
        raw_payload: Raw request body bytes
        webhook_signature: HMAC signature from header
        webhook_secret: Webhook secret for verification

    Returns:
        Processing result
    """
    try:
        from rosteriq.services.tanda_webhooks import (
            TandaEventType,
            WebhookPayload,
            get_registry,
        )
    except ImportError:
        logger.error("Failed to import webhook services")
        raise HTTPException(status_code=503, detail="Webhook services not available")

    # Verify signature. Fail CLOSED: when a webhook secret is configured, a
    # valid signature is REQUIRED — a missing signature is rejected, not
    # silently accepted (the previous `secret and signature` guard let
    # unsigned payloads through whenever the signature header was absent).
    if webhook_secret:
        if not webhook_signature:
            logger.warning("Webhook rejected: signature required but missing")
            raise HTTPException(status_code=401, detail="Missing webhook signature")
        if not verify_hmac_signature(raw_payload, webhook_signature, webhook_secret):
            logger.warning("Invalid webhook signature")
            raise HTTPException(status_code=401, detail="Invalid signature")
    else:
        # No secret configured. In PRODUCTION this is unsafe (any caller could
        # forge events), so fail CLOSED and reject. Outside production we keep
        # the warn-and-accept dev behaviour for local/testing convenience.
        if os.environ.get("ENVIRONMENT") == "production":
            logger.error(
                "TANDA_WEBHOOK_SECRET not set in production — rejecting inbound "
                "webhook (cannot verify signature)."
            )
            raise HTTPException(
                status_code=503,
                detail="webhook verification not configured",
            )
        logger.warning(
            "TANDA_WEBHOOK_SECRET not set — inbound webhook signatures are NOT "
            "verified. Set it in production to prevent forged webhook events."
        )

    # Extract payload
    event_type_str = payload_dict.get("event_type")
    webhook_id = payload_dict.get("id", str(uuid4()))
    timestamp_str = payload_dict.get("timestamp", datetime.utcnow().isoformat())
    venue_id = payload_dict.get("venue_id")
    data = payload_dict.get("data", {})
    user_id = payload_dict.get("user_id")

    # Check idempotency (database-backed)
    if is_webhook_duplicate(webhook_id, raw_payload, event_type_str):
        logger.info(f"Duplicate webhook {webhook_id}, skipping")
        return {"status": "duplicate", "webhook_id": webhook_id}

    # Parse event type
    try:
        event_type = TandaEventType(event_type_str)
    except ValueError:
        logger.warning(f"Unknown event type: {event_type_str}")
        raise HTTPException(status_code=400, detail=f"Unknown event type: {event_type_str}")

    # Create webhook payload
    try:
        timestamp = datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        timestamp = datetime.utcnow()

    # Replay protection: reject events whose timestamp is outside a freshness
    # window. Combined with enforced signatures (the signature covers the
    # timestamp), this stops a captured signed payload being replayed later.
    MAX_WEBHOOK_AGE_SECONDS = 300
    now_utc = datetime.now(timezone.utc)
    ts_utc = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    age_seconds = abs((now_utc - ts_utc).total_seconds())
    if age_seconds > MAX_WEBHOOK_AGE_SECONDS:
        logger.warning(f"Webhook {webhook_id} rejected: timestamp {age_seconds:.0f}s outside window")
        raise HTTPException(status_code=400, detail="Webhook timestamp outside acceptable window")

    payload = WebhookPayload(
        event_type=event_type,
        webhook_id=webhook_id,
        timestamp=timestamp,
        venue_id=venue_id,
        data=data,
        user_id=user_id,
    )

    # Dispatch to handlers
    registry = get_registry()
    result = await registry.dispatch(payload, _ws_hub)

    # Record event in webhook manager
    try:
        from rosteriq.services.tanda_webhook_manager import get_webhook_manager
        manager = get_webhook_manager()
        manager.record_event_received(venue_id, event_type_str)
    except Exception as e:
        logger.error(f"Failed to record event: {e}")

    return {
        "status": "processed",
        "webhook_id": webhook_id,
        "event_type": event_type_str,
        "handlers": result,
    }


@router.post("/tanda")
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """
    Receive Tanda webhook event.

    Returns 200 immediately, processes asynchronously via BackgroundTasks.

    Expected headers:
    - X-Tanda-Signature: HMAC-SHA256 signature (optional but recommended)

    Expected body:
    {
        "id": "webhook-id",
        "event_type": "roster.published",
        "timestamp": "2026-04-23T12:34:56Z",
        "venue_id": "venue-123",
        "user_id": "user-456",
        "data": { ... event-specific data ... }
    }
    """
    if not (APIRouter and Request):
        raise HTTPException(status_code=503, detail="FastAPI not available")

    # Get raw body for signature verification
    raw_payload = await request.body()

    # Parse JSON
    try:
        payload_dict = json.loads(raw_payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    # Get signature and secret
    webhook_signature = request.headers.get("x-tanda-signature")
    webhook_secret = os.environ.get("TANDA_WEBHOOK_SECRET")

    # Fail CLOSED in production: without a configured secret we cannot verify
    # the signature, so an unconfigured secret must reject inbound webhooks
    # (rather than letting forgeable, unsigned payloads through). Processing
    # happens in a background task that returns 200 to the client, so the
    # rejection has to be enforced here on the request path.
    if not webhook_secret and os.environ.get("ENVIRONMENT") == "production":
        logger.error(
            "TANDA_WEBHOOK_SECRET not set in production — rejecting inbound webhook."
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="webhook verification not configured",
        )

    # Reject unsigned requests if secret is configured
    if webhook_secret and not webhook_signature:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing webhook signature",
        )

    # Queue processing in background
    background_tasks.add_task(
        process_webhook_event,
        payload_dict=payload_dict,
        raw_payload=raw_payload,
        webhook_signature=webhook_signature,
        webhook_secret=webhook_secret,
    )

    # Return 200 immediately
    return {
        "status": "received",
        "webhook_id": payload_dict.get("id"),
        "event_type": payload_dict.get("event_type"),
    }


@router.get("/status")
async def webhook_status() -> Dict[str, Any]:
    """
    Get webhook subscription status.

    Returns:
        List of all webhook subscriptions and their status
    """
    try:
        from rosteriq.services.tanda_webhook_manager import get_webhook_manager
    except ImportError:
        raise HTTPException(status_code=503, detail="Webhook manager not available")

    manager = get_webhook_manager()
    subscriptions = manager.get_all_subscriptions()

    return {
        "status": "ok",
        "total_subscriptions": len(subscriptions),
        "subscriptions": [sub.to_dict() for sub in subscriptions],
    }


@router.post("/register")
async def register_webhook(
    venue_id: str,
    event_type: str,
    endpoint_url: str,
) -> Dict[str, Any]:
    """
    Register a new webhook subscription with Tanda.

    Query parameters:
    - venue_id: Tanda venue ID
    - event_type: Event type (e.g. 'roster.published')
    - endpoint_url: Full endpoint URL where Tanda should POST

    Returns:
        WebhookSubscription details
    """
    try:
        from rosteriq.services.tanda_webhook_manager import get_webhook_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook manager not available",
        )

    manager = get_webhook_manager()

    try:
        subscription = await manager.register_webhook(
            venue_id=venue_id,
            event_type=event_type,
            endpoint_url=endpoint_url,
        )
        return {
            "status": "registered",
            "subscription": subscription.to_dict(),
        }
    except Exception as e:
        logger.error(f"Failed to register webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.post("/deregister")
async def deregister_webhook(
    subscription_id: str,
    venue_id: str,
    event_type: str,
) -> Dict[str, Any]:
    """
    Deregister a webhook subscription with Tanda.

    Query parameters:
    - subscription_id: Subscription ID to deregister
    - venue_id: Tanda venue ID (for verification)
    - event_type: Event type (for verification)

    Returns:
        Success confirmation
    """
    try:
        from rosteriq.services.tanda_webhook_manager import get_webhook_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook manager not available",
        )

    manager = get_webhook_manager()

    try:
        success = await manager.deregister_webhook(
            subscription_id=subscription_id,
            venue_id=venue_id,
            event_type=event_type,
        )
        return {
            "status": "deregistered",
            "subscription_id": subscription_id,
            "success": success,
        }
    except Exception as e:
        logger.error(f"Failed to deregister webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("/test")
async def test_webhook() -> Dict[str, Any]:
    """
    Test endpoint to verify webhook receiver is working.

    Returns:
        Test response
    """
    return {
        "status": "ok",
        "message": "Webhook receiver is running",
        "timestamp": datetime.utcnow().isoformat(),
    }
