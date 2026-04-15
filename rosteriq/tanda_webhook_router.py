"""FastAPI router for Tanda webhooks.

Endpoints:
- POST /api/v1/tanda/webhook — receive webhook, verify signature, dispatch
- GET /api/v1/tanda/webhook/events — list recent events for org
- POST /api/v1/tanda/webhook/replay — re-dispatch stored event for testing
"""
import logging
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request

from rosteriq.tanda_webhooks import (
    dispatch,
    get_webhook_store,
    parse_tanda_event,
    verify_tanda_signature,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/tanda/webhook", tags=["tanda-webhook"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tanda_event_to_dict(event) -> dict:
    """Convert TandaWebhookEvent to JSON-safe dict."""
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "org_id": event.org_id,
        "occurred_at": event.occurred_at.isoformat(),
        "data": event.data,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/")
async def receive_webhook(request: Request) -> dict:
    """Receive and process a Tanda webhook.

    Verifies X-Tanda-Signature header using HMAC-SHA256.
    If TANDA_WEBHOOK_SECRET not set, accepts in demo mode with WARNING log.
    Rejects requests > 32KB.

    Body: JSON webhook payload
    {
        "event_type": str,
        "org_id" or "organisation_id": str,
        "occurred_at": ISO datetime (optional),
        "data": {...} or other shape
    }

    Returns 200 and dispatches on valid signature.
    Returns 403 on invalid signature.
    Returns 400 on invalid payload.
    """
    # Body size limit: 32KB
    body_bytes = await request.body()
    if len(body_bytes) > 32 * 1024:
        raise HTTPException(status_code=400, detail="Request body too large (max 32KB)")

    # Parse JSON
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    # Extract signature header
    signature_header = request.headers.get("X-Tanda-Signature", "")
    secret = os.getenv("TANDA_WEBHOOK_SECRET", "")

    # Verify signature
    if secret:
        # Production: verify signature
        if not verify_tanda_signature(signature_header, body_bytes, secret):
            logger.warning(
                f"Invalid Tanda signature: {signature_header[:20]}... "
                f"(payload size {len(body_bytes)} bytes)"
            )
            raise HTTPException(status_code=403, detail="Invalid signature")
    else:
        # Demo mode: accept without verification but warn
        logger.warning(
            "TANDA_WEBHOOK_SECRET not set; accepting webhook without signature "
            "verification (demo mode)"
        )

    # Parse event
    try:
        event = parse_tanda_event(payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid event: {str(e)}")

    # Dispatch (stores event automatically)
    await dispatch(event)

    return {
        "received": True,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "org_id": event.org_id,
    }


@router.get("/events")
async def list_events(org_id: str, limit: int = 50) -> dict:
    """List recent webhook events for an organization.

    Query params:
    - org_id: Organization ID (required)
    - limit: Max events to return (default 50)

    Returns list of events newest first.
    """
    if not org_id:
        raise HTTPException(status_code=400, detail="org_id is required")

    if limit < 1 or limit > 500:
        raise HTTPException(
            status_code=400, detail="limit must be between 1 and 500"
        )

    store = get_webhook_store()
    events = store.list_for_org(org_id, limit=limit)

    return {
        "org_id": org_id,
        "count": len(events),
        "events": [_tanda_event_to_dict(e) for e in events],
    }


@router.post("/replay")
async def replay_event(body: dict) -> dict:
    """Re-dispatch a stored webhook event for testing/debugging.

    Body:
    {
        "event_id": str
    }

    Returns error if event not found.
    """
    event_id = body.get("event_id", "").strip()
    if not event_id:
        raise HTTPException(status_code=400, detail="event_id is required")

    store = get_webhook_store()
    event = store.get_event(event_id)

    if not event:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")

    # Re-dispatch
    logger.info(f"Replaying event {event_id}")
    await dispatch(event)

    return {
        "replayed": True,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "org_id": event.org_id,
    }
