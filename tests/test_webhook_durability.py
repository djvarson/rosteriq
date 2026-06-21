"""
Tests for durable outbound webhook delivery.

Verifies that OutboundWebhookService.deliver_webhook now ENQUEUES into the
persistent WebhookRetryQueue (so deliveries survive a restart) instead of
retrying in-process, that the full delivery (url/payload/headers) is persisted,
that the HMAC signature stays valid through the queue's re-serialisation, and
that the queue delivers, retries, and dead-letters correctly.
"""

import hashlib
import hmac
import json

import httpx
import pytest
import respx

import rosteriq.services.webhook_queue as wq_module
from rosteriq.database import get_db, reset_db
from rosteriq.services.outbound_webhooks import OutboundWebhookService
from rosteriq.services.webhook_queue import WebhookRetryQueue, MAX_ATTEMPTS


SECRET = "whsec_test_123"
CALLBACK = "https://example.test/webhook"


def _make_service():
    # Use the global store so the service and the queue singleton (which
    # deliver_webhook resolves via get_webhook_queue → get_db) share one db,
    # mirroring production where everything resolves to get_db().
    reset_db()
    wq_module._queue = None  # reset queue singleton to bind to the fresh db
    db = get_db()
    svc = OutboundWebhookService(db=db)
    sub_id = svc.register_subscription(
        venue_id="venue-1",
        callback_url=CALLBACK,
        events=["roster.published"],
        secret=SECRET,
    )
    return db, svc, sub_id


async def test_deliver_webhook_enqueues_full_delivery():
    """deliver_webhook persists a pending delivery with url/payload/headers."""
    db, svc, sub_id = _make_service()
    sub = db.list_webhook_subscriptions("venue-1")[0]

    await svc.deliver_webhook(sub, "roster.published", {"date": "2026-06-15"})

    # Exactly one pending delivery is queued, carrying everything needed to send.
    pending = list(db._webhook_retry_queue.values())
    assert len(pending) == 1
    d = pending[0]
    assert d["status"] == "pending"
    assert d["url"] == CALLBACK
    assert d["payload"]["event_type"] == "roster.published"
    assert d["payload"]["data"] == {"date": "2026-06-15"}
    assert d["venue_id"] == "venue-1"
    assert d["attempt"] == 0
    # First attempt is eligible immediately (no backoff on attempt 0).
    assert d["next_retry_at"] is not None


async def test_enqueued_signature_is_valid_over_payload():
    """The X-RosterIQ-Signature header matches an HMAC of the queued payload,
    serialised the same way the queue will serialise it before sending."""
    db, svc, sub_id = _make_service()
    sub = db.list_webhook_subscriptions("venue-1")[0]

    await svc.deliver_webhook(sub, "roster.published", {"n": 1})

    d = list(db._webhook_retry_queue.values())[0]
    sig_header = d["headers"]["X-RosterIQ-Signature"]
    # Recompute over the queued payload using the queue's exact serialisation.
    body_bytes = json.dumps(d["payload"], default=str).encode()
    expected = "sha256=" + hmac.new(SECRET.encode(), body_bytes, hashlib.sha256).hexdigest()
    assert sig_header == expected


@respx.mock
async def test_queue_delivers_enqueued_webhook_end_to_end():
    """Enqueue via deliver_webhook, then the queue processor delivers it (2xx)."""
    route = respx.post(CALLBACK).mock(return_value=httpx.Response(200))
    db, svc, sub_id = _make_service()
    sub = db.list_webhook_subscriptions("venue-1")[0]
    await svc.deliver_webhook(sub, "roster.published", {"ok": True})

    queue = WebhookRetryQueue(db=db)
    delivery = await queue.get_next_retry()
    assert delivery is not None
    await queue._attempt_delivery(delivery)

    assert route.called
    # Receiver got the signed body.
    sent = route.calls.last.request
    assert sent.headers["X-RosterIQ-Event"] == "roster.published"
    assert "sha256=" in sent.headers["X-RosterIQ-Signature"]
    # Delivery marked success and no longer pending.
    stored = db.get_webhook_delivery(delivery["id"])
    assert stored["status"] == "success"
    assert not db.list_pending_retries(__import__("datetime").datetime.now(__import__("datetime").timezone.utc))


@respx.mock
async def test_failed_delivery_retries_then_dead_letters():
    """A persistently-failing endpoint exhausts retries and dead-letters."""
    respx.post(CALLBACK).mock(return_value=httpx.Response(500))
    db, svc, sub_id = _make_service()
    sub = db.list_webhook_subscriptions("venue-1")[0]
    await svc.deliver_webhook(sub, "roster.published", {"x": 1})

    queue = WebhookRetryQueue(db=db)
    delivery = db.get_webhook_delivery(list(db._webhook_retry_queue.values())[0]["id"])

    # Drive each attempt manually (bypassing real backoff sleeps).
    for _ in range(MAX_ATTEMPTS):
        await queue._attempt_delivery(delivery)
        delivery = db.get_webhook_delivery(delivery["id"])

    assert delivery["status"] == "dead_letter"
    dead = await queue.list_dead_letters(venue_id="venue-1")
    assert len(dead) == 1
    # The dead letter retains the URL + payload so it can be replayed.
    assert dead[0]["url"] == CALLBACK
    assert dead[0]["payload"]["data"] == {"x": 1}
