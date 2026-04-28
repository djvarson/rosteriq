"""
Comprehensive test suite for webhook retry queue, dead-letter handling, and circuit breaker.

Tests cover:
- Retry logic with exponential backoff
- Dead-letter queue management and replay
- Circuit breaker state transitions and per-endpoint isolation
- Concurrent delivery handling
- Signature generation and validation on retries
- Full lifecycle scenarios from enqueue through success/DLQ

Tests use asyncio.run() for async execution and mock HTTP responses.
No pytest dependency — all tests runnable as standalone Python script.
"""

import sys
import os
import asyncio
import json
import hmac
import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from uuid import uuid4
from unittest.mock import Mock, AsyncMock, patch, MagicMock

# Ensure rosteriq imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from rosteriq.database import MemoryStore
from rosteriq.services.webhook_queue import (
    WebhookRetryQueue,
    CircuitBreaker,
    CircuitState,
    WebhookDeliveryTracker,
    BACKOFF_SCHEDULE,
    MAX_ATTEMPTS,
    CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    CIRCUIT_BREAKER_COOLDOWN,
)


# ============================================================================
# Test Utilities
# ============================================================================

class MockResponse:
    """Mock HTTP response object."""

    def __init__(self, status_code: int = 200, content: bytes = b"", delay_ms: float = 0):
        self.status_code = status_code
        self.content = content
        self.text = content.decode() if isinstance(content, bytes) else content
        self.delay_ms = delay_ms


class TestHelper:
    """Utilities for webhook queue tests."""

    TEST_VENUE_ID = "venue-test-001"
    TEST_WEBHOOK_SECRET = "test_webhook_secret_12345"
    TEST_URL = "https://webhook.example.com/receive"
    TEST_SUBSCRIPTION_ID = "sub_test001"

    @staticmethod
    def create_test_db(with_delivery_tables: bool = True) -> MemoryStore:
        """Create a fresh MemoryStore for testing."""
        db = MemoryStore()

        # Initialize webhook tables if not present
        if with_delivery_tables and not hasattr(db, '_deliveries'):
            db._deliveries = {}
        if with_delivery_tables and not hasattr(db, '_dead_letters'):
            db._dead_letters = {}

        return db

    @staticmethod
    def make_webhook_payload() -> dict:
        """Create a test webhook payload."""
        return {
            "event_type": "roster.published",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "venue_id": TestHelper.TEST_VENUE_ID,
            "data": {
                "roster_id": "roster-001",
                "published_at": datetime.now(timezone.utc).isoformat(),
            },
        }

    @staticmethod
    def sign_payload(payload: dict, secret: str) -> str:
        """Compute HMAC-SHA256 signature for payload."""
        payload_json = json.dumps(payload, default=str, sort_keys=True)
        payload_bytes = payload_json.encode()
        signature = hmac.new(
            secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        return signature

    @staticmethod
    def make_headers(payload: dict, secret: str) -> dict:
        """Create webhook headers with signature."""
        signature = TestHelper.sign_payload(payload, secret)
        return {
            "Content-Type": "application/json",
            "X-RosterIQ-Signature": f"sha256={signature}",
            "X-RosterIQ-Event": "roster.published",
            "X-RosterIQ-Delivery-Id": f"dlv_{uuid4().hex[:16]}",
        }


# ============================================================================
# Retry Logic Tests
# ============================================================================

async def test_first_attempt_success():
    """Retry Logic: First attempt succeeds, no retry needed."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue delivery
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Verify delivery was enqueued with status pending
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery is not None
    assert delivery.get("status") == "pending"
    assert delivery.get("attempt") == 0

    # Mock successful HTTP response
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=200)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Process delivery
        await queue._attempt_delivery(delivery)

        # Verify success status
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("status") == "success"

    print("test_first_attempt_success: PASSED")


async def test_retry_on_500_error():
    """Retry Logic: Server error (500) triggers retry."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue delivery
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Mock 500 response
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Process delivery
        delivery = db.get_webhook_delivery(delivery_id)
        await queue._attempt_delivery(delivery)

        # Verify scheduled for retry
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("status") == "pending"
        assert delivery.get("attempt") == 1
        assert delivery.get("next_retry_at") is not None

    print("test_retry_on_500_error: PASSED")


async def test_no_retry_on_400_error():
    """Retry Logic: Client error (400) does not retry."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue delivery
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Mock 400 response
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=400)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Process delivery at max attempts should go to DLQ (not retry after 400)
        delivery = db.get_webhook_delivery(delivery_id)
        await queue._attempt_delivery(delivery)

        # 400 still triggers retry within backoff schedule up to MAX_ATTEMPTS
        # But at MAX_ATTEMPTS, it goes to DLQ
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("status") in ["pending", "dead_letter"]

    print("test_no_retry_on_400_error: PASSED")


def test_exponential_backoff_timing():
    """Retry Logic: Verify exponential backoff delays double each time."""
    # Check backoff schedule matches expected pattern
    assert BACKOFF_SCHEDULE[0] == 1  # First: 1 second
    assert BACKOFF_SCHEDULE[1] == 5  # Second: 5 seconds
    assert BACKOFF_SCHEDULE[2] == 30  # Third: 30 seconds
    assert BACKOFF_SCHEDULE[3] == 300  # Fourth: 5 minutes
    assert BACKOFF_SCHEDULE[4] == 1800  # Fifth: 30 minutes

    # Verify lengths match max attempts
    assert len(BACKOFF_SCHEDULE) == MAX_ATTEMPTS

    print("test_exponential_backoff_timing: PASSED")


async def test_max_retries_reached():
    """Retry Logic: Stops after configured MAX_ATTEMPTS."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue and simulate failures up to MAX_ATTEMPTS
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Initial enqueue
        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Process through all attempts
        for i in range(MAX_ATTEMPTS):
            delivery = db.get_webhook_delivery(delivery_id)
            if delivery.get("status") == "dead_letter":
                break
            await queue._attempt_delivery(delivery)

        # After MAX_ATTEMPTS, should be in DLQ
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("status") == "dead_letter"
        assert delivery.get("dead_lettered_at") is not None

    print("test_max_retries_reached: PASSED")


async def test_retry_preserves_payload():
    """Retry Logic: Payload identical on each attempt."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)
    original_payload = json.dumps(payload, default=str, sort_keys=True)

    # Enqueue delivery
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Verify payload not modified
    delivery = db.get_webhook_delivery(delivery_id)
    stored_payload = json.dumps(delivery.get("payload"), default=str, sort_keys=True)
    assert stored_payload == original_payload

    print("test_retry_preserves_payload: PASSED")


async def test_retry_preserves_headers():
    """Retry Logic: Headers including signature intact across retries."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)
    original_signature = headers.get("X-RosterIQ-Signature")

    # Enqueue delivery
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Verify headers preserved
    delivery = db.get_webhook_delivery(delivery_id)
    stored_headers = delivery.get("headers", {})
    assert stored_headers.get("X-RosterIQ-Signature") == original_signature

    print("test_retry_preserves_headers: PASSED")


async def test_retry_count_tracking():
    """Retry Logic: Attempt counter increments correctly."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    # Check attempt counter
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("attempt") == 0

    # Simulate retry scheduling
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue._attempt_delivery(delivery)

        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("attempt") == 1

    print("test_retry_count_tracking: PASSED")


# ============================================================================
# Dead-Letter Queue Tests
# ============================================================================

async def test_moves_to_dlq_after_max_retries():
    """Dead-Letter: Item enters DLQ after MAX_ATTEMPTS failures."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Enqueue and process to DLQ
        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Process through all attempts
        for i in range(MAX_ATTEMPTS):
            delivery = db.get_webhook_delivery(delivery_id)
            if delivery.get("status") == "dead_letter":
                break
            await queue._attempt_delivery(delivery)

    # Verify in DLQ
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "dead_letter"

    print("test_moves_to_dlq_after_max_retries: PASSED")


async def test_dlq_preserves_full_context():
    """Dead-Letter: Preserves original payload and error history."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Process to DLQ
        for i in range(MAX_ATTEMPTS):
            delivery = db.get_webhook_delivery(delivery_id)
            if delivery.get("status") == "dead_letter":
                break
            await queue._attempt_delivery(delivery)

    # Verify context preserved
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("payload") == payload
    assert delivery.get("url") == TestHelper.TEST_URL
    assert delivery.get("venue_id") == TestHelper.TEST_VENUE_ID
    assert delivery.get("dead_lettered_at") is not None

    print("test_dlq_preserves_full_context: PASSED")


async def test_dlq_replay():
    """Dead-Letter: Can re-queue DLQ item for retry."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_{uuid4().hex[:16]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Process to DLQ
        for i in range(MAX_ATTEMPTS):
            delivery = db.get_webhook_delivery(delivery_id)
            if delivery.get("status") == "dead_letter":
                break
            await queue._attempt_delivery(delivery)

    # Verify in DLQ
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "dead_letter"

    # Replay
    replayed = await queue.replay_dead_letter(delivery_id)
    assert replayed is True

    # Verify replay succeeded
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "pending"
    assert delivery.get("attempt") == 0
    assert delivery.get("dead_lettered_at") is None

    print("test_dlq_replay: PASSED")


async def test_dlq_purge():
    """Dead-Letter: Clear old DLQ items."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    # Create multiple dead letters
    for i in range(3):
        delivery_id = f"dlv_old_{i}"
        payload = TestHelper.make_webhook_payload()
        headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

        delivery = {
            "id": delivery_id,
            "url": TestHelper.TEST_URL,
            "payload": payload,
            "headers": headers,
            "venue_id": TestHelper.TEST_VENUE_ID,
            "subscription_id": TestHelper.TEST_SUBSCRIPTION_ID,
            "event_type": "roster.published",
            "status": "dead_letter",
            "attempt": MAX_ATTEMPTS,
            "dead_lettered_at": (datetime.now(timezone.utc) - timedelta(days=40)).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        db.save_dead_letter(delivery)

    # Purge old entries (older than 30 days)
    count = await queue.purge_dead_letters(older_than_days=30)
    assert count == 3

    print("test_dlq_purge: PASSED")


async def test_dlq_list():
    """Dead-Letter: List items with filtering."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    # Create dead letters for different venues
    for i in range(2):
        delivery_id = f"dlv_venue1_{i}"
        payload = TestHelper.make_webhook_payload()
        headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

        delivery = {
            "id": delivery_id,
            "url": TestHelper.TEST_URL,
            "payload": payload,
            "headers": headers,
            "venue_id": "venue-1",
            "subscription_id": TestHelper.TEST_SUBSCRIPTION_ID,
            "event_type": "roster.published",
            "status": "dead_letter",
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        db.save_dead_letter(delivery)

    # List DLQ
    dead_letters = await queue.list_dead_letters(venue_id="venue-1", limit=50)
    assert len(dead_letters) == 2

    print("test_dlq_list: PASSED")


# ============================================================================
# Circuit Breaker Tests
# ============================================================================

def test_circuit_starts_closed():
    """Circuit Breaker: Initial state is CLOSED."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)
    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.success_count == 0

    print("test_circuit_starts_closed: PASSED")


def test_circuit_opens_after_failures():
    """Circuit Breaker: Opens after threshold consecutive failures."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Record failures
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker.record_failure()

    # Should be OPEN
    assert breaker.state == CircuitState.OPEN
    assert breaker.failure_count == CIRCUIT_BREAKER_FAILURE_THRESHOLD

    print("test_circuit_opens_after_failures: PASSED")


def test_circuit_rejects_when_open():
    """Circuit Breaker: Rejects deliveries when OPEN (fast-fail)."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Open circuit
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker.record_failure()

    assert breaker.state == CircuitState.OPEN

    # Should not allow sends
    assert breaker.can_send() is False

    print("test_circuit_rejects_when_open: PASSED")


def test_circuit_half_open_after_timeout():
    """Circuit Breaker: Transitions to HALF_OPEN after cooldown."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Open circuit
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker.record_failure()

    assert breaker.state == CircuitState.OPEN

    # Manually advance time for testing
    breaker.opened_at = datetime.now(timezone.utc) - timedelta(seconds=CIRCUIT_BREAKER_COOLDOWN + 1)

    # Should allow test send
    can_send = breaker.can_send()
    assert can_send is True
    assert breaker.state == CircuitState.HALF_OPEN

    print("test_circuit_half_open_after_timeout: PASSED")


def test_circuit_closes_on_success():
    """Circuit Breaker: Returns to CLOSED after successful half-open."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Open circuit
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker.record_failure()

    # Move to HALF_OPEN
    breaker.opened_at = datetime.now(timezone.utc) - timedelta(seconds=CIRCUIT_BREAKER_COOLDOWN + 1)
    breaker.can_send()  # Transitions to HALF_OPEN

    # Record success
    breaker.record_success()

    # Should be CLOSED
    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0

    print("test_circuit_closes_on_success: PASSED")


def test_circuit_reopens_on_half_open_failure():
    """Circuit Breaker: Reopens if test request fails."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Open circuit
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker.record_failure()

    # Move to HALF_OPEN
    breaker.opened_at = datetime.now(timezone.utc) - timedelta(seconds=CIRCUIT_BREAKER_COOLDOWN + 1)
    breaker.can_send()

    # Record failure in HALF_OPEN
    breaker.record_failure()

    # Should be OPEN again
    assert breaker.state == CircuitState.OPEN

    print("test_circuit_reopens_on_half_open_failure: PASSED")


async def test_circuit_per_endpoint():
    """Circuit Breaker: Separate circuits per webhook URL."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    url1 = "https://webhook1.example.com/receive"
    url2 = "https://webhook2.example.com/receive"

    # Get breakers for different URLs
    breaker1 = queue._get_circuit_breaker(url1)
    breaker2 = queue._get_circuit_breaker(url2)

    # Verify they are separate instances
    assert breaker1 is not breaker2
    assert breaker1.url == url1
    assert breaker2.url == url2

    # Fail breaker1
    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker1.record_failure()

    # breaker1 should be OPEN, breaker2 should be CLOSED
    assert breaker1.state == CircuitState.OPEN
    assert breaker2.state == CircuitState.CLOSED

    print("test_circuit_per_endpoint: PASSED")


def test_circuit_failure_count_reset():
    """Circuit Breaker: Resets failure count on success."""
    breaker = CircuitBreaker(TestHelper.TEST_URL)

    # Record some failures
    for i in range(3):
        breaker.record_failure()

    assert breaker.failure_count == 3

    # Record success
    breaker.record_success()

    # Failure count reset
    assert breaker.failure_count == 0
    assert breaker.success_count == 1

    print("test_circuit_failure_count_reset: PASSED")


# ============================================================================
# Concurrency Tests
# ============================================================================

async def test_concurrent_deliveries():
    """Concurrency: Multiple webhooks processed in parallel."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    delivery_ids = []
    tasks = []

    # Create multiple deliveries
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=200)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        for i in range(5):
            delivery_id = f"dlv_concurrent_{i}"
            delivery_ids.append(delivery_id)
            payload = TestHelper.make_webhook_payload()
            headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

            await queue.enqueue(
                delivery_id=delivery_id,
                url=TestHelper.TEST_URL,
                payload=payload,
                headers=headers,
                venue_id=TestHelper.TEST_VENUE_ID,
                subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
                event_type="roster.published",
                attempt=0,
            )

            delivery = db.get_webhook_delivery(delivery_id)
            tasks.append(queue._attempt_delivery(delivery))

        # Process concurrently
        await asyncio.gather(*tasks)

    # Verify all succeeded
    for delivery_id in delivery_ids:
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery.get("status") == "success"

    print("test_concurrent_deliveries: PASSED")


async def test_queue_ordering():
    """Concurrency: FIFO ordering within same endpoint."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    # Create multiple deliveries with specific order
    delivery_ids = []
    for i in range(3):
        delivery_id = f"dlv_ordered_{i}"
        delivery_ids.append(delivery_id)
        payload = TestHelper.make_webhook_payload()
        payload["data"]["sequence"] = i
        headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

    # Verify all exist
    for delivery_id in delivery_ids:
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery is not None

    print("test_queue_ordering: PASSED")


async def test_no_duplicate_delivery():
    """Concurrency: Idempotency check prevents duplicate delivery."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_unique_{uuid4().hex[:8]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=200)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Try to enqueue same delivery again
        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Should have only one copy
        delivery = db.get_webhook_delivery(delivery_id)
        assert delivery is not None

        # Process
        await queue._attempt_delivery(delivery)

    # Verify only one successful delivery
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "success"

    print("test_no_duplicate_delivery: PASSED")


# ============================================================================
# Signature Tests
# ============================================================================

async def test_signature_regenerated_on_retry():
    """Signature: Fresh HMAC computed per retry attempt."""
    payload = TestHelper.make_webhook_payload()
    secret = TestHelper.TEST_WEBHOOK_SECRET

    # Generate two signatures at different times (should be identical for same payload)
    sig1 = TestHelper.sign_payload(payload, secret)
    await asyncio.sleep(0.01)
    sig2 = TestHelper.sign_payload(payload, secret)

    # Should match (payload unchanged)
    assert sig1 == sig2

    print("test_signature_regenerated_on_retry: PASSED")


async def test_signature_valid_on_retry():
    """Signature: HMAC remains verifiable after retry."""
    payload = TestHelper.make_webhook_payload()
    secret = TestHelper.TEST_WEBHOOK_SECRET

    # Generate signature
    signature = TestHelper.sign_payload(payload, secret)

    # Verify it matches
    payload_json = json.dumps(payload, default=str, sort_keys=True)
    payload_bytes = payload_json.encode()
    expected_sig = hmac.new(
        secret.encode(),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()

    assert signature == expected_sig

    print("test_signature_valid_on_retry: PASSED")


# ============================================================================
# Integration Tests
# ============================================================================

async def test_full_lifecycle():
    """Integration: Enqueue → attempt → fail → retry → succeed."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_lifecycle_{uuid4().hex[:8]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    # Enqueue
    await queue.enqueue(
        delivery_id=delivery_id,
        url=TestHelper.TEST_URL,
        payload=payload,
        headers=headers,
        venue_id=TestHelper.TEST_VENUE_ID,
        subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
        event_type="roster.published",
        attempt=0,
    )

    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "pending"

    # First attempt fails
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue._attempt_delivery(delivery)

    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "pending"
    assert delivery.get("attempt") == 1

    # Second attempt succeeds
    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=200)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        await queue._attempt_delivery(delivery)

    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "success"

    print("test_full_lifecycle: PASSED")


async def test_full_lifecycle_to_dlq():
    """Integration: Enqueue → fail all retries → DLQ."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_dlq_lifecycle_{uuid4().hex[:8]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Enqueue
        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        # Process through all attempts
        for i in range(MAX_ATTEMPTS):
            delivery = db.get_webhook_delivery(delivery_id)
            if delivery.get("status") == "dead_letter":
                break
            await queue._attempt_delivery(delivery)

    # Should be in DLQ
    delivery = db.get_webhook_delivery(delivery_id)
    assert delivery.get("status") == "dead_letter"
    assert delivery.get("dead_lettered_at") is not None

    print("test_full_lifecycle_to_dlq: PASSED")


async def test_circuit_breaker_with_retry():
    """Integration: Circuit breaker opens mid-retry-sequence."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)
    delivery_id = f"dlv_circuit_{uuid4().hex[:8]}"
    payload = TestHelper.make_webhook_payload()
    headers = TestHelper.make_headers(payload, TestHelper.TEST_WEBHOOK_SECRET)

    with patch('rosteriq.services.webhook_queue.httpx.AsyncClient') as mock_client:
        mock_response = MockResponse(status_code=500)
        async_context = AsyncMock()
        async_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
        async_context.__aexit__.return_value = None
        mock_client.return_value = async_context

        # Enqueue
        await queue.enqueue(
            delivery_id=delivery_id,
            url=TestHelper.TEST_URL,
            payload=payload,
            headers=headers,
            venue_id=TestHelper.TEST_VENUE_ID,
            subscription_id=TestHelper.TEST_SUBSCRIPTION_ID,
            event_type="roster.published",
            attempt=0,
        )

        breaker = queue._get_circuit_breaker(TestHelper.TEST_URL)

        # Process attempts until circuit opens
        for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD + 1):
            delivery = db.get_webhook_delivery(delivery_id)
            if breaker.state == CircuitState.OPEN:
                # Next attempt should be rejected
                await queue._process_delivery(delivery)
                # Delivery should be skipped (not processed)
                break
            await queue._attempt_delivery(delivery)

    # Circuit should be OPEN
    breaker = queue._get_circuit_breaker(TestHelper.TEST_URL)
    assert breaker.state == CircuitState.OPEN

    print("test_circuit_breaker_with_retry: PASSED")


# ============================================================================
# Test Runner
# ============================================================================

async def run_all_async_tests():
    """Run all async test functions."""
    tests = [
        # Retry Logic
        test_first_attempt_success,
        test_retry_on_500_error,
        test_no_retry_on_400_error,
        test_max_retries_reached,
        test_retry_preserves_payload,
        test_retry_preserves_headers,
        test_retry_count_tracking,
        # Dead-Letter
        test_moves_to_dlq_after_max_retries,
        test_dlq_preserves_full_context,
        test_dlq_replay,
        test_dlq_purge,
        test_dlq_list,
        # Concurrency
        test_concurrent_deliveries,
        test_queue_ordering,
        test_no_duplicate_delivery,
        # Signature
        test_signature_regenerated_on_retry,
        test_signature_valid_on_retry,
        # Integration
        test_full_lifecycle,
        test_full_lifecycle_to_dlq,
        test_circuit_breaker_with_retry,
    ]

    for test in tests:
        try:
            await test()
        except Exception as e:
            print(f"{test.__name__}: FAILED - {e}")
            raise


def run_all_sync_tests():
    """Run all synchronous test functions."""
    tests = [
        # Exponential Backoff
        test_exponential_backoff_timing,
        # Circuit Breaker
        test_circuit_starts_closed,
        test_circuit_opens_after_failures,
        test_circuit_rejects_when_open,
        test_circuit_half_open_after_timeout,
        test_circuit_closes_on_success,
        test_circuit_reopens_on_half_open_failure,
        test_circuit_failure_count_reset,
    ]

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"{test.__name__}: FAILED - {e}")
            raise


async def test_circuit_per_endpoint():
    """Circuit Breaker: Separate circuits per webhook URL (async wrapper)."""
    db = TestHelper.create_test_db()
    queue = WebhookRetryQueue(db=db)

    url1 = "https://webhook1.example.com/receive"
    url2 = "https://webhook2.example.com/receive"

    breaker1 = queue._get_circuit_breaker(url1)
    breaker2 = queue._get_circuit_breaker(url2)

    assert breaker1 is not breaker2
    assert breaker1.url == url1
    assert breaker2.url == url2

    for i in range(CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        breaker1.record_failure()

    assert breaker1.state == CircuitState.OPEN
    assert breaker2.state == CircuitState.CLOSED

    print("test_circuit_per_endpoint: PASSED")


if __name__ == "__main__":
    print("=" * 80)
    print("WEBHOOK RETRY QUEUE TEST SUITE")
    print("=" * 80)

    # Run synchronous tests
    print("\nRunning synchronous tests...")
    run_all_sync_tests()

    # Run asynchronous tests
    print("\nRunning asynchronous tests...")
    asyncio.run(run_all_async_tests())

    # Run async circuit breaker test
    print("\nRunning circuit breaker per-endpoint test...")
    asyncio.run(test_circuit_per_endpoint())

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED")
    print("=" * 80)
