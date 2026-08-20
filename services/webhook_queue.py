"""
Persistent webhook retry queue with dead-letter handling and circuit breaker.

Implements:
- Exponential backoff retry strategy with configurable attempts
- Dead-letter queue for permanently failed deliveries
- Circuit breaker per destination URL to prevent cascading failures
- Delivery tracking with attempt logs and response metrics
- Async queue processing with proper error isolation
"""

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, Dict, List
import httpx

from rosteriq.database import get_db

logger = logging.getLogger(__name__)

# Retry configuration
BACKOFF_SCHEDULE = [1, 5, 30, 300, 1800]  # 1s, 5s, 30s, 5min, 30min
MAX_ATTEMPTS = 5
DELIVERY_TIMEOUT = 10
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
CIRCUIT_BREAKER_COOLDOWN = 60  # seconds


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Per-destination-URL circuit breaker to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests allowed
    - OPEN: Too many failures, requests rejected immediately
    - HALF_OPEN: Testing if destination is recovered after cooldown
    """

    def __init__(self, url: str):
        self.url = url
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_at: Optional[datetime] = None
        self.opened_at: Optional[datetime] = None

    def can_send(self) -> bool:
        """Check if a request can be sent to this URL."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if cooldown has elapsed
            if self.opened_at and \
               (datetime.now(timezone.utc) - self.opened_at).total_seconds() >= CIRCUIT_BREAKER_COOLDOWN:
                self.state = CircuitState.HALF_OPEN
                logger.info(f"Circuit breaker for {self.url} transitioning to HALF_OPEN")
                return True
            return False

        # HALF_OPEN: allow one test request
        return True

    def record_success(self) -> None:
        """Record a successful delivery."""
        self.failure_count = 0
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.opened_at = None
            logger.info(f"Circuit breaker for {self.url} returned to CLOSED")

    def record_failure(self) -> None:
        """Record a failed delivery."""
        self.failure_count += 1
        self.last_failure_at = datetime.now(timezone.utc)
        self.success_count = 0

        if self.failure_count >= CIRCUIT_BREAKER_FAILURE_THRESHOLD and \
           self.state != CircuitState.OPEN:
            self.state = CircuitState.OPEN
            self.opened_at = datetime.now(timezone.utc)
            logger.warning(
                f"Circuit breaker for {self.url} opened after "
                f"{self.failure_count} consecutive failures"
            )

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.opened_at = datetime.now(timezone.utc)
            logger.warning(f"Circuit breaker for {self.url} reopened during testing")

    def get_status(self) -> dict:
        """Get current circuit breaker status."""
        cooldown_remaining = 0
        if self.state == CircuitState.OPEN and self.opened_at:
            elapsed = (datetime.now(timezone.utc) - self.opened_at).total_seconds()
            cooldown_remaining = max(0, CIRCUIT_BREAKER_COOLDOWN - elapsed)

        return {
            "url": self.url,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_at": self.last_failure_at.isoformat() if self.last_failure_at else None,
            "cooldown_remaining_seconds": cooldown_remaining,
        }


class WebhookDeliveryTracker:
    """Track delivery attempts and compute statistics."""

    def __init__(self, db=None):
        self.db = db or get_db()

    async def record_attempt(
        self,
        delivery_id: str,
        url: str,
        status_code: Optional[int],
        response_time_ms: float,
        error: Optional[str],
    ) -> None:
        """Record a delivery attempt."""
        delivery = self.db.get_webhook_delivery(delivery_id)
        if not delivery:
            return

        if not delivery.get("attempts"):
            delivery["attempts"] = []

        delivery["attempts"].append({
            "attempt_number": len(delivery["attempts"]) + 1,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            "error": error,
        })

        self.db.save_webhook_delivery(delivery)

    async def get_delivery_log(self, delivery_id: str) -> Optional[dict]:
        """Get delivery attempt log."""
        delivery = self.db.get_webhook_delivery(delivery_id)
        if not delivery:
            return None

        return {
            "id": delivery.get("id"),
            "url": delivery.get("url"),
            "status": delivery.get("status"),
            "attempts": delivery.get("attempts", []),
            "created_at": delivery.get("created_at"),
            "last_attempt_at": delivery.get("last_attempt_at"),
        }

    async def get_stats(self, venue_id: Optional[str] = None) -> dict:
        """Get webhook delivery statistics."""
        # This would typically query all deliveries for a venue
        # For now, return a summary structure
        return {
            "total_deliveries": 0,
            "successful": 0,
            "failed": 0,
            "pending": 0,
            "success_rate": 0.0,
            "avg_response_time_ms": 0.0,
            "failure_breakdown": {
                "timeout": 0,
                "connection_error": 0,
                "http_error": 0,
                "other": 0,
            },
        }


class WebhookRetryQueue:
    """
    Persistent retry queue for webhook deliveries with exponential backoff,
    dead-letter handling, and circuit breaker pattern.
    """

    def __init__(self, db=None, poll_interval_seconds: int = 5):
        self.db = db or get_db()
        self.poll_interval = poll_interval_seconds
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.tracker = WebhookDeliveryTracker(self.db)
        self.processing = False
        self.process_task: Optional[asyncio.Task] = None

    # ========================================================================
    # Queue Management
    # ========================================================================

    async def enqueue(
        self,
        delivery_id: str,
        url: str,
        payload: dict,
        headers: dict,
        venue_id: str,
        subscription_id: str,
        event_type: str,
        attempt: int = 0,
    ) -> None:
        """
        Add a delivery to the retry queue.

        Args:
            delivery_id: Unique delivery ID
            url: Destination URL
            payload: JSON payload to send
            headers: HTTP headers (will include signature)
            venue_id: Venue ID for tracking
            subscription_id: Webhook subscription ID
            event_type: Event type being delivered
            attempt: Current attempt number (0-indexed)
        """
        delivery = {
            "id": delivery_id,
            "url": url,
            "payload": payload,
            "headers": headers,
            "venue_id": venue_id,
            "subscription_id": subscription_id,
            "event_type": event_type,
            "status": "pending",
            "attempt": attempt,
            "attempts": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_attempt_at": None,
            "next_retry_at": None,
            "response_code": None,
            "error": None,
        }

        # Schedule when this delivery becomes eligible to send.
        # The very first attempt (attempt 0) should be sent immediately — backoff
        # only applies to *retries* after a failure. Re-enqueuing at a later attempt
        # (attempt > 0) honours the exponential backoff schedule.
        if attempt < MAX_ATTEMPTS:
            delay_seconds = 0 if attempt == 0 else BACKOFF_SCHEDULE[attempt]
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
            delivery["next_retry_at"] = retry_at.isoformat()

        self.db.save_webhook_delivery(delivery)
        logger.info(
            f"Enqueued webhook delivery {delivery_id} to {url} "
            f"(attempt {attempt + 1}/{MAX_ATTEMPTS})"
        )

    async def get_next_retry(self) -> Optional[dict]:
        """
        Get the next delivery that's ready for retry.

        Returns:
            Delivery dict or None if queue is empty
        """
        now = datetime.now(timezone.utc)
        pending = self.db.list_pending_retries(now)

        if not pending:
            return None

        # Return oldest pending retry
        return pending[0] if pending else None

    async def process_queue(self) -> None:
        """
        Main async loop that processes pending retries.

        Call this as a background task on startup. It will continuously
        poll the queue and attempt deliveries with proper error handling.
        """
        self.processing = True
        logger.info("Starting webhook queue processor")

        try:
            while self.processing:
                try:
                    delivery = await self.get_next_retry()
                    if delivery:
                        await self._process_delivery(delivery)
                    else:
                        # No pending deliveries; sleep briefly
                        await asyncio.sleep(self.poll_interval)

                except Exception as e:
                    logger.error(
                        f"Error processing webhook queue: {e}",
                        exc_info=True,
                    )
                    # Prevent busy loop on persistent errors
                    await asyncio.sleep(self.poll_interval)

        finally:
            logger.info("Webhook queue processor stopped")
            self.processing = False

    async def start(self) -> None:
        """Start the queue processor as a background task."""
        if self.process_task:
            return

        self.process_task = asyncio.create_task(self.process_queue())
        logger.info("Webhook queue processor started")

    async def stop(self) -> None:
        """Stop the queue processor."""
        self.processing = False
        if self.process_task:
            try:
                await asyncio.wait_for(self.process_task, timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Webhook queue processor did not stop gracefully")
                self.process_task.cancel()
            except Exception as e:
                logger.error(f"Error stopping webhook queue: {e}")
            finally:
                self.process_task = None

    # ========================================================================
    # Delivery Processing
    # ========================================================================

    async def _process_delivery(self, delivery: dict) -> None:
        """
        Process a single delivery with circuit breaker and retry logic.

        Error isolation: one failed delivery never blocks others.
        """
        delivery_id = delivery.get("id")
        url = delivery.get("url")
        attempt = delivery.get("attempt", 0)

        try:
            # Check circuit breaker
            breaker = self._get_circuit_breaker(url)
            if not breaker.can_send():
                logger.debug(
                    f"Circuit breaker OPEN for {url}, skipping delivery {delivery_id}"
                )
                return

            # Attempt delivery
            await self._attempt_delivery(delivery)

        except Exception as e:
            logger.error(
                f"Unexpected error processing delivery {delivery_id}: {e}",
                exc_info=True,
            )

    async def _attempt_delivery(self, delivery: dict) -> None:
        """Attempt to deliver a webhook with timeout and error handling."""
        delivery_id = delivery.get("id")
        url = delivery.get("url")
        payload = delivery.get("payload")
        headers = delivery.get("headers", {})
        attempt = delivery.get("attempt", 0)

        breaker = self._get_circuit_breaker(url)
        start_time = datetime.now(timezone.utc)
        status_code = None
        error = None

        try:
            # Serialize payload
            body_json = json.dumps(payload, default=str)
            body_bytes = body_json.encode()

            # Attempt delivery
            async with httpx.AsyncClient(timeout=DELIVERY_TIMEOUT) as client:
                response = await client.post(
                    url,
                    content=body_bytes,
                    headers=headers,
                )
                status_code = response.status_code

                # Record attempt
                response_time_ms = (
                    (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                )
                await self.tracker.record_attempt(
                    delivery_id,
                    url,
                    status_code,
                    response_time_ms,
                    None,
                )

                # Success
                if 200 <= status_code < 300:
                    delivery["status"] = "success"
                    delivery["response_code"] = status_code
                    delivery["last_attempt_at"] = datetime.now(timezone.utc).isoformat()
                    self.db.save_webhook_delivery(delivery)
                    breaker.record_success()
                    logger.info(
                        f"Webhook {delivery_id} delivered to {url} on "
                        f"attempt {attempt + 1}"
                    )
                    return

                # Non-2xx response; will retry or go to DLQ
                logger.warning(
                    f"Webhook {delivery_id} got {status_code} on attempt {attempt + 1}"
                )
                breaker.record_failure()

                # Schedule retry or move to dead letter
                await self._schedule_retry_or_deadletter(delivery, attempt)

        except (httpx.RequestError, httpx.TimeoutException, Exception) as e:
            error = str(e)
            response_time_ms = (
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            # Record attempt
            await self.tracker.record_attempt(
                delivery_id,
                url,
                None,
                response_time_ms,
                error,
            )

            logger.warning(
                f"Webhook {delivery_id} failed on attempt {attempt + 1}: {error}"
            )
            breaker.record_failure()

            # Schedule retry or move to dead letter
            await self._schedule_retry_or_deadletter(delivery, attempt)

    async def _schedule_retry_or_deadletter(self, delivery: dict, attempt: int) -> None:
        """Schedule a retry or move to dead letter queue."""
        delivery_id = delivery.get("id")
        next_attempt = attempt + 1

        if next_attempt < MAX_ATTEMPTS:
            # Schedule retry
            delay_seconds = BACKOFF_SCHEDULE[next_attempt]
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

            delivery["attempt"] = next_attempt
            delivery["next_retry_at"] = retry_at.isoformat()
            delivery["last_attempt_at"] = datetime.now(timezone.utc).isoformat()
            self.db.save_webhook_delivery(delivery)

            logger.info(
                f"Webhook {delivery_id} scheduled for retry in {delay_seconds}s "
                f"(attempt {next_attempt + 1}/{MAX_ATTEMPTS})"
            )
        else:
            # Move to dead letter queue
            await self._move_to_deadletter(delivery)

    # ========================================================================
    # Dead Letter Queue
    # ========================================================================

    async def _move_to_deadletter(self, delivery: dict) -> None:
        """Move a failed delivery to the dead letter queue."""
        delivery_id = delivery.get("id")
        delivery["status"] = "dead_letter"
        delivery["dead_lettered_at"] = datetime.now(timezone.utc).isoformat()

        self.db.save_dead_letter(delivery)
        # Also persist the terminal status on the SOURCE row. Without this the
        # delivery stays status='pending' with next_retry_at in the past, so
        # list_pending_retries hands it back on every poll — a permanently
        # dead endpoint becomes an infinite redelivery loop that spins the
        # queue worker forever. (MemoryStore mutates the same dict, which is
        # why tests never saw it.)
        try:
            self.db.save_webhook_delivery(delivery)
        except Exception as e:  # noqa: BLE001 — the DLQ write is what matters
            logger.error(f"Could not persist dead-letter status for {delivery_id}: {e}")
        logger.error(f"Webhook {delivery_id} moved to dead letter queue after {MAX_ATTEMPTS} attempts")

    async def replay_dead_letter(self, delivery_id: str) -> bool:
        """
        Re-enqueue a dead letter for retry.

        Args:
            delivery_id: Dead letter delivery ID

        Returns:
            True if replayed, False if not found
        """
        delivery = self.db.get_webhook_delivery(delivery_id)
        if not delivery or delivery.get("status") != "dead_letter":
            return False

        # Reset for retry
        delivery["status"] = "pending"
        delivery["attempt"] = 0
        delivery["next_retry_at"] = datetime.now(timezone.utc).isoformat()
        delivery["dead_lettered_at"] = None

        self.db.save_webhook_delivery(delivery)
        logger.info(f"Replayed dead letter {delivery_id}")
        return True

    async def list_dead_letters(
        self,
        venue_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[dict]:
        """
        List dead letter queue entries.

        Args:
            venue_id: Optional filter by venue
            limit: Max results to return

        Returns:
            List of dead letter deliveries
        """
        dead_letters = self.db.list_dead_letters(venue_id, limit)
        return dead_letters

    async def purge_dead_letters(self, older_than_days: int = 30) -> int:
        """
        Delete old dead letter entries.

        Args:
            older_than_days: Delete entries older than this many days

        Returns:
            Number of entries deleted
        """
        before = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        count = self.db.purge_dead_letters(before)
        logger.info(f"Purged {count} dead letter entries older than {older_than_days} days")
        return count

    # ========================================================================
    # Circuit Breaker Management
    # ========================================================================

    def _get_circuit_breaker(self, url: str) -> CircuitBreaker:
        """Get or create circuit breaker for URL."""
        if url not in self.circuit_breakers:
            self.circuit_breakers[url] = CircuitBreaker(url)
        return self.circuit_breakers[url]

    def get_circuit_status(self, url: Optional[str] = None) -> List[dict]:
        """
        Get circuit breaker status for all or specific URL.

        Args:
            url: Optional URL to filter

        Returns:
            List of circuit breaker status dicts
        """
        if url:
            breaker = self._get_circuit_breaker(url)
            return [breaker.get_status()]

        return [cb.get_status() for cb in self.circuit_breakers.values()]

    def reset_circuit(self, url: str) -> bool:
        """
        Manually reset a circuit breaker.

        Args:
            url: URL to reset

        Returns:
            True if reset, False if not found
        """
        if url not in self.circuit_breakers:
            return False

        breaker = self.circuit_breakers[url]
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0
        breaker.success_count = 0
        breaker.last_failure_at = None
        breaker.opened_at = None

        logger.info(f"Reset circuit breaker for {url}")
        return True


# Singleton instance
_queue: Optional[WebhookRetryQueue] = None


def get_webhook_queue() -> WebhookRetryQueue:
    """Get the singleton WebhookRetryQueue instance."""
    global _queue
    if _queue is None:
        _queue = WebhookRetryQueue()
    return _queue
