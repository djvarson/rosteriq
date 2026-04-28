# Webhook Retry Queue with Circuit Breaker

A production-quality persistent webhook delivery system for RosterIQ with exponential backoff retries, dead-letter queue handling, and per-destination circuit breakers.

## Architecture

### Components

1. **WebhookRetryQueue** — Main queue manager
   - Enqueues webhook deliveries for retry
   - Processes pending retries with exponential backoff
   - Moves permanently failed deliveries to dead-letter queue
   - Manages circuit breaker state per destination URL

2. **CircuitBreaker** — Per-URL failure protection
   - States: CLOSED (normal), OPEN (failing, reject immediately), HALF_OPEN (testing)
   - Prevents cascading failures by stopping requests to failing endpoints
   - Automatic recovery with cooldown and test phase

3. **WebhookDeliveryTracker** — Attempt logging and metrics
   - Records each delivery attempt with status and response time
   - Tracks errors and error types
   - Enables observability and debugging

4. **Database Layer** — Persistent storage
   - `BaseStore` interface with implementations for MemoryStore and PostgresStore
   - Methods for queue management, dead-letter handling, and purging

### Retry Strategy

**Exponential Backoff Schedule:**
- Attempt 1 → 1 second
- Attempt 2 → 5 seconds
- Attempt 3 → 30 seconds
- Attempt 4 → 5 minutes
- Attempt 5 → 30 minutes
- **Max 5 attempts total**

After max attempts, delivery moves to dead-letter queue for manual review and replay.

### Circuit Breaker Behavior

```
CLOSED (normal operation)
  ↓
  5 consecutive failures
  ↓
OPEN (reject requests immediately)
  ↓
  60 second cooldown
  ↓
HALF_OPEN (test 1 request)
  ↓
  Success → CLOSED
  Failure → OPEN
```

## API Endpoints

### Queue Status

```
GET /api/webhooks/queue/status
```

Returns queue depth, processing state, and oldest pending item.

```json
{
  "queue_depth": 42,
  "processing": true,
  "poll_interval_seconds": 5,
  "oldest_pending": {
    "id": "dlv_abc123",
    "url": "https://example.com/webhook",
    "created_at": "2026-04-27T12:00:00+00:00",
    "next_retry_at": "2026-04-27T12:05:15+00:00",
    "attempt": 1
  }
}
```

### Dead Letter Queue

```
GET /api/webhooks/queue/dead-letters?venue_id=v_123&limit=50
```

List failed deliveries for manual review.

```json
{
  "count": 3,
  "limit": 50,
  "venue_id_filter": "v_123",
  "dead_letters": [
    {
      "id": "dlv_xyz789",
      "url": "https://customer.example.com/webhook",
      "status": "dead_letter",
      "event_type": "roster.published",
      "venue_id": "v_123",
      "subscription_id": "sub_456",
      "created_at": "2026-04-27T10:00:00+00:00",
      "dead_lettered_at": "2026-04-27T12:30:00+00:00",
      "attempts": 5,
      "last_error": "Connection timeout"
    }
  ]
}
```

**Replay a dead letter:**

```
POST /api/webhooks/queue/replay/dlv_xyz789
```

Re-enqueues the delivery for retry starting from attempt 0.

```json
{
  "message": "Replayed dead letter dlv_xyz789",
  "delivery_id": "dlv_xyz789",
  "status": "replay_scheduled"
}
```

**Purge old dead letters:**

```
POST /api/webhooks/queue/purge?older_than_days=30
```

Delete dead letters older than 30 days.

```json
{
  "message": "Purged dead letter entries older than 30 days",
  "count_deleted": 12
}
```

### Circuit Breaker Status

```
GET /api/webhooks/queue/circuits?url=https://example.com/webhook
```

List circuit breaker status for all or specific URL.

```json
{
  "count": 1,
  "url_filter": "https://example.com/webhook",
  "circuits": [
    {
      "url": "https://example.com/webhook",
      "state": "open",
      "failure_count": 5,
      "success_count": 0,
      "last_failure_at": "2026-04-27T12:15:00+00:00",
      "cooldown_remaining_seconds": 45
    }
  ]
}
```

**Reset a circuit breaker:**

```
POST /api/webhooks/queue/circuits/https://example.com/webhook/reset
```

Manually close a circuit and clear failure counts (allows immediate retry).

```json
{
  "message": "Reset circuit breaker for https://example.com/webhook",
  "url": "https://example.com/webhook",
  "circuit": {
    "url": "https://example.com/webhook",
    "state": "closed",
    "failure_count": 0,
    "success_count": 0,
    "last_failure_at": null,
    "cooldown_remaining_seconds": 0
  }
}
```

## Database Methods

### BaseStore Interface

All methods are available on both `MemoryStore` (development) and `PostgresStore` (production).

```python
# Save/retrieve deliveries
def save_webhook_delivery(self, delivery: dict) -> None:
    """Save a webhook delivery record."""

def get_webhook_delivery(self, delivery_id: str) -> Optional[dict]:
    """Get a delivery by ID."""

def list_pending_retries(self, before: datetime) -> list[dict]:
    """List pending deliveries ready for retry (next_retry_at <= before)."""

# Dead letter queue
def save_dead_letter(self, dead_letter: dict) -> None:
    """Save a failed delivery to dead letter queue."""

def list_dead_letters(self, venue_id: Optional[str] = None, limit: int = 50) -> list[dict]:
    """List dead letters with optional venue filter."""

def delete_dead_letter(self, delivery_id: str) -> None:
    """Delete a specific dead letter."""

def purge_dead_letters(self, before: datetime) -> int:
    """Delete dead letters older than date. Returns count deleted."""
```

## Integration Guide

### 1. Enqueueing Deliveries

When you want to send a webhook, use the queue instead of sending directly:

```python
from rosteriq.services.webhook_queue import get_webhook_queue

queue = get_webhook_queue()

await queue.enqueue(
    delivery_id="dlv_abc123",
    url="https://customer.example.com/webhook",
    payload={
        "event_type": "roster.published",
        "timestamp": "2026-04-27T12:00:00Z",
        "venue_id": "v_123",
        "data": {"roster_id": "r_789", "date": "2026-05-01"},
    },
    headers={
        "Content-Type": "application/json",
        "X-RosterIQ-Signature": "sha256=...",
        "X-RosterIQ-Event": "roster.published",
        "X-RosterIQ-Delivery-Id": "dlv_abc123",
    },
    venue_id="v_123",
    subscription_id="sub_456",
    event_type="roster.published",
)
```

### 2. Automatic Processing

The queue processor starts automatically on app startup:

```python
# In api.py startup event
queue = get_webhook_queue()
await queue.start()  # Starts background task
```

The processor:
- Polls the queue every 5 seconds
- Checks circuit breakers before sending
- Records attempts and metrics
- Schedules retries with exponential backoff
- Moves failed deliveries to dead-letter after max attempts
- Never blocks on individual failures (error isolation)

### 3. Monitoring

Check queue health via the status endpoint:

```bash
curl http://localhost:8000/api/webhooks/queue/status
```

Monitor circuit breakers for stuck endpoints:

```bash
curl http://localhost:8000/api/webhooks/queue/circuits
```

Review failed deliveries:

```bash
curl http://localhost:8000/api/webhooks/queue/dead-letters?limit=20
```

### 4. Recovery

**For stuck endpoints:**

1. Check circuit status: `GET /api/webhooks/queue/circuits`
2. If OPEN, verify endpoint is working
3. Reset circuit: `POST /api/webhooks/queue/circuits/{url}/reset`
4. Queue will resume sending after cooldown or manual reset

**For failed deliveries:**

1. List dead letters: `GET /api/webhooks/queue/dead-letters`
2. Review error messages and details
3. Fix the endpoint (customer contact, firewall rule, auth token, etc.)
4. Replay delivery: `POST /api/webhooks/queue/replay/{delivery_id}`
5. Or cleanup old entries: `POST /api/webhooks/queue/purge?older_than_days=30`

## Performance Characteristics

- **Queue polling**: 5 second default interval (configurable)
- **Attempt timeout**: 10 seconds per request
- **Max queue size**: Unlimited (persistent storage)
- **Dead letter retention**: 30 days default purge
- **Memory usage**: O(number of circuit breakers), typically <1MB for 1000+ URLs

## Error Isolation

Critical design principle: **One failed delivery never blocks others**.

- Each delivery processed independently
- Exceptions logged but don't propagate
- Circuit breaker per URL (not global)
- Timeout prevents indefinite hangs
- Failed delivery doesn't prevent queue processing

## Testing

Run unit tests:

```bash
pytest tests/test_webhook_queue.py -v
```

Tests cover:
- Circuit breaker state transitions
- Exponential backoff schedule
- Retry/dead-letter logic
- Dead letter replay and purging
- Delivery attempt tracking
- Error isolation

## Configuration

Adjust defaults in `services/webhook_queue.py`:

```python
# Retry configuration
BACKOFF_SCHEDULE = [1, 5, 30, 300, 1800]  # seconds
MAX_ATTEMPTS = 5
DELIVERY_TIMEOUT = 10  # seconds

# Circuit breaker
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5  # failures to open
CIRCUIT_BREAKER_COOLDOWN = 60  # seconds before testing
```

Adjust queue polling in `WebhookRetryQueue`:

```python
queue = WebhookRetryQueue(db=get_db(), poll_interval_seconds=5)
```

## Production Deployment

### Database Migration (PostgreSQL)

Create required tables:

```sql
-- Webhook deliveries (existing table, may need updates)
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id VARCHAR(32) PRIMARY KEY,
    subscription_id VARCHAR(32),
    event_type VARCHAR(100),
    status VARCHAR(20),
    response_code INTEGER,
    attempts JSONB,
    last_attempt_at TIMESTAMP,
    next_retry_at TIMESTAMP,
    created_at TIMESTAMP,
    INDEX (status, next_retry_at),
    INDEX (subscription_id)
);

-- Dead letter queue
CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    id VARCHAR(32) PRIMARY KEY,
    url TEXT,
    payload JSONB,
    headers JSONB,
    venue_id VARCHAR(32),
    subscription_id VARCHAR(32),
    event_type VARCHAR(100),
    status VARCHAR(20),
    attempt INTEGER,
    attempts JSONB,
    created_at TIMESTAMP,
    dead_lettered_at TIMESTAMP,
    error TEXT,
    response_code INTEGER,
    last_attempt_at TIMESTAMP,
    INDEX (status),
    INDEX (venue_id),
    INDEX (dead_lettered_at)
);
```

### Monitoring

Set up alerts:

- Queue depth exceeds threshold (100+ pending)
- Circuit breaker OPEN for > 5 minutes
- Dead letters accumulating (>10 per hour)
- Processor stopped (no activity for 10+ minutes)

### Logging

Check logs for webhook queue activity:

```bash
# Queue processor started/stopped
grep "webhook queue processor" app.log

# Deliveries moved to dead letter
grep "moved to dead letter queue" app.log

# Circuit breaker state changes
grep "Circuit breaker" app.log
```

## Troubleshooting

### Queue Not Processing

1. Check if processor is running: `GET /api/webhooks/queue/status` → `"processing": true`
2. Check logs for startup errors
3. Verify database connectivity
4. Restart application

### Circuit Breaker Stuck OPEN

1. Check endpoint health from server
2. Verify network connectivity, TLS certificates, auth tokens
3. Fix underlying issue
4. Reset circuit: `POST /api/webhooks/queue/circuits/{url}/reset`

### High Failure Rate

1. Check failed deliveries: `GET /api/webhooks/queue/dead-letters`
2. Review error messages (timeouts, auth failures, etc.)
3. Contact customer to verify endpoint configuration
4. Increase timeout if needed (modify `DELIVERY_TIMEOUT`)

### Memory Growing

- Normal: circuit breakers are cached per URL
- Check unique URL count: `GET /api/webhooks/queue/circuits` → count
- Purge old dead letters: `POST /api/webhooks/queue/purge?older_than_days=7`

## Files

- **services/webhook_queue.py** — Queue, circuit breaker, tracker implementations
- **routes/webhook_queue.py** — REST API endpoints
- **database.py** — BaseStore, MemoryStore, PostgresStore methods
- **api.py** — Queue startup/shutdown, route registration
- **tests/test_webhook_queue.py** — Unit tests

## Future Enhancements

- [ ] Webhook signature verification in tracker
- [ ] Histogram metrics (response times by percentile)
- [ ] Batch delivery attempts (reduce polling overhead)
- [ ] Dashboard UI for queue management
- [ ] Webhook templates/templating for common events
- [ ] Rate limiting per subscription
