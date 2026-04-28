# Webhook Queue Quick Start

## Five-Minute Setup

### 1. Start the Application

The webhook queue processor starts automatically on app startup. No additional configuration needed.

```bash
uvicorn rosteriq.api:app --reload
```

You'll see in the logs:
```
Webhook queue processor started
```

### 2. Send Your First Webhook

When a webhook subscription fires an event, enqueue it for delivery:

```python
from rosteriq.services.webhook_queue import get_webhook_queue
import asyncio

async def send_webhook_event():
    queue = get_webhook_queue()
    
    await queue.enqueue(
        delivery_id="dlv_test_001",
        url="https://customer.example.com/webhook",
        payload={
            "event_type": "roster.published",
            "venue_id": "v_123",
            "roster_id": "r_456",
            "date": "2026-05-01",
        },
        headers={
            "Content-Type": "application/json",
            "X-RosterIQ-Signature": "sha256=abc123...",
            "X-RosterIQ-Event": "roster.published",
            "X-RosterIQ-Delivery-Id": "dlv_test_001",
        },
        venue_id="v_123",
        subscription_id="sub_789",
        event_type="roster.published",
    )

# Run it
asyncio.run(send_webhook_event())
```

The webhook will:
- Be queued immediately
- Attempt delivery within 1 second
- Retry with exponential backoff if it fails
- Auto-recover if the endpoint is temporarily down
- Move to dead-letter queue after 5 failed attempts

### 3. Check Queue Status

```bash
curl http://localhost:8000/api/webhooks/queue/status
```

Response:
```json
{
  "queue_depth": 3,
  "processing": true,
  "poll_interval_seconds": 5,
  "oldest_pending": {
    "id": "dlv_test_001",
    "url": "https://customer.example.com/webhook",
    "created_at": "2026-04-27T10:00:00+00:00",
    "next_retry_at": "2026-04-27T10:00:01+00:00",
    "attempt": 0
  }
}
```

### 4. Monitor Circuit Breakers

See which endpoints are having issues:

```bash
curl http://localhost:8000/api/webhooks/queue/circuits
```

Response:
```json
{
  "count": 2,
  "circuits": [
    {
      "url": "https://customer1.example.com/webhook",
      "state": "closed",
      "failure_count": 0,
      "success_count": 3
    },
    {
      "url": "https://customer2.example.com/webhook",
      "state": "open",
      "failure_count": 5,
      "cooldown_remaining_seconds": 42
    }
  ]
}
```

If a circuit is OPEN, the endpoint is failing. Fix the issue and reset:

```bash
curl -X POST http://localhost:8000/api/webhooks/queue/circuits/https://customer2.example.com/webhook/reset
```

### 5. Review Failed Deliveries

Check what webhooks failed and why:

```bash
curl http://localhost:8000/api/webhooks/queue/dead-letters?limit=10
```

Response:
```json
{
  "count": 2,
  "dead_letters": [
    {
      "id": "dlv_fail_001",
      "url": "https://customer.example.com/webhook",
      "event_type": "roster.published",
      "created_at": "2026-04-27T09:00:00+00:00",
      "dead_lettered_at": "2026-04-27T10:30:00+00:00",
      "attempts": 5,
      "last_error": "Connection refused"
    },
    {
      "id": "dlv_fail_002",
      "url": "https://customer.example.com/webhook",
      "event_type": "shift.created",
      "created_at": "2026-04-27T09:15:00+00:00",
      "dead_lettered_at": "2026-04-27T10:35:00+00:00",
      "attempts": 5,
      "last_error": "HTTP 500"
    }
  ]
}
```

### 6. Replay Failed Webhooks

After customer fixes their endpoint:

```bash
curl -X POST http://localhost:8000/api/webhooks/queue/replay/dlv_fail_001
```

The delivery will be re-attempted with fresh backoff schedule.

### 7. Clean Up Old Dead Letters

Delete entries older than 30 days (for GDPR compliance):

```bash
curl -X POST http://localhost:8000/api/webhooks/queue/purge?older_than_days=30
```

## Common Scenarios

### Scenario 1: Customer's endpoint is temporarily down

**What happens:**
- Queue attempts delivery, gets connection timeout
- Circuit breaker records failure
- After 5 consecutive failures, circuit opens
- Circuit breaker rejects further attempts
- Customer sees "Circuit OPEN" in dashboard

**Resolution:**
1. Customer fixes their endpoint
2. You reset circuit: `POST /api/webhooks/queue/circuits/{url}/reset`
3. Queue immediately retries pending deliveries
4. If still failing, check the dead-letter queue for error details

### Scenario 2: Customer's endpoint needs a new auth token

**What happens:**
- Queue sends webhook with old token
- Gets HTTP 401 Unauthorized
- Retries with same token (fails)
- After 5 failures, moves to dead-letter
- You see HTTP 401 error in dead-letter details

**Resolution:**
1. Contact customer for new auth token
2. Update their webhook subscription with new headers
3. Replay dead letter: `POST /api/webhooks/queue/replay/{delivery_id}`
4. Queue retries with new token

### Scenario 3: A customer receives high webhook volume

**What happens:**
- 1000s of deliveries queued
- Queue processes ~1 per second (based on backoff and latency)
- If endpoint is slow, queue backs up
- But never blocks other customers (error isolation)

**Resolution:**
1. Check queue depth: `GET /api/webhooks/queue/status` → `queue_depth`
2. Contact customer to optimize their endpoint
3. Or increase poll interval for batch processing (advanced)

### Scenario 4: Webhook must be sent within 5 minutes

**What happens:**
- Default backoff: 1s, 5s, 30s, 5min, 30min
- After 5 minutes, delivery will have been attempted 4 times
- Still in queue if not successful
- This is optimal: gives customer time to recover, respects urgency

**If you need faster retry:**
1. Edit `BACKOFF_SCHEDULE` in `services/webhook_queue.py`
2. Example for urgent webhooks: `[1, 2, 5, 15, 30]` (all within ~1min)
3. Redeploy and restart queue

## Testing

### Test with curl

```bash
# Create a test subscription
curl -X POST http://localhost:8000/api/webhooks/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "venue_id": "v_test",
    "callback_url": "https://webhook.site/unique-id",
    "events": ["roster.published"],
    "secret": "test_secret_123"
  }'

# Fire a test event
curl -X POST http://localhost:8000/api/webhooks/fire-event \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "roster.published",
    "venue_id": "v_test",
    "payload": {
      "roster_id": "r_test",
      "date": "2026-05-01"
    }
  }'

# Monitor delivery
curl http://localhost:8000/api/webhooks/queue/status
curl http://localhost:8000/api/webhooks/queue/dead-letters
```

### Test with Python

```python
import asyncio
from rosteriq.services.webhook_queue import get_webhook_queue
from rosteriq.database import MemoryStore

async def test():
    # Create queue with test database
    queue = get_webhook_queue()
    
    # Enqueue a test delivery
    await queue.enqueue(
        delivery_id="test_dlv_001",
        url="https://webhook.site/test",
        payload={"test": "data"},
        headers={"X-Test": "true"},
        venue_id="v_test",
        subscription_id="sub_test",
        event_type="test.event",
    )
    
    # Check status
    status = await queue.get_next_retry()
    print(f"Next retry: {status}")
    
    # Get circuit status
    circuits = queue.get_circuit_status()
    print(f"Circuits: {circuits}")

asyncio.run(test())
```

## Monitoring Dashboard

Create a simple monitoring page with these endpoints:

```html
<html>
<body>
  <h1>Webhook Queue Monitor</h1>
  
  <h2>Queue Status</h2>
  <div id="queue-status"></div>
  
  <h2>Circuit Breakers</h2>
  <div id="circuits"></div>
  
  <h2>Dead Letters</h2>
  <div id="dead-letters"></div>
  
  <script>
    async function refresh() {
      // Queue status
      const status = await fetch('/api/webhooks/queue/status').then(r => r.json());
      document.getElementById('queue-status').innerText = JSON.stringify(status, null, 2);
      
      // Circuits
      const circuits = await fetch('/api/webhooks/queue/circuits').then(r => r.json());
      document.getElementById('circuits').innerText = JSON.stringify(circuits, null, 2);
      
      // Dead letters
      const dls = await fetch('/api/webhooks/queue/dead-letters?limit=5').then(r => r.json());
      document.getElementById('dead-letters').innerText = JSON.stringify(dls, null, 2);
    }
    
    refresh();
    setInterval(refresh, 5000); // Refresh every 5 seconds
  </script>
</body>
</html>
```

## Troubleshooting

**Q: Queue is not processing, status shows "processing": false**
- A: Queue crashed or wasn't started. Check logs, restart app.

**Q: Circuit breaker stuck OPEN for customer**
- A: Endpoint is failing. Fix it, then `POST /api/webhooks/queue/circuits/{url}/reset`

**Q: Dead letters accumulating**
- A: Review error messages, contact customers, replay when fixed.

**Q: How do I know a webhook was delivered?**
- A: Check queue status (if pending, not yet). Check logs (search delivery_id).

**Q: How do I increase retry attempts?**
- A: Edit `MAX_ATTEMPTS` in `services/webhook_queue.py`, redeploy.

## Next Steps

1. **Read full documentation**: See `WEBHOOK_QUEUE_README.md`
2. **Run tests**: `pytest tests/test_webhook_queue.py -v`
3. **Deploy to production**: Set up PostgreSQL tables (see migration in README)
4. **Monitor closely**: Check /metrics and dead-letters daily for first week
5. **Tune backoff**: Adjust `BACKOFF_SCHEDULE` based on your customer latency

Happy webhooking!
