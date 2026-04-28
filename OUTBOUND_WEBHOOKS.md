# Outbound Webhooks System

## Overview

RosterIQ now fires webhooks to external systems when events occur. This enables real-time integration with external applications and custom workflows.

## Components

### 1. Service: `services/outbound_webhooks.py`

Core webhook service with three main responsibilities:

#### OutboundWebhookService Class

**Subscription Management:**
- `register_subscription(venue_id, callback_url, events, secret)` → subscription_id
- `list_subscriptions(venue_id)` → list[dict]
- `delete_subscription(subscription_id)` → bool
- `update_subscription(subscription_id, events=None, callback_url=None, active=None)` → dict

**Event Firing:**
- `fire_event(event_type, venue_id, payload)` → int (matched subscriptions count)
  - Queues webhook delivery for all matching subscriptions
  - Async operation with retry logic

**Delivery:**
- `deliver_webhook(subscription, event_type, payload)` → None
  - HTTP POST to callback_url
  - HMAC-SHA256 signature in X-RosterIQ-Signature header
  - 3 retry attempts with exponential backoff: 5s, 30s, 120s
  - 10 second timeout per attempt
  - Async operation

**Delivery Log:**
- `get_delivery_log(subscription_id, limit=50)` → list[dict]
  - Returns recent delivery records with status, attempts, timestamps

#### Event Types (EventType enum)

```python
ROSTER_PUBLISHED = "roster.published"
ROSTER_UPDATED = "roster.updated"
SHIFT_CREATED = "shift.created"
SHIFT_SWAPPED = "shift.swapped"
SHIFT_CANCELLED = "shift.cancelled"
EMPLOYEE_ADDED = "employee.added"
EMPLOYEE_UPDATED = "employee.updated"
ALERT_COMPLIANCE = "alert.compliance"
ALERT_VARIANCE = "alert.variance"
FORECAST_UPDATED = "forecast.updated"
```

### 2. Routes: `routes/outbound_webhooks.py`

FastAPI routes for webhook management:

```
POST   /api/webhooks/subscribe                    — Register subscription
GET    /api/webhooks/subscriptions/{venue_id}    — List subscriptions
DELETE /api/webhooks/subscriptions/{subscription_id} — Delete subscription
PUT    /api/webhooks/subscriptions/{subscription_id} — Update subscription
GET    /api/webhooks/deliveries/{subscription_id}   — Get delivery log
POST   /api/webhooks/test/{subscription_id}     — Send test event
```

#### Request/Response Models

**SubscribeRequest:**
```json
{
  "venue_id": "string",
  "callback_url": "https://...",
  "events": ["roster.published", "shift.created"],
  "secret": "your-secret-key"
}
```

**UpdateSubscriptionRequest:**
```json
{
  "events": ["roster.published"],
  "callback_url": "https://...",
  "active": true
}
```

**TestEventRequest:**
```json
{
  "event_type": "roster.published",
  "data": {
    "custom_field": "value"
  }
}
```

### 3. Database Layer

Added methods to `BaseStore`, `MemoryStore`, and `PostgresStore`:

**BaseStore (interface):**
- `save_webhook_subscription(subscription: dict)` → None
- `get_webhook_subscription(subscription_id: str)` → Optional[dict]
- `list_webhook_subscriptions(venue_id: str)` → list[dict]
- `delete_webhook_subscription(subscription_id: str)` → None
- `save_webhook_delivery(delivery: dict)` → None
- `list_webhook_deliveries(subscription_id: str, limit: int)` → list[dict]

**MemoryStore:**
- In-memory storage for development/testing
- Keeps last 1000 deliveries per subscription
- Subscriptions stored by subscription_id
- Deliveries stored as list per subscription (newest first)

**PostgresStore:**
- Requires PostgreSQL schema below
- Automatic JSON serialization for events list
- Delivery records indexed by subscription_id
- Auto-cleanup of subscriptions when deleted

## Webhook Payload Format

```json
{
  "event_type": "roster.published",
  "timestamp": "2026-04-25T10:30:00+00:00",
  "venue_id": "venue-123",
  "data": {
    "roster_id": "roster-456",
    "week_start": "2026-04-27",
    "total_cost": 5000.00
  },
  "webhook_id": "dlv_a1b2c3d4e5f6g7h8"
}
```

## Webhook Headers

```
Content-Type: application/json
X-RosterIQ-Signature: sha256=<hmac-sha256-hex>
X-RosterIQ-Event: roster.published
X-RosterIQ-Delivery-Id: dlv_a1b2c3d4e5f6g7h8
```

### Signature Verification

The `X-RosterIQ-Signature` header contains an HMAC-SHA256 hash of the request body using the subscription secret:

```python
import hmac
import hashlib

signature = hmac.new(
    secret.encode(),
    body_bytes,
    hashlib.sha256,
).hexdigest()

# Header format: "sha256=<hex>"
expected = f"sha256={signature}"
assert hmac.compare_digest(expected, header_value)
```

## Database Schema

### PostgreSQL Migration

Create the required tables:

```sql
-- Webhook subscriptions
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
  id VARCHAR(32) PRIMARY KEY,
  venue_id VARCHAR(255) NOT NULL,
  callback_url TEXT NOT NULL,
  events JSONB DEFAULT '[]'::jsonb,
  secret TEXT NOT NULL,
  active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  CONSTRAINT fk_webhook_subscriptions_venue
    FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE
);

CREATE INDEX idx_webhook_subscriptions_venue_id
  ON webhook_subscriptions(venue_id);

-- Webhook delivery log
CREATE TABLE IF NOT EXISTS webhook_deliveries (
  id VARCHAR(32) PRIMARY KEY,
  subscription_id VARCHAR(32) NOT NULL,
  event_type VARCHAR(255) NOT NULL,
  status VARCHAR(20) NOT NULL, -- 'success', 'pending', 'failed'
  response_code INTEGER,
  attempts INTEGER DEFAULT 0,
  last_attempt_at TIMESTAMP,
  next_retry_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT now(),
  CONSTRAINT fk_webhook_deliveries_subscription
    FOREIGN KEY (subscription_id) REFERENCES webhook_subscriptions(id) ON DELETE CASCADE
);

CREATE INDEX idx_webhook_deliveries_subscription_id
  ON webhook_deliveries(subscription_id);
CREATE INDEX idx_webhook_deliveries_created_at
  ON webhook_deliveries(created_at DESC);
```

## Usage Examples

### Register a Subscription

```bash
curl -X POST http://localhost:8000/api/webhooks/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "venue_id": "venue-123",
    "callback_url": "https://example.com/webhooks/rosteriq",
    "events": ["roster.published", "shift.created"],
    "secret": "super-secret-key"
  }'
```

Response:
```json
{
  "status": "created",
  "subscription_id": "sub_a1b2c3d4e5f6g7h8",
  "venue_id": "venue-123",
  "callback_url": "https://example.com/webhooks/rosteriq",
  "events": ["roster.published", "shift.created"]
}
```

### Fire an Event (from application code)

```python
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service

service = get_outbound_webhook_service()

# Fire event
await service.fire_event(
    event_type="roster.published",
    venue_id="venue-123",
    payload={
        "roster_id": "roster-456",
        "week_start": "2026-04-27",
        "total_cost": 5000.00,
        "shifts_count": 42,
    }
)
```

### List Subscriptions

```bash
curl http://localhost:8000/api/webhooks/subscriptions/venue-123
```

Response:
```json
{
  "status": "ok",
  "venue_id": "venue-123",
  "subscriptions": [
    {
      "id": "sub_a1b2c3d4e5f6g7h8",
      "venue_id": "venue-123",
      "callback_url": "https://example.com/webhooks/rosteriq",
      "events": ["roster.published", "shift.created"],
      "active": true,
      "created_at": "2026-04-25T10:00:00+00:00",
      "updated_at": "2026-04-25T10:00:00+00:00"
    }
  ],
  "count": 1
}
```

### Get Delivery Log

```bash
curl http://localhost:8000/api/webhooks/deliveries/sub_a1b2c3d4e5f6g7h8?limit=10
```

Response:
```json
{
  "status": "ok",
  "subscription_id": "sub_a1b2c3d4e5f6g7h8",
  "deliveries": [
    {
      "id": "dlv_z9y8x7w6v5u4t3s2",
      "event_type": "shift.created",
      "status": "success",
      "response_code": 200,
      "attempts": 1,
      "last_attempt_at": "2026-04-25T10:25:30+00:00",
      "next_retry_at": null,
      "created_at": "2026-04-25T10:25:30+00:00"
    },
    {
      "id": "dlv_a1b2c3d4e5f6g7h8",
      "event_type": "roster.published",
      "status": "success",
      "response_code": 202,
      "attempts": 1,
      "last_attempt_at": "2026-04-25T10:15:00+00:00",
      "next_retry_at": null,
      "created_at": "2026-04-25T10:15:00+00:00"
    }
  ],
  "count": 2
}
```

### Send Test Event

```bash
curl -X POST http://localhost:8000/api/webhooks/test/sub_a1b2c3d4e5f6g7h8 \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "roster.published",
    "data": {
      "test": true
    }
  }'
```

Response:
```json
{
  "status": "queued",
  "subscription_id": "sub_a1b2c3d4e5f6g7h8",
  "event_type": "roster.published",
  "delivery_id": "test_a1b2c3d4e5f6",
  "message": "Test event queued for delivery"
}
```

## Retry Logic

Failed deliveries automatically retry with exponential backoff:

1. **First attempt:** Immediate
2. **Second attempt:** After 5 seconds
3. **Third attempt:** After 30 seconds
4. **Final attempt:** After 120 seconds

After 3 failed attempts, the delivery is marked as failed and logged. Check the delivery log via the API to investigate failures.

## Integration Points

### Where to Fire Events

Common integration points in the application:

```python
# Roster published
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service
service = get_outbound_webhook_service()

await service.fire_event(
    "roster.published",
    venue_id,
    {"roster_id": roster.id, "week_start": roster.week_start, ...}
)
```

Add these calls to:
- Roster generation endpoints
- Shift creation/modification endpoints
- Employee management endpoints
- Alert generation logic
- Forecast updates

## Configuration

- **Timeout:** 10 seconds per delivery attempt
- **Retries:** 3 attempts
- **Backoff delays:** 5s, 30s, 120s
- **Delivery log retention:** Last 1000 deliveries per subscription (in-memory), unlimited (PostgreSQL)
- **Callback URL:** Must be HTTPS

## Security Considerations

1. **Always verify signatures** on the receiver side using HMAC-SHA256
2. **Use HTTPS only** for callback URLs
3. **Rotate secrets** periodically
4. **Rate limit** webhook processing on receiver side
5. **Log all deliveries** for audit trail
6. **Implement idempotency** using delivery_id to handle retries
7. **Never log secrets** in error messages or logs

## Monitoring

Use the delivery log endpoints to monitor webhook health:

```python
# Get failed deliveries
from rosteriq.services.outbound_webhooks import get_outbound_webhook_service

service = get_outbound_webhook_service()
deliveries = service.get_delivery_log(subscription_id, limit=100)

failed = [d for d in deliveries if d['status'] == 'failed']
pending = [d for d in deliveries if d['status'] == 'pending']
```

## Files Created/Modified

### New Files
- `/RosterIQ/services/outbound_webhooks.py` (350 lines) — Core service
- `/RosterIQ/routes/outbound_webhooks.py` (350 lines) — API routes

### Modified Files
- `/RosterIQ/database.py` — Added 6 methods to BaseStore, 6 to MemoryStore, 6 to PostgresStore
- `/RosterIQ/api.py` — Registered outbound webhook routes

### SQL Schema Required
- `webhook_subscriptions` table
- `webhook_deliveries` table
- Two indexes for performance

## Testing

Test the webhook system:

```bash
# 1. Register subscription
SUBSCRIPTION_ID=$(curl -X POST http://localhost:8000/api/webhooks/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "venue_id": "test-venue",
    "callback_url": "https://httpbin.org/post",
    "events": ["roster.published"],
    "secret": "test-secret"
  }' | jq -r .subscription_id)

# 2. Send test event
curl -X POST http://localhost:8000/api/webhooks/test/$SUBSCRIPTION_ID

# 3. Check delivery log
curl http://localhost:8000/api/webhooks/deliveries/$SUBSCRIPTION_ID

# 4. Verify signature on receiver (pseudo-code)
# Extract X-RosterIQ-Signature header
# Compute HMAC-SHA256 of body with secret
# Compare using hmac.compare_digest()
```
