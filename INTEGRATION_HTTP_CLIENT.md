# Resilient HTTP Client Integration Guide

## Overview

RosterIQ provides a shared resilient HTTP client (`ResilientHttpClient`) for all external API calls. It handles:

- **Connection pooling**: Shared async connection pool with max 100 connections, 20 keepalive per host
- **Retry logic**: Exponential backoff with jitter for transient failures (429, 5xx errors)
- **Circuit breaker**: Per-host state machine to prevent cascading failures
- **Metrics**: Detailed per-host statistics (latency, error rates, retry counts)
- **Structured logging**: Correlation IDs, sanitized URLs, detailed timing

## Quick Start

### Using Pre-configured Clients

```python
from rosteriq.services.http_client import (
    get_tanda_client,
    get_stripe_client,
    get_xero_client,
    get_weather_client,
)

# Tanda API calls
tanda_client = get_tanda_client()
response = await tanda_client.get(
    "https://api.tanda.co/v2/schedules",
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

# Stripe API calls
stripe_client = get_stripe_client()
response = await stripe_client.post(
    "https://api.stripe.com/v1/charges",
    data={"amount": 1000}
)

# BOM weather API
weather_client = get_weather_client()
response = await weather_client.get(
    "https://api.bom.gov.au/v1/locations/SYDNEY"
)
```

### Using the Generic Client

```python
from rosteriq.services.http_client import get_http_client

client = await get_http_client()

# GET request
response = await client.get(
    "https://api.example.com/data",
    params={"limit": 10}
)

# POST request
response = await client.post(
    "https://api.example.com/data",
    json={"key": "value"}
)

# PUT request
response = await client.put(
    "https://api.example.com/data/123",
    json={"updated": "value"}
)

# DELETE request
response = await client.delete("https://api.example.com/data/123")
```

## Configuration

### Per-Host Timeouts and Retries

```python
from rosteriq.services.http_client import get_http_client

client = await get_http_client()

# Configure a specific API host
client.configure_host(
    host="api.example.com",
    connect_timeout=5.0,      # TCP connection timeout
    read_timeout=60.0,        # Response read timeout
    write_timeout=10.0,       # Request body write timeout
    max_retries=3,            # Number of retry attempts
    backoff_base=1.0,         # Initial backoff delay (seconds)
    backoff_max=30.0,         # Maximum backoff delay (seconds)
)
```

### Backoff Strategy

The client uses exponential backoff with jitter:

```
delay = min(base * 2^attempt, max)
with jitter: delay ± 25%
```

Example progression (base=1.0, max=30.0):
- Attempt 0: 1s ± 0.25s = 0.75-1.25s
- Attempt 1: 2s ± 0.5s = 1.5-2.5s
- Attempt 2: 4s ± 1s = 3-5s
- Attempt 3: 8s ± 2s = 6-10s

## Error Handling

### Retryable Errors

The client automatically retries on:
- `429 Too Many Requests` (respects Retry-After header)
- `500 Internal Server Error`
- `502 Bad Gateway`
- `503 Service Unavailable`
- `504 Gateway Timeout`
- Network timeouts and connection errors

### Non-Retryable Errors

The client does NOT retry:
- `400 Bad Request`
- `401 Unauthorized`
- `403 Forbidden`
- `404 Not Found`
- `422 Unprocessable Entity`

### Exception Handling

```python
import httpx
from rosteriq.services.http_client import get_http_client

client = await get_http_client()

try:
    response = await client.get("https://api.example.com/data")
    response.raise_for_status()
    data = response.json()
except httpx.TimeoutException:
    # Connection or read timeout after retries
    logger.error("Request timed out")
except httpx.ConnectError:
    # Unable to connect to host
    logger.error("Connection failed")
except RuntimeError as e:
    # Circuit breaker is OPEN for this host
    if "Circuit breaker OPEN" in str(e):
        logger.error(f"Host is temporarily unavailable: {e}")
except httpx.HTTPStatusError:
    # Server returned 4xx or 5xx (non-retryable)
    logger.error(f"HTTP error: {e.response.status_code}")
```

## Circuit Breaker

### How It Works

The client tracks failures per host and opens a circuit breaker when failures spike:

1. **CLOSED** (normal): All requests allowed
2. **OPEN** (failing): Requests rejected immediately, preventing cascading failures
3. **HALF_OPEN** (testing): After cooldown, one probe request allowed to test recovery
4. **CLOSED** (recovered): Probe succeeded, back to normal operation

### Configuration

```python
from rosteriq.services.http_client import ResilientHttpClient, CircuitBreakerConfig

# Default config (in ResilientHttpClient.__init__)
cb_config = CircuitBreakerConfig(
    failure_threshold=5,           # Open after 5 failures
    failure_window_seconds=60,     # Within 60 second window
    cooldown_seconds=30,           # Stay open for 30 seconds
    half_open_max_calls=1,         # Allow 1 probe in HALF_OPEN state
)
```

### Example: Circuit Breaker Behavior

```python
import asyncio
from rosteriq.services.http_client import get_http_client

client = await get_http_client()

# Configure a failing API
client.configure_host("api.failing.com", max_retries=1)

# Make 5 requests to failing API
for i in range(5):
    try:
        await client.get("https://api.failing.com/data")
    except Exception as e:
        print(f"Request {i+1} failed")

# Circuit breaker is now OPEN
try:
    await client.get("https://api.failing.com/data")
except RuntimeError as e:
    print(f"Rejected: {e}")  # Circuit breaker OPEN

# Wait for cooldown
await asyncio.sleep(31)

# Circuit breaker is HALF_OPEN, next request is a probe
try:
    await client.get("https://api.failing.com/data")
except Exception:
    pass  # Probe failed, circuit reopens
```

## Structured Logging

All requests are logged with:
- Correlation ID (for request tracing)
- HTTP method and sanitized URL
- Response status code
- Duration in milliseconds
- Retry count
- Circuit breaker state

Example log output:

```
[abc123def456] GET https://api.example.com/data → 200 [45.3ms, retry_count=0]
[xyz789uvw012] POST https://api.example.com/data → 429 (retryable), waiting 1.50s before retry 1
[xyz789uvw012] POST https://api.example.com/data → 200 [120.5ms, retry_count=1]
[error001] GET https://failing.com/data → raised TimeoutError after 1 retries
[circuit001] GET https://failing.com/data raised RuntimeError: Circuit breaker OPEN for failing.com
```

## Monitoring and Metrics

### Get All Statistics

```python
from rosteriq.services.http_client import get_http_client

client = await get_http_client()
stats = client.stats()

# stats is a dict keyed by host:
# {
#     "api.tanda.co": {
#         "request_count": 150,
#         "error_count": 3,
#         "success_count": 147,
#         "avg_latency_ms": 245.6,
#         "min_latency_ms": 45.2,
#         "max_latency_ms": 1200.8,
#         "retry_count": 5,
#         "circuit_state": "closed",
#         "last_error_at": "2026-04-27T21:15:00+00:00",
#         "last_success_at": "2026-04-27T21:20:15+00:00",
#     },
#     ...
# }

for host, metrics in stats.items():
    print(f"{host}:")
    print(f"  Requests: {metrics['request_count']}")
    print(f"  Errors: {metrics['error_count']}")
    print(f"  Avg latency: {metrics['avg_latency_ms']:.2f}ms")
    print(f"  Circuit state: {metrics['circuit_state']}")
```

### Admin Endpoint

The API provides `/api/v1/admin/http-stats` endpoint:

```bash
curl http://localhost:8000/api/v1/admin/http-stats | jq
```

Response:

```json
{
  "status": "ok",
  "timestamp": "2026-04-27T21:20:15.123456",
  "hosts": {
    "api.tanda.co": {
      "request_count": 150,
      "error_count": 3,
      "success_count": 147,
      "avg_latency_ms": 245.6,
      "min_latency_ms": 45.2,
      "max_latency_ms": 1200.8,
      "retry_count": 5,
      "circuit_state": "closed",
      "last_error_at": "2026-04-27T21:15:00+00:00",
      "last_success_at": "2026-04-27T21:20:15+00:00"
    }
  }
}
```

## Migration from Direct httpx Usage

### Before (Old Pattern)

```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.get(
        "https://api.tanda.co/v2/schedules",
        headers={"Authorization": "Bearer TOKEN"}
    )
```

### After (Resilient Pattern)

```python
from rosteriq.services.http_client import get_tanda_client

client = get_tanda_client()
response = await client.get(
    "https://api.tanda.co/v2/schedules",
    headers={"Authorization": "Bearer TOKEN"}
)
# Automatic retry, circuit breaker, connection pooling, metrics
```

## Integration Checklist

- [ ] Replace `httpx.AsyncClient()` with `get_http_client()` or pre-configured client
- [ ] Use the appropriate pre-configured client (Tanda, Stripe, Xero, Weather)
- [ ] Pass `correlation_id` parameter for request tracing if needed
- [ ] Handle `RuntimeError` for circuit breaker OPEN state
- [ ] Monitor `/api/v1/admin/http-stats` endpoint in production
- [ ] Set up alerts for high error rates or circuit breaker opens
- [ ] Test failure scenarios with the monitoring dashboard

## Performance Characteristics

- **Connection pooling**: Reuses up to 100 total connections, 20 per host
- **Keep-alive**: Persistent connections reduce latency for subsequent requests
- **Backoff jitter**: Prevents thundering herd when services recover
- **Circuit breaker**: Fails fast for known-failing hosts (avoids timeouts)
- **Metrics overhead**: Negligible (<1ms per request for metrics tracking)

## Testing

Example test helper:

```python
import pytest
from unittest.mock import AsyncMock, patch
from rosteriq.services.http_client import get_http_client

@pytest.mark.asyncio
async def test_with_mocked_http():
    with patch('rosteriq.services.http_client.get_http_client') as mock:
        mock_client = AsyncMock()
        mock.return_value = mock_client
        
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        mock_client.get.return_value = mock_response
        
        client = await get_http_client()
        response = await client.get("https://api.example.com/data")
        
        assert response.status_code == 200
        assert await response.json() == {"status": "ok"}
```
