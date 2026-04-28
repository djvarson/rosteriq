"""
Shared resilient HTTP client with retry, circuit breaker, and connection pooling.

Provides a singleton `ResilientHttpClient` that all external API calls should use.
Implements:
- Shared async connection pooling via httpx.AsyncClient
- Exponential backoff retry logic with Retry-After header support
- Per-host circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED)
- Structured logging with correlation IDs
- Per-host metrics tracking (request count, error count, latency)
- Host-specific timeout and retry configuration

Usage:
    from rosteriq.services.http_client import get_http_client

    client = get_http_client()
    response = await client.get("https://api.example.com/data")

    # Or use pre-configured clients
    from rosteriq.services.http_client import get_tanda_client
    tanda_client = get_tanda_client()
    response = await tanda_client.get("https://tanda.co/api/...")
"""

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, Optional, Any, Tuple
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


# ============================================================================
# Enums and data structures
# ============================================================================


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"          # Too many failures, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class HostConfig:
    """Configuration for a specific API host."""
    host: str
    connect_timeout: float = 5.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0
    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 30.0


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker per host."""
    failure_threshold: int = 5
    failure_window_seconds: int = 60
    cooldown_seconds: int = 30
    half_open_max_calls: int = 1


@dataclass
class RequestMetrics:
    """Metrics for a single request."""
    method: str
    url: str
    status_code: Optional[int] = None
    duration_ms: float = 0.0
    retry_count: int = 0
    circuit_state: str = CircuitState.CLOSED.value
    error: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class HostMetrics:
    """Aggregated metrics per host."""
    host: str
    request_count: int = 0
    error_count: int = 0
    success_count: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    retry_count: int = 0
    circuit_state: str = CircuitState.CLOSED.value
    last_error_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None

    @property
    def avg_latency_ms(self) -> float:
        """Average latency across all requests."""
        if self.request_count == 0:
            return 0.0
        return self.total_latency_ms / self.request_count

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "host": self.host,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "success_count": self.success_count,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "min_latency_ms": round(self.min_latency_ms, 2) if self.min_latency_ms != float('inf') else 0,
            "max_latency_ms": round(self.max_latency_ms, 2),
            "retry_count": self.retry_count,
            "circuit_state": self.circuit_state,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
        }


# ============================================================================
# Circuit Breaker
# ============================================================================


class CircuitBreaker:
    """
    Per-host circuit breaker to prevent cascading failures.

    State transitions:
    - CLOSED: Normal operation, all requests allowed
    - OPEN: Too many failures detected, requests rejected immediately
    - HALF_OPEN: Cooldown expired, testing if destination recovered (1 probe allowed)
    """

    def __init__(self, host: str, config: CircuitBreakerConfig):
        self.host = host
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_timestamps: list = []
        self.success_count = 0
        self.opened_at: Optional[datetime] = None

    def can_execute(self) -> bool:
        """Check if a request can be sent."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if cooldown has elapsed
            if self.opened_at:
                elapsed = (datetime.now(timezone.utc) - self.opened_at).total_seconds()
                if elapsed >= self.config.cooldown_seconds:
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info(
                        f"Circuit breaker for {self.host} transitioned to HALF_OPEN "
                        f"after {elapsed:.1f}s cooldown"
                    )
                    return True
            return False

        # HALF_OPEN: allow exactly one probe
        if self.success_count == 0:
            return True
        return False

    def record_success(self) -> None:
        """Record a successful request."""
        self.failure_timestamps.clear()
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.opened_at = None
            logger.info(f"Circuit breaker for {self.host} returned to CLOSED after successful probe")

    def record_failure(self) -> None:
        """Record a failed request."""
        now = datetime.now(timezone.utc)
        self.failure_timestamps.append(now)

        # Remove failures outside the window
        cutoff = now - timedelta(seconds=self.config.failure_window_seconds)
        self.failure_timestamps = [ts for ts in self.failure_timestamps if ts > cutoff]

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.opened_at = now
            logger.warning(
                f"Circuit breaker for {self.host} reopened: probe failed, "
                f"returning to OPEN"
            )
            return

        # Check if we've exceeded failure threshold
        if len(self.failure_timestamps) >= self.config.failure_threshold:
            if self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                self.opened_at = now
                logger.warning(
                    f"Circuit breaker for {self.host} opened: {len(self.failure_timestamps)} "
                    f"failures in {self.config.failure_window_seconds}s window"
                )

    def get_state(self) -> str:
        """Get current circuit state."""
        return self.state.value


# ============================================================================
# Resilient HTTP Client
# ============================================================================


class ResilientHttpClient:
    """
    Shared resilient HTTP client with retry, circuit breaker, and pooling.

    Connection pooling:
    - Uses httpx.AsyncClient with persistent connection pool
    - max_connections: 100 (total concurrent connections)
    - max_keepalive: 20 (connections kept alive per host)
    - Lazy initialization on first request

    Retry logic:
    - Exponential backoff with jitter (±25%)
    - Retries: 429, 500, 502, 503, 504
    - No retry: 400, 401, 403, 404, 422
    - Respects Retry-After header from server

    Circuit breaker (per-host):
    - CLOSED → OPEN after 5 failures in 60s
    - OPEN → HALF_OPEN after 30s cooldown
    - HALF_OPEN → CLOSED on successful probe
    - HALF_OPEN → OPEN on failed probe

    Structured logging:
    - Logs all requests with method, URL, status, duration, retry count
    - Includes correlation IDs for request tracing
    - Sanitizes URLs (removes auth tokens)
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()

        # Per-host configuration
        self._host_configs: Dict[str, HostConfig] = {}

        # Per-host circuit breaker
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._cb_config = CircuitBreakerConfig()

        # Per-host metrics
        self._metrics: Dict[str, HostMetrics] = {}

        # Non-retryable status codes
        self._no_retry_statuses = {400, 401, 403, 404, 422}

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Lazily initialize the shared asyncio client."""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        limits=httpx.Limits(
                            max_connections=100,
                            max_keepalive_connections=20,
                        ),
                        timeout=httpx.Timeout(30.0),  # Default timeout
                    )
                    logger.debug("Shared HTTP client initialized with connection pooling")
        return self._client

    def _get_host(self, url: str) -> str:
        """Extract host from URL."""
        parsed = urlparse(url)
        return parsed.netloc or parsed.path.split('/')[0]

    def _get_host_config(self, host: str) -> HostConfig:
        """Get or create host-specific configuration."""
        if host not in self._host_configs:
            self._host_configs[host] = HostConfig(host=host)
        return self._host_configs[host]

    def _get_circuit_breaker(self, host: str) -> CircuitBreaker:
        """Get or create circuit breaker for a host."""
        if host not in self._circuit_breakers:
            self._circuit_breakers[host] = CircuitBreaker(host, self._cb_config)
        return self._circuit_breakers[host]

    def _get_metrics(self, host: str) -> HostMetrics:
        """Get or create metrics for a host."""
        if host not in self._metrics:
            self._metrics[host] = HostMetrics(host=host)
        return self._metrics[host]

    def configure_host(
        self,
        host: str,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
        write_timeout: float = 10.0,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_max: float = 30.0,
    ) -> None:
        """Configure retry and timeout parameters for a specific host."""
        self._host_configs[host] = HostConfig(
            host=host,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
        )
        logger.debug(f"Configured host {host}: timeouts=({connect_timeout}s, {read_timeout}s, {write_timeout}s), max_retries={max_retries}")

    def _sanitize_url(self, url: str) -> str:
        """Remove auth tokens and sensitive data from URL for logging."""
        try:
            parsed = urlparse(url)
            # Remove common auth params
            if parsed.query:
                query_parts = parsed.query.split('&')
                safe_parts = [
                    p for p in query_parts
                    if not any(k in p.lower() for k in ['token', 'key', 'auth', 'secret', 'password'])
                ]
                safe_query = '&'.join(safe_parts) if safe_parts else '[REDACTED]'
                return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{safe_query}"
        except Exception:
            pass
        return url

    def _calculate_backoff(
        self,
        attempt: int,
        backoff_base: float,
        backoff_max: float,
        retry_after: Optional[float] = None,
    ) -> float:
        """Calculate backoff delay with exponential growth and jitter."""
        if retry_after is not None:
            return retry_after

        # Exponential backoff: base * (2 ^ attempt)
        delay = backoff_base * (2 ** attempt)

        # Cap at max
        delay = min(delay, backoff_max)

        # Add jitter (±25%)
        jitter = delay * 0.25 * (2 * random.random() - 1)
        return max(0.1, delay + jitter)

    def _get_retry_after(self, response: httpx.Response) -> Optional[float]:
        """Extract Retry-After header from response."""
        retry_after = response.headers.get('Retry-After')
        if not retry_after:
            return None

        try:
            # Try parsing as seconds (integer)
            return float(retry_after)
        except ValueError:
            # Could be HTTP-date format; for simplicity, use default
            logger.debug(f"Could not parse Retry-After header: {retry_after}")
            return None

    async def _execute_with_retry(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[httpx.Timeout] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """Execute request with retry and circuit breaker logic."""
        host = self._get_host(url)
        host_config = self._get_host_config(host)
        circuit_breaker = self._get_circuit_breaker(host)
        metrics = self._get_metrics(host)

        correlation_id = correlation_id or str(uuid.uuid4())

        # Check circuit breaker
        if not circuit_breaker.can_execute():
            metrics.circuit_state = circuit_breaker.get_state()
            logger.warning(
                f"[{correlation_id}] Circuit breaker OPEN for {host}: "
                f"{method} {self._sanitize_url(url)}"
            )
            raise RuntimeError(f"Circuit breaker OPEN for {host}")

        # Prepare headers
        if headers is None:
            headers = {}
        headers['X-Correlation-ID'] = correlation_id

        # Create timeout
        if timeout is None:
            timeout = httpx.Timeout(
                timeout=host_config.read_timeout,
                connect=host_config.connect_timeout,
                write=host_config.write_timeout,
            )

        client = await self._ensure_client()

        retry_count = 0
        last_error = None

        for attempt in range(host_config.max_retries + 1):
            start_time = time.time()
            response = None
            error = None

            try:
                # Make request
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json,
                    data=data,
                    timeout=timeout,
                    follow_redirects=True,
                )

                duration_ms = (time.time() - start_time) * 1000

                # Update metrics
                metrics.request_count += 1
                metrics.total_latency_ms += duration_ms
                metrics.min_latency_ms = min(metrics.min_latency_ms, duration_ms)
                metrics.max_latency_ms = max(metrics.max_latency_ms, duration_ms)
                metrics.circuit_state = circuit_breaker.get_state()

                # Non-retryable error codes
                if response.status_code in self._no_retry_statuses:
                    logger.info(
                        f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                        f"→ {response.status_code} (no retry) [{duration_ms:.1f}ms]"
                    )
                    metrics.error_count += 1
                    metrics.last_error_at = datetime.now(timezone.utc)
                    circuit_breaker.record_failure()
                    return response

                # Successful response
                if response.status_code < 400:
                    logger.info(
                        f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                        f"→ {response.status_code} "
                        f"[{duration_ms:.1f}ms, retry_count={retry_count}]"
                    )
                    metrics.success_count += 1
                    metrics.last_success_at = datetime.now(timezone.utc)
                    circuit_breaker.record_success()
                    return response

                # Retryable error codes
                if response.status_code not in {429, 500, 502, 503, 504}:
                    # Not retryable
                    logger.info(
                        f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                        f"→ {response.status_code} (non-retryable) [{duration_ms:.1f}ms]"
                    )
                    metrics.error_count += 1
                    metrics.last_error_at = datetime.now(timezone.utc)
                    circuit_breaker.record_failure()
                    return response

                # Retryable error: check if we should retry
                if attempt >= host_config.max_retries:
                    logger.warning(
                        f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                        f"→ {response.status_code} after {attempt} retries [max_retries={host_config.max_retries}]"
                    )
                    metrics.error_count += 1
                    metrics.last_error_at = datetime.now(timezone.utc)
                    circuit_breaker.record_failure()
                    return response

                # Wait and retry
                retry_after = self._get_retry_after(response)
                backoff = self._calculate_backoff(
                    attempt,
                    host_config.backoff_base,
                    host_config.backoff_max,
                    retry_after,
                )
                retry_count += 1
                metrics.retry_count += 1

                logger.info(
                    f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                    f"→ {response.status_code} (retryable), waiting {backoff:.2f}s before retry {attempt + 1}"
                )

                await asyncio.sleep(backoff)

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                duration_ms = (time.time() - start_time) * 1000
                error = str(e)
                last_error = e

                # Check if we should retry
                if attempt >= host_config.max_retries:
                    logger.error(
                        f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                        f"raised {type(e).__name__} after {attempt} retries: {error}"
                    )
                    metrics.error_count += 1
                    metrics.last_error_at = datetime.now(timezone.utc)
                    circuit_breaker.record_failure()
                    raise

                retry_count += 1
                metrics.retry_count += 1
                backoff = self._calculate_backoff(
                    attempt,
                    host_config.backoff_base,
                    host_config.backoff_max,
                )

                logger.warning(
                    f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                    f"raised {type(e).__name__}, waiting {backoff:.2f}s before retry {attempt + 1}: {error}"
                )

                await asyncio.sleep(backoff)

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error = str(e)
                logger.error(
                    f"[{correlation_id}] {method} {self._sanitize_url(url)} "
                    f"raised {type(e).__name__}: {error}"
                )
                metrics.error_count += 1
                metrics.last_error_at = datetime.now(timezone.utc)
                circuit_breaker.record_failure()
                raise

        # Should not reach here
        if last_error:
            raise last_error
        raise RuntimeError(f"Request failed after {host_config.max_retries} retries")

    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[httpx.Timeout] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """GET request with retry and circuit breaker."""
        # Manually add params to URL (httpx doesn't accept params in our wrapper)
        if params:
            from urllib.parse import urlencode
            separator = '&' if '?' in url else '?'
            url = f"{url}{separator}{urlencode(params)}"

        return await self._execute_with_retry(
            method='GET',
            url=url,
            headers=headers,
            timeout=timeout,
            correlation_id=correlation_id,
        )

    async def post(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[httpx.Timeout] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """POST request with retry and circuit breaker."""
        return await self._execute_with_retry(
            method='POST',
            url=url,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
            correlation_id=correlation_id,
        )

    async def put(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        timeout: Optional[httpx.Timeout] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """PUT request with retry and circuit breaker."""
        return await self._execute_with_retry(
            method='PUT',
            url=url,
            headers=headers,
            json=json,
            timeout=timeout,
            correlation_id=correlation_id,
        )

    async def delete(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[httpx.Timeout] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """DELETE request with retry and circuit breaker."""
        return await self._execute_with_retry(
            method='DELETE',
            url=url,
            headers=headers,
            timeout=timeout,
            correlation_id=correlation_id,
        )

    def stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all tracked hosts."""
        return {
            host: metrics.to_dict()
            for host, metrics in self._metrics.items()
        }

    def stats_for_host(self, host: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific host."""
        if host in self._metrics:
            return self._metrics[host].to_dict()
        return None

    async def close(self) -> None:
        """Close the shared HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.debug("Shared HTTP client closed")


# ============================================================================
# Singleton instance and factory functions
# ============================================================================

_global_http_client: Optional[ResilientHttpClient] = None
_global_lock = asyncio.Lock()


async def get_http_client() -> ResilientHttpClient:
    """Get the global singleton HTTP client instance."""
    global _global_http_client

    if _global_http_client is None:
        async with _global_lock:
            if _global_http_client is None:
                _global_http_client = ResilientHttpClient()
                logger.debug("Created global HTTP client singleton")

    return _global_http_client


def get_tanda_client() -> ResilientHttpClient:
    """Get HTTP client pre-configured for Tanda API."""
    client = _get_or_create_client()
    client.configure_host(
        host="api.tanda.co",
        connect_timeout=5.0,
        read_timeout=60.0,
        write_timeout=10.0,
        max_retries=3,
        backoff_base=1.0,
        backoff_max=30.0,
    )
    return client


def get_stripe_client() -> ResilientHttpClient:
    """Get HTTP client pre-configured for Stripe API."""
    client = _get_or_create_client()
    client.configure_host(
        host="api.stripe.com",
        connect_timeout=5.0,
        read_timeout=30.0,
        write_timeout=10.0,
        max_retries=3,
        backoff_base=1.0,
        backoff_max=30.0,
    )
    return client


def get_xero_client() -> ResilientHttpClient:
    """Get HTTP client pre-configured for Xero API."""
    client = _get_or_create_client()
    client.configure_host(
        host="api.xero.com",
        connect_timeout=5.0,
        read_timeout=45.0,
        write_timeout=10.0,
        max_retries=3,
        backoff_base=1.0,
        backoff_max=30.0,
    )
    return client


def get_weather_client() -> ResilientHttpClient:
    """Get HTTP client pre-configured for BOM (weather) API."""
    client = _get_or_create_client()
    client.configure_host(
        host="api.bom.gov.au",
        connect_timeout=5.0,
        read_timeout=15.0,
        write_timeout=5.0,
        max_retries=2,
        backoff_base=0.5,
        backoff_max=10.0,
    )
    return client


def _get_or_create_client() -> ResilientHttpClient:
    """Get the global client instance synchronously (for sync contexts)."""
    global _global_http_client

    if _global_http_client is None:
        _global_http_client = ResilientHttpClient()
        logger.debug("Created global HTTP client singleton")

    return _global_http_client
