"""
Rate limiting middleware for RosterIQ API.

Implements per-client token bucket rate limiting with different tiers:
- Unauthenticated: 60 req/min
- Authenticated (JWT): 200 req/min
- API key: 500 req/min

Clients are identified by IP address (from X-Forwarded-For or client.host).
Exempt paths: /health, /ready, /metrics (never rate limited).

Uses Redis for atomic counter increments across workers, falls back to in-memory.
"""

import asyncio
import time
import logging
import os
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Try to import Redis store
try:
    from rosteriq.services.redis_store import get_redis
    _redis_available = True
except ImportError:
    _redis_available = False
    get_redis = None


class TokenBucket:
    """Token bucket for rate limiting with burst capacity."""

    def __init__(self, capacity: float, refill_rate: float):
        """
        Initialize a token bucket.

        Args:
            capacity: Maximum tokens (burst capacity)
            refill_rate: Tokens per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume tokens. Returns True if allowed, False if rate limited.
        """
        self._refill()

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self):
        """Refill tokens based on time elapsed since last refill."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity,
            self.tokens + (elapsed * self.refill_rate)
        )
        self.last_refill = now

    def get_remaining(self) -> float:
        """Get current token count."""
        self._refill()
        return self.tokens

    def get_reset_time(self) -> float:
        """Get timestamp when bucket will be full."""
        if self.tokens >= self.capacity:
            return time.time()

        tokens_needed = self.capacity - self.tokens
        seconds_needed = tokens_needed / self.refill_rate
        return time.time() + seconds_needed


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware using token bucket algorithm.

    Per-client rate limits based on authentication tier:
    - Unauthenticated: 60 req/min = 1 req/sec, 60 burst
    - Authenticated: 200 req/min = 3.33 req/sec, 200 burst
    - API key: 500 req/min = 8.33 req/sec, 500 burst
    """

    # Exempt paths (never rate limited)
    EXEMPT_PATHS = {"/health", "/ready", "/metrics"}

    # Rate limits: (capacity, refill_rate_per_second)
    TIER_LIMITS = {
        "unauthenticated": (60, 1.0),           # 60 req/min
        "authenticated": (200, 3.333),          # ~200 req/min
        "api_key": (500, 8.333),                # ~500 req/min
    }

    def __init__(self, app):
        super().__init__(app)
        self.buckets: Dict[str, TokenBucket] = {}
        self.lock = asyncio.Lock()
        self.last_cleanup = time.time()
        self.cleanup_interval = 600  # 10 minutes

        # Try to initialize Redis store for distributed rate limiting
        self.redis_store = None
        self.using_redis = False

        if _redis_available and get_redis:
            try:
                redis_url = os.getenv("REDIS_URL")
                if redis_url:
                    self.redis_store = get_redis(redis_url)
                    self.using_redis = self.redis_store.using_redis
                    if self.using_redis:
                        logger.info("RateLimiterMiddleware initialized with Redis")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis in RateLimiterMiddleware: {e}")
                self.redis_store = None
                self.using_redis = False

    async def dispatch(self, request: Request, call_next) -> Response:
        """Apply rate limiting to request."""

        # Exempt paths bypass rate limiting
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Get client identifier and tier
        client_id = self._get_client_id(request)
        tier = self._get_client_tier(request)
        capacity, refill_rate = self.TIER_LIMITS[tier]

        # Use Redis atomic counter if available, otherwise use token bucket
        if self.using_redis and self.redis_store:
            return await self._dispatch_redis(
                request, call_next, client_id, tier, capacity, refill_rate
            )
        else:
            return await self._dispatch_local(
                request, call_next, client_id, tier
            )

    async def _dispatch_redis(
        self, request: Request, call_next, client_id: str, tier: str,
        capacity: float, refill_rate: float
    ) -> Response:
        """Rate limiting using Redis atomic counters."""
        # Window of 60 seconds for rate limiting
        window_seconds = 60
        counter_key = f"{client_id}:{tier}"

        try:
            # Atomic increment with Redis
            count = await self.redis_store.increment_counter(counter_key, window_seconds)

            if count > capacity:
                # Rate limited
                reset_time = time.time() + window_seconds
                retry_after = window_seconds

                response = JSONResponse(
                    {
                        "detail": "Rate limit exceeded",
                        "tier": tier,
                        "limit": int(capacity),
                        "retry_after": retry_after,
                    },
                    status_code=429,
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(int(capacity)),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(reset_time)),
                    },
                )
                return response

            # Request allowed, process it
            response = await call_next(request)

            # Add rate limit headers
            remaining = max(0, int(capacity - count))
            reset_time = time.time() + window_seconds

            response.headers["X-RateLimit-Limit"] = str(int(capacity))
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(int(reset_time))

            return response
        except Exception as e:
            logger.warning(f"Redis rate limiting failed: {e}. Falling back to local.")
            return await self._dispatch_local(request, call_next, client_id, tier)

    async def _dispatch_local(
        self, request: Request, call_next, client_id: str, tier: str
    ) -> Response:
        """Rate limiting using in-memory token buckets."""
        # Get or create bucket
        bucket = await self._get_bucket(client_id, tier)

        # Check rate limit
        if not bucket.consume():
            # Rate limited
            reset_time = bucket.get_reset_time()
            retry_after = max(1, int(reset_time - time.time()))

            response = JSONResponse(
                {
                    "detail": "Rate limit exceeded",
                    "tier": tier,
                    "limit": bucket.capacity,
                    "retry_after": retry_after,
                },
                status_code=429,
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(int(bucket.capacity)),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(reset_time)),
                },
            )
            return response

        # Request allowed, process it
        response = await call_next(request)

        # Add rate limit headers to response
        remaining = max(0, int(bucket.get_remaining()))
        reset_time = bucket.get_reset_time()

        response.headers["X-RateLimit-Limit"] = str(int(bucket.capacity))
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(reset_time))

        # Periodic cleanup of stale buckets
        await self._cleanup_stale_buckets()

        return response

    def _get_client_id(self, request: Request) -> str:
        """
        Client IP used as the rate-limit identity.

        Use request.client.host directly and do NOT parse X-Forwarded-For here.
        Whether forwarded headers are honoured is decided one layer down by
        uvicorn's proxy-headers handling, which only rewrites client.host from
        X-Forwarded-For when the immediate peer is in --forwarded-allow-ips
        (env: FORWARDED_ALLOW_IPS). Trusting the raw header in app code instead
        would let any caller spoof their identity and mint unlimited buckets,
        regardless of proxy configuration. See FORWARDED_ALLOW_IPS in
        .env.example for how to set this correctly per deployment.
        """
        if request.client:
            return request.client.host
        return "unknown"

    def _get_client_tier(self, request: Request) -> str:
        """Determine rate limit tier based on authentication."""
        # Check for API key first (highest tier)
        if request.headers.get("X-API-Key"):
            return "api_key"

        # Check for JWT token
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            return "authenticated"

        # Unauthenticated
        return "unauthenticated"

    async def _get_bucket(self, client_id: str, tier: str) -> TokenBucket:
        """Get or create token bucket for a (client, tier) pair."""
        # Key by tier as well as client, so an IP gets the correct limit for its
        # current auth tier instead of being pinned to whatever tier first
        # created its bucket (e.g. an authenticated user stuck on the lower
        # unauthenticated limit). Mirrors the Redis path's `{client_id}:{tier}`.
        bucket_key = f"{client_id}:{tier}"
        async with self.lock:
            if bucket_key not in self.buckets:
                capacity, refill_rate = self.TIER_LIMITS[tier]
                self.buckets[bucket_key] = TokenBucket(capacity, refill_rate)

            return self.buckets[bucket_key]

    async def _cleanup_stale_buckets(self):
        """Remove buckets for clients not seen in 10+ minutes."""
        now = time.time()

        if now - self.last_cleanup < self.cleanup_interval:
            return

        async with self.lock:
            stale_clients = []

            for client_id, bucket in self.buckets.items():
                # Consider a bucket stale if last refill was > 10 minutes ago
                age = now - bucket.last_refill
                if age > 600:  # 10 minutes
                    stale_clients.append(client_id)

            for client_id in stale_clients:
                del self.buckets[client_id]
                logger.debug(f"Cleaned up stale rate limit bucket: {client_id}")

            self.last_cleanup = now
