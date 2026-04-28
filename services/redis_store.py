"""
Redis-backed cache, session store, and pub/sub for RosterIQ.

Provides a unified interface for distributed caching, session management,
rate limiting counters, and WebSocket message broadcasting across worker processes.

Includes graceful fallback to in-memory storage when Redis is unavailable,
enabling development and testing without a Redis server.
"""

import json
import logging
import asyncio
import time
from typing import Any, Optional, Dict, AsyncGenerator
from collections import defaultdict
import redis
from redis.connection import ConnectionPool

logger = logging.getLogger(__name__)


class InMemoryFallback:
    """In-memory cache fallback when Redis is unavailable."""

    def __init__(self):
        """Initialize in-memory storage."""
        self.data: Dict[str, tuple] = {}  # key -> (value, expiry_time)
        self.hits = 0
        self.misses = 0
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from in-memory store."""
        async with self.lock:
            if key not in self.data:
                self.misses += 1
                return None

            value, expiry_time = self.data[key]

            # Check if expired
            if time.time() >= expiry_time:
                del self.data[key]
                self.misses += 1
                return None

            self.hits += 1
            return value

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value with TTL in in-memory store."""
        async with self.lock:
            expiry_time = time.time() + ttl
            self.data[key] = (value, expiry_time)

    async def delete(self, key: str) -> None:
        """Delete key from in-memory store."""
        async with self.lock:
            self.data.pop(key, None)

    async def get_many(self, keys: list) -> dict:
        """Get multiple keys from in-memory store."""
        async with self.lock:
            result = {}
            now = time.time()
            for key in keys:
                if key in self.data:
                    value, expiry_time = self.data[key]
                    if now < expiry_time:
                        result[key] = value
            return result

    async def invalidate_pattern(self, pattern: str) -> int:
        """Delete keys matching glob pattern."""
        import fnmatch

        async with self.lock:
            matching_keys = [
                k for k in self.data.keys()
                if fnmatch.fnmatch(k, pattern)
            ]
            for key in matching_keys:
                del self.data[key]
            return len(matching_keys)

    async def increment_counter(self, key: str, window_seconds: int) -> int:
        """Increment counter with window expiration."""
        async with self.lock:
            if key not in self.data:
                self.data[key] = (1, time.time() + window_seconds)
                return 1

            value, expiry_time = self.data[key]

            # Check if expired
            if time.time() >= expiry_time:
                self.data[key] = (1, time.time() + window_seconds)
                return 1

            # Increment and update
            new_value = value + 1
            self.data[key] = (new_value, expiry_time)
            return new_value

    async def get_counter(self, key: str) -> int:
        """Get counter value."""
        async with self.lock:
            if key not in self.data:
                return 0

            value, expiry_time = self.data[key]
            if time.time() >= expiry_time:
                del self.data[key]
                return 0

            return value

    async def ping(self) -> bool:
        """Ping in-memory store (always succeeds)."""
        return True

    async def info(self) -> dict:
        """Get in-memory store info."""
        async with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests) if total_requests > 0 else 0.0

            return {
                "mode": "in_memory",
                "entries": len(self.data),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(hit_rate, 4),
                "total_requests": total_requests,
            }

    async def publish(self, channel: str, message: dict) -> int:
        """Publish message (no-op for in-memory fallback)."""
        logger.warning(
            f"Publishing to channel '{channel}' with in-memory fallback (subscribers: 0)"
        )
        return 0

    async def subscribe(self, channel: str) -> AsyncGenerator:
        """Subscribe to channel (in-memory fallback always empty)."""
        # For in-memory, this just yields nothing
        return
        yield  # Never reached, but makes this an async generator


class RedisStore:
    """Redis-backed distributed cache and session store."""

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize Redis store with connection pooling.

        Args:
            redis_url: Redis connection URL (e.g., redis://localhost:6379/0)
                      If None or connection fails, falls back to in-memory storage.
        """
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.fallback: Optional[InMemoryFallback] = None
        self.using_redis = False
        self.using_fallback = False

        # Try to connect to Redis
        if redis_url:
            try:
                pool = ConnectionPool.from_url(
                    redis_url,
                    max_connections=10,
                    socket_keepalive=True,
                    socket_keepalive_options={1: 1},  # TCP_KEEPIDLE
                    decode_responses=True,
                )
                self.redis = redis.Redis(connection_pool=pool)
                # Test connection
                self.redis.ping()
                self.using_redis = True
                logger.info(f"Redis connected: {redis_url}")
            except Exception as e:
                logger.warning(
                    f"Failed to connect to Redis ({redis_url}): {e}. "
                    "Falling back to in-memory cache."
                )
                self.redis = None
                self.fallback = InMemoryFallback()
                self.using_fallback = True
        else:
            logger.info("No REDIS_URL configured. Using in-memory cache.")
            self.fallback = InMemoryFallback()
            self.using_fallback = True

    # =========================================================================
    # Cache Operations
    # =========================================================================

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache, JSON-deserialized."""
        if self.using_fallback:
            return await self.fallback.get(key)

        try:
            value = self.redis.get(key)
            if value is None:
                return None
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis get failed for {key}: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value in cache with TTL, JSON-serialized."""
        if self.using_fallback:
            await self.fallback.set(key, value, ttl)
            return

        try:
            serialized = json.dumps(value)
            self.redis.setex(key, ttl, serialized)
        except Exception as e:
            logger.error(f"Redis set failed for {key}: {e}")

    async def delete(self, key: str) -> None:
        """Delete key from cache."""
        if self.using_fallback:
            await self.fallback.delete(key)
            return

        try:
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Redis delete failed for {key}: {e}")

    async def get_many(self, keys: list[str]) -> dict:
        """Get multiple keys from cache."""
        if self.using_fallback:
            return await self.fallback.get_many(keys)

        try:
            values = self.redis.mget(keys)
            result = {}
            for key, value in zip(keys, values):
                if value is not None:
                    result[key] = json.loads(value)
            return result
        except Exception as e:
            logger.error(f"Redis mget failed: {e}")
            return {}

    async def invalidate_pattern(self, pattern: str) -> int:
        """Delete keys matching glob pattern."""
        if self.using_fallback:
            return await self.fallback.invalidate_pattern(pattern)

        try:
            # Redis SCAN with pattern matching
            keys = []
            cursor = 0
            while True:
                cursor, batch = self.redis.scan(cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break

            if keys:
                self.redis.delete(*keys)
            return len(keys)
        except Exception as e:
            logger.error(f"Redis pattern invalidation failed: {e}")
            return 0

    async def cache_stats(self) -> dict:
        """Get cache statistics."""
        if self.using_fallback:
            return await self.fallback.info()

        try:
            info = self.redis.info("stats")
            return {
                "mode": "redis",
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "used_memory": info.get("used_memory", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
                "total_connections_received": info.get("total_connections_received", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
            }
        except Exception as e:
            logger.error(f"Redis info failed: {e}")
            return {"mode": "redis", "error": str(e)}

    # =========================================================================
    # Session Operations
    # =========================================================================

    async def store_session(
        self, session_id: str, data: dict, ttl: int = 3600
    ) -> None:
        """Store session with TTL."""
        await self.set(f"session:{session_id}", data, ttl)

    async def get_session(self, session_id: str) -> Optional[dict]:
        """Retrieve session data."""
        return await self.get(f"session:{session_id}")

    async def delete_session(self, session_id: str) -> None:
        """Delete session."""
        await self.delete(f"session:{session_id}")

    async def extend_session(self, session_id: str, ttl: int = 3600) -> bool:
        """Extend session TTL. Returns True if session existed."""
        if self.using_fallback:
            data = await self.fallback.get(f"session:{session_id}")
            if data is not None:
                await self.fallback.set(f"session:{session_id}", data, ttl)
                return True
            return False

        try:
            return bool(self.redis.expire(f"session:{session_id}", ttl))
        except Exception as e:
            logger.error(f"Redis extend_session failed: {e}")
            return False

    # =========================================================================
    # Rate Limiter Operations (Atomic)
    # =========================================================================

    async def increment_counter(self, key: str, window_seconds: int) -> int:
        """Increment counter atomically with window expiration."""
        if self.using_fallback:
            return await self.fallback.increment_counter(key, window_seconds)

        try:
            counter_key = f"counter:{key}"
            pipe = self.redis.pipeline()
            pipe.incr(counter_key)
            pipe.expire(counter_key, window_seconds)
            results = pipe.execute()
            return results[0]
        except Exception as e:
            logger.error(f"Redis increment_counter failed: {e}")
            return 0

    async def get_counter(self, key: str) -> int:
        """Get counter value."""
        if self.using_fallback:
            return await self.fallback.get_counter(key)

        try:
            value = self.redis.get(f"counter:{key}")
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"Redis get_counter failed: {e}")
            return 0

    # =========================================================================
    # Pub/Sub for WebSocket Fan-out
    # =========================================================================

    async def publish(self, channel: str, message: dict) -> int:
        """Publish message to channel."""
        if self.using_fallback:
            return await self.fallback.publish(channel, message)

        try:
            serialized = json.dumps(message)
            return self.redis.publish(f"channel:{channel}", serialized)
        except Exception as e:
            logger.error(f"Redis publish failed: {e}")
            return 0

    async def subscribe(self, channel: str) -> AsyncGenerator:
        """Subscribe to channel and yield messages."""
        if self.using_fallback:
            async for msg in self.fallback.subscribe(channel):
                yield msg
            return

        pubsub = None
        try:
            pubsub = self.redis.pubsub()
            pubsub.subscribe(f"channel:{channel}")

            for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        yield json.loads(message["data"])
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode message: {message['data']}")
        except Exception as e:
            logger.error(f"Redis subscribe failed: {e}")
        finally:
            if pubsub:
                pubsub.close()

    # =========================================================================
    # Health Checks
    # =========================================================================

    async def ping(self) -> bool:
        """Ping Redis (or in-memory fallback)."""
        if self.using_fallback:
            return await self.fallback.ping()

        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False

    async def info(self) -> dict:
        """Get Redis info (or fallback info)."""
        if self.using_fallback:
            return await self.fallback.info()

        try:
            server_info = self.redis.info("server")
            memory_info = self.redis.info("memory")
            stats_info = self.redis.info("stats")

            return {
                "mode": "redis",
                "redis_version": server_info.get("redis_version", "unknown"),
                "uptime_in_seconds": server_info.get("uptime_in_seconds", 0),
                "connected_clients": stats_info.get("connected_clients", 0),
                "used_memory": memory_info.get("used_memory", 0),
                "used_memory_human": memory_info.get("used_memory_human", "0B"),
                "used_memory_peak_human": memory_info.get("used_memory_peak_human", "0B"),
                "total_connections_received": stats_info.get(
                    "total_connections_received", 0
                ),
                "total_commands_processed": stats_info.get(
                    "total_commands_processed", 0
                ),
            }
        except Exception as e:
            logger.error(f"Redis info failed: {e}")
            return {"mode": "redis", "error": str(e)}


# Global singleton instance
_redis_store: Optional[RedisStore] = None


def get_redis(redis_url: Optional[str] = None) -> RedisStore:
    """Get or initialize the global Redis store singleton."""
    global _redis_store

    if _redis_store is None:
        _redis_store = RedisStore(redis_url)

    return _redis_store


def reset_redis() -> None:
    """Reset the global Redis store (for testing)."""
    global _redis_store
    _redis_store = None
