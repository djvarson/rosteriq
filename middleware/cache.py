"""
In-memory TTL caching layer for RosterIQ API.

Provides a thread-safe cache manager with per-entry TTL expiration,
background eviction of expired entries, and a cache-aside pattern decorator
for FastAPI route handlers.

Automatically uses Redis if available, falls back to in-memory cache.

Default caches:
  - venue_configs: 5 min TTL
  - employee_lists: 2 min TTL
  - forecast_data: 10 min TTL
  - roster_data: 1 min TTL
"""

import asyncio
import time
import logging
import os
from typing import Any, Optional, Callable, Dict, Awaitable
from functools import wraps
from datetime import datetime

from fastapi import Request

logger = logging.getLogger(__name__)

# Try to import Redis store
try:
    from rosteriq.services.redis_store import get_redis
    _redis_available = True
except ImportError:
    _redis_available = False
    get_redis = None


class TTLCache:
    """Dictionary-like cache with per-entry TTL expiration."""

    def __init__(self, default_ttl: float = 300):
        """
        Initialize TTL cache.

        Args:
            default_ttl: Default TTL in seconds for entries
        """
        self.default_ttl = default_ttl
        self.cache: Dict[str, tuple] = {}  # key -> (value, expiry_time)
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if it exists and hasn't expired."""
        if key not in self.cache:
            self.misses += 1
            return None

        value, expiry_time = self.cache[key]

        # Check if expired
        if time.time() >= expiry_time:
            del self.cache[key]
            self.evictions += 1
            self.misses += 1
            return None

        self.hits += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Store value in cache with optional custom TTL."""
        if ttl is None:
            ttl = self.default_ttl

        expiry_time = time.time() + ttl
        self.cache[key] = (value, expiry_time)

    def invalidate(self, key: str) -> bool:
        """Remove a specific entry. Returns True if it existed."""
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    def invalidate_all(self) -> None:
        """Clear all entries from cache."""
        count = len(self.cache)
        self.cache.clear()
        self.evictions += count
        logger.debug(f"Invalidated all cache entries ({count} removed)")

    def evict_expired(self) -> int:
        """Remove all expired entries. Returns count of evicted entries."""
        now = time.time()
        expired_keys = [
            key for key, (_, expiry_time) in self.cache.items()
            if now >= expiry_time
        ]

        for key in expired_keys:
            del self.cache[key]
            self.evictions += 1

        return len(expired_keys)

    def get_stats(self) -> dict:
        """Return cache statistics."""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests) if total_requests > 0 else 0

        return {
            "entries": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "hit_rate": round(hit_rate, 4),
            "total_requests": total_requests,
        }

    def __len__(self) -> int:
        """Return number of entries."""
        return len(self.cache)


class CacheManager:
    """Singleton managing multiple named TTL caches.

    Uses Redis if available, falls back to in-memory caches.
    """

    _instance: Optional['CacheManager'] = None
    _lock = asyncio.Lock()

    def __new__(cls) -> 'CacheManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize cache manager with default caches."""
        if self._initialized:
            return

        # Try to initialize Redis store
        self.redis_store = None
        self.using_redis = False

        if _redis_available and get_redis:
            try:
                redis_url = os.getenv("REDIS_URL")
                if redis_url:
                    self.redis_store = get_redis(redis_url)
                    self.using_redis = self.redis_store.using_redis
                    logger.info(
                        f"CacheManager initialized with Redis: {self.using_redis}"
                    )
            except Exception as e:
                logger.warning(f"Failed to initialize Redis in CacheManager: {e}")
                self.redis_store = None
                self.using_redis = False

        # Keep in-memory caches as fallback
        self.caches: Dict[str, TTLCache] = {
            "venue_configs": TTLCache(default_ttl=300),    # 5 min
            "employee_lists": TTLCache(default_ttl=120),   # 2 min
            "forecast_data": TTLCache(default_ttl=600),    # 10 min
            "roster_data": TTLCache(default_ttl=60),       # 1 min
        }
        self.lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._initialized = True

    async def start_cleanup_task(self) -> None:
        """Start background task to evict expired entries every 60 seconds."""
        if self._cleanup_task is not None:
            return

        async def cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(60)

                    async with self.lock:
                        total_evicted = 0
                        for cache_name, cache in self.caches.items():
                            evicted = cache.evict_expired()
                            if evicted > 0:
                                logger.debug(
                                    f"Cache '{cache_name}': evicted {evicted} expired entries"
                                )
                                total_evicted += evicted

                        if total_evicted > 0:
                            logger.debug(f"Total evicted across all caches: {total_evicted}")

                except asyncio.CancelledError:
                    logger.debug("Cache cleanup task cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in cache cleanup task: {e}")

        self._cleanup_task = asyncio.create_task(cleanup_loop())
        logger.info("Started background cache cleanup task")

    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.info("Stopped background cache cleanup task")

    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from named cache (Redis or in-memory)."""
        # Try Redis first if available
        if self.using_redis and self.redis_store:
            try:
                redis_key = f"{cache_name}:{key}"
                value = await self.redis_store.get(redis_key)
                if value is not None:
                    return value
            except Exception as e:
                logger.warning(f"Redis get failed for {redis_key}: {e}")

        # Fall back to in-memory cache
        cache = self._get_cache(cache_name)
        return cache.get(key)

    async def set(
        self,
        cache_name: str,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Store value in named cache (Redis and in-memory)."""
        # Use cache's default TTL if not specified
        if ttl is None:
            cache = self._get_cache(cache_name)
            ttl = cache.default_ttl

        # Store in Redis if available
        if self.using_redis and self.redis_store:
            try:
                redis_key = f"{cache_name}:{key}"
                await self.redis_store.set(redis_key, value, int(ttl))
            except Exception as e:
                logger.warning(f"Redis set failed for {redis_key}: {e}")

        # Also store in in-memory cache as local fallback
        cache = self._get_cache(cache_name)
        cache.set(key, value, ttl)

    async def invalidate(self, cache_name: str, key: str) -> bool:
        """Remove specific entry from named cache."""
        cache = self._get_cache(cache_name)
        return cache.invalidate(key)

    async def invalidate_all(self, cache_name: str) -> None:
        """Clear entire named cache."""
        cache = self._get_cache(cache_name)
        cache.invalidate_all()

    async def get_or_set(
        self,
        cache_name: str,
        key: str,
        factory_fn: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """
        Get value from cache, or call factory_fn to compute and cache it.

        Cache-aside (lazy loading) pattern.
        """
        cache = self._get_cache(cache_name)

        # Try to get from cache
        value = cache.get(key)
        if value is not None:
            return value

        # Cache miss: compute value
        value = factory_fn()
        cache.set(key, value, ttl)
        return value

    async def get_or_set_async(
        self,
        cache_name: str,
        key: str,
        factory_fn: Callable[[], Awaitable[Any]],
        ttl: Optional[float] = None,
    ) -> Any:
        """
        Async version of get_or_set.

        Get value from cache, or await factory_fn to compute and cache it.
        """
        cache = self._get_cache(cache_name)

        # Try to get from cache
        value = cache.get(key)
        if value is not None:
            return value

        # Cache miss: compute value
        value = await factory_fn()
        cache.set(key, value, ttl)
        return value

    def _get_cache(self, cache_name: str) -> TTLCache:
        """Get named cache, raise error if not found."""
        if cache_name not in self.caches:
            raise KeyError(f"Cache '{cache_name}' not found")
        return self.caches[cache_name]

    def get_stats(self) -> dict:
        """Return statistics for all caches."""
        stats = {
            cache_name: cache.get_stats()
            for cache_name, cache in self.caches.items()
        }

        # Add Redis stats if available
        if self.using_redis and self.redis_store:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                redis_info = loop.run_until_complete(self.redis_store.info())
                stats["redis"] = redis_info
            except Exception as e:
                logger.warning(f"Failed to get Redis stats: {e}")

        return stats


def get_cache_manager() -> CacheManager:
    """Get the singleton CacheManager instance."""
    return CacheManager()


def cache_response(
    cache_name: str,
    key_fn: Callable[[Request], str],
    ttl: Optional[float] = None,
):
    """
    Decorator for FastAPI route handlers to cache responses.

    Args:
        cache_name: Name of cache to use
        key_fn: Function that extracts cache key from request
        ttl: Custom TTL for cached value (optional)

    Example:
        @app.get("/venues")
        @cache_response("venue_configs", lambda req: "all_venues", ttl=300)
        async def list_venues(request: Request):
            ...
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request from args/kwargs
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if request is None and "request" in kwargs:
                request = kwargs["request"]

            if request is None:
                # No request object, skip caching
                return await func(*args, **kwargs)

            # Get cache key
            cache_key = key_fn(request)

            # Try to get from cache
            cache_manager = get_cache_manager()
            cached = await cache_manager.get(cache_name, cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {cache_name}:{cache_key}")
                return cached

            # Cache miss: call handler
            result = await func(*args, **kwargs)

            # Store in cache
            await cache_manager.set(cache_name, cache_key, result, ttl)
            logger.debug(f"Cached {cache_name}:{cache_key}")

            return result

        return wrapper

    return decorator
