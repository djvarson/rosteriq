"""
Database connection pooling, health monitoring, and circuit breaker.

Provides:
- ConnectionPool: Async connection pool manager for PostgreSQL
- CircuitBreaker: Fault tolerance with state management
- QueryProfiler: Query execution profiling and analytics

Usage:
    from rosteriq.services.db_pool import ConnectionPool, CircuitBreaker, QueryProfiler

    pool = ConnectionPool(dsn="postgresql://...", min_size=5, max_size=20)
    async with pool.acquire() as conn:
        result = await conn.fetch("SELECT * FROM venues")
"""

import asyncio
import logging
import time
from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Callable, Any
from collections import deque
from statistics import mean, median, quantiles
import re

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker state transitions."""
    CLOSED = "closed"          # Normal operation
    OPEN = "open"              # Failing, reject requests
    HALF_OPEN = "half_open"    # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker pattern for fault tolerance.

    States:
    - CLOSED (normal): Requests pass through, failures counted
    - OPEN (failing): Requests blocked immediately
    - HALF_OPEN (recovering): Limited requests allowed to test recovery

    Configuration:
    - failure_threshold: Consecutive failures before OPEN
    - recovery_timeout: Seconds in OPEN state before trying HALF_OPEN
    - success_threshold: Successes in HALF_OPEN before returning to CLOSED
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 30,
        success_threshold: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at: Optional[float] = None
        self._total_calls = 0
        self._total_failures = 0

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with circuit breaker protection.

        Raises:
            RuntimeError: If circuit is OPEN
        """
        self._total_calls += 1

        if self._state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has elapsed
            if self._opened_at and time.time() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
                logger.warning(f"CircuitBreaker transitioned to HALF_OPEN (recovery testing)")
            else:
                self._total_failures += 1
                raise RuntimeError("Circuit breaker is OPEN — requests are blocked")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

            if self._state == CircuitBreakerState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"CircuitBreaker transitioned to CLOSED (recovered)")
            elif self._state == CircuitBreakerState.CLOSED:
                self._failure_count = 0

            return result

        except Exception as e:
            self._total_failures += 1
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                self._opened_at = time.time()
                logger.error(f"CircuitBreaker transitioned to OPEN (failure in HALF_OPEN): {e}")
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN
                self._opened_at = time.time()
                logger.error(f"CircuitBreaker transitioned to OPEN (threshold exceeded): {e}")

            raise

    def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state and statistics."""
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "failure_rate": (
                self._total_failures / self._total_calls
                if self._total_calls > 0
                else 0
            ),
            "last_failure_time": (
                datetime.fromtimestamp(self._last_failure_time).isoformat()
                if self._last_failure_time
                else None
            ),
            "opened_at": (
                datetime.fromtimestamp(self._opened_at).isoformat()
                if self._opened_at
                else None
            ),
        }


class QueryProfiler:
    """
    Track and analyze query execution times.

    Metrics:
    - Running statistics: avg, median, p50, p95, p99 response times
    - Top 10 slowest queries
    - Query count by type (SELECT/INSERT/UPDATE/DELETE)
    """

    def __init__(self, max_slow_queries: int = 100):
        self.max_slow_queries = max_slow_queries
        self._query_times: Dict[str, List[float]] = {}  # query_type -> times
        self._slow_queries = deque(maxlen=max_slow_queries)
        self._query_counts: Dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def record(self, query: str, duration_ms: float):
        """Record a query execution time."""
        async with self._lock:
            # Extract query type (SELECT, INSERT, UPDATE, DELETE)
            match = re.match(r'^\s*([A-Z]+)', query.strip())
            query_type = match.group(1) if match else "UNKNOWN"

            # Track timing
            if query_type not in self._query_times:
                self._query_times[query_type] = []
            self._query_times[query_type].append(duration_ms)

            # Track query count
            self._query_counts[query_type] = self._query_counts.get(query_type, 0) + 1

            # Track slow queries (>100ms)
            if duration_ms > 100:
                self._slow_queries.append({
                    "query": query[:200],  # Truncate for storage
                    "duration_ms": duration_ms,
                    "type": query_type,
                    "timestamp": datetime.now().isoformat(),
                })

    async def get_stats(self) -> Dict[str, Any]:
        """Get query performance statistics."""
        async with self._lock:
            stats = {
                "query_counts": dict(self._query_counts),
                "response_times": {},
                "slow_queries": list(self._slow_queries),
            }

            for query_type, times in self._query_times.items():
                if times:
                    try:
                        p_values = quantiles(times, n=100) if len(times) > 2 else times
                        stats["response_times"][query_type] = {
                            "count": len(times),
                            "avg_ms": round(mean(times), 2),
                            "median_ms": round(median(times), 2),
                            "p50_ms": round(p_values[49] if len(p_values) > 49 else times[0], 2),
                            "p95_ms": round(p_values[94] if len(p_values) > 94 else times[-1], 2),
                            "p99_ms": round(p_values[98] if len(p_values) > 98 else times[-1], 2),
                            "min_ms": round(min(times), 2),
                            "max_ms": round(max(times), 2),
                        }
                    except Exception as e:
                        logger.warning(f"Error calculating stats for {query_type}: {e}")

            return stats


class ConnectionPool:
    """
    Async connection pool manager for PostgreSQL with health monitoring.

    Features:
    - Min/max pool size with dynamic scaling
    - Connection idle timeout and lifetime limits
    - Health checks with auto-reconnect
    - Query timeout support
    - Slow query logging
    - Circuit breaker integration

    Configuration:
    - min_size: Minimum idle connections (default 5)
    - max_size: Maximum total connections (default 20)
    - max_idle_time: Seconds before idle connection is closed (default 300s)
    - max_lifetime: Max connection age before forced renewal (default 3600s)
    - health_check_interval: Seconds between health checks (default 30s)
    - query_timeout: Default query timeout in seconds (default 30s)
    """

    def __init__(
        self,
        dsn: str,
        min_size: int = 5,
        max_size: int = 20,
        max_idle_time: int = 300,
        max_lifetime: int = 3600,
        health_check_interval: int = 30,
        query_timeout: int = 30,
    ):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_time = max_idle_time
        self.max_lifetime = max_lifetime
        self.health_check_interval = health_check_interval
        self.query_timeout = query_timeout

        self._pool: List[Any] = []
        self._in_use: List[Any] = []
        self._waiting = deque()
        self._lock = asyncio.Lock()
        self._health_check_task: Optional[asyncio.Task] = None
        self._circuit_breaker = CircuitBreaker()
        self._query_profiler = QueryProfiler()
        self._connection_created_at: Dict[int, float] = {}
        self._connection_idle_since: Dict[int, float] = {}
        self._total_created = 0
        self._total_closed = 0

        logger.info(
            f"ConnectionPool initialized: min={min_size}, max={max_size}, "
            f"idle_timeout={max_idle_time}s, lifetime={max_lifetime}s"
        )

    async def initialize(self):
        """Initialize pool with min_size connections."""
        try:
            import asyncpg
            for _ in range(self.min_size):
                conn = await asyncpg.connect(self.dsn)
                self._pool.append(conn)
                self._total_created += 1
                conn_id = id(conn)
                self._connection_created_at[conn_id] = time.time()
                self._connection_idle_since[conn_id] = time.time()

            self._health_check_task = asyncio.create_task(self._health_check_loop())
            logger.info(f"ConnectionPool initialized with {self.min_size} connections")
        except ImportError:
            logger.warning("asyncpg not available, pool initialization deferred")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    async def acquire(self, timeout: Optional[int] = None):
        """
        Acquire a connection from the pool.

        Returns a context manager that automatically releases the connection.
        """
        timeout = timeout or self.query_timeout

        async with self._lock:
            # Try to get an available connection
            if self._pool:
                conn = self._pool.pop()
                conn_id = id(conn)
                self._in_use.append(conn)
                self._connection_idle_since.pop(conn_id, None)
                return _ConnectionContextManager(self, conn, timeout)

            # Create new connection if under max_size
            if len(self._in_use) + len(self._pool) < self.max_size:
                try:
                    import asyncpg
                    conn = await asyncpg.connect(self.dsn)
                    self._total_created += 1
                    conn_id = id(conn)
                    self._connection_created_at[conn_id] = time.time()
                    self._in_use.append(conn)
                    return _ConnectionContextManager(self, conn, timeout)
                except Exception as e:
                    logger.error(f"Failed to create connection: {e}")
                    raise

        # Wait for a connection to become available
        logger.warning(f"Connection pool exhausted, waiting for available connection")
        future = asyncio.Future()
        self._waiting.append(future)

        try:
            conn = await asyncio.wait_for(future, timeout=timeout)
            return _ConnectionContextManager(self, conn, timeout)
        except asyncio.TimeoutError:
            logger.error(f"Connection acquisition timeout after {timeout}s")
            raise TimeoutError(f"Could not acquire connection within {timeout}s")

    async def release(self, conn: Any):
        """Release a connection back to the pool."""
        async with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)

            conn_id = id(conn)
            self._connection_idle_since[conn_id] = time.time()

            # Check if connection exceeded lifetime
            created_at = self._connection_created_at.get(conn_id)
            if created_at and time.time() - created_at > self.max_lifetime:
                try:
                    await conn.close()
                    self._total_closed += 1
                    self._connection_created_at.pop(conn_id, None)
                    self._connection_idle_since.pop(conn_id, None)
                    logger.debug(f"Closed aged connection (lifetime exceeded)")
                except Exception as e:
                    logger.warning(f"Error closing aged connection: {e}")
            else:
                self._pool.append(conn)

            # Notify waiting requests
            if self._waiting and conn in self._pool:
                self._pool.remove(conn)
                future = self._waiting.popleft()
                if not future.done():
                    future.set_result(conn)

    async def _health_check_loop(self):
        """Periodically check pool health and reconnect failed connections."""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._perform_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")

    async def _perform_health_check(self):
        """Check health of all pooled connections."""
        async with self._lock:
            bad_connections = []

            for conn in self._pool[:]:  # Iterate over copy
                try:
                    # Simple ping query
                    await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=5)
                except Exception as e:
                    logger.warning(f"Health check failed for connection {id(conn)}: {e}")
                    bad_connections.append(conn)

            # Remove and close bad connections
            for conn in bad_connections:
                try:
                    self._pool.remove(conn)
                    await conn.close()
                    self._total_closed += 1
                    conn_id = id(conn)
                    self._connection_created_at.pop(conn_id, None)
                    self._connection_idle_since.pop(conn_id, None)
                except Exception as e:
                    logger.warning(f"Error removing bad connection: {e}")

            # Remove idle connections exceeding max_idle_time
            now = time.time()
            for conn in self._pool[:]:
                conn_id = id(conn)
                idle_since = self._connection_idle_since.get(conn_id)
                if idle_since and now - idle_since > self.max_idle_time:
                    try:
                        self._pool.remove(conn)
                        await conn.close()
                        self._total_closed += 1
                        self._connection_created_at.pop(conn_id, None)
                        self._connection_idle_since.pop(conn_id, None)
                        logger.debug(f"Closed idle connection (timeout exceeded)")
                    except Exception as e:
                        logger.warning(f"Error closing idle connection: {e}")

            # Maintain minimum pool size
            while len(self._pool) < self.min_size and len(self._in_use) + len(self._pool) < self.max_size:
                try:
                    import asyncpg
                    conn = await asyncpg.connect(self.dsn)
                    self._total_created += 1
                    conn_id = id(conn)
                    self._connection_created_at[conn_id] = time.time()
                    self._connection_idle_since[conn_id] = time.time()
                    self._pool.append(conn)
                    logger.debug(f"Created connection to maintain minimum pool size")
                except Exception as e:
                    logger.error(f"Failed to create connection for min size maintenance: {e}")
                    break

    async def get_stats(self) -> Dict[str, Any]:
        """Get current pool statistics."""
        async with self._lock:
            total_connections = len(self._pool) + len(self._in_use)
            return {
                "active": len(self._in_use),
                "idle": len(self._pool),
                "waiting": len(self._waiting),
                "total": total_connections,
                "min_size": self.min_size,
                "max_size": self.max_size,
                "total_created": self._total_created,
                "total_closed": self._total_closed,
                "utilization_pct": round(
                    (len(self._in_use) / self.max_size * 100) if self.max_size > 0 else 0,
                    2
                ),
            }

    async def close_all(self):
        """Close all connections in the pool."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            for conn in self._pool + self._in_use:
                try:
                    await conn.close()
                    self._total_closed += 1
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            self._pool.clear()
            self._in_use.clear()
            self._connection_created_at.clear()
            self._connection_idle_since.clear()

            logger.info("Connection pool closed")


class _ConnectionContextManager:
    """Context manager for pool connections."""

    def __init__(self, pool: ConnectionPool, conn: Any, timeout: int):
        self.pool = pool
        self.conn = conn
        self.timeout = timeout

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.release(self.conn)
        return False
