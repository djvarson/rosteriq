"""
Database health monitoring endpoints.

Provides REST endpoints for:
- Connection pool statistics
- Circuit breaker state
- Query performance profiling
- Slow query tracking
- Pool resizing (admin only)

Usage:
    from rosteriq.routes.db_health import db_health_router
    app.include_router(db_health_router)
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Will be injected by api.py
_pool: Optional[object] = None


class PoolStatsResponse(BaseModel):
    """Connection pool statistics."""
    active: int
    idle: int
    waiting: int
    total: int
    min_size: int
    max_size: int
    total_created: int
    total_closed: int
    utilization_pct: float


class CircuitBreakerStateResponse(BaseModel):
    """Circuit breaker state and statistics."""
    state: str
    failure_count: int
    success_count: int
    total_calls: int
    total_failures: int
    failure_rate: float
    last_failure_time: Optional[str]
    opened_at: Optional[str]


class QueryStats(BaseModel):
    """Query performance statistics."""
    count: int
    avg_ms: float
    median_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float


class QueryProfileResponse(BaseModel):
    """Query profiling statistics."""
    query_counts: dict
    response_times: dict
    slow_queries: list


class SlowQueryResponse(BaseModel):
    """Slow query record."""
    query: str
    duration_ms: float
    type: str
    timestamp: str


class PoolResizeRequest(BaseModel):
    """Request to resize connection pool."""
    min_size: int
    max_size: int


def create_db_health_router(pool: object) -> APIRouter:
    """Create router with injected pool instance."""
    global _pool
    _pool = pool

    router = APIRouter(prefix="/api/v1/admin/db", tags=["database-health"])

    @router.get("/pool-stats", response_model=PoolStatsResponse)
    async def get_pool_stats():
        """
        Get current connection pool statistics.

        Returns:
        - active: Number of connections in use
        - idle: Number of idle connections available
        - waiting: Number of requests waiting for a connection
        - total: Total connections (active + idle)
        - min_size, max_size: Pool configuration
        - total_created, total_closed: Historical counters
        - utilization_pct: Active connections as percentage of max_size
        """
        if not _pool:
            raise HTTPException(status_code=503, detail="Pool not initialized")

        try:
            stats = await _pool.get_stats()
            return PoolStatsResponse(**stats)
        except Exception as e:
            logger.error(f"Error fetching pool stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/circuit-breaker", response_model=CircuitBreakerStateResponse)
    async def get_circuit_breaker_state():
        """
        Get circuit breaker state and statistics.

        States:
        - closed: Normal operation (requests pass through)
        - open: Failing state (requests blocked)
        - half_open: Recovery testing (limited requests allowed)

        Returns:
        - state: Current circuit breaker state
        - failure_count: Consecutive failures in current state
        - success_count: Successes in current state
        - total_calls: Historical total calls
        - total_failures: Historical total failures
        - failure_rate: Total failures / total calls
        - last_failure_time: When last failure occurred
        - opened_at: When circuit transitioned to OPEN
        """
        if not _pool or not hasattr(_pool, '_circuit_breaker'):
            raise HTTPException(status_code=503, detail="Circuit breaker not available")

        try:
            state = _pool._circuit_breaker.get_state()
            return CircuitBreakerStateResponse(**state)
        except Exception as e:
            logger.error(f"Error fetching circuit breaker state: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/query-profile", response_model=QueryProfileResponse)
    async def get_query_profile():
        """
        Get query performance statistics.

        Returns metrics for each query type (SELECT, INSERT, UPDATE, DELETE):
        - count: Total queries of this type
        - avg_ms: Average execution time
        - median_ms: Median execution time
        - p50_ms, p95_ms, p99_ms: Percentiles
        - min_ms, max_ms: Min/max execution times

        Also includes list of recent slow queries (>100ms).
        """
        if not _pool or not hasattr(_pool, '_query_profiler'):
            raise HTTPException(status_code=503, detail="Query profiler not available")

        try:
            stats = await _pool._query_profiler.get_stats()
            return QueryProfileResponse(**stats)
        except Exception as e:
            logger.error(f"Error fetching query profile: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/slow-queries", response_model=list[SlowQueryResponse])
    async def get_slow_queries(
        limit: int = Query(10, ge=1, le=100, description="Max number of slow queries to return")
    ):
        """
        Get recent slow queries (>100ms).

        Parameters:
        - limit: Maximum number of queries to return (default 10, max 100)

        Returns recent slow queries with execution time and timestamp.
        """
        if not _pool or not hasattr(_pool, '_query_profiler'):
            raise HTTPException(status_code=503, detail="Query profiler not available")

        try:
            stats = await _pool._query_profiler.get_stats()
            slow_queries = stats.get("slow_queries", [])
            # Return most recent first
            return [SlowQueryResponse(**q) for q in slow_queries[-limit:]][::-1]
        except Exception as e:
            logger.error(f"Error fetching slow queries: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/pool/resize")
    async def resize_pool(request: PoolResizeRequest):
        """
        Resize connection pool (admin only).

        Parameters:
        - min_size: New minimum pool size (1-50)
        - max_size: New maximum pool size (min_size-100)

        Note: This is a placeholder. Actual implementation would require
        gradual connection migration or pool replacement.
        """
        if not _pool:
            raise HTTPException(status_code=503, detail="Pool not initialized")

        # Validation
        if request.min_size < 1 or request.min_size > 50:
            raise HTTPException(status_code=400, detail="min_size must be 1-50")
        if request.max_size < request.min_size or request.max_size > 100:
            raise HTTPException(status_code=400, detail="max_size must be >= min_size and <= 100")

        try:
            # Update pool configuration
            _pool.min_size = request.min_size
            _pool.max_size = request.max_size

            logger.info(f"Pool resized: min_size={request.min_size}, max_size={request.max_size}")

            return {
                "status": "resized",
                "min_size": request.min_size,
                "max_size": request.max_size,
                "message": "Pool configuration updated. Changes will take effect on next connection cycle.",
            }
        except Exception as e:
            logger.error(f"Error resizing pool: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router
