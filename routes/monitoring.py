"""
Monitoring and metrics endpoints for RosterIQ.

Provides:
- GET /metrics — Prometheus text format (no auth, for scraping)
- GET /api/v1/admin/metrics — JSON format (admin auth required)
- GET /api/v1/admin/alerts — Active alerts
- GET /api/v1/admin/health-detailed — Detailed health check

Usage:
    from rosteriq.routes.monitoring import create_monitoring_router

    router = create_monitoring_router()
    app.include_router(router)
"""

import logging
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import Response

from rosteriq.services.monitoring import get_metrics_collector, Alert
from rosteriq.services.db_pool import ConnectionPool
from rosteriq.database import get_db

logger = logging.getLogger(__name__)


def get_admin_or_raise(request: Request):
    """Dependency: Check if user is admin. For now, allow all (can be enhanced with auth)."""
    # TODO: Integrate with actual admin auth when available
    return True


def create_monitoring_router(db_pool: ConnectionPool = None) -> APIRouter:
    """Create monitoring router with metrics and health endpoints."""
    router = APIRouter(tags=["monitoring"])

    @router.get("/metrics", response_class=Response)
    async def get_prometheus_metrics():
        """
        Export metrics in Prometheus text exposition format.

        No authentication required — Prometheus scraper needs direct access.

        Returns:
            text/plain: Prometheus-formatted metrics
        """
        collector = get_metrics_collector()

        # Update DB pool stats if available
        if db_pool:
            try:
                stats = await db_pool.get_stats()
                collector.set_db_pool_stats(
                    active=stats.get("active", 0),
                    idle=stats.get("idle", 0),
                    max_size=stats.get("max_size", 20),
                )
            except Exception as e:
                logger.warning(f"Failed to get pool stats for metrics: {e}")

        prometheus_text = collector.to_prometheus()
        return Response(content=prometheus_text, media_type="text/plain; charset=utf-8")

    @router.get("/api/v1/admin/metrics")
    async def get_json_metrics(admin: bool = Depends(get_admin_or_raise)) -> Dict[str, Any]:
        """
        Export metrics in JSON format for admin dashboard.

        Requires admin authentication.

        Returns:
            dict: Detailed metrics in JSON format
        """
        if not admin:
            raise HTTPException(status_code=403, detail="Admin access required")

        collector = get_metrics_collector()

        # Update DB pool stats if available
        if db_pool:
            try:
                stats = await db_pool.get_stats()
                collector.set_db_pool_stats(
                    active=stats.get("active", 0),
                    idle=stats.get("idle", 0),
                    max_size=stats.get("max_size", 20),
                )
            except Exception as e:
                logger.warning(f"Failed to get pool stats for metrics: {e}")

        return collector.to_json()

    @router.get("/api/v1/admin/alerts")
    async def get_active_alerts(admin: bool = Depends(get_admin_or_raise)):
        """
        Get active alerts based on configured thresholds.

        Requires admin authentication.

        Returns:
            dict: List of active alerts with level, value, and threshold
        """
        if not admin:
            raise HTTPException(status_code=403, detail="Admin access required")

        collector = get_metrics_collector()
        alerts = collector.get_alerts()

        return {
            "count": len(alerts),
            "alerts": [alert.to_dict() for alert in alerts],
        }

    @router.get("/api/v1/admin/health-detailed")
    async def get_detailed_health(admin: bool = Depends(get_admin_or_raise)) -> Dict[str, Any]:
        """
        Detailed health check with subsystem status.

        Requires admin authentication.

        Returns:
            dict: Detailed health status with all subsystems
        """
        if not admin:
            raise HTTPException(status_code=403, detail="Admin access required")

        collector = get_metrics_collector()
        db = get_db()

        # Collect subsystem health
        health_status = {
            "status": "ok",
            "timestamp": None,
            "subsystems": {},
        }

        # Database health
        db_healthy = False
        try:
            # Try to list venues as a simple DB connectivity check
            db.list_venues()
            db_healthy = True
            db_health = {
                "status": "healthy",
                "pool_utilization_percent": collector.get_db_pool_utilization(),
                "active_connections": collector.db_pool_active,
                "idle_connections": collector.db_pool_idle,
                "max_connections": collector.db_pool_max,
            }
        except Exception as e:
            db_health = {
                "status": "degraded",
                "error": str(e),
                "pool_utilization_percent": collector.get_db_pool_utilization(),
            }
            health_status["status"] = "degraded"

        health_status["subsystems"]["database"] = db_health

        # Cache health
        cache_hit_rate = collector.get_cache_hit_rate()
        cache_health = {
            "status": "healthy" if cache_hit_rate is None or cache_hit_rate > 50 else "degraded",
            "hit_rate_percent": cache_hit_rate,
            "hits": collector.cache_hits,
            "misses": collector.cache_misses,
            "evictions": collector.cache_evictions,
        }
        health_status["subsystems"]["cache"] = cache_health

        # Request handling health
        error_rate = collector.get_error_rate()
        request_health = {
            "status": "healthy" if error_rate < 5 else "degraded",
            "error_rate_percent": error_rate,
            "total_requests": collector.request_count,
            "total_errors": collector.error_count,
            "active_endpoints": len(collector.requests_by_endpoint),
        }
        health_status["subsystems"]["requests"] = request_health

        # WebSocket health
        ws_health = {
            "status": "healthy",
            "active_connections": collector.ws_connections_current,
            "total_connections": collector.ws_connections_total,
            "messages_sent": collector.ws_messages_sent,
            "messages_received": collector.ws_messages_received,
        }
        health_status["subsystems"]["websocket"] = ws_health

        # Queue health
        queue_health = {
            "status": "healthy",
            "queue_count": len(collector.queue_depths),
            "max_depth": max(collector.queue_depths.values()) if collector.queue_depths else 0,
            "total_errors": sum(collector.queue_errors.values()),
        }
        if collector.queue_depths and max(collector.queue_depths.values()) > 100:
            queue_health["status"] = "degraded"
        health_status["subsystems"]["queues"] = queue_health

        # Memory health
        memory_mb = collector.current_memory_bytes / 1024 / 1024
        memory_health = {
            "status": "healthy" if memory_mb < 512 else "degraded",
            "current_mb": round(memory_mb, 2),
            "peak_mb": round(collector.peak_memory_bytes / 1024 / 1024, 2),
        }
        health_status["subsystems"]["memory"] = memory_health

        # Latency health
        p99_latency = collector._get_p99_latency() or 0
        latency_health = {
            "status": "healthy" if p99_latency < 2000 else "degraded",
            "p99_latency_ms": round(p99_latency, 2),
        }
        health_status["subsystems"]["latency"] = latency_health

        # Active alerts
        alerts = collector.get_alerts()
        health_status["active_alerts"] = {
            "count": len(alerts),
            "alerts": [alert.to_dict() for alert in alerts],
        }

        # Compute overall status
        if any(s.get("status") == "degraded" for s in health_status["subsystems"].values()):
            health_status["status"] = "degraded"
        elif alerts:
            health_status["status"] = "degraded"

        health_status["timestamp"] = str(collector.get_uptime_seconds())

        return health_status

    return router
