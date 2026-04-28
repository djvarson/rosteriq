"""
Metrics collection middleware for RosterIQ API.

Records HTTP request metrics (method, path, status code, duration) and
forwards them to the MetricsCollector for Prometheus export.

Middleware wraps every request except /metrics, /health, /ready.
"""

import time
import logging
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from rosteriq.services.monitoring import get_metrics_collector

logger = logging.getLogger(__name__)


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    HTTP middleware that records request metrics.

    Records:
    - Request method and path
    - Response status code
    - Request duration in milliseconds

    Metrics are forwarded to MetricsCollector for aggregation and export.
    """

    # Exempt paths (don't record metrics)
    EXEMPT_PATHS = {"/metrics", "/health", "/ready", "/docs", "/openapi.json", "/redoc"}

    def __init__(self, app):
        super().__init__(app)
        self.collector = get_metrics_collector()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Record metrics for incoming request."""

        # Skip exempt paths
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Record start time
        start_time = time.time()

        try:
            # Process request
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            # Record as 500 error if unhandled exception
            status_code = 500
            raise
        finally:
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000

            # Record metrics
            try:
                self.collector.record_request(
                    method=request.method,
                    path=request.url.path,
                    status_code=status_code,
                    duration_ms=duration_ms,
                )
            except Exception as e:
                logger.warning(f"Failed to record metrics: {e}")

        return response
