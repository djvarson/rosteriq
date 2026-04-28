"""
Abuse detection middleware for RosterIQ API.

Integrates the AbuseDetector service with FastAPI middleware.
Tracks per-IP and per-user request patterns and blocks abusive traffic.

Configuration via environment variables:
- ABUSE_DETECTION_ENABLED: Enable/disable abuse detection (default: True)
- ABUSE_BLOCK_COOLDOWN_MINUTES: Block duration (default: 15)
- ABUSE_CLEANUP_INTERVAL_SECONDS: Stale data cleanup interval (default: 300)
"""

import asyncio
import logging
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import os

from rosteriq.services.abuse_detection import AbuseDetector

logger = logging.getLogger(__name__)


class AbuseDetectionMiddleware(BaseHTTPMiddleware):
    """
    Middleware to detect and block abusive API usage.

    Runs after rate limiting to catch sophisticated attacks
    like credential stuffing, data scraping, and API key probing.
    """

    # Exempt paths (never checked for abuse)
    EXEMPT_PATHS = {"/health", "/ready", "/metrics", "/docs", "/redoc"}

    def __init__(self, app):
        super().__init__(app)

        # Load config from environment
        enabled = os.getenv("ABUSE_DETECTION_ENABLED", "true").lower() in ("true", "1", "yes")
        cooldown_minutes = int(os.getenv("ABUSE_BLOCK_COOLDOWN_MINUTES", "15"))
        cleanup_interval = int(os.getenv("ABUSE_CLEANUP_INTERVAL_SECONDS", "300"))

        if not enabled:
            self.detector = None
            logger.info("Abuse detection middleware disabled")
            return

        self.detector = AbuseDetector(
            auto_block_cooldown_minutes=cooldown_minutes,
            cleanup_interval_seconds=cleanup_interval,
        )
        logger.info(
            f"Abuse detection middleware enabled (cooldown={cooldown_minutes}min, "
            f"cleanup={cleanup_interval}s)"
        )

    async def dispatch(self, request: Request, call_next) -> any:
        """Apply abuse detection to request."""

        # Skip if disabled
        if not self.detector:
            return await call_next(request)

        # Skip exempt paths
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Get client IP and user ID
        client_ip = self._get_client_id(request)
        user_id = self._get_user_id(request)

        # Record request after processing (to get status code)
        response = await call_next(request)

        # Determine if request failed (4xx/5xx status)
        is_failed = response.status_code >= 400

        # Record the request
        await self.detector.record_request(
            ip=client_ip,
            user_id=user_id,
            endpoint=request.url.path,
            method=request.method,
            status_code=response.status_code,
            is_failed=is_failed,
        )

        # Check for abuse patterns
        abuse_check = await self.detector.check_abuse(client_ip, user_id)

        # Take action based on abuse check result
        if abuse_check.action == "block":
            # Return 429 (Too Many Requests) for blocked IPs
            return JSONResponse(
                {
                    "detail": "Blocked due to abuse detection",
                    "reason": abuse_check.reason,
                    "blocked_until": (
                        abuse_check.blocked_until.isoformat()
                        if abuse_check.blocked_until
                        else None
                    ),
                },
                status_code=429,
                headers={
                    "Retry-After": "3600",  # Suggest retrying in 1 hour
                    "X-Abuse-Detection": "blocked",
                },
            )

        elif abuse_check.action == "throttle":
            # Add warning header but allow request
            response.headers["X-Abuse-Detection"] = "throttle"
            response.headers["X-Abuse-Reason"] = abuse_check.reason or ""

        elif abuse_check.action == "warn":
            # Add warning header
            response.headers["X-Abuse-Detection"] = "warn"
            response.headers["X-Abuse-Reason"] = abuse_check.reason or ""

        # Allow request (normal or with warning/throttle headers)
        return response

    def _get_client_id(self, request: Request) -> str:
        """Extract client IP from request (with X-Forwarded-For support)."""
        # Check for proxy headers first (common in containerized deployments)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # X-Forwarded-For can contain multiple IPs, take the first
            return forwarded_for.split(",")[0].strip()

        # Fall back to direct client connection
        if request.client:
            return request.client.host

        return "unknown"

    def _get_user_id(self, request: Request) -> Optional[str]:
        """Extract user ID from request context (if available)."""
        # Try to get from request state (set by auth middleware)
        if hasattr(request.state, "user_id"):
            return request.state.user_id

        if hasattr(request.state, "user") and hasattr(request.state.user, "id"):
            return request.state.user.id

        # Try to extract from JWT token
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            # In production, decode JWT and extract user_id
            # For now, just use a placeholder
            return "authenticated"

        return None

    def get_detector(self) -> Optional[AbuseDetector]:
        """Get the abuse detector instance (for testing and admin endpoints)."""
        return self.detector


# Global instance for access from route handlers
_abuse_detector_middleware: Optional[AbuseDetectionMiddleware] = None


def get_abuse_detector() -> Optional[AbuseDetector]:
    """Get the abuse detector instance from middleware."""
    if _abuse_detector_middleware:
        return _abuse_detector_middleware.get_detector()
    return None
