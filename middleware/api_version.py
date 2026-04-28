"""
API versioning middleware for RosterIQ.

Handles API version negotiation via:
1. URL prefix: /api/v1/venues → routes to /venues
2. Header: X-API-Version: 1
3. Default: v1 (current)

Supported versions: v1
Unsupported versions (v2+) are rejected with 400 status.

Usage:
    app.add_middleware(APIVersionMiddleware)
"""

import re
import logging
from typing import Optional
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# Supported API versions
SUPPORTED_VERSIONS = {"1": "v1"}
DEFAULT_VERSION = "1"


class APIVersionMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle API versioning.

    - Extracts version from URL prefix (/api/v{N}/), header (X-API-Version), or default
    - Strips /api/v{N} prefix from request path so handlers work unchanged
    - Rejects unsupported versions (v2+) with 400 error
    - Adds X-API-Version header to response
    """

    async def dispatch(self, request: Request, call_next):
        """Process request through versioning logic."""

        # Extract version from request
        version = self._extract_version(request)

        # Check if version is supported
        if version not in SUPPORTED_VERSIONS:
            logger.warning(
                f"Unsupported API version: {version} from {request.client.host}"
            )
            return JSONResponse(
                status_code=400,
                content={
                    "error": "API version not supported",
                    "requested_version": version,
                    "supported_versions": list(SUPPORTED_VERSIONS.keys()),
                },
                headers={"X-API-Version": DEFAULT_VERSION},
            )

        # Store version in request state for downstream access
        request.state.api_version = version

        # Strip /api/v{N} prefix if present, so handlers work unchanged
        original_path = request.url.path
        request._path = self._strip_version_prefix(original_path)

        logger.debug(
            f"API v{version} request: {request.method} {request.url.path}"
        )

        # Call next middleware/handler
        response = await call_next(request)

        # Add version header to response
        response.headers["X-API-Version"] = version

        return response

    def _extract_version(self, request: Request) -> str:
        """
        Extract API version from URL, header, or default.

        Priority:
        1. X-API-Version header (e.g., "1")
        2. /api/v{N} prefix in URL (e.g., "/api/v1/venues" → "1")
        3. Default: v1
        """

        # Check header first
        if "x-api-version" in request.headers:
            version_header = request.headers.get("x-api-version", "").strip()
            if version_header:
                return version_header

        # Check URL prefix: /api/v{N}/...
        path = request.url.path
        match = re.match(r"^/api/v(\d+)(/|$)", path)
        if match:
            return match.group(1)

        # Default to v1
        return DEFAULT_VERSION

    def _strip_version_prefix(self, path: str) -> str:
        """
        Strip /api/v{N} prefix from path.

        /api/v1/venues → /venues
        /api/v1/ → /
        /venues → /venues (no change if no prefix)
        """

        # Only strip if path has /api/v{N} prefix
        new_path = re.sub(r"^/api/v\d+", "", path)

        # Ensure path starts with /
        if not new_path:
            new_path = "/"

        return new_path
