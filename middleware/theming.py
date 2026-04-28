"""
Theming middleware for RosterIQ.

Injects theme CSS variables into HTML responses for server-side theming.
Intercepts responses to /dashboard, /staff, /admin and injects theme styles.

The middleware:
1. Checks if response is HTML
2. Extracts venue_id from JWT token in request
3. Generates theme CSS variables
4. Injects <style> before </head>
5. Caches themes in memory (5 min TTL)
"""

import logging
import time
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from starlette.datastructures import MutableHeaders

from rosteriq.services.theming import ThemeService

logger = logging.getLogger(__name__)


class ThemeInjectorMiddleware(BaseHTTPMiddleware):
    """
    Middleware that injects theme CSS variables into HTML responses.

    Routes intercepted: /dashboard, /staff, /admin
    """

    def __init__(self, app, theme_service: Optional[ThemeService] = None):
        super().__init__(app)
        self.theme_service = theme_service or ThemeService()
        self._theme_cache = {}  # {venue_id: (theme_css, timestamp)}
        self._cache_ttl = 300  # 5 minutes

    def _get_venue_id_from_request(self, request: Request) -> Optional[str]:
        """
        Extract venue_id from JWT token in request.

        Looks for:
        1. Authorization: Bearer <jwt>
        2. x-venue-id header
        3. venue_id query parameter
        """
        # Try Authorization header
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                # Decode JWT to extract venue_id
                # This is a simplified version; in production, use jwt.decode() with secret
                import jwt
                import os

                secret = os.getenv("JWT_SECRET", "dev-only")
                decoded = jwt.decode(token, secret, algorithms=["HS256"])
                venue_id = decoded.get("venue_id") or decoded.get("sub")
                if venue_id:
                    return venue_id
            except Exception as e:
                logger.debug(f"Failed to decode JWT: {e}")

        # Try headers
        venue_id = request.headers.get("x-venue-id")
        if venue_id:
            return venue_id

        # Try query params
        venue_id = request.query_params.get("venue_id")
        if venue_id:
            return venue_id

        return None

    def _should_inject(self, request: Request) -> bool:
        """Check if this request should have theme injected."""
        path = request.url.path
        return any(
            path.startswith(prefix)
            for prefix in ["/dashboard", "/staff", "/admin", "/settings"]
        )

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process request and inject theme if applicable.
        """
        # Check if this request should be themed
        if not self._should_inject(request):
            return await call_next(request)

        # Get venue ID
        venue_id = self._get_venue_id_from_request(request)
        if not venue_id:
            # No venue ID, skip theming
            return await call_next(request)

        # Process request
        response = await call_next(request)

        # Check if response is HTML
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            return response

        # Get or generate theme CSS
        css_content = self._get_cached_css(venue_id)
        if not css_content:
            try:
                css_content = self.theme_service.generate_css_variables(venue_id)
                self._cache_css(venue_id, css_content)
            except Exception as e:
                logger.warning(f"Failed to generate theme CSS for {venue_id}: {e}")
                return response

        # Inject CSS into response
        try:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Inject style before </head>
            body_str = body.decode("utf-8")
            style_tag = f"<style>{css_content}</style>"

            # Find </head> and inject before it
            if "</head>" in body_str:
                body_str = body_str.replace("</head>", f"{style_tag}\n</head>", 1)
            elif "<head>" in body_str:
                # If no closing </head>, inject after <head>
                body_str = body_str.replace("<head>", f"<head>\n{style_tag}", 1)
            else:
                # No head tag, prepend to body (fallback)
                body_str = f"{style_tag}\n{body_str}"

            body = body_str.encode("utf-8")

            # Create new response with modified body
            headers = MutableHeaders(response.headers)
            headers["Content-Length"] = str(len(body))

            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(headers),
                media_type=response.media_type,
            )

        except Exception as e:
            logger.error(f"Failed to inject theme CSS: {e}")
            return response

    def _get_cached_css(self, venue_id: str) -> Optional[str]:
        """Get cached theme CSS if available and not expired."""
        if venue_id not in self._theme_cache:
            return None

        css_content, timestamp = self._theme_cache[venue_id]
        if time.time() - timestamp > self._cache_ttl:
            # Cache expired
            del self._theme_cache[venue_id]
            return None

        return css_content

    def _cache_css(self, venue_id: str, css_content: str) -> None:
        """Cache theme CSS with timestamp."""
        self._theme_cache[venue_id] = (css_content, time.time())
