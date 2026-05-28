"""
Comprehensive security headers middleware for RosterIQ API.

Implements OWASP-recommended security headers including:
- Content-Security-Policy (CSP) with configurable directives
- Strict-Transport-Security (HSTS)
- X-Frame-Options (clickjacking protection)
- X-Content-Type-Options (MIME sniffing protection)
- Referrer-Policy (referrer control)
- Permissions-Policy (feature delegation)
- CORS handling with explicit origin allowlists
- Cache-Control headers
- X-Request-ID generation and tracking

Configuration varies by environment:
- Development: relaxed CSP for hot reload, no HSTS, localhost allowed
- Staging: stricter CSP, HSTS enabled, limited origins
- Production: maximum security, HSTS with preload, strict CSP

Usage:
    from rosteriq.middleware.security_headers import SecurityHeadersMiddleware, get_security_config
    from fastapi import FastAPI

    app = FastAPI()
    config = SecurityConfig(environment="production")
    app.add_middleware(SecurityHeadersMiddleware, config=config)
"""

import uuid
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    """Deployment environment."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class SecurityConfig:
    """
    Configuration for security headers middleware.

    Attributes:
        environment: Deployment environment affecting header strictness
        enable_hsts: Whether to include HSTS header (disabled in dev)
        hsts_max_age: HSTS max-age in seconds (default: 1 year)
        csp_report_uri: Optional CSP violation report endpoint
        allowed_origins: Explicit list of allowed origins (no wildcard in prod)
        additional_csp_directives: Override or extend CSP directives
        enable_cors_preflight_caching: Cache CORS preflight responses
        cors_max_age: CORS preflight cache duration in seconds
    """
    environment: Environment = Environment.PRODUCTION
    enable_hsts: bool = True
    hsts_max_age: int = 31536000  # 1 year
    csp_report_uri: Optional[str] = None
    allowed_origins: List[str] = field(default_factory=lambda: ["https://app.rosteriq.com"])
    additional_csp_directives: Dict[str, str] = field(default_factory=dict)
    enable_cors_preflight_caching: bool = True
    cors_max_age: int = 86400  # 24 hours

    def __post_init__(self):
        """Validate configuration based on environment."""
        # In development, disable HSTS and allow localhost
        if self.environment == Environment.DEVELOPMENT:
            self.enable_hsts = False
            if "http://localhost:3000" not in self.allowed_origins:
                self.allowed_origins.append("http://localhost:3000")
            if "http://localhost:5173" not in self.allowed_origins:
                self.allowed_origins.append("http://localhost:5173")
            if "http://127.0.0.1:3000" not in self.allowed_origins:
                self.allowed_origins.append("http://127.0.0.1:3000")

        # In staging and production, warn if wildcard is in origins
        if self.environment in (Environment.STAGING, Environment.PRODUCTION):
            if "*" in self.allowed_origins:
                logger.warning(
                    f"Wildcard origin found in {self.environment} - "
                    "this is a security risk and will be ignored in CORS handling"
                )


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add comprehensive security headers to all responses.

    Handles:
    - Content-Security-Policy with configurable directives
    - CORS with explicit origin allowlists
    - HSTS for HTTPS enforcement
    - Clickjacking, MIME sniffing, XSS protections
    - Referrer policy and feature permissions
    - Cache control and request ID tracking
    """

    # Base CSP directives (can be overridden per-environment)
    BASE_CSP_DIRECTIVES = {
        "default-src": "'self'",
        "script-src": "'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com",
        "style-src": "'self' 'unsafe-inline' https://fonts.googleapis.com",
        "img-src": "'self' data: blob:",
        "font-src": "'self' https://fonts.gstatic.com",
        "connect-src": "'self' wss: https://api.stripe.com",
        "frame-ancestors": "'none'",
        "form-action": "'self'",
        "base-uri": "'self'",
    }

    # Development CSP: more permissive for hot reload
    DEV_CSP_DIRECTIVES = {
        **BASE_CSP_DIRECTIVES,
        "script-src": "'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com",
        "connect-src": "'self' 'unsafe-inline' ws: wss: http://localhost:* http://127.0.0.1:*",
    }

    def __init__(self, app, config: Optional[SecurityConfig] = None):
        """
        Initialize security headers middleware.

        Args:
            app: FastAPI application instance
            config: SecurityConfig instance (creates default if None)
        """
        super().__init__(app)
        self.config = config or SecurityConfig()
        self.request_id_header = "X-Request-ID"
        self._build_csp_header()

    def _build_csp_header(self) -> str:
        """Build Content-Security-Policy header string."""
        # Start with environment-appropriate directives
        if self.config.environment == Environment.DEVELOPMENT:
            directives = self.DEV_CSP_DIRECTIVES.copy()
        else:
            directives = self.BASE_CSP_DIRECTIVES.copy()

        # Apply additional/override directives from config
        directives.update(self.config.additional_csp_directives)

        # Add CSP report-uri if configured
        if self.config.csp_report_uri:
            directives["report-uri"] = self.config.csp_report_uri

        # Build header value
        self.csp_header = "; ".join(f"{key} {value}" for key, value in directives.items())
        return self.csp_header

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Intercept request and response to add security headers.

        Args:
            request: Incoming HTTP request
            call_next: Callable to invoke next middleware/handler

        Returns:
            Response with security headers added
        """
        # Generate request ID if not present
        request_id = request.headers.get(self.request_id_header) or str(uuid.uuid4())
        request.state.request_id = request_id

        # Process request through the application
        response = await call_next(request)

        # Add security headers
        self._add_security_headers(response, request)

        return response

    def _add_security_headers(self, response: Response, request: Request) -> None:
        """
        Add all security headers to the response.

        Args:
            response: HTTP response to modify
            request: Original HTTP request
        """
        # Content-Security-Policy
        response.headers["Content-Security-Policy"] = self.csp_header

        # HTTP Strict-Transport-Security (HSTS)
        if self.config.enable_hsts:
            hsts_value = f"max-age={self.config.hsts_max_age}; includeSubDomains; preload"
            response.headers["Strict-Transport-Security"] = hsts_value

        # X-Frame-Options: prevent clickjacking
        response.headers["X-Frame-Options"] = "DENY"

        # X-Content-Type-Options: prevent MIME sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # X-XSS-Protection: deprecated but good for legacy browser support
        response.headers["X-XSS-Protection"] = "0"

        # Referrer-Policy: control referrer information
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Permissions-Policy: restrict browser features
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=(), payment=(self)"
        )

        # Cache-Control based on response type
        self._add_cache_control_header(response, request)

        # CORS headers if applicable
        self._add_cors_headers(response, request)

        # X-Request-ID for tracing
        response.headers[self.request_id_header] = request.state.request_id

    def _add_cache_control_header(self, response: Response, request: Request) -> None:
        """
        Add appropriate Cache-Control header based on response type.

        Args:
            response: HTTP response
            request: Original request
        """
        # Skip if Cache-Control already set
        if "Cache-Control" in response.headers:
            return

        # API responses: no caching
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        # Static assets and docs: can be cached
        elif request.url.path.startswith(("/static/", "/docs", "/redoc", "/openapi.json")):
            response.headers["Cache-Control"] = "public, max-age=3600, immutable"
        # Default: no caching
        else:
            response.headers["Cache-Control"] = "no-store"

    def _add_cors_headers(self, response: Response, request: Request) -> None:
        """
        Add CORS headers based on request origin and configuration.

        Args:
            response: HTTP response
            request: Original request
        """
        origin = request.headers.get("Origin")

        if not origin:
            return

        # Check if origin is in allowlist
        if origin in self.config.allowed_origins:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"

            # Add preflight caching header
            if self.config.enable_cors_preflight_caching:
                response.headers["Access-Control-Max-Age"] = str(self.config.cors_max_age)

        # For preflight requests, add allowed methods and headers
        if request.method == "OPTIONS":
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, PATCH, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = (
                "Content-Type, Authorization, X-API-Key, X-Request-ID"
            )

    def get_config_dict(self) -> dict:
        """
        Get current security configuration as dictionary.

        Returns:
            Dictionary representation of SecurityConfig
        """
        config_dict = asdict(self.config)
        config_dict["environment"] = self.config.environment.value
        return config_dict


def get_security_config(middleware: SecurityHeadersMiddleware) -> Dict:
    """
    Helper function to retrieve security configuration from middleware.

    Intended for use in admin endpoints to expose current security settings.

    Args:
        middleware: SecurityHeadersMiddleware instance

    Returns:
        Dictionary with current security configuration and CSP header
    """
    config = middleware.get_config_dict()
    config["csp_header"] = middleware.csp_header
    config["hsts_enabled"] = middleware.config.enable_hsts
    return config
