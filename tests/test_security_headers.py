"""
Unit tests for security headers middleware.

Tests:
- Header presence and correctness
- Environment-specific configuration
- CORS origin validation
- CSP directive configuration
- HSTS enforcement
"""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime

# Note: These tests require fastapi and starlette to be installed
# Run with: pytest RosterIQ/tests/test_security_headers.py

try:
    from fastapi import FastAPI, Request
    from starlette.testclient import TestClient
    from starlette.responses import Response

    from rosteriq.middleware.security_headers import (
        SecurityHeadersMiddleware,
        SecurityConfig,
        Environment,
        get_security_config,
    )
    from rosteriq.routes.security import create_security_router

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not installed")
class TestSecurityHeadersMiddleware:
    """Test security headers middleware."""

    def test_production_config(self):
        """Test production environment configuration."""
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            allowed_origins=["https://app.rosteriq.com"],
        )

        assert config.environment == Environment.PRODUCTION
        assert config.enable_hsts is True
        assert "http://localhost" not in config.allowed_origins

    def test_development_config(self):
        """Test development environment configuration."""
        config = SecurityConfig(
            environment=Environment.DEVELOPMENT,
        )

        assert config.environment == Environment.DEVELOPMENT
        assert config.enable_hsts is False
        assert any("localhost" in origin for origin in config.allowed_origins)

    def test_middleware_initialization(self):
        """Test middleware initialization with config."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)

        middleware = SecurityHeadersMiddleware(app, config=config)

        assert middleware.config == config
        assert middleware.config.environment == Environment.PRODUCTION

    def test_csp_header_building(self):
        """Test CSP header construction."""
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            csp_report_uri="/api/v1/admin/security/csp-report",
        )
        middleware = SecurityHeadersMiddleware(Mock(), config=config)

        assert "default-src 'self'" in middleware.csp_header
        assert "script-src" in middleware.csp_header
        assert "report-uri /api/v1/admin/security/csp-report" in middleware.csp_header

    def test_csp_header_production_vs_development(self):
        """Test CSP differs between environments."""
        prod_config = SecurityConfig(environment=Environment.PRODUCTION)
        prod_middleware = SecurityHeadersMiddleware(Mock(), config=prod_config)

        dev_config = SecurityConfig(environment=Environment.DEVELOPMENT)
        dev_middleware = SecurityHeadersMiddleware(Mock(), config=dev_config)

        # Development should have unsafe-eval for hot reload
        assert "'unsafe-eval'" in dev_middleware.csp_header
        assert "'unsafe-eval'" not in prod_middleware.csp_header

    def test_default_config(self):
        """Test default SecurityConfig values."""
        config = SecurityConfig()

        assert config.environment == Environment.PRODUCTION
        assert config.enable_hsts is True
        assert config.hsts_max_age == 31536000  # 1 year
        assert config.cors_max_age == 86400  # 24 hours

    def test_additional_csp_directives(self):
        """Test overriding CSP directives."""
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            additional_csp_directives={
                "img-src": "'self' https://cdn.example.com",
                "custom-directive": "'none'",
            },
        )
        middleware = SecurityHeadersMiddleware(Mock(), config=config)

        assert "https://cdn.example.com" in middleware.csp_header
        assert "custom-directive 'none'" in middleware.csp_header

    def test_get_security_config_helper(self):
        """Test security config helper function."""
        app = FastAPI()
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            csp_report_uri="/api/v1/admin/security/csp-report",
        )
        middleware = SecurityHeadersMiddleware(app, config=config)

        result = get_security_config(middleware)

        assert result["environment"] == "production"
        assert result["hsts_enabled"] is True
        assert "csp_header" in result
        assert "default-src" in result["csp_header"]

    def test_security_router_creation(self):
        """Test security router creation."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)
        middleware = SecurityHeadersMiddleware(app, config=config)

        router = create_security_router(middleware)

        assert router is not None
        # Check that routes are registered (through mock inspection)
        assert hasattr(router, 'routes')


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not installed")
class TestSecurityHeadersIntegration:
    """Integration tests with FastAPI."""

    def test_middleware_adds_headers_to_response(self):
        """Test that middleware adds security headers to all responses."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/test")

        # Check critical headers are present
        assert "Content-Security-Policy" in response.headers
        assert "X-Frame-Options" in response.headers
        assert "X-Content-Type-Options" in response.headers
        assert "Strict-Transport-Security" in response.headers
        assert "X-XSS-Protection" in response.headers
        assert "Referrer-Policy" in response.headers
        assert "Permissions-Policy" in response.headers

    def test_hsts_header_production(self):
        """Test HSTS header in production."""
        app = FastAPI()
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            hsts_max_age=31536000,
        )
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/test")

        hsts = response.headers.get("Strict-Transport-Security")
        assert hsts is not None
        assert "max-age=31536000" in hsts
        assert "includeSubDomains" in hsts
        assert "preload" in hsts

    def test_hsts_header_development(self):
        """Test HSTS disabled in development."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.DEVELOPMENT)
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/test")

        # HSTS should not be present in development
        assert "Strict-Transport-Security" not in response.headers

    def test_cache_control_api_response(self):
        """Test cache control for API endpoints."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/api/v1/test")
        async def api_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/api/v1/test")

        cache_control = response.headers.get("Cache-Control")
        assert "no-store" in cache_control
        assert "no-cache" in cache_control

    def test_cache_control_static_response(self):
        """Test cache control for static assets."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/static/app.js")
        async def static_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/static/app.js")

        cache_control = response.headers.get("Cache-Control")
        assert "public" in cache_control
        assert "max-age=3600" in cache_control

    def test_x_request_id_header(self):
        """Test X-Request-ID header generation."""
        app = FastAPI()
        config = SecurityConfig(environment=Environment.PRODUCTION)
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get("/test")

        request_id = response.headers.get("X-Request-ID")
        assert request_id is not None
        assert len(request_id) > 0

    def test_cors_headers_allowed_origin(self):
        """Test CORS headers for allowed origin."""
        app = FastAPI()
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            allowed_origins=["https://app.example.com"],
        )
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.options("/test")
        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.options(
            "/test",
            headers={"Origin": "https://app.example.com"},
        )

        assert response.headers.get("Access-Control-Allow-Origin") == "https://app.example.com"
        assert response.headers.get("Access-Control-Allow-Credentials") == "true"

    def test_cors_headers_disallowed_origin(self):
        """Test CORS headers for disallowed origin."""
        app = FastAPI()
        config = SecurityConfig(
            environment=Environment.PRODUCTION,
            allowed_origins=["https://app.example.com"],
        )
        app.add_middleware(SecurityHeadersMiddleware, config=config)

        @app.options("/test")
        @app.get("/test")
        async def test_endpoint():
            return {"status": "ok"}

        client = TestClient(app)
        response = client.get(
            "/test",
            headers={"Origin": "https://malicious.example.com"},
        )

        # CORS headers should not be present for disallowed origin
        assert "Access-Control-Allow-Origin" not in response.headers


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
