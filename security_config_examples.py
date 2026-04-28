"""
Example security header configurations for different deployment scenarios.

Use these as templates when deploying RosterIQ to different environments.
Modify origins, CSP directives, and other settings as needed for your deployment.

Usage:
    from rosteriq.security_config_examples import get_production_config
    config = get_production_config()
    app.add_middleware(SecurityHeadersMiddleware, config=config)
"""

from rosteriq.middleware.security_headers import SecurityConfig, Environment


def get_development_config() -> SecurityConfig:
    """
    Development environment configuration.

    Features:
    - No HSTS enforcement (allows local HTTP testing)
    - Relaxed CSP with unsafe-eval (hot module reload)
    - Localhost origins for local development
    - WebSocket support for development servers

    Use for: Local development, testing, debugging
    """
    return SecurityConfig(
        environment=Environment.DEVELOPMENT,
        enable_hsts=False,  # Unnecessary for localhost
        allowed_origins=[
            "http://localhost:3000",
            "http://localhost:5173",
            "http://localhost:8000",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173",
            "http://127.0.0.1:8000",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
    )


def get_staging_config() -> SecurityConfig:
    """
    Staging environment configuration.

    Features:
    - HSTS enforcement enabled
    - Strict CSP (no unsafe-eval)
    - Limited staging origins
    - CSP reporting enabled for pre-production testing

    Use for: Pre-production testing, QA, staging deployments
    """
    return SecurityConfig(
        environment=Environment.STAGING,
        enable_hsts=True,
        hsts_max_age=31536000,  # 1 year
        allowed_origins=[
            "https://staging.app.rosteriq.com",
            "https://staging.dashboard.rosteriq.com",
            "https://staging-admin.rosteriq.com",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
        additional_csp_directives={},
    )


def get_production_config() -> SecurityConfig:
    """
    Production environment configuration.

    Features:
    - Maximum security: HSTS with preload directive
    - Strict CSP enforcement
    - Limited production origins (no wildcards)
    - CSP violation monitoring enabled
    - All security features active

    Use for: Production deployments serving real users

    IMPORTANT: Update `allowed_origins` with your actual production domains
    """
    return SecurityConfig(
        environment=Environment.PRODUCTION,
        enable_hsts=True,
        hsts_max_age=31536000,  # 1 year, added to preload list
        allowed_origins=[
            "https://app.rosteriq.com",
            "https://dashboard.rosteriq.com",
            "https://admin.rosteriq.com",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
        enable_cors_preflight_caching=True,
        cors_max_age=86400,  # 24 hours
        additional_csp_directives={},
    )


def get_production_config_with_cdn() -> SecurityConfig:
    """
    Production configuration with third-party CDN support.

    Extends production config to allow resources from trusted CDNs
    (e.g., analytics, monitoring, error tracking services).

    Example: Adding analytics and error tracking services
    """
    return SecurityConfig(
        environment=Environment.PRODUCTION,
        enable_hsts=True,
        hsts_max_age=31536000,
        allowed_origins=[
            "https://app.rosteriq.com",
            "https://dashboard.rosteriq.com",
            "https://admin.rosteriq.com",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
        additional_csp_directives={
            # Add Sentry error tracking
            "script-src": (
                "'self' 'unsafe-inline' https://cdnjs.cloudflare.com "
                "https://browser.sentry-cdn.com"
            ),
            "connect-src": (
                "'self' wss: https://api.stripe.com "
                "https://*.sentry.io"
            ),
            # Add Google Analytics (if needed)
            # "script-src": "'self' https://www.googletagmanager.com",
            # "img-src": "'self' data: blob: https://www.google-analytics.com",
        },
    )


def get_production_config_ecommerce() -> SecurityConfig:
    """
    Production configuration for e-commerce deployments.

    Extends production config with payment processor integrations
    (Stripe, PayPal, etc.) and additional analytics services.

    Example: Supporting Stripe payment forms and Google Analytics
    """
    return SecurityConfig(
        environment=Environment.PRODUCTION,
        enable_hsts=True,
        hsts_max_age=31536000,
        allowed_origins=[
            "https://app.rosteriq.com",
            "https://dashboard.rosteriq.com",
            "https://billing.rosteriq.com",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
        additional_csp_directives={
            # Stripe payment processing
            "script-src": (
                "'self' 'unsafe-inline' https://cdnjs.cloudflare.com "
                "https://js.stripe.com"
            ),
            "frame-src": "https://js.stripe.com https://hooks.stripe.com",
            "connect-src": (
                "'self' wss: https://api.stripe.com "
                "https://m.stripe.network"
            ),
            # PayPal (if using)
            # "script-src": "https://www.paypal.com https://www.paypalobjects.com",
            # "frame-src": "https://www.paypal.com",
        },
    )


def get_production_config_regulated() -> SecurityConfig:
    """
    Production configuration for regulated industries.

    Enhanced security for compliance-sensitive deployments
    (healthcare, finance, government). Stricter CSP, audit logging,
    compliance framework support.

    Features:
    - Stricter CSP with no inline scripts
    - Audit trail support
    - Enhanced CORS restrictions
    - Compliance with HIPAA, GDPR, SOC2
    """
    return SecurityConfig(
        environment=Environment.PRODUCTION,
        enable_hsts=True,
        hsts_max_age=31536000,
        allowed_origins=[
            "https://app.rosteriq.com",
            "https://admin.rosteriq.com",
        ],
        csp_report_uri="/api/v1/admin/security/csp-report",
        enable_cors_preflight_caching=False,  # No caching for sensitive data
        cors_max_age=300,  # 5 minutes only
        additional_csp_directives={
            # Remove 'unsafe-inline' for compliance
            "script-src": "'self' https://cdnjs.cloudflare.com",
            "style-src": "'self' https://fonts.googleapis.com",
            # Prevent form submission to external hosts
            "form-action": "'self'",
            # Prevent framing entirely
            "frame-ancestors": "'none'",
        },
    )


def get_custom_config(
    environment: str,
    allowed_origins: list[str],
    csp_directives: dict = None,
    csp_report_uri: str = None,
) -> SecurityConfig:
    """
    Create a custom security configuration.

    Args:
        environment: "development", "staging", or "production"
        allowed_origins: List of allowed CORS origins
        csp_directives: Optional dict of CSP directive overrides
        csp_report_uri: Optional CSP violation report endpoint

    Returns:
        Configured SecurityConfig instance

    Example:
        config = get_custom_config(
            environment="production",
            allowed_origins=["https://myapp.com"],
            csp_directives={
                "img-src": "'self' https://cdn.myapp.com",
            }
        )
    """
    env_map = {
        "development": Environment.DEVELOPMENT,
        "staging": Environment.STAGING,
        "production": Environment.PRODUCTION,
    }

    return SecurityConfig(
        environment=env_map.get(environment, Environment.PRODUCTION),
        enable_hsts=environment in ("staging", "production"),
        hsts_max_age=31536000,
        allowed_origins=allowed_origins,
        csp_report_uri=csp_report_uri,
        additional_csp_directives=csp_directives or {},
    )


# Example configurations for different use cases

# Single domain (most common)
SINGLE_DOMAIN = SecurityConfig(
    environment=Environment.PRODUCTION,
    allowed_origins=["https://app.example.com"],
)

# Multiple subdomains
MULTI_SUBDOMAIN = SecurityConfig(
    environment=Environment.PRODUCTION,
    allowed_origins=[
        "https://app.example.com",
        "https://api.example.com",
        "https://dashboard.example.com",
    ],
)

# Multi-tenant with different domains
MULTI_TENANT = SecurityConfig(
    environment=Environment.PRODUCTION,
    allowed_origins=[
        "https://client1.example.com",
        "https://client2.example.com",
        "https://client3.example.com",
    ],
)

# With partner integrations
WITH_PARTNERS = SecurityConfig(
    environment=Environment.PRODUCTION,
    allowed_origins=[
        "https://app.example.com",
        "https://partner1.example.com",
        "https://partner2.example.com",
    ],
    additional_csp_directives={
        "connect-src": (
            "'self' wss: https://api.stripe.com "
            "https://partner1.example.com https://partner2.example.com"
        ),
    },
)
