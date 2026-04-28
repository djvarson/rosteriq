"""Centralized configuration service for RosterIQ.

Validates all environment variables on startup, provides type-safe access to config,
and masks sensitive values in logs.

Usage:
    from rosteriq.services.config import get_config

    config = get_config()
    errors = config.validate()
    if errors:
        # Handle errors
        pass
    config.log_summary()
"""

import os
import sys
import logging
from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum


logger = logging.getLogger(__name__)


class Environment(Enum):
    """Supported deployment environments."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class AppConfig:
    """Application configuration loaded from environment variables.

    Uses dataclass for clean representation and validation. All required
    variables are validated on access via from_env(). Optional variables
    have sensible defaults.
    """

    # Required variables (fail fast in production if missing)
    database_url: str
    jwt_secret: str
    stripe_secret_key: str

    # Environment and core settings
    environment: str = "development"
    port: int = 8000
    log_level: str = "INFO"

    # Caching
    redis_url: str = ""

    # Tanda integration
    tanda_client_id: str = ""
    tanda_client_secret: str = ""
    tanda_webhook_secret: str = ""
    tanda_redirect_uri: str = "http://localhost:8000/tanda/callback"

    # Xero integration
    xero_client_id: str = ""
    xero_client_secret: str = ""
    xero_redirect_uri: str = "http://localhost:8000/api/xero/callback"

    # Email/SMTP
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    smtp_from: str = "noreply@rosteriq.com"

    # SMS/Twilio
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""

    # Error tracking
    sentry_dsn: str = ""
    sentry_release: str = "unknown"
    sentry_environment: str = "production"
    sentry_traces_sample_rate: float = 0.1

    # Push notifications
    vapid_public_key: str = ""
    vapid_private_key: str = ""

    # CORS and API
    allowed_origins: str = "*"
    cors_origins: List[str] = field(default_factory=lambda: ["*"])

    # Database connection pooling
    db_pool_min_size: int = 5
    db_pool_max_size: int = 20
    db_pool_idle_timeout: int = 300
    db_pool_lifetime: int = 3600
    db_health_check_interval: int = 30
    db_query_timeout: int = 30

    # Performance
    max_workers: int = 2
    abuse_detection_enabled: bool = True
    abuse_block_cooldown_minutes: int = 15
    abuse_cleanup_interval_seconds: int = 300
    slow_request_threshold_ms: int = 1000

    # Storage and backups
    backup_dir: str = "/tmp/backups"
    feed_config_encryption_key: str = "dev-key-not-secure"

    # Logging format
    log_format: str = "json"

    # Stripe webhook
    stripe_webhook_secret: str = ""

    # Application URL (for redirects, notifications, etc.)
    app_url: str = "https://app.rosteriq.io"

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables.

        Returns:
            AppConfig instance with all values loaded from environment.

        Raises:
            ValueError: If any required variable is malformed or cannot be coerced.
        """
        try:
            # Required variables
            database_url = os.environ.get("DATABASE_URL")
            jwt_secret = os.environ.get("JWT_SECRET")
            stripe_secret_key = os.environ.get("STRIPE_SECRET_KEY")

            # Build config with required fields and optional overrides
            config = cls(
                database_url=database_url or "",
                jwt_secret=jwt_secret or "",
                stripe_secret_key=stripe_secret_key or "",

                # Environment
                environment=os.environ.get("ENVIRONMENT", "development"),
                port=int(os.environ.get("PORT", "8000")),
                log_level=os.environ.get("LOG_LEVEL", "INFO"),
                log_format=os.environ.get("LOG_FORMAT", "json"),

                # Redis
                redis_url=os.environ.get("REDIS_URL", ""),

                # Tanda
                tanda_client_id=os.environ.get("TANDA_CLIENT_ID", ""),
                tanda_client_secret=os.environ.get("TANDA_CLIENT_SECRET", ""),
                tanda_webhook_secret=os.environ.get("TANDA_WEBHOOK_SECRET", ""),
                tanda_redirect_uri=os.environ.get(
                    "TANDA_REDIRECT_URI",
                    "http://localhost:8000/tanda/callback"
                ),

                # Xero
                xero_client_id=os.environ.get("XERO_CLIENT_ID", ""),
                xero_client_secret=os.environ.get("XERO_CLIENT_SECRET", ""),
                xero_redirect_uri=os.environ.get(
                    "XERO_REDIRECT_URI",
                    "http://localhost:8000/api/xero/callback"
                ),

                # SMTP
                smtp_host=os.environ.get("SMTP_HOST", "smtp.gmail.com"),
                smtp_port=int(os.environ.get("SMTP_PORT", "587")),
                smtp_user=os.environ.get("SMTP_USER", ""),
                smtp_pass=os.environ.get("SMTP_PASS", ""),
                smtp_from=os.environ.get("SMTP_FROM", "noreply@rosteriq.com"),

                # Twilio
                twilio_account_sid=os.environ.get("TWILIO_ACCOUNT_SID", ""),
                twilio_auth_token=os.environ.get("TWILIO_AUTH_TOKEN", ""),
                twilio_from_number=os.environ.get("TWILIO_FROM_NUMBER", ""),

                # Sentry
                sentry_dsn=os.environ.get("SENTRY_DSN", ""),
                sentry_release=os.environ.get("SENTRY_RELEASE", "unknown"),
                sentry_environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
                sentry_traces_sample_rate=float(
                    os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")
                ),

                # VAPID (push notifications)
                vapid_public_key=os.environ.get("VAPID_PUBLIC_KEY", ""),
                vapid_private_key=os.environ.get("VAPID_PRIVATE_KEY", ""),

                # CORS
                allowed_origins=os.environ.get("ALLOWED_ORIGINS", "*"),
                cors_origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","),

                # Database pooling
                db_pool_min_size=int(os.environ.get("DB_POOL_MIN_SIZE", "5")),
                db_pool_max_size=int(os.environ.get("DB_POOL_MAX_SIZE", "20")),
                db_pool_idle_timeout=int(os.environ.get("DB_POOL_IDLE_TIMEOUT", "300")),
                db_pool_lifetime=int(os.environ.get("DB_POOL_LIFETIME", "3600")),
                db_health_check_interval=int(
                    os.environ.get("DB_HEALTH_CHECK_INTERVAL", "30")
                ),
                db_query_timeout=int(os.environ.get("DB_QUERY_TIMEOUT", "30")),

                # Performance
                max_workers=int(os.environ.get("MAX_WORKERS", "2")),
                abuse_detection_enabled=os.environ.get(
                    "ABUSE_DETECTION_ENABLED", "true"
                ).lower() in ("true", "1", "yes"),
                abuse_block_cooldown_minutes=int(
                    os.environ.get("ABUSE_BLOCK_COOLDOWN_MINUTES", "15")
                ),
                abuse_cleanup_interval_seconds=int(
                    os.environ.get("ABUSE_CLEANUP_INTERVAL_SECONDS", "300")
                ),
                slow_request_threshold_ms=int(
                    os.environ.get("SLOW_REQUEST_THRESHOLD_MS", "1000")
                ),

                # Storage
                backup_dir=os.environ.get("BACKUP_DIR", "/tmp/backups"),
                feed_config_encryption_key=os.environ.get(
                    "FEED_CONFIG_ENCRYPTION_KEY", "dev-key-not-secure"
                ),

                # Stripe
                stripe_webhook_secret=os.environ.get("STRIPE_WEBHOOK_SECRET", ""),

                # App URL
                app_url=os.environ.get("APP_URL", "https://app.rosteriq.io"),
            )

            return config

        except ValueError as e:
            logger.error(f"Configuration parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            raise

    def validate(self) -> List[str]:
        """Validate all required configuration values.

        Returns:
            List of validation error messages. Empty if all required values are present.
        """
        errors = []

        # Required fields
        if not self.database_url:
            errors.append("DATABASE_URL is required (PostgreSQL connection string)")
        elif not self.database_url.startswith("postgresql://"):
            errors.append(
                "DATABASE_URL must be a valid PostgreSQL connection string "
                "(start with 'postgresql://')"
            )

        if not self.jwt_secret:
            errors.append(
                "JWT_SECRET is required (minimum 64 random characters for production)"
            )
        elif len(self.jwt_secret) < 32:
            errors.append(
                f"JWT_SECRET is too short (got {len(self.jwt_secret)} chars, "
                "need at least 32, recommend 64+)"
            )

        if not self.stripe_secret_key:
            errors.append(
                "STRIPE_SECRET_KEY is required for billing integration"
            )
        elif not self.stripe_secret_key.startswith("sk_"):
            errors.append(
                "STRIPE_SECRET_KEY must be a valid Stripe secret key "
                "(starts with 'sk_')"
            )

        # Environment validation
        try:
            Environment(self.environment)
        except ValueError:
            errors.append(
                f"ENVIRONMENT must be one of: "
                f"{', '.join(e.value for e in Environment)} "
                f"(got '{self.environment}')"
            )

        # Port validation
        if not 1 <= self.port <= 65535:
            errors.append(
                f"PORT must be between 1-65535 (got {self.port})"
            )

        # Log level validation
        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_log_levels:
            errors.append(
                f"LOG_LEVEL must be one of: {', '.join(sorted(valid_log_levels))} "
                f"(got '{self.log_level}')"
            )

        # Redis URL validation (optional, but if provided must be valid)
        if self.redis_url and not self.redis_url.startswith("redis://"):
            errors.append(
                "REDIS_URL must be a valid Redis connection string "
                "(start with 'redis://')"
            )

        return errors

    def is_production(self) -> bool:
        """Check if running in production environment.

        Returns:
            True if environment is 'production', False otherwise.
        """
        return self.environment.lower() == "production"

    def is_development(self) -> bool:
        """Check if running in development environment.

        Returns:
            True if environment is 'development', False otherwise.
        """
        return self.environment.lower() == "development"

    def log_summary(self) -> None:
        """Log all configuration values with secrets masked.

        Shows first 4 characters + **** for sensitive values like:
        - Database passwords
        - API keys and secrets
        - JWT and encryption keys
        """
        def mask_secret(value: str, min_show: int = 4) -> str:
            """Mask a secret value, showing only first N characters."""
            if not value:
                return "(not set)"
            if len(value) <= min_show:
                return "****"
            return value[:min_show] + "*" * (len(value) - min_show)

        summary = [
            "=" * 70,
            "RosterIQ Configuration Summary",
            "=" * 70,
            f"Environment:                    {self.environment}",
            f"Port:                           {self.port}",
            f"Log Level:                      {self.log_level}",
            f"Log Format:                     {self.log_format}",
            "",
            "Database:",
            f"  URL:                          {mask_secret(self.database_url)}",
            f"  Pool Min Size:                {self.db_pool_min_size}",
            f"  Pool Max Size:                {self.db_pool_max_size}",
            f"  Pool Idle Timeout (s):        {self.db_pool_idle_timeout}",
            f"  Pool Lifetime (s):            {self.db_pool_lifetime}",
            f"  Health Check Interval (s):    {self.db_health_check_interval}",
            f"  Query Timeout (s):            {self.db_query_timeout}",
            "",
            "Security:",
            f"  JWT Secret:                   {mask_secret(self.jwt_secret)}",
            f"  VAPID Public Key:             {mask_secret(self.vapid_public_key)}",
            "",
            "Caching:",
            f"  Redis URL:                    {mask_secret(self.redis_url) if self.redis_url else '(in-memory)'}",
            "",
            "Integrations:",
            f"  Tanda Client ID:              {mask_secret(self.tanda_client_id)}",
            f"  Tanda Webhook Secret:         {mask_secret(self.tanda_webhook_secret)}",
            f"  Xero Client ID:               {mask_secret(self.xero_client_id)}",
            f"  Stripe Secret Key:            {mask_secret(self.stripe_secret_key)}",
            f"  Stripe Webhook Secret:        {mask_secret(self.stripe_webhook_secret)}",
            "",
            "Email (SMTP):",
            f"  Host:                         {self.smtp_host}",
            f"  Port:                         {self.smtp_port}",
            f"  User:                         {mask_secret(self.smtp_user)}",
            f"  From:                         {self.smtp_from}",
            "",
            "SMS (Twilio):",
            f"  Account SID:                  {mask_secret(self.twilio_account_sid)}",
            f"  From Number:                  {self.twilio_from_number}",
            "",
            "Error Tracking (Sentry):",
            f"  DSN:                          {mask_secret(self.sentry_dsn) if self.sentry_dsn else '(disabled)'}",
            f"  Release:                      {self.sentry_release}",
            "",
            "Performance:",
            f"  Max Workers:                  {self.max_workers}",
            f"  Abuse Detection:              {self.abuse_detection_enabled}",
            f"  Slow Request Threshold (ms):  {self.slow_request_threshold_ms}",
            "",
            "API:",
            f"  Allowed Origins:              {self.allowed_origins}",
            f"  App URL:                      {self.app_url}",
            "",
            "Storage:",
            f"  Backup Directory:             {self.backup_dir}",
            "=" * 70,
        ]

        logger.info("\n" + "\n".join(summary))

    def get_database_url(self) -> str:
        """Get the database URL (already validated).

        Returns:
            PostgreSQL connection string.
        """
        return self.database_url

    def get_redis_url(self) -> Optional[str]:
        """Get the Redis URL if configured, None otherwise.

        Returns:
            Redis connection string, or None if not configured (in-memory fallback).
        """
        return self.redis_url if self.redis_url else None

    def get_environment_enum(self) -> Environment:
        """Get environment as an Enum for type-safe switching.

        Returns:
            Environment enum value.
        """
        return Environment(self.environment)


# Global singleton instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get or initialize the global configuration singleton.

    This is the primary entry point for accessing configuration throughout
    the application. On first call, loads config from environment variables.
    Subsequent calls return the cached instance.

    Returns:
        AppConfig singleton instance.

    Raises:
        RuntimeError: If called after config initialization fails.
    """
    global _config

    if _config is None:
        _config = AppConfig.from_env()

    return _config


def init_config() -> AppConfig:
    """Initialize configuration explicitly (for testing or setup).

    Usually you want get_config() instead. This is useful for:
    - Explicit initialization in tests
    - Forcing a reload (not recommended in production)

    Returns:
        AppConfig singleton instance.
    """
    global _config
    _config = AppConfig.from_env()
    return _config


def reset_config() -> None:
    """Reset the global config singleton (for testing only).

    WARNING: Do not use in production. This is for test isolation.
    """
    global _config
    _config = None
