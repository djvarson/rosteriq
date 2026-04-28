"""
Sentry error reporting integration for RosterIQ.

Provides conditional error tracking and reporting when SENTRY_DSN is configured.
Gracefully degrades if sentry-sdk is not installed.
"""

import os
import logging
from typing import Any, Optional, Dict
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Attempt to import sentry_sdk
try:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False
    sentry_sdk = None


def init_sentry(app: Any) -> bool:
    """
    Initialize Sentry error reporting for FastAPI app.

    Only initializes if SENTRY_DSN environment variable is set and sentry-sdk is installed.
    Returns True if successfully initialized, False otherwise.

    Args:
        app: FastAPI application instance

    Returns:
        bool: True if Sentry was initialized, False otherwise
    """
    sentry_dsn = os.getenv("SENTRY_DSN")

    if not sentry_dsn:
        logger.debug("SENTRY_DSN not configured, error reporting disabled")
        return False

    if not SENTRY_AVAILABLE:
        logger.warning(
            "SENTRY_DSN is set but sentry-sdk is not installed. "
            "Install sentry-sdk to enable error reporting: pip install sentry-sdk"
        )
        return False

    try:
        release = os.getenv("SENTRY_RELEASE", "unknown")
        environment = os.getenv("SENTRY_ENVIRONMENT", "production")
        traces_sample_rate = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1"))

        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[
                FastApiIntegration(),
                StarletteIntegration(),
            ],
            environment=environment,
            release=release,
            traces_sample_rate=traces_sample_rate,
            attach_stacktrace=True,
            include_source_context=True,
            request_bodies="small",
        )

        logger.info(
            f"Sentry initialized: environment={environment}, release={release}"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")
        return False


def capture_exception(error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
    """
    Capture and report an exception to Sentry with optional context.

    Gracefully handles case where Sentry is not available.

    Args:
        error: The exception to report
        context: Optional dictionary of additional context data
    """
    if not SENTRY_AVAILABLE:
        logger.debug(f"Error captured (Sentry not available): {error}")
        return

    if not sentry_sdk.get_client().is_active():
        logger.debug(f"Error captured (Sentry not initialized): {error}")
        return

    try:
        with sentry_sdk.push_scope() as scope:
            if context:
                for key, value in context.items():
                    scope.set_context("rosteriq", {key: value})

            scope.set_tag("error_type", type(error).__name__)
            sentry_sdk.capture_exception(error)

    except Exception as e:
        logger.error(f"Failed to report exception to Sentry: {e}")


def capture_message(
    message: str,
    level: str = "info",
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Capture and report a custom message to Sentry.

    Gracefully handles case where Sentry is not available.

    Args:
        message: The message to report
        level: Log level ('debug', 'info', 'warning', 'error', 'fatal')
        context: Optional dictionary of additional context data
    """
    if not SENTRY_AVAILABLE:
        logger.debug(f"Message captured (Sentry not available): [{level}] {message}")
        return

    if not sentry_sdk.get_client().is_active():
        logger.debug(f"Message captured (Sentry not initialized): [{level}] {message}")
        return

    try:
        with sentry_sdk.push_scope() as scope:
            if context:
                for key, value in context.items():
                    scope.set_context("rosteriq", {key: value})

            scope.set_tag("message_type", "custom_event")
            sentry_sdk.capture_message(message, level=level)

    except Exception as e:
        logger.error(f"Failed to report message to Sentry: {e}")


def set_user_context(user_id: str, email: Optional[str] = None) -> None:
    """
    Set user context for error reporting.

    Called from authenticated request handlers to track user-specific errors.

    Args:
        user_id: Unique user identifier
        email: Optional user email address
    """
    if not SENTRY_AVAILABLE or not sentry_sdk.get_client().is_active():
        return

    try:
        sentry_sdk.set_user({
            "id": user_id,
            "email": email or "unknown",
        })
    except Exception as e:
        logger.error(f"Failed to set user context in Sentry: {e}")


def set_request_context(venue_id: Optional[str] = None, request_id: Optional[str] = None) -> None:
    """
    Set request-specific context for error reporting.

    Args:
        venue_id: Optional venue identifier
        request_id: Optional request tracking ID
    """
    if not SENTRY_AVAILABLE or not sentry_sdk.get_client().is_active():
        return

    try:
        with sentry_sdk.push_scope() as scope:
            context = {}
            if venue_id:
                context["venue_id"] = venue_id
            if request_id:
                context["request_id"] = request_id

            if context:
                scope.set_context("request", context)

    except Exception as e:
        logger.error(f"Failed to set request context in Sentry: {e}")


@contextmanager
def sentry_context(context_name: str, context_data: Dict[str, Any]):
    """
    Context manager for temporarily setting Sentry context.

    Usage:
        with sentry_context("roster_generation", {"venue_id": "123", "date": "2026-04-25"}):
            # ... code that might raise errors
    """
    if not SENTRY_AVAILABLE or not sentry_sdk.get_client().is_active():
        yield
        return

    try:
        with sentry_sdk.push_scope() as scope:
            scope.set_context(context_name, context_data)
            yield
    except Exception as e:
        logger.error(f"Error in sentry_context: {e}")
        yield
