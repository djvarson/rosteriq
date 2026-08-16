"""
Structured logging middleware for RosterIQ API.

Provides JSON-formatted logs with request tracing and correlation IDs for
integration with log aggregation platforms (ELK, Datadog, etc).

Features:
- UUID correlation IDs per request (or use X-Request-ID header)
- Contextvars for propagation to service layer
- Request/response logging with timing and metadata
- Sensitive data sanitization (auth tokens, passwords, API keys)
- JSON formatter for structured log aggregation
- Slow request warnings
"""

import json
import logging
import time
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Optional
from datetime import datetime

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


# ============================================================================
# Context variables for request tracing
# ============================================================================

_correlation_id_var: ContextVar[str] = ContextVar(
    'correlation_id', default=None
)
_request_user_var: ContextVar[Optional[str]] = ContextVar(
    'request_user', default=None
)
_request_ip_var: ContextVar[Optional[str]] = ContextVar(
    'request_ip', default=None
)


def set_request_ip(ip: Optional[str]) -> None:
    """Client IP for the current request (X-Forwarded-For first hop, else peer)."""
    _request_ip_var.set(ip)


def get_request_ip() -> Optional[str]:
    return _request_ip_var.get()


# ============================================================================
# Helper functions for contextvars
# ============================================================================

def get_correlation_id() -> str:
    """
    Retrieve the current request's correlation ID from contextvars.

    Returns:
        Correlation ID string (UUID format)
    """
    correlation_id = _correlation_id_var.get()
    if correlation_id is None:
        return str(uuid.uuid4())
    return correlation_id


def set_correlation_id(correlation_id: str) -> None:
    """
    Set the correlation ID in contextvars.

    Args:
        correlation_id: UUID string to associate with current request
    """
    _correlation_id_var.set(correlation_id)


def set_request_user(user_id: Optional[str]) -> None:
    """
    Set the authenticated user ID in contextvars.

    Args:
        user_id: User ID string or None if unauthenticated
    """
    _request_user_var.set(user_id)


def get_request_user() -> Optional[str]:
    """
    Retrieve the authenticated user ID from contextvars.

    Returns:
        User ID string or None if not set
    """
    return _request_user_var.get()


# ============================================================================
# Sanitization helpers
# ============================================================================

def sanitize_headers(headers: dict) -> dict:
    """
    Mask sensitive headers to prevent token leakage in logs.

    Sanitizes: Authorization, X-API-Key, X-Auth-Token, Cookie, etc.

    Args:
        headers: Request headers dictionary

    Returns:
        Sanitized headers with tokens masked
    """
    if not headers:
        return {}

    sensitive_headers = {
        'authorization',
        'x-api-key',
        'x-auth-token',
        'cookie',
        'x-csrf-token',
        'x-access-token',
        'x-refresh-token',
    }

    sanitized = {}
    for key, value in headers.items():
        key_lower = key.lower()

        if key_lower in sensitive_headers:
            # Mask sensitive headers
            if value and len(value) > 10:
                sanitized[key] = f"{value[:10]}***masked***"
            else:
                sanitized[key] = "***masked***"
        else:
            sanitized[key] = value

    return sanitized


def sanitize_body(body: Any) -> Any:
    """
    Recursively mask sensitive fields in request/response bodies.

    Sanitizes: password, pwd, token, api_key, secret, key, auth, etc.

    Args:
        body: Request/response body (dict, list, str, etc)

    Returns:
        Sanitized body with sensitive fields masked
    """
    if isinstance(body, dict):
        sensitive_keys = {
            'password', 'pwd', 'passwd',
            'token', 'access_token', 'refresh_token',
            'api_key', 'apikey',
            'secret', 'client_secret',
            'auth', 'authorization',
            'api_secret',
            'hmac',
            'signature',
        }

        sanitized = {}
        for key, value in body.items():
            key_lower = key.lower()

            if key_lower in sensitive_keys:
                if isinstance(value, str) and len(value) > 10:
                    sanitized[key] = f"{value[:10]}***masked***"
                else:
                    sanitized[key] = "***masked***"
            elif isinstance(value, (dict, list)):
                sanitized[key] = sanitize_body(value)
            else:
                sanitized[key] = value

        return sanitized

    elif isinstance(body, list):
        return [sanitize_body(item) for item in body]

    else:
        return body


# ============================================================================
# JSON Formatter for structured logging
# ============================================================================

class JSONFormatter(logging.Formatter):
    """
    Format log records as JSON for structured log aggregation.

    Includes: timestamp, level, logger name, message, correlation_id,
    exception info, and any extra context fields.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as JSON.

        Args:
            record: LogRecord to format

        Returns:
            JSON string with log data
        """
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }

        # Add correlation ID if available
        correlation_id = getattr(record, 'correlation_id', None)
        if correlation_id:
            log_data['correlation_id'] = correlation_id
        else:
            log_data['correlation_id'] = get_correlation_id()

        # Add user ID if available
        request_user = getattr(record, 'request_user', None)
        if request_user:
            log_data['user_id'] = request_user

        # Add any extra fields from the record
        extra_keys = set(record.__dict__.keys()) - {
            'name', 'msg', 'args', 'created', 'filename', 'funcName',
            'levelname', 'levelno', 'lineno', 'module', 'msecs',
            'message', 'pathname', 'process', 'processName', 'relativeCreated',
            'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info',
            'correlation_id', 'request_user',
        }

        for key in extra_keys:
            try:
                value = getattr(record, key)
                # Only include serializable values
                json.dumps(value)
                log_data[key] = value
            except (TypeError, ValueError):
                # Skip non-serializable values
                pass

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
            }

        try:
            return json.dumps(log_data, default=str)
        except Exception:
            # Fallback to simple format if JSON encoding fails
            return super().format(record)


# ============================================================================
# Structured Logging Middleware
# ============================================================================

class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for structured request/response logging with correlation IDs.

    - Generates UUID correlation_id per request (or uses X-Request-ID header)
    - Stores in contextvars for propagation to service layer
    - Logs request: method, path, query params, client IP, user agent, size
    - Logs response: status code, response time (ms), size
    - Sanitizes sensitive fields: auth headers, passwords, API keys
    - Outputs JSON for log aggregation (ELK/Datadog/etc)
    """

    EXEMPT_PATHS = {'/health', '/ready', '/metrics', '/docs', '/redoc', '/openapi.json'}

    def __init__(self, app, log_level: str = None, log_format: str = None,
                 slow_request_threshold_ms: int = None):
        """
        Initialize structured logging middleware.

        Args:
            app: FastAPI application
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR) or env LOG_LEVEL
            log_format: Log format (json|text) or env LOG_FORMAT (default json)
            slow_request_threshold_ms: Threshold for slow request warning (env SLOW_REQUEST_THRESHOLD_MS)
        """
        super().__init__(app)

        import os

        self.log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
        self.log_format = log_format or os.getenv('LOG_FORMAT', 'json')
        self.slow_request_threshold_ms = slow_request_threshold_ms or int(
            os.getenv('SLOW_REQUEST_THRESHOLD_MS', '1000')
        )

        self.logger = get_structured_logger(__name__)
        self.logger.setLevel(self.log_level)

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process request and log structured data.

        Args:
            request: Incoming request
            call_next: Next middleware/handler

        Returns:
            Response with logging applied
        """

        # Skip logging for exempt paths
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Generate or retrieve correlation ID
        correlation_id = request.headers.get(
            'X-Request-ID',
            str(uuid.uuid4())
        )
        set_correlation_id(correlation_id)

        # Extract user ID from request (JWT, session, etc)
        # This is a basic extraction; customize based on your auth scheme
        user_id = self._extract_user_id(request)
        if user_id:
            set_request_user(user_id)

        # Log incoming request
        start_time = time.time()
        request_log_data = {
            'correlation_id': correlation_id,
            'method': request.method,
            'path': request.url.path,
            'query': dict(request.query_params) if request.query_params else None,
            'client_ip': self._get_client_ip(request),
            'user_agent': request.headers.get('User-Agent'),
            'content_length': request.headers.get('Content-Length'),
            'headers': sanitize_headers(dict(request.headers)),
        }

        if user_id:
            request_log_data['user_id'] = user_id

        # Try to read request body for logging (if present)
        if request.method in {'POST', 'PUT', 'PATCH'}:
            try:
                body = await request.body()
                if body:
                    try:
                        body_dict = json.loads(body)
                        request_log_data['body'] = sanitize_body(body_dict)
                    except json.JSONDecodeError:
                        request_log_data['body'] = '[non-JSON]'
            except Exception:
                pass

        self.logger.info(
            f"Request: {request.method} {request.url.path}",
            extra=request_log_data
        )

        # Process request through handler
        response = await call_next(request)

        # Calculate response time
        duration_ms = (time.time() - start_time) * 1000

        # Log outgoing response
        response_log_data = {
            'correlation_id': correlation_id,
            'method': request.method,
            'path': request.url.path,
            'status_code': response.status_code,
            'response_time_ms': round(duration_ms, 2),
            'content_length': response.headers.get('Content-Length'),
        }

        if user_id:
            response_log_data['user_id'] = user_id

        # Log slow requests at WARNING level
        if duration_ms > self.slow_request_threshold_ms:
            response_log_data['slow_request'] = True
            self.logger.warning(
                f"Slow request: {request.method} {request.url.path} ({duration_ms:.0f}ms)",
                extra=response_log_data
            )
        else:
            self.logger.info(
                f"Response: {response.status_code}",
                extra=response_log_data
            )

        # Add correlation ID to response headers for client tracking
        response.headers['X-Correlation-ID'] = correlation_id

        return response

    def _get_client_ip(self, request: Request) -> str:
        """
        Extract client IP from request, respecting X-Forwarded-For header.

        Args:
            request: FastAPI Request object

        Returns:
            Client IP address string
        """
        # Check for X-Forwarded-For (common in load-balanced environments)
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()

        # Fall back to direct client connection
        if request.client:
            return request.client.host

        return 'unknown'

    def _extract_user_id(self, request: Request) -> Optional[str]:
        """
        Extract user ID from request (customize based on your auth scheme).

        Args:
            request: FastAPI Request object

        Returns:
            User ID string or None if unauthenticated
        """
        # Try to extract from JWT token (Bearer scheme)
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            # In a real implementation, you'd decode the JWT here
            # For now, just return a placeholder
            try:
                import base64
                token = auth_header[7:]
                # Simple JWT structure: header.payload.signature
                parts = token.split('.')
                if len(parts) == 3:
                    # Decode payload (with padding)
                    payload = parts[1]
                    padding = 4 - len(payload) % 4
                    if padding != 4:
                        payload += '=' * padding
                    decoded = base64.urlsafe_b64decode(payload)
                    payload_dict = json.loads(decoded)
                    # Extract user ID from common claims
                    return payload_dict.get('sub') or payload_dict.get('user_id') or payload_dict.get('email')
            except Exception:
                pass

        return None


# ============================================================================
# Logger factory with JSON formatting
# ============================================================================

def get_structured_logger(name: str) -> logging.Logger:
    """
    Create a logger with JSON formatter for structured logging.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logging.Logger instance
    """
    import os

    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler()

        # Use JSON formatter by default
        log_format = os.getenv('LOG_FORMAT', 'json')
        if log_format == 'json':
            formatter = JSONFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Set level from environment or default to INFO
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logger.setLevel(getattr(logging, log_level, logging.INFO))

    return logger
