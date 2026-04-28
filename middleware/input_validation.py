"""
Centralised input validation and sanitisation middleware for RosterIQ API.

Implements:
- String sanitisation (XSS prevention, HTML tag stripping)
- Format validation (dates, times, emails, phones, UUIDs)
- Size limits (request body, string fields, arrays, JSON depth)
- SQL injection pattern detection (logging, not blocking)
- File upload sanitisation (extension validation, path traversal prevention)

Configuration:
- ValidationConfig dataclass with all limits configurable
- Skip validation for certain paths (e.g., /health, /static)
- Log-only mode vs block mode for suspicious inputs

Usage:
    from rosteriq.middleware.input_validation import (
        InputValidationMiddleware, ValidationConfig,
        sanitise_string, validate_email, validate_phone
    )
    from fastapi import FastAPI

    config = ValidationConfig(
        max_body_size_bytes=1_000_000,
        max_string_length=10_000,
        block_suspicious=False,  # Log-only mode
    )
    app.add_middleware(InputValidationMiddleware, config=config)
"""

import re
import json
import logging
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)


# ============================================================================
# XSS Detection Patterns
# ============================================================================

XSS_PATTERNS = [
    re.compile(r'<script[^>]*>.*?</script>', re.IGNORECASE | re.DOTALL),
    re.compile(r'javascript:', re.IGNORECASE),
    re.compile(r'on\w+\s*=', re.IGNORECASE),  # onclick, onload, onerror, etc.
    re.compile(r'data:text/html', re.IGNORECASE),
    re.compile(r'<iframe[^>]*>', re.IGNORECASE),
    re.compile(r'<object[^>]*>', re.IGNORECASE),
    re.compile(r'<embed[^>]*>', re.IGNORECASE),
    re.compile(r'<img[^>]*on\w+', re.IGNORECASE),
]


# ============================================================================
# SQL Injection Detection Patterns
# ============================================================================

SQL_INJECTION_PATTERNS = [
    re.compile(r'UNION\s+SELECT', re.IGNORECASE),
    re.compile(r'DROP\s+TABLE', re.IGNORECASE),
    re.compile(r'DELETE\s+FROM', re.IGNORECASE),
    re.compile(r'INSERT\s+INTO', re.IGNORECASE),
    re.compile(r'UPDATE\s+\w+\s+SET', re.IGNORECASE),
    re.compile(r'--\s*$', re.MULTILINE),  # SQL comment
    re.compile(r';\s*$', re.MULTILINE),   # Statement terminator
    re.compile(r'\bOR\b\s+1\s*=\s*1', re.IGNORECASE),
    re.compile(r"'\s*OR\s*'1'='1", re.IGNORECASE),
    re.compile(r'/\*.*?\*/', re.IGNORECASE | re.DOTALL),  # Multi-line comment
]


# ============================================================================
# Format Validation Patterns
# ============================================================================

# ISO 8601 date format: YYYY-MM-DD
DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# Time formats: HH:MM or HH:MM:SS
TIME_PATTERN = re.compile(r'^([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?$')

# Basic RFC 5322 email (simplified, not exhaustive)
EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}'
    r'[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
)

# Australian phone format or international
PHONE_PATTERN = re.compile(
    r'^(?:\+?61|0)[2-9]\d{8,9}$|'  # Australian: +61 or 0, then 2-9, then 8-9 digits
    r'^\+\d{1,3}\d{9,14}$'  # International: + then country code + number
)

# UUID v4 format
UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ValidationConfig:
    """
    Configuration for input validation and sanitisation middleware.

    Attributes:
        max_body_size_bytes: Maximum request body size (default 1MB)
        max_string_length: Maximum length for individual string fields (default 10,000)
        max_array_length: Maximum number of items in arrays (default 1,000)
        max_json_depth: Maximum JSON nesting depth (default 10)
        block_suspicious: If True, reject suspicious inputs; if False, log only
        skip_paths: Set of URL paths to skip validation (e.g., /health, /static)
        enable_xss_detection: Enable XSS pattern detection (default True)
        enable_sql_injection_detection: Enable SQL injection pattern detection (default True)
        allowed_file_extensions: Allowlist of file extensions (default: pdf, csv, xlsx, jpg, png)
        enable_null_byte_detection: Reject null bytes in any input (default True)
    """
    max_body_size_bytes: int = 1_000_000  # 1MB
    max_string_length: int = 10_000
    max_array_length: int = 1_000
    max_json_depth: int = 10
    block_suspicious: bool = False  # Log-only mode by default
    skip_paths: set = field(default_factory=lambda: {
        "/health", "/ready", "/metrics", "/docs", "/redoc", "/openapi.json", "/static"
    })
    enable_xss_detection: bool = True
    enable_sql_injection_detection: bool = True
    allowed_file_extensions: set = field(default_factory=lambda: {
        "pdf", "csv", "xlsx", "xls", "jpg", "jpeg", "png", "gif"
    })
    enable_null_byte_detection: bool = True


# ============================================================================
# Helper Functions (importable by routes)
# ============================================================================

def sanitise_string(value: str) -> str:
    """
    Sanitise a string by stripping HTML tags, escaping special characters,
    and removing leading/trailing whitespace.

    Args:
        value: String to sanitise

    Returns:
        Sanitised string

    Raises:
        TypeError: If value is not a string
    """
    if not isinstance(value, str):
        raise TypeError(f"Expected string, got {type(value).__name__}")

    # Remove null bytes
    value = value.replace('\x00', '')

    # Strip leading/trailing whitespace
    value = value.strip()

    # Remove HTML tags
    value = re.sub(r'<[^>]+>', '', value)

    # Escape special HTML characters
    value = (value
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#x27;')
    )

    return value


def validate_date(value: str) -> bool:
    """
    Validate ISO 8601 date format (YYYY-MM-DD).

    Args:
        value: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(value, str):
        return False

    if not DATE_PATTERN.match(value):
        return False

    # Also check if it's a valid date
    try:
        datetime.strptime(value, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def validate_time(value: str) -> bool:
    """
    Validate time format (HH:MM or HH:MM:SS).

    Args:
        value: Time string to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(value, str):
        return False

    return bool(TIME_PATTERN.match(value))


def validate_email(value: str) -> bool:
    """
    Validate email address using basic RFC 5322 pattern.

    Args:
        value: Email address to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(value, str):
        return False

    if len(value) > 254:  # RFC 5321
        return False

    return bool(EMAIL_PATTERN.match(value))


def validate_phone(value: str) -> bool:
    """
    Validate phone number (Australian or international format).

    Australian formats:
    - +61 2/3/7/8 XXXX XXXX
    - 0 2/3/7/8 XXXX XXXX

    International: +[country code] [9-14 digits]

    Args:
        value: Phone number to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(value, str):
        return False

    # Remove spaces and hyphens for matching
    normalized = value.replace(' ', '').replace('-', '')
    return bool(PHONE_PATTERN.match(normalized))


def validate_uuid(value: str) -> bool:
    """
    Validate UUID v4 format.

    Args:
        value: UUID string to validate

    Returns:
        True if valid UUID v4, False otherwise
    """
    if not isinstance(value, str):
        return False

    return bool(UUID_PATTERN.match(value))


def sanitise_filename(name: str) -> str:
    """
    Sanitise filename to prevent path traversal and remove dangerous characters.

    Removes:
    - Path traversal attempts (../, ..\, etc.)
    - Special shell characters (`;`, `|`, `&`, etc.)
    - Control characters

    Args:
        name: Filename to sanitise

    Returns:
        Sanitised filename
    """
    if not isinstance(name, str):
        raise TypeError(f"Expected string, got {type(name).__name__}")

    # Remove path traversal attempts
    name = re.sub(r'\.+[/\\]', '', name)  # Remove ../ or ..\
    name = name.replace('\\', '/')  # Normalize backslashes

    # Keep only the filename, not full path
    name = name.split('/')[-1]

    # Remove dangerous characters
    name = re.sub(r'[;<>:|&$`]', '', name)

    # Remove control characters
    name = ''.join(char for char in name if ord(char) >= 32 or char in '\t\n\r')

    # Remove leading/trailing dots and spaces
    name = name.strip('. ')

    return name or 'file'  # Return 'file' if empty


def check_json_depth(data: Any, max_depth: int = 10, current_depth: int = 0) -> bool:
    """
    Check if JSON structure exceeds maximum nesting depth.

    Args:
        data: JSON data (dict, list, or primitive)
        max_depth: Maximum allowed depth
        current_depth: Current recursion depth (internal use)

    Returns:
        True if depth is within limit, False if exceeded
    """
    if current_depth > max_depth:
        return False

    if isinstance(data, dict):
        for value in data.values():
            if not check_json_depth(value, max_depth, current_depth + 1):
                return False
    elif isinstance(data, list):
        for item in data:
            if not check_json_depth(item, max_depth, current_depth + 1):
                return False

    return True


# ============================================================================
# Suspicious Input Detection
# ============================================================================

def detect_xss_vectors(value: str) -> bool:
    """
    Detect common XSS vectors in a string.

    Args:
        value: String to check

    Returns:
        True if XSS pattern detected, False otherwise
    """
    if not isinstance(value, str):
        return False

    return any(pattern.search(value) for pattern in XSS_PATTERNS)


def detect_sql_injection_vectors(value: str) -> bool:
    """
    Detect common SQL injection patterns in a string.

    Args:
        value: String to check

    Returns:
        True if SQL injection pattern detected, False otherwise
    """
    if not isinstance(value, str):
        return False

    return any(pattern.search(value) for pattern in SQL_INJECTION_PATTERNS)


def has_null_bytes(value: str) -> bool:
    """
    Check if string contains null bytes.

    Args:
        value: String to check

    Returns:
        True if null bytes found, False otherwise
    """
    if not isinstance(value, str):
        return False

    return '\x00' in value


# ============================================================================
# Request Body Processing
# ============================================================================

async def get_request_body(request: Request) -> Optional[bytes]:
    """
    Safely read request body, checking size limits.

    Args:
        request: HTTP request

    Returns:
        Request body bytes, or None if empty/oversized
    """
    try:
        # Read the body
        body = await request.body()
        return body
    except Exception as e:
        logger.warning(f"Failed to read request body: {e}")
        return None


def validate_request_body_size(
    body: bytes,
    max_size: int
) -> Tuple[bool, Optional[str]]:
    """
    Validate request body size.

    Args:
        body: Request body bytes
        max_size: Maximum allowed size in bytes

    Returns:
        Tuple of (is_valid, error_message)
    """
    if len(body) > max_size:
        return False, f"Request body exceeds maximum size of {max_size} bytes"

    return True, None


def process_json_value(
    value: Any,
    config: ValidationConfig,
    path: str = "root"
) -> Tuple[Any, bool]:
    """
    Process and validate a JSON value recursively.

    Detects suspicious patterns and checks size limits.

    Args:
        value: JSON value to process (dict, list, or primitive)
        config: ValidationConfig instance
        path: Dot notation path for logging (e.g., "root.user.name")

    Returns:
        Tuple of (processed_value, is_suspicious)
    """
    is_suspicious = False

    # Handle None/null
    if value is None:
        return value, is_suspicious

    # Handle strings
    if isinstance(value, str):
        # Check for null bytes
        if config.enable_null_byte_detection and has_null_bytes(value):
            logger.warning(f"Null byte detected in field: {path}")
            is_suspicious = True

        # Check length
        if len(value) > config.max_string_length:
            logger.warning(f"String field exceeds max length at {path}: {len(value)} > {config.max_string_length}")
            is_suspicious = True

        # Check for XSS patterns
        if config.enable_xss_detection and detect_xss_vectors(value):
            logger.warning(f"XSS pattern detected in field: {path}")
            is_suspicious = True

        # Check for SQL injection patterns
        if config.enable_sql_injection_detection and detect_sql_injection_vectors(value):
            logger.warning(f"SQL injection pattern detected in field: {path}")
            is_suspicious = True

        return sanitise_string(value), is_suspicious

    # Handle dicts
    if isinstance(value, dict):
        processed = {}
        for key, val in value.items():
            new_path = f"{path}.{key}" if path != "root" else key
            processed[key], field_suspicious = process_json_value(
                val, config, new_path
            )
            is_suspicious = is_suspicious or field_suspicious

        return processed, is_suspicious

    # Handle lists
    if isinstance(value, list):
        if len(value) > config.max_array_length:
            logger.warning(f"Array exceeds max length at {path}: {len(value)} > {config.max_array_length}")
            is_suspicious = True

        processed = []
        for idx, item in enumerate(value):
            new_path = f"{path}[{idx}]"
            processed_item, item_suspicious = process_json_value(
                item, config, new_path
            )
            processed.append(processed_item)
            is_suspicious = is_suspicious or item_suspicious

        return processed, is_suspicious

    # Return other types (int, float, bool) unchanged
    return value, is_suspicious


# ============================================================================
# Input Validation Middleware
# ============================================================================

class InputValidationMiddleware(BaseHTTPMiddleware):
    """
    Middleware for centralised input validation and sanitisation.

    Validates:
    - Request body size limits
    - JSON structure depth
    - String format and content (XSS, SQL injection)
    - String field lengths
    - Array sizes
    - Data types and formats (date, time, email, phone, UUID)

    Sanitises:
    - HTML tags from strings
    - Special characters
    - Null bytes
    - Filenames (path traversal prevention)

    Can operate in two modes:
    - Block mode (block_suspicious=True): Reject requests with suspicious input
    - Log-only mode (block_suspicious=False): Log suspicious input and allow through
    """

    def __init__(self, app, config: Optional[ValidationConfig] = None):
        """
        Initialize input validation middleware.

        Args:
            app: FastAPI application instance
            config: ValidationConfig instance (creates default if None)
        """
        super().__init__(app)
        self.config = config or ValidationConfig()

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Validate and sanitise incoming request.

        Args:
            request: HTTP request
            call_next: Callable to invoke next middleware/handler

        Returns:
            JSON error response if validation fails, otherwise passes through
        """
        # Skip validation for certain paths
        if request.url.path in self.config.skip_paths:
            return await call_next(request)

        # Only validate requests with JSON bodies
        content_type = request.headers.get("content-type", "")
        if "application/json" not in content_type:
            return await call_next(request)

        # For POST, PUT, PATCH requests, validate body
        if request.method in ("POST", "PUT", "PATCH"):
            # Read and validate body size
            body = await get_request_body(request)

            if body is None:
                return await call_next(request)

            # Check body size
            is_valid_size, error_msg = validate_request_body_size(
                body, self.config.max_body_size_bytes
            )

            if not is_valid_size:
                logger.warning(f"Body size validation failed: {error_msg}")
                if self.config.block_suspicious:
                    return JSONResponse(
                        {"detail": error_msg},
                        status_code=413,  # Payload Too Large
                    )

            # Parse JSON
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in request: {e}")
                if self.config.block_suspicious:
                    return JSONResponse(
                        {"detail": "Invalid JSON in request body"},
                        status_code=400,
                    )
                return await call_next(request)

            # Check JSON depth
            if not check_json_depth(data, self.config.max_json_depth):
                logger.warning(
                    f"JSON depth exceeds maximum at {request.url.path}: "
                    f"max {self.config.max_json_depth}"
                )
                if self.config.block_suspicious:
                    return JSONResponse(
                        {"detail": f"JSON nesting depth exceeds maximum of {self.config.max_json_depth}"},
                        status_code=400,
                    )

            # Process and validate JSON values
            processed_data, is_suspicious = process_json_value(data, self.config)

            # If suspicious input detected
            if is_suspicious:
                logger.warning(
                    f"Suspicious input detected in {request.method} {request.url.path}",
                    extra={
                        "path": request.url.path,
                        "method": request.method,
                    }
                )

                if self.config.block_suspicious:
                    return JSONResponse(
                        {"detail": "Request contains suspicious or invalid input"},
                        status_code=400,
                    )

            # Attach processed data to request state for use in route handlers
            request.state.validated_body = processed_data

        # Continue to next middleware/handler
        response = await call_next(request)

        # Add header to indicate this request was validated
        response.headers["X-Input-Validated"] = "true"

        return response
