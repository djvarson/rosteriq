"""
Tenant context middleware and dependencies for multi-tenancy data isolation.

Ensures all requests carry tenant context (venue_ids) extracted from JWT tokens.
Provides dependency injection for tenant context and venue access validation.
Uses thread-local storage for accessing tenant context in DB methods.
"""

import threading
import logging
from typing import Optional, List
from functools import wraps

from fastapi import Depends, HTTPException, sttus, Request
from starlette.middleware.base import BaseHTTPMiddleware

from rosteriq.middleware.auth import UserContext, get_current_user, SKIP_AUTH_PATHS, WEBHOOK_PATHS


logger = logging.getLogger(__name__)


# Thread-local storage for tenant context
_tenant_context: threading.local = threading.local()


class TenantContext:
    """Holds the current request's tenant (venue) context."""

    def __init__(self, user_id: str, venue_ids: List[str], is_owner: bool = False):
        """
        Initialize tenant context.

        Args:
            user_id: ID of the authenticated user
            venue_ids: List of venue IDs the user has access to
            is_owner: Whether the user is a system owner (unrestricted access)
        """
        self.user_id = user_id
        self.venue_ids = venue_ids
        self.is_owner = is_owner

    def has_access_to(self, venue_id: str) -> bool:
        """Check if user has access to a specific venue."""
        if self.is_owner:
            return True
        return venue_id in self.venue_ids

    def __repr__(self):
        return f"<TenantContext(user_id={self.user_id}, venues={self.venue_ids}, owner={self.is_owner})>"


# Exempt paths from tenant validation
EXEMPT_PATHS = {
    "/",
    "/health",
    "/ready",
    "/metrics",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/auth/register",
    "/api/auth/login",
    "/api/auth/refresh",
    "/api/auth/logout",
    "/graphql",
    "/admin",
    "/staff",
    "/sw.js",
    "/docs/api",
    "/favicon.ico",
}

# Path prefixes exempt from tenant validation
EXEMPT_PREFIXES = {"/static/", "/api/auth/"}

WEBHOOK_EXEMPT = {"/api/webhooks", "/api/events"}


class TenantMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that extracts and stores tenant context from authenticated user.
    Sets thread-local TenantContext for DB layer access.
    """

    async def dispatch(self, request: Request, call_next):
        # Skip tenant context for exempt paths
        if self._is_exempt(request.url.path):
            response = await call_next(request)
            return response

        try:
            # Try to get current user from request
            user = None
            try:
                user = await get_current_user(request)
            except HTTPException:
                # Some endpoints allow webhooks or API keys
                if self._is_webhook_exempt(request.url.path):
                    response = await call_next(request)
                    return response
                raise

            if user:
                # Store tenant context in thread-local storage
                _tenant_context.value = TenantContext(
                    user_id=user.user_id,
                    venue_ids=user.venue_ids,
                    is_owner=user.is_owner,
                )

            response = await call_next(request)
            return response

        except Exception as e:
            logger.error(f"TenantMiddleware error: {e}", exc_info=True)
            raise
        finally:
            # Clean up thread-local storage
            if hasattr(_tenant_context, 'value'):
                delattr(_tenant_context, 'value')

    @staticmethod
    def _is_exempt(path: str) -> bool:
        """Check if path is exempt from tenant validation."""
        if path in EXEMPT_PATHS:
            return True
        return any(path.startswith(p) for p in EXEMPT_PREFIXES)

    @staticmethod
    def _is_webhook_exempt(path: str) -> bool:
        """Check if path is webhook-related and exempt from auth."""
        return any(path.startswith(p) for p in WEBHOOK_EXEMPT)


def get_tenant_context() -> TenantContext:
    """
    Dependency function to get current tenant context.
    Raises 401 if no tenant context is available.

    Usage in route:
        @app.get("/venues")
        async def list_venues(tenant: TenantContext = Depends(get_tenant_context)):
            ...
    """
    if not hasattr(_tenant_context, 'value') or _tenant_context.value is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant context not found. Authentication required.",
        )
    return _tenant_context.value


def get_tenant_context_optional() -> Optional[TenantContext]:
    """
    Optional version of get_tenant_context.
    Returns TenantContext if available, None otherwise.
    """
    return getattr(_tenant_context, 'value', None)


def require_venue_access(venue_id: str):
    """
    Decorator to enforce venue-scoped access control.
    Raises 403 if user doesn't have access to the venue.

    Usage:
        @require_venue_access("venue-123")
        async def update_venue(venue_id: str):
            ...
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tenant = get_tenant_context()
            if not tenant.has_access_to(venue_id):
                logger.warning(
                    f"Access denied for user {tenant.user_id} to venue {venue_id}",
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"You do not have access to venue {venue_id}",
                )
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def require_venue_access_from_param(param_name: str = "venue_id"):
    """
    Decorator to enforce venue access based on a path/query parameter.
    Extracts venue_id from kwargs and validates access.

    Usage:
        @require_venue_access_from_param("venue_id")
        async def update_venue(venue_id: str):
            ...
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tenant = get_tenant_context()
            venue_id = kwargs.get(param_name)

            if not venue_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required parameter: {param_name}",
                )

            if not tenant.has_access_to(venue_id):
                logger.warning(
                    f"Access denied for user {tenant.user_id} to venue {venue_id}",
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"You do not have access to venue {venue_id}",
                )

            return await func(*args, **kwargs)

        return wrapper

    return decorator


def get_current_tenant_id() -> str:
    """
    Get the primary tenant ID for the current user.
    Returns the first venue_id if user has multiple, or raises 401.
    """
    tenant = get_tenant_context()
    if not tenant.venue_ids:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User has no assigned venues",
        )
    return tenant.venue_ids[0]


def audit_cross_tenant_attempt(
    venue_id: str,
    resource_type: str,
    action: str = "access",
) -> None:
    """
    Log a suspicious cross-tenant access attempt.
    Called by TenantScopedDB when access is denied.

    Args:
        venue_id: The venue_id that was attempted
        resource_type: Type of resource (employee, roster, forecast, etc)
        action: Type of action (access, read, write, delete)
    """
    tenant = get_tenant_context_optional()
    if tenant:
        logger.warning(
            f"Cross-tenant access attempt detected",
            extra={
                "user_id": tenant.user_id,
                "attempted_venue": venue_id,
                "user_venues": tenant.venue_ids,
                "resource_type": resource_type,
                "action": action,
            },
        )
