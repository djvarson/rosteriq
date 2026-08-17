"""
Authentication middleware and dependencies for FastAPI.
Handles JWT extraction, user context, role-based permissions, and API key auth.

Uses the custom BaseStore database layer instead of SQLAlchemy.
"""
from typing import Optional, List
from functools import wraps

from fastapi import Depends, HTTPException, status, Request, params

from rosteriq.database import get_db
from rosteriq.services.auth import auth_service


class UserContext:
    """Represents the authenticated user context."""

    def __init__(
        self,
        user_id: str,
        email: str,
        name: str,
        role: str,
        venue_ids: List[str],
    ):
        self.user_id = user_id
        self.email = email
        self.name = name
        self.role = role
        self.venue_ids = venue_ids

    @property
    def is_owner(self) -> bool:
        return self.role == "owner"

    @property
    def is_manager(self) -> bool:
        return self.role == "manager"

    @property
    def is_staff(self) -> bool:
        return self.role == "staff"

    def __repr__(self):
        return f"<UserContext(id={self.user_id}, email={self.email}, role={self.role})>"


# Paths that don't require authentication
SKIP_AUTH_PATHS = {
    "/health",
    "/api/health",
    "/api/ready",
    "/api/metrics",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/auth/register",
    "/api/auth/login",
}

SKIP_AUTH_PREFIXES = {
    "/static/",
    "/api/v1/costing/health",
}

# Paths that don't require auth but are webhook-related
WEBHOOK_PATHS = {
    "/api/webhooks",
}


async def get_current_user(
    request: Request,
    db = Depends(get_db),
) -> UserContext:
    """
    Extract and validate JWT token from Authorization header.
    Returns UserContext with current user info.
    Raises HTTPException if token is missing or invalid.

    Note: when called directly (not via FastAPI dependency injection, e.g. from
    TenantMiddleware) the caller must pass a real `db` — `get_db()`. Otherwise
    `db` is the unresolved Depends marker.
    """
    # Resolve db if called outside FastAPI's dependency injection (db left as
    # the Depends marker rather than a real store).
    if isinstance(db, params.Depends):
        db = get_db()

    # Skip auth for certain paths
    if request.url.path in SKIP_AUTH_PATHS or any(
        request.url.path.startswith(p) for p in WEBHOOK_PATHS
    ) or any(
        request.url.path.startswith(p) for p in SKIP_AUTH_PREFIXES
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Public endpoint called with auth dependency",
        )

    auth_header = request.headers.get("Authorization")

    if not auth_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Parse "Bearer <token>"
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header format",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = parts[1]

    # Verify token
    payload = auth_service.verify_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("role")

    # Verify user still exists and is active
    user = db.get_user_by_id(user_id)
    if not user or not user.get("is_active"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    # Get venue IDs for this user
    venue_ids = user.get("venue_ids", [])

    # Identity hook for the event log: stash who this is on the shared request
    # scope (visible to OUTER middleware, e.g. the 401/403/429 security-event
    # recorder in api.py — contextvars set here do not propagate outward
    # because each BaseHTTPMiddleware layer runs call_next in its own task)
    # and on the request-user contextvar (visible to everything downstream in
    # this task, so events recorded by tenant-exempt routes such as /api/auth/*
    # still carry the actor). Best-effort: never breaks auth.
    try:
        request.state.user_id = user["id"]
        request.state.user_role = user["role"]
    except Exception:
        pass
    try:
        from rosteriq.middleware.logging import set_request_user
        set_request_user(user["id"])
    except Exception:
        pass

    return UserContext(
        user_id=user["id"],
        email=user["email"],
        name=user["name"],
        role=user["role"],
        venue_ids=venue_ids,
    )


async def get_current_user_optional(
    request: Request,
    db = Depends(get_db),
) -> Optional[UserContext]:
    """
    Optional version of get_current_user.
    Returns UserContext if token present and valid, None otherwise.
    Does not raise exceptions.
    """
    try:
        return await get_current_user(request, db)
    except HTTPException:
        return None


async def get_api_key_user(
    request: Request,
    db = Depends(get_db),
) -> UserContext:
    """
    Extract and validate API key from X-API-Key header.
    Returns UserContext with current user info.
    Raises HTTPException if key is missing or invalid.
    """
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    # Verify API key
    user = auth_service.verify_api_key(api_key)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    # Get venue IDs for this user
    venue_ids = user.get("venue_ids", [])

    return UserContext(
        user_id=user["id"],
        email=user["email"],
        name=user["name"],
        role=user["role"],
        venue_ids=venue_ids,
    )


def _role_name(r) -> str:
    return str(getattr(r, "value", r))


def require_role(*allowed_roles):
    """
    Enforce role-based access control. Two forms:

      @require_role("owner", "manager")            # decorator
      require_role(current_user, [UserRole.owner, UserRole.manager])   # inline

    The inline form raises 403 immediately when the user's role is not in
    the list. (Before this, the inline form silently returned a decorator
    and never checked anything — 11 changelog/roster-diff routes believed
    they were manager-only and were open to staff.)
    """
    if allowed_roles and hasattr(allowed_roles[0], "role") and not callable(allowed_roles[0]):
        user = allowed_roles[0]
        roles = allowed_roles[1] if len(allowed_roles) > 1 else ()
        if isinstance(roles, (str, bytes)) or not hasattr(roles, "__iter__"):
            roles = [roles]
        names = [_role_name(r) for r in roles]
        if _role_name(getattr(user, "role", None)) not in names:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires one of these roles: {', '.join(names)}",
            )
        return None

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, current_user: UserContext = None, **kwargs):
            if current_user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="User context required",
                )

            if current_user.role not in allowed_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"This action requires one of these roles: {', '.join(allowed_roles)}",
                )

            return await func(*args, current_user=current_user, **kwargs)

        return wrapper

    return decorator


def check_venue_access(current_user: UserContext, venue_id: str) -> bool:
    """
    Check if current user has access to a specific venue.
    Owners have access to all venues, managers only to their assigned venues.
    """
    if current_user.is_owner:
        return True

    return venue_id in current_user.venue_ids


def require_venue_access(venue_id: str):
    """
    Decorator to enforce venue-scoped access.
    Usage: @require_venue_access(venue_id)
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, current_user: UserContext = None, **kwargs):
            if current_user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="User context required",
                )

            if not check_venue_access(current_user, venue_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You do not have access to this venue",
                )

            return await func(*args, current_user=current_user, **kwargs)

        return wrapper

    return decorator


async def require_owner(
    current_user: UserContext = Depends(get_current_user),
) -> UserContext:
    """
    Dependency that requires the user to have owner role.
    Raises HTTPException with 403 if user is not an owner.
    """
    if current_user.role != "owner":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This operation requires owner role",
        )
    return current_user
