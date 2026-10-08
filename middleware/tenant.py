"""
Tenant context middleware and dependencies for multi-tenancy data isolation.

Ensures all requests carry tenant context (venue_ids) extracted from JWT tokens.
Provides dependency injection for tenant context and venue access validation.
Uses a contextvars.ContextVar so the tenant context is correct under async
concurrency (a thread-local is unsafe here: FastAPI may run awaited code on a
shared thread pool, which could leak one request's tenant context into
another). A ContextVar is isolated per-task and per-thread.
"""

import logging
from contextvars import ContextVar
from typing import Optional, List
from functools import wraps

from fastapi import Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user, SKIP_AUTH_PATHS, WEBHOOK_PATHS


logger = logging.getLogger(__name__)


# Per-request tenant context. ContextVar is async-task isolated, so concurrent
# requests cannot observe each other's context. Default None = no context set.
_tenant_context_var: ContextVar[Optional["TenantContext"]] = ContextVar(
    "tenant_context", default=None
)


class _TenantContextProxy:
    """
    Backward-compatible accessor over the ContextVar.

    Existing code and tests use ``_tenant_context.value = ...`` / read
    ``_tenant_context.value``. This proxy preserves that surface while the
    underlying storage is an async-safe ContextVar.
    """

    @property
    def value(self) -> Optional["TenantContext"]:
        return _tenant_context_var.get()

    @value.setter
    def value(self, ctx: Optional["TenantContext"]) -> None:
        _tenant_context_var.set(ctx)

    def clear(self) -> None:
        _tenant_context_var.set(None)


_tenant_context = _TenantContextProxy()


class TenantContext:
    """Holds the current request's tenant (venue) context."""

    def __init__(self, user_id: str, venue_ids: List[str], is_owner: bool = False,
                 role: Optional[str] = None, section_grants: Optional[dict] = None):
        """
        Initialize tenant context.

        Args:
            user_id: ID of the authenticated user
            venue_ids: List of venue IDs the user has access to
            is_owner: Whether the user is a system owner (unrestricted access)
            role: The user's role ("owner"/"manager"/"staff"). Carried so
                role-gated helpers (enforce_venue_manager) don't need a DB hit.
            section_grants: venue_id -> [section names] a DEPARTMENT manager is
                confined to. Absent/empty for a venue = full venue manager.
                Carried so section-gated helpers need no DB hit.
        """
        self.user_id = user_id
        self.venue_ids = venue_ids
        self.is_owner = is_owner
        self.role = role
        self.section_grants = section_grants or {}

    def is_manager_or_owner(self) -> bool:
        """True if the user may perform manager-level actions (role manager or
        owner, or the platform-owner flag)."""
        return self.is_owner or self.role in ("manager", "owner")

    def managed_sections(self, venue_id: str) -> Optional[List[str]]:
        """The sections this user is confined to for ``venue_id`` as a department
        manager, or None if they are a FULL manager of it (owner, or a manager
        with no section restriction). Returns [] only if explicitly granted no
        sections (an effectively powerless grant)."""
        if self.is_owner:
            return None
        grants = self.section_grants or {}
        if venue_id not in grants:
            return None
        return list(grants.get(venue_id) or [])

    def is_section_restricted(self, venue_id: str) -> bool:
        """True if the user is a DEPARTMENT manager of this venue (scoped to a
        subset of sections), i.e. NOT a full venue manager."""
        return self.managed_sections(venue_id) is not None

    def has_access_to(self, venue_id: str) -> bool:
        """Check if user has access to a specific venue."""
        if self.is_owner:
            return True
        return venue_id in self.venue_ids

    def __repr__(self):
        return f"<TenantContext(user_id={self.user_id}, venues={self.venue_ids}, owner={self.is_owner})>"


# Exempt paths from tenant validation
# Try Demo sessions may not write to these (see _deny_demo_side_effects).
DEMO_DENIED_PREFIXES = (
    "/api/notifications/", "/api/push/", "/api/sms/",
    "/api/deputy/", "/deputy/", "/api/myob/", "/api/xero/", "/api/humanforce/",
    "/tanda/", "/api/tanda/", "/api/keypay/", "/api/pos/", "/api/reservations/",
    "/api/function-tracker/", "/api/connections/", "/api/feeds/config", "/api/plugins/",
    "/api/marketplace/", "/api/billing/", "/employees/", "/venues/",
    "/api/onboarding/", "/api/webhooks/", "/api/payroll/export/", "/api/experiments/",
)
DEMO_DENIED_PATHS = frozenset({
    "/employees", "/venues", "/api/setup/import-staff", "/api/staff/profile",
    "/api/me/link",
})
# ...and /api/v1/ paths containing any of these: digest sends, onboarding
# checklists/templates/reminders (reminders message staff) and labour alert
# thresholds (the last two live in per-worker memory a demo reset can't clear
# on the other worker).
DEMO_DENIED_V1_PARTS = ("/digest/send", "/onboarding", "/labour-thresholds")

OAUTH_CALLBACK_PATHS = frozenset({
    "/api/deputy/callback",
    "/deputy/callback",
    "/api/humanforce/callback",
    "/api/myob/callback",
    "/api/xero/callback",
    "/tanda/callback",
})

EXEMPT_PATHS = {
    "/",
    "/health",
    "/api/health",
    "/ready",
    "/api/ready",
    # /metrics is NOT exempt: it reports platform-wide tenant counts and is
    # owner-only (api.py metrics() calls enforce_owner, which needs a context).
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/auth/register",
    "/api/auth/login",
    "/api/auth/refresh",
    "/api/auth/logout",
    "/api/status",
    "/admin",
    "/staff",
    "/login",
    "/reset-password",  # a locked-out user is unauthenticated by definition
    "/register",
    "/savings",
    "/timeclock",
    "/my",
    "/connections",
    "/sw.js",
    "/docs/api",
    "/favicon.ico",
}

# Path prefixes exempt from tenant validation
EXEMPT_PREFIXES = {"/static/", "/api/auth/"}

# Truly public inbound receivers: no JWT, authenticated by a provider signature
# (HMAC) inside the handler. This must be an EXACT-PATH allowlist, not a prefix —
# a prefix like "/api/webhooks" also matched the admin routes mounted under it
# (/api/webhooks/queue/*, /api/webhooks/register, the outbound-webhook manager,
# and /api/events read APIs), waving them through with no auth at all.
#
# Tanda Marketplace install/uninstall are here for the same reason: they are
# called by Tanda directly (no venue/user exists yet at install, so there is no
# JWT) and are authenticated by the X-Tanda-Signature HMAC check in
# routes/tanda_plugin.py (verify_tanda_signature). Kept in sync with
# middleware/auth.py WEBHOOK_PATHS.
WEBHOOK_EXEMPT = {
    "/api/webhooks/tanda",
    "/api/tanda/plugin/install",
    "/api/tanda/plugin/uninstall",
}


class TenantMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that extracts and stores tenant context from authenticated user.
    Sets the per-request TenantContext (async-safe ContextVar) for DB layer access.
    """

    async def dispatch(self, request: Request, call_next):
        # Skip tenant context for exempt paths
        if self._is_exempt(request.url.path):
            response = await call_next(request)
            return response

        # Token lets us reset the ContextVar to its prior state for THIS task
        # on the way out, rather than blindly clearing shared state.
        ctx_token = None
        try:
            # Try to get current user from request. Pass a real db store —
            # called outside FastAPI dependency injection, the Depends default
            # would otherwise be unresolved.
            user = None
            try:
                user = await get_current_user(request, get_db())
            except HTTPException as exc:
                # Some endpoints allow webhooks or API keys
                if self._is_webhook_exempt(request.url.path):
                    response = await call_next(request)
                    return response
                # HTTPException raised inside middleware is NOT handled by
                # FastAPI's exception handlers (those only cover the routing
                # layer), so it would surface as a 500. Convert it to the
                # intended status code (e.g. 401) here.
                return JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": exc.detail},
                    headers=getattr(exc, "headers", None),
                )

            if user:
                # Store tenant context in the async-safe ContextVar.
                ctx_token = _tenant_context_var.set(
                    TenantContext(
                        user_id=user.user_id,
                        venue_ids=user.venue_ids,
                        is_owner=user.is_owner,
                        role=getattr(user, "role", None),
                        section_grants=getattr(user, "section_grants", None),
                    )
                )

                # Enforce venue ownership for path-scoped venue endpoints (covers all
                # current + future /dashboard/{venue_id}/... routes in one place).
                denied = self._enforce_path_venue_scope(request.url.path, user)
                if denied is not None:
                    return denied
                denied = self._deny_demo_side_effects(request, user)
                if denied is not None:
                    return denied
                await self._note_demo_activity(request, user)

            response = await call_next(request)
            return response

        except HTTPException as exc:
            # Any other HTTPException bubbling up through middleware also needs
            # to be converted to a proper response rather than a 500.
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail},
                headers=getattr(exc, "headers", None),
            )
        except Exception as e:
            logger.error(f"TenantMiddleware error: {e}", exc_info=True)
            raise
        finally:
            # Reset the ContextVar for this task so the context never leaks.
            if ctx_token is not None:
                _tenant_context_var.reset(ctx_token)

    @staticmethod
    async def _note_demo_activity(request: Request, user) -> None:
        """Tell the Try Demo reset this client is still using the demo, so a
        newcomer's Try Demo doesn't wipe it mid-pitch (services/demo_reset.py)."""
        try:
            from rosteriq.services.demo import is_demo_identity
            if not is_demo_identity(getattr(user, "user_id", None), getattr(user, "email", None)):
                return
            from rosteriq.services.demo_reset import activity_due, client_address, note_demo_activity
            address = client_address(request)
            if not activity_due(address):
                return
            db = get_db()
            if db.__class__.__name__ == "PostgresStore":
                import asyncio
                await asyncio.to_thread(note_demo_activity, db, address)
            else:
                note_demo_activity(db, address)
        except Exception:
            pass

    @staticmethod
    def _deny_demo_side_effects(request: Request, user) -> Optional[JSONResponse]:
        """The public Try Demo identities are shared by every visitor. Writes
        that reach outside the sandbox (email/SMS/push to caller-chosen
        recipients, integrations and their credentials, staff-record imports
        and edits) are refused as a class, so a new endpoint under one of these
        prefixes is covered without remembering to guard it."""
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return None
        from rosteriq.services.demo import is_demo_identity
        if not is_demo_identity(getattr(user, "user_id", None), getattr(user, "email", None)):
            return None
        path = request.url.path
        if path.startswith(DEMO_DENIED_PREFIXES) or path in DEMO_DENIED_PATHS or (
                path.startswith("/api/v1/") and any(p in path for p in DEMO_DENIED_V1_PARTS)):
            return JSONResponse(status_code=403, content={
                "detail": "The demo can't do that \u2014 sign up to try it with your own venue."})
        return None

    @staticmethod
    def _enforce_path_venue_scope(path: str, user) -> Optional[JSONResponse]:
        """
        For path-scoped venue endpoints, deny if the user can't access that venue.
        Returns a 403 JSONResponse to short-circuit, or None to proceed.

        Owners (platform admins) pass. Currently guards the /dashboard/{venue_id}/...
        family (cross-venue analytics); other venue-scoped routes enforce explicitly
        via enforce_venue_access(). Path-based so it covers future dashboard routes too.
        """
        if user.is_owner:
            return None
        segments = [s for s in path.split("/") if s]
        venue_id = None
        if len(segments) >= 2 and segments[0] == "dashboard":
            venue_id = segments[1]
        if venue_id and venue_id not in user.venue_ids:
            audit_cross_tenant_attempt(venue_id, "dashboard", "access")
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "You do not have access to this venue"},
            )
        return None

    @staticmethod
    def _is_exempt(path: str) -> bool:
        """Check if path is exempt from tenant validation."""
        if path in EXEMPT_PATHS:
            return True
        # OAuth callbacks are hit by the provider's browser redirect with no JWT —
        # they identify the venue from the signed `state` param, not app auth.
        # Listed explicitly: a suffix match also exempted every route whose last
        # path parameter was sent as the literal "callback". A new connector's
        # callback must be added here or its connect flow 401s.
        if path in OAUTH_CALLBACK_PATHS:
            return True
        return any(path.startswith(p) for p in EXEMPT_PREFIXES)

    @staticmethod
    def _is_webhook_exempt(path: str) -> bool:
        """Check if path is a public inbound receiver (exact match only — see
        WEBHOOK_EXEMPT). Admin routes under /api/webhooks are NOT exempt."""
        return path in WEBHOOK_EXEMPT


def get_tenant_context() -> TenantContext:
    """
    Dependency function to get current tenant context.
    Raises 401 if no tenant context is available.

    Usage in route:
        @app.get("/venues")
        async def list_venues(tenant: TenantContext = Depends(get_tenant_context)):
            ...
    """
    ctx = _tenant_context_var.get()
    if ctx is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant context not found. Authentication required.",
        )
    return ctx


def get_tenant_context_optional() -> Optional[TenantContext]:
    """
    Optional version of get_tenant_context.
    Returns TenantContext if available, None otherwise.
    """
    return _tenant_context_var.get()


def enforce_venue_access(venue_id: Optional[str]) -> None:
    """
    Enforce that the CURRENT request's user may act on ``venue_id``. Call this at
    the top of any venue-scoped route handler — it reads the tenant context the
    middleware already set (no route-signature change needed).

    Owners (platform admins, role="owner") pass; managers/staff are limited to
    their venue_ids. Raises HTTP 403 on denial. A None/empty venue_id is a no-op
    (the handler validates presence itself). If there is no tenant context on a
    venue-scoped endpoint, access is denied (fail closed).
    """
    if not venue_id:
        return
    tenant = get_tenant_context_optional()
    if tenant is None:
        # No tenant context = not inside an authenticated HTTP request (direct unit
        # call, background task, scheduler). Every venue-scoped HTTP route reaches
        # this only AFTER TenantMiddleware sets the context, so a None here is never
        # a real request — no-op (consistent with TenantScopedDB._check_venue_access).
        return
    if tenant.has_access_to(venue_id):
        return
    audit_cross_tenant_attempt(venue_id, "venue_scoped", "access")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You do not have access to this venue",
    )


def enforce_venue_manager(venue_id: Optional[str]) -> None:
    """
    Like ``enforce_venue_access`` but ALSO requires the caller to be a FULL
    manager or owner of the venue — venue membership alone is not enough, and a
    DEPARTMENT manager (one scoped to a subset of sections) is NOT enough either.
    Use for every VENUE-WIDE manager action: venue/org config, integration
    credentials, data imports, payroll, roster publishing, templates, webhooks,
    broadcast messaging, billing, backups, user/role management. For actions that
    belong to one section (a kitchen stocktake, a bar roster edit) use
    ``enforce_section_manager`` instead, which a department manager can pass.

    Order matters: membership is checked first (a non-member gets the same
    "no access to this venue" 403 as ``enforce_venue_access``, not a role hint
    that would confirm the venue exists), then role, then full-manager (not
    section-restricted). Owners (platform admins) pass all. Raises HTTP 403.

    Fail-open on a missing tenant context is deliberate and matches
    ``enforce_venue_access``: there is no context only outside an authenticated
    HTTP request (a direct unit call, background task, or scheduler), never on a
    real venue-scoped route — TenantMiddleware always sets it first.
    """
    enforce_venue_access(venue_id)
    tenant = get_tenant_context_optional()
    if tenant is None:
        return
    if tenant.is_owner:
        return
    if tenant.is_manager_or_owner() and not tenant.is_section_restricted(venue_id):
        return
    if tenant.is_section_restricted(venue_id):
        audit_cross_tenant_attempt(venue_id, "venue_scoped", "section_manager_denied")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=("This is a venue-wide action; a department manager is scoped "
                    "to specific sections and cannot perform it."),
        )
    audit_cross_tenant_attempt(venue_id, "venue_scoped", "role_denied")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="This action requires one of these roles: manager, owner",
    )


def enforce_section_manager(venue_id: Optional[str], section: Optional[str]) -> None:
    """
    Require the caller to manage ``section`` within ``venue_id``. Use for actions
    that belong to one department (a kitchen/bar stocktake, a section's roster
    shifts, a section-targeted announcement).

    Passes for: a platform owner; a FULL venue manager (any section); a DEPARTMENT
    manager whose grant for this venue includes ``section``. Fails (403) for:
    staff; a manager of another venue; a department manager asked for a section
    outside their grant. Section comparison is case/space-insensitive, matching
    the min_staff-key / inventory-section normalisation used elsewhere.

    Membership is enforced first (same 403 as ``enforce_venue_access`` for a
    non-member). A missing tenant context is a no-op, matching the other helpers
    (background/direct calls run outside an HTTP request).
    """
    enforce_venue_access(venue_id)
    tenant = get_tenant_context_optional()
    if tenant is None:
        return
    if tenant.is_owner:
        return
    if not tenant.is_manager_or_owner():
        audit_cross_tenant_attempt(venue_id, "section_scoped", "role_denied")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires one of these roles: manager, owner",
        )
    allowed = tenant.managed_sections(venue_id)
    if allowed is None:
        # Full venue manager — may act on any section.
        return
    norm = lambda s: str(s or "").strip().lower()
    if section is not None and norm(section) in {norm(s) for s in allowed}:
        return
    audit_cross_tenant_attempt(venue_id, "section_scoped", "section_denied")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=(f"You manage the {', '.join(allowed) or '(no)'} section(s) of this "
                f"venue, not '{section}'."),
    )


def enforce_venue_manager_any(venue_id: Optional[str]) -> None:
    """
    Coarse gate: membership AND manager/owner of the venue — FULL or DEPARTMENT.
    Use BEFORE loading a section-bearing resource so staff and non-members are
    refused before any existence check (no "does resource X exist" oracle), then
    pair it with ``enforce_section_manager(venue_id, resource.section)`` AFTER the
    load for the fine per-section decision. A department manager passes this
    coarse check (they ARE a manager here) and is narrowed by the section check.

    Missing tenant context is a no-op, matching the other helpers.
    """
    enforce_venue_access(venue_id)
    tenant = get_tenant_context_optional()
    if tenant is None:
        return
    if tenant.is_manager_or_owner():
        return
    audit_cross_tenant_attempt(venue_id, "venue_scoped", "role_denied")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="This action requires one of these roles: manager, owner",
    )


def enforce_owner() -> None:
    """
    Require the caller to be a platform owner. Use for global, non-venue-scoped
    admin actions (DB pool resize, security-log purge, webhook dead-letter
    purge/circuit reset) — actions that touch shared infrastructure or every
    tenant at once. Raises HTTP 403 on denial.

    Fail-open on a missing tenant context matches ``enforce_venue_access`` (no
    context = not a real authenticated request). Any HTTP route reaching this is
    behind TenantMiddleware, so an unauthenticated request never gets here — it
    is rejected with 401 before the handler runs.
    """
    tenant = get_tenant_context_optional()
    if tenant is None:
        return
    if tenant.is_owner or tenant.role == "owner":
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="This action requires the owner role",
    )


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


# ============================================================================
# Scoped record loaders — the ONE way a by-id route should fetch a tenant
# record. Load → check the record's venue against the caller → the SAME 404
# for "missing" and "not yours", so ids are never an oracle for other tenants.
# Owners pass everything. Use these instead of a bare db.get_*() in any route
# that takes a roster/employee/shift id from the request.
# ============================================================================

def _scoped(record, venue_id: Optional[str], not_found: str, resource_type: str):
    if record is None:
        raise HTTPException(status_code=404, detail=not_found)
    tenant = get_tenant_context_optional()
    # A record with no venue proves nothing about who may see it — deny
    # non-owners (fail closed, as load_shift_in_scope does).
    if tenant is not None and not venue_id and not getattr(tenant, "is_owner", False):
        raise HTTPException(status_code=404, detail=not_found)
    if tenant is not None and venue_id and not tenant.has_access_to(venue_id):
        audit_cross_tenant_attempt(venue_id, resource_type, "access")
        try:
            from rosteriq.services.events import security as _sec
            _sec("access.cross_tenant", venue_id=venue_id, resource_type=resource_type)
        except Exception:
            pass
        raise HTTPException(status_code=404, detail=not_found)
    return record


def load_roster_in_scope(db, roster_id: str):
    """Roster by id, or 404 (missing OR belongs to another tenant)."""
    r = db.get_roster(roster_id)
    return _scoped(r, getattr(r, "venue_id", None), "Roster not found", "roster")


def scoped_employee_map(db, venue_id: str) -> dict:
    """id -> Employee for pricing a venue's shifts: that venue's staff plus
    staff of any OTHER venue the caller also holds (an operator's shared
    staff working across their own venues). Never another tenant's staff.
    Platform owners (and callers with no request context) get the whole
    store, as before — it is only ever used as an id lookup for this venue's
    own shifts."""
    tenant = get_tenant_context_optional()
    if tenant is None or getattr(tenant, "is_owner", False):
        return {e.id: e for e in (db.list_employees() or [])}
    venue_ids = {venue_id} | set(getattr(tenant, "venue_ids", None) or [])
    out = {}
    for vid in venue_ids:
        for e in db.get_employees(vid) or []:
            out[e.id] = e
    return out


def load_employee_in_scope(db, employee_id: str):
    """Employee by id, or 404 (missing OR belongs to another tenant)."""
    e = db.get_employee(employee_id)
    return _scoped(e, getattr(e, "venue_id", None), "Employee not found", "employee")


def load_shift_in_scope(db, shift_id: str):
    """Shift by id, or 404 (missing OR another tenant's).

    Shift objects carry no venue_id, so the venue is resolved through the
    roster (db.venue_id_for_shift). If the shift exists but its venue cannot
    be resolved, a non-owner is DENIED (fail closed) rather than waved
    through — this guard used to fail open on Postgres for exactly that
    reason."""
    s = db.get_shift(shift_id) if hasattr(db, "get_shift") else None
    venue_id = getattr(s, "venue_id", None) if s is not None else None
    if not venue_id:
        try:
            venue_id = db.venue_id_for_shift(shift_id) if hasattr(db, "venue_id_for_shift") else None
        except Exception:
            venue_id = None
    if s is None and venue_id:
        # Not in the shifts index (in-memory store keeps shifts inside rosters)
        try:
            for roster in db.list_rosters() or []:
                for sh in getattr(roster, "shifts", None) or []:
                    if getattr(sh, "id", None) == shift_id:
                        s = sh
                        break
                if s is not None:
                    break
        except Exception:
            s = None
    if s is None and not venue_id:
        # Last resort for stores whose venue_id_for_shift is unavailable
        try:
            for roster in db.list_rosters() or []:
                for sh in getattr(roster, "shifts", None) or []:
                    if getattr(sh, "id", None) == shift_id:
                        s, venue_id = sh, getattr(roster, "venue_id", None)
                        break
                if s is not None:
                    break
        except Exception:
            s = None
    if s is not None and not venue_id:
        tenant = get_tenant_context_optional()
        if tenant is not None and not tenant.is_owner:
            audit_cross_tenant_attempt("<unresolved>", "shift", "access")
            raise HTTPException(status_code=404, detail="Shift not found")
    return _scoped(s, venue_id, "Shift not found", "shift")


def scoped_venue_ids(requested: Optional[List[str]] = None) -> Optional[List[str]]:
    """
    Venue ids the caller may list. Owners: ``requested`` as-is (None = all).
    Managers/staff: their venue_ids, intersected with ``requested`` if given.
    Never returns another tenant's venue; returns [] when the request asks
    only for venues outside scope (a list route then answers empty, not 403).
    """
    tenant = get_tenant_context_optional()
    if tenant is None or tenant.is_owner:
        return requested
    mine = list(tenant.venue_ids or [])
    if not requested:
        return mine
    return [v for v in requested if v in mine]
