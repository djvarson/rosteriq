"""
RosterIQ Tenants Management Router

Provides endpoints for:
- Tenant CRUD operations (read, update)
- Venue management within tenants
- Usage tracking and billing
- Admin operations (system-admin only)

All endpoints require JWT authentication. Admin endpoints require ROSTERIQ_ADMIN_TOKEN.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from rosteriq.auth import User, get_current_user, AccessLevel, require_access
from rosteriq.tenants import (
    Tenant,
    TenantStore,
    TenantStatus,
    BillingTier,
    TenantUsageSnapshot,
    get_tenant_store,
    check_tier_allows,
)

logger = logging.getLogger(__name__)

# Admin token from environment
ADMIN_TOKEN = os.getenv("ROSTERIQ_ADMIN_TOKEN", "")

# ============================================================================
# Response Models (Pydantic)
# ============================================================================


class TenantResponse(BaseModel):
    """Tenant data response."""
    tenant_id: str
    name: str
    slug: str
    created_at: str
    billing_tier: str
    status: str
    trial_ends_at: Optional[str] = None
    owner_user_id: Optional[str] = None
    venue_ids: List[str]
    max_venues: int
    max_employees: int
    contact_email: str
    notes: dict[str, Any]

    @classmethod
    def from_tenant(cls, tenant: Tenant) -> TenantResponse:
        """Convert Tenant to response."""
        return cls(
            tenant_id=tenant.tenant_id,
            name=tenant.name,
            slug=tenant.slug,
            created_at=tenant.created_at.isoformat(),
            billing_tier=tenant.billing_tier.value,
            status=tenant.status.value,
            trial_ends_at=tenant.trial_ends_at.isoformat() if tenant.trial_ends_at else None,
            owner_user_id=tenant.owner_user_id,
            venue_ids=tenant.venue_ids,
            max_venues=tenant.max_venues,
            max_employees=tenant.max_employees,
            contact_email=tenant.contact_email,
            notes=tenant.notes,
        )


class TenantUpdateRequest(BaseModel):
    """Request to update tenant."""
    name: Optional[str] = None
    contact_email: Optional[str] = None
    notes: Optional[dict[str, Any]] = None


class VenueAddRequest(BaseModel):
    """Request to add venue to tenant."""
    venue_id: str = Field(..., description="Venue ID to add")


class UsageResponse(BaseModel):
    """Usage snapshot response."""
    tenant_id: str
    snapshot_date: str
    active_venues: int
    total_employees: int
    rosters_generated_month: int
    ask_queries_month: int
    call_ins_sent_month: int
    sms_credits_used: int
    billable_amount: float

    @classmethod
    def from_snapshot(cls, snapshot: TenantUsageSnapshot) -> UsageResponse:
        """Convert snapshot to response."""
        return cls(
            tenant_id=snapshot.tenant_id,
            snapshot_date=snapshot.snapshot_date,
            active_venues=snapshot.active_venues,
            total_employees=snapshot.total_employees,
            rosters_generated_month=snapshot.rosters_generated_month,
            ask_queries_month=snapshot.ask_queries_month,
            call_ins_sent_month=snapshot.call_ins_sent_month,
            sms_credits_used=snapshot.sms_credits_used,
            billable_amount=snapshot.billable_amount,
        )


class TenantCreateRequest(BaseModel):
    """Admin request to create new tenant."""
    tenant_id: str = Field(..., description="Unique tenant ID")
    name: str = Field(..., description="Human-readable tenant name")
    slug: str = Field(..., description="URL-safe slug")
    billing_tier: str = Field(default="startup", description="startup|pro|enterprise")
    owner_user_id: Optional[str] = None
    contact_email: Optional[str] = None


# ============================================================================
# Admin Authentication Helper
# ============================================================================


async def verify_admin_token(request: Request) -> bool:
    """
    Verify admin token from X-Admin-Token header.

    Returns:
        True if token is valid, False otherwise
    """
    if not ADMIN_TOKEN:
        # Admin token not configured; require it
        return False

    admin_header = request.headers.get("x-admin-token", "")
    return admin_header == ADMIN_TOKEN


# ============================================================================
# Router
# ============================================================================

tenants_router = APIRouter(prefix="/api/v1/tenants", tags=["tenants"])


@tenants_router.get("/me", response_model=TenantResponse)
async def get_my_tenant(
    user: User = Depends(get_current_user),
) -> TenantResponse:
    """
    Get the current user's tenant.

    Any authenticated user can access this endpoint.

    Returns:
        Current user's tenant
    """
    store = get_tenant_store()
    tenant_id = user.tenant_id or "demo-tenant-001"
    tenant = store.get(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    return TenantResponse.from_tenant(tenant)


@tenants_router.get("/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: str,
    user: User = Depends(get_current_user),
) -> TenantResponse:
    """
    Get a specific tenant.

    Only the OWNER of that tenant can access this endpoint.

    Args:
        tenant_id: Tenant to retrieve

    Returns:
        Tenant data
    """
    # Check authorization: user must be the tenant owner or admin
    user_tenant = user.tenant_id or "demo-tenant-001"
    if user_tenant != tenant_id:
        logger.warning(
            f"User {user.id} (tenant {user_tenant}) attempted to access tenant {tenant_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only access your own tenant",
        )

    store = get_tenant_store()
    tenant = store.get(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    return TenantResponse.from_tenant(tenant)


@tenants_router.patch("/{tenant_id}", response_model=TenantResponse)
async def update_tenant(
    tenant_id: str,
    update: TenantUpdateRequest,
    user: User = Depends(get_current_user),
) -> TenantResponse:
    """
    Update a tenant's settings.

    Only the OWNER of that tenant can update it.

    Args:
        tenant_id: Tenant to update
        update: Update request

    Returns:
        Updated tenant
    """
    # Check authorization
    user_tenant = user.tenant_id or "demo-tenant-001"
    if user_tenant != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only update your own tenant",
        )

    store = get_tenant_store()
    tenant = store.get(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    # Build update fields
    fields = {}
    if update.name is not None:
        fields["name"] = update.name
    if update.contact_email is not None:
        fields["contact_email"] = update.contact_email
    if update.notes is not None:
        # Merge notes
        fields["notes"] = {**tenant.notes, **update.notes}

    if fields:
        tenant = store.update(tenant_id, **fields)
        logger.info(f"Tenant {tenant_id} updated by user {user.id}")

    return TenantResponse.from_tenant(tenant)


@tenants_router.get("/{tenant_id}/usage", response_model=Optional[UsageResponse])
async def get_tenant_usage(
    tenant_id: str,
    month: Optional[str] = None,
    user: User = Depends(get_current_user),
) -> Optional[UsageResponse]:
    """
    Get usage snapshot for a tenant.

    Only the tenant OWNER can access usage data.

    Args:
        tenant_id: Tenant to get usage for
        month: Optional month filter (YYYY-MM)

    Returns:
        Usage snapshot, or None if no data available
    """
    # Check authorization
    user_tenant = user.tenant_id or "demo-tenant-001"
    if user_tenant != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only view usage for your own tenant",
        )

    store = get_tenant_store()
    snapshot = store.get_usage(tenant_id, month)

    if not snapshot:
        return None

    return UsageResponse.from_snapshot(snapshot)


@tenants_router.post("/{tenant_id}/venues", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def add_venue_to_tenant(
    tenant_id: str,
    request_data: VenueAddRequest,
    user: User = Depends(get_current_user),
) -> TenantResponse:
    """
    Add a venue to a tenant.

    Only the tenant OWNER can add venues. Respects max_venues limit.

    Args:
        tenant_id: Tenant to add venue to
        request_data: Venue to add

    Returns:
        Updated tenant
    """
    # Check authorization
    user_tenant = user.tenant_id or "demo-tenant-001"
    if user_tenant != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only manage venues for your own tenant",
        )

    store = get_tenant_store()
    tenant = store.get(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    # Try to add venue
    try:
        tenant = store.add_venue(tenant_id, request_data.venue_id)
        logger.info(f"Venue {request_data.venue_id} added to tenant {tenant_id} by user {user.id}")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    return TenantResponse.from_tenant(tenant)


@tenants_router.delete("/{tenant_id}/venues/{venue_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_venue_from_tenant(
    tenant_id: str,
    venue_id: str,
    user: User = Depends(get_current_user),
) -> None:
    """
    Remove a venue from a tenant.

    Only the tenant OWNER can remove venues. This is irreversible.

    Args:
        tenant_id: Tenant to remove venue from
        venue_id: Venue to remove
    """
    # Check authorization
    user_tenant = user.tenant_id or "demo-tenant-001"
    if user_tenant != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only manage venues for your own tenant",
        )

    store = get_tenant_store()
    tenant = store.get(tenant_id)

    if not tenant or venue_id not in tenant.venue_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Venue not found in tenant",
        )

    store.remove_venue(tenant_id, venue_id)
    logger.warning(f"Venue {venue_id} removed from tenant {tenant_id} by user {user.id}")


# ============================================================================
# Admin Endpoints
# ============================================================================


@tenants_router.get("/admin/list", response_model=List[TenantResponse])
async def admin_list_tenants(
    request: Request,
) -> List[TenantResponse]:
    """
    List all tenants (system admin only).

    Requires X-Admin-Token header with ROSTERIQ_ADMIN_TOKEN value.

    Returns:
        All tenants
    """
    if not await verify_admin_token(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing admin token",
        )

    store = get_tenant_store()
    tenants = store.list_all()
    return [TenantResponse.from_tenant(t) for t in tenants]


@tenants_router.post("/admin/create", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def admin_create_tenant(
    request_data: TenantCreateRequest,
    request: Request,
) -> TenantResponse:
    """
    Create a new tenant (system admin only).

    Requires X-Admin-Token header with ROSTERIQ_ADMIN_TOKEN value.

    Args:
        request_data: Tenant creation request

    Returns:
        Created tenant
    """
    if not await verify_admin_token(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing admin token",
        )

    # Parse billing tier
    try:
        billing_tier = BillingTier(request_data.billing_tier)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid billing tier: {request_data.billing_tier}",
        )

    store = get_tenant_store()

    try:
        tenant = store.create(
            tenant_id=request_data.tenant_id,
            name=request_data.name,
            slug=request_data.slug,
            billing_tier=billing_tier,
            owner_user_id=request_data.owner_user_id,
            contact_email=request_data.contact_email or "",
        )
        logger.info(f"Tenant {request_data.tenant_id} created by admin")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    return TenantResponse.from_tenant(tenant)


@tenants_router.post("/admin/suspend/{tenant_id}", response_model=TenantResponse)
async def admin_suspend_tenant(
    tenant_id: str,
    request: Request,
) -> TenantResponse:
    """
    Suspend a tenant (system admin only).

    Requires X-Admin-Token header.

    Args:
        tenant_id: Tenant to suspend

    Returns:
        Updated tenant
    """
    if not await verify_admin_token(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing admin token",
        )

    store = get_tenant_store()
    tenant = store.suspend(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    logger.warning(f"Tenant {tenant_id} suspended by admin")
    return TenantResponse.from_tenant(tenant)


@tenants_router.post("/admin/activate/{tenant_id}", response_model=TenantResponse)
async def admin_activate_tenant(
    tenant_id: str,
    request: Request,
) -> TenantResponse:
    """
    Activate a suspended tenant (system admin only).

    Requires X-Admin-Token header.

    Args:
        tenant_id: Tenant to activate

    Returns:
        Updated tenant
    """
    if not await verify_admin_token(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing admin token",
        )

    store = get_tenant_store()
    tenant = store.activate(tenant_id)

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant {tenant_id} not found",
        )

    logger.info(f"Tenant {tenant_id} activated by admin")
    return TenantResponse.from_tenant(tenant)
