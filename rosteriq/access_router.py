"""
RosterIQ Access Control API

Provides endpoints for managing role-based access levels and permissions.
- GET /api/v1/access/me: Get current user's access level and permissions
- POST /api/v1/access/grant: Grant access level to a user (OWNER only)
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from rosteriq.auth import (
    AccessLevel,
    User,
    get_user_by_id,
    require_access,
    _users,
    AUTH_ENABLED,
)

logger = logging.getLogger(__name__)

# ============================================================================
# Models
# ============================================================================


class AccessMeResponse(BaseModel):
    """User's access level and permissions."""

    user_id: str
    email: str
    access_level: str = Field(..., description="Access level: l1, l2, or owner")
    permissions: List[str] = Field(..., description="List of granted permissions")


class GrantAccessRequest(BaseModel):
    """Request to grant access level to a user."""

    user_id: str = Field(..., description="User ID to grant access to")
    access_level: str = Field(..., description="Access level to grant: l1, l2, or owner")


class GrantAccessResponse(BaseModel):
    """Response after granting access."""

    success: bool
    message: str
    user_id: str
    access_level: str


# ============================================================================
# Helper Functions
# ============================================================================


def compute_permissions(access_level: AccessLevel) -> List[str]:
    """
    Compute list of permissions for an access level.

    L1 (Supervisor): view_live_data, log_shift_events, use_headcount_clicker, use_call_in
    L2 (Roster Maker): L1 + edit_roster, view_history, run_scenarios, use_ask_agent
    OWNER: L2 + view_multi_venue, view_accountability, manage_users

    Args:
        access_level: User's access level

    Returns:
        List of permission strings
    """
    permissions = []

    # L1 Supervisor permissions
    if access_level in (AccessLevel.L1_SUPERVISOR, AccessLevel.L2_ROSTER_MAKER, AccessLevel.OWNER):
        permissions.extend([
            "view_live_data",
            "log_shift_events",
            "use_headcount_clicker",
            "use_call_in",
        ])

    # L2 Roster Maker permissions
    if access_level in (AccessLevel.L2_ROSTER_MAKER, AccessLevel.OWNER):
        permissions.extend([
            "edit_roster",
            "view_history",
            "run_scenarios",
            "use_ask_agent",
        ])

    # OWNER permissions
    if access_level == AccessLevel.OWNER:
        permissions.extend([
            "view_multi_venue",
            "view_accountability",
            "manage_users",
        ])

    return permissions


# ============================================================================
# Router
# ============================================================================

access_router = APIRouter(prefix="/api/v1/access", tags=["access"])


@access_router.get("/me", response_model=AccessMeResponse)
async def get_access_info(user: User = Depends(require_access(AccessLevel.L1_SUPERVISOR))) -> AccessMeResponse:
    """
    Get current user's access level and permissions.

    Requires: L1 Supervisor or higher (i.e., any authenticated user in auth mode,
    or any user in demo mode).

    Returns:
        User ID, email, access level, and list of granted permissions
    """
    permissions = compute_permissions(user.access_level)

    return AccessMeResponse(
        user_id=user.id,
        email=user.email,
        access_level=user.access_level.value,
        permissions=permissions,
    )


@access_router.post("/grant", response_model=GrantAccessResponse)
async def grant_access(
    request: GrantAccessRequest,
    user: User = Depends(require_access(AccessLevel.OWNER)),
) -> GrantAccessResponse:
    """
    Grant access level to a user.

    Requires: OWNER access level.

    In demo mode (AUTH_ENABLED=False), this is a no-op that returns success.
    In live mode, it updates the in-memory user store.

    Args:
        request: User ID and access level to grant
        user: Current authenticated user (must be OWNER)

    Returns:
        Success status and updated access level

    Raises:
        HTTPException: If user not found or access level is invalid
    """
    # Demo mode: short-circuit with success
    if not AUTH_ENABLED:
        return GrantAccessResponse(
            success=True,
            message="Access granted (demo mode)",
            user_id=request.user_id,
            access_level=request.access_level,
        )

    # Validate access level
    try:
        new_level = AccessLevel(request.access_level)
    except (ValueError, KeyError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid access level '{request.access_level}'. Use 'l1', 'l2', or 'owner'.",
        )

    # Find user by ID
    target_user = get_user_by_id(request.user_id)
    if not target_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User {request.user_id} not found",
        )

    # Update user's access level in-memory store
    for email, user_data in _users.items():
        if user_data.get("id") == request.user_id:
            user_data["access_level"] = new_level
            logger.info(
                f"User {user.id} (OWNER) granted {new_level.value} access to {request.user_id}"
            )
            return GrantAccessResponse(
                success=True,
                message=f"Access level updated to {new_level.value}",
                user_id=request.user_id,
                access_level=new_level.value,
            )

    # Should not reach here if get_user_by_id found the user
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to update user access level",
    )
