"""
Dashboard configuration routes.

Provides REST endpoints for retrieving role-based dashboard configurations,
menu structures, and permission settings.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional

from rosteriq.middleware.auth import get_current_user, UserContext, require_owner
from rosteriq.services.dashboard_router import DashboardRouter, MenuItem, DashboardConfig


# ============================================================================
# Response Models
# ============================================================================

class MenuItemResponse(BaseModel):
    """Menu item for navigation."""

    label: str
    icon: str
    panel_id: str
    badge_count: Optional[int] = None


class PermissionsResponse(BaseModel):
    """Permission flags for a user role."""

    can_view_all_venues: bool
    can_manage_users: bool
    can_configure_system: bool
    can_view_billing: bool
    can_backup_restore: bool
    can_view_analytics: bool
    can_publish_roster: bool
    can_manage_staff: bool
    can_view_logs: bool
    can_export_data: bool
    can_access_api: bool


class DashboardConfigResponse(BaseModel):
    """Complete dashboard configuration for a user role."""

    role: str
    visible_panels: List[str]
    available_actions: List[str]
    menu_items: List[MenuItemResponse]
    quick_stats: List[str]
    default_view: str
    permissions: PermissionsResponse


class DashboardMenuResponse(BaseModel):
    """Dashboard navigation menu for current user."""

    menu_items: List[MenuItemResponse]
    default_view: str


class PanelsResponse(BaseModel):
    """List of accessible panels for a role."""

    role: str
    panels: List[str]


class ActionsResponse(BaseModel):
    """List of available actions for a role."""

    role: str
    actions: List[str]


class ActionCheckResponse(BaseModel):
    """Response for action permission check."""

    role: str
    action: str
    can_perform: bool


class PanelCheckResponse(BaseModel):
    """Response for panel visibility check."""

    role: str
    panel_id: str
    can_view: bool


class PermissionCheckResponse(BaseModel):
    """Response for permission flag check."""

    role: str
    permission: str
    has_permission: bool


# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


# ============================================================================
# Routes
# ============================================================================

@router.get("/config", response_model=DashboardConfigResponse)
async def get_dashboard_config(
    current_user: UserContext = Depends(get_current_user),
) -> DashboardConfigResponse:
    """
    GET /api/v1/dashboard/config

    Get the complete dashboard configuration for the authenticated user's role.

    Returns:
        DashboardConfigResponse with role-specific layout, panels, actions, and permissions

    Example response:
        {
            "role": "manager",
            "visible_panels": ["roster", "analytics", "conflicts", ...],
            "available_actions": ["generate_roster", "approve_roster", ...],
            "menu_items": [
                {"label": "Rosters", "icon": "calendar", "panel_id": "roster"}
            ],
            "quick_stats": ["todays_roster", "labour_percentage", ...],
            "default_view": "roster",
            "permissions": {...}
        }
    """
    try:
        config = DashboardRouter.get_dashboard_config(
            role=current_user.role,
            venue_id=current_user.venue_ids[0] if current_user.venue_ids else None,
        )

        # Convert menu items to response format
        menu_items = [
            MenuItemResponse(
                label=item.label,
                icon=item.icon,
                panel_id=item.panel_id,
                badge_count=item.badge_count,
            )
            for item in config.menu_items
        ]

        # Get permissions
        permissions_dict = DashboardRouter.get_role_permissions(current_user.role)
        permissions = PermissionsResponse(**permissions_dict)

        return DashboardConfigResponse(
            role=config.role,
            visible_panels=config.visible_panels,
            available_actions=config.available_actions,
            menu_items=menu_items,
            quick_stats=config.quick_stats,
            default_view=config.default_view,
            permissions=permissions,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role configuration: {str(e)}",
        )


@router.get("/config/{role}", response_model=DashboardConfigResponse)
async def get_dashboard_config_for_role(
    role: str,
    current_user: UserContext = Depends(require_owner),
) -> DashboardConfigResponse:
    """
    GET /api/v1/dashboard/config/{role}

    Preview dashboard configuration for a specific role.
    Admin/Owner only — useful for testing role configurations.

    Path Parameters:
        role: User role ('owner', 'manager', 'staff', 'admin')

    Returns:
        DashboardConfigResponse for the specified role

    Raises:
        403 Forbidden: If user is not an owner
        400 Bad Request: If role is invalid
    """
    valid_roles = ["owner", "manager", "staff", "admin"]
    if role not in valid_roles:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role. Must be one of: {', '.join(valid_roles)}",
        )

    try:
        config = DashboardRouter.get_dashboard_config(role=role)

        # Convert menu items to response format
        menu_items = [
            MenuItemResponse(
                label=item.label,
                icon=item.icon,
                panel_id=item.panel_id,
                badge_count=item.badge_count,
            )
            for item in config.menu_items
        ]

        # Get permissions
        permissions_dict = DashboardRouter.get_role_permissions(role)
        permissions = PermissionsResponse(**permissions_dict)

        return DashboardConfigResponse(
            role=config.role,
            visible_panels=config.visible_panels,
            available_actions=config.available_actions,
            menu_items=menu_items,
            quick_stats=config.quick_stats,
            default_view=config.default_view,
            permissions=permissions,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role configuration: {str(e)}",
        )


@router.get("/menu", response_model=DashboardMenuResponse)
async def get_dashboard_menu(
    current_user: UserContext = Depends(get_current_user),
) -> DashboardMenuResponse:
    """
    GET /api/v1/dashboard/menu

    Get just the navigation menu items for the authenticated user's role.
    Lightweight endpoint useful for rendering navigation menus.

    Returns:
        DashboardMenuResponse with menu items and default view

    Example response:
        {
            "menu_items": [
                {"label": "Rosters", "icon": "calendar", "panel_id": "roster"},
                {"label": "Analytics", "icon": "chart-bar", "panel_id": "analytics"}
            ],
            "default_view": "roster"
        }
    """
    try:
        config = DashboardRouter.get_dashboard_config(role=current_user.role)

        menu_items = [
            MenuItemResponse(
                label=item.label,
                icon=item.icon,
                panel_id=item.panel_id,
                badge_count=item.badge_count,
            )
            for item in config.menu_items
        ]

        return DashboardMenuResponse(
            menu_items=menu_items,
            default_view=config.default_view,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role: {str(e)}",
        )


@router.get("/panels", response_model=PanelsResponse)
async def get_accessible_panels(
    current_user: UserContext = Depends(get_current_user),
) -> PanelsResponse:
    """
    GET /api/v1/dashboard/panels

    Get list of accessible dashboard panels for the authenticated user.

    Returns:
        PanelsResponse with role and panel list

    Example response:
        {
            "role": "manager",
            "panels": ["roster", "analytics", "conflicts", "staff", ...]
        }
    """
    try:
        panels = DashboardRouter.get_accessible_panels(role=current_user.role)
        return PanelsResponse(role=current_user.role, panels=panels)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role: {str(e)}",
        )


@router.get("/actions", response_model=ActionsResponse)
async def get_available_actions(
    current_user: UserContext = Depends(get_current_user),
) -> ActionsResponse:
    """
    GET /api/v1/dashboard/actions

    Get list of available actions for the authenticated user's role.

    Returns:
        ActionsResponse with role and available actions

    Example response:
        {
            "role": "manager",
            "actions": ["generate_roster", "approve_roster", "publish", ...]
        }
    """
    try:
        actions = DashboardRouter.get_available_actions(role=current_user.role)
        return ActionsResponse(role=current_user.role, actions=actions)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role: {str(e)}",
        )


@router.get("/permissions", response_model=PermissionsResponse)
async def get_permissions(
    current_user: UserContext = Depends(get_current_user),
) -> PermissionsResponse:
    """
    GET /api/v1/dashboard/permissions

    Get granular permission flags for the authenticated user's role.

    Returns:
        PermissionsResponse with all permission flags

    Example response:
        {
            "can_view_all_venues": false,
            "can_manage_users": false,
            "can_configure_system": false,
            "can_view_billing": false,
            "can_backup_restore": false,
            "can_view_analytics": true,
            "can_publish_roster": true,
            "can_manage_staff": true,
            "can_view_logs": false,
            "can_export_data": true,
            "can_access_api": false
        }
    """
    try:
        permissions = DashboardRouter.get_role_permissions(role=current_user.role)
        return PermissionsResponse(**permissions)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid role: {str(e)}",
        )


@router.post("/check-action", response_model=ActionCheckResponse)
async def check_action_permission(
    action: str,
    current_user: UserContext = Depends(get_current_user),
) -> ActionCheckResponse:
    """
    POST /api/v1/dashboard/check-action?action=generate_roster

    Check if the authenticated user can perform a specific action.

    Query Parameters:
        action: Action name to check

    Returns:
        ActionCheckResponse indicating if action is available

    Example response:
        {
            "role": "manager",
            "action": "generate_roster",
            "can_perform": true
        }
    """
    can_perform = DashboardRouter.can_perform_action(
        role=current_user.role,
        action=action,
    )

    return ActionCheckResponse(
        role=current_user.role,
        action=action,
        can_perform=can_perform,
    )


@router.post("/check-panel", response_model=PanelCheckResponse)
async def check_panel_access(
    panel_id: str,
    current_user: UserContext = Depends(get_current_user),
) -> PanelCheckResponse:
    """
    POST /api/v1/dashboard/check-panel?panel_id=analytics

    Check if the authenticated user can view a specific panel.

    Query Parameters:
        panel_id: Panel ID to check

    Returns:
        PanelCheckResponse indicating if panel is visible

    Example response:
        {
            "role": "manager",
            "panel_id": "analytics",
            "can_view": true
        }
    """
    can_view = DashboardRouter.can_view_panel(
        role=current_user.role,
        panel_id=panel_id,
    )

    return PanelCheckResponse(
        role=current_user.role,
        panel_id=panel_id,
        can_view=can_view,
    )


@router.post("/check-permission", response_model=PermissionCheckResponse)
async def check_permission(
    permission: str,
    current_user: UserContext = Depends(get_current_user),
) -> PermissionCheckResponse:
    """
    POST /api/v1/dashboard/check-permission?permission=can_publish_roster

    Check if the authenticated user has a specific permission flag.

    Query Parameters:
        permission: Permission flag name to check

    Returns:
        PermissionCheckResponse indicating if permission is granted

    Example response:
        {
            "role": "manager",
            "permission": "can_publish_roster",
            "has_permission": true
        }
    """
    has_permission = DashboardRouter.has_permission(
        role=current_user.role,
        permission=permission,
    )

    return PermissionCheckResponse(
        role=current_user.role,
        permission=permission,
        has_permission=has_permission,
    )
