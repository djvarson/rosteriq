"""
Role-based dashboard routing service for RosterIQ.

Provides configuration and navigation structure for different user roles:
- Owner: Full system access across all venues
- Manager: Venue-scoped access to roster management
- Staff: Self-service shift and availability management
- Admin: System administration and monitoring
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class MenuItem:
    """Navigation menu item configuration."""

    label: str
    icon: str
    panel_id: str
    badge_count: Optional[int] = None
    requires_role: List[str] = field(default_factory=list)
    requires_venue_id: bool = False


@dataclass
class DashboardConfig:
    """Dashboard configuration for a specific user role."""

    role: str
    visible_panels: List[str] = field(default_factory=list)
    available_actions: List[str] = field(default_factory=list)
    menu_items: List[MenuItem] = field(default_factory=list)
    quick_stats: List[str] = field(default_factory=list)
    default_view: str = "overview"
    permissions: Dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "role": self.role,
            "visible_panels": self.visible_panels,
            "available_actions": self.available_actions,
            "menu_items": [
                {
                    "label": item.label,
                    "icon": item.icon,
                    "panel_id": item.panel_id,
                    "badge_count": item.badge_count,
                }
                for item in self.menu_items
            ],
            "quick_stats": self.quick_stats,
            "default_view": self.default_view,
            "permissions": self.permissions,
        }


# ============================================================================
# Dashboard Router Service
# ============================================================================

class DashboardRouter:
    """Routes dashboard configuration and navigation based on user role."""

    # Panel definitions per role
    ROLE_PANELS = {
        "owner": [
            "roster",
            "analytics",
            "billing",
            "backup",
            "conflicts",
            "optimiser",
            "notifications",
            "staff",
            "settings",
        ],
        "manager": [
            "roster",
            "analytics",
            "conflicts",
            "staff",
            "shift_bidding",
            "approvals",
        ],
        "staff": [
            "my_shifts",
            "availability",
            "swap_requests",
            "leave",
            "documents",
            "notifications",
        ],
        "admin": [
            "roster",
            "analytics",
            "billing",
            "backup",
            "conflicts",
            "optimiser",
            "notifications",
            "staff",
            "settings",
            "logs",
            "db_health",
            "api_metrics",
            "user_management",
        ],
    }

    # Actions per role
    ROLE_ACTIONS = {
        "owner": [
            "generate_roster",
            "approve_roster",
            "publish",
            "backup",
            "restore",
            "manage_billing",
            "manage_staff",
            "configure_system",
            "view_analytics",
            "export_data",
        ],
        "manager": [
            "generate_roster",
            "approve_roster",
            "publish",
            "manage_shifts",
            "approve_swaps",
            "approve_leave",
            "view_analytics",
            "manage_staff",
        ],
        "staff": [
            "update_availability",
            "request_swap",
            "request_leave",
            "bid_on_shift",
            "update_profile",
            "view_shifts",
        ],
        "admin": [
            "generate_roster",
            "approve_roster",
            "publish",
            "backup",
            "restore",
            "manage_billing",
            "manage_staff",
            "configure_system",
            "view_analytics",
            "export_data",
            "manage_users",
            "view_logs",
            "manage_db",
            "view_system_health",
        ],
    }

    # Quick stats per role
    ROLE_QUICK_STATS = {
        "owner": [
            "revenue",
            "labour_percentage",
            "forecast_accuracy",
            "venues_count",
            "total_staff",
        ],
        "manager": [
            "todays_roster",
            "labour_percentage",
            "conflicts_count",
            "pending_approvals",
        ],
        "staff": [
            "next_shift",
            "hours_this_week",
            "upcoming_leave",
        ],
        "admin": [
            "revenue",
            "labour_percentage",
            "forecast_accuracy",
            "venues_count",
            "total_staff",
            "system_health",
            "api_health",
        ],
    }

    # Permission flags per role
    ROLE_PERMISSIONS = {
        "owner": {
            "can_view_all_venues": True,
            "can_manage_users": True,
            "can_configure_system": True,
            "can_view_billing": True,
            "can_backup_restore": True,
            "can_view_analytics": True,
            "can_publish_roster": True,
            "can_manage_staff": True,
            "can_view_logs": True,
            "can_export_data": True,
            "can_access_api": True,
        },
        "manager": {
            "can_view_all_venues": False,
            "can_manage_users": False,
            "can_configure_system": False,
            "can_view_billing": False,
            "can_backup_restore": False,
            "can_view_analytics": True,
            "can_publish_roster": True,
            "can_manage_staff": True,
            "can_view_logs": False,
            "can_export_data": True,
            "can_access_api": False,
        },
        "staff": {
            "can_view_all_venues": False,
            "can_manage_users": False,
            "can_configure_system": False,
            "can_view_billing": False,
            "can_backup_restore": False,
            "can_view_analytics": False,
            "can_publish_roster": False,
            "can_manage_staff": False,
            "can_view_logs": False,
            "can_export_data": False,
            "can_access_api": False,
        },
        "admin": {
            "can_view_all_venues": True,
            "can_manage_users": True,
            "can_configure_system": True,
            "can_view_billing": True,
            "can_backup_restore": True,
            "can_view_analytics": True,
            "can_publish_roster": True,
            "can_manage_staff": True,
            "can_view_logs": True,
            "can_export_data": True,
            "can_access_api": True,
        },
    }

    @staticmethod
    def get_dashboard_config(role: str, venue_id: Optional[str] = None) -> DashboardConfig:
        """
        Get dashboard configuration for a user role.

        Args:
            role: User role ('owner', 'manager', 'staff', 'admin')
            venue_id: Optional venue ID for manager-scoped configurations

        Returns:
            DashboardConfig with role-specific layout and permissions

        Raises:
            ValueError: If role is not recognized
        """
        if role not in DashboardRouter.ROLE_PANELS:
            raise ValueError(f"Unknown role: {role}")

        visible_panels = DashboardRouter.ROLE_PANELS[role]
        available_actions = DashboardRouter.ROLE_ACTIONS[role]
        quick_stats = DashboardRouter.ROLE_QUICK_STATS[role]
        permissions = DashboardRouter.ROLE_PERMISSIONS[role]

        # Build menu items
        menu_items = DashboardRouter._build_menu_for_role(role)

        # Determine default view
        default_view = DashboardRouter._get_default_view_for_role(role)

        return DashboardConfig(
            role=role,
            visible_panels=visible_panels,
            available_actions=available_actions,
            menu_items=menu_items,
            quick_stats=quick_stats,
            default_view=default_view,
            permissions=permissions,
        )

    @staticmethod
    def _build_menu_for_role(role: str) -> List[MenuItem]:
        """
        Build navigation menu for a specific role.

        Args:
            role: User role

        Returns:
            List of MenuItem objects
        """
        menu_config = {
            "owner": [
                MenuItem(
                    label="Dashboard",
                    icon="dashboard",
                    panel_id="overview",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Rosters",
                    icon="calendar",
                    panel_id="roster",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Analytics",
                    icon="chart-bar",
                    panel_id="analytics",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Conflicts",
                    icon="alert-circle",
                    panel_id="conflicts",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Optimiser",
                    icon="zap",
                    panel_id="optimiser",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Staff",
                    icon="users",
                    panel_id="staff",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Billing",
                    icon="credit-card",
                    panel_id="billing",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Backup",
                    icon="download",
                    panel_id="backup",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Notifications",
                    icon="bell",
                    panel_id="notifications",
                    requires_role=["owner"],
                ),
                MenuItem(
                    label="Settings",
                    icon="settings",
                    panel_id="settings",
                    requires_role=["owner"],
                ),
            ],
            "manager": [
                MenuItem(
                    label="Dashboard",
                    icon="dashboard",
                    panel_id="overview",
                    requires_role=["manager"],
                ),
                MenuItem(
                    label="Rosters",
                    icon="calendar",
                    panel_id="roster",
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
                MenuItem(
                    label="Approvals",
                    icon="check-circle",
                    panel_id="approvals",
                    badge_count=0,
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
                MenuItem(
                    label="Conflicts",
                    icon="alert-circle",
                    panel_id="conflicts",
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
                MenuItem(
                    label="Analytics",
                    icon="chart-bar",
                    panel_id="analytics",
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
                MenuItem(
                    label="Shift Bidding",
                    icon="trending-up",
                    panel_id="shift_bidding",
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
                MenuItem(
                    label="Staff",
                    icon="users",
                    panel_id="staff",
                    requires_role=["manager"],
                    requires_venue_id=True,
                ),
            ],
            "staff": [
                MenuItem(
                    label="My Dashboard",
                    icon="dashboard",
                    panel_id="overview",
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="My Shifts",
                    icon="calendar",
                    panel_id="my_shifts",
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="Availability",
                    icon="clock",
                    panel_id="availability",
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="Swap Requests",
                    icon="shuffle",
                    panel_id="swap_requests",
                    badge_count=0,
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="Leave",
                    icon="umbrella",
                    panel_id="leave",
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="Documents",
                    icon="file",
                    panel_id="documents",
                    requires_role=["staff"],
                ),
                MenuItem(
                    label="Notifications",
                    icon="bell",
                    panel_id="notifications",
                    badge_count=0,
                    requires_role=["staff"],
                ),
            ],
            "admin": [
                MenuItem(
                    label="Dashboard",
                    icon="dashboard",
                    panel_id="overview",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Rosters",
                    icon="calendar",
                    panel_id="roster",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Analytics",
                    icon="chart-bar",
                    panel_id="analytics",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Conflicts",
                    icon="alert-circle",
                    panel_id="conflicts",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Optimiser",
                    icon="zap",
                    panel_id="optimiser",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Staff",
                    icon="users",
                    panel_id="staff",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Billing",
                    icon="credit-card",
                    panel_id="billing",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Backup",
                    icon="download",
                    panel_id="backup",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Notifications",
                    icon="bell",
                    panel_id="notifications",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Settings",
                    icon="settings",
                    panel_id="settings",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="System Logs",
                    icon="log",
                    panel_id="logs",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Database Health",
                    icon="database",
                    panel_id="db_health",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="API Metrics",
                    icon="activity",
                    panel_id="api_metrics",
                    requires_role=["admin"],
                ),
                MenuItem(
                    label="Users",
                    icon="user-check",
                    panel_id="user_management",
                    requires_role=["admin"],
                ),
            ],
        }

        return menu_config.get(role, [])

    @staticmethod
    def _get_default_view_for_role(role: str) -> str:
        """
        Get the default dashboard view for a role.

        Args:
            role: User role

        Returns:
            Default view panel ID
        """
        defaults = {
            "owner": "overview",
            "manager": "roster",
            "staff": "my_shifts",
            "admin": "overview",
        }
        return defaults.get(role, "overview")

    @staticmethod
    def get_role_permissions(role: str) -> Dict[str, bool]:
        """
        Get granular permission flags for a role.

        Args:
            role: User role

        Returns:
            Dictionary of permission flags
        """
        if role not in DashboardRouter.ROLE_PERMISSIONS:
            raise ValueError(f"Unknown role: {role}")

        return DashboardRouter.ROLE_PERMISSIONS[role]

    @staticmethod
    def get_accessible_panels(role: str) -> List[str]:
        """
        Get list of accessible panels for a role.

        Args:
            role: User role

        Returns:
            List of panel IDs
        """
        if role not in DashboardRouter.ROLE_PANELS:
            raise ValueError(f"Unknown role: {role}")

        return DashboardRouter.ROLE_PANELS[role]

    @staticmethod
    def get_available_actions(role: str) -> List[str]:
        """
        Get list of available actions for a role.

        Args:
            role: User role

        Returns:
            List of action names
        """
        if role not in DashboardRouter.ROLE_ACTIONS:
            raise ValueError(f"Unknown role: {role}")

        return DashboardRouter.ROLE_ACTIONS[role]

    @staticmethod
    def can_perform_action(role: str, action: str) -> bool:
        """
        Check if a role can perform a specific action.

        Args:
            role: User role
            action: Action name

        Returns:
            True if action is available for role, False otherwise
        """
        if role not in DashboardRouter.ROLE_ACTIONS:
            return False

        return action in DashboardRouter.ROLE_ACTIONS[role]

    @staticmethod
    def can_view_panel(role: str, panel_id: str) -> bool:
        """
        Check if a role can view a specific panel.

        Args:
            role: User role
            panel_id: Panel ID

        Returns:
            True if panel is visible to role, False otherwise
        """
        if role not in DashboardRouter.ROLE_PANELS:
            return False

        return panel_id in DashboardRouter.ROLE_PANELS[role]

    @staticmethod
    def has_permission(role: str, permission: str) -> bool:
        """
        Check if a role has a specific permission.

        Args:
            role: User role
            permission: Permission flag name

        Returns:
            True if permission is granted to role, False otherwise
        """
        if role not in DashboardRouter.ROLE_PERMISSIONS:
            return False

        return DashboardRouter.ROLE_PERMISSIONS[role].get(permission, False)
