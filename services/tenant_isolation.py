"""
Tenant isolation enforcement layer.

Wraps the BaseStore to enforce automatic venue_id filtering on all queries.
Blocks access to resources from unauthorized venues.
Provides audit logging for isolation violations.
"""

import logging
from datetime import date, datetime
from typing import Optional, List

from rosteriq.database import BaseStore
from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig, User,
)
from rosteriq.middleware.tenant import (
    get_tenant_context, get_tenant_context_optional,
    audit_cross_tenant_attempt,
)


logger = logging.getLogger(__name__)


class IsolationViolation(Exception):
    """Raised when a cross-tenant access is attempted."""

    def __init__(self, venue_id: str, resource_type: str, user_id: str):
        self.venue_id = venue_id
        self.resource_type = resource_type
        self.user_id = user_id
        super().__init__(
            f"User {user_id} attempted to access {resource_type} from venue {venue_id}"
        )


def validate_venue_access(user_venues: List[str], requested_venue: str, is_owner: bool = False) -> bool:
    """
    Validate that a user has access to a specific venue.

    Args:
        user_venues: List of venue IDs the user has access to
        requested_venue: The venue being requested
        is_owner: Whether user is a system owner (unrestricted)

    Returns:
        True if user has access, False otherwise
    """
    if is_owner:
        return True
    return requested_venue in user_venues


class TenantScopedDB:
    """
    Wrapper around BaseStore that enforces tenant isolation.

    Auto-filters all queries by current user's venue_ids.
    Blocks access to resources from unauthorized venues.
    """

    def __init__(self, store: BaseStore):
        """
        Initialize with a BaseStore instance.

        Args:
            store: The underlying database store to wrap
        """
        self._store = store

    def _get_current_venues(self) -> tuple[List[str], bool]:
        """
        Get current user's venue IDs and owner status.
        Returns (venue_ids, is_owner).
        """
        tenant = get_tenant_context_optional()
        if not tenant:
            # Auth-exempt paths (health, metrics, etc) won't have tenant context
            return ([], False)
        return (tenant.venue_ids, tenant.is_owner)

    def _check_venue_access(self, venue_id: str, resource_type: str) -> None:
        """
        Check if current user has access to the venue.
        Raises IsolationViolation if not.
        """
        if not venue_id:
            return  # No venue_id to check

        tenant = get_tenant_context_optional()
        if not tenant:
            # Auth-exempt paths won't have tenant context
            return

        if not validate_venue_access(tenant.venue_ids, venue_id, tenant.is_owner):
            # Log the violation
            audit_cross_tenant_attempt(venue_id, resource_type, "access")
            raise IsolationViolation(venue_id, resource_type, tenant.user_id)

    # ========================================================================
    # Venue operations
    # ========================================================================

    def save_venue(self, venue: VenueConfig) -> None:
        """Save a venue (auth required)."""
        self._check_venue_access(venue.id, "venue")
        return self._store.save_venue(venue)

    def list_venues(self) -> list[VenueConfig]:
        """List only venues the user has access to."""
        venues, is_owner = self._get_current_venues()

        all_venues = self._store.list_venues()
        if is_owner:
            return all_venues

        # Filter to user's venues
        return [v for v in all_venues if v.id in venues]

    def get_venue(self, venue_id: str) -> Optional[VenueConfig]:
        """Get a venue (must have access)."""
        self._check_venue_access(venue_id, "venue")
        return self._store.get_venue(venue_id)

    # ========================================================================
    # Employee operations
    # ========================================================================

    def save_employee(self, employee: Employee) -> None:
        """Save an employee (must belong to authorized venue)."""
        venue_id = getattr(employee, 'venue_id', None)
        self._check_venue_access(venue_id, "employee")
        return self._store.save_employee(employee)

    def save_employees(self, employees: list[Employee]) -> None:
        """Save multiple employees."""
        for emp in employees:
            self.save_employee(emp)

    def list_employees(self) -> list[Employee]:
        """List employees only from authorized venues."""
        venues, is_owner = self._get_current_venues()

        all_employees = self._store.list_employees()
        if is_owner:
            return all_employees

        # Filter to employees in user's venues
        return [e for e in all_employees if getattr(e, 'venue_id', None) in venues]

    def get_employee(self, employee_id: str) -> Optional[Employee]:
        """Get an employee (must verify venue access)."""
        emp = self._store.get_employee(employee_id)
        if not emp:
            return None

        venue_id = getattr(emp, 'venue_id', None)
        self._check_venue_access(venue_id, "employee")
        return emp

    def get_employees_dict(self) -> dict[str, Employee]:
        """Get employees dict for authorized venues only."""
        employees = self.list_employees()
        return {e.id: e for e in employees}

    # ========================================================================
    # Forecast operations
    # ========================================================================

    def add_forecasts(self, forecasts: list[DemandForecast]) -> None:
        """Add forecasts (must belong to authorized venue)."""
        for fc in forecasts:
            self._check_venue_access(fc.venue_id, "forecast")
        return self._store.add_forecasts(forecasts)

    def get_forecasts(
        self,
        venue_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[DemandForecast]:
        """
        Get forecasts for authorized venues only.
        If venue_id specified, verify access. Otherwise, return all user's forecasts.
        """
        if venue_id:
            self._check_venue_access(venue_id, "forecast")
            return self._store.get_forecasts(venue_id, start_date, end_date)

        # Get forecasts for all user's venues
        venues, is_owner = self._get_current_venues()
        all_forecasts = self._store.get_forecasts(None, start_date, end_date)

        if is_owner:
            return all_forecasts

        return [f for f in all_forecasts if f.venue_id in venues]

    # ========================================================================
    # Roster operations
    # ========================================================================

    def save_roster(self, roster: Roster) -> None:
        """Save a roster (must belong to authorized venue)."""
        self._check_venue_access(roster.venue_id, "roster")
        return self._store.save_roster(roster)

    def list_rosters(self) -> list[Roster]:
        """List rosters from authorized venues only."""
        venues, is_owner = self._get_current_venues()

        all_rosters = self._store.list_rosters()
        if is_owner:
            return all_rosters

        return [r for r in all_rosters if r.venue_id in venues]

    def get_roster(self, roster_id: str) -> Optional[Roster]:
        """Get a roster (must verify venue access)."""
        roster = self._store.get_roster(roster_id)
        if not roster:
            return None

        self._check_venue_access(roster.venue_id, "roster")
        return roster

    # ========================================================================
    # Webhook operations (pass-through, webhooks handle their own auth)
    # ========================================================================

    def is_webhook_processed(self, webhook_id: str) -> bool:
        """Check if webhook processed (auth-agnostic)."""
        return self._store.is_webhook_processed(webhook_id)

    def save_webhook_event(self, webhook_id: str, event_type: str, payload_hash: str) -> None:
        """Save webhook event (auth-agnostic)."""
        return self._store.save_webhook_event(webhook_id, event_type, payload_hash)

    # ========================================================================
    # User operations (cross-tenant, not filtered)
    # ========================================================================

    def save_user(self, user: dict) -> None:
        """Save a user (auth-agnostic, cross-tenant)."""
        return self._store.save_user(user)

    def get_user_by_email(self, email: str) -> Optional[dict]:
        """Get user by email (auth-agnostic, cross-tenant)."""
        return self._store.get_user_by_email(email)

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        """Get user by ID (auth-agnostic, cross-tenant)."""
        return self._store.get_user_by_id(user_id)

    def list_users(self) -> list[dict]:
        """List all users (owner-only in practice, but auth-agnostic here)."""
        return self._store.list_users()

    def save_refresh_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save refresh token (auth-agnostic)."""
        return self._store.save_refresh_token(token_hash, user_id, expires_at)

    def get_refresh_token(self, token_hash: str) -> Optional[dict]:
        """Get refresh token (auth-agnostic)."""
        return self._store.get_refresh_token(token_hash)

    def revoke_refresh_token(self, token_hash: str) -> None:
        """Revoke refresh token (auth-agnostic)."""
        return self._store.revoke_refresh_token(token_hash)

    def record_login_attempt(self, email: str, ip_address: str, success: bool) -> None:
        """Record login attempt (auth-agnostic)."""
        return self._store.record_login_attempt(email, ip_address, success)

    def check_login_rate_limit(self, ip_address: str, minutes: int = 1) -> int:
        """Check login rate limit (auth-agnostic)."""
        return self._store.check_login_rate_limit(ip_address, minutes)

    # ========================================================================
    # Subscription operations (venue-scoped)
    # ========================================================================

    def save_subscription(self, subscription: dict) -> None:
        """Save subscription (must belong to authorized venue)."""
        venue_id = subscription.get("venue_id")
        self._check_venue_access(venue_id, "subscription")
        return self._store.save_subscription(subscription)

    def get_subscription(self, venue_id: str) -> Optional[dict]:
        """Get subscription (must verify venue access)."""
        self._check_venue_access(venue_id, "subscription")
        return self._store.get_subscription(venue_id)

    def list_subscriptions(self) -> list[dict]:
        """List subscriptions for authorized venues only."""
        venues, is_owner = self._get_current_venues()

        all_subs = self._store.list_subscriptions()
        if is_owner:
            return all_subs

        return [s for s in all_subs if s.get("venue_id") in venues]

    # ========================================================================
    # Billing operations
    # ========================================================================

    def save_billing_event(self, event: dict) -> None:
        """Save billing event (must belong to authorized venue)."""
        venue_id = event.get("venue_id")
        if venue_id:
            self._check_venue_access(venue_id, "billing_event")
        return self._store.save_billing_event(event)

    # ========================================================================
    # Audit log operations (new)
    # ========================================================================

    def save_audit_log(self, entry: dict) -> None:
        """Save an audit log entry (venue-scoped)."""
        venue_id = entry.get("venue_id")
        if venue_id:
            self._check_venue_access(venue_id, "audit_log")
        return self._store.save_audit_log(entry)

    def list_audit_logs(
        self, venue_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """List audit logs for a venue (must have access)."""
        self._check_venue_access(venue_id, "audit_log")
        return self._store.list_audit_logs(venue_id, limit, offset)

    # ========================================================================
    # Delegate all other methods to underlying store
    # ========================================================================

    def __getattr__(self, name: str):
        """Delegate unknown methods to underlying store."""
        return getattr(self._store, name)
