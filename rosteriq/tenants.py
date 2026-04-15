"""
RosterIQ Multi-Tenant Layer

Provides:
- Tenant (customer organization) model with billing tiers and usage tracking
- TenantStore for CRUD operations and venue management
- Tenant-scoped access control (users belong to tenants, venues belong to tenants)
- Billing tier enforcement for premium features
- Demo-mode compatibility (single "demo-tenant-001" owns all demo venues)

MIGRATION NOTES:
The following endpoints should eventually use require_tenant_access() to enforce
tenant scoping as the product scales. This is NOT critical for MVP but prevents
future collision bugs:

  - /api/v1/rosters/generate (roster_router)
  - /api/v1/call-in/* (call_in_router)
  - /api/v1/shift-events/* (shift_events_router)
  - /api/v1/accountability/* (accountability_router)
  - /api/v1/forecasts (forecast_router)
  - /api/v1/awards/* (award_router)
  - /api/v1/ask/* (ask_router)
  - /api/v1/brief-subscriptions/* (brief_subscriptions_router)

Currently, per-venue in-memory stores (accountability_store, shift_events, call_in_store, etc.)
are keyed by venue_id only. The require_tenant_access guard ensures users can only
access venues that belong to their tenant, which is sufficient protection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# Tenant Data Models (stdlib dataclasses, no Pydantic)
# ============================================================================


class BillingTier(str, Enum):
    """Billing tier for tenant."""
    STARTUP = "startup"      # Free tier: 1 venue, basic features
    PRO = "pro"              # Mid tier: up to 10 venues, AI features
    ENTERPRISE = "enterprise"  # Premium tier: unlimited venues, all features


class TenantStatus(str, Enum):
    """Tenant operational status."""
    TRIAL = "trial"
    ACTIVE = "active"
    SUSPENDED = "suspended"


@dataclass
class Tenant:
    """
    Represents a customer organization.

    Attributes:
        tenant_id: Unique identifier for the tenant
        name: Human-readable tenant name (e.g., "Varsity Bars Group")
        slug: URL-safe identifier derived from name
        created_at: Timestamp when tenant was created
        billing_tier: Current billing tier (startup/pro/enterprise)
        status: Operational status (trial/active/suspended)
        trial_ends_at: When trial period expires (None if not in trial)
        owner_user_id: ID of the user who owns this tenant
        venue_ids: List of venue IDs owned by this tenant
        max_venues: Maximum venues allowed by tier
        max_employees: Maximum employees across all venues
        contact_email: Billing/support contact email
        notes: Dict for arbitrary metadata (custom fields, flags, etc.)
    """
    tenant_id: str
    name: str
    slug: str
    created_at: datetime
    billing_tier: BillingTier = BillingTier.STARTUP
    status: TenantStatus = TenantStatus.ACTIVE
    trial_ends_at: Optional[datetime] = None
    owner_user_id: Optional[str] = None
    venue_ids: list[str] = field(default_factory=list)
    max_venues: int = 1  # 1 for startup, 10 for pro, unlimited for enterprise
    max_employees: int = 50
    contact_email: str = ""
    notes: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Tenant:
        """Reconstruct Tenant from dict (e.g., from JSON)."""
        # Parse enums
        if isinstance(data.get("billing_tier"), str):
            data["billing_tier"] = BillingTier(data["billing_tier"])
        if isinstance(data.get("status"), str):
            data["status"] = TenantStatus(data["status"])
        # Parse datetimes if they're strings
        for field_name in ["created_at", "trial_ends_at"]:
            if field_name in data and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "tenant_id": self.tenant_id,
            "name": self.name,
            "slug": self.slug,
            "created_at": self.created_at.isoformat(),
            "billing_tier": self.billing_tier.value,
            "status": self.status.value,
            "trial_ends_at": self.trial_ends_at.isoformat() if self.trial_ends_at else None,
            "owner_user_id": self.owner_user_id,
            "venue_ids": self.venue_ids,
            "max_venues": self.max_venues,
            "max_employees": self.max_employees,
            "contact_email": self.contact_email,
            "notes": self.notes,
        }


@dataclass
class TenantUsageSnapshot:
    """
    Point-in-time usage snapshot for a tenant.

    Attributes:
        tenant_id: Tenant this snapshot belongs to
        snapshot_date: Date of the snapshot (YYYY-MM-DD)
        active_venues: Number of venues actively used
        total_employees: Total distinct employees across venues
        rosters_generated_month: Rosters generated in this month
        ask_queries_month: Ask agent queries in this month
        call_ins_sent_month: Call-in messages sent in this month
        sms_credits_used: SMS credits consumed
        billable_amount: Computed billable amount (USD)
    """
    tenant_id: str
    snapshot_date: str  # YYYY-MM-DD
    active_venues: int = 0
    total_employees: int = 0
    rosters_generated_month: int = 0
    ask_queries_month: int = 0
    call_ins_sent_month: int = 0
    sms_credits_used: int = 0
    billable_amount: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "tenant_id": self.tenant_id,
            "snapshot_date": self.snapshot_date,
            "active_venues": self.active_venues,
            "total_employees": self.total_employees,
            "rosters_generated_month": self.rosters_generated_month,
            "ask_queries_month": self.ask_queries_month,
            "call_ins_sent_month": self.call_ins_sent_month,
            "sms_credits_used": self.sms_credits_used,
            "billable_amount": self.billable_amount,
        }


# ============================================================================
# Tenant Store (in-memory, stdlib-only)
# ============================================================================


class TenantStore:
    """
    In-memory store for tenants and usage data.

    Keyed by tenant_id. All operations are in-memory; for production,
    replace with database operations.
    """

    def __init__(self):
        """Initialize empty store."""
        self._tenants: dict[str, Tenant] = {}  # tenant_id -> Tenant
        self._venue_to_tenant: dict[str, str] = {}  # venue_id -> tenant_id (for fast lookup)
        self._usage_snapshots: dict[str, list[TenantUsageSnapshot]] = {}  # tenant_id -> [snapshots]
        self._initialized = False

    def _seed_demo_tenant(self) -> None:
        """
        Seed the demo tenant on first access.

        Creates "demo-tenant-001" owning all known demo venue IDs.
        This ensures existing demo flows work without tenant setup.
        """
        if self._initialized:
            return

        self._initialized = True

        # List of known demo venues (from api_v2.py venues list)
        demo_venues = [
            "venue_demo_001",  # Mojo's Bar
            "venue_demo_002",  # Earl's Kitchen
            "venue_demo_003",  # Francine's
            "venue-royal-oak",  # From auth demo user
        ]

        demo_tenant = Tenant(
            tenant_id="demo-tenant-001",
            name="Demo Tenant",
            slug="demo-tenant-001",
            created_at=datetime.now(timezone.utc),
            billing_tier=BillingTier.ENTERPRISE,  # Demo can use all features
            status=TenantStatus.ACTIVE,
            owner_user_id="demo-user",
            venue_ids=demo_venues,
            max_venues=999,
            max_employees=9999,
            contact_email="demo@rosteriq.local",
            notes={"is_demo": True},
        )

        self._tenants["demo-tenant-001"] = demo_tenant
        for venue_id in demo_venues:
            self._venue_to_tenant[venue_id] = "demo-tenant-001"

        logger.info(f"Seeded demo tenant with {len(demo_venues)} demo venues")

    def create(
        self,
        tenant_id: str,
        name: str,
        slug: str,
        billing_tier: BillingTier = BillingTier.STARTUP,
        owner_user_id: Optional[str] = None,
        contact_email: str = "",
        notes: Optional[dict[str, Any]] = None,
    ) -> Tenant:
        """
        Create a new tenant.

        Args:
            tenant_id: Unique tenant identifier
            name: Human-readable name
            slug: URL-safe slug
            billing_tier: Billing tier (default: startup)
            owner_user_id: Owning user ID
            contact_email: Contact email
            notes: Arbitrary metadata

        Returns:
            Created Tenant

        Raises:
            ValueError: If tenant_id already exists
        """
        if tenant_id in self._tenants:
            raise ValueError(f"Tenant {tenant_id} already exists")

        # Determine max_venues from tier
        max_venues_by_tier = {
            BillingTier.STARTUP: 1,
            BillingTier.PRO: 10,
            BillingTier.ENTERPRISE: 9999,
        }

        tenant = Tenant(
            tenant_id=tenant_id,
            name=name,
            slug=slug,
            created_at=datetime.now(timezone.utc),
            billing_tier=billing_tier,
            status=TenantStatus.ACTIVE,
            owner_user_id=owner_user_id,
            venue_ids=[],
            max_venues=max_venues_by_tier.get(billing_tier, 1),
            contact_email=contact_email,
            notes=notes or {},
        )

        self._tenants[tenant_id] = tenant
        logger.info(f"Created tenant {tenant_id} ({name}) with tier {billing_tier.value}")
        return tenant

    def get(self, tenant_id: str) -> Optional[Tenant]:
        """Retrieve tenant by ID."""
        self._seed_demo_tenant()
        return self._tenants.get(tenant_id)

    def get_by_slug(self, slug: str) -> Optional[Tenant]:
        """Retrieve tenant by URL-safe slug."""
        self._seed_demo_tenant()
        for tenant in self._tenants.values():
            if tenant.slug == slug:
                return tenant
        return None

    def list_all(self) -> list[Tenant]:
        """List all tenants."""
        self._seed_demo_tenant()
        return list(self._tenants.values())

    def update(
        self,
        tenant_id: str,
        **fields,
    ) -> Optional[Tenant]:
        """
        Update a tenant's fields.

        Args:
            tenant_id: Tenant to update
            **fields: Fields to update (name, contact_email, notes, billing_tier, status, etc.)

        Returns:
            Updated Tenant, or None if not found
        """
        tenant = self.get(tenant_id)
        if not tenant:
            return None

        # Use dataclass replace to create updated copy
        tenant = replace(tenant, **fields)
        self._tenants[tenant_id] = tenant
        logger.info(f"Updated tenant {tenant_id}: {list(fields.keys())}")
        return tenant

    def suspend(self, tenant_id: str) -> Optional[Tenant]:
        """Suspend a tenant (prevent data access)."""
        return self.update(tenant_id, status=TenantStatus.SUSPENDED)

    def activate(self, tenant_id: str) -> Optional[Tenant]:
        """Activate a suspended tenant."""
        return self.update(tenant_id, status=TenantStatus.ACTIVE)

    def delete(self, tenant_id: str) -> bool:
        """
        Delete a tenant (irreversible).

        Returns:
            True if deleted, False if not found
        """
        if tenant_id not in self._tenants:
            return False

        tenant = self._tenants.pop(tenant_id)
        # Remove venue mappings
        for venue_id in tenant.venue_ids:
            self._venue_to_tenant.pop(venue_id, None)
        # Remove usage snapshots
        self._usage_snapshots.pop(tenant_id, None)

        logger.warning(f"Deleted tenant {tenant_id} ({tenant.name})")
        return True

    def add_venue(self, tenant_id: str, venue_id: str) -> Optional[Tenant]:
        """
        Add a venue to a tenant.

        Args:
            tenant_id: Tenant to add venue to
            venue_id: Venue ID to add

        Returns:
            Updated Tenant

        Raises:
            ValueError: If venue_id already belongs to another tenant or max_venues exceeded
        """
        tenant = self.get(tenant_id)
        if not tenant:
            raise ValueError(f"Tenant {tenant_id} not found")

        if venue_id in self._venue_to_tenant:
            other_tenant = self._venue_to_tenant[venue_id]
            if other_tenant != tenant_id:
                raise ValueError(f"Venue {venue_id} already belongs to tenant {other_tenant}")
            return tenant  # Already in this tenant

        if len(tenant.venue_ids) >= tenant.max_venues:
            raise ValueError(
                f"Tenant {tenant_id} has reached max venues limit ({tenant.max_venues})"
            )

        venue_ids = tenant.venue_ids + [venue_id]
        tenant = self.update(tenant_id, venue_ids=venue_ids)
        self._venue_to_tenant[venue_id] = tenant_id

        logger.info(f"Added venue {venue_id} to tenant {tenant_id}")
        return tenant

    def remove_venue(self, tenant_id: str, venue_id: str) -> Optional[Tenant]:
        """
        Remove a venue from a tenant.

        This is irreversible and will prevent access to that venue.

        Args:
            tenant_id: Tenant to remove venue from
            venue_id: Venue ID to remove

        Returns:
            Updated Tenant
        """
        tenant = self.get(tenant_id)
        if not tenant or venue_id not in tenant.venue_ids:
            return tenant

        venue_ids = [v for v in tenant.venue_ids if v != venue_id]
        tenant = self.update(tenant_id, venue_ids=venue_ids)
        self._venue_to_tenant.pop(venue_id, None)

        logger.warning(f"Removed venue {venue_id} from tenant {tenant_id}")
        return tenant

    def find_tenant_for_venue(self, venue_id: str) -> Optional[Tenant]:
        """
        Find the tenant that owns a venue.

        Args:
            venue_id: Venue ID to look up

        Returns:
            Tenant that owns the venue, or None if not found
        """
        self._seed_demo_tenant()
        tenant_id = self._venue_to_tenant.get(venue_id)
        if tenant_id:
            return self.get(tenant_id)
        return None

    def assert_venue_in_tenant(self, venue_id: str, tenant_id: str) -> None:
        """
        Verify that a venue belongs to a tenant.

        Args:
            venue_id: Venue to check
            tenant_id: Tenant it should belong to

        Raises:
            ValueError: If venue does not belong to tenant
        """
        actual_tenant = self.find_tenant_for_venue(venue_id)
        if not actual_tenant or actual_tenant.tenant_id != tenant_id:
            raise ValueError(
                f"Venue {venue_id} is not in tenant {tenant_id}"
                f" (actual: {actual_tenant.tenant_id if actual_tenant else 'none'})"
            )

    def record_usage(self, snapshot: TenantUsageSnapshot) -> None:
        """
        Record a usage snapshot for a tenant.

        Args:
            snapshot: TenantUsageSnapshot to record
        """
        if snapshot.tenant_id not in self._usage_snapshots:
            self._usage_snapshots[snapshot.tenant_id] = []

        self._usage_snapshots[snapshot.tenant_id].append(snapshot)
        logger.debug(f"Recorded usage snapshot for tenant {snapshot.tenant_id} on {snapshot.snapshot_date}")

    def get_usage(self, tenant_id: str, month: Optional[str] = None) -> Optional[TenantUsageSnapshot]:
        """
        Get usage snapshot for a tenant.

        Args:
            tenant_id: Tenant to get usage for
            month: Optional month filter (YYYY-MM)

        Returns:
            Most recent matching snapshot, or None if no snapshots
        """
        snapshots = self._usage_snapshots.get(tenant_id, [])
        if not snapshots:
            return None

        if month:
            # Filter to month
            snapshots = [s for s in snapshots if s.snapshot_date.startswith(month)]

        return snapshots[-1] if snapshots else None

    def clear(self) -> None:
        """Clear all tenants and usage data (for testing)."""
        self._tenants.clear()
        self._venue_to_tenant.clear()
        self._usage_snapshots.clear()
        self._initialized = False
        logger.debug("Cleared all tenant data")


# ============================================================================
# Module Singleton
# ============================================================================

_tenant_store_instance: Optional[TenantStore] = None


def get_tenant_store() -> TenantStore:
    """
    Get or create the module-level TenantStore singleton.

    Returns:
        Global TenantStore instance
    """
    global _tenant_store_instance
    if _tenant_store_instance is None:
        _tenant_store_instance = TenantStore()
        # Seed demo tenant on first access
        _tenant_store_instance._seed_demo_tenant()
    return _tenant_store_instance


# ============================================================================
# Billing Tier Enforcement
# ============================================================================


def check_tier_allows(tenant: Tenant, feature: str) -> tuple[bool, Optional[str]]:
    """
    Check if a tenant's billing tier allows a feature.

    Feature matrix:
    - "real_time_notifications": enterprise only
    - "multi_venue": enterprise only (OWNER can see multiple venues)
    - "conversational_ai": pro + enterprise
    - "scenario_solver": pro + enterprise
    - "extended_forecast_horizon": pro + enterprise
      (basic = 7 days, pro = 14, enterprise = 30)
    - Everything else: all tiers

    Args:
        tenant: Tenant to check
        feature: Feature name

    Returns:
        Tuple of (allowed: bool, reason_if_not: Optional[str])
        If allowed=True, reason is None.
        If allowed=False, reason explains why (for user-facing messages).
    """
    tier = tenant.billing_tier

    # Enterprise-only features
    if feature in ("real_time_notifications", "multi_venue"):
        if tier == BillingTier.ENTERPRISE:
            return (True, None)
        return (False, f"Feature '{feature}' requires Enterprise plan")

    # Pro + Enterprise features
    if feature in ("conversational_ai", "scenario_solver", "extended_forecast_horizon"):
        if tier in (BillingTier.PRO, BillingTier.ENTERPRISE):
            return (True, None)
        return (False, f"Feature '{feature}' requires Pro plan or higher")

    # Everything else is available on all tiers
    return (True, None)


if __name__ == "__main__":
    # Quick smoke test
    print("Testing TenantStore...")
    store = get_tenant_store()

    # Demo tenant should auto-seed
    demo = store.get("demo-tenant-001")
    print(f"Demo tenant: {demo.name} with {len(demo.venue_ids)} venues")

    # Create a new tenant
    new_tenant = store.create(
        "test-tenant-123",
        "Test Venues Inc",
        "test-venues-inc",
        billing_tier=BillingTier.PRO,
        owner_user_id="owner-456",
    )
    print(f"Created: {new_tenant.name} (tier: {new_tenant.billing_tier.value})")

    # Add venues
    store.add_venue("test-tenant-123", "venue-test-1")
    store.add_venue("test-tenant-123", "venue-test-2")
    updated = store.get("test-tenant-123")
    print(f"After adding venues: {len(updated.venue_ids)} venues")

    # Check tier
    allowed, reason = check_tier_allows(new_tenant, "conversational_ai")
    print(f"conversational_ai allowed: {allowed}")

    print("All tests passed!")
