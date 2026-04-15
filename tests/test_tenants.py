"""
Tests for rosteriq.tenants — TenantStore CRUD, scoping, billing tiers.

Runs with: python tests/test_tenants.py
Pure-stdlib runner — no pytest, no FastAPI required at test-collection time.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq import auth  # noqa: E402
from rosteriq.tenants import (  # noqa: E402
    Tenant,
    TenantStore,
    TenantStatus,
    BillingTier,
    TenantUsageSnapshot,
    get_tenant_store,
    check_tier_allows,
)


def _reset_auth():
    """Clear auth in-memory stores."""
    auth._users.clear()
    auth._api_keys.clear()
    auth._failed_login_attempts.clear()


def _reset_tenants():
    """Clear tenant store."""
    # Reset the singleton
    import rosteriq.tenants as tenants_module
    tenants_module._tenant_store_instance = None
    # Get fresh instance (which will auto-seed demo tenant)
    get_tenant_store().clear()


# ---------------------------------------------------------------------------
# TenantStore CRUD
# ---------------------------------------------------------------------------


def test_tenant_store_create():
    """Test creating a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    tenant = store.create(
        "test-org-1",
        "Test Organization",
        "test-org",
        billing_tier=BillingTier.PRO,
        owner_user_id="user-123",
        contact_email="contact@test.org",
    )

    assert tenant.tenant_id == "test-org-1"
    assert tenant.name == "Test Organization"
    assert tenant.billing_tier == BillingTier.PRO
    assert tenant.status == TenantStatus.ACTIVE
    assert tenant.owner_user_id == "user-123"
    assert tenant.contact_email == "contact@test.org"
    assert tenant.venue_ids == []
    assert tenant.max_venues == 10  # Pro tier


def test_tenant_store_get():
    """Test retrieving a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    created = store.create(
        "test-get",
        "Get Test",
        "get-test",
    )

    retrieved = store.get("test-get")
    assert retrieved is not None
    assert retrieved.tenant_id == "test-get"
    assert retrieved.name == "Get Test"


def test_tenant_store_get_by_slug():
    """Test retrieving tenant by slug."""
    _reset_tenants()
    store = get_tenant_store()

    store.create(
        "slug-test-1",
        "Slug Test",
        "slug-test",
    )

    retrieved = store.get_by_slug("slug-test")
    assert retrieved is not None
    assert retrieved.tenant_id == "slug-test-1"


def test_tenant_store_list_all():
    """Test listing all tenants."""
    _reset_tenants()
    store = get_tenant_store()

    # Demo tenant auto-seeded
    store.create("tenant-1", "Tenant 1", "tenant-1")
    store.create("tenant-2", "Tenant 2", "tenant-2")

    all_tenants = store.list_all()
    assert len(all_tenants) >= 3  # demo + 2 created
    tenant_ids = [t.tenant_id for t in all_tenants]
    assert "demo-tenant-001" in tenant_ids
    assert "tenant-1" in tenant_ids
    assert "tenant-2" in tenant_ids


def test_tenant_store_update():
    """Test updating a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("update-test", "Original", "original")

    updated = store.update(
        "update-test",
        name="Updated",
        contact_email="new@test.org",
    )

    assert updated.name == "Updated"
    assert updated.contact_email == "new@test.org"

    # Verify persistence
    retrieved = store.get("update-test")
    assert retrieved.name == "Updated"


def test_tenant_store_delete():
    """Test deleting a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("delete-test", "To Delete", "delete-test")
    assert store.get("delete-test") is not None

    result = store.delete("delete-test")
    assert result is True
    assert store.get("delete-test") is None


def test_tenant_store_suspend_activate():
    """Test suspending and activating tenants."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("suspend-test", "Suspend Test", "suspend-test")

    suspended = store.suspend("suspend-test")
    assert suspended.status == TenantStatus.SUSPENDED

    activated = store.activate("suspend-test")
    assert activated.status == TenantStatus.ACTIVE


# ---------------------------------------------------------------------------
# Venue Management
# ---------------------------------------------------------------------------


def test_add_venue_to_tenant():
    """Test adding a venue to a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("venue-test", "Venue Test", "venue-test")
    tenant = store.add_venue("venue-test", "venue-123")

    assert "venue-123" in tenant.venue_ids
    assert len(tenant.venue_ids) == 1


def test_add_venue_enforces_max_venues():
    """Test max_venues limit is enforced."""
    _reset_tenants()
    store = get_tenant_store()

    # Startup tier has max_venues=1
    store.create(
        "max-test",
        "Max Test",
        "max-test",
        billing_tier=BillingTier.STARTUP,
    )

    store.add_venue("max-test", "venue-1")

    # Try to add second venue; should fail
    try:
        store.add_venue("max-test", "venue-2")
        raise AssertionError("Expected ValueError for max_venues exceeded")
    except ValueError as e:
        assert "max venues limit" in str(e).lower()


def test_add_venue_detects_collision():
    """Test that adding a venue already in another tenant fails."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("tenant-a", "Tenant A", "tenant-a")
    store.create("tenant-b", "Tenant B", "tenant-b", billing_tier=BillingTier.PRO)

    store.add_venue("tenant-a", "shared-venue")

    # Try to add same venue to tenant-b
    try:
        store.add_venue("tenant-b", "shared-venue")
        raise AssertionError("Expected ValueError for venue already in another tenant")
    except ValueError as e:
        assert "already belongs to tenant" in str(e).lower()


def test_find_tenant_for_venue():
    """Test finding the tenant that owns a venue."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("find-test", "Find Test", "find-test")
    store.add_venue("find-test", "venue-x")

    found = store.find_tenant_for_venue("venue-x")
    assert found is not None
    assert found.tenant_id == "find-test"

    # Non-existent venue
    not_found = store.find_tenant_for_venue("venue-nonexistent")
    assert not_found is None


def test_remove_venue():
    """Test removing a venue from a tenant."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("remove-test", "Remove Test", "remove-test", billing_tier=BillingTier.PRO)
    store.add_venue("remove-test", "venue-a")
    store.add_venue("remove-test", "venue-b")

    tenant = store.get("remove-test")
    assert len(tenant.venue_ids) == 2

    updated = store.remove_venue("remove-test", "venue-a")
    assert "venue-a" not in updated.venue_ids
    assert "venue-b" in updated.venue_ids


def test_assert_venue_in_tenant_valid():
    """Test assert_venue_in_tenant with valid venue."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("assert-test", "Assert Test", "assert-test")
    store.add_venue("assert-test", "venue-1")

    # Should not raise
    store.assert_venue_in_tenant("venue-1", "assert-test")


def test_assert_venue_in_tenant_invalid():
    """Test assert_venue_in_tenant with invalid venue."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("assert-test", "Assert Test", "assert-test")

    try:
        store.assert_venue_in_tenant("venue-nonexistent", "assert-test")
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# Demo Tenant Auto-Seeding
# ---------------------------------------------------------------------------


def test_demo_tenant_auto_seeded():
    """Test that demo tenant is auto-seeded on first access."""
    _reset_tenants()
    store = get_tenant_store()

    demo = store.get("demo-tenant-001")
    assert demo is not None
    assert demo.name == "Demo Tenant"
    assert demo.billing_tier == BillingTier.ENTERPRISE
    assert "venue_demo_001" in demo.venue_ids
    assert "venue-royal-oak" in demo.venue_ids


def test_demo_tenant_owns_all_demo_venues():
    """Test that demo tenant owns all known demo venues."""
    _reset_tenants()
    store = get_tenant_store()

    demo_venues = [
        "venue_demo_001",
        "venue_demo_002",
        "venue_demo_003",
        "venue-royal-oak",
    ]

    for venue_id in demo_venues:
        tenant = store.find_tenant_for_venue(venue_id)
        assert tenant is not None
        assert tenant.tenant_id == "demo-tenant-001"


# ---------------------------------------------------------------------------
# Usage Tracking
# ---------------------------------------------------------------------------


def test_record_and_get_usage():
    """Test recording and retrieving usage snapshots."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("usage-test", "Usage Test", "usage-test")

    snapshot = TenantUsageSnapshot(
        tenant_id="usage-test",
        snapshot_date="2026-04-15",
        active_venues=2,
        total_employees=15,
        rosters_generated_month=5,
    )

    store.record_usage(snapshot)
    retrieved = store.get_usage("usage-test")

    assert retrieved is not None
    assert retrieved.snapshot_date == "2026-04-15"
    assert retrieved.active_venues == 2
    assert retrieved.total_employees == 15


def test_get_usage_by_month():
    """Test filtering usage by month."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("month-test", "Month Test", "month-test")

    # Record two snapshots in different months
    store.record_usage(TenantUsageSnapshot(
        tenant_id="month-test",
        snapshot_date="2026-03-15",
        active_venues=1,
    ))
    store.record_usage(TenantUsageSnapshot(
        tenant_id="month-test",
        snapshot_date="2026-04-15",
        active_venues=2,
    ))

    # Get April snapshot
    april = store.get_usage("month-test", "2026-04")
    assert april.snapshot_date == "2026-04-15"
    assert april.active_venues == 2


# ---------------------------------------------------------------------------
# Billing Tier Enforcement
# ---------------------------------------------------------------------------


def test_check_tier_allows_startup():
    """Test that startup tier rejects premium features."""
    tenant = Tenant(
        tenant_id="startup-test",
        name="Startup Test",
        slug="startup-test",
        created_at=datetime.now(timezone.utc),
        billing_tier=BillingTier.STARTUP,
    )

    # Startup should reject premium features
    allowed, reason = check_tier_allows(tenant, "real_time_notifications")
    assert allowed is False
    assert reason is not None


def test_check_tier_allows_pro():
    """Test that pro tier allows mid-tier features."""
    tenant = Tenant(
        tenant_id="pro-test",
        name="Pro Test",
        slug="pro-test",
        created_at=datetime.now(timezone.utc),
        billing_tier=BillingTier.PRO,
    )

    # Pro should allow conversational_ai
    allowed, reason = check_tier_allows(tenant, "conversational_ai")
    assert allowed is True
    assert reason is None

    # Pro should reject enterprise features
    allowed, reason = check_tier_allows(tenant, "real_time_notifications")
    assert allowed is False


def test_check_tier_allows_enterprise():
    """Test that enterprise tier allows all features."""
    tenant = Tenant(
        tenant_id="enterprise-test",
        name="Enterprise Test",
        slug="enterprise-test",
        created_at=datetime.now(timezone.utc),
        billing_tier=BillingTier.ENTERPRISE,
    )

    # Enterprise should allow everything
    for feature in [
        "real_time_notifications",
        "multi_venue",
        "conversational_ai",
        "scenario_solver",
    ]:
        allowed, reason = check_tier_allows(tenant, feature)
        assert allowed is True, f"Enterprise should allow {feature}"


def test_check_tier_allows_basic_features_all_tiers():
    """Test that basic features are available to all tiers."""
    for tier in [BillingTier.STARTUP, BillingTier.PRO, BillingTier.ENTERPRISE]:
        tenant = Tenant(
            tenant_id=f"test-{tier.value}",
            name=f"Test {tier.value}",
            slug=f"test-{tier.value}",
            created_at=datetime.now(timezone.utc),
            billing_tier=tier,
        )

        # Basic feature should work on all tiers
        allowed, reason = check_tier_allows(tenant, "basic_rostering")
        assert allowed is True


# ---------------------------------------------------------------------------
# Auth Integration: User with Tenant
# ---------------------------------------------------------------------------


def test_create_user_with_tenant():
    """Test that users can be created with tenant_id."""
    _reset_auth()
    _reset_tenants()

    user_create = auth.UserCreate(
        email="tenant-user@test.org",
        password="SecurePass123!",
        name="Tenant User",
        venue_id="venue-1",
        tenant_id="test-tenant",
    )

    user = auth.create_user(user_create)

    assert user.tenant_id == "test-tenant"
    assert user.venue_ids == ["venue-1"]


def test_create_user_defaults_tenant():
    """Test that users default to demo-tenant-001 if not specified."""
    _reset_auth()

    user_create = auth.UserCreate(
        email="default-tenant@test.org",
        password="SecurePass123!",
        name="Default User",
        venue_id="venue-1",
        # No tenant_id specified
    )

    user = auth.create_user(user_create)

    assert user.tenant_id == "demo-tenant-001"


def test_jwt_includes_tenant_id():
    """Test that JWT tokens include tenant_id claim."""
    _reset_auth()

    token = auth.create_access_token(
        user_id="user-1",
        venue_id="venue-1",
        role="manager",
        tenant_id="my-tenant",
    )

    payload = auth.decode_token(token)
    assert payload["tid"] == "my-tenant"


def test_jwt_tenant_id_backward_compatible():
    """Test that old JWT tokens without tid default to demo-tenant-001."""
    _reset_auth()

    # Create token with old create_access_token (but force no tenant_id)
    import datetime as dt
    from rosteriq.auth import JWT_SECRET, JWT_ALGORITHM
    import jwt as pyjwt

    now = dt.datetime.now(dt.timezone.utc)
    expires = now + dt.timedelta(hours=24)

    # Old payload without "tid"
    old_payload = {
        "sub": "user-old",
        "venue_id": "venue-1",
        "role": "manager",
        "al": "l1",
        "iat": now,
        "exp": expires,
    }

    token = pyjwt.encode(old_payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    # Decode should add "tid" with default
    payload = auth.decode_token(token)
    assert payload["tid"] == "demo-tenant-001"


# ---------------------------------------------------------------------------
# Dataclass Serialization
# ---------------------------------------------------------------------------


def test_tenant_to_dict():
    """Test Tenant serialization to dict."""
    tenant = Tenant(
        tenant_id="ser-test",
        name="Serialization Test",
        slug="ser-test",
        created_at=datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc),
        billing_tier=BillingTier.PRO,
        status=TenantStatus.ACTIVE,
        venue_ids=["venue-1"],
    )

    data = tenant.to_dict()

    assert data["tenant_id"] == "ser-test"
    assert data["billing_tier"] == "pro"
    assert data["status"] == "active"
    assert "2026-04-15" in data["created_at"]


def test_tenant_from_dict():
    """Test Tenant reconstruction from dict."""
    data = {
        "tenant_id": "ser-test",
        "name": "Serialization Test",
        "slug": "ser-test",
        "created_at": "2026-04-15T12:00:00+00:00",
        "billing_tier": "pro",
        "status": "active",
        "venue_ids": ["venue-1"],
        "owner_user_id": None,
        "trial_ends_at": None,
        "max_venues": 10,
        "max_employees": 50,
        "contact_email": "",
        "notes": {},
    }

    tenant = Tenant.from_dict(data)

    assert tenant.tenant_id == "ser-test"
    assert tenant.billing_tier == BillingTier.PRO
    assert tenant.status == TenantStatus.ACTIVE


# ---------------------------------------------------------------------------
# Clear Store
# ---------------------------------------------------------------------------


def test_clear():
    """Test clearing the tenant store."""
    _reset_tenants()
    store = get_tenant_store()

    store.create("clear-test", "Clear Test", "clear-test")
    assert len(store.list_all()) >= 2  # demo + test

    store.clear()
    all_tenants = store.list_all()
    assert len(all_tenants) == 0


# ---------------------------------------------------------------------------
# Run All Tests
# ---------------------------------------------------------------------------


def _run_all_tests():
    """Run all test functions."""
    tests = [
        ("TenantStore.create", test_tenant_store_create),
        ("TenantStore.get", test_tenant_store_get),
        ("TenantStore.get_by_slug", test_tenant_store_get_by_slug),
        ("TenantStore.list_all", test_tenant_store_list_all),
        ("TenantStore.update", test_tenant_store_update),
        ("TenantStore.delete", test_tenant_store_delete),
        ("TenantStore.suspend/activate", test_tenant_store_suspend_activate),
        ("add_venue_to_tenant", test_add_venue_to_tenant),
        ("add_venue_enforces_max_venues", test_add_venue_enforces_max_venues),
        ("add_venue_detects_collision", test_add_venue_detects_collision),
        ("find_tenant_for_venue", test_find_tenant_for_venue),
        ("remove_venue", test_remove_venue),
        ("assert_venue_in_tenant_valid", test_assert_venue_in_tenant_valid),
        ("assert_venue_in_tenant_invalid", test_assert_venue_in_tenant_invalid),
        ("demo_tenant_auto_seeded", test_demo_tenant_auto_seeded),
        ("demo_tenant_owns_all_demo_venues", test_demo_tenant_owns_all_demo_venues),
        ("record_and_get_usage", test_record_and_get_usage),
        ("get_usage_by_month", test_get_usage_by_month),
        ("check_tier_allows_startup", test_check_tier_allows_startup),
        ("check_tier_allows_pro", test_check_tier_allows_pro),
        ("check_tier_allows_enterprise", test_check_tier_allows_enterprise),
        ("check_tier_allows_basic_features_all_tiers", test_check_tier_allows_basic_features_all_tiers),
        ("create_user_with_tenant", test_create_user_with_tenant),
        ("create_user_defaults_tenant", test_create_user_defaults_tenant),
        ("jwt_includes_tenant_id", test_jwt_includes_tenant_id),
        ("jwt_tenant_id_backward_compatible", test_jwt_tenant_id_backward_compatible),
        ("tenant_to_dict", test_tenant_to_dict),
        ("tenant_from_dict", test_tenant_from_dict),
        ("clear", test_clear),
    ]

    passed = 0
    failed = 0
    errors = []

    for test_name, test_func in tests:
        try:
            test_func()
            print(f"✓ {test_name}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {test_name}: {e}")
            failed += 1
            errors.append((test_name, str(e)))
        except Exception as e:
            print(f"✗ {test_name}: {type(e).__name__}: {e}")
            failed += 1
            errors.append((test_name, f"{type(e).__name__}: {e}"))

    print(f"\n{'='*70}")
    print(f"Tests: {passed} passed, {failed} failed")
    print(f"{'='*70}")

    if errors:
        print("\nFailures:")
        for test_name, error in errors:
            print(f"  {test_name}: {error}")
        sys.exit(1)
    else:
        print("All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    _run_all_tests()
