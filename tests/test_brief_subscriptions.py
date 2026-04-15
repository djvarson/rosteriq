"""Tests for brief subscription store.

Tests CRUD operations, filtering, and API router endpoints.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq import brief_subscriptions


def reset_state():
    """Reset all module singletons."""
    brief_subscriptions.reset_subscription_store_for_tests()


def test_create_subscription():
    """Test creating a subscription."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="manager@example.com",
        phone="+61412345678",
        brief_types=["morning", "weekly"],
        delivery_channels=["email"],
        local_tz="Australia/Perth",
    )

    assert sub.venue_id == "venue_001"
    assert sub.user_id == "user_001"
    assert sub.user_role == "manager"
    assert sub.email == "manager@example.com"
    assert sub.phone == "+61412345678"
    assert sub.brief_types == ["morning", "weekly"]
    assert sub.delivery_channels == ["email"]
    assert sub.local_tz == "Australia/Perth"
    assert sub.enabled is True


def test_get_subscription():
    """Test retrieving a subscription."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="test@example.com",
    )

    retrieved = store.get(sub.subscription_id)
    assert retrieved is not None
    assert retrieved.subscription_id == sub.subscription_id
    assert retrieved.user_id == "user_001"


def test_list_for_venue():
    """Test listing subscriptions for a venue."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub1 = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="user1@example.com",
    )
    sub2 = store.create(
        venue_id="venue_001",
        user_id="user_002",
        user_role="manager",
        email="user2@example.com",
    )
    sub3 = store.create(
        venue_id="venue_002",
        user_id="user_003",
        user_role="manager",
        email="user3@example.com",
    )

    venue_001_subs = store.list_for_venue("venue_001")
    assert len(venue_001_subs) == 2
    assert all(s.venue_id == "venue_001" for s in venue_001_subs)

    venue_002_subs = store.list_for_venue("venue_002")
    assert len(venue_002_subs) == 1
    assert venue_002_subs[0].venue_id == "venue_002"


def test_list_for_brief():
    """Test listing subscriptions by brief type."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub1 = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="user1@example.com",
        brief_types=["morning"],
    )
    sub2 = store.create(
        venue_id="venue_001",
        user_id="user_002",
        user_role="manager",
        email="user2@example.com",
        brief_types=["weekly"],
    )
    sub3 = store.create(
        venue_id="venue_001",
        user_id="user_003",
        user_role="manager",
        email="user3@example.com",
        brief_types=["morning", "weekly"],
    )

    morning_subs = store.list_for_brief("venue_001", "morning")
    assert len(morning_subs) == 2  # sub1 and sub3

    weekly_subs = store.list_for_brief("venue_001", "weekly")
    assert len(weekly_subs) == 2  # sub2 and sub3


def test_update_subscription():
    """Test updating a subscription."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="user@example.com",
        brief_types=["morning"],
        delivery_channels=["email"],
        local_tz="Australia/Perth",
    )

    updated = store.update(
        sub.subscription_id,
        enabled=False,
        delivery_channels=["sms"],
        brief_types=["morning", "weekly"],
        local_tz="Australia/Brisbane",
    )

    assert updated is not None
    assert updated.enabled is False
    assert updated.delivery_channels == ["sms"]
    assert updated.brief_types == ["morning", "weekly"]
    assert updated.local_tz == "Australia/Brisbane"


def test_delete_subscription():
    """Test deleting a subscription."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="user@example.com",
    )

    deleted = store.delete(sub.subscription_id)
    assert deleted is True

    retrieved = store.get(sub.subscription_id)
    assert retrieved is None


def test_disabled_subscriptions_excluded_from_list():
    """Test that disabled subscriptions are not returned by list_for_venue."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub1 = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="manager",
        email="user1@example.com",
    )
    sub2 = store.create(
        venue_id="venue_001",
        user_id="user_002",
        user_role="manager",
        email="user2@example.com",
    )

    # Disable sub2
    store.update(sub2.subscription_id, enabled=False)

    subs = store.list_for_venue("venue_001")
    assert len(subs) == 1
    assert subs[0].subscription_id == sub1.subscription_id


def test_to_dict():
    """Test converting subscription to dict."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    store.clear()

    sub = store.create(
        venue_id="venue_001",
        user_id="user_001",
        user_role="owner",
        email="owner@example.com",
        phone="+61412345678",
        brief_types=["morning", "weekly", "portfolio"],
        delivery_channels=["email", "sms"],
        local_tz="Australia/Sydney",
    )

    d = sub.to_dict()
    assert d["venue_id"] == "venue_001"
    assert d["user_role"] == "owner"
    assert d["email"] == "owner@example.com"
    assert d["phone"] == "+61412345678"
    assert d["brief_types"] == ["morning", "weekly", "portfolio"]
    assert d["delivery_channels"] == ["email", "sms"]
    assert d["local_tz"] == "Australia/Sydney"
    assert d["enabled"] is True


def test_seed_demo_subscriptions():
    """Test that seeding creates demo subscriptions."""
    reset_state()
    store = brief_subscriptions.get_subscription_store()
    # Calling get_subscription_store seeds automatically

    subs = store.all()
    assert len(subs) > 0

    # Should have at least one owner subscription
    owners = [s for s in subs if s.user_role == "owner"]
    assert len(owners) > 0


# Run all tests
def main():
    tests = [
        test_create_subscription,
        test_get_subscription,
        test_list_for_venue,
        test_list_for_brief,
        test_update_subscription,
        test_delete_subscription,
        test_disabled_subscriptions_excluded_from_list,
        test_to_dict,
        test_seed_demo_subscriptions,
    ]

    for test in tests:
        try:
            print(f"Running {test.__name__}...", end=" ")
            test()
            print("PASS")
        except AssertionError as e:
            print(f"FAIL: {e}")
        except Exception as e:
            print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
