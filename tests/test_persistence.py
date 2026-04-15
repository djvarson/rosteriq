"""Tests for the SQLite persistence backend (Round 11).

Covers the round-trip path: write to one store instance with persistence
ON, drop the singletons, rehydrate from disk, and confirm state survives.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone


def _reset_persistence_with_path(path: str) -> None:
    """Point persistence at a fresh DB and reset all singletons."""
    from rosteriq import persistence as _p
    os.environ["ROSTERIQ_DB_PATH"] = path
    _p.reset_for_tests()
    _p.reset_rehydrate_for_tests()


class PersistenceModuleTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        _reset_persistence_with_path(self.db_path)

    def tearDown(self):
        from rosteriq import persistence as _p
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.tmpdir.cleanup()

    def test_is_persistence_enabled_when_path_set(self):
        from rosteriq import persistence as _p
        self.assertTrue(_p.is_persistence_enabled())

    def test_is_disabled_when_unset(self):
        from rosteriq import persistence as _p
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.assertFalse(_p.is_persistence_enabled())

    def test_in_memory_treated_as_disabled(self):
        from rosteriq import persistence as _p
        os.environ["ROSTERIQ_DB_PATH"] = ":memory:"
        self.assertFalse(_p.is_persistence_enabled())

    def test_upsert_roundtrip(self):
        from rosteriq import persistence as _p
        _p.register_schema(
            "ut_test_table",
            "CREATE TABLE IF NOT EXISTS ut_test (id TEXT PRIMARY KEY, val TEXT)",
        )
        _p.connection()  # apply schemas
        _p.upsert("ut_test", {"id": "a", "val": "first"}, pk="id")
        _p.upsert("ut_test", {"id": "a", "val": "second"}, pk="id")
        rows = _p.fetchall("SELECT * FROM ut_test")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["val"], "second")

    def test_json_helpers(self):
        from rosteriq import persistence as _p
        s = _p.json_dumps({"a": [1, 2, 3], "b": "x"})
        self.assertIn("\"a\"", s)
        self.assertEqual(_p.json_loads(s), {"a": [1, 2, 3], "b": "x"})
        self.assertEqual(_p.json_loads(None, default={}), {})
        self.assertEqual(_p.json_loads("not json", default=[]), [])


class TenantPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "tenants.db")
        _reset_persistence_with_path(self.db_path)
        # Reset the tenant store singleton
        import rosteriq.tenants as _t
        _t._tenant_store_instance = None

    def tearDown(self):
        from rosteriq import persistence as _p
        import rosteriq.tenants as _t
        _t._tenant_store_instance = None
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.tmpdir.cleanup()

    def test_tenant_create_persists_to_disk(self):
        from rosteriq.tenants import get_tenant_store, BillingTier
        store = get_tenant_store()
        store.create(
            tenant_id="t-persist-1",
            name="Persist Co",
            slug="persist-co",
            billing_tier=BillingTier.PRO,
            owner_user_id="u1",
            contact_email="ops@persist.co",
        )
        store.add_venue("t-persist-1", "venue-x")

        # Drop singleton, rehydrate
        import rosteriq.tenants as _t
        _t._tenant_store_instance = None
        from rosteriq import persistence as _p
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store2 = get_tenant_store()
        t = store2.get("t-persist-1")
        self.assertIsNotNone(t)
        self.assertEqual(t.name, "Persist Co")
        self.assertEqual(t.billing_tier, BillingTier.PRO)
        self.assertIn("venue-x", t.venue_ids)
        # venue→tenant index also rebuilt
        found = store2.find_tenant_for_venue("venue-x")
        self.assertEqual(found.tenant_id, "t-persist-1")

    def test_tenant_delete_clears_disk(self):
        from rosteriq.tenants import get_tenant_store
        store = get_tenant_store()
        store.create(tenant_id="t-del", name="Delete Me", slug="delete-me")
        store.delete("t-del")

        import rosteriq.tenants as _t
        _t._tenant_store_instance = None
        from rosteriq import persistence as _p
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store2 = get_tenant_store()
        self.assertIsNone(store2.get("t-del"))

    def test_usage_snapshot_round_trip(self):
        from rosteriq.tenants import get_tenant_store, TenantUsageSnapshot
        store = get_tenant_store()
        store.create(tenant_id="t-usage", name="UsageCo", slug="usageco")
        snap = TenantUsageSnapshot(
            tenant_id="t-usage",
            snapshot_date="2026-04-16",
            active_venues=2,
            total_employees=18,
            rosters_generated_month=12,
            billable_amount=14.50,
        )
        store.record_usage(snap)

        import rosteriq.tenants as _t
        _t._tenant_store_instance = None
        from rosteriq import persistence as _p
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store2 = get_tenant_store()
        usage = store2.get_usage("t-usage", month="2026-04")
        self.assertIsNotNone(usage)
        self.assertEqual(usage.active_venues, 2)
        self.assertAlmostEqual(usage.billable_amount, 14.50)


class SubscriptionPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "subs.db")
        _reset_persistence_with_path(self.db_path)
        import rosteriq.billing as _b
        _b._subscription_store_instance = None

    def tearDown(self):
        from rosteriq import persistence as _p
        import rosteriq.billing as _b
        _b._subscription_store_instance = None
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.tmpdir.cleanup()

    def test_subscription_round_trip(self):
        from rosteriq.billing import (
            get_subscription_store, Subscription, SubscriptionStatus,
        )
        now = datetime.now(timezone.utc)
        sub = Subscription(
            tenant_id="t-sub",
            stripe_subscription_id="sub_abc",
            stripe_customer_id="cus_abc",
            status=SubscriptionStatus.ACTIVE,
            tier="pro",
            current_period_end=now + timedelta(days=30),
            quantity=3,
            created_at=now,
            updated_at=now,
        )
        get_subscription_store().create(sub)

        import rosteriq.billing as _b
        _b._subscription_store_instance = None
        from rosteriq import persistence as _p
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store2 = get_subscription_store()
        s = store2.get("t-sub")
        self.assertIsNotNone(s)
        self.assertEqual(s.tier, "pro")
        self.assertEqual(s.quantity, 3)
        self.assertEqual(s.status, SubscriptionStatus.ACTIVE)
        # find_by_stripe_id index also rebuilt
        s2 = store2.find_by_stripe_id("sub_abc")
        self.assertEqual(s2.tenant_id, "t-sub")


class OnboardingPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "onboarding.db")
        _reset_persistence_with_path(self.db_path)
        import rosteriq.onboarding as _o
        _o._store = None

    def tearDown(self):
        from rosteriq import persistence as _p
        import rosteriq.onboarding as _o
        _o._store = None
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        os.environ.pop("ROSTERIQ_DB_PATH", None)
        self.tmpdir.cleanup()

    def test_wizard_state_round_trip(self):
        from rosteriq.onboarding import get_onboarding_store
        store = get_onboarding_store()
        state = store.start("tenant-x")
        wid = state.wizard_id
        store.submit_step(wid, "venue_basics", {
            "venue_name": "Mojo's", "timezone": "Australia/Perth",
        })

        import rosteriq.onboarding as _o
        _o._store = None
        from rosteriq import persistence as _p
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        store2 = get_onboarding_store()
        recovered = store2.get(wid)
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.tenant_id, "tenant-x")
        self.assertIn("venue_basics", recovered.completed_steps)
        self.assertEqual(
            recovered.data["venue_basics"]["venue_name"], "Mojo's"
        )
        self.assertEqual(recovered.current_step, "tanda_connect")


if __name__ == "__main__":
    unittest.main()
