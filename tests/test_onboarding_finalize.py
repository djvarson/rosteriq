"""Tests for onboarding finalize hooks (Round 15)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from rosteriq.onboarding_finalize import (
    _ensure_tenant,
    _flag_history_backfill,
    _register_tanda_creds,
    _seed_concierge_kb,
    run_finalize,
)


def _fake_state(tenant_id="t-fin", data=None):
    return SimpleNamespace(tenant_id=tenant_id, data=data or {})


class EnsureTenantTests(unittest.TestCase):
    def setUp(self):
        from rosteriq import tenants as _t
        # Fresh store
        _t._tenant_store_instance = None
        self.store = _t.get_tenant_store()

    def test_creates_tenant_when_missing(self):
        _ensure_tenant("t-new", {"venue_name": "The Pub"}, "startup")
        tenant = self.store.get("t-new")
        self.assertIsNotNone(tenant)
        self.assertEqual(tenant.name, "The Pub")

    def test_updates_existing_tenant(self):
        from rosteriq.tenants import BillingTier
        self.store.create(
            tenant_id="t-exist",
            name="old name",
            slug="t-exist",
            billing_tier=BillingTier.STARTUP,
            contact_email="",
        )
        _ensure_tenant("t-exist", {"venue_name": "New Name"}, "pro")
        tenant = self.store.get("t-exist")
        self.assertEqual(tenant.name, "New Name")
        self.assertEqual(tenant.billing_tier, BillingTier.PRO)

    def test_trial_maps_to_startup(self):
        from rosteriq.tenants import BillingTier
        _ensure_tenant("t-trial", {"venue_name": "X"}, "trial")
        tenant = self.store.get("t-trial")
        self.assertEqual(tenant.billing_tier, BillingTier.STARTUP)


class SeedConciergeKBTests(unittest.TestCase):
    def setUp(self):
        from rosteriq import concierge as _c
        _c._kb = None

    def test_seeds_kb_with_hours(self):
        from rosteriq.concierge import get_kb
        _seed_concierge_kb(
            "t-kb",
            {"venue_name": "Corner Bar", "open_time": "10:00", "close_time": "22:00"},
        )
        kb = get_kb().get("t-kb")
        self.assertIsNotNone(kb)
        self.assertEqual(kb.venue_name, "Corner Bar")
        self.assertEqual(kb.live_context["open_time"], "10:00")
        self.assertEqual(kb.live_context["close_time"], "22:00")
        self.assertTrue(len(kb.faqs) > 0)

    def test_seed_uses_defaults_when_hours_missing(self):
        from rosteriq.concierge import get_kb
        _seed_concierge_kb("t-kb2", {"venue_name": "Y"})
        kb = get_kb().get("t-kb2")
        self.assertEqual(kb.live_context["open_time"], "11:00")
        self.assertEqual(kb.live_context["close_time"], "23:00")


class RegisterTandaCredsTests(unittest.TestCase):
    def setUp(self):
        from rosteriq import tenants as _t
        _t._tenant_store_instance = None
        self.store = _t.get_tenant_store()
        from rosteriq.tenants import BillingTier
        self.store.create(
            tenant_id="t-tanda",
            name="n",
            slug="s",
            billing_tier=BillingTier.STARTUP,
            contact_email="",
        )

    def test_skip_does_nothing(self):
        _register_tanda_creds("t-tanda", {"skip": True})
        tenant = self.store.get("t-tanda")
        self.assertNotIn("tanda_connected", tenant.notes or {})

    def test_missing_key_does_nothing(self):
        _register_tanda_creds("t-tanda", {})
        tenant = self.store.get("t-tanda")
        self.assertNotIn("tanda_connected", tenant.notes or {})

    def test_happy_path_records_creds(self):
        _register_tanda_creds(
            "t-tanda",
            {"tanda_api_key": "secret", "tanda_org_id": "org-99"},
        )
        tenant = self.store.get("t-tanda")
        self.assertTrue(tenant.notes.get("tanda_connected"))
        self.assertEqual(tenant.notes.get("tanda_org_id"), "org-99")
        self.assertTrue(tenant.notes.get("tanda_api_key_present"))


class FlagHistoryBackfillTests(unittest.TestCase):
    def test_skip(self):
        msgs = _flag_history_backfill("t", {"skip": True})
        self.assertEqual(len(msgs), 1)
        self.assertIn("skipped", msgs[0].lower())

    def test_no_key(self):
        msgs = _flag_history_backfill("t", {})
        self.assertIn("No Tanda key", msgs[0])

    def test_queued(self):
        msgs = _flag_history_backfill("t-xyz", {"tanda_api_key": "k"})
        self.assertIn("backfill queued", msgs[0])
        self.assertIn("t-xyz", msgs[0])


class RunFinalizeTests(unittest.TestCase):
    def setUp(self):
        from rosteriq import tenants as _t, concierge as _c
        _t._tenant_store_instance = None
        _c._kb = None

    def test_full_run_returns_summary(self):
        state = _fake_state(
            tenant_id="t-e2e",
            data={
                "venue_basics": {
                    "venue_name": "E2E Venue",
                    "open_time": "09:00",
                    "close_time": "21:00",
                },
                "tanda_connect": {"tanda_api_key": "k", "tanda_org_id": "org-e2e"},
                "billing_tier": {"tier": "pro"},
            },
        )
        summary = run_finalize(state)
        self.assertEqual(summary["tenant_id"], "t-e2e")
        self.assertIn("ensure_tenant", summary["steps"])
        self.assertIn("seed_concierge_kb", summary["steps"])
        self.assertIn("register_tanda_creds", summary["steps"])
        self.assertEqual(summary["errors"], [])
        self.assertEqual(len(summary["next_actions"]), 1)

        # Verify side-effects
        from rosteriq.tenants import get_tenant_store
        from rosteriq.concierge import get_kb
        tenant = get_tenant_store().get("t-e2e")
        self.assertIsNotNone(tenant)
        kb = get_kb().get("t-e2e")
        self.assertIsNotNone(kb)

    def test_tanda_skip_path(self):
        state = _fake_state(
            tenant_id="t-skip",
            data={
                "venue_basics": {"venue_name": "S"},
                "tanda_connect": {"skip": True},
                "billing_tier": {"tier": "startup"},
            },
        )
        summary = run_finalize(state)
        self.assertEqual(summary["errors"], [])
        self.assertIn("skipped", summary["next_actions"][0].lower())

    def test_empty_data(self):
        state = _fake_state(tenant_id="t-empty", data={})
        summary = run_finalize(state)
        # Should still run all hooks without raising
        self.assertEqual(summary["tenant_id"], "t-empty")
        self.assertEqual(summary["errors"], [])


if __name__ == "__main__":
    unittest.main()
