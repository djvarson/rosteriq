"""Tests for the onboarding wizard (Round 8 Track D)."""

from __future__ import annotations

import threading
import unittest

from rosteriq.onboarding import (
    STEPS,
    OnboardingStore,
    get_onboarding_store,
    validate_step,
)


class ValidationTest(unittest.TestCase):
    def test_unknown_step(self):
        self.assertEqual(
            validate_step("nope", {})[0],
            "unknown step 'nope'",
        )

    def test_venue_basics_required(self):
        errs = validate_step("venue_basics", {})
        self.assertTrue(any("venue_name" in e for e in errs))
        self.assertTrue(any("timezone" in e for e in errs))

    def test_tanda_skip_ok(self):
        self.assertEqual(validate_step("tanda_connect", {"skip": True}), [])

    def test_tanda_needs_key_or_skip(self):
        errs = validate_step("tanda_connect", {})
        self.assertTrue(any("tanda_api_key" in e for e in errs))

    def test_team_size_invalid(self):
        errs = validate_step("team_size", {"headcount": "abc"})
        self.assertTrue(errs)

    def test_team_size_zero(self):
        errs = validate_step("team_size", {"headcount": 0})
        self.assertTrue(errs)

    def test_billing_tier_bad(self):
        errs = validate_step("billing_tier", {"tier": "platinum"})
        self.assertTrue(errs)

    def test_billing_tier_good(self):
        self.assertEqual(validate_step("billing_tier", {"tier": "pro"}), [])


class WizardFlowTest(unittest.TestCase):
    def setUp(self):
        self.store = OnboardingStore()

    def _drive_through(self, tenant_id="t1"):
        s = self.store.start(tenant_id)
        self.store.submit_step(s.wizard_id, "venue_basics", {
            "venue_name": "The Test Pub", "timezone": "Australia/Perth",
        })
        self.store.submit_step(s.wizard_id, "tanda_connect", {"skip": True})
        self.store.submit_step(s.wizard_id, "data_feeds", {
            "pos_provider": "swiftpos",
        })
        self.store.submit_step(s.wizard_id, "team_size", {"headcount": 12})
        self.store.submit_step(s.wizard_id, "billing_tier", {"tier": "startup"})
        return s.wizard_id

    def test_full_flow_advances_steps(self):
        wid = self._drive_through()
        state = self.store.get(wid)
        self.assertEqual(state.current_step, "confirm")
        self.assertEqual(len(state.completed_steps), 5)

    def test_complete_requires_steps(self):
        s = self.store.start("t1")
        with self.assertRaises(ValueError):
            self.store.complete(s.wizard_id)

    def test_complete_succeeds_after_all_steps(self):
        wid = self._drive_through()
        state = self.store.complete(wid)
        self.assertTrue(state.completed)
        self.assertIsNotNone(state.completed_at)

    def test_double_complete_idempotent(self):
        wid = self._drive_through()
        self.store.complete(wid)
        again = self.store.complete(wid)
        self.assertTrue(again.completed)

    def test_cant_submit_after_complete(self):
        wid = self._drive_through()
        self.store.complete(wid)
        with self.assertRaises(ValueError):
            self.store.submit_step(wid, "venue_basics", {
                "venue_name": "x", "timezone": "y",
            })

    def test_unknown_wizard(self):
        with self.assertRaises(KeyError):
            self.store.submit_step("wiz_missing", "venue_basics", {
                "venue_name": "x", "timezone": "y",
            })

    def test_invalid_payload_raises(self):
        s = self.store.start("t1")
        with self.assertRaises(ValueError):
            self.store.submit_step(s.wizard_id, "venue_basics", {})

    def test_finalize_called(self):
        wid = self._drive_through()
        called = []
        self.store.complete(wid, finalize=lambda s: called.append(s.wizard_id))
        self.assertEqual(called, [wid])

    def test_finalize_exception_swallowed(self):
        wid = self._drive_through()
        def boom(_state):
            raise RuntimeError("boom")
        # Should not raise — finalize errors are logged + swallowed
        state = self.store.complete(wid, finalize=boom)
        self.assertTrue(state.completed)


class ConcurrencyTest(unittest.TestCase):
    def test_thread_safety(self):
        store = OnboardingStore()
        s = store.start("t1")
        wid = s.wizard_id

        def worker():
            try:
                store.submit_step(wid, "venue_basics", {
                    "venue_name": "x", "timezone": "y",
                })
            except ValueError:
                # Expected: only one thread can advance at a time successfully;
                # the rest may see "already completed" or no-op behaviour.
                pass

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        state = store.get(wid)
        self.assertIn("venue_basics", state.completed_steps)
        # Should appear exactly once
        self.assertEqual(state.completed_steps.count("venue_basics"), 1)


class SingletonTest(unittest.TestCase):
    def test_get_store_returns_same(self):
        a = get_onboarding_store()
        b = get_onboarding_store()
        self.assertIs(a, b)


if __name__ == "__main__":
    unittest.main()
