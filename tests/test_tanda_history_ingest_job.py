"""Tests for the daily Tanda history ingest scheduled job (Round 14)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from rosteriq.scheduled_jobs import make_tanda_history_ingest_job


class _FakeAdapter:
    """Adapter stub — TandaHistoryIngestor tolerates missing methods."""

    def __init__(self):
        self.calls: Dict[str, List[Tuple[str, Any, Any]]] = {
            "shifts": [], "timesheets": [], "forecasts": [], "employees": [],
        }

    async def get_shifts(self, org_id, window):
        self.calls["shifts"].append((org_id, window[0], window[1]))
        return []

    async def get_timesheets(self, org_id, window):
        self.calls["timesheets"].append((org_id, window[0], window[1]))
        return []

    async def get_forecast_revenue(self, org_id, window):
        self.calls["forecasts"].append((org_id, window[0], window[1]))
        return []

    async def get_employees(self, org_id):
        self.calls["employees"].append((org_id, None, None))
        return []


class MakeIngestJobTests(unittest.TestCase):
    def test_no_venues_returns_ok_with_message(self):
        job = make_tanda_history_ingest_job(
            venue_map_fn=lambda: [],
            adapter_factory=lambda: _FakeAdapter(),
        )
        result = job.fn(datetime.now(timezone.utc))
        self.assertTrue(result["ok"])
        self.assertEqual(result["runs"], [])
        self.assertIn("no venues", result["message"])

    def test_single_venue_ingest_runs(self):
        adapter = _FakeAdapter()
        job = make_tanda_history_ingest_job(
            venue_map_fn=lambda: [("venue-1", "org-1")],
            adapter_factory=lambda: adapter,
            lookback_days=2,
        )
        result = job.fn(datetime(2026, 4, 16, tzinfo=timezone.utc))
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["runs"]), 1)
        self.assertTrue(result["runs"][0]["ok"])
        # Adapter saw the calls
        self.assertEqual(len(adapter.calls["shifts"]), 1)
        self.assertEqual(adapter.calls["shifts"][0][0], "org-1")

    def test_multi_venue_independent(self):
        """One venue failing should not block others (partial success)."""
        class HalfBrokenAdapter:
            def __init__(self):
                self.n = 0

            async def get_employees(self, org_id):
                return []

            async def get_shifts(self, org_id, window):
                # Fail for org-bad; succeed for org-good
                if org_id == "org-bad":
                    raise RuntimeError("simulated tanda timeout")
                return []

            async def get_timesheets(self, org_id, window):
                return []

            async def get_forecast_revenue(self, org_id, window):
                return []

        job = make_tanda_history_ingest_job(
            venue_map_fn=lambda: [("v-good", "org-good"), ("v-bad", "org-bad")],
            adapter_factory=lambda: HalfBrokenAdapter(),
        )
        result = job.fn(datetime.now(timezone.utc))
        # ingest_range itself is tolerant of get_shifts failure, so both
        # runs report ok=True from the scheduler's perspective.
        runs = {r["venue_id"]: r for r in result["runs"]}
        self.assertIn("v-good", runs)
        self.assertIn("v-bad", runs)

    def test_adapter_factory_failure_is_not_fatal(self):
        def _raise():
            raise RuntimeError("no adapter for you")
        job = make_tanda_history_ingest_job(
            venue_map_fn=lambda: [("v", "o")],
            adapter_factory=_raise,
        )
        result = job.fn(datetime.now(timezone.utc))
        self.assertFalse(result["ok"])
        self.assertIn("adapter init failed", result["error"])


if __name__ == "__main__":
    unittest.main()
