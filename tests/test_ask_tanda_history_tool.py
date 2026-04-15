"""Tests for the Ask-agent get_tanda_history tool (Round 8 Track C)."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from rosteriq.ask_agent import AskAgent
from rosteriq.tanda_history import (
    DailyActuals,
    get_history_store,
)


class AskHistoryToolTest(unittest.TestCase):
    def setUp(self):
        self.agent = AskAgent()
        self.store = get_history_store()
        self.store.clear()

    def test_tool_listed(self):
        tool_names = [t["name"] for t in self.agent._get_llm_tools()]
        self.assertIn("get_tanda_history", tool_names)

    def test_tool_returns_empty_with_message(self):
        result = self.agent._tool_get_tanda_history(
            {"venue_id": "v_demo"}, context=None, today=date.today(),
        )
        self.assertIn("message", result)
        self.assertEqual(result["rows"], [])

    def test_tool_returns_rows_when_data_present(self):
        today = date.today()
        # Seed three days of data
        for i in range(3):
            d = today - timedelta(days=i)
            self.store.upsert_daily(DailyActuals(
                venue_id="v1", day=d,
                rostered_hours=8 * 5,  # 5 staff x 8h
                worked_hours=8 * 5 + i,
                worked_cost=1000.0 + (i * 100),
                actual_revenue=4000.0,
            ))
        result = self.agent._tool_get_tanda_history(
            {"venue_id": "v1", "days": 3}, context=None, today=today,
        )
        self.assertEqual(result["row_count"], 3)
        self.assertEqual(len(result["rows"]), 3)
        self.assertIn("summary", result)
        self.assertGreater(result["summary"]["actual_revenue"], 0)

    def test_tool_invalid_date(self):
        result = self.agent._tool_get_tanda_history(
            {"venue_id": "v1", "from_date": "not-a-date"},
            context=None, today=date.today(),
        )
        self.assertIn("error", result)

    def test_tool_invalid_range(self):
        today = date.today()
        result = self.agent._tool_get_tanda_history(
            {"venue_id": "v1",
             "from_date": today.isoformat(),
             "to_date": (today - timedelta(days=2)).isoformat()},
            context=None, today=today,
        )
        self.assertIn("error", result)

    def test_tool_missing_venue(self):
        result = self.agent._tool_get_tanda_history(
            {}, context=None, today=date.today(),
        )
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
