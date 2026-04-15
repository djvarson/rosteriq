"""
Tests for AskAgent LLM-backed methods.

Covers answer_with_llm(), tool execution, multi-turn conversations,
and fallback behavior when LLM is not configured.

Uses stdlib + unittest.mock; no pytest or httpx required.
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

# Mock httpx before importing llm_backends/ask_agent
sys.modules["httpx"] = MagicMock()

from rosteriq.ask_agent import AskAgent, QueryIntent
from rosteriq.ask_context import (
    ShiftRow,
    RosterRow,
    VendorForecastRow,
    HeadCountRow,
    EmployeeRow,
)
from rosteriq.llm_backends import NoOpBackend, AnthropicBackend


# ============================================================================
# Mock QueryContext
# ============================================================================

class MockQueryContext:
    """Lightweight mock of QueryContext for testing."""

    def __init__(self):
        self.venue_id = "test-venue"
        self.today = date(2024, 4, 15)
        self.rosters = []
        self.vendor_forecasts = []
        self.head_counts = []
        self.employees = {}
        self.timezone_label = "Australia/Melbourne"

        # Demo employees
        self.employees = {
            "EMP001": EmployeeRow(id="EMP001", name="Sarah Chen", employment_type="fulltime"),
            "EMP002": EmployeeRow(id="EMP002", name="Marcus Johnson", employment_type="casual"),
        }

        # Week of shifts
        roster = RosterRow(venue_id=self.venue_id, week_start=date(2024, 4, 8))
        for i in range(7):
            day = date(2024, 4, 8).__class__(2024, 4, 8 + i)
            shift = ShiftRow(
                employee_id="EMP001" if i % 2 == 0 else "EMP002",
                date=day,
                start_time=time(17, 0),
                end_time=time(22, 0),
                hours=5.0,
                cost=Decimal("150.00"),
                role="floor",
            )
            roster.shifts.append(shift)

        self.rosters.append(roster)

        # Vendor forecasts
        for i in range(7):
            day = date(2024, 4, 8).__class__(2024, 4, 8 + i)
            dt_start = datetime(2024, 4, 8 + i, 0, 0, tzinfo=timezone.utc)
            dt_end = datetime(2024, 4, 8 + i + 1, 0, 0, tzinfo=timezone.utc)
            forecast = VendorForecastRow(
                bucket_start=dt_start,
                bucket_end=dt_end,
                metric="revenue",
                amount=Decimal("12500.00"),
            )
            self.vendor_forecasts.append(forecast)


# ============================================================================
# Test answer_with_llm with NoOpBackend
# ============================================================================

class TestAnswerWithLLMNoOp(unittest.TestCase):
    """Test answer_with_llm with NoOpBackend (falls back to rule-based)."""

    def setUp(self):
        """Set up agent with mock context builder."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        self.agent = AskAgent(context_builder=mock_context_builder)

    def test_noop_backend_falls_back_to_rule_based(self):
        """answer_with_llm with NoOp backend delegates to answer()."""
        with patch("rosteriq.llm_backends.get_llm_backend") as mock_get:
            mock_get.return_value = NoOpBackend()

            result = asyncio.run(
                self.agent.answer_with_llm(
                    query="What was our wage cost last week?",
                    venue_id="test-venue",
                    today=date(2024, 4, 15),
                )
            )

            # Should get a response from rule-based path
            assert result["text"]
            assert result["backend_used"] == "rule_based"
            assert result["tool_calls"] == []
            assert result["intent"] in [
                QueryIntent.LABOUR_COST.value,
                QueryIntent.UNKNOWN.value,
            ]

    def test_noop_preserves_result_structure(self):
        """answer_with_llm returns required fields."""
        with patch("rosteriq.llm_backends.get_llm_backend") as mock_get:
            mock_get.return_value = NoOpBackend()

            result = asyncio.run(
                self.agent.answer_with_llm(
                    query="Show me Fridays with rain",
                    venue_id="test-venue",
                    today=date(2024, 4, 15),
                )
            )

            assert "text" in result
            assert "intent" in result
            assert "tool_calls" in result
            assert "backend_used" in result
            assert "data" in result
            assert "confidence" in result
            assert "source_rows" in result
            assert "timestamp" in result

    def test_noop_with_custom_context(self):
        """answer_with_llm accepts pre-built context."""
        with patch("rosteriq.llm_backends.get_llm_backend") as mock_get:
            mock_get.return_value = NoOpBackend()

            context = MockQueryContext()
            result = asyncio.run(
                self.agent.answer_with_llm(
                    query="Best hour last Saturday",
                    venue_id="test-venue",
                    today=date(2024, 4, 15),
                    context=context,
                )
            )

            assert result["text"]
            assert result["backend_used"] == "rule_based"


# ============================================================================
# Test answer_with_llm with Mocked LLM Backend
# ============================================================================

class TestAnswerWithLLMBackend(unittest.TestCase):
    """Test answer_with_llm with a real-ish LLM backend (mocked HTTP)."""

    def setUp(self):
        """Set up agent with mock context builder."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        self.agent = AskAgent(context_builder=mock_context_builder)

    @patch("rosteriq.llm_backends.get_llm_backend")
    def test_llm_without_tool_calls(self, mock_get):
        """answer_with_llm with LLM that doesn't call tools returns text."""
        mock_backend = AsyncMock()
        mock_backend.complete.return_value = {
            "text": "The forecast looks good for tomorrow with revenue around $15,000.",
            "tool_calls": None,
            "raw": {"id": "msg-123"},
        }
        mock_get.return_value = mock_backend

        result = asyncio.run(
            self.agent.answer_with_llm(
                query="What's tomorrow looking like?",
                venue_id="test-venue",
                today=date(2024, 4, 15),
            )
        )

        assert "tomorrow" in result["text"].lower() or "forecast" in result["text"].lower()
        assert result["backend_used"] in ["anthropic", "openai"]
        assert result["tool_calls"] == []

    @patch("rosteriq.llm_backends.get_llm_backend")
    def test_llm_with_tool_calls(self, mock_get):
        """answer_with_llm executes tool calls and continues conversation."""
        # Create a mock backend that calls a tool then returns final answer
        mock_backend = AsyncMock()

        # First call: LLM asks for labour cost data
        mock_backend.complete.side_effect = [
            {
                "text": "I'll get the labour cost data for you.",
                "tool_calls": [
                    {
                        "id": "tc-1",
                        "name": "get_labour_cost",
                        "input": {
                            "start_date": "2024-04-08",
                            "end_date": "2024-04-15",
                            "venue_id": "test-venue",
                        },
                    }
                ],
                "raw": {},
            },
            # Second call: LLM returns final answer with tool result
            {
                "text": "Last week your labour cost was $1,050 across 7 shifts.",
                "tool_calls": None,
                "raw": {},
            },
        ]

        mock_get.return_value = mock_backend

        result = asyncio.run(
            self.agent.answer_with_llm(
                query="What was our labour cost last week?",
                venue_id="test-venue",
                today=date(2024, 4, 15),
            )
        )

        assert "labour" in result["text"].lower() or "cost" in result["text"].lower()
        assert len(result["tool_calls"]) >= 1
        assert result["tool_calls"][0]["name"] == "get_labour_cost"

    def test_tool_execution_get_forecast(self):
        """_tool_get_forecast returns correct structure."""
        context = MockQueryContext()
        result = self.agent._tool_get_forecast(
            {"date": "2024-04-08", "venue_id": "test-venue"},
            context,
        )

        assert "date" in result
        assert "forecast_revenue" in result
        assert "num_buckets" in result
        assert isinstance(result["forecast_revenue"], float)

    def test_tool_execution_get_labour_cost(self):
        """_tool_get_labour_cost returns correct structure."""
        context = MockQueryContext()
        result = self.agent._tool_get_labour_cost(
            {
                "start_date": "2024-04-08",
                "end_date": "2024-04-15",
                "venue_id": "test-venue",
            },
            context,
        )

        assert "period_start" in result
        assert "period_end" in result
        assert "total_labour_cost" in result
        assert "num_shifts" in result

    def test_tool_execution_get_staff_list(self):
        """_tool_get_staff_list returns staff with shift counts."""
        context = MockQueryContext()
        result = self.agent._tool_get_staff_list(
            {"venue_id": "test-venue"},
            context,
        )

        assert "period_start" in result
        assert "staff_count" in result
        assert "top_staff" in result
        assert isinstance(result["top_staff"], list)

    def test_tool_execution_error_handling(self):
        """Tool execution handles errors gracefully."""
        context = MockQueryContext()
        # Invalid date format
        result = self.agent._tool_get_forecast({"date": "invalid-date"}, context)
        assert "error" in result


# ============================================================================
# Test Max-Turn Limit
# ============================================================================

class TestMaxTurnLimit(unittest.TestCase):
    """Test that max-turn limit (3) is enforced."""

    def setUp(self):
        """Set up agent."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        self.agent = AskAgent(context_builder=mock_context_builder)

    @patch("rosteriq.llm_backends.get_llm_backend")
    def test_stops_after_max_turns(self, mock_get):
        """answer_with_llm stops after 3 turns even if tools keep returning."""
        mock_backend = AsyncMock()

        # Simulate backend that always returns tool calls (no text)
        mock_backend.complete.return_value = {
            "text": "",
            "tool_calls": [
                {
                    "id": "tc-1",
                    "name": "get_forecast",
                    "input": {"date": "2024-04-16", "venue_id": "test-venue"},
                }
            ],
            "raw": {},
        }

        mock_get.return_value = mock_backend

        result = asyncio.run(
            self.agent.answer_with_llm(
                query="Forecast?",
                venue_id="test-venue",
                today=date(2024, 4, 15),
            )
        )

        # Should complete after max turns (3)
        assert mock_backend.complete.call_count <= 3


# ============================================================================
# Test Tool Definitions
# ============================================================================

class TestToolDefinitions(unittest.TestCase):
    """Test that tool definitions are properly formatted."""

    def setUp(self):
        """Set up agent."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        self.agent = AskAgent(context_builder=mock_context_builder)

    def test_tools_have_required_fields(self):
        """Tool definitions have name, description, input_schema."""
        tools = self.agent._get_llm_tools()

        assert len(tools) > 0

        for tool in tools:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool

            schema = tool["input_schema"]
            assert "type" in schema
            assert "properties" in schema

    def test_system_prompt_exists(self):
        """System prompt is non-empty string."""
        prompt = self.agent._get_system_prompt()
        assert isinstance(prompt, str)
        assert len(prompt) > 0
        assert "RosterIQ" in prompt or "venue" in prompt.lower()


# ============================================================================
# Integration Test
# ============================================================================

class TestAnswerWithLLMIntegration(unittest.TestCase):
    """Integration test for the full answer_with_llm flow."""

    def setUp(self):
        """Set up agent."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        self.agent = AskAgent(context_builder=mock_context_builder)

    @patch("rosteriq.llm_backends.get_llm_backend")
    def test_full_flow_with_single_tool_call(self, mock_get):
        """Full flow: question -> tool call -> answer."""
        mock_backend = AsyncMock()

        # Step 1: LLM identifies need and makes tool call
        # Step 2: LLM gets result and returns final answer
        mock_backend.complete.side_effect = [
            {
                "text": "Let me check the historical sales for that date.",
                "tool_calls": [
                    {
                        "id": "tc-sales-1",
                        "name": "get_historical_sales",
                        "input": {
                            "date": "2024-04-13",
                            "venue_id": "test-venue",
                        },
                    }
                ],
                "raw": {},
            },
            {
                "text": "On Saturday, April 13, 2024, your venue had total revenue of $12,500.",
                "tool_calls": None,
                "raw": {},
            },
        ]

        mock_get.return_value = mock_backend

        result = asyncio.run(
            self.agent.answer_with_llm(
                query="How much did we sell last Saturday?",
                venue_id="test-venue",
                today=date(2024, 4, 15),
            )
        )

        # Verify result structure
        assert "revenue" in result["text"].lower() or "sold" in result["text"].lower()
        assert len(result["tool_calls"]) == 1
        assert result["tool_calls"][0]["name"] == "get_historical_sales"
        assert result["backend_used"] in ["anthropic", "openai"]

    def test_answer_with_llm_handles_llm_exception(self):
        """answer_with_llm falls back to rule-based if LLM throws."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)

        with patch("rosteriq.llm_backends.get_llm_backend") as mock_get:
            mock_backend = AsyncMock()
            mock_backend.complete.side_effect = Exception("LLM service down")
            mock_get.return_value = mock_backend

            result = asyncio.run(
                agent.answer_with_llm(
                    query="What was our wage cost?",
                    venue_id="test-venue",
                    today=date(2024, 4, 15),
                )
            )

            # Should fall back to rule-based
            assert result["backend_used"] == "rule_based"
            assert result["text"]  # Should still have an answer


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    unittest.main()
