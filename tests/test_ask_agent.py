"""
Tests for the conversational ask_agent module.

Covers intent classification, filter extraction, and end-to-end
query answering with mock context.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timezone
from decimal import Decimal

from rosteriq.ask_agent import (
    AskAgent,
    QueryIntent,
    classify_intent,
    extract_filters,
)
from rosteriq.ask_context import (
    ShiftRow,
    RosterRow,
    VendorForecastRow,
    HeadCountRow,
    EmployeeRow,
)


# ============================================================================
# Test Intent Classification
# ============================================================================

class TestClassifyIntent:
    """Test classify_intent pure function."""

    def test_historical_compare(self):
        """HISTORICAL_COMPARE: 'compare', 'vs', 'versus', 'difference'."""
        assert classify_intent("How did last Friday compare to this Friday?") == QueryIntent.HISTORICAL_COMPARE
        assert classify_intent("Friday vs Saturday") == QueryIntent.HISTORICAL_COMPARE
        assert classify_intent("what's the difference between last week and this week") == QueryIntent.HISTORICAL_COMPARE

    def test_pattern_lookup(self):
        """PATTERN_LOOKUP: 'rain', 'weather', 'sunny', 'hot', 'cold'."""
        assert classify_intent("Show me Fridays with rain") == QueryIntent.PATTERN_LOOKUP
        assert classify_intent("What were the rainy Saturdays?") == QueryIntent.PATTERN_LOOKUP
        assert classify_intent("Sunny days in June") == QueryIntent.PATTERN_LOOKUP
        assert classify_intent("How hot was it on Fridays?") == QueryIntent.PATTERN_LOOKUP

    def test_labour_cost(self):
        """LABOUR_COST: 'wage', 'labour', 'cost', 'payroll'."""
        assert classify_intent("What was our wage cost last week?") == QueryIntent.LABOUR_COST
        assert classify_intent("Labour spend yesterday") == QueryIntent.LABOUR_COST
        assert classify_intent("How much did we spend on payroll?") == QueryIntent.LABOUR_COST

    def test_forecast_query(self):
        """FORECAST_QUERY: 'forecast', 'tomorrow', 'next week'."""
        assert classify_intent("What's tomorrow looking like?") == QueryIntent.FORECAST_QUERY
        assert classify_intent("Forecast for next Friday") == QueryIntent.FORECAST_QUERY
        assert classify_intent("What's the forecast for tomorrow?") == QueryIntent.FORECAST_QUERY

    def test_staff_query(self):
        """STAFF_QUERY: 'who', 'staff', 'employee', 'person', 'team'."""
        assert classify_intent("Who's been at the venue most?") == QueryIntent.STAFF_QUERY
        assert classify_intent("Which staff member has worked the most hours?") == QueryIntent.STAFF_QUERY
        assert classify_intent("Who worked most this month?") == QueryIntent.STAFF_QUERY

    def test_sales_query(self):
        """SALES_QUERY: 'sales', 'revenue', 'covers', 'turnover', 'hour'."""
        assert classify_intent("Best hour last Saturday") == QueryIntent.SALES_QUERY
        assert classify_intent("Show me our best sales day") == QueryIntent.SALES_QUERY
        assert classify_intent("Covers on Friday") == QueryIntent.SALES_QUERY

    def test_unknown(self):
        """UNKNOWN: no keyword match."""
        assert classify_intent("Tell me something random") == QueryIntent.UNKNOWN
        assert classify_intent("What's the capital of France?") == QueryIntent.UNKNOWN


# ============================================================================
# Test Filter Extraction
# ============================================================================

class TestExtractFilters:
    """Test extract_filters pure function."""

    def test_day_of_week(self):
        """Extract day of week from question."""
        assert extract_filters("Show me Fridays")["dayofweek"] == 4
        assert extract_filters("What about Mondays?")["dayofweek"] == 0
        assert extract_filters("Saturdays are busy")["dayofweek"] == 5

    def test_weather(self):
        """Extract weather conditions."""
        assert extract_filters("rainy Fridays")["weather_condition"] == "rain"
        assert extract_filters("sunny days")["weather_condition"] == "sunny"
        assert extract_filters("hot Saturdays")["weather_condition"] == "hot"

    def test_month(self):
        """Extract month from question."""
        assert extract_filters("Fridays in June")["month"] == 6
        assert extract_filters("July data")["month"] == 7
        assert extract_filters("December was busy")["month"] == 12

    def test_role(self):
        """Extract role from question."""
        assert extract_filters("bar staff")["role"] == "bar"
        assert extract_filters("kitchen costs")["role"] == "kitchen"
        assert extract_filters("floor team")["role"] == "floor"

    def test_relative_date(self):
        """Extract relative dates."""
        assert extract_filters("last week")["relative_date"] == "last_week"
        assert extract_filters("last month")["relative_date"] == "last_month"
        assert extract_filters("yesterday")["relative_date"] == "yesterday"

    def test_multiple_filters(self):
        """Extract multiple filters from one question."""
        filters = extract_filters("Show me rainy Fridays in June")
        assert filters["dayofweek"] == 4
        assert filters["weather_condition"] == "rain"
        assert filters["month"] == 6

    def test_no_filters(self):
        """Return empty dict if no recognized filters."""
        assert extract_filters("hello world") == {}


# ============================================================================
# Mock QueryContext for Agent Testing
# ============================================================================

class MockQueryContext:
    """Lightweight mock of QueryContext for testing."""

    def __init__(self):
        self.venue_id = "test-venue"
        self.today = date(2024, 4, 15)  # A Monday
        self.rosters = []
        self.vendor_forecasts = []
        self.head_counts = []
        self.employees = {}
        self.timezone_label = "Australia/Melbourne"

        # Create demo employees
        self.employees = {
            "EMP001": EmployeeRow(id="EMP001", name="Sarah Chen", employment_type="fulltime"),
            "EMP002": EmployeeRow(id="EMP002", name="Marcus Johnson", employment_type="casual"),
        }

        # Create a week of shifts
        roster = RosterRow(venue_id=self.venue_id, week_start=date(2024, 4, 8))
        for i in range(7):
            day = date(2024, 4, 8) + __import__("datetime").timedelta(days=i)
            shift = ShiftRow(
                employee_id="EMP001",
                date=day,
                start_time=time(17, 0),
                end_time=time(22, 0),
                hours=5.0,
                cost=Decimal("150.00"),
                role="floor",
            )
            roster.shifts.append(shift)

        self.rosters.append(roster)

        # Create vendor forecasts
        for i in range(7):
            day = date(2024, 4, 8) + __import__("datetime").timedelta(days=i)
            dt_start = __import__("datetime").datetime.combine(day, __import__("datetime").time(0, 0), tzinfo=timezone.utc)
            dt_end = dt_start + __import__("datetime").timedelta(days=1)
            forecast = VendorForecastRow(
                bucket_start=dt_start,
                bucket_end=dt_end,
                metric="revenue",
                amount=Decimal("12500.00"),
            )
            self.vendor_forecasts.append(forecast)


# ============================================================================
# Test AskAgent
# ============================================================================

class TestAskAgent:
    """Test AskAgent answer() method."""

    async def test_pattern_lookup_Friday_rain(self):
        """Answer: 'Show me Fridays with rain' should find Friday shifts."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="Show me Fridays with rain",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.PATTERN_LOOKUP
        assert result.confidence >= 0.5
        assert len(result.answer) > 0
        assert "Friday" in result.answer or "Fridays" in result.answer or "Found" in result.answer

    
    async def test_labour_cost_last_week(self):
        """Answer: 'what was our wage cost last week' should return cost data."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="What was our wage cost last week?",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.LABOUR_COST
        assert result.confidence >= 0.5
        assert "wage cost" in result.answer.lower() or "total" in result.answer.lower()
        assert result.source_rows >= 0

    
    async def test_staff_query(self):
        """Answer: 'who's been at the venue most' should list top staff."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="Who's been at the venue most this month?",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.STAFF_QUERY
        assert result.confidence >= 0.5
        assert len(result.answer) > 0

    
    async def test_sales_query(self):
        """Answer: 'best hour last Saturday' should find peak sales hour."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="Best hour last Saturday",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.SALES_QUERY
        assert result.confidence >= 0.5

    
    async def test_forecast_query(self):
        """Answer: 'what's tomorrow looking like' should return forecast."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="What's tomorrow looking like?",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.FORECAST_QUERY
        assert result.confidence >= 0.5

    
    async def test_unknown_intent(self):
        """Answer: unknown question should return helpful message."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="Tell me a joke",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.intent == QueryIntent.UNKNOWN
        assert result.confidence == 0.3
        assert "help you" in result.answer.lower()
        assert "questions" in result.answer.lower()

    
    async def test_result_structure(self):
        """Verify QueryResult has all required fields."""
        def mock_context_builder(venue_id, today, weeks_of_history=12):
            return MockQueryContext()

        agent = AskAgent(context_builder=mock_context_builder)
        result = await agent.answer(
            question="What's tomorrow?",
            venue_id="test-venue",
            today=date(2024, 4, 15),
        )

        assert result.question == "What's tomorrow?"
        assert isinstance(result.intent, QueryIntent)
        assert isinstance(result.answer, str)
        assert isinstance(result.data, dict)
        assert 0.0 <= result.confidence <= 1.0
        assert isinstance(result.source_rows, int)
        assert isinstance(result.timestamp, datetime)


# ============================================================================
# Run tests
# ============================================================================

if __name__ == "__main__":
    # pytest.main([__file__, "-v"])
    pass
