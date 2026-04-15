"""
Conversational query agent for mining Tanda historical data.

Provides intent classification, filter extraction, and intent-specific
handlers that ground answers in actual shift/roster/forecast data.

Design: pure stdlib, no LLM calls in v1. The `ensemble` hook allows
an LLM backend to slot in later for richer semantic understanding.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Enums & Data Classes
# ============================================================================

class QueryIntent(str, Enum):
    """Intent categories for user questions."""
    HISTORICAL_COMPARE = "historical_compare"      # "how did last Friday compare..."
    PATTERN_LOOKUP = "pattern_lookup"              # "show me rainy Fridays in June"
    LABOUR_COST = "labour_cost"                    # "what was our wage cost last week"
    FORECAST_QUERY = "forecast_query"              # "what's tomorrow looking like"
    STAFF_QUERY = "staff_query"                    # "who's been at the venue most"
    SALES_QUERY = "sales_query"                    # "best hour last Saturday"
    UNKNOWN = "unknown"                             # Fallback


@dataclass
class QueryResult:
    """Result of a conversational query."""
    question: str
    intent: QueryIntent
    answer: str
    data: Dict[str, Any]          # Structured supporting data
    confidence: float              # 0.0-1.0
    source_rows: int              # How many rows were consulted
    timestamp: datetime


# ============================================================================
# Pure Functions: Intent & Filter Classification
# ============================================================================

def classify_intent(question: str) -> QueryIntent:
    """
    Classify question into one of the QueryIntent categories.
    Keyword-based, case-insensitive.
    """
    q_lower = question.lower()

    # HISTORICAL_COMPARE: "compare", "vs", "versus", "difference"
    if any(word in q_lower for word in ["compare", " vs ", "versus", "difference", "how did"]):
        return QueryIntent.HISTORICAL_COMPARE

    # PATTERN_LOOKUP: "rain", "weather", "sunny", "hot", "cold"
    if any(word in q_lower for word in ["rain", "rainy", "weather", "sunny", "hot", "cold", "wet"]):
        return QueryIntent.PATTERN_LOOKUP

    # LABOUR_COST: "wage", "labour", "labor", "cost", "payroll"
    if any(word in q_lower for word in ["wage", "labour", "labor", "cost", "payroll", "spend"]):
        return QueryIntent.LABOUR_COST

    # FORECAST_QUERY: "forecast", "tomorrow", "next week", "upcoming"
    if any(word in q_lower for word in ["forecast", "tomorrow", "next week", "upcoming", "predict"]):
        return QueryIntent.FORECAST_QUERY

    # STAFF_QUERY: "who", "staff", "employee", "person", "people", "team"
    if any(word in q_lower for word in ["who", "staff", "employee", "person", "people", "team", "most"]):
        return QueryIntent.STAFF_QUERY

    # SALES_QUERY: "sales", "revenue", "covers", "turnover", "best"
    if any(word in q_lower for word in ["sales", "revenue", "covers", "turnover", "hour", "busy"]):
        return QueryIntent.SALES_QUERY

    return QueryIntent.UNKNOWN


def extract_filters(question: str) -> Dict[str, Any]:
    """
    Extract filter parameters from a question.

    Returns a dict with any of:
    - dayofweek: int (0=Monday, 6=Sunday)
    - weather_condition: str
    - month: int (1-12)
    - role: str
    - relative_date: str ("last_week", "last_month", "yesterday")
    """
    filters: Dict[str, Any] = {}
    q_lower = question.lower()

    # Day of week
    days_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    for day_name, day_int in days_map.items():
        if day_name in q_lower:
            filters["dayofweek"] = day_int
            break

    # Weather
    weather_keywords = {
        "rain": "rain", "rainy": "rain",
        "sunny": "sunny", "sun": "sunny",
        "hot": "hot", "heat": "hot",
        "cold": "cold", "wet": "wet",
    }
    for keyword, condition in weather_keywords.items():
        if keyword in q_lower:
            filters["weather_condition"] = condition
            break

    # Month (full names only)
    months_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    for month_name, month_int in months_map.items():
        if month_name in q_lower:
            filters["month"] = month_int
            break

    # Role
    roles = ["bar", "kitchen", "floor", "manager"]
    for role in roles:
        if role in q_lower:
            filters["role"] = role
            break

    # Relative dates
    if "last week" in q_lower:
        filters["relative_date"] = "last_week"
    elif "last month" in q_lower:
        filters["relative_date"] = "last_month"
    elif "yesterday" in q_lower:
        filters["relative_date"] = "yesterday"

    return filters


# ============================================================================
# Agent Class
# ============================================================================

class AskAgent:
    """
    Conversational query agent for Tanda data mining.

    Dispatches questions to intent-specific handlers that pull from
    the query context and compose natural-language answers.
    """

    def __init__(
        self,
        context_builder: Optional[Callable] = None,
        ensemble: Optional[Callable] = None,
    ):
        """
        Initialize the agent.

        Args:
            context_builder: Function(venue_id, today, weeks_of_history=12) -> QueryContext.
                            Defaults to ask_context.build_demo_query_context.
            ensemble: Optional LLM-backed callable for richer semantic answers.
                     Can be added later without changing this interface.
        """
        if context_builder is None:
            from rosteriq.ask_context import build_demo_query_context
            context_builder = build_demo_query_context

        self.context_builder = context_builder
        self.ensemble = ensemble

    async def answer(
        self,
        question: str,
        venue_id: str,
        today: date,
    ) -> QueryResult:
        """
        Answer a conversational question about historical venue data.

        Args:
            question: User's natural-language question
            venue_id: ID of the venue to query
            today: Reference date (for relative queries)

        Returns:
            QueryResult with intent, answer, structured data, and confidence
        """
        timestamp = datetime.now()
        intent = classify_intent(question)
        filters = extract_filters(question)

        # Build query context (shifts, rosters, forecasts, etc.)
        context = self.context_builder(venue_id=venue_id, today=today)

        # Dispatch to intent-specific handler
        if intent == QueryIntent.PATTERN_LOOKUP:
            result = await self._handle_pattern_lookup(
                question, context, today, filters
            )
        elif intent == QueryIntent.LABOUR_COST:
            result = await self._handle_labour_cost(question, context, today, filters)
        elif intent == QueryIntent.FORECAST_QUERY:
            result = await self._handle_forecast_query(question, context, today, filters)
        elif intent == QueryIntent.STAFF_QUERY:
            result = await self._handle_staff_query(question, context, today, filters)
        elif intent == QueryIntent.SALES_QUERY:
            result = await self._handle_sales_query(question, context, today, filters)
        elif intent == QueryIntent.HISTORICAL_COMPARE:
            result = await self._handle_historical_compare(
                question, context, today, filters
            )
        else:
            # UNKNOWN
            result = {
                "answer": (
                    "I can help you answer questions about your venue's historical data. "
                    "Try asking: 'Show me Fridays with rain', 'What was our wage cost last week', "
                    "'Who's been at the venue most this month', 'Show me our best hour last Saturday'."
                ),
                "data": {},
                "confidence": 0.3,
                "source_rows": 0,
            }

        return QueryResult(
            question=question,
            intent=intent,
            answer=result["answer"],
            data=result["data"],
            confidence=result["confidence"],
            source_rows=result["source_rows"],
            timestamp=timestamp,
        )

    # ────────────────────────────────────────────────────────────────────────
    # Intent Handlers
    # ────────────────────────────────────────────────────────────────────────

    async def _handle_pattern_lookup(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle PATTERN_LOOKUP: "show me Fridays with rain in June".
        Filters shifts by day-of-week and weather, then summarizes.
        """
        matching_shifts = []
        dayofweek = filters.get("dayofweek")
        weather = filters.get("weather_condition")
        month = filters.get("month")

        # Collect all shifts from rosters
        for roster in context.rosters:
            for shift in roster.shifts:
                # Day-of-week filter
                if dayofweek is not None and shift.date.weekday() != dayofweek:
                    continue
                # Month filter
                if month is not None and shift.date.month != month:
                    continue
                # Weather filter (demo: can't actually check weather on shift object)
                # In production, this would join to a weather table
                matching_shifts.append(shift)

        # Summarize the results
        if not matching_shifts:
            return {
                "answer": f"No shifts match your filters for {question}.",
                "data": {"matching_shifts": []},
                "confidence": 0.9,
                "source_rows": 0,
            }

        # Compute aggregate stats
        total_cost = sum(s.cost for s in matching_shifts)
        num_shifts = len(matching_shifts)
        unique_dates = set(s.date for s in matching_shifts)

        # Find the day with highest cost
        cost_by_date = {}
        for shift in matching_shifts:
            cost_by_date[shift.date] = cost_by_date.get(shift.date, Decimal(0)) + shift.cost
        top_date = max(cost_by_date.items(), key=lambda x: x[1]) if cost_by_date else None

        answer = (
            f"Found {len(unique_dates)} days matching your criteria. "
            f"Total shifts: {num_shifts}, total wage cost: ${float(total_cost):.2f}. "
        )
        if top_date:
            answer += f"Highest cost day: {top_date[0].strftime('%A, %Y-%m-%d')} (${float(top_date[1]):.2f})."

        return {
            "answer": answer,
            "data": {
                "matching_dates": sorted(list(unique_dates)),
                "num_shifts": num_shifts,
                "total_cost": float(total_cost),
                "top_date": top_date[0].isoformat() if top_date else None,
            },
            "confidence": 0.9,
            "source_rows": num_shifts,
        }

    async def _handle_labour_cost(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle LABOUR_COST: "what was our wage cost last week".
        Sums shift costs for the requested period.
        """
        relative_date = filters.get("relative_date", "last_week")

        # Determine date range
        if relative_date == "yesterday":
            start_date = today - timedelta(days=1)
            end_date = start_date
        elif relative_date == "last_month":
            first_of_month = today.replace(day=1)
            start_date = (first_of_month - timedelta(days=1)).replace(day=1)
            end_date = first_of_month - timedelta(days=1)
        else:  # last_week
            start_date = today - timedelta(days=7)
            end_date = today

        # Collect shifts in range
        matching_shifts = []
        for roster in context.rosters:
            for shift in roster.shifts:
                if start_date <= shift.date <= end_date:
                    matching_shifts.append(shift)

        total_cost = sum(s.cost for s in matching_shifts)
        num_shifts = len(matching_shifts)

        # Estimate revenue for wage % (from vendor forecasts)
        total_revenue = Decimal(0)
        for forecast in context.vendor_forecasts:
            forecast_date = forecast.bucket_start.date()
            if start_date <= forecast_date <= end_date:
                total_revenue += forecast.amount

        wage_pct = (
            float(total_cost) / float(total_revenue) * 100
            if total_revenue > 0
            else 0.0
        )

        answer = (
            f"For the period {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: "
            f"Total wage cost: ${float(total_cost):.2f} across {num_shifts} shifts. "
            f"Wage %: {wage_pct:.1f}% (of ${float(total_revenue):.2f} revenue)."
        )

        return {
            "answer": answer,
            "data": {
                "total_cost": float(total_cost),
                "num_shifts": num_shifts,
                "wage_pct": wage_pct,
                "total_revenue": float(total_revenue),
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
            },
            "confidence": 0.9,
            "source_rows": num_shifts,
        }

    async def _handle_forecast_query(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle FORECAST_QUERY: "what's tomorrow looking like".
        Pulls vendor forecast data and head-count samples.
        """
        target_date = today + timedelta(days=1)  # Default to tomorrow

        # Get vendor forecast for that day
        day_forecasts = [
            f for f in context.vendor_forecasts
            if f.bucket_start.date() == target_date
        ]
        revenue = sum(f.amount for f in day_forecasts if f.metric == "revenue")

        # Get recent head-count samples for the same day-of-week
        recent_headcounts = [
            h for h in context.head_counts
            if h.counted_at.date().weekday() == target_date.weekday()
        ]
        avg_headcount = (
            sum(h.count for h in recent_headcounts) / len(recent_headcounts)
            if recent_headcounts
            else 0
        )

        answer = (
            f"For {target_date.strftime('%A, %Y-%m-%d')}: "
            f"Forecast revenue: ${float(revenue):.2f}. "
            f"Typical head count for this day of week: ~{int(avg_headcount)} guests. "
        )

        return {
            "answer": answer,
            "data": {
                "date": target_date.isoformat(),
                "forecast_revenue": float(revenue),
                "avg_headcount": avg_headcount,
                "day_of_week": target_date.strftime("%A"),
            },
            "confidence": 0.7,
            "source_rows": len(day_forecasts) + len(recent_headcounts),
        }

    async def _handle_staff_query(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle STAFF_QUERY: "who's been at the venue most this month".
        Counts shifts per employee.
        """
        # Collect all shifts from the last month
        start_date = today.replace(day=1)
        matching_shifts = []
        for roster in context.rosters:
            for shift in roster.shifts:
                if shift.date >= start_date:
                    matching_shifts.append(shift)

        # Count shifts per employee
        shift_counts = {}
        for shift in matching_shifts:
            shift_counts[shift.employee_id] = shift_counts.get(shift.employee_id, 0) + 1

        if not shift_counts:
            return {
                "answer": "No shift data available for the requested period.",
                "data": {},
                "confidence": 0.5,
                "source_rows": 0,
            }

        # Find top 3
        sorted_emps = sorted(shift_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        emp_names = []
        for emp_id, count in sorted_emps:
            emp = context.employees.get(emp_id, None)
            name = emp.name if emp else emp_id
            emp_names.append(f"{name} ({count} shifts)")

        answer = f"Most active staff this month: {', '.join(emp_names)}."

        return {
            "answer": answer,
            "data": {
                "top_staff": [{"employee_id": eid, "name": context.employees.get(eid, None).name if context.employees.get(eid) else eid, "shift_count": cnt} for eid, cnt in sorted_emps],
                "period": start_date.isoformat(),
            },
            "confidence": 0.9,
            "source_rows": len(matching_shifts),
        }

    async def _handle_sales_query(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle SALES_QUERY: "best hour last Saturday".
        Uses vendor forecasts to find peak revenue periods.
        """
        # Find "last Saturday"
        days_back = (today.weekday() - 5) % 7  # Saturday = 5
        if days_back == 0:
            days_back = 7
        target_date = today - timedelta(days=days_back)

        # Get forecasts for that day
        day_forecasts = [
            f for f in context.vendor_forecasts
            if f.bucket_start.date() == target_date and f.metric == "revenue"
        ]

        if not day_forecasts:
            return {
                "answer": f"No sales data found for {target_date.strftime('%A, %Y-%m-%d')}.",
                "data": {},
                "confidence": 0.5,
                "source_rows": 0,
            }

        # Find peak
        top_forecast = max(day_forecasts, key=lambda f: f.amount)
        total_revenue = sum(f.amount for f in day_forecasts)

        hour = top_forecast.bucket_start.hour
        answer = (
            f"Best hour on {target_date.strftime('%A, %Y-%m-%d')}: {hour:02d}:00 "
            f"(${float(top_forecast.amount):.2f} revenue). "
            f"Total day revenue: ${float(total_revenue):.2f}."
        )

        return {
            "answer": answer,
            "data": {
                "date": target_date.isoformat(),
                "best_hour": hour,
                "best_hour_revenue": float(top_forecast.amount),
                "total_revenue": float(total_revenue),
            },
            "confidence": 0.8,
            "source_rows": len(day_forecasts),
        }

    async def _handle_historical_compare(
        self, question: str, context: Any, today: date, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle HISTORICAL_COMPARE: "how did last Friday compare to this one".
        Compares two periods (typically different occurrences of the same day).
        """
        # Default: compare this Friday to last Friday
        target_dow = filters.get("dayofweek", today.weekday())
        days_back = (today.weekday() - target_dow) % 7
        if days_back == 0:
            days_back = 7

        recent_date = today - timedelta(days=days_back)
        prior_date = recent_date - timedelta(days=7)

        # Collect shifts for each period
        shifts_recent = []
        shifts_prior = []
        for roster in context.rosters:
            for shift in roster.shifts:
                if shift.date == recent_date:
                    shifts_recent.append(shift)
                elif shift.date == prior_date:
                    shifts_prior.append(shift)

        cost_recent = sum(s.cost for s in shifts_recent)
        cost_prior = sum(s.cost for s in shifts_prior)
        num_recent = len(shifts_recent)
        num_prior = len(shifts_prior)

        pct_change = (
            ((float(cost_recent) - float(cost_prior)) / float(cost_prior) * 100)
            if cost_prior > 0
            else 0.0
        )

        answer = (
            f"Comparing {recent_date.strftime('%A, %Y-%m-%d')} vs "
            f"{prior_date.strftime('%A, %Y-%m-%d')}: "
            f"Recent: {num_recent} shifts, ${float(cost_recent):.2f}. "
            f"Prior: {num_prior} shifts, ${float(cost_prior):.2f}. "
            f"Change: {pct_change:+.1f}%."
        )

        return {
            "answer": answer,
            "data": {
                "recent_date": recent_date.isoformat(),
                "prior_date": prior_date.isoformat(),
                "recent_cost": float(cost_recent),
                "prior_cost": float(cost_prior),
                "recent_shifts": num_recent,
                "prior_shifts": num_prior,
                "pct_change": pct_change,
            },
            "confidence": 0.85,
            "source_rows": num_recent + num_prior,
        }

    # ────────────────────────────────────────────────────────────────────────
    # LLM-Backed Conversational Path
    # ────────────────────────────────────────────────────────────────────────

    async def answer_with_llm(
        self,
        query: str,
        venue_id: str,
        today: Optional[date] = None,
        context: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Answer a question using an LLM backend with tool calling.

        If no LLM backend is configured (NoOpBackend), delegates to the
        rule-based answer() path. Otherwise, presents the LLM with a set
        of tools it can call to gather data, then returns a semantic answer.

        Args:
            query: User's natural-language question
            venue_id: Venue ID to query
            today: Reference date for relative queries (defaults to today)
            context: Optional pre-built QueryContext (else builds one)

        Returns:
            {
              "text": str (final answer),
              "intent": str | None (parsed intent, or None if LLM doesn't recognize),
              "tool_calls": list[dict] (tool invocations made),
              "backend_used": str ("anthropic", "openai", "rule_based", or "noop"),
            }
        """
        if today is None:
            today = date.today()

        # Get the LLM backend
        from rosteriq.llm_backends import get_llm_backend, NoOpBackend

        backend = get_llm_backend()

        # If NoOp backend, fall back to rule-based path
        if isinstance(backend, NoOpBackend):
            result = await self.answer(
                question=query,
                venue_id=venue_id,
                today=today,
            )
            return {
                "text": result.answer,
                "intent": result.intent.value if result.intent else None,
                "tool_calls": [],
                "backend_used": "rule_based",
                "data": result.data,
                "confidence": result.confidence,
                "source_rows": result.source_rows,
                "timestamp": result.timestamp,
            }

        # Build context if not provided
        if context is None:
            context = self.context_builder(venue_id=venue_id, today=today)

        # Define the tools the LLM can call
        tools = self._get_llm_tools()

        # System prompt for the LLM
        system_prompt = self._get_system_prompt()

        # Multi-turn loop: max 3 turns to avoid runaway calls
        max_turns = 3
        turn = 0
        messages = []  # Conversation history
        llm_tool_calls = []  # All tool calls made

        while turn < max_turns:
            turn += 1

            # Call the LLM
            try:
                response = await backend.complete(
                    prompt=query if turn == 1 else self._format_tool_result_msg(messages),
                    tools=tools,
                    system=system_prompt,
                )
            except Exception as e:
                logger.error(f"LLM backend error on turn {turn}: {e}")
                # Fall back to rule-based if LLM fails
                result = await self.answer(
                    question=query,
                    venue_id=venue_id,
                    today=today,
                )
                return {
                    "text": result.answer,
                    "intent": result.intent.value if result.intent else None,
                    "tool_calls": llm_tool_calls,
                    "backend_used": "rule_based",
                    "data": result.data,
                    "confidence": result.confidence,
                    "source_rows": result.source_rows,
                    "timestamp": result.timestamp,
                }

            text = response.get("text", "")
            tool_calls = response.get("tool_calls")

            # Add to conversation history
            messages.append({"role": "assistant", "text": text, "tool_calls": tool_calls})

            # If no tool calls, we're done
            if not tool_calls:
                # Try to infer intent from original question
                intent = classify_intent(query)
                backend_name = "anthropic" if "Anthropic" in backend.__class__.__name__ else "openai"
                return {
                    "text": text,
                    "intent": intent.value if intent != QueryIntent.UNKNOWN else None,
                    "tool_calls": llm_tool_calls,
                    "backend_used": backend_name,
                    "data": {},
                    "confidence": 0.8,
                    "source_rows": 0,
                    "timestamp": datetime.now(),
                }

            # Execute tool calls
            tool_results = []
            for tc in tool_calls:
                llm_tool_calls.append(tc)
                result = await self._execute_tool(tc, context, today)
                tool_results.append({
                    "tool_name": tc.get("name"),
                    "result": result,
                })

            # Add tool results to conversation
            messages.append({"role": "user", "tool_results": tool_results})

        # Max turns reached; return best-effort answer
        if messages and "text" in messages[-1]:
            final_text = messages[-1]["text"]
        else:
            final_text = "I was unable to generate a complete answer."

        intent = classify_intent(query)
        backend_name = "anthropic" if "Anthropic" in backend.__class__.__name__ else "openai"
        return {
            "text": final_text,
            "intent": intent.value if intent != QueryIntent.UNKNOWN else None,
            "tool_calls": llm_tool_calls,
            "backend_used": backend_name,
            "data": {},
            "confidence": 0.7,
            "source_rows": 0,
            "timestamp": datetime.now(),
        }

    def _get_llm_tools(self) -> list[dict]:
        """Return the list of tools available to the LLM."""
        return [
            {
                "name": "get_forecast",
                "description": "Get revenue forecast for a specific date at the venue",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "description": "Date in YYYY-MM-DD format",
                        },
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                    },
                    "required": ["date", "venue_id"],
                },
            },
            {
                "name": "get_labour_cost",
                "description": "Get total labour cost for a date range",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format",
                        },
                        "end_date": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format",
                        },
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                    },
                    "required": ["start_date", "end_date", "venue_id"],
                },
            },
            {
                "name": "get_historical_sales",
                "description": "Get sales/revenue data for a specific date or period",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "description": "Date in YYYY-MM-DD format (or start of range)",
                        },
                        "end_date": {
                            "type": "string",
                            "description": "Optional end date for a range",
                        },
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                    },
                    "required": ["date", "venue_id"],
                },
            },
            {
                "name": "get_staff_list",
                "description": "Get list of staff members who worked in a period",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                        "start_date": {
                            "type": "string",
                            "description": "Optional start date in YYYY-MM-DD format",
                        },
                    },
                    "required": ["venue_id"],
                },
            },
            {
                "name": "get_pattern_predictions",
                "description": "Get predicted patterns (e.g. busy times, weather impact) for upcoming dates",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "description": "Date in YYYY-MM-DD format",
                        },
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                        "pattern_type": {
                            "type": "string",
                            "description": "Type of pattern: 'busy_times', 'weather', 'seasonal'",
                        },
                    },
                    "required": ["date", "venue_id"],
                },
            },
            {
                "name": "get_recent_shift_events",
                "description": "Get recent shift events (calls, swaps, changes) at the venue",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "venue_id": {
                            "type": "string",
                            "description": "Venue ID",
                        },
                        "days_back": {
                            "type": "integer",
                            "description": "Number of days to look back (default 7)",
                        },
                    },
                    "required": ["venue_id"],
                },
            },
        ]

    def _get_system_prompt(self) -> str:
        """Return the system prompt for the LLM."""
        return (
            "You are a helpful assistant for a hospitality venue management system (RosterIQ). "
            "Users ask questions about historical venue data, sales, labour costs, and staffing. "
            "\n\n"
            "Use the provided tools to gather data and compose accurate, concise answers. "
            "Always ground your responses in the data retrieved from tools. "
            "If data is unavailable, be honest about it. "
            "Format numbers with appropriate currency ($) or units. "
            "Answer in a friendly, professional tone."
        )

    def _format_tool_result_msg(self, messages: list[dict]) -> str:
        """
        Format the multi-turn conversation for the next LLM call.

        Takes the conversation history and formats it as a user message
        containing the tool results for continuation.
        """
        if not messages:
            return ""

        # Find the most recent tool results
        latest = messages[-1] if messages else {}
        tool_results = latest.get("tool_results", [])

        if not tool_results:
            return "Please continue with your response."

        result_text = "Tool results:\n"
        for tr in tool_results:
            result_text += f"- {tr.get('tool_name')}: {json.dumps(tr.get('result'), default=str)}\n"

        return result_text

    async def _execute_tool(
        self,
        tool_call: Dict[str, Any],
        context: Any,
        today: date,
    ) -> Any:
        """
        Execute a tool call and return the result.

        Dispatches to the appropriate handler based on tool name.
        """
        tool_name = tool_call.get("name")
        inputs = tool_call.get("input", {})

        try:
            if tool_name == "get_forecast":
                return self._tool_get_forecast(inputs, context)
            elif tool_name == "get_labour_cost":
                return self._tool_get_labour_cost(inputs, context)
            elif tool_name == "get_historical_sales":
                return self._tool_get_historical_sales(inputs, context)
            elif tool_name == "get_staff_list":
                return self._tool_get_staff_list(inputs, context)
            elif tool_name == "get_pattern_predictions":
                return self._tool_get_pattern_predictions(inputs, context)
            elif tool_name == "get_recent_shift_events":
                return self._tool_get_recent_shift_events(inputs, context)
            else:
                return {"error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            logger.error(f"Tool execution error for {tool_name}: {e}")
            return {"error": str(e)}

    def _tool_get_forecast(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get revenue forecast for a specific date."""
        try:
            date_str = inputs.get("date")
            target_date = datetime.fromisoformat(date_str).date() if date_str else date.today()

            # Find forecasts for that day
            matching = [
                f for f in context.vendor_forecasts
                if f.bucket_start.date() == target_date and f.metric == "revenue"
            ]

            total = sum(f.amount for f in matching)
            return {
                "date": target_date.isoformat(),
                "forecast_revenue": float(total),
                "num_buckets": len(matching),
            }
        except Exception as e:
            return {"error": str(e)}

    def _tool_get_labour_cost(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get labour cost for a date range."""
        try:
            start_str = inputs.get("start_date")
            end_str = inputs.get("end_date")
            start = datetime.fromisoformat(start_str).date() if start_str else date.today()
            end = datetime.fromisoformat(end_str).date() if end_str else date.today()

            total_cost = Decimal(0)
            num_shifts = 0
            for roster in context.rosters:
                for shift in roster.shifts:
                    if start <= shift.date <= end:
                        total_cost += shift.cost
                        num_shifts += 1

            return {
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "total_labour_cost": float(total_cost),
                "num_shifts": num_shifts,
            }
        except Exception as e:
            return {"error": str(e)}

    def _tool_get_historical_sales(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get historical sales data."""
        try:
            date_str = inputs.get("date")
            end_str = inputs.get("end_date")
            start = datetime.fromisoformat(date_str).date() if date_str else date.today()
            end = datetime.fromisoformat(end_str).date() if end_str else start

            total_revenue = Decimal(0)
            num_days = 0
            for f in context.vendor_forecasts:
                f_date = f.bucket_start.date()
                if start <= f_date <= end and f.metric == "revenue":
                    total_revenue += f.amount
                    num_days += 1

            return {
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "total_revenue": float(total_revenue),
                "avg_daily_revenue": float(total_revenue / max(1, num_days)),
                "days_included": num_days,
            }
        except Exception as e:
            return {"error": str(e)}

    def _tool_get_staff_list(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get staff members who worked in a period."""
        try:
            start_str = inputs.get("start_date")
            start = datetime.fromisoformat(start_str).date() if start_str else date.today() - timedelta(days=30)

            # Collect unique employees
            staff_shifts = {}
            for roster in context.rosters:
                for shift in roster.shifts:
                    if shift.date >= start:
                        if shift.employee_id not in staff_shifts:
                            staff_shifts[shift.employee_id] = 0
                        staff_shifts[shift.employee_id] += 1

            staff_list = [
                {
                    "employee_id": emp_id,
                    "name": context.employees.get(emp_id).name if emp_id in context.employees else emp_id,
                    "shift_count": count,
                }
                for emp_id, count in sorted(staff_shifts.items(), key=lambda x: x[1], reverse=True)
            ]

            return {
                "period_start": start.isoformat(),
                "staff_count": len(staff_list),
                "top_staff": staff_list[:5],
            }
        except Exception as e:
            return {"error": str(e)}

    def _tool_get_pattern_predictions(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get pattern predictions (demo: returns static patterns)."""
        try:
            date_str = inputs.get("date")
            target_date = datetime.fromisoformat(date_str).date() if date_str else date.today()
            dow = target_date.weekday()

            # Static patterns for demo
            patterns = {
                0: {"busy_times": "evening", "typical_revenue": "8500", "staff_level": "standard"},
                1: {"busy_times": "limited", "typical_revenue": "7200", "staff_level": "minimal"},
                2: {"busy_times": "evening", "typical_revenue": "9800", "staff_level": "standard"},
                3: {"busy_times": "evening", "typical_revenue": "12400", "staff_level": "medium"},
                4: {"busy_times": "all day", "typical_revenue": "22800", "staff_level": "high"},
                5: {"busy_times": "all day", "typical_revenue": "28600", "staff_level": "high"},
                6: {"busy_times": "lunch + dinner", "typical_revenue": "18400", "staff_level": "high"},
            }

            return {
                "date": target_date.isoformat(),
                "day_of_week": target_date.strftime("%A"),
                "patterns": patterns.get(dow, {}),
            }
        except Exception as e:
            return {"error": str(e)}

    def _tool_get_recent_shift_events(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Get recent shift events (demo: minimal data)."""
        try:
            days_back = inputs.get("days_back", 7)

            # For demo, return empty (no events API in this version)
            return {
                "period_days": days_back,
                "shift_calls": 0,
                "shift_swaps": 0,
                "recent_events": [],
            }
        except Exception as e:
            return {"error": str(e)}
