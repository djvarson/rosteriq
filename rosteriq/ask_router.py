"""
API router for conversational query endpoint.

Wires the AskAgent to FastAPI with two endpoints:
  - POST /api/v1/ask: Answer a question
  - GET /api/v1/ask/examples: List example questions by intent
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any, List, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.ask_agent import AskAgent, QueryIntent, QueryResult


# ============================================================================
# Request/Response Models
# ============================================================================

class AskRequest(BaseModel):
    """Request body for /api/v1/ask endpoint."""
    question: str = Field(..., min_length=1, description="User's question")
    venue_id: str = Field(default="demo-venue", description="Venue ID to query")


class QueryResultResponse(BaseModel):
    """Response shape for a query result."""
    question: str
    intent: str
    answer: str
    data: Dict[str, Any]
    confidence: float
    source_rows: int
    timestamp: str  # ISO format


class ExampleQuestion(BaseModel):
    """One example question grouped by intent."""
    intent: str
    question: str
    description: str


# ============================================================================
# Router Setup
# ============================================================================

_ask_agent = AskAgent()

ask_router = APIRouter(prefix="/api/v1/ask", tags=["ask"])


# ============================================================================
# Endpoints
# ============================================================================

@ask_router.post("", response_model=QueryResultResponse)
async def ask_question(req: AskRequest) -> QueryResultResponse:
    """
    Answer a conversational question about venue historical data.

    Uses intent classification and pattern matching to dispatch the
    question to the appropriate handler.

    Data mode (demo vs live) is controlled via ROSTERIQ_DATA_MODE env var.
    """
    if not req.question or not req.question.strip():
        raise HTTPException(status_code=400, detail="question cannot be empty")

    today = date.today()

    # Call the async agent
    result: QueryResult = await _ask_agent.answer(
        question=req.question,
        venue_id=req.venue_id,
        today=today,
    )

    return QueryResultResponse(
        question=result.question,
        intent=result.intent.value,
        answer=result.answer,
        data=result.data,
        confidence=result.confidence,
        source_rows=result.source_rows,
        timestamp=result.timestamp.isoformat(),
    )


@ask_router.get("/examples", response_model=List[ExampleQuestion])
async def ask_examples() -> List[ExampleQuestion]:
    """
    List example questions grouped by intent.

    Helps users understand what questions the agent can answer.
    """
    examples = [
        # HISTORICAL_COMPARE
        ExampleQuestion(
            intent=QueryIntent.HISTORICAL_COMPARE.value,
            question="How did last Friday compare to this Friday?",
            description="Compare metrics across two similar periods",
        ),
        ExampleQuestion(
            intent=QueryIntent.HISTORICAL_COMPARE.value,
            question="What was different about last week vs this week?",
            description="Broad historical comparison",
        ),

        # PATTERN_LOOKUP
        ExampleQuestion(
            intent=QueryIntent.PATTERN_LOOKUP.value,
            question="Show me Fridays with rain in July",
            description="Find days matching specific conditions",
        ),
        ExampleQuestion(
            intent=QueryIntent.PATTERN_LOOKUP.value,
            question="What were the rainy Saturdays like?",
            description="Analyze patterns across weather conditions",
        ),

        # LABOUR_COST
        ExampleQuestion(
            intent=QueryIntent.LABOUR_COST.value,
            question="What was our wage cost last week?",
            description="Get wage metrics for a period",
        ),
        ExampleQuestion(
            intent=QueryIntent.LABOUR_COST.value,
            question="How much did we spend on labour yesterday?",
            description="Labour cost for a specific day",
        ),

        # FORECAST_QUERY
        ExampleQuestion(
            intent=QueryIntent.FORECAST_QUERY.value,
            question="What's tomorrow looking like?",
            description="Forecast for the next day",
        ),
        ExampleQuestion(
            intent=QueryIntent.FORECAST_QUERY.value,
            question="Forecast for next Friday",
            description="Demand and staffing outlook",
        ),

        # STAFF_QUERY
        ExampleQuestion(
            intent=QueryIntent.STAFF_QUERY.value,
            question="Who's been at the venue most this month?",
            description="Identify active staff members",
        ),
        ExampleQuestion(
            intent=QueryIntent.STAFF_QUERY.value,
            question="Which staff member has worked the most hours?",
            description="Staff workload analysis",
        ),

        # SALES_QUERY
        ExampleQuestion(
            intent=QueryIntent.SALES_QUERY.value,
            question="Best hour last Saturday",
            description="Peak sales periods",
        ),
        ExampleQuestion(
            intent=QueryIntent.SALES_QUERY.value,
            question="Show me our best sales day in the last month",
            description="Historical sales performance",
        ),
    ]
    return examples
