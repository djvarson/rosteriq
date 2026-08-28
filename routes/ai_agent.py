"""
AI Agent API routes.

Provides chat, insights, and action endpoints for the RosterIQ AI copilot.

Routes:
    POST /api/ai/chat       -- Send a message, get AI response with venue context
    GET  /api/ai/insights   -- Get proactive insight cards for the dashboard
    POST /api/ai/action     -- Execute an action the agent suggested (with confirmation)
    GET  /api/ai/status     -- Check if AI agent is configured and available
"""

import logging
import os
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.ai_agent import (
    RosterIQAgent,
    generate_insights,
    llm_configured,
    LLM_PROVIDER,
    GEMINI_MODEL,
    MINIMAX_MODEL,
)
from rosteriq.middleware.tenant import get_tenant_context_optional
from rosteriq.services.demo import DEMO_USER_ID, DEMO_VENUE_ID


def _active_model() -> Optional[str]:
    if not llm_configured():
        return None
    return MINIMAX_MODEL if LLM_PROVIDER == "minimax" else GEMINI_MODEL


def _guard_demo_scope(venue_id: str) -> None:
    """The sandboxed demo user may only target the demo venue. Real users are
    unaffected. Prevents the public demo session from being a cross-tenant lever."""
    ctx = get_tenant_context_optional()
    if ctx and ctx.user_id == DEMO_USER_ID and venue_id != DEMO_VENUE_ID:
        raise HTTPException(
            status_code=403,
            detail="The demo account can only access the demo venue.",
        )

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ai", tags=["ai-agent"])


# ============================================================================
# Request / Response models
# ============================================================================

class ChatMessage(BaseModel):
    role: str = Field(..., description="'user' or 'assistant'")
    content: str = Field(..., description="Message text")


class ChatRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    message: str = Field(..., min_length=1, max_length=4000, description="User's message")
    conversation_id: Optional[str] = Field(None, description="Conversation ID for continuity")
    history: list[ChatMessage] = Field(default_factory=list, description="Previous messages in this conversation")


class ActionRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    action_type: str = Field(..., description="Action type from agent suggestion")
    params: dict = Field(default_factory=dict, description="Action parameters")


# ============================================================================
# In-memory conversation store (per-venue, last N conversations)
# In production this would be in Redis/Postgres.
# ============================================================================

_conversations: dict[str, list[dict]] = {}
_MAX_CONVERSATIONS = 100
_MAX_HISTORY_MESSAGES = 30


def _get_conversation(conv_id: str) -> list[dict]:
    return _conversations.get(conv_id, [])


def _save_conversation(conv_id: str, messages: list[dict]):
    # Trim to last N messages
    _conversations[conv_id] = messages[-_MAX_HISTORY_MESSAGES:]
    # Evict old conversations if too many
    if len(_conversations) > _MAX_CONVERSATIONS:
        oldest = list(_conversations.keys())[0]
        del _conversations[oldest]


# ============================================================================
# Chat endpoint
# ============================================================================


# ---------------------------------------------------------------------------
# Per-venue daily budget. Every venue's questions bill ONE shared MiniMax
# key, so a single venue's runaway script (or an over-enthusiastic staff
# member) could exhaust the quota and take the AI down for everyone. The
# counter is in-process per worker (two workers => an effective cap of up to
# 2x AI_DAILY_LIMIT) — that coarseness is fine: the point is stopping
# thousands, not policing the boundary at one.
# ---------------------------------------------------------------------------
AI_DAILY_LIMIT = int(os.environ.get("AI_DAILY_LIMIT", "300"))
_ai_usage: dict = {}      # (venue_id, yyyymmdd) -> count


def _check_ai_budget(venue_id: str) -> None:
    from datetime import datetime as _dt
    day = _dt.utcnow().strftime("%Y%m%d")
    key = (venue_id or "?", day)
    used = _ai_usage.get(key, 0) + 1
    _ai_usage[key] = used
    if len(_ai_usage) > 2000:
        _ai_usage.clear()
        _ai_usage[key] = used
    if used > AI_DAILY_LIMIT:
        try:
            from rosteriq.services.events import security
            security("ai.budget_exceeded", venue_id=venue_id, outcome="throttled",
                     used=used, limit=AI_DAILY_LIMIT)
        except Exception:
            pass
        raise HTTPException(
            status_code=429,
            detail=("This venue has reached today's AI limit "
                    f"({AI_DAILY_LIMIT} questions). It resets at midnight UTC — "
                    "the dashboards and reports keep working as normal."),
        )

@router.post("/chat")
async def chat(body: ChatRequest) -> dict:
    """
    Send a message to the AI agent and get a response.

    The agent has access to the venue's real data and can use tools
    to look up employees, shifts, costs, compliance, and more.
    It can also suggest actions (roster generation, shift changes, messaging).
    """
    _guard_demo_scope(body.venue_id)
    _check_ai_budget(body.venue_id)

    if not llm_configured():
        env = "MINIMAX_API_KEY" if LLM_PROVIDER == "minimax" else "GEMINI_API_KEY"
        raise HTTPException(
            status_code=503,
            detail=f"AI agent not configured. Set the {env} environment variable.",
        )

    # Build or restore conversation
    conv_id = body.conversation_id or str(uuid.uuid4())
    messages = _get_conversation(conv_id)

    # If client sent history, use that (for page refreshes)
    if body.history and not messages:
        messages = [{"role": m.role, "content": m.content} for m in body.history]

    # Add the new user message
    messages.append({"role": "user", "content": body.message})

    # Run the agent
    try:
        agent = RosterIQAgent(venue_id=body.venue_id)
        result = await agent.chat(messages)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"AI agent error for venue {body.venue_id}: {e}", exc_info=True)
        detail = str(e) if str(e) else "AI agent encountered an error. Please try again."
        raise HTTPException(status_code=500, detail=detail)

    # Save conversation with the assistant's response
    messages.append({"role": "assistant", "content": result["response"]})
    _save_conversation(conv_id, messages)

    return {
        "conversation_id": conv_id,
        "response": result["response"],
        "actions": result.get("actions", []),
        "tool_calls": result.get("tool_calls", []),
    }


# ============================================================================
# Insights endpoint
# ============================================================================

@router.get("/insights")
async def get_insights(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    max_count: int = Query(5, ge=1, le=10, description="Maximum insights to return"),
) -> dict:
    """
    Get proactive AI insight cards for the dashboard.

    These are fast rule-based checks (no LLM call) that surface
    staffing gaps, cost alerts, compliance issues, and demand signals.
    """
    _guard_demo_scope(venue_id)

    try:
        insights = await generate_insights(venue_id, max_insights=max_count)
    except Exception as e:
        logger.error(f"Insight generation failed for venue {venue_id}: {e}")
        insights = []

    return {
        "venue_id": venue_id,
        "insights": insights,
        "count": len(insights),
        "generated_at": datetime.utcnow().isoformat(),
    }


# ============================================================================
# Action execution endpoint
# ============================================================================

@router.post("/action")
async def execute_action(body: ActionRequest) -> dict:
    """
    Execute an action that the AI agent suggested.

    The agent returns action_required=True for operations that need
    confirmation (roster generation, shift changes, messaging).
    The frontend shows a confirmation button, then calls this endpoint.
    """
    db = get_db()
    action_type = body.action_type

    # NOTE: these AI-suggested actions are not yet wired to the execution engines.
    # They previously returned a fabricated success ("Message sent to 8 staff",
    # "Roster generation queued") while doing NOTHING — dangerous on the messaging
    # path especially. Until they're wired to the real engines (roster generator /
    # shift update / notification hub) they FAIL LOUD and point at the working UI,
    # so a manager is never misled into thinking staff were notified.
    try:
        known = {"generate_roster", "adjust_shift", "send_message"}
        if action_type in known:
            redirect = {
                "generate_roster": "roster",
                "adjust_shift": "roster",
                "send_message": "staff",
            }[action_type]
            guidance = {
                "generate_roster": "Open the Roster tab and click Generate to build the roster.",
                "adjust_shift": "Edit the shift directly in the Roster tab.",
                "send_message": "Use the Staff tab to message your team — the AI shortcut isn't wired yet.",
            }[action_type]
            raise HTTPException(
                status_code=501,
                detail={
                    "status": "not_implemented",
                    "action_type": action_type,
                    "message": guidance,
                    "redirect": redirect,
                },
            )

        raise HTTPException(status_code=400, detail=f"Unknown action type: {action_type}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Action execution error ({action_type}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to execute action: {e}")


# ============================================================================
# Status endpoint
# ============================================================================

@router.get("/status")
async def ai_status() -> dict:
    """Check if the AI agent is configured and available."""
    available = llm_configured()
    return {
        "available": available,
        "provider": LLM_PROVIDER,
        "model": _active_model(),
        "capabilities": ["chat", "insights", "actions"] if available else [],
    }
