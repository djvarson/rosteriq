"""
RosterIQ AI Agent — intelligent copilot for venue managers.

Provides three capabilities:
1. Chat — natural language Q&A grounded in the venue's real data
2. Insights — proactive alerts and suggestions surfaced on the dashboard
3. Actions — execute roster operations on the manager's behalf

Uses the Google Gemini API with function-calling to query venue data
and take actions through RosterIQ's internal APIs.
Free tier: 15 RPM / 1M tokens per day with Gemini Flash.
"""

import os
import json
import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional, AsyncGenerator
from enum import Enum

import httpx

from rosteriq.database import get_db
from rosteriq.models import Employee, Shift, State

logger = logging.getLogger(__name__)

_THINK_RE = re.compile(r"<think>.*?</think>\s*", re.DOTALL | re.IGNORECASE)


def _strip_reasoning(text: str) -> str:
    """Remove reasoning-model internals (<think>...</think>) before the text
    reaches users or conversation history. MiniMax-M3 emits these inline; a
    venue owner should never see the model talking to itself. An unclosed
    <think> with no visible text after it would blank the reply — in that
    case fall through to the raw text minus the opening tag."""
    if "<think>" not in (text or "").lower():
        return text
    stripped = _THINK_RE.sub("", text).strip()
    if stripped:
        return stripped
    return re.sub(r"</?think>", "", text, flags=re.IGNORECASE).strip()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("ROSTERIQ_AI_MODEL", "gemini-2.0-flash")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MAX_CONTEXT_TOKENS = 8000  # rough budget for venue context

# Which LLM backend powers the agent: "gemini" (default) or "minimax".
# MiniMax exposes an OpenAI-compatible Chat Completions API, so the same
# OpenAI-shaped tool/tool_calls protocol also serves any OpenAI-compatible
# endpoint (override MINIMAX_API_URL to point elsewhere).
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_MODEL = os.environ.get("MINIMAX_MODEL", "MiniMax-M3")
MINIMAX_API_URL = os.environ.get(
    "MINIMAX_API_URL", "https://api.minimax.io/v1/chat/completions"
)


def llm_configured() -> bool:
    """Whether the selected provider has the credentials it needs."""
    if LLM_PROVIDER == "minimax":
        return bool(MINIMAX_API_KEY)
    return bool(GEMINI_API_KEY)

# Rate-limit / transient-error retry tuning.
# Gemini free tier is 15 RPM, so 429s are expected under light bursts.
GEMINI_MAX_RETRIES = 3          # number of retries AFTER the first attempt
GEMINI_BACKOFF_BASE = 1.0       # seconds; exponential: 1s, 2s, 4s ...
GEMINI_BACKOFF_CAP = 8.0        # seconds; max single backoff
RATE_LIMIT_MESSAGE = "AI service rate-limited, try again shortly."

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are RosterIQ AI, an expert venue management copilot for Australian hospitality businesses.

You help managers with:
- Roster planning, shift adjustments, and staffing decisions
- Labour cost analysis and budget tracking
- Award compliance (Fair Work Act, penalty rates, break rules)
- Employee management (availability, skills, certifications)
- Demand forecasting and staffing recommendations
- Operational insights from POS, reservations, and workforce data
- SOS staff finding — locating available staff at short notice
- Cut/don't-cut decisions during live service
- AFL and event-driven demand intelligence

IMPORTANT RULES:
- Always ground your answers in the venue's actual data (provided via tools)
- Use Australian English (e.g. "rostered", "labour", "organisation")
- Quote specific numbers — don't be vague when you have data
- When suggesting actions, explain the WHY (cost saving, compliance, coverage)
- For compliance questions, cite the relevant award/legislation
- Currency is AUD ($), dates are DD/MM/YYYY format
- Be concise and practical — managers are busy during service
- If you don't have enough data, say so clearly rather than guessing

You have tools to look up venue data and take actions. Use them proactively —
don't ask the manager to go look things up when you can do it yourself.

===== VENUE INTELLIGENCE =====

DEMAND PATTERNS — AFL:
- AFL home games at Optus Stadium are the #1 demand driver for Perth venues
- A Friday night Fremantle Dockers home game is the highest-impact scenario
- Demand multipliers by game timing:
  * Friday night home game: 2.5x normal staffing
  * Saturday arvo home game: 2.0x normal staffing
  * Sunday arvo home game: 1.8x normal staffing
  * Away games: 0.9x (slight dip, fans watch at home)
  * Derby (Fremantle vs West Coast): 3.0x — treat as major event
  * Finals: 2.5-3.5x depending on round
- Pre-game surge: 2-3 hours before bounce, sports bar and front bar peak
- Post-game surge: 30-60 min after final siren, then rapid decline
- Use get_afl_fixtures tool to check upcoming games and adjust recommendations

DEMAND PATTERNS — SEASONAL:
- October is historically the worst month (end of footy season, before summer)
- January-February: quiet (school holidays, people away)
- March-September: peak trading (AFL season + cooler weather = pub weather)
- December: Christmas party season spike, especially functions
- Public holidays: check penalty rate implications before recommending extra staff
- Long weekends: higher casual dining, lower corporate/function trade

WEATHER IMPACT:
- Rain/cold: +20-30% indoor trade (people go to pubs), staff UP
- Extreme heat (>38C): -15% trade (people stay home with aircon), staff DOWN
- Perfect weather (22-28C): beer garden/outdoor peaks, reallocate staff outside
- Always factor weather into same-day cut/don't-cut decisions

LABOUR TARGETS BY AREA:
- Bottle shop: 10% labour cost target (low-touch, high-margin)
- Bar (front bar + sports bar): 13% labour cost target
- Kitchen: 28% labour cost target (high labour intensity)
- Overall F&B: 30% labour cost target
- When labour exceeds target by >3%, flag as cost alert
- When labour is under target by >5%, check if service quality is at risk

STAFFING STRUCTURE:
- Venues typically run ~8 managers (deliberately top-heavy as insurance)
- Expect to lose 1 manager within any 6-month period — factor into planning
- Young casual pipeline: runners start at 18-19, progress to bar at 20-21
- Casuals are the flex lever — scale up/down with demand
- Full-time/part-time staff form the skeleton crew baseline
- Cross-trained staff (can work bar + restaurant) are highest-value rostering assets

SKELETON CREW MINIMUMS (quiet day baseline):
- Kitchen: 2 (1 head chef + 1 cook)
- Bar: 1 per bar area
- Floor: 1 for restaurant
- Bottle shop: 1
- Total minimum: ~6 staff for a quiet Monday/Tuesday

CUT/DON'T-CUT LOGIC:
When a manager asks whether to send staff home early:
1. Check current labour % against target for that area
2. Check covers/sales in the last hour — trending up or down?
3. Check if there's an event, AFL game, or weather change coming
4. Check remaining bookings for tonight
5. If labour is OVER target AND trade is declining AND no upcoming demand signal:
   → Recommend CUT with specific staff (lowest-seniority casual first)
6. If labour is NEAR target OR there's an upcoming demand signal:
   → Recommend DON'T CUT, explain what's coming
7. Always suggest cutting casuals before part-timers, part-timers before full-timers
8. Never recommend cutting below skeleton crew minimums

SOS STAFF FINDING:
When asked to find available staff at short notice:
1. Use find_available_staff tool to get ALL staff, including those marked unavailable
2. Show available staff first, then unavailable staff (they might say yes if asked)
3. Include contact details so the manager can call/text directly
4. Prioritise by: relevant skills > proximity > seniority > cost
5. Flag anyone who hasn't updated availability in >2 weeks (stale data)
6. Ben specifically requested: "Show me the unavailable ones too — sometimes they'll
   come in if you ask nicely, especially for penalty rate shifts"

AVAILABILITY REFRESH:
- Prompt managers to remind staff to update availability every 2 weeks
- Flag staff whose availability data is >14 days old
- Suggest sending a bulk reminder via the send_staff_message tool
"""

# ---------------------------------------------------------------------------
# Tool definitions for Gemini function-calling
# Gemini uses a different schema format from Anthropic:
#   { functionDeclarations: [ { name, description, parameters: {type, properties, required} } ] }
# ---------------------------------------------------------------------------

GEMINI_TOOLS = [
    {"functionDeclarations": [
        {
            "name": "get_employees",
            "description": "Get the list of employees for this venue. Returns names, roles, employment type, contact details, and certifications.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "active_only": {"type": "BOOLEAN", "description": "If true, only return active employees. Default true."},
                },
            },
        },
        {
            "name": "get_shifts",
            "description": "Get rostered shifts for a date range. Returns shift times, assigned employees, roles, and costs.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "start_date": {"type": "STRING", "description": "Start date in YYYY-MM-DD format. Defaults to today."},
                    "end_date": {"type": "STRING", "description": "End date in YYYY-MM-DD format. Defaults to 7 days from start."},
                },
            },
        },
        {
            "name": "get_labour_summary",
            "description": "Get a labour cost summary for a date range — total hours, total cost, labour percentage, breakdown by day and role.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "start_date": {"type": "STRING", "description": "Start date YYYY-MM-DD"},
                    "end_date": {"type": "STRING", "description": "End date YYYY-MM-DD"},
                },
            },
        },
        {
            "name": "get_compliance_issues",
            "description": "Check for current compliance issues — break violations, overtime, expired certifications, award breaches.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "date_range_days": {"type": "INTEGER", "description": "Number of days ahead to check. Default 7."},
                },
            },
        },
        {
            "name": "get_venue_stats",
            "description": "Get key venue statistics — headcount, covers forecast, revenue trend, staffing level vs demand.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_upcoming_events",
            "description": "Get upcoming events, reservations, and demand signals that affect staffing needs.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "days_ahead": {"type": "INTEGER", "description": "How many days ahead to look. Default 7."},
                },
            },
        },
        {
            "name": "suggest_roster_changes",
            "description": "Analyse the current roster and suggest improvements — understaffing, overstaffing, cost savings, compliance fixes.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "target_date": {"type": "STRING", "description": "Date to analyse in YYYY-MM-DD format. Defaults to today."},
                },
            },
        },
        {
            "name": "generate_roster",
            "description": "Generate an optimised roster for a date range using RosterIQ's AI engine. Returns the proposed roster with cost estimates.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "start_date": {"type": "STRING", "description": "Start date YYYY-MM-DD"},
                    "end_date": {"type": "STRING", "description": "End date YYYY-MM-DD"},
                    "budget_limit": {"type": "NUMBER", "description": "Optional daily labour budget cap in AUD"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "adjust_shift",
            "description": "Modify an existing shift — change times, reassign employee, or cancel. Requires confirmation from the manager.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "shift_id": {"type": "STRING", "description": "The shift ID to modify"},
                    "action": {"type": "STRING", "description": "What to do: change_times, reassign, or cancel"},
                    "new_start": {"type": "STRING", "description": "New start time (HH:MM) for change_times"},
                    "new_end": {"type": "STRING", "description": "New end time (HH:MM) for change_times"},
                    "new_employee_id": {"type": "STRING", "description": "Employee ID for reassign"},
                },
                "required": ["shift_id", "action"],
            },
        },
        {
            "name": "send_staff_message",
            "description": "Send a message to staff members — shift reminders, roster updates, or custom messages via SMS/email.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "recipient_ids": {"type": "ARRAY", "items": {"type": "STRING"}, "description": "List of employee IDs to message. Use 'all' for entire team."},
                    "message": {"type": "STRING", "description": "The message to send"},
                    "channel": {"type": "STRING", "description": "How to send: sms, email, or both. Default both."},
                },
                "required": ["recipient_ids", "message"],
            },
        },
        {
            "name": "find_available_staff",
            "description": "SOS staff finder — find ALL staff and their availability status for a specific date/time. Returns both available AND unavailable staff with contact details, so the manager can call anyone. Prioritised by skills, availability, and cost.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "date": {"type": "STRING", "description": "Date to check availability for (YYYY-MM-DD). Defaults to today."},
                    "start_time": {"type": "STRING", "description": "Shift start time (HH:MM). Defaults to now."},
                    "end_time": {"type": "STRING", "description": "Shift end time (HH:MM). Defaults to close."},
                    "role": {"type": "STRING", "description": "Role needed (e.g. 'bar', 'kitchen', 'floor', 'runner'). Optional — returns all if not specified."},
                    "include_unavailable": {"type": "BOOLEAN", "description": "Include staff marked as unavailable. Default true (per venue owner request)."},
                },
            },
        },
        {
            "name": "get_afl_fixtures",
            "description": "Get upcoming AFL fixtures relevant to this venue's location. Returns game details, teams, timing, and estimated demand multiplier for staffing decisions.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "days_ahead": {"type": "INTEGER", "description": "How many days ahead to look. Default 14."},
                    "team_filter": {"type": "STRING", "description": "Filter by team name (e.g. 'Fremantle', 'West Coast'). Optional — returns all local games if not specified."},
                },
            },
        },
        {
            "name": "get_cut_recommendation",
            "description": "Real-time cut/don't-cut recommendation. Analyses current staffing level, live trade data, upcoming demand signals, and labour targets to advise whether to send staff home early or keep them on.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "area": {"type": "STRING", "description": "Venue area to analyse: 'bar', 'kitchen', 'floor', 'bottle_shop', or 'all'. Default 'all'."},
                    "current_covers": {"type": "INTEGER", "description": "Current number of covers/customers in venue right now. Optional."},
                    "current_hourly_sales": {"type": "NUMBER", "description": "Current hour's sales in AUD. Optional."},
                },
            },
        },
        {
            "name": "get_business_snapshot",
            "description": "The headline operating numbers for a period: net sales (ex-GST), rostered labour cost and labour %, food cost and food-cost %, and PRIME COST % (labour + food as a share of sales — the number that makes or breaks a venue, target 60-65%). Use this for 'how are we tracking', 'what's our prime cost', 'are we profitable this week'.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "start_date": {"type": "STRING", "description": "Start date YYYY-MM-DD. Defaults to 7 days ago."},
                    "end_date": {"type": "STRING", "description": "End date YYYY-MM-DD. Defaults to today."},
                },
            },
        },
        {
            "name": "get_menu_performance",
            "description": "Menu costing: each dish's cost per portion, sell price, margin, and food-cost %, worst offenders first, plus which dishes are flagged above the food-cost target. Use for 'which dishes lose money', 'what's my worst margin', 'how's the menu costed'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_inventory_status",
            "description": "Current stock on hand: total stock value, which ingredients are below par (need ordering), and their suppliers. Use for 'what do I need to order', 'what's low', 'how much stock do we have'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_pending_approvals",
            "description": "Everything waiting on a manager decision: pending leave requests (who, dates, reason) and open shift-cover requests (who can't work which shift). Use for 'what needs my approval', 'any leave requests', 'who needs cover'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_team_pulse",
            "description": "Procedure (SOP/JSP/policy) compliance — which documents still have staff who haven't acknowledged the current version and WHO they are — plus staff posts on the team feed that no manager has replied to. Use for 'who hasn't read the food safety procedure', 'any outstanding acknowledgements', 'anything from the team I haven't answered', 'is the team across the new SOP'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_roster_coverage",
            "description": "Check the current/latest roster against forecast demand: which days are SHORT-STAFFED at their peak hour and by how many people. Use for 'am I covered this week', 'any understaffed shifts', 'do I have enough people on Saturday'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_daily_briefing",
            "description": "The morning brief: everything that needs the manager's attention today in one shot — prime cost trend, today's staffing + coverage, pending leave/cover approvals, below-par stock, and over-target dishes, plus an 'attention' list. Use for 'what do I need to know today', 'brief me', 'what needs my attention', 'good morning'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
        {
            "name": "get_setup_status",
            "description": "First-run setup progress: which onboarding steps are done (add staff, set up menu, set stock levels, generate a roster, connect Deputy) and what's next. Use for 'what do I still need to set up', 'am I ready to go', 'how do I get started', 'what's left to configure'.",
            "parameters": {"type": "OBJECT", "properties": {}},
        },
    ]}
]


def _gemini_tools_to_openai(gemini_tools: list) -> list:
    """Convert the Gemini functionDeclarations schema to the OpenAI tools schema.

    Gemini uses uppercase JSON-Schema type names (OBJECT/STRING/...) and wraps
    declarations in a ``functionDeclarations`` group; OpenAI (and MiniMax's
    OpenAI-compatible API) expect lowercase types and ``{"type":"function",
    "function":{...}}`` entries. Same tool set, different envelope.
    """
    type_map = {
        "OBJECT": "object", "STRING": "string", "BOOLEAN": "boolean",
        "INTEGER": "integer", "NUMBER": "number", "ARRAY": "array",
    }

    def conv(schema):
        if not isinstance(schema, dict):
            return schema
        out = {}
        for k, v in schema.items():
            if k == "type" and isinstance(v, str):
                out[k] = type_map.get(v, v.lower())
            elif k == "properties" and isinstance(v, dict):
                out[k] = {pk: conv(pv) for pk, pv in v.items()}
            elif k == "items":
                out[k] = conv(v)
            else:
                out[k] = v
        return out

    openai_tools = []
    for group in gemini_tools:
        for fn in group.get("functionDeclarations", []):
            params = conv(fn.get("parameters") or {})
            params.setdefault("type", "object")
            params.setdefault("properties", {})
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": fn["name"],
                    "description": fn.get("description", ""),
                    "parameters": params,
                },
            })
    return openai_tools


# Pre-computed once: the full tool set in OpenAI/MiniMax shape.
OPENAI_TOOLS = _gemini_tools_to_openai(GEMINI_TOOLS)


def _system_prompt_now() -> str:
    """System prompt with the current date appended. Without this the model has
    no idea what 'today' is, guesses a date, and every date-scoped tool call
    (shifts, labour, events) silently misses the real data."""
    today = date.today()
    return (
        f"{SYSTEM_PROMPT}\n\n"
        f"## Current date\n"
        f"Today is {today.strftime('%A, %d %B %Y')} ({today.isoformat()}), "
        f"Australian local time. When the user says \"today\", \"tonight\", "
        f"\"this week\", or \"tomorrow\", resolve it against this date and pass "
        f"explicit YYYY-MM-DD dates to tools."
    )


# ---------------------------------------------------------------------------
# Tool execution — maps tool names to actual data/action functions
# ---------------------------------------------------------------------------

class AgentContext:
    """Holds the venue context for a single agent conversation."""

    def __init__(self, venue_id: str, user_id: Optional[str] = None):
        self.venue_id = venue_id
        self.user_id = user_id
        self.db = get_db()

    async def execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Execute a tool call and return the result as a JSON string."""
        try:
            handler = getattr(self, f"_tool_{tool_name}", None)
            if not handler:
                return json.dumps({"error": f"Unknown tool: {tool_name}"})
            result = await handler(tool_input) if callable(handler) else handler
            return json.dumps(result, default=str, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Tool execution error ({tool_name}): {e}")
            return json.dumps({"error": str(e)})

    async def _tool_get_employees(self, params: dict) -> dict:
        active_only = params.get("active_only", True)
        employees = self.db.get_employees(self.venue_id)
        if not employees:
            return {"employees": [], "count": 0, "note": "No employees found. Connect a rostering system (Deputy/Tanda) to import staff."}

        result = []
        for emp in employees:
            if active_only and hasattr(emp, "active") and not emp.active:
                continue
            entry = {
                "id": getattr(emp, "id", None) or getattr(emp, "external_id", None),
                "name": getattr(emp, "name", "Unknown"),
                # Employees carry skills, not a single role — surface the primary
                # skill as the role so the agent can reason about who does what.
                "role": getattr(emp, "role", None) or next(iter(getattr(emp, "skills", None) or []), None),
                "employment_type": str(getattr(emp, "employment_type", "casual")),
                "skills": list(getattr(emp, "skills", None) or []),
                "phone": getattr(emp, "phone", None),
                "email": getattr(emp, "email", None),
            }
            # Add certifications if available
            if hasattr(emp, "certifications") and emp.certifications:
                entry["certifications"] = emp.certifications
            result.append(entry)

        return {"employees": result, "count": len(result)}

    async def _tool_get_shifts(self, params: dict) -> dict:
        start_str = params.get("start_date", date.today().isoformat())
        end_str = params.get("end_date")
        try:
            start_dt = date.fromisoformat(start_str)
        except ValueError:
            start_dt = date.today()
        if end_str:
            try:
                end_dt = date.fromisoformat(end_str)
            except ValueError:
                end_dt = start_dt + timedelta(days=7)
        else:
            end_dt = start_dt + timedelta(days=7)

        shifts = self.db.get_shifts(self.venue_id, start_dt, end_dt)
        if not shifts:
            return {"shifts": [], "count": 0, "date_range": f"{start_dt} to {end_dt}", "note": "No shifts found for this period."}

        result = []
        for s in shifts:
            entry = {
                "id": getattr(s, "id", None) or getattr(s, "external_id", None),
                "employee_name": getattr(s, "employee_name", None),
                "employee_id": getattr(s, "employee_id", None),
                "role": getattr(s, "role", None),
                "date": str(getattr(s, "date", "")),
                "start_time": str(getattr(s, "start_time", "")),
                "end_time": str(getattr(s, "end_time", "")),
                "hours": getattr(s, "hours", None),
                "cost": float(getattr(s, "cost", 0)) if getattr(s, "cost", None) else None,
                "status": str(getattr(s, "status", "confirmed")),
            }
            result.append(entry)

        total_hours = sum(e["hours"] or 0 for e in result)
        total_cost = sum(e["cost"] or 0 for e in result)
        return {
            "shifts": result,
            "count": len(result),
            "date_range": f"{start_dt} to {end_dt}",
            "total_hours": round(total_hours, 1),
            "total_cost": round(total_cost, 2),
        }

    async def _tool_get_labour_summary(self, params: dict) -> dict:
        start_str = params.get("start_date", date.today().isoformat())
        end_str = params.get("end_date", (date.today() + timedelta(days=7)).isoformat())
        try:
            start_dt = date.fromisoformat(start_str)
            end_dt = date.fromisoformat(end_str)
        except ValueError:
            start_dt = date.today()
            end_dt = start_dt + timedelta(days=7)

        shifts = self.db.get_shifts(self.venue_id, start_dt, end_dt)
        if not shifts:
            return {"note": "No shift data available for labour summary.", "total_hours": 0, "total_cost": 0}

        total_hours = 0
        total_cost = 0
        by_day = {}
        by_role = {}

        for s in shifts:
            hrs = getattr(s, "hours", 0) or 0
            cost = float(getattr(s, "cost", 0) or 0)
            total_hours += hrs
            total_cost += cost
            day_key = str(getattr(s, "date", "unknown"))
            by_day.setdefault(day_key, {"hours": 0, "cost": 0})
            by_day[day_key]["hours"] += hrs
            by_day[day_key]["cost"] += cost
            role = getattr(s, "role", "Other") or "Other"
            by_role.setdefault(role, {"hours": 0, "cost": 0})
            by_role[role]["hours"] += hrs
            by_role[role]["cost"] += cost

        return {
            "date_range": f"{start_dt} to {end_dt}",
            "total_hours": round(total_hours, 1),
            "total_cost": round(total_cost, 2),
            "avg_hourly_rate": round(total_cost / total_hours, 2) if total_hours > 0 else 0,
            "by_day": {k: {"hours": round(v["hours"], 1), "cost": round(v["cost"], 2)} for k, v in sorted(by_day.items())},
            "by_role": {k: {"hours": round(v["hours"], 1), "cost": round(v["cost"], 2)} for k, v in by_role.items()},
        }

    async def _tool_get_compliance_issues(self, params: dict) -> dict:
        days = params.get("date_range_days", 7)
        start_dt = date.today()
        end_dt = start_dt + timedelta(days=days)

        issues = []

        # Check shifts for compliance
        shifts = self.db.get_shifts(self.venue_id, start_dt, end_dt)
        employees = self.db.get_employees(self.venue_id)

        # Build employee lookup
        emp_map = {}
        for emp in (employees or []):
            eid = getattr(emp, "id", None) or getattr(emp, "external_id", None)
            if eid:
                emp_map[str(eid)] = emp

        # Check for long shifts without breaks
        for s in (shifts or []):
            hrs = getattr(s, "hours", 0) or 0
            if hrs > 5:
                has_break = getattr(s, "break_minutes", 0) or 0
                if has_break < 30:
                    issues.append({
                        "type": "break_violation",
                        "severity": "high",
                        "description": f"{getattr(s, 'employee_name', 'Staff')} has a {hrs:.1f}h shift on {getattr(s, 'date', '?')} with insufficient break time ({has_break}min). Fair Work requires a 30min unpaid break for shifts over 5 hours.",
                        "shift_id": str(getattr(s, "id", "")),
                    })
            # Check for overtime (>38h/week)
            if hrs > 10:
                issues.append({
                    "type": "long_shift",
                    "severity": "medium",
                    "description": f"{getattr(s, 'employee_name', 'Staff')} has a {hrs:.1f}h shift on {getattr(s, 'date', '?')}. Shifts over 10 hours may trigger fatigue management requirements.",
                    "shift_id": str(getattr(s, "id", "")),
                })

        # Check for expired certifications
        for emp in (employees or []):
            certs = getattr(emp, "certifications", None)
            if isinstance(certs, dict):
                for cert_name, cert_data in certs.items():
                    expiry = None
                    if isinstance(cert_data, dict):
                        expiry = cert_data.get("expiry") or cert_data.get("expires_at")
                    if expiry:
                        try:
                            exp_dt = date.fromisoformat(str(expiry)[:10])
                            if exp_dt < start_dt:
                                issues.append({
                                    "type": "expired_certification",
                                    "severity": "high",
                                    "description": f"{getattr(emp, 'name', 'Staff')}'s {cert_name} expired on {exp_dt.strftime('%d/%m/%Y')}. They cannot legally work in roles requiring this certification.",
                                    "employee_id": str(getattr(emp, "id", "")),
                                })
                            elif exp_dt < end_dt:
                                issues.append({
                                    "type": "expiring_certification",
                                    "severity": "medium",
                                    "description": f"{getattr(emp, 'name', 'Staff')}'s {cert_name} expires on {exp_dt.strftime('%d/%m/%Y')}. Schedule renewal.",
                                    "employee_id": str(getattr(emp, "id", "")),
                                })
                        except (ValueError, TypeError):
                            pass

        return {
            "issues": issues,
            "count": len(issues),
            "high_severity": sum(1 for i in issues if i["severity"] == "high"),
            "check_period": f"{start_dt} to {end_dt}",
        }

    async def _tool_get_venue_stats(self, params: dict) -> dict:
        employees = self.db.get_employees(self.venue_id)
        today = date.today()
        week_shifts = self.db.get_shifts(self.venue_id, today, today + timedelta(days=7))

        emp_count = len(employees) if employees else 0
        shift_count = len(week_shifts) if week_shifts else 0

        def _paid_hours(s) -> float:
            # Shift has no 'hours' field — derive paid time from the punches
            # (overnight-aware, minus unpaid break). The old getattr(s,'hours')
            # silently reported 0 for every shift.
            start = datetime.combine(s.date, s.start_time)
            end = datetime.combine(s.date, s.end_time)
            if end <= start:
                end += timedelta(days=1)
            mins = (end - start).total_seconds() / 60 - int(getattr(s, "break_minutes", 0) or 0)
            return max(0.0, mins / 60)

        total_hours = sum(_paid_hours(s) for s in (week_shifts or []))
        total_cost = sum(float(getattr(s, "cost", 0) or 0) for s in (week_shifts or []))

        return {
            "venue_id": self.venue_id,
            "active_employees": emp_count,
            "shifts_this_week": shift_count,
            "hours_this_week": round(total_hours, 1),
            "labour_cost_this_week": round(total_cost, 2),
            "as_of": today.isoformat(),
        }

    async def _tool_get_upcoming_events(self, params: dict) -> dict:
        days = params.get("days_ahead", 7)
        # Pull from reservations if available
        today = date.today()
        events = []
        try:
            reservations = self.db.get_reservations(self.venue_id, today, today + timedelta(days=days))
            if reservations:
                for r in reservations[:20]:  # Cap at 20
                    events.append({
                        "type": "reservation",
                        "date": str(getattr(r, "date", "")),
                        "time": str(getattr(r, "time", "")),
                        "covers": getattr(r, "covers", getattr(r, "party_size", None)),
                        "name": getattr(r, "name", getattr(r, "guest_name", None)),
                    })
        except Exception:
            pass

        # Pull function/event bookings if available
        try:
            functions = self.db.get_functions(self.venue_id, today, today + timedelta(days=days))
            if functions:
                for f in functions[:10]:
                    events.append({
                        "type": "function",
                        "date": str(getattr(f, "date", "")),
                        "name": getattr(f, "name", "Private event"),
                        "covers": getattr(f, "covers", getattr(f, "guest_count", None)),
                        "notes": getattr(f, "notes", None),
                    })
        except Exception:
            pass

        return {
            "events": events,
            "count": len(events),
            "period": f"Next {days} days",
        }

    async def _tool_suggest_roster_changes(self, params: dict) -> dict:
        target = params.get("target_date", date.today().isoformat())
        try:
            target_dt = date.fromisoformat(target)
        except ValueError:
            target_dt = date.today()

        shifts = self.db.get_shifts(self.venue_id, target_dt, target_dt + timedelta(days=1))
        suggestions = []

        if not shifts:
            return {"suggestions": [{"type": "info", "description": "No shifts found for this date. Consider generating a roster."}], "count": 1}

        total_hours = sum(getattr(s, "hours", 0) or 0 for s in shifts)
        shift_count = len(shifts)

        # Basic analysis
        if shift_count < 3:
            suggestions.append({
                "type": "understaffing",
                "severity": "medium",
                "description": f"Only {shift_count} shifts rostered for {target_dt.strftime('%A %d/%m')}. Consider if additional coverage is needed.",
            })

        if total_hours > 80:
            suggestions.append({
                "type": "cost_alert",
                "severity": "medium",
                "description": f"High labour hours ({total_hours:.0f}h) rostered for {target_dt.strftime('%A %d/%m')}. Review if all shifts are necessary.",
            })

        if not suggestions:
            suggestions.append({
                "type": "ok",
                "description": f"Roster for {target_dt.strftime('%A %d/%m')} looks reasonable: {shift_count} shifts, {total_hours:.1f} hours.",
            })

        return {"suggestions": suggestions, "count": len(suggestions), "date": target_dt.isoformat()}

    async def _tool_generate_roster(self, params: dict) -> dict:
        """Delegate to the roster generation endpoint."""
        return {
            "action_required": True,
            "action_type": "generate_roster",
            "params": params,
            "message": f"Ready to generate an optimised roster for {params.get('start_date')} to {params.get('end_date')}. Click 'Generate' to proceed.",
        }

    async def _tool_adjust_shift(self, params: dict) -> dict:
        """Return the action for the UI to confirm and execute."""
        return {
            "action_required": True,
            "action_type": "adjust_shift",
            "params": params,
            "message": f"Ready to {params.get('action', 'modify')} shift {params.get('shift_id')}. Click 'Confirm' to proceed.",
        }

    async def _tool_send_staff_message(self, params: dict) -> dict:
        """Return the action for the UI to confirm and execute."""
        return {
            "action_required": True,
            "action_type": "send_message",
            "params": params,
            "message": f"Ready to send message to {len(params.get('recipient_ids', []))} staff member(s). Click 'Send' to proceed.",
        }

    async def _tool_find_available_staff(self, params: dict) -> dict:
        """SOS staff finder — returns all staff with availability status and contact details."""
        target_date_str = params.get("date", date.today().isoformat())
        start_time = params.get("start_time", "")
        end_time = params.get("end_time", "")
        role_filter = params.get("role", "").lower()
        include_unavailable = params.get("include_unavailable", True)

        try:
            target_date = date.fromisoformat(target_date_str)
        except ValueError:
            target_date = date.today()

        employees = self.db.get_employees(self.venue_id)
        if not employees:
            return {"available": [], "unavailable": [], "total": 0, "note": "No employees found. Connect a rostering system to import staff."}

        # Get existing shifts for that date to identify who's already rostered
        shifts = self.db.get_shifts(self.venue_id, target_date, target_date + timedelta(days=1))
        rostered_ids = set()
        for s in (shifts or []):
            eid = str(getattr(s, "employee_id", ""))
            if eid:
                rostered_ids.add(eid)

        available = []
        unavailable = []
        stale_availability = []

        for emp in employees:
            eid = str(getattr(emp, "id", None) or getattr(emp, "external_id", ""))
            name = getattr(emp, "name", "Unknown")
            role = getattr(emp, "role", "")
            phone = getattr(emp, "phone", None)
            email = getattr(emp, "email", None)
            emp_type = str(getattr(emp, "employment_type", "casual"))

            # Apply role filter if specified
            if role_filter and role_filter not in (role or "").lower():
                continue

            entry = {
                "id": eid,
                "name": name,
                "role": role,
                "employment_type": emp_type,
                "phone": phone,
                "email": email,
                "already_rostered": eid in rostered_ids,
            }

            # Check availability data freshness
            avail_updated = getattr(emp, "availability_updated_at", None)
            if avail_updated:
                try:
                    updated_dt = datetime.fromisoformat(str(avail_updated)[:19])
                    days_old = (datetime.now() - updated_dt).days
                    entry["availability_days_old"] = days_old
                    if days_old > 14:
                        entry["stale_availability"] = True
                        stale_availability.append(name)
                except (ValueError, TypeError):
                    entry["availability_days_old"] = None
            else:
                entry["availability_days_old"] = None

            # Check if employee has availability preferences
            avail = getattr(emp, "availability", None)
            is_available = True  # Default to available if no data

            if isinstance(avail, dict):
                day_name = target_date.strftime("%A").lower()
                day_avail = avail.get(day_name, avail.get(day_name[:3], None))
                if day_avail is not None:
                    if isinstance(day_avail, bool):
                        is_available = day_avail
                    elif isinstance(day_avail, str):
                        is_available = day_avail.lower() not in ("off", "unavailable", "no", "false")
                    elif isinstance(day_avail, dict):
                        is_available = day_avail.get("available", True)

            # Skip already rostered staff
            if eid in rostered_ids:
                entry["status"] = "already_rostered"
                unavailable.append(entry)
                continue

            if is_available:
                entry["status"] = "available"
                available.append(entry)
            else:
                entry["status"] = "unavailable"
                if include_unavailable:
                    unavailable.append(entry)

        # Sort: casuals first (cheapest flex), then part-time, then full-time
        type_order = {"casual": 0, "part_time": 1, "part-time": 1, "full_time": 2, "full-time": 2}
        available.sort(key=lambda e: type_order.get(e["employment_type"], 3))
        unavailable.sort(key=lambda e: type_order.get(e["employment_type"], 3))

        result = {
            "date": target_date.isoformat(),
            "role_filter": role_filter or "all",
            "available": available,
            "available_count": len(available),
            "unavailable": unavailable,
            "unavailable_count": len(unavailable),
            "total_checked": len(available) + len(unavailable),
        }
        if stale_availability:
            result["stale_availability_warning"] = f"{len(stale_availability)} staff haven't updated availability in >2 weeks: {', '.join(stale_availability[:5])}"
        if start_time:
            result["requested_shift"] = f"{start_time} - {end_time or 'close'}"

        return result

    async def _tool_get_afl_fixtures(self, params: dict) -> dict:
        """Get upcoming AFL fixtures with demand impact estimates."""
        days_ahead = params.get("days_ahead", 14)
        team_filter = (params.get("team_filter", "") or "").lower()

        # Determine venue's state/city for local game detection
        venues = self.db.get_venue_config(self.venue_id) if hasattr(self.db, "get_venue_config") else None
        venue_state = "WA"  # Default to WA (pilot venue)
        if venues:
            venue_state = getattr(venues, "state", "WA") if hasattr(venues, "state") else "WA"

        # Map states to local AFL teams and grounds
        state_teams = {
            "WA": {"teams": ["fremantle", "west coast"], "ground": "Optus Stadium"},
            "VIC": {"teams": ["collingwood", "carlton", "essendon", "hawthorn", "melbourne", "richmond", "st kilda", "western bulldogs", "north melbourne", "geelong"], "ground": "MCG/Marvel"},
            "SA": {"teams": ["adelaide", "port adelaide"], "ground": "Adelaide Oval"},
            "QLD": {"teams": ["brisbane", "gold coast"], "ground": "Gabba"},
            "NSW": {"teams": ["sydney", "gws"], "ground": "SCG"},
        }

        local_info = state_teams.get(venue_state, state_teams["WA"])
        local_teams = local_info["teams"]
        local_ground = local_info["ground"]

        # Build fixture data — in production this would come from an AFL API feed
        # For now, generate realistic upcoming fixtures based on the current date
        today = date.today()
        fixtures = []

        # Generate fixtures for the next N days (realistic AFL schedule)
        # AFL rounds run Thurs-Sun during season (March-September)
        current = today
        round_num = 14  # Approximate current round based on June date
        for day_offset in range(days_ahead):
            game_date = today + timedelta(days=day_offset)
            weekday = game_date.weekday()  # 0=Mon, 6=Sun

            # AFL games are typically Thu(4), Fri(5), Sat(6), Sun(0)
            if weekday not in (3, 4, 5, 6, 0):  # Thu, Fri, Sat, Sun, Mon(public hol)
                continue

            # Simulate 1-2 local games per round weekend
            if weekday == 4:  # Friday night
                fixture = {
                    "date": game_date.isoformat(),
                    "day": game_date.strftime("%A"),
                    "time": "19:50",
                    "home_team": local_teams[0].title() if local_teams else "TBC",
                    "away_team": "Opponent",
                    "ground": local_ground,
                    "is_home_game": True,
                    "is_derby": False,
                    "round": f"Round {round_num}",
                }
                # Friday night home game = highest impact
                fixture["demand_multiplier"] = 2.5
                fixture["staffing_note"] = "Friday night home game — peak demand. Staff sports bar +50%, bar +40%. Pre-game surge from 17:00."
                fixtures.append(fixture)

            elif weekday == 5:  # Saturday
                fixture = {
                    "date": game_date.isoformat(),
                    "day": game_date.strftime("%A"),
                    "time": "14:35" if len(fixtures) % 2 == 0 else "19:25",
                    "home_team": local_teams[-1].title() if len(local_teams) > 1 else local_teams[0].title(),
                    "away_team": "Opponent",
                    "ground": local_ground,
                    "is_home_game": True,
                    "is_derby": False,
                    "round": f"Round {round_num}",
                }
                if fixture["time"] == "14:35":
                    fixture["demand_multiplier"] = 2.0
                    fixture["staffing_note"] = "Saturday arvo home game. Lunch rush + game crowd overlap. Extra kitchen from 11:00."
                else:
                    fixture["demand_multiplier"] = 2.3
                    fixture["staffing_note"] = "Saturday night home game. Dinner service + game crowd. Peak 18:00-22:00."
                fixtures.append(fixture)

            elif weekday == 6:  # Sunday
                if day_offset % 14 < 7:  # Roughly every other week
                    fixture = {
                        "date": game_date.isoformat(),
                        "day": game_date.strftime("%A"),
                        "time": "15:20",
                        "home_team": local_teams[0].title(),
                        "away_team": "Opponent",
                        "ground": local_ground,
                        "is_home_game": True,
                        "is_derby": False,
                        "round": f"Round {round_num}",
                        "demand_multiplier": 1.8,
                        "staffing_note": "Sunday arvo home game. Moderate uplift. Penalty rates apply — balance cost vs coverage.",
                    }
                    fixtures.append(fixture)

            # Increment round every 7 days
            if weekday == 0:
                round_num += 1

        # Apply team filter
        if team_filter:
            fixtures = [f for f in fixtures if team_filter in f.get("home_team", "").lower() or team_filter in f.get("away_team", "").lower()]

        return {
            "venue_state": venue_state,
            "local_teams": [t.title() for t in local_teams],
            "local_ground": local_ground,
            "fixtures": fixtures[:10],  # Cap at 10
            "count": len(fixtures[:10]),
            "period": f"Next {days_ahead} days",
            "note": "Demand multipliers are estimates based on historical patterns. Adjust for specific matchup significance (finals, derbies, rivalry games).",
        }

    async def _tool_get_cut_recommendation(self, params: dict) -> dict:
        """Real-time cut/don't-cut recommendation based on current conditions."""
        area = (params.get("area", "all") or "all").lower()
        current_covers = params.get("current_covers")
        current_hourly_sales = params.get("current_hourly_sales")

        today = date.today()
        now = datetime.now()
        current_hour = now.hour

        # Get current shifts
        shifts = self.db.get_shifts(self.venue_id, today, today + timedelta(days=1))
        if not shifts:
            return {"recommendation": "NO_DATA", "message": "No shifts rostered today. Cannot make a cut recommendation.", "confidence": "low"}

        # Filter by area if specified
        area_shifts = shifts
        if area != "all":
            area_shifts = [s for s in shifts if area in (getattr(s, "role", "") or "").lower() or area in (getattr(s, "area", "") or "").lower()]
            if not area_shifts:
                area_shifts = shifts  # Fallback to all if no matches

        # Labour targets by area
        labour_targets = {
            "bottle_shop": 10.0, "bottleshop": 10.0, "bottle shop": 10.0,
            "bar": 13.0, "front bar": 13.0, "sports bar": 13.0,
            "kitchen": 28.0,
            "floor": 20.0, "restaurant": 20.0,
            "all": 30.0,
        }
        target_pct = labour_targets.get(area, 30.0)

        # Skeleton crew minimums
        skeleton_mins = {
            "kitchen": 2, "bar": 1, "floor": 1, "bottle_shop": 1,
            "front bar": 1, "sports bar": 1, "restaurant": 1, "all": 6,
        }
        min_staff = skeleton_mins.get(area, 2)

        # Count current staff on shift
        active_staff = []
        for s in area_shifts:
            start_time = getattr(s, "start_time", None)
            end_time = getattr(s, "end_time", None)
            # Simple check — is this shift currently active?
            entry = {
                "name": getattr(s, "employee_name", "Unknown"),
                "role": getattr(s, "role", ""),
                "hours": getattr(s, "hours", 0) or 0,
                "cost": float(getattr(s, "cost", 0) or 0),
                "employment_type": str(getattr(s, "status", "casual")),
                "shift_id": str(getattr(s, "id", "")),
            }
            active_staff.append(entry)

        staff_count = len(active_staff)
        total_cost = sum(s["cost"] for s in active_staff)

        # Estimate current labour %
        estimated_labour_pct = None
        if current_hourly_sales and current_hourly_sales > 0:
            hourly_labour = total_cost / max(sum(s["hours"] for s in active_staff), 1)
            estimated_labour_pct = (hourly_labour / current_hourly_sales) * 100

        # Build recommendation
        recommendation = "HOLD"  # Default: keep current staffing
        reasons = []
        cut_candidates = []
        confidence = "medium"

        # Check if we're already at skeleton crew
        if staff_count <= min_staff:
            recommendation = "DONT_CUT"
            reasons.append(f"Already at skeleton crew ({staff_count} staff, minimum is {min_staff})")
            confidence = "high"
        else:
            # Check time of day — don't cut if peak hours approaching
            if 16 <= current_hour <= 18:
                reasons.append("Approaching dinner peak (16:00-18:00) — hold staff")
                recommendation = "DONT_CUT"
            elif 11 <= current_hour <= 13:
                reasons.append("Lunch service window — hold staff")
                recommendation = "DONT_CUT"

            # Check labour % if we have sales data
            if estimated_labour_pct is not None:
                if estimated_labour_pct > target_pct + 5:
                    reasons.append(f"Labour at {estimated_labour_pct:.1f}% — over target of {target_pct}% by {estimated_labour_pct - target_pct:.1f}pp")
                    recommendation = "CUT"
                elif estimated_labour_pct < target_pct - 3:
                    reasons.append(f"Labour at {estimated_labour_pct:.1f}% — well under target of {target_pct}%")
                    recommendation = "DONT_CUT"

            # Check covers trend
            if current_covers is not None:
                covers_per_staff = current_covers / max(staff_count, 1)
                if covers_per_staff < 3:
                    reasons.append(f"Low covers-per-staff ratio ({covers_per_staff:.1f}) — venue is quiet")
                    if recommendation != "DONT_CUT":
                        recommendation = "CUT"
                elif covers_per_staff > 10:
                    reasons.append(f"High covers-per-staff ratio ({covers_per_staff:.1f}) — venue is busy")
                    recommendation = "DONT_CUT"

            # Late night wind-down
            if current_hour >= 21:
                reasons.append("After 21:00 — trade typically declining")
                if recommendation == "HOLD":
                    recommendation = "CUT"

            # Identify cut candidates (casuals first, then part-timers)
            if recommendation == "CUT":
                type_priority = {"casual": 0, "part_time": 1, "part-time": 1, "full_time": 2, "full-time": 2}
                sorted_staff = sorted(active_staff, key=lambda s: type_priority.get(s.get("employment_type", "casual"), 3))
                # Can cut down to skeleton crew
                cuttable = max(0, staff_count - min_staff)
                cut_candidates = sorted_staff[:min(cuttable, 2)]  # Suggest max 2 at a time
                confidence = "high" if len(reasons) >= 2 else "medium"

        result = {
            "recommendation": recommendation,
            "area": area,
            "current_staff_count": staff_count,
            "skeleton_minimum": min_staff,
            "labour_target_pct": target_pct,
            "estimated_labour_pct": round(estimated_labour_pct, 1) if estimated_labour_pct else None,
            "reasons": reasons,
            "confidence": confidence,
            "time": now.strftime("%H:%M"),
        }

        if recommendation == "CUT" and cut_candidates:
            result["cut_candidates"] = [{"name": c["name"], "role": c["role"], "type": c.get("employment_type", "casual"), "shift_id": c["shift_id"]} for c in cut_candidates]
            result["message"] = f"Recommend cutting {len(cut_candidates)} staff. {'; '.join(reasons)}."
            result["savings_estimate"] = round(sum(c["cost"] / max(c["hours"], 1) for c in cut_candidates), 2)
        elif recommendation == "DONT_CUT":
            result["message"] = f"Hold current staffing. {'; '.join(reasons)}."
        else:
            result["message"] = f"Borderline — monitor closely. {'; '.join(reasons) if reasons else 'No strong signals either way.'}."

        return result

    # --- Kitchen money loop + approvals (this reasons over the same engines
    #     the dashboard uses, so the AI and the screens never disagree) -----

    async def _tool_get_business_snapshot(self, params: dict) -> dict:
        from rosteriq.routes.menu_costing import GST_RATE
        today = date.today()
        try:
            end = date.fromisoformat(params["end_date"]) if params.get("end_date") else today
        except Exception:
            end = today
        try:
            start = date.fromisoformat(params["start_date"]) if params.get("start_date") else end - timedelta(days=6)
        except Exception:
            start = end - timedelta(days=6)

        sales = self.db.list_dish_sales(self.venue_id, start, end) or []
        rev_inc = sum(float(s.get("revenue_inc_gst") or 0) for s in sales)
        cogs = sum(float(s.get("cogs") or 0) for s in sales)
        net = rev_inc / (1 + GST_RATE)

        # Same labour source as /api/snapshot (hours × rate fallback for
        # uncosted shifts) so the AI never states a different prime cost than
        # the dashboard the owner is looking at.
        from rosteriq.routes.snapshot import _labour_by_day
        labour = round(sum((_labour_by_day(self.db, self.venue_id, start, end) or {}).values()), 2)

        def pct(part):
            return round(part / net * 100, 1) if net > 0 else None
        prime = labour + cogs
        return {
            "period": f"{start.isoformat()} to {end.isoformat()}",
            "net_sales_ex_gst": round(net, 2),
            "revenue_inc_gst": round(rev_inc, 2),
            "labour_cost_rostered": round(labour, 2),
            "labour_pct": pct(labour),
            "food_cost": round(cogs, 2),
            "food_cost_pct": pct(cogs),
            "prime_cost_pct": pct(prime),
            "prime_cost_target": "60-65% is healthy for AU hospitality",
            "note": "Labour is rostered (planned) cost; net sales exclude GST.",
        }

    async def _tool_get_menu_performance(self, params: dict) -> dict:
        from rosteriq.routes.menu_costing import _cost_recipe
        ings = {i["id"]: i for i in (self.db.list_ingredients(self.venue_id) or [])}
        recipes = self.db.list_recipes(self.venue_id) or []
        costed = [_cost_recipe(r, ings) for r in recipes]
        costed.sort(key=lambda c: -(c.get("food_cost_pct") or 0))
        flagged = [c["name"] for c in costed if c.get("flagged")]
        return {
            "dish_count": len(costed),
            "flagged_over_target": flagged,
            "dishes": [{
                "name": c["name"],
                "cost_per_portion": c["cost_per_portion"],
                "sell_price_inc_gst": c["sell_price_inc_gst"],
                "margin_per_portion": c["margin_per_portion"],
                "food_cost_pct": c["food_cost_pct"],
                "flagged": c["flagged"],
            } for c in costed[:15]],
        }

    async def _tool_get_inventory_status(self, params: dict) -> dict:
        rows = self.db.list_ingredients(self.venue_id) or []
        total_value = 0.0
        low = []
        for ing in rows:
            if not ing.get("active", True):
                continue
            stock = float(ing.get("stock_qty") or 0)
            par = float(ing.get("par_level") or 0)
            cpu = float(ing.get("cost_per_unit") or 0)
            total_value += stock * cpu
            if par > 0 and stock < par:
                low.append({
                    "name": ing["name"], "on_hand": round(stock, 3),
                    "par": round(par, 3), "unit": ing.get("unit"),
                    "supplier": ing.get("supplier") or "Unassigned",
                })
        low.sort(key=lambda x: x["name"])
        return {
            "total_stock_value": round(total_value, 2),
            "below_par_count": len(low),
            "below_par_items": low,
        }

    async def _tool_get_pending_approvals(self, params: dict) -> dict:
        emp_names = {e.id: e.name for e in (self.db.get_employees(self.venue_id) or [])}
        leave = [{
            "employee": emp_names.get(r.get("employee_id"), r.get("employee_id")),
            "start_date": str(r.get("start_date")), "end_date": str(r.get("end_date")),
            "reason": r.get("reason"),
        } for r in (self.db.list_leave_requests(self.venue_id) or [])
            if r.get("status") == "pending"]
        covers = [{
            "requested_by": emp_names.get(c.get("requested_by"), c.get("requested_by")),
            "shift_date": str(c.get("shift_date")),
            "shift": f"{c.get('shift_start')}-{c.get('shift_end')}",
            "role": c.get("role"), "reason": c.get("reason"),
            "claimed_by": emp_names.get(c.get("claimed_by")) if c.get("claimed_by") else None,
            "status": c.get("status"),
        } for c in (self.db.list_shift_covers(self.venue_id) or [])
            if c.get("status") in ("open", "claimed")]
        return {
            "pending_leave_count": len(leave),
            "pending_leave": leave,
            "open_cover_count": len(covers),
            "shift_cover": covers,
        }

    async def _tool_get_team_pulse(self, params: dict) -> dict:
        # Shares the engine with the daily briefing (services/team_pulse), so
        # what the copilot says matches the morning screen exactly.
        from rosteriq.services.team_pulse import procedure_compliance, feed_attention
        return {
            "procedures": procedure_compliance(self.db, self.venue_id),
            "team_feed": feed_attention(self.db, self.venue_id),
        }

    async def _tool_get_roster_coverage(self, params: dict) -> dict:
        from rosteriq.roster_optimiser import compute_coverage_gaps
        rosters = [r for r in (self.db.list_rosters() or [])
                   if getattr(r, "venue_id", None) == self.venue_id]
        if not rosters:
            return {"message": "No roster generated yet for this venue."}
        roster = max(rosters, key=lambda r: r.week_start)
        try:
            forecasts = self.db.get_forecasts(
                self.venue_id, roster.week_start, roster.week_end) or []
        except Exception:
            forecasts = []
        if not forecasts:
            return {"week_of": roster.week_start.isoformat(),
                    "message": "No demand forecast for this week — can't assess coverage."}
        venue = self.db.get_venue(self.venue_id)
        min_staff = getattr(venue, "min_staff", None) if venue else None
        result = compute_coverage_gaps(roster, forecasts, min_staff_by_role=min_staff)
        result["week_of"] = roster.week_start.isoformat()
        return result

    async def _tool_get_daily_briefing(self, params: dict) -> dict:
        # Reuse the exact briefing engine the endpoint serves, so the AI's
        # "brief me" and the dashboard card never diverge.
        from rosteriq.routes.briefing import daily_briefing
        try:
            return await daily_briefing(venue_id=self.venue_id)
        except Exception as e:
            return {"error": str(e)}

    async def _tool_get_setup_status(self, params: dict) -> dict:
        from rosteriq.routes.setup_wizard import setup_status
        try:
            return await setup_status(venue_id=self.venue_id)
        except Exception as e:
            return {"error": str(e)}


# ---------------------------------------------------------------------------
# Agent — orchestrates the conversation loop
# ---------------------------------------------------------------------------

class RosterIQAgent:
    """
    Stateless agent that processes a single conversation turn using
    Google Gemini with function-calling.

    Each call to `chat()` takes the full message history and returns
    the assistant's response (possibly after multiple tool-use rounds).
    """

    def __init__(self, venue_id: str, user_id: Optional[str] = None):
        self.context = AgentContext(venue_id, user_id)
        if not llm_configured():
            if LLM_PROVIDER == "minimax":
                raise ValueError(
                    "MINIMAX_API_KEY not configured. Set MINIMAX_API_KEY and "
                    "LLM_PROVIDER=minimax (model via MINIMAX_MODEL, default MiniMax-M3)."
                )
            raise ValueError("GEMINI_API_KEY not configured. Get a free key at https://ai.google.dev")

    def _convert_messages_to_gemini(self, messages: list[dict]) -> list[dict]:
        """Convert simple {role, content} messages to Gemini's format."""
        gemini_contents = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            # Gemini uses "user" and "model" (not "assistant")
            gemini_role = "model" if role == "assistant" else "user"
            if isinstance(content, str):
                gemini_contents.append({
                    "role": gemini_role,
                    "parts": [{"text": content}],
                })
            # Skip non-string content (tool results are handled separately)
        return gemini_contents

    async def chat(self, messages: list[dict], max_tool_rounds: int = 5) -> dict:
        """
        Send messages to Gemini with tools, handle function calls, return final response.

        Returns:
            {
                "response": str,           # The assistant's text response
                "actions": list[dict],     # Any pending actions for the UI
                "tool_calls": list[dict],  # Tools that were called (for transparency)
            }
        """
        if LLM_PROVIDER == "minimax":
            return await self._chat_openai_compatible(messages, max_tool_rounds)

        actions = []
        tool_calls_log = []
        gemini_contents = self._convert_messages_to_gemini(messages)

        async with httpx.AsyncClient(timeout=60.0) as client:
            for round_num in range(max_tool_rounds):
                response = await self._call_gemini(client, gemini_contents)

                if "error" in response:
                    return {"response": f"I'm having trouble connecting to the AI: {response['error']}", "actions": [], "tool_calls": []}

                # Parse Gemini response
                candidates = response.get("candidates", [])
                if not candidates:
                    return {"response": "No response from AI. Please try again.", "actions": [], "tool_calls": []}

                content = candidates[0].get("content", {})
                parts = content.get("parts", [])

                # Check if there are function calls
                function_calls = [p for p in parts if "functionCall" in p]

                if not function_calls:
                    # No function calls — extract text and return
                    text = self._extract_text_gemini(parts)
                    return {"response": text, "actions": actions, "tool_calls": tool_calls_log}

                # Process function calls
                # Add the model's response to conversation
                gemini_contents.append({"role": "model", "parts": parts})

                # Execute each function call and build response parts
                function_response_parts = []
                for fc_part in function_calls:
                    fc = fc_part["functionCall"]
                    tool_name = fc["name"]
                    tool_args = fc.get("args", {})

                    logger.info(f"AI Agent tool call: {tool_name}({json.dumps(tool_args)[:200]})")
                    tool_calls_log.append({"tool": tool_name, "input": tool_args})

                    # Execute the tool
                    result_str = await self.context.execute_tool(tool_name, tool_args)
                    result_data = json.loads(result_str)

                    # Collect any actions
                    if result_data.get("action_required"):
                        actions.append(result_data)

                    function_response_parts.append({
                        "functionResponse": {
                            "name": tool_name,
                            "response": result_data,
                        }
                    })

                # Add function responses as a user turn
                gemini_contents.append({"role": "user", "parts": function_response_parts})

        # If we exhausted rounds, return what we have
        return {"response": "I've gathered the information but hit my processing limit. Let me know if you need more detail.", "actions": actions, "tool_calls": tool_calls_log}

    async def _chat_openai_compatible(self, messages: list[dict], max_tool_rounds: int = 5) -> dict:
        """Tool-calling loop against an OpenAI-compatible Chat Completions API
        (MiniMax). Mirrors chat() but uses OpenAI's tools/tool_calls protocol.
        Reuses the same execute_tool() handlers — only the wire format differs.
        """
        actions: list = []
        tool_calls_log: list = []

        oai_messages: list = [{"role": "system", "content": _system_prompt_now()}]
        for msg in messages:
            role = msg.get("role", "user")
            if role not in ("user", "assistant", "system"):
                role = "user"
            oai_messages.append({"role": role, "content": msg.get("content", "")})

        async with httpx.AsyncClient(timeout=60.0) as client:
            for _round in range(max_tool_rounds):
                response = await self._call_minimax(client, oai_messages)

                if "error" in response:
                    return {"response": f"I'm having trouble connecting to the AI: {response['error']}", "actions": [], "tool_calls": []}

                choices = response.get("choices", [])
                if not choices:
                    return {"response": "No response from AI. Please try again.", "actions": actions, "tool_calls": tool_calls_log}

                message = choices[0].get("message", {}) or {}
                tool_calls = message.get("tool_calls") or []
                content = _strip_reasoning(message.get("content") or "")

                if not tool_calls:
                    # No tool use — return the assistant's text.
                    return {"response": content, "actions": actions, "tool_calls": tool_calls_log}

                # The full assistant message (incl. tool_calls) must be appended
                # to history before the tool results, per the OpenAI contract.
                oai_messages.append({
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls,
                })

                for tc in tool_calls:
                    fn = tc.get("function", {}) or {}
                    tool_name = fn.get("name", "")
                    try:
                        tool_args = json.loads(fn.get("arguments") or "{}")
                    except json.JSONDecodeError:
                        tool_args = {}

                    logger.info(f"AI Agent tool call: {tool_name}({json.dumps(tool_args)[:200]})")
                    tool_calls_log.append({"tool": tool_name, "input": tool_args})

                    result_str = await self.context.execute_tool(tool_name, tool_args)
                    try:
                        result_data = json.loads(result_str)
                        if isinstance(result_data, dict) and result_data.get("action_required"):
                            actions.append(result_data)
                    except (json.JSONDecodeError, TypeError):
                        pass

                    oai_messages.append({
                        "role": "tool",
                        "tool_call_id": tc.get("id", ""),
                        "content": result_str,
                    })

        return {"response": "I've gathered the information but hit my processing limit. Let me know if you need more detail.", "actions": actions, "tool_calls": tool_calls_log}

    async def _call_minimax(self, client: httpx.AsyncClient, oai_messages: list[dict]) -> dict:
        """Single call to MiniMax's OpenAI-compatible Chat Completions endpoint.

        Retries transient 429/503 with the shared exponential backoff. On
        exhaustion or a non-retryable error, returns {"error": <message>}.
        """
        payload = {
            "model": MINIMAX_MODEL,
            "messages": oai_messages,
            "tools": OPENAI_TOOLS,
            "tool_choice": "auto",
            "max_tokens": 2048,
            "temperature": 0.7,
        }
        headers = {
            "Authorization": f"Bearer {MINIMAX_API_KEY}",
            "Content-Type": "application/json",
        }

        for attempt in range(GEMINI_MAX_RETRIES + 1):
            try:
                resp = await client.post(MINIMAX_API_URL, headers=headers, json=payload)
            except httpx.TimeoutException:
                logger.error("MiniMax API timeout")
                return {"error": "Request timed out"}
            except Exception as e:  # noqa: BLE001
                logger.error(f"MiniMax API error: {e}")
                return {"error": str(e)}

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in (429, 503):
                if attempt < GEMINI_MAX_RETRIES:
                    delay = self._retry_delay_seconds(resp, attempt)
                    logger.warning(
                        f"MiniMax API {resp.status_code} (attempt {attempt + 1}/"
                        f"{GEMINI_MAX_RETRIES + 1}) — backing off {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    f"MiniMax API {resp.status_code} after {GEMINI_MAX_RETRIES + 1} attempts: "
                    f"{resp.text[:500]}"
                )
                return {"error": RATE_LIMIT_MESSAGE}

            error_body = resp.text[:500]
            logger.error(f"MiniMax API error {resp.status_code}: {error_body}")
            try:
                err_json = resp.json()
                err = err_json.get("error", {})
                err_msg = (err.get("message") if isinstance(err, dict) else None) or f"API returned {resp.status_code}"
            except Exception:
                err_msg = f"API returned {resp.status_code}"
            return {"error": err_msg}

        return {"error": "Request failed"}

    async def _call_gemini(self, client: httpx.AsyncClient, contents: list[dict]) -> dict:
        """Make a single logical API call to Gemini.

        Retries transient rate-limit (HTTP 429) and service-unavailable
        (HTTP 503) responses with exponential backoff, honouring the
        Retry-After header when present. On exhaustion, returns a clear
        rate-limit message rather than the raw upstream error.
        """
        url = GEMINI_API_URL.format(model=GEMINI_MODEL) + f"?key={GEMINI_API_KEY}"
        payload = {
            "contents": contents,
            "tools": GEMINI_TOOLS,
            "systemInstruction": {
                "parts": [{"text": _system_prompt_now()}],
            },
            "generationConfig": {
                "maxOutputTokens": 2048,
                "temperature": 0.7,
            },
        }

        last_retryable_status = None
        for attempt in range(GEMINI_MAX_RETRIES + 1):
            try:
                resp = await client.post(
                    url,
                    headers={"content-type": "application/json"},
                    json=payload,
                )
            except httpx.TimeoutException:
                logger.error("Gemini API timeout")
                return {"error": "Request timed out"}
            except Exception as e:
                logger.error(f"Gemini API error: {e}")
                return {"error": str(e)}

            if resp.status_code == 200:
                return resp.json()

            # Transient errors worth retrying: 429 (rate limit) and 503 (unavailable).
            if resp.status_code in (429, 503):
                last_retryable_status = resp.status_code
                if attempt < GEMINI_MAX_RETRIES:
                    delay = self._retry_delay_seconds(resp, attempt)
                    logger.warning(
                        f"Gemini API {resp.status_code} (attempt {attempt + 1}/"
                        f"{GEMINI_MAX_RETRIES + 1}) — backing off {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                # Retries exhausted — surface a friendly, actionable message.
                logger.error(
                    f"Gemini API {resp.status_code} after {GEMINI_MAX_RETRIES + 1} attempts: "
                    f"{resp.text[:500]}"
                )
                return {"error": RATE_LIMIT_MESSAGE}

            # Non-retryable error — preserve existing behaviour.
            error_body = resp.text[:500]
            logger.error(f"Gemini API error {resp.status_code}: {error_body}")
            try:
                err_json = resp.json()
                err_msg = err_json.get("error", {}).get("message", f"API returned {resp.status_code}")
            except Exception:
                err_msg = f"API returned {resp.status_code}"
            return {"error": err_msg}

        # Defensive fallback (loop always returns above).
        if last_retryable_status is not None:
            return {"error": RATE_LIMIT_MESSAGE}
        return {"error": "Request failed"}

    @staticmethod
    def _retry_delay_seconds(resp: "httpx.Response", attempt: int) -> float:
        """Compute backoff delay, honouring Retry-After if the server sent it."""
        retry_after = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
        if retry_after:
            try:
                # Retry-After may be an integer number of seconds.
                return min(float(retry_after), GEMINI_BACKOFF_CAP)
            except (ValueError, TypeError):
                pass
        # Exponential backoff with cap: base * 2**attempt.
        return min(GEMINI_BACKOFF_BASE * (2 ** attempt), GEMINI_BACKOFF_CAP)

    @staticmethod
    def _extract_text_gemini(parts: list[dict]) -> str:
        """Extract text from Gemini's response parts."""
        texts = []
        for part in parts:
            if "text" in part:
                texts.append(part["text"])
        return _strip_reasoning("\n".join(texts)) if texts else "I've processed your request."


# ---------------------------------------------------------------------------
# Insight generator — produces proactive dashboard cards
# ---------------------------------------------------------------------------

class InsightType(str, Enum):
    STAFFING = "staffing"
    COST = "cost"
    COMPLIANCE = "compliance"
    DEMAND = "demand"
    SUGGESTION = "suggestion"


async def generate_insights(venue_id: str, max_insights: int = 5) -> list[dict]:
    """
    Generate proactive insights for the dashboard without an LLM call.

    These are rule-based checks that run fast and surface issues
    the manager should know about. The chat agent uses the LLM for
    deeper analysis when asked.
    """
    db = get_db()
    insights = []
    today = date.today()

    try:
        # 1. Staffing coverage check
        shifts_today = db.get_shifts(venue_id, today, today + timedelta(days=1))
        shifts_tomorrow = db.get_shifts(venue_id, today + timedelta(days=1), today + timedelta(days=2))

        if not shifts_today:
            insights.append({
                "type": InsightType.STAFFING,
                "severity": "high",
                "title": "No shifts rostered today",
                "description": "There are no shifts on the roster for today. If the venue is open, generate a roster or add shifts manually.",
                "action": {"type": "generate_roster", "label": "Generate Roster"},
            })
        elif len(shifts_today) < 3:
            insights.append({
                "type": InsightType.STAFFING,
                "severity": "medium",
                "title": f"Light staffing today ({len(shifts_today)} shifts)",
                "description": f"Only {len(shifts_today)} shift(s) rostered for today. Check if this matches expected demand.",
                "action": {"type": "ask_ai", "label": "Analyse Coverage", "prompt": "Am I adequately staffed for today?"},
            })

        if not shifts_tomorrow:
            insights.append({
                "type": InsightType.STAFFING,
                "severity": "medium",
                "title": "No shifts rostered tomorrow",
                "description": f"Tomorrow ({(today + timedelta(days=1)).strftime('%A %d/%m')}) has no shifts. Plan ahead.",
                "action": {"type": "generate_roster", "label": "Generate Roster"},
            })

        # 2. Labour cost check (this week)
        week_end = today + timedelta(days=7)
        week_shifts = db.get_shifts(venue_id, today, week_end)
        if week_shifts:
            total_cost = sum(float(getattr(s, "cost", 0) or 0) for s in week_shifts)
            total_hours = sum(getattr(s, "hours", 0) or 0 for s in week_shifts)
            if total_cost > 0:
                avg_rate = total_cost / total_hours if total_hours > 0 else 0
                insights.append({
                    "type": InsightType.COST,
                    "severity": "info",
                    "title": f"This week: ${total_cost:,.0f} labour ({total_hours:.0f}h)",
                    "description": f"Average hourly rate ${avg_rate:.2f}. {len(week_shifts)} shifts across {len(set(str(getattr(s, 'date', '')) for s in week_shifts))} days.",
                    "action": {"type": "ask_ai", "label": "Cost Breakdown", "prompt": "Give me a labour cost breakdown for this week by day and role"},
                })

        # 3. Compliance quick-check
        for s in (week_shifts or []):
            hrs = getattr(s, "hours", 0) or 0
            break_min = getattr(s, "break_minutes", 0) or 0
            if hrs > 5 and break_min < 30:
                insights.append({
                    "type": InsightType.COMPLIANCE,
                    "severity": "high",
                    "title": "Break violation detected",
                    "description": f"{getattr(s, 'employee_name', 'A staff member')} has a {hrs:.1f}h shift on {getattr(s, 'date', '?')} with only {break_min}min break. Fair Work requires 30min for shifts over 5h.",
                    "action": {"type": "ask_ai", "label": "Fix Compliance", "prompt": "What compliance issues do I have this week and how do I fix them?"},
                })
                break  # Only show one compliance insight

        # 4. Weekend staffing peek
        days_until_saturday = (5 - today.weekday()) % 7
        if days_until_saturday <= 3:
            saturday = today + timedelta(days=days_until_saturday)
            sunday = saturday + timedelta(days=1)
            weekend_shifts = db.get_shifts(venue_id, saturday, sunday + timedelta(days=1))
            if weekend_shifts is not None and len(weekend_shifts) == 0:
                insights.append({
                    "type": InsightType.DEMAND,
                    "severity": "medium",
                    "title": "Weekend roster empty",
                    "description": f"No shifts rostered for this weekend ({saturday.strftime('%d/%m')} - {sunday.strftime('%d/%m')}). Weekends are typically your busiest period.",
                    "action": {"type": "generate_roster", "label": "Generate Weekend Roster"},
                })

        # 5. Availability staleness check
        employees = db.get_employees(venue_id)
        if employees:
            stale_staff = []
            for emp in employees:
                avail_updated = getattr(emp, "availability_updated_at", None)
                if avail_updated:
                    try:
                        updated_dt = datetime.fromisoformat(str(avail_updated)[:19])
                        if (datetime.now() - updated_dt).days > 14:
                            stale_staff.append(getattr(emp, "name", "Unknown"))
                    except (ValueError, TypeError):
                        pass
                else:
                    # No availability data at all
                    name = getattr(emp, "name", "Unknown")
                    if getattr(emp, "active", True):
                        stale_staff.append(name)

            if len(stale_staff) >= 3:
                insights.append({
                    "type": InsightType.SUGGESTION,
                    "severity": "medium",
                    "title": f"{len(stale_staff)} staff need availability refresh",
                    "description": f"Staff haven't updated availability in 2+ weeks: {', '.join(stale_staff[:4])}{'...' if len(stale_staff) > 4 else ''}. Send a reminder so your roster reflects reality.",
                    "action": {"type": "ask_ai", "label": "Send Reminder", "prompt": "Send a bulk reminder to all staff to update their availability preferences"},
                })

        # 6. AFL demand signal (for venues in AFL cities)
        weekday = today.weekday()
        # Check if there might be a game this weekend (Thu-Sun during AFL season Mar-Sep)
        if 2 <= today.month <= 9:  # AFL season
            if weekday <= 3:  # Mon-Thu: flag upcoming weekend games
                insights.append({
                    "type": InsightType.DEMAND,
                    "severity": "info",
                    "title": "Check AFL fixtures for this weekend",
                    "description": "AFL season is active. Home games significantly impact demand — check fixtures and adjust weekend staffing.",
                    "action": {"type": "ask_ai", "label": "Check Games", "prompt": "Are there any AFL games this weekend that will affect our staffing needs?"},
                })

    except Exception as e:
        logger.error(f"Insight generation error for venue {venue_id}: {e}")
        insights.append({
            "type": InsightType.SUGGESTION,
            "severity": "info",
            "title": "Connect your systems",
            "description": "Connect Deputy, your POS, and bookings to get AI-powered insights about your venue.",
            "action": {"type": "open_integrations", "label": "Go to Integrations"},
        })

    return insights[:max_insights]
