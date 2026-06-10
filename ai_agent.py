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
import logging
from datetime import date, datetime, timedelta
from typing import Optional, AsyncGenerator
from enum import Enum

import httpx

from rosteriq.database import get_db
from rosteriq.models import Employee, Shift, State

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("ROSTERIQ_AI_MODEL", "gemini-2.0-flash")
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MAX_CONTEXT_TOKENS = 8000  # rough budget for venue context

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
    ]}
]


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
                "role": getattr(emp, "role", None),
                "employment_type": str(getattr(emp, "employment_type", "casual")),
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
        total_hours = sum(getattr(s, "hours", 0) or 0 for s in (week_shifts or []))
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
        if not GEMINI_API_KEY:
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

    async def _call_gemini(self, client: httpx.AsyncClient, contents: list[dict]) -> dict:
        """Make a single API call to Gemini."""
        url = GEMINI_API_URL.format(model=GEMINI_MODEL) + f"?key={GEMINI_API_KEY}"
        try:
            resp = await client.post(
                url,
                headers={"content-type": "application/json"},
                json={
                    "contents": contents,
                    "tools": GEMINI_TOOLS,
                    "systemInstruction": {
                        "parts": [{"text": SYSTEM_PROMPT}],
                    },
                    "generationConfig": {
                        "maxOutputTokens": 2048,
                        "temperature": 0.7,
                    },
                },
            )
            if resp.status_code != 200:
                error_body = resp.text[:500]
                logger.error(f"Gemini API error {resp.status_code}: {error_body}")
                # Parse error message if possible
                try:
                    err_json = resp.json()
                    err_msg = err_json.get("error", {}).get("message", f"API returned {resp.status_code}")
                except Exception:
                    err_msg = f"API returned {resp.status_code}"
                return {"error": err_msg}
            return resp.json()
        except httpx.TimeoutException:
            logger.error("Gemini API timeout")
            return {"error": "Request timed out"}
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return {"error": str(e)}

    @staticmethod
    def _extract_text_gemini(parts: list[dict]) -> str:
        """Extract text from Gemini's response parts."""
        texts = []
        for part in parts:
            if "text" in part:
                texts.append(part["text"])
        return "\n".join(texts) if texts else "I've processed your request."


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
