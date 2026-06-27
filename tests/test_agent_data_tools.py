"""
The AI agent's data tools must return REAL data, not swallowed errors.

The agent (AgentContext) calls venue-scoped store readers — get_employees,
get_shifts, get_reservations, get_functions, get_venue_config. Several didn't
exist on the store, so execute_tool's try/except turned every such call into an
{"error": ...} payload to the model. These tests seed a venue + staff + a roster
with shifts and assert each tool comes back with data, not an error.
"""

import asyncio
import json
from datetime import date, time, datetime, timedelta
from decimal import Decimal

from rosteriq.database import get_db
from rosteriq.ai_agent import AgentContext
from rosteriq.models import (
    VenueConfig, Employee, EmploymentType, AwardLevel, State,
    Roster, Shift, ShiftStatus,
)

VENUE = "agent-tools-test-venue"


def _seed(db):
    now = datetime(2026, 1, 1)
    db.save_venue(VenueConfig(
        id=VENUE, name="Tool Test Venue", tanda_org_id=f"org-{VENUE}",
        state=State.vic, max_labour_pct=30.0, created_at=now,
    ))
    db.save_employees([
        Employee(
            id=f"e{i}", venue_id=VENUE, name=f"Emp {i}",
            employment_type=EmploymentType.casual, award_level=AwardLevel.level_2,
            state=State.vic, hourly_base_rate=Decimal("30.00"),
            skills=["bar"], created_at=now, updated_at=now,
        ) for i in range(1, 4)
    ])
    today = date.today()
    ws = today - timedelta(days=today.weekday())
    shifts = [
        Shift(
            id=f"s{i}", employee_id=f"e{i}", date=today,
            start_time=time(10, 0), end_time=time(18, 0), break_minutes=30,
            status=ShiftStatus.scheduled, role="bar", cost=Decimal("240.00"),
        ) for i in range(1, 4)
    ]
    db.save_roster(Roster(
        id="rtest", venue_id=VENUE, week_start=ws, week_end=ws + timedelta(days=6),
        shifts=shifts, total_cost=Decimal("720.00"), created_at=now,
    ))


def test_system_prompt_carries_todays_date():
    """The model must be told today's date, or every 'today/this week' tool call
    queries a guessed (wrong) date and silently misses the real data."""
    from rosteriq.ai_agent import _system_prompt_now
    assert date.today().isoformat() in _system_prompt_now()


def test_store_helpers_exist_and_return_data():
    db = get_db()
    _seed(db)
    today = date.today()
    nxt = today + timedelta(days=7)

    # get_shifts flattens the roster's shifts for the venue/date-range
    shifts = db.get_shifts(VENUE, today, nxt)
    assert len(shifts) == 3
    # get_venue_config aliases get_venue
    assert db.get_venue_config(VENUE) is not None
    # reservations/functions degrade gracefully (no rows -> empty, never error)
    assert db.get_reservations(VENUE, today, nxt) == []
    assert db.get_functions(VENUE, today, nxt) == []


def test_every_agent_tool_returns_data_not_error():
    db = get_db()
    _seed(db)
    ctx = AgentContext(VENUE)
    today = date.today().isoformat()
    nxt = (date.today() + timedelta(days=7)).isoformat()

    cases = [
        ("get_employees", {}),
        ("get_shifts", {"start_date": today, "end_date": nxt}),
        ("get_labour_summary", {"start_date": today, "end_date": nxt}),
        ("get_venue_stats", {}),
        ("suggest_roster_changes", {"target_date": today}),
        ("get_upcoming_events", {"days_ahead": 7}),
    ]
    for tool, args in cases:
        data = json.loads(asyncio.run(ctx.execute_tool(tool, args)))
        assert "error" not in data, f"tool {tool} returned an error: {data}"

    # the staff tool now surfaces the primary skill as the role
    emp_data = json.loads(asyncio.run(ctx.execute_tool("get_employees", {})))
    rows = emp_data.get("employees", emp_data if isinstance(emp_data, list) else [])
    assert any((r.get("role") == "bar") for r in rows), emp_data
