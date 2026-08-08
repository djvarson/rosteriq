"""
AI agent's Venue OS tools: the new read tools (business snapshot, menu
performance, inventory status, pending approvals) are registered and return
correct data grounded in the same engines the dashboard uses.
"""

import json
import uuid
from datetime import date, datetime, time as dtime, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.ai_agent import AgentContext, GEMINI_TOOLS


def _owner(c):
    email = f"at{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _tool_names():
    names = set()
    for group in GEMINI_TOOLS:
        for fn in group.get("function_declarations", group.get("functionDeclarations", [])):
            names.add(fn["name"])
    return names


def test_new_venueos_tools_are_registered():
    names = _tool_names()
    for t in ("get_business_snapshot", "get_menu_performance",
              "get_inventory_status", "get_pending_approvals",
              "get_roster_coverage", "get_daily_briefing", "get_setup_status"):
        assert t in names, f"{t} not registered in GEMINI_TOOLS"


@pytest.mark.asyncio
async def test_snapshot_menu_inventory_approvals_tools_ground_in_real_data():
    c = TestClient(app)
    h = _owner(c)
    vid = "ai-tool-venue"
    c.post("/venues", json={
        "id": vid, "name": "AI Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Ada Cook", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00", "email": "ada@x.com",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)

    ing = c.post("/api/menu/ingredients", json={
        "venue_id": vid, "name": "Chicken", "unit": "kg",
        "purchase_size": 5, "purchase_cost": 50, "supplier": "Acme",
    }, headers=h).json()["ingredient_id"]
    recipe = c.post("/api/menu/recipes", json={
        "venue_id": vid, "name": "Bowl", "sell_price_inc_gst": 11.0,
        "items": [{"ingredient_id": ing, "qty": 250, "unit": "g"}],
    }, headers=h).json()["recipe_id"]
    c.post("/api/inventory/levels", json={
        "venue_id": vid, "ingredient_id": ing, "stock_qty": 3, "par_level": 10,
    }, headers=h)
    c.post("/api/sales/record", json={
        "venue_id": vid, "items": [{"recipe_id": recipe, "qty": 100}],
    }, headers=h)

    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    db = get_db()
    db.save_roster(Roster(
        id="ai-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id="ai-s1", employee_id=f"{vid}-emp", date=today,
                      start_time=dtime(9, 0), end_time=dtime(17, 0), break_minutes=0,
                      status=ShiftStatus.scheduled, role="kitchen")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))
    # A pending leave request + an open cover, via the staff endpoints
    c.post("/api/me/leave", json={  # owner is not linked, so seed via db
        "start_date": (today + timedelta(days=5)).isoformat(),
        "end_date": (today + timedelta(days=6)).isoformat(), "reason": "Trip",
    }, headers=h) if False else None
    db.save_leave_request({
        "id": "ai-lv-1", "venue_id": vid, "employee_id": f"{vid}-emp",
        "start_date": today + timedelta(days=5), "end_date": today + timedelta(days=6),
        "reason": "Family trip", "status": "pending", "created_at": datetime(2026, 7, 1),
    })

    ctx = AgentContext(vid)

    # Business snapshot: 100 bowls -> net 1000; COGS 250; labour 8h*$30=240 (cost cached? no)
    snap = json.loads(await ctx.execute_tool("get_business_snapshot", {}))
    assert snap["net_sales_ex_gst"] == 1000.0
    assert snap["food_cost"] == 250.0
    assert snap["food_cost_pct"] == 25.0
    assert snap["prime_cost_pct"] is not None

    menu = json.loads(await ctx.execute_tool("get_menu_performance", {}))
    assert menu["dish_count"] == 1
    assert menu["dishes"][0]["name"] == "Bowl" and menu["dishes"][0]["food_cost_pct"] == 25.0

    inv = json.loads(await ctx.execute_tool("get_inventory_status", {}))
    assert inv["below_par_count"] == 1
    assert inv["below_par_items"][0]["name"] == "Chicken"
    assert inv["below_par_items"][0]["supplier"] == "Acme"

    appr = json.loads(await ctx.execute_tool("get_pending_approvals", {}))
    assert appr["pending_leave_count"] == 1
    assert appr["pending_leave"][0]["employee"] == "Ada Cook"
    assert appr["pending_leave"][0]["reason"] == "Family trip"
