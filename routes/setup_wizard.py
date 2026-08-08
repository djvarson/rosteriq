"""
First-run setup wizard — guides a brand-new venue owner from an empty
account to a working one, computing each step's status from REAL venue state
(not a stored checklist that can drift out of sync). Deterministic, so the
dashboard card and the AI can both answer "what do I still need to set up".

Steps (venue_created is implicit — you can't call this without a venue):
    staff        -- at least one employee
    menu         -- at least one recipe (seed or built)
    stock        -- par levels set on at least one ingredient
    roster       -- a roster generated for the venue
    connect      -- Deputy connected (OPTIONAL — for migrating existing data)

Route:
    GET /api/setup?venue_id=
"""

import logging

from fastapi import APIRouter, Query

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


@router.get("")
async def setup_status(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()

    employees = db.get_employees(venue_id) or []
    recipes = db.list_recipes(venue_id) or []
    ingredients = db.list_ingredients(venue_id) or []
    has_par = any(float(i.get("par_level") or 0) > 0 for i in ingredients)
    rosters = [r for r in (db.list_rosters() or []) if getattr(r, "venue_id", None) == venue_id]
    try:
        deputy = db.get_plugin_install(f"deputy_{venue_id}")
        connected = bool(deputy and deputy.get("status") not in (None, "uninstalled"))
    except Exception:
        connected = False

    steps = [
        {
            "key": "staff", "label": "Add your team",
            "done": len(employees) > 0,
            "detail": f"{len(employees)} staff added" if employees
                      else "Add staff in Staff — name, award level, pay rate, and their email so they can use /my.",
            "cta": "Staff", "optional": False,
        },
        {
            "key": "menu", "label": "Set up your menu",
            "done": len(recipes) > 0,
            "detail": f"{len(recipes)} dishes costed" if recipes
                      else "In Menu & Sales, add ingredients and dishes (or Seed starter menu) so costing and inventory work.",
            "cta": "Menu & Sales", "optional": False,
        },
        {
            "key": "stock", "label": "Set stock levels",
            "done": has_par,
            "detail": "Par levels set" if has_par
                      else "In Inventory, set a par level per ingredient so below-par ordering and stocktakes work.",
            "cta": "Inventory", "optional": False,
        },
        {
            "key": "roster", "label": "Generate your first roster",
            "done": len(rosters) > 0,
            "detail": "Roster generated" if rosters
                      else "On the Roster page, Generate Roster — the AI builds a week from your staff and demand.",
            "cta": "Roster", "optional": False,
        },
        {
            "key": "connect", "label": "Connect Deputy (optional)",
            "done": connected,
            "detail": "Deputy connected" if connected
                      else "On Connections, paste a Deputy access token to import your existing staff and shifts. Skip if starting fresh.",
            "cta": "Connections", "optional": True,
        },
    ]

    required = [s for s in steps if not s["optional"]]
    done_required = [s for s in required if s["done"]]
    percent = round(len(done_required) / len(required) * 100) if required else 100
    next_step = next((s for s in required if not s["done"]), None)

    return {
        "venue_id": venue_id,
        "complete": len(done_required) == len(required),
        "percent": percent,
        "steps_done": len(done_required),
        "steps_total": len(required),
        "next_step": next_step,
        "steps": steps,
    }
