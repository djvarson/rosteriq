"""
Daily briefing — the manager's "good morning" screen and the natural payload
for a future daily SMS/push. One deterministic call that answers "what do I
need to know and do today", synthesised from the same verified engines the
dashboard renders (snapshot, coverage, inventory, approvals). No LLM cost,
so it can run every morning for every venue.
"""

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.menu_costing import GST_RATE, _cost_recipe
from rosteriq.roster_optimiser import compute_coverage_gaps

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/briefing", tags=["briefing"])


@router.get("")
async def daily_briefing(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    today = date.today()
    week_ago = today - timedelta(days=6)
    attention = []  # deterministic "what needs you today", worst first

    # --- Money: last-7-days prime cost (mirror of the snapshot) -----------
    sales = db.list_dish_sales(venue_id, week_ago, today) or []
    rev_inc = sum(float(s.get("revenue_inc_gst") or 0) for s in sales)
    cogs = sum(float(s.get("cogs") or 0) for s in sales)
    net = rev_inc / (1 + GST_RATE)
    # Use the SAME labour source as the snapshot (falls back to paid-hours ×
    # base rate when a shift's cost isn't cached) — summing only cost-bearing
    # shifts would silently understate prime cost and suppress the alert.
    from rosteriq.routes.snapshot import _labour_by_day
    labour_failed = False
    try:
        labour_7d = round(sum((_labour_by_day(db, venue_id, week_ago, today) or {}).values()), 2)
    except Exception as e:
        logger.warning(f"Briefing labour calc failed for {venue_id}: {e}")
        labour_7d = 0.0
        labour_failed = True
    prime_pct = round((labour_7d + cogs) / net * 100, 1) if (net > 0 and not labour_failed) else None
    if prime_pct is not None and prime_pct > 65:
        attention.append(f"Prime cost is {prime_pct}% (target ≤65%) — trim labour or lift price.")
    elif labour_failed:
        attention.append("Couldn't calculate labour/prime cost — check the dashboard.")

    # --- Today: roster + coverage ----------------------------------------
    today_rosters = []
    try:
        today_rosters = [r for r in (db.get_rosters_by_date_range(venue_id, today, today) or [])]
    except Exception:
        pass
    on_today = 0
    labour_today = 0.0
    seen = set()
    for r in today_rosters:
        for sh in r.shifts:
            if sh.date == today and sh.id not in seen:
                seen.add(sh.id)
                on_today += 1
                labour_today += float(sh.cost or 0)

    coverage = None
    try:
        # The roster that COVERS today (week_start <= today <= week_end), not
        # merely the one with the latest week_start — otherwise a
        # next-week roster hides today's shortfall.
        candidates = [r for r in (db.list_rosters() or [])
                      if getattr(r, "venue_id", None) == venue_id
                      and r.week_start <= today <= r.week_end]
        if candidates:
            active = max(candidates, key=lambda r: r.week_start)
            fcs = db.get_forecasts(venue_id, active.week_start, active.week_end) or []
            if fcs:
                venue = db.get_venue(venue_id)
                min_staff = getattr(venue, "min_staff", None) if venue else None
                cov = compute_coverage_gaps(active, fcs, min_staff_by_role=min_staff)
                today_row = [d for d in cov["days"] if d["date"] == today.isoformat()]
                coverage = {
                    "fully_covered": cov["fully_covered"],
                    "total_missing_staff": cov["total_missing_staff"],
                    "today": today_row[0] if today_row else None,
                }
                if today_row and today_row[0]["gap"] > 0:
                    t = today_row[0]
                    attention.append(
                        f"Today is {t['gap']} short at {t['peak_hour']} — arrange cover.")
    except Exception as e:
        logger.warning(f"Briefing coverage failed for {venue_id}: {e}")
        attention.append("Couldn't verify today's coverage — check the roster.")

    # --- Approvals waiting ------------------------------------------------
    pending_leave = len([r for r in (db.list_leave_requests(venue_id) or [])
                         if r.get("status") == "pending"])
    open_covers = len([c for c in (db.list_shift_covers(venue_id) or [])
                      if c.get("status") in ("open", "claimed")])
    if pending_leave:
        attention.append(f"{pending_leave} leave request(s) awaiting your decision.")
    if open_covers:
        attention.append(f"{open_covers} shift(s) up for cover need approval.")

    # --- Kitchen: stock + menu flags -------------------------------------
    ingredients = [i for i in (db.list_ingredients(venue_id) or []) if i.get("active", True)]
    stock_value = 0.0
    below_par = 0
    for ing in ingredients:
        stock = float(ing.get("stock_qty") or 0)
        par = float(ing.get("par_level") or 0)
        stock_value += stock * float(ing.get("cost_per_unit") or 0)
        if par > 0 and stock < par:
            below_par += 1
    if below_par:
        attention.append(f"{below_par} ingredient(s) below par — draft a supplier order.")

    ings_by_id = {i["id"]: i for i in ingredients}
    flagged_dishes = 0
    for recipe in (db.list_recipes(venue_id) or []):
        # One recipe with a bad unit conversion must not sink the whole
        # briefing — skip it, don't 422 the entire morning brief.
        try:
            c = _cost_recipe(recipe, ings_by_id)
        except Exception:
            continue
        if c.get("flagged"):
            flagged_dishes += 1
    if flagged_dishes:
        attention.append(f"{flagged_dishes} dish(es) over the food-cost target — review pricing.")

    # Waste in the last 7 days — flag it if it's material vs net sales (>2%)
    waste_7d = 0.0
    try:
        waste_7d = round(sum(float(w.get("value") or 0)
                             for w in (db.list_waste_entries(venue_id, week_ago, today) or [])), 2)
    except Exception:
        pass
    if net > 0 and waste_7d > 0.02 * net:
        attention.append(f"${waste_7d:.0f} of waste logged this week "
                         f"({round(waste_7d / net * 100, 1)}% of sales) — tighten prep.")

    return {
        "venue_id": venue_id,
        "date": today.isoformat(),
        "prime_cost_pct_7d": prime_pct,
        "net_sales_7d": round(net, 2),
        "today": {
            "staff_rostered": on_today,
            "labour_cost": round(labour_today, 2),
            "coverage": coverage,
        },
        "approvals": {"pending_leave": pending_leave, "open_covers": open_covers},
        "kitchen": {"stock_value": round(stock_value, 2),
                    "below_par_count": below_par,
                    "flagged_dishes": flagged_dishes,
                    "waste_7d": waste_7d},
        "attention": attention,
        "all_clear": len(attention) == 0,
    }
