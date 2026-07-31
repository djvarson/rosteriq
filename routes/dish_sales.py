"""
Dish sales — the loop that makes food cost REAL-TIME: record what sold,
stock depletes through the recipes automatically, and the summary shows
revenue vs COGS vs food-cost % for any period.

    Sell 12 Parmys -> 12 x 250g chicken leaves the shelf -> tonight's
    food cost % is fact, not last month's stocktake guess.

Routes (venue-scoped):
    POST /api/sales/record   -- record dish sales (depletes stock atomically)
    GET  /api/sales/summary  -- revenue / COGS / GP / food-cost % + breakdown

Depletion is theoretical usage (recipe quantities x qty sold). Stock is
allowed to go NEGATIVE — a negative shelf number is a loud signal that the
recipe, the counts, or the pours are off, which is exactly what the next
stocktake's variance report is for. We never clamp it quietly.
"""

import logging
import uuid
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.menu_costing import GST_RATE, _cost_recipe, _qty_in_unit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sales", tags=["dish-sales"])


class SaleLine(BaseModel):
    recipe_id: str
    qty: float = Field(..., gt=0, le=10000)


class RecordSalesBody(BaseModel):
    venue_id: str
    sale_date: Optional[date] = None
    items: List[SaleLine] = Field(..., min_length=1)


@router.post("/record")
async def record_sales(body: RecordSalesBody) -> dict:
    """Record what sold and deplete ingredient stock through the recipes."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    sale_date = body.sale_date or date.today()
    if sale_date > date.today():
        raise HTTPException(status_code=422, detail="Can't record sales for a future date")

    recipes = {r["id"]: r for r in (db.list_recipes(body.venue_id) or [])}
    unknown = [l.recipe_id for l in body.items if l.recipe_id not in recipes]
    if unknown:
        raise HTTPException(status_code=422,
                            detail=f"Unknown recipes for this venue: {', '.join(unknown)}")

    ingredients = {i["id"]: i for i in (db.list_ingredients(body.venue_id) or [])}
    recorded = []
    total_revenue = 0.0
    total_cogs = 0.0
    depleted: dict = {}

    for line in body.items:
        recipe = recipes[line.recipe_id]
        costing = _cost_recipe(recipe, ingredients)
        revenue = round(float(costing["sell_price_inc_gst"]) * line.qty, 2)
        cogs = round(float(costing["cost_per_portion"]) * line.qty, 2)
        yield_portions = float(recipe.get("yield_portions", 1)) or 1

        # Deplete each recipe ingredient, converted to its stock unit
        for item in recipe.get("items", []):
            ing = ingredients.get(item.get("ingredient_id"))
            if not ing:
                continue  # costing already reported it as missing
            per_portion = _qty_in_unit(
                float(item.get("qty", 0)), item.get("unit"), ing.get("unit", "each"),
            ) / yield_portions
            depleted[ing["id"]] = depleted.get(ing["id"], 0.0) + per_portion * line.qty

        db.save_dish_sale({
            "id": f"ds-{uuid.uuid4().hex[:12]}",
            "venue_id": body.venue_id,
            "sale_date": sale_date,
            "recipe_id": line.recipe_id,
            "recipe_name": recipe.get("name"),
            "qty": line.qty,
            "revenue_inc_gst": revenue,
            "cogs": cogs,
            "source": "manual",
            "created_at": datetime.utcnow(),
        })
        recorded.append({"recipe": recipe.get("name"), "qty": line.qty,
                         "revenue_inc_gst": revenue, "cogs": cogs})
        total_revenue += revenue
        total_cogs += cogs

    negative = []
    for ing_id, qty in depleted.items():
        db.increment_ingredient_stock(ing_id, -qty)
        after = db.get_ingredient(ing_id)
        if after is not None and float(after.get("stock_qty") or 0) < 0:
            negative.append(after.get("name"))

    logger.info(f"Sales recorded at {body.venue_id}: {len(recorded)} lines, "
                f"rev ${round(total_revenue, 2)}, cogs ${round(total_cogs, 2)}")
    return {
        "status": "recorded",
        "sale_date": sale_date.isoformat(),
        "lines": recorded,
        "total_revenue_inc_gst": round(total_revenue, 2),
        "total_cogs": round(total_cogs, 2),
        "stock_depleted": {k: round(v, 3) for k, v in depleted.items()},
        "negative_stock_warning": sorted(n for n in negative if n),
    }


@router.get("/summary")
async def sales_summary(venue_id: str = Query(...),
                        start_date: Optional[date] = Query(None),
                        end_date: Optional[date] = Query(None)) -> dict:
    """Revenue / COGS / gross profit / food-cost % for a period (default 7 days)."""
    enforce_venue_access(venue_id)
    db = get_db()
    end = end_date or date.today()
    start = start_date or (end - timedelta(days=6))
    if start > end:
        raise HTTPException(status_code=422, detail="start_date is after end_date")

    rows = db.list_dish_sales(venue_id, start, end) or []
    total_rev_inc = sum(float(r.get("revenue_inc_gst") or 0) for r in rows)
    total_cogs = sum(float(r.get("cogs") or 0) for r in rows)
    total_rev_ex = total_rev_inc / (1 + GST_RATE)

    by_recipe: dict = {}
    by_day: dict = {}
    for r in rows:
        name = r.get("recipe_name") or r.get("recipe_id")
        agg = by_recipe.setdefault(name, {"qty": 0.0, "revenue_inc_gst": 0.0, "cogs": 0.0})
        agg["qty"] += float(r.get("qty") or 0)
        agg["revenue_inc_gst"] += float(r.get("revenue_inc_gst") or 0)
        agg["cogs"] += float(r.get("cogs") or 0)
        day = str(r.get("sale_date"))[:10]
        d = by_day.setdefault(day, {"revenue_inc_gst": 0.0, "cogs": 0.0})
        d["revenue_inc_gst"] += float(r.get("revenue_inc_gst") or 0)
        d["cogs"] += float(r.get("cogs") or 0)

    return {
        "venue_id": venue_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "total_revenue_inc_gst": round(total_rev_inc, 2),
        "total_revenue_ex_gst": round(total_rev_ex, 2),
        "total_cogs": round(total_cogs, 2),
        "gross_profit": round(total_rev_ex - total_cogs, 2),
        "food_cost_pct": round(total_cogs / total_rev_ex * 100, 1) if total_rev_ex > 0 else 0.0,
        "by_recipe": [dict(name=k, qty=v["qty"],
                           revenue_inc_gst=round(v["revenue_inc_gst"], 2),
                           cogs=round(v["cogs"], 2))
                      for k, v in sorted(by_recipe.items(),
                                         key=lambda kv: -kv[1]["revenue_inc_gst"])],
        "by_day": [dict(date=k, revenue_inc_gst=round(v["revenue_inc_gst"], 2),
                        cogs=round(v["cogs"], 2))
                   for k, v in sorted(by_day.items())],
    }
